%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing information about space transfers stats cache.
%%% @end
%%%-------------------------------------------------------------------
-module(space_transfer_cache).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([save/3, get/2, delete/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type space_transfer_cache() :: #space_transfer_cache{}.
-type doc() :: datastore_doc:doc(space_transfer_cache()).

-export_type([space_transfer_cache/0, doc/0]).

-define(TRANSFER_INACTIVITY,
    application:get_env(?APP_NAME, gui_transfer_inactivity, 20)).
-define(MINUTE_STAT_EXPIRATION,
    application:get_env(?APP_NAME, gui_min_stat_expiration, timer:seconds(5))).
-define(HOUR_STAT_EXPIRATION,
    application:get_env(?APP_NAME, gui_hour_stat_expiration, timer:seconds(10))).
-define(DAY_STAT_EXPIRATION,
    application:get_env(?APP_NAME, gui_day_stat_expiration, timer:seconds(15))).
-define(MONTH_STAT_EXPIRATION,
    application:get_env(?APP_NAME, gui_month_stat_expiration, timer:seconds(20))).

-define(TRIMMED_TIMESTAMP(__Timestamp),
    (__Timestamp - ((__Timestamp rem ?FIVE_SEC_TIME_WINDOW) + 5*?FIVE_SEC_TIME_WINDOW))).

-define(CTX, #{
    model => ?MODULE,
    routing => local,
    disc_driver => undefined
}).


%%%===================================================================
%%% API
%%%===================================================================


%%-------------------------------------------------------------------
%% @doc
%% Saves statistics of given type for given space.
%% @end
%%-------------------------------------------------------------------
-spec save(SpaceId :: od_space:id(), Stats :: space_transfer_cache(),
    StatsType :: binary()) -> ok | {error, term()}.
save(SpaceId, Stats, StatsType) ->
    Key = datastore_utils:gen_key(StatsType, SpaceId),
    case datastore_model:save(?CTX, #document{key = Key, value = Stats}) of
        {ok, _} -> ok;
        Error -> Error
    end.


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfers for given space.
%% @end
%%-------------------------------------------------------------------
-spec get(HistogramType :: binary(), SpaceId :: od_space:id()) ->
    space_transfer_cache() | {error, term()}.
get(SpaceId, HistogramType) ->
    Now = time_utils:system_time_millis(),
    Key = datastore_utils:gen_key(HistogramType, SpaceId),
    Result = case datastore_model:get(?CTX, Key) of
        {ok, #document{value = #space_transfer_cache{expires = Expires} = Stats}} when Now < Expires ->
            Stats;
        {ok, #document{}} ->
            {error, not_found};
        {error, Reason} ->
            {error, Reason}
    end,
    case Result of
        {error, not_found} ->
            StatsTypes = case HistogramType of
                ?MINUTE_STAT_TYPE -> [?MINUTE_STAT_TYPE];
                _ -> [?MINUTE_STAT_TYPE, HistogramType]
            end,
            CurrentStats = get_stats(SpaceId, StatsTypes),
            TrimmedStats = trim_stats(CurrentStats),

            % Filter out histograms with only zeroes and
            % do not store mapping for stats other than minute one
            Pred = fun(_Provider, Histogram) -> lists:sum(Histogram) > 0 end,
            FilteredStats = lists:map(fun({Stats, StatsType}) ->
                NewStats = Stats#space_transfer_cache{
                    stats_in = maps:filter(Pred, Stats#space_transfer_cache.stats_in),
                    stats_out = maps:filter(Pred, Stats#space_transfer_cache.stats_out),
                    mapping = case StatsTypes of
                        ?MINUTE_STAT_TYPE -> Stats#space_transfer_cache.mapping;
                        _ -> undefined
                    end
                },
                {NewStats, StatsType}
            end, TrimmedStats),

            VelocityStats = lists:map(fun({Stats, StatsType}) ->
                histograms_to_speed_charts(Stats, StatsType)
            end, FilteredStats),

            % cache computed velocity stats
            lists:foreach(fun({Stats, StatsType}) ->
                save(SpaceId, Stats, StatsType)
            end, lists:zip(VelocityStats, StatsTypes)),

            % return stats of requested type
            lists:last(VelocityStats);
        _ ->
            Result
    end.


%%-------------------------------------------------------------------
%% @doc
%% Deletes space transfer cache document.
%% @end
%%-------------------------------------------------------------------
-spec delete(SpaceId :: od_space:id(), HistogramType :: binary()) -> ok | {error, term()}.
delete(SpaceId, HistogramType) ->
    Key = datastore_utils:gen_key(HistogramType, SpaceId),
    datastore_model:delete(?CTX, Key).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {expires, integer},
        {timestamp, integer},
        {stats_in, #{string => [integer]}},
        {stats_out, #{string => [integer]}},
        {mapping, #{string => [string]}}
    ]}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Gather summarized transfer statistics for given space and histogram types.
%% @end
%%--------------------------------------------------------------------
-spec get_stats(SpaceId :: od_space:id(), HistogramTypes :: [binary()]) ->
    [{space_transfer_cache(), binary()}].
get_stats(SpaceId, HistogramTypes) ->
    {ok, #document{value = Space}} = od_space:get(SpaceId),
    SpaceTransfers = lists:foldl(fun(Provider, STs) ->
        case space_transfer:get(Provider, SpaceId) of
            {ok, SpaceTransfer} ->
                STs#{Provider => SpaceTransfer};
            {error, not_found} ->
                STs;
            Error ->
                ?error("Failed to retrieve Space Transfer Document
                       for space ~p and provider ~p because of: ~p", [
                    SpaceId, Provider, Error
                ]),
                STs
        end
    end, #{}, maps:keys(Space#od_space.providers)),

    CurrentTime = provider_logic:zone_time_seconds(),
    LocalTime = time_utils:system_time_millis(),
    EmptyStats = [
        {#space_transfer_cache{
            expires = LocalTime + histogram_type_to_expiration_timeout(HistogramType),
            timestamp = CurrentTime
        }, HistogramType} || HistogramType <- HistogramTypes
    ],

    maps:fold(fun(Provider, SpaceTransfer, CurrentStats) ->
        lists:map(fun({Stats, StatsType}) ->
            update_stats(Stats, StatsType, SpaceTransfer, CurrentTime, Provider)
        end, CurrentStats)
    end, EmptyStats, SpaceTransfers).


%%--------------------------------------------------------------------
%% @doc
%% Update statistics for provider based on given space transfer record.
%% @end
%%--------------------------------------------------------------------
-spec update_stats(CurrentStat :: space_transfer_cache(), HistogramType :: binary(),
    SpaceTransfer :: space_transfer:space_transfer(), CurrentTime :: non_neg_integer(),
    Provider :: od_provider:id()) -> space_transfer_cache().
update_stats(CurrentStat, HistogramType, SpaceTransfer, CurrentTime, Provider) ->
    #space_transfer_cache{
        stats_in = StatsIn,
        stats_out = StatsOut,
        mapping = Mapping
    } = CurrentStat,

    LastUpdates = SpaceTransfer#space_transfer.last_update,
    {Histograms, TimeWindow} = case HistogramType of
        ?MINUTE_STAT_TYPE -> {SpaceTransfer#space_transfer.min_hist, ?FIVE_SEC_TIME_WINDOW};
        ?HOUR_STAT_TYPE -> {SpaceTransfer#space_transfer.hr_hist, ?MIN_TIME_WINDOW};
        ?DAY_STAT_TYPE -> {SpaceTransfer#space_transfer.dy_hist, ?HOUR_TIME_WINDOW};
        ?MONTH_STAT_TYPE -> {SpaceTransfer#space_transfer.mth_hist, ?DAY_TIME_WINDOW}
    end,
    ZeroedHistogram = transfer_histogram:new_time_slot_histogram(CurrentTime, TimeWindow),

    {HistogramIn, HistogramsOut, SourceProviders} = maps:fold(fun(SourceProvider, Histogram, Acc) ->
        {OldHistogramIn, OldHistogramsOut, OldSources} = Acc,
        LastUpdate = maps:get(SourceProvider, LastUpdates),
        TimeSlotHist = time_slot_histogram:new(LastUpdate, TimeWindow, Histogram),
        NewHistogramIn = time_slot_histogram:merge(OldHistogramIn, TimeSlotHist),
        NewHistogramsOut = OldHistogramsOut#{SourceProvider => TimeSlotHist},
        NewSources = case CurrentTime - LastUpdate =< ?TRANSFER_INACTIVITY of
            false -> OldSources;
            true -> [SourceProvider | OldSources]
        end,
        {NewHistogramIn, NewHistogramsOut, NewSources}
    end, {ZeroedHistogram, #{}, []}, Histograms),

    NewStatsIn = StatsIn#{Provider => time_slot_histogram:get_histogram_values(HistogramIn)},

    NewStatsOut = maps:fold(fun(SourceProvider, Histogram, OldStatsOut) ->
        OldHistogramOut = case maps:get(SourceProvider, OldStatsOut, none) of
            none -> ZeroedHistogram;
            Hist -> time_slot_histogram:new(CurrentTime, TimeWindow, Hist)
        end,
        NewHistogramOut = time_slot_histogram:merge(OldHistogramOut, Histogram),
        OldStatsOut#{SourceProvider => time_slot_histogram:get_histogram_values(NewHistogramOut)}
    end, StatsOut, HistogramsOut),

    NewMapping = lists:foldl(fun(SourceProvider, OldMapping) ->
        DestinationProviders = maps:get(SourceProvider, OldMapping, []),
        OldMapping#{SourceProvider => [Provider | DestinationProviders]}
    end, Mapping, SourceProviders),

    CurrentStat#space_transfer_cache{
        stats_in = NewStatsIn,
        stats_out = NewStatsOut,
        mapping = NewMapping
    }.


%%--------------------------------------------------------------------
%% @doc
%% Erase recent 30s of histograms to avoid fluctuations on charts.
%% @end
%%--------------------------------------------------------------------
-spec trim_stats([{space_transfer_cache(), binary()}]) ->
    [{space_transfer_cache(), binary()}].
trim_stats([{Stats, ?MINUTE_STAT_TYPE}]) ->
    TrimFun = fun(_Provider, Histogram) ->
        {_, NewHistogram} = lists:split(6, Histogram),
        NewHistogram
    end,
    NewStats = Stats#space_transfer_cache{
        timestamp = ?TRIMMED_TIMESTAMP(Stats#space_transfer_cache.timestamp),
        stats_in = maps:map(TrimFun, Stats#space_transfer_cache.stats_in),
        stats_out = maps:map(TrimFun, Stats#space_transfer_cache.stats_out)
    },
    [{NewStats, ?MINUTE_STAT_TYPE}];

trim_stats([{MinStats, ?MINUTE_STAT_TYPE}, {Stats, StatsType}]) ->
    OldTimestamp = MinStats#space_transfer_cache.timestamp,
    NewTimestamp = ?TRIMMED_TIMESTAMP(OldTimestamp),
    TimeWindow = histogram_type_to_time_window(StatsType),
    TrimFun = fun(Histogram1, [FstSlot, SndSlot | Rest]) ->
        {ErasedSlots, NewHistogram1} = lists:split(6, Histogram1),
        ErasedBytes = lists:sum(ErasedSlots),
        Histogram2 = case ErasedBytes > FstSlot of
             true ->
                 [0, SndSlot - (ErasedBytes - FstSlot) | Rest];
             false ->
                 [FstSlot - ErasedBytes, SndSlot | Rest]
         end,
        NewHistogram2 = case (OldTimestamp div TimeWindow) =:= (NewTimestamp div TimeWindow) of
            true ->
                lists:droplast(Histogram2);
            false ->
                tl(Histogram2)
        end,
        {NewHistogram1, NewHistogram2}
    end,

    [StatsIn, StatsOut] = lists:map(fun({CurrentStats1, CurrentStats2}) ->
        maps:fold(fun(Provider, Hist1, {OldStats1, OldStats2}) ->
            Hist2 = maps:get(Provider, CurrentStats2),
            {NewHist1, NewHist2} = TrimFun(Hist1, Hist2),
            {OldStats1#{Provider => NewHist1}, OldStats2#{Provider => NewHist2}}
        end, {#{}, #{}}, CurrentStats1)
    end, [
        {MinStats#space_transfer_cache.stats_in, Stats#space_transfer_cache.stats_in},
        {MinStats#space_transfer_cache.stats_out, Stats#space_transfer_cache.stats_out}
    ]),

    {NewMinStatsIn, NewStatsIn} = StatsIn,
    {NewMinStatsOut, NewStatsOut} = StatsOut,
    NewMinStats = MinStats#space_transfer_cache{
        timestamp = NewTimestamp,
        stats_in = NewMinStatsIn,
        stats_out = NewMinStatsOut
    },
    NewStats2 = Stats#space_transfer_cache{
        timestamp = NewTimestamp,
        stats_in = NewStatsIn,
        stats_out = NewStatsOut
    },
    [{NewMinStats, ?MINUTE_STAT_TYPE}, {NewStats2, StatsType}].


-spec histograms_to_speed_charts(Stats :: space_transfer_cache(),
    StatsType :: binary()) -> space_transfer_cache().
histograms_to_speed_charts(Stats, StatsType) ->
    Timestamp = Stats#space_transfer_cache.timestamp,
    TimeWindow = histogram_type_to_time_window(StatsType),
    FstSlotDuration = (Timestamp rem TimeWindow) + 1,
    ToChartFun = fun(_, [FstSlot | Rest]) ->
        [FstSlot/FstSlotDuration | [Bytes/TimeWindow || Bytes <- Rest]]
    end,
    Stats#space_transfer_cache{
        stats_in = maps:map(ToChartFun, Stats#space_transfer_cache.stats_in),
        stats_out = maps:map(ToChartFun, Stats#space_transfer_cache.stats_out)
    }.


-spec histogram_type_to_expiration_timeout(binary()) -> non_neg_integer().
histogram_type_to_expiration_timeout(?MINUTE_STAT_TYPE) -> ?MINUTE_STAT_EXPIRATION;
histogram_type_to_expiration_timeout(?HOUR_STAT_TYPE) -> ?HOUR_STAT_EXPIRATION;
histogram_type_to_expiration_timeout(?DAY_STAT_TYPE) -> ?DAY_STAT_EXPIRATION;
histogram_type_to_expiration_timeout(?MONTH_STAT_TYPE) -> ?MONTH_STAT_EXPIRATION.


-spec histogram_type_to_time_window(binary()) -> non_neg_integer().
histogram_type_to_time_window(?MINUTE_STAT_TYPE) -> ?FIVE_SEC_TIME_WINDOW;
histogram_type_to_time_window(?HOUR_STAT_TYPE) -> ?MIN_TIME_WINDOW;
histogram_type_to_time_window(?DAY_STAT_TYPE) -> ?HOUR_TIME_WINDOW;
histogram_type_to_time_window(?MONTH_STAT_TYPE) -> ?DAY_TIME_WINDOW.
