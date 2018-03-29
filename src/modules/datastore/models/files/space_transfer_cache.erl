%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing statistics about all transfers in given space.
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

-type timestamp() :: non_neg_integer().
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
%% Saves transfer statistics of given type for given space.
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
%% Returns space transfer statistics of requested type for given space.
%% @end
%%-------------------------------------------------------------------
-spec get(SpaceId :: od_space:id(), StatsType :: binary()) ->
    space_transfer_cache() | {error, term()}.
get(SpaceId, RequestedStatsType) ->
    Now = time_utils:system_time_millis(),
    Key = datastore_utils:gen_key(RequestedStatsType, SpaceId),
    Fetched = case datastore_model:get(?CTX, Key) of
        {ok, #document{value = Stats}} ->
            case Now < Stats#space_transfer_cache.expires of
                true -> Stats;
                false -> {error, not_found}
            end;
        Error ->
            Error
    end,
    case Fetched of
        {error, not_found} ->
            % In case of stats other than minute one, we need to calculate also
            % minutes to be able to trim recent n-seconds
            StatsTypesToCalc = case RequestedStatsType of
                ?MINUTE_STAT_TYPE -> [?MINUTE_STAT_TYPE];
                _ -> [?MINUTE_STAT_TYPE, RequestedStatsType]
            end,
            CalculatedStats = get_stats(SpaceId, StatsTypesToCalc),
            TrimmedStats = trim_stats(CalculatedStats),

            % Filter out from stats histograms with only zeroes, also
            % do not store mapping for stats other than minute one
            Pred = fun(_Provider, Histogram) -> lists:sum(Histogram) > 0 end,
            FilteredStats = lists:map(fun({Stats, StatsType}) ->
                NewStats = Stats#space_transfer_cache{
                    stats_in = maps:filter(Pred, Stats#space_transfer_cache.stats_in),
                    stats_out = maps:filter(Pred, Stats#space_transfer_cache.stats_out),
                    mapping = case StatsType of
                        ?MINUTE_STAT_TYPE -> Stats#space_transfer_cache.mapping;
                        _ -> undefined
                    end
                },
                {NewStats, StatsType}
            end, TrimmedStats),

            SpeedStats = lists:map(fun({Stats, StatsType}) ->
                {histograms_to_speed_charts(Stats, StatsType), StatsType}
            end, FilteredStats),

            % cache computed velocity stats
            lists:foreach(fun({Stats, StatsType}) ->
                save(SpaceId, Stats, StatsType)
            end, SpeedStats),

            {RequestedStats, RequestedStatsType} = lists:last(SpeedStats),
            RequestedStats;
        _ ->
            Fetched
    end.


%%-------------------------------------------------------------------
%% @doc
%% Deletes space transfer statistics of given type for given space.
%% @end
%%-------------------------------------------------------------------
-spec delete(SpaceId :: od_space:id(), StatsType :: binary()) ->
    ok | {error, term()}.
delete(SpaceId, StatsType) ->
    Key = datastore_utils:gen_key(StatsType, SpaceId),
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
%% @private
%% @doc
%% Gather transfer statistics of requested types for given space.
%% @end
%%--------------------------------------------------------------------
-spec get_stats(SpaceId :: od_space:id(), HistogramTypes :: [binary()]) ->
    [{space_transfer_cache(), binary()}].
get_stats(SpaceId, RequestedStatsTypes) ->
    {ok, #document{value = Space}} = od_space:get(SpaceId),
    SpaceTransfers = lists:foldl(fun(Provider, STs) ->
        case space_transfer:get(Provider, SpaceId) of
            {ok, SpaceTransfer} ->
                STs#{Provider => SpaceTransfer};
            {error, not_found} ->
                STs;
            Error ->
                ?error("Failed to retrieve Space Transfer Document
                       for space ~p and provider ~p due to: ~p", [
                    SpaceId, Provider, Error
                ]),
                STs
        end
    end, #{}, maps:keys(Space#od_space.providers)),

    LocalTime = time_utils:system_time_millis(),
    CurrentTime = provider_logic:zone_time_seconds(),
    EmptyStats = [
        {#space_transfer_cache{
            expires = LocalTime + stats_type_to_expiration_timeout(StatsType),
            timestamp = CurrentTime
        }, StatsType} || StatsType <- RequestedStatsTypes
    ],

    maps:fold(fun(Provider, SpaceTransfer, CurrentStats) ->
        lists:map(fun({Stats, StatsType}) ->
            NewStats = update_stats(
                Stats, StatsType, SpaceTransfer, CurrentTime, Provider
            ),
            {NewStats, StatsType}
        end, CurrentStats)
    end, EmptyStats, SpaceTransfers).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Update given statistics for provider based on given space transfer record.
%% @end
%%--------------------------------------------------------------------
-spec update_stats(OldStats :: space_transfer_cache(), StatsType :: binary(),
    ST :: space_transfer:space_transfer(), CurrentTime :: timestamp(),
    Provider :: od_provider:id()) -> space_transfer_cache().
update_stats(OldStats, StatsType, ST, CurrentTime, Provider) ->
    #space_transfer_cache{
        stats_in = StatsIn,
        stats_out = StatsOut,
        mapping = Mapping
    } = OldStats,

    LastUpdates = ST#space_transfer.last_update,
    {Histograms, TimeWindow} = case StatsType of
        ?MINUTE_STAT_TYPE -> {ST#space_transfer.min_hist, ?FIVE_SEC_TIME_WINDOW};
        ?HOUR_STAT_TYPE -> {ST#space_transfer.hr_hist, ?MIN_TIME_WINDOW};
        ?DAY_STAT_TYPE -> {ST#space_transfer.dy_hist, ?HOUR_TIME_WINDOW};
        ?MONTH_STAT_TYPE -> {ST#space_transfer.mth_hist, ?DAY_TIME_WINDOW}
    end,
    ZeroedHist = transfer_histogram:new_time_slot_histogram(CurrentTime, TimeWindow),

    {HistIn, HistsOut, SrcProviders} = maps:fold(fun(SrcProvider, Hist, Acc) ->
        {OldHistIn, OldHistsOut, OldSrcProviders} = Acc,
        LastUpdate = maps:get(SrcProvider, LastUpdates),
        TimeSlotHist = time_slot_histogram:new(LastUpdate, TimeWindow, Hist),
        NewHistIn = time_slot_histogram:merge(OldHistIn, TimeSlotHist),
        NewHistsOut = OldHistsOut#{SrcProvider => TimeSlotHist},
        NewSrcProviders = case CurrentTime - LastUpdate =< ?TRANSFER_INACTIVITY of
            false -> OldSrcProviders;
            true -> [SrcProvider | OldSrcProviders]
        end,
        {NewHistIn, NewHistsOut, NewSrcProviders}
    end, {ZeroedHist, #{}, []}, Histograms),

    NewStatsIn = StatsIn#{
        Provider => time_slot_histogram:get_histogram_values(HistIn)
    },
    NewStatsOut = maps:fold(fun(SrcProvider, Hist, OldStatsOut) ->
        OldHistOut = case maps:get(SrcProvider, OldStatsOut, none) of
            none -> ZeroedHist;
            HistOut -> time_slot_histogram:new(CurrentTime, TimeWindow, HistOut)
        end,
        NewHistOut = time_slot_histogram:merge(OldHistOut, Hist),
        OldStatsOut#{
            SrcProvider => time_slot_histogram:get_histogram_values(NewHistOut)
        }
    end, StatsOut, HistsOut),

    NewMapping = lists:foldl(fun(SrcProvider, OldMapping) ->
        DestinationProviders = maps:get(SrcProvider, OldMapping, []),
        OldMapping#{SrcProvider => [Provider | DestinationProviders]}
    end, Mapping, SrcProviders),

    OldStats#space_transfer_cache{
        stats_in = NewStatsIn,
        stats_out = NewStatsOut,
        mapping = NewMapping
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Erase recent n-seconds of histograms based on difference between expected
%% slots in speed histograms and bytes histograms.
%% It helps to avoid fluctuations on charts due to synchronization
%% between providers.
%% @end
%%--------------------------------------------------------------------
-spec trim_stats([{space_transfer_cache(), binary()}]) ->
    [{space_transfer_cache(), binary()}].
trim_stats([{Stats, ?MINUTE_STAT_TYPE}]) ->
    SlotsToRemove = ?MIN_HIST_LENGTH - ?MIN_SPEED_HIST_LENGTH,
    TrimFun = fun(_Provider, Histogram) ->
        {_, NewHistogram} = lists:split(SlotsToRemove, Histogram),
        NewHistogram
    end,
    NewStats = Stats#space_transfer_cache{
        timestamp = transfer_histogram:trim_timestamp(Stats#space_transfer_cache.timestamp),
        stats_in = maps:map(TrimFun, Stats#space_transfer_cache.stats_in),
        stats_out = maps:map(TrimFun, Stats#space_transfer_cache.stats_out)
    },
    [{NewStats, ?MINUTE_STAT_TYPE}];

trim_stats([{MinStats, ?MINUTE_STAT_TYPE}, {RequestedStats, RequestedStatsType}]) ->
    OldTimestamp = MinStats#space_transfer_cache.timestamp,
    NewTimestamp = transfer_histogram:trim_timestamp(OldTimestamp),
    TimeWindow = transfer_histogram:stats_type_to_time_window(RequestedStatsType),
    TrimFun = fun(OldHist1, [FstSlot, SndSlot | Rest]) ->
        MinSlotsToRemove = ?MIN_HIST_LENGTH - ?MIN_SPEED_HIST_LENGTH,
        {RemovedSlots, NewHist1} = lists:split(MinSlotsToRemove, OldHist1),
        RemovedBytes = lists:sum(RemovedSlots),
        OldHist2 = case RemovedBytes > FstSlot of
            true -> [0, SndSlot - (RemovedBytes - FstSlot) | Rest];
            false -> [FstSlot - RemovedBytes, SndSlot | Rest]
        end,
        Hist2 = case (OldTimestamp div TimeWindow) == (NewTimestamp div TimeWindow) of
            true -> lists:droplast(OldHist2);
            false -> tl(OldHist2)
        end,
        Len = transfer_histogram:stats_type_to_speed_chart_len(RequestedStatsType),
        NewHist2 = lists:sublist(Hist2, Len),
        {NewHist1, NewHist2}
    end,

    [StatsIn, StatsOut] = lists:map(fun({CurrentStats1, CurrentStats2}) ->
        maps:fold(fun(Provider, Hist1, {OldStats1, OldStats2}) ->
            Hist2 = maps:get(Provider, CurrentStats2),
            {NewHist1, NewHist2} = TrimFun(Hist1, Hist2),
            {OldStats1#{Provider => NewHist1}, OldStats2#{Provider => NewHist2}}
        end, {#{}, #{}}, CurrentStats1)
    end, [
        {MinStats#space_transfer_cache.stats_in,
            RequestedStats#space_transfer_cache.stats_in},
        {MinStats#space_transfer_cache.stats_out,
            RequestedStats#space_transfer_cache.stats_out}
    ]),
    {NewMinStatsIn, NewRequestedStatsIn} = StatsIn,
    {NewMinStatsOut, NewRequestedStatsOut} = StatsOut,

    NewMinStats = MinStats#space_transfer_cache{
        timestamp = NewTimestamp,
        stats_in = NewMinStatsIn,
        stats_out = NewMinStatsOut
    },
    NewRequestedStats = RequestedStats#space_transfer_cache{
        timestamp = NewTimestamp,
        stats_in = NewRequestedStatsIn,
        stats_out = NewRequestedStatsOut
    },
    [{NewMinStats, ?MINUTE_STAT_TYPE}, {NewRequestedStats, RequestedStatsType}].


-spec histograms_to_speed_charts(Stats :: space_transfer_cache(),
    StatsType :: binary()) -> space_transfer_cache().
histograms_to_speed_charts(Stats, StatsType) ->
    Timestamp = Stats#space_transfer_cache.timestamp,
    TimeWindow = transfer_histogram:stats_type_to_time_window(StatsType),
    FstSlotDuration = case Timestamp rem TimeWindow of
        0 -> TimeWindow;
        Rem -> Rem + 1
    end,
    ToSpeedChart = fun(_, [FstSlot | Rest]) ->
        [FstSlot/FstSlotDuration | [Bytes/TimeWindow || Bytes <- Rest]]
    end,
    Stats#space_transfer_cache{
        stats_in = maps:map(ToSpeedChart, Stats#space_transfer_cache.stats_in),
        stats_out = maps:map(ToSpeedChart, Stats#space_transfer_cache.stats_out)
    }.


-spec stats_type_to_expiration_timeout(binary()) -> non_neg_integer().
stats_type_to_expiration_timeout(?MINUTE_STAT_TYPE) -> ?MINUTE_STAT_EXPIRATION;
stats_type_to_expiration_timeout(?HOUR_STAT_TYPE) -> ?HOUR_STAT_EXPIRATION;
stats_type_to_expiration_timeout(?DAY_STAT_TYPE) -> ?DAY_STAT_EXPIRATION;
stats_type_to_expiration_timeout(?MONTH_STAT_TYPE) -> ?MONTH_STAT_EXPIRATION.
