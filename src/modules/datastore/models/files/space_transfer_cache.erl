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
-spec save(SpaceId :: od_space:id(), HistogramType :: binary(),
    Stats :: space_transfer_cache()) -> ok | {error, term()}.
save(SpaceId, HistogramType, Stats) ->
    Key = datastore_utils:gen_key(HistogramType, SpaceId),
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
            GatheredStats = get_stats(SpaceId, StatsTypes),

        %%    TrimmedStats = trim_stats(CurrentStats, 0),
        %%
        %%    lists:map(fun({CurrentStat, HistogramType}) ->
        %%        histograms_to_speed_charts(CurrentStat, HistogramType)
        %%    end, lists:zip(TrimmedStats, HistogramTypes)).

            lists:foreach(fun({Stats, StatsType}) ->
                save(SpaceId, StatsType, Stats)
            end, lists:zip(GatheredStats, StatsTypes)),
            lists:last(GatheredStats);
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
-spec get_stats(SpaceId :: od_space:id(), HistogramTypes :: [binary()]) -> [space_transfer_cache()].
get_stats(SpaceId, HistogramTypes) ->
    {ok, #document{value = Space}} = od_space:get(SpaceId),
    SpaceTransfers = lists:foldl(fun(Provider, STs) ->
        case space_transfer:get(Provider, SpaceId) of
            {ok, SpaceTransfer} ->
                STs#{Provider => SpaceTransfer};
            {error, not_found} ->
                STs;
            Error ->
                ?error("Failef to retrieve Space Transfer Document
                       for space ~p and provider ~p because of: ~p", [
                    SpaceId, Provider, Error
                ]),
                STs
        end
    end, #{}, maps:keys(Space#od_space.providers)),

    CurrentTime = provider_logic:zone_time_seconds(),
    LocalTime = time_utils:system_time_millis(),
    EmptyStats = [
        #space_transfer_cache{
            expires = LocalTime + histogram_type_to_expiration_timeout(HistogramType),
            timestamp = CurrentTime
        } || HistogramType <- HistogramTypes
    ],

    maps:fold(fun(Provider, SpaceTransfer, Stats) ->
        lists:map(fun({Stat, HistType}) ->
            update_stats(Stat, HistType, SpaceTransfer, CurrentTime, Provider)
        end, lists:zip(Stats, HistogramTypes))
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


%%%%--------------------------------------------------------------------
%%%% @doc
%%%% Recalculate bytes histogram into speed charts.
%%%% @end
%%%%--------------------------------------------------------------------
%%trim_min_histogram(MinHistogram) ->
%%    {A, NewHistogram} = lists:split(6, MinHistogram),
%%    {lists:sum(A), NewHistogram}.
%%
%%
%%%%--------------------------------------------------------------------
%%%% @doc
%%%% Recalculate bytes histogram into speed charts.
%%%% @end
%%%%--------------------------------------------------------------------
%%trim_histograms(MinHistogram, Other) ->
%%    {A, NewMinHistogram} = trim_min_histogram(MinHistogram),
%%
%%
%%    {} = lists:split(6, MinHistogram),
%%
%%    #space_transfer_cache{
%%        stats_in = StatsIn,
%%        stats_out = StatsOut,
%%    } = MinuteStats,
%%
%%    TrimmedStatsIn = maps:map(fun(_SourceProvider, Histogram) ->
%%        ok
%%    end, StatsIn),
%%    ok.
%%
%%
%%%%--------------------------------------------------------------------
%%%% @doc
%%%% Recalculate bytes histogram into speed charts.
%%%% @end
%%%%--------------------------------------------------------------------
%%trim_min_stats(MinuteStats) ->
%%    #space_transfer_cache{timestamp = Timestamp, stats_in = StatsIn, stats_out = StatsOut
%%    } = MinuteStats,
%%
%%    NewTimestamp = (Timestamp rem ?FIVE_SEC_TIME_WINDOW) + 5*?FIVE_SEC_TIME_WINDOW,
%%
%%    TrimFun = fun(_Provider, Histogram) -> trim_min_histogram(Histogram) end,
%%
%%    MinuteStats#space_transfer_cache{
%%        timestamp = NewTimestamp,
%%        stats_in = maps:map(TrimFun, StatsIn),
%%        stats_out = maps:map(TrimFun, StatsOut)
%%    }.
%%
%%
%%%%--------------------------------------------------------------------
%%%% @doc
%%%% Recalculate bytes histogram into speed charts.
%%%% @end
%%%%--------------------------------------------------------------------
%%trim_stats(MinuteStats, OtherStats) ->
%%    #space_transfer_cache{
%%        timestamp = Timestamp,
%%        stats_in = MinStatsIn,
%%        stats_out = MinStatsOut
%%    } = MinuteStats,
%%
%%    #space_transfer_cache{
%%        stats_in = OtherStatsIn,
%%        stats_out = OtherStatsOut
%%    } = OtherStats,
%%
%%    NewTimestamp = (Timestamp rem ?FIVE_SEC_TIME_WINDOW) + 5*?FIVE_SEC_TIME_WINDOW,
%%
%%    {NewMinStatsIn, NewOtherStatsIn} = maps:fold(fun(Provider, Histogram, {MinStatsIn, OtherStatsIn1}) ->
%%        OtherHistIn = maps:get(Provider, OtherStatsIn),
%%        {NewMinHistIn, NewOtherHistIn} = trim_histograms(Histogram, OtherHistIn),
%%        {MinStatsIn#{Provider => NewMinHistIn}, OtherStatsIn1#{Provider => NewOtherHistIn}}
%%    end, {#{}, #{}}, MinStatsIn),
%%
%%    NewMinStats = MinuteStats#space_transfer_cache{
%%        timestamp = NewTimestamp,
%%        stats_in = NewMinStatsIn,
%%        stats_out = NewMinStatsOut
%%    },
%%    NewOtherStats = OtherStats#space_transfer_cache{
%%        timestamp = NewTimestamp,
%%        stats_in = NewOtherStatsIn,
%%        stats_out = NewOtherStatsOut
%%    },
%%    {NewMinStats, NewOtherStats}.
%%
%%
%%%%--------------------------------------------------------------------
%%%% @doc
%%%% Recalculate bytes histogram into speed charts.
%%%% @end
%%%%--------------------------------------------------------------------
%%-spec histograms_to_speed_charts(CurrentStat :: space_transfer_cache(),
%%    HistogramType :: binary()) -> space_transfer_cache().
%%histograms_to_speed_charts(CurrentStat, HistogramType) ->
%%    #space_transfer_cache{
%%        stats_in = StatsIn,
%%        stats_out = StatsOut
%%    } = CurrentStat,
%%
%%    TimeWindow = case HistogramType of
%%        ?MINUTE_STAT_TYPE -> ?FIVE_SEC_TIME_WINDOW;
%%        ?HOUR_STAT_TYPE -> ?MIN_TIME_WINDOW;
%%        ?DAY_STAT_TYPE -> ?HOUR_TIME_WINDOW;
%%        ?MONTH_STAT_TYPE -> ?DAY_TIME_WINDOW
%%    end,
%%
%%    SpeedChart = fun(_, Histogram) -> [Bytes/TimeWindow || Bytes <- Histogram] end,
%%
%%    #space_transfer_cache{
%%        stats_in = maps:map(SpeedChart, StatsIn),
%%        stats_out = maps:map(SpeedChart, StatsOut)
%%    }.


-spec histogram_type_to_expiration_timeout(binary()) -> non_neg_integer().
histogram_type_to_expiration_timeout(?MINUTE_STAT_TYPE) -> ?MINUTE_STAT_EXPIRATION;
histogram_type_to_expiration_timeout(?HOUR_STAT_TYPE) -> ?HOUR_STAT_EXPIRATION;
histogram_type_to_expiration_timeout(?DAY_STAT_TYPE) -> ?DAY_STAT_EXPIRATION;
histogram_type_to_expiration_timeout(?MONTH_STAT_TYPE) -> ?MONTH_STAT_EXPIRATION.
