%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing aggregated statistics about all transfers in given space.
%%% @end
%%%-------------------------------------------------------------------
-module(space_transfer_stats_cache).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    save/5,
    get/4, get_active_channels/1,
    update/5,
    delete/4
]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type transfer_stats() :: #{od_provider:id() => #space_transfer_stats{}}.
-type space_transfer_stats_cache() :: #space_transfer_stats_cache{}.
-type doc() :: datastore_doc:doc(space_transfer_stats_cache()).

-export_type([space_transfer_stats_cache/0, doc/0]).

-define(TRANSFER_INACTIVITY, application:get_env(
    ?APP_NAME, gui_transfer_inactivity_threshold, 20)
).
-define(MINUTE_STAT_EXPIRATION, application:get_env(
    ?APP_NAME, gui_transfer_min_stat_expiration, timer:seconds(5))
).
-define(HOUR_STAT_EXPIRATION, application:get_env(
    ?APP_NAME, gui_transfer_hour_stat_expiration, timer:seconds(10))
).
-define(DAY_STAT_EXPIRATION, application:get_env(
    ?APP_NAME, gui_transfer_day_stat_expiration, timer:minutes(1))
).
-define(MONTH_STAT_EXPIRATION, application:get_env(
    ?APP_NAME, gui_transfer_month_stat_expiration, timer:minutes(5))
).

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
%% Saves transfer statistics of given type for specified target provider, space
%% and transfer type.
%% When storing aggregated statistics for all providers in space,
%% TargetProvider should be given as 'undefined'.
%% @end
%%-------------------------------------------------------------------
-spec save(TargetProvider :: od_provider:id() | undefined,
    SpaceId :: od_space:id(), TransferType :: binary(), StatsType :: binary(),
    Stats :: space_transfer_stats_cache()
) ->
    ok | {error, term()}.
save(TargetProvider, SpaceId, TransferType, StatsType, Stats) ->
    Key = key(TargetProvider, SpaceId, TransferType, StatsType),
    ?extract_ok(datastore_model:save(?CTX, #document{key = Key, value = Stats})).


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfer statistics of requested type for specified
%% target provider, space and transfer type.
%% When retrieving aggregated statistics for all providers in space,
%% TargetProvider should be given as 'undefined'.
%% @end
%%-------------------------------------------------------------------
-spec get(TargetProvider :: od_provider:id() | undefined,
    SpaceId :: od_space:id(), TransferType :: binary(), StatsType :: binary()
) ->
    space_transfer_stats_cache() | {error, term()}.
get(TargetProvider, SpaceId, TransferType, StatsType) ->
    Key = key(TargetProvider, SpaceId, TransferType, StatsType),
    ResultFromCache = case datastore_model:get(?CTX, Key) of
        {ok, #document{value = Stats}} ->
            case countdown_timer:is_expired(Stats#space_transfer_stats_cache.expiration_timer) of
                false -> Stats;
                true -> {error, not_found}
            end;
        {error, _} = Error ->
            Error
    end,
    case ResultFromCache of
        {error, not_found} ->
            {TransferStatsPerType, LatestUpdate} = get_transfer_stats(TransferType, SpaceId),
            CurrentMonotonicTime = transfer_histograms:get_current_monotonic_time(LatestUpdate),
            prepare_aggregated_stats(TargetProvider, SpaceId,
                TransferType, StatsType, TransferStatsPerType, CurrentMonotonicTime
            );
        Other ->
            Other
    end.


%%-------------------------------------------------------------------
%% @doc
%% Returns active channels for given space (providers mapping to providers
%% they recently sent data to). For efficiency reasons they are stored only
%% with minute stats.
%% @end
%%-------------------------------------------------------------------
-spec get_active_channels(SpaceId :: od_space:id()) ->
    {ok, #{od_provider:id() => [od_provider:id()]}} | {error, term()}.
get_active_channels(SpaceId) ->
    case get(undefined, SpaceId, ?JOB_TRANSFERS_TYPE, ?MINUTE_PERIOD) of
        #space_transfer_stats_cache{active_channels = ActiveChannels} ->
            {ok, ActiveChannels};
        {error, _} = Error ->
            Error
    end.


%%-------------------------------------------------------------------
%% @doc
%% Update transfer statistics of given type for specified target provider,
%% space and transfer type. If previously cached stats has not expired yet
%% do not overwrite it. It helps prevent situation when the same stats
%% are cached 2 times longer than they should (e.g. when counting minute stats,
%% entire time slots making up to recent n-seconds are trimmed meaning that
%% stats counted for timestamps: 61 and 64 are the same).
%% When updating aggregated statistics for all providers in space,
%% TargetProvider should be given as 'undefined'.
%% @end
%%-------------------------------------------------------------------
-spec update(TargetProvider :: od_provider:id() | undefined,
    SpaceId :: od_space:id(), TransferType :: binary(), StatsType :: binary(),
    Stats :: space_transfer_stats_cache()
) ->
    ok | {error, term()}.
update(TargetProvider, SpaceId, TransferType, StatsType, Stats) ->
    Key = key(TargetProvider, SpaceId, TransferType, StatsType),
    Diff = fun(OldStats) ->
        NewStats = case countdown_timer:is_expired(OldStats#space_transfer_stats_cache.expiration_timer) of
            false -> OldStats;
            true -> Stats
        end,
        {ok, NewStats}
    end,
    ?extract_ok(datastore_model:update(?CTX, Key, Diff, Stats)).


%%-------------------------------------------------------------------
%% @doc
%% Deletes space transfer statistics of given type for given space and
%% transfer type.
%% When deleting aggregated statistics for all providers in space,
%% TargetProvider should be given as 'undefined'.
%% @end
%%-------------------------------------------------------------------
-spec delete(TargetProvider :: od_provider:id() | undefined,
    SpaceId :: od_space:id(), TransferType :: binary(), StatsType :: binary()
) ->
    ok | {error, term()}.
delete(TargetProvider, SpaceId, TransferType, StatsType) ->
    Key = key(TargetProvider, SpaceId, TransferType, StatsType),
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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gather statistics of requested types from given transfer statistics
%% for specified target provider, space and transfer type.
%% Pad them with zeroes to current time and erase recent n-seconds to avoid
%% fluctuations on charts. To do that for type other than minute one,
%% it is required to calculate also minute stats (otherwise it is not possible
%% to trim histograms of other types).
%% @end
%%--------------------------------------------------------------------
-spec prepare_aggregated_stats(TargetProvider :: od_provider:id() | undefined,
    SpaceId :: od_space:id(), TransferType :: binary(), StatsType :: binary(),
    TransferStatsMap :: #{binary() => transfer_stats()},
    CurrentMonotonicTime :: transfer_histograms:monotonic_timestamp()
) ->
    space_transfer_stats_cache().
prepare_aggregated_stats(TargetProvider, SpaceId, ?ALL_TRANSFERS_TYPE,
    StatsType, TransferStatsPerType, CurrentMonotonicTime
) ->
    JobStats = prepare_aggregated_stats(TargetProvider, SpaceId,
        ?JOB_TRANSFERS_TYPE, StatsType, TransferStatsPerType, CurrentMonotonicTime
    ),
    OnTheFlyStats = prepare_aggregated_stats(TargetProvider, SpaceId,
        ?ON_THE_FLY_TRANSFERS_TYPE, StatsType, TransferStatsPerType, CurrentMonotonicTime
    ),
    AllStats = merge_stats(JobStats, OnTheFlyStats),
    update(TargetProvider, SpaceId, ?ALL_TRANSFERS_TYPE, StatsType, AllStats),
    AllStats;

prepare_aggregated_stats(TargetProvider, SpaceId, TransferType,
    ?MINUTE_PERIOD, TransferStatsPerType, CurrentMonotonicTime
) ->
    TransferStats = maps:get(TransferType, TransferStatsPerType),
    [MinStats] = aggregate_stats(
        TargetProvider, TransferStats, [?MINUTE_PERIOD], CurrentMonotonicTime
    ),

    #space_transfer_stats_cache{
        last_update = LastUpdate,
        stats_in = StatsIn,
        stats_out = StatsOut
    } = MinStats,

    {TrimmedStatsIn, TrimmedLastUpdate} =
        transfer_histograms:trim_min_histograms(StatsIn, LastUpdate),
    {TrimmedStatsOut, TrimmedLastUpdate} =
        transfer_histograms:trim_min_histograms(StatsOut, LastUpdate),

    Pred = fun(_Provider, Histogram) -> lists:sum(Histogram) > 0 end,
    NewMinStats = MinStats#space_transfer_stats_cache{
        last_update = TrimmedLastUpdate,
        stats_in = maps:filter(Pred, TrimmedStatsIn),
        stats_out = maps:filter(Pred, TrimmedStatsOut)
    },

    update(TargetProvider, SpaceId, TransferType, ?MINUTE_PERIOD, NewMinStats),
    NewMinStats;

prepare_aggregated_stats(TargetProvider, SpaceId, TransferType,
    RequestedStatsType, TransferStatsMap, CurrentMonotonicTime
) ->
    TransferStats = maps:get(TransferType, TransferStatsMap),
    [MinStats, RequestedStats] = aggregate_stats(TargetProvider,
        TransferStats, [?MINUTE_PERIOD, RequestedStatsType], CurrentMonotonicTime
    ),

    #space_transfer_stats_cache{
        last_update = LastUpdate,
        stats_in = MinStatsIn,
        stats_out = MinStatsOut
    } = MinStats,
    #space_transfer_stats_cache{
        last_update = LastUpdate,
        stats_in = StatsIn,
        stats_out = StatsOut
    } = RequestedStats,

    TimeWindow = transfer_histograms:period_to_time_window(RequestedStatsType),
    {NewMinStatsIn, NewStatsIn, TrimmedLastUpdate} =
        transfer_histograms:trim_histograms(
            MinStatsIn, StatsIn, TimeWindow, LastUpdate),
    {NewMinStatsOut, NewStatsOut, TrimmedLastUpdate} =
        transfer_histograms:trim_histograms(
            MinStatsOut, StatsOut, TimeWindow, LastUpdate),

    Pred = fun(_Provider, Histogram) -> lists:sum(Histogram) > 0 end,
    NewMinStats = MinStats#space_transfer_stats_cache{
        last_update = TrimmedLastUpdate,
        stats_in = maps:filter(Pred, NewMinStatsIn),
        stats_out = maps:filter(Pred, NewMinStatsOut)
    },
    NewRequestedStats = RequestedStats#space_transfer_stats_cache{
        last_update = TrimmedLastUpdate,
        stats_in = maps:filter(Pred, NewStatsIn),
        stats_out = maps:filter(Pred, NewStatsOut),
        active_channels = undefined
    },

    update(TargetProvider, SpaceId, TransferType, ?MINUTE_PERIOD, NewMinStats),
    update(TargetProvider, SpaceId, TransferType, RequestedStatsType, NewRequestedStats),
    NewRequestedStats.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Aggregate statistics of specified type from given transfer statistics
%% for specified target provider.
%% When aggregating statistics for all providers in space,
%% TargetProvider should be given as 'undefined'.
%% @end
%%--------------------------------------------------------------------
-spec aggregate_stats(TargetProvider :: od_provider:id() | undefined, transfer_stats(),
    HistogramTypes :: [binary()], CurrentMonotonicTime :: transfer_histograms:monotonic_timestamp()
) ->
    [space_transfer_stats_cache()].
aggregate_stats(TargetProvider, TransferStats, RequestedStatsTypes, CurrentMonotonicTime) ->
    EmptyStats = [
        #space_transfer_stats_cache{
            expiration_timer = countdown_timer:start_millis(stats_type_to_expiration_timeout(StatsType)),
            last_update = transfer_histograms:monotonic_timestamp_value(CurrentMonotonicTime)
        } || StatsType <- RequestedStatsTypes
    ],

    maps:fold(fun(Provider, TransferStat, AggregatedStats) ->
        lists:map(fun({Stats, StatsType}) ->
            update_stats(TargetProvider, Stats, StatsType,
                Provider, TransferStat, CurrentMonotonicTime)
        end, lists:zip(AggregatedStats, RequestedStatsTypes))
    end, EmptyStats, TransferStats).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Update given aggregated statistics of specified type and for specified
%% TargetProvider (undefined if aggregating for all providers in space) with
%% stats of given provider.
%% @end
%%--------------------------------------------------------------------
-spec update_stats(TargetProvider :: od_provider:id() | undefined,
    OldStats :: space_transfer_stats_cache(), StatsType :: binary(),
    Provider :: od_provider:id(), TransferStats :: #space_transfer_stats{},
    CurrentMonotonicTime :: transfer_histograms:monotonic_timestamp()
) ->
    space_transfer_stats_cache().
update_stats(undefined, OldStats, StatsType,
    Provider, TransferStats, CurrentMonotonicTime
) ->
    #space_transfer_stats_cache{
        stats_in = StatsIn,
        stats_out = StatsOut,
        active_channels = ActiveChannels
    } = OldStats,

    Histograms = get_histograms(StatsType, TransferStats),
    LastUpdates = TransferStats#space_transfer_stats.last_update,
    TimeWindow = transfer_histograms:period_to_time_window(StatsType),
    ZeroedHist = histogram:new(transfer_histograms:period_to_hist_length(StatsType)),

    {HistIn, NewStatsOut, NewActiveChannels} = maps:fold(fun(SrcProvider, Hist, Acc) ->
        {OldHistIn, OldStatsOut, OldActiveChannels} = Acc,
        LastUpdate = maps:get(SrcProvider, LastUpdates),
        CurrentTime = transfer_histograms:monotonic_timestamp_value(CurrentMonotonicTime),
        NewHistIn = merge_histograms(
            OldHistIn, CurrentTime, Hist, LastUpdate, TimeWindow
        ),

        OldHistOut = maps:get(SrcProvider, OldStatsOut, ZeroedHist),
        NewHistOut = merge_histograms(
            OldHistOut, CurrentTime, Hist, LastUpdate, TimeWindow
        ),

        NewChannels = case CurrentTime - LastUpdate =< ?TRANSFER_INACTIVITY of
            false ->
                OldActiveChannels;
            true ->
                DestinationProviders = maps:get(SrcProvider, OldActiveChannels, []),
                OldActiveChannels#{SrcProvider => [Provider | DestinationProviders]}
        end,
        {NewHistIn, OldStatsOut#{SrcProvider => NewHistOut}, NewChannels}
    end, {ZeroedHist, StatsOut, ActiveChannels}, Histograms),

    OldStats#space_transfer_stats_cache{
        stats_in = StatsIn#{Provider => HistIn},
        stats_out = NewStatsOut,
        active_channels = NewActiveChannels
    };

update_stats(TargetProvider, OldStats, StatsType,
    TargetProvider, TransferStats, CurrentMonotonicTime
) ->
    Histograms = get_histograms(StatsType, TransferStats),
    LastUpdates = TransferStats#space_transfer_stats.last_update,
    Window = transfer_histograms:period_to_time_window(StatsType),
    OldStats#space_transfer_stats_cache{
        stats_in = transfer_histograms:pad_with_zeroes(
            Histograms, Window, LastUpdates, CurrentMonotonicTime)
    };

update_stats(TargetProvider, OldStats, StatsType,
    Provider, TransferStats, CurrentMonotonicTime
) ->
    Histograms = get_histograms(StatsType, TransferStats),
    case maps:find(TargetProvider, Histograms) of
        {ok, Histogram} ->
            LastUpdate = maps:get(TargetProvider,
                TransferStats#space_transfer_stats.last_update),
            Window = transfer_histograms:period_to_time_window(StatsType),
            ShiftSize = transfer_histograms:calc_shift_size(Window, LastUpdate, CurrentMonotonicTime),
            PaddedHistogram = histogram:shift(Histogram, ShiftSize),

            StatsOut = OldStats#space_transfer_stats_cache.stats_out,
            OldStats#space_transfer_stats_cache{
                stats_out = StatsOut#{Provider => PaddedHistogram}
            };
        error ->
            OldStats
    end.


-spec merge_stats(space_transfer_stats_cache(), space_transfer_stats_cache()) ->
    space_transfer_stats_cache().
merge_stats(Stats1, Stats2) ->
    #space_transfer_stats_cache{
        last_update = LastUpdate,
        stats_in = StatsIn1,
        stats_out = StatsOut1
    } = Stats1,
    #space_transfer_stats_cache{
        expiration_timer = ExpirationTimer,
        last_update = LastUpdate,
        stats_in = StatsIn2,
        stats_out = StatsOut2
    } = Stats2,

    MergeFun = fun(ProviderId, Hist1, Stats) ->
        case maps:find(ProviderId, Stats) of
            {ok, Hist2} -> Stats#{ProviderId => histogram:merge(Hist1, Hist2)};
            error -> Stats#{ProviderId => Hist1}
        end
    end,

    #space_transfer_stats_cache{
        expiration_timer = ExpirationTimer,
        last_update = LastUpdate,
        stats_in = maps:fold(MergeFun, StatsIn1, StatsIn2),
        stats_out = maps:fold(MergeFun, StatsOut1, StatsOut2),
        active_channels = undefined
    }.


-spec stats_type_to_expiration_timeout(binary()) -> time:millis().
stats_type_to_expiration_timeout(?MINUTE_PERIOD) -> ?MINUTE_STAT_EXPIRATION;
stats_type_to_expiration_timeout(?HOUR_PERIOD)   -> ?HOUR_STAT_EXPIRATION;
stats_type_to_expiration_timeout(?DAY_PERIOD)    -> ?DAY_STAT_EXPIRATION;
stats_type_to_expiration_timeout(?MONTH_PERIOD)  -> ?MONTH_STAT_EXPIRATION.


-spec get_histograms(Period :: binary(), #space_transfer_stats{}) ->
    transfer_histograms:histograms().
get_histograms(?MINUTE_PERIOD, #space_transfer_stats{min_hist = Histograms}) ->
    Histograms;
get_histograms(?HOUR_PERIOD, #space_transfer_stats{hr_hist = Histograms}) ->
    Histograms;
get_histograms(?DAY_PERIOD, #space_transfer_stats{dy_hist = Histograms}) ->
    Histograms;
get_histograms(?MONTH_PERIOD, #space_transfer_stats{mth_hist = Histograms}) ->
    Histograms.


-spec merge_histograms(histogram:histogram(), transfer_histograms:timestamp(),
    histogram:histogram(), transfer_histograms:timestamp(), transfer_histograms:window()) ->
    histogram:histogram().
merge_histograms(Hist1, LastUpdate1, Hist2, LastUpdate2, TimeWindow) ->
    case LastUpdate1 > LastUpdate2 of
        true ->
            ShiftSize = (LastUpdate1 div TimeWindow) - (LastUpdate2 div TimeWindow),
            histogram:merge(Hist1, histogram:shift(Hist2, ShiftSize));
        false ->
            ShiftSize = (LastUpdate2 div TimeWindow) - (LastUpdate1 div TimeWindow),
            histogram:merge(histogram:shift(Hist1, ShiftSize), Hist2)
    end.


-spec key(TargetProvider :: od_provider:id() | undefined,
    SpaceId :: od_space:id(), TransferType :: binary(), StatsType :: binary()
) ->
    binary().
key(undefined, SpaceId, TransferType, StatsType) ->
    key(<<"all">>, SpaceId, TransferType, StatsType);
key(TargetProvider, SpaceId, TransferType, StatsType) ->
    datastore_key:adjacent_from_digest([TargetProvider, TransferType, StatsType], SpaceId).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gathers transfer statistics of requested transfer type(s) for given space.
%% Returns a map with requested type(s) and corresponding stats, along with the
%% timestamp of latest update of any stats that are gathered.
%% @end
%%--------------------------------------------------------------------
-spec get_transfer_stats(RequestedTransferType :: binary(), od_space:id()) ->
    {#{binary() => transfer_stats()}, LatestUpdate :: transfer_histograms:timestamp()}.
get_transfer_stats(RequestedTransferType, SpaceId) ->
    TransferTypesToGet = case RequestedTransferType of
        ?ALL_TRANSFERS_TYPE ->
            [?JOB_TRANSFERS_TYPE, ?ON_THE_FLY_TRANSFERS_TYPE];
        _ ->
            [RequestedTransferType]
    end,
    {ok, SupportingProviders} = space_logic:get_provider_ids(?ROOT_SESS_ID, SpaceId),

    lists:foldl(fun(TransferType, {TransferStatsMap, AccGlobalLastUpdate}) ->
        {TransferStats, GlobalLastUpdate} = lists:foldl(fun(Provider, {AccTransferStats, AccLastUpdate}) ->
            case space_transfer_stats:get(Provider, TransferType, SpaceId) of
                {ok, #document{value = TransferStats}} ->
                    CurrentLastUpdate = space_transfer_stats:get_last_update(TransferStats),
                    {AccTransferStats#{Provider => TransferStats}, max(AccLastUpdate, CurrentLastUpdate)};
                {error, not_found} ->
                    {AccTransferStats, AccLastUpdate};
                {error, Error} ->
                    ?error("Failed to retrieve Space Transfer Stats Document
                            for space ~tp, provider ~tp and transfer type ~tp
                            due to: ~tp", [
                        SpaceId, Provider, TransferType, Error
                    ]),
                    {AccTransferStats, AccLastUpdate}
            end
        end, {#{}, AccGlobalLastUpdate}, SupportingProviders),
        {TransferStatsMap#{TransferType => TransferStats}, GlobalLastUpdate}
    end, {#{}, 0}, TransferTypesToGet).
