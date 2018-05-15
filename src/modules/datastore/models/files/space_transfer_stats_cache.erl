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
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    save/5,
    get/4, get_active_links/1,
    update/5,
    delete/4
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type timestamp() :: non_neg_integer().
-type transfer_stats() :: #{od_provider:id() => #space_transfer_stats{}}.
-type space_transfer_stats_cache() :: #space_transfer_stats_cache{}.
-type doc() :: datastore_doc:doc(space_transfer_stats_cache()).

-export_type([space_transfer_stats_cache/0, doc/0]).

-define(TRANSFER_INACTIVITY, application:get_env(
    ?APP_NAME, gui_transfer_inactivity_treshold, 20)
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
    case datastore_model:save(?CTX, #document{key = Key, value = Stats}) of
        {ok, _} -> ok;
        Error -> Error
    end.


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
    Now = time_utils:system_time_millis(),
    Key = key(TargetProvider, SpaceId, TransferType, StatsType),
    Fetched = case datastore_model:get(?CTX, Key) of
        {ok, #document{value = Stats}} ->
            case Now < Stats#space_transfer_stats_cache.expires of
                true -> Stats;
                false -> {error, not_found}
            end;
        Error ->
            Error
    end,
    case Fetched of
        {error, not_found} ->
            TransferStatsPerType = get_transfer_stats(TransferType, SpaceId),
            CurrentTime = provider_logic:zone_time_seconds(),
            prepare_aggregated_stats(TargetProvider, SpaceId,
                TransferType, StatsType, TransferStatsPerType, CurrentTime
            );
        _ ->
            Fetched
    end.


%%-------------------------------------------------------------------
%% @doc
%% Returns active links for given space (providers mapping to providers
%% they recently sent data to). For efficiency reasons they are stored only
%% with minute stats.
%% @end
%%-------------------------------------------------------------------
-spec get_active_links(SpaceId :: od_space:id()) ->
    {ok, #{od_provider:id() => [od_provider:id()]}} | {error, term()}.
get_active_links(SpaceId) ->
    case get(undefined, ?JOB_TRANSFERS_TYPE, ?MINUTE_STAT_TYPE, SpaceId) of
        #space_transfer_stats_cache{active_links = ActiveLinks} ->
            {ok, ActiveLinks};
        Error ->
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
        Now = time_utils:system_time_millis(),
        NewStats = case Now < OldStats#space_transfer_stats_cache.expires of
            true -> OldStats;
            false -> Stats
        end,
        {ok, NewStats}
    end,
    case datastore_model:update(?CTX, Key, Diff, Stats) of
        {ok, _} -> ok;
        Error -> Error
    end.


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
        {active_links, #{string => [string]}}
    ]}.


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
    CurrentTime :: timestamp()
) ->
    space_transfer_stats_cache().
prepare_aggregated_stats(TargetProvider, SpaceId, ?ALL_TRANSFERS_TYPE,
    StatsType, TransferStatsPerType, CurrentTime
) ->
    JobStats = prepare_aggregated_stats(TargetProvider, SpaceId,
        ?JOB_TRANSFERS_TYPE, StatsType, TransferStatsPerType, CurrentTime
    ),
    OnTheFlyStats = prepare_aggregated_stats(TargetProvider, SpaceId,
        ?ON_THE_FLY_TRANSFERS_TYPE, StatsType, TransferStatsPerType, CurrentTime
    ),
    AllStats = merge_stats(JobStats, OnTheFlyStats),
    update(TargetProvider, SpaceId, ?ALL_TRANSFERS_TYPE, StatsType, AllStats),
    AllStats;

prepare_aggregated_stats(TargetProvider, SpaceId, TransferType,
    ?MINUTE_STAT_TYPE, TransferStatsPerType, CurrentTime
) ->
    TransferStats = maps:get(TransferType, TransferStatsPerType),
    [MinStats] = aggregate_stats(
        TargetProvider, TransferStats, [?MINUTE_STAT_TYPE], CurrentTime
    ),

    #space_transfer_stats_cache{
        timestamp = Timestamp,
        stats_in = StatsIn,
        stats_out = StatsOut
    } = MinStats,

    {TrimmedStatsIn, TrimmedTimestamp} =
        transfer_histograms:trim_min_histograms(StatsIn, Timestamp),
    {TrimmedStatsOut, TrimmedTimestamp} =
        transfer_histograms:trim_min_histograms(StatsOut, Timestamp),

    Pred = fun(_Provider, Histogram) -> lists:sum(Histogram) > 0 end,
    NewMinStats = MinStats#space_transfer_stats_cache{
        timestamp = TrimmedTimestamp,
        stats_in = maps:filter(Pred, TrimmedStatsIn),
        stats_out = maps:filter(Pred, TrimmedStatsOut)
    },

    update(TargetProvider, SpaceId, TransferType, ?MINUTE_STAT_TYPE, NewMinStats),
    NewMinStats;

prepare_aggregated_stats(TargetProvider, SpaceId, TransferType,
    RequestedStatsType, TransferStatsMap, CurrentTime
) ->
    TransferStats = maps:get(TransferType, TransferStatsMap),
    [MinStats, RequestedStats] = aggregate_stats(TargetProvider,
        TransferStats, [?MINUTE_STAT_TYPE, RequestedStatsType], CurrentTime
    ),

    #space_transfer_stats_cache{
        timestamp = Timestamp,
        stats_in = MinStatsIn,
        stats_out = MinStatsOut
    } = MinStats,
    #space_transfer_stats_cache{
        timestamp = Timestamp,
        stats_in = StatsIn,
        stats_out = StatsOut
    } = RequestedStats,

    TimeWindow = transfer_histograms:type_to_time_window(RequestedStatsType),
    {NewMinStatsIn, NewStatsIn, NewTimestamp} =
        transfer_histograms:trim_histograms(
            MinStatsIn, StatsIn, TimeWindow, Timestamp),
    {NewMinStatsOut, NewStatsOut, NewTimestamp} =
        transfer_histograms:trim_histograms(
            MinStatsOut, StatsOut, TimeWindow, Timestamp),

    Pred = fun(_Provider, Histogram) -> lists:sum(Histogram) > 0 end,
    NewMinStats = MinStats#space_transfer_stats_cache{
        timestamp = NewTimestamp,
        stats_in = maps:filter(Pred, NewMinStatsIn),
        stats_out = maps:filter(Pred, NewMinStatsOut)
    },
    NewRequestedStats = RequestedStats#space_transfer_stats_cache{
        timestamp = NewTimestamp,
        stats_in = maps:filter(Pred, NewStatsIn),
        stats_out = maps:filter(Pred, NewStatsOut),
        active_links = undefined
    },

    update(TargetProvider, SpaceId, TransferType, ?MINUTE_STAT_TYPE, NewMinStats),
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
-spec aggregate_stats(TargetProvider :: od_provider:id() | undefined,
    transfer_stats(), HistogramTypes :: [binary()], CurrentTime :: timestamp()
) ->
    [space_transfer_stats_cache()].
aggregate_stats(TargetProvider, TransferStats, RequestedStatsTypes, CurrentTime) ->
    LocalTime = time_utils:system_time_millis(),
    EmptyStats = [
        #space_transfer_stats_cache{
            expires = LocalTime + stats_type_to_expiration_timeout(StatsType),
            timestamp = CurrentTime
        } || StatsType <- RequestedStatsTypes
    ],

    maps:fold(fun(Provider, TransferStat, AggregatedStats) ->
        lists:map(fun({Stats, StatsType}) ->
            update_stats(TargetProvider, Stats, StatsType,
                Provider, TransferStat, CurrentTime)
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
    CurrentTime :: timestamp()
) ->
    space_transfer_stats_cache().
update_stats(undefined, OldStats, StatsType,
    Provider, TransferStats, CurrentTime
) ->
    #space_transfer_stats_cache{
        stats_in = StatsIn,
        stats_out = StatsOut,
        active_links = ActiveLinks
    } = OldStats,

    Histograms = get_histograms(StatsType, TransferStats),
    LastUpdates = TransferStats#space_transfer_stats.last_update,
    TimeWindow = transfer_histograms:type_to_time_window(StatsType),
    ZeroedHist = histogram:new(transfer_histograms:type_to_hist_length(StatsType)),

    {HistIn, NewStatsOut, NewActiveLinks} = maps:fold(fun(SrcProvider, Hist, Acc) ->
        {OldHistIn, OldStatsOut, OldActiveLinks} = Acc,
        LastUpdate = maps:get(SrcProvider, LastUpdates),
        NewHistIn = merge_histograms(
            OldHistIn, CurrentTime, Hist, LastUpdate, TimeWindow
        ),

        OldHistOut = maps:get(SrcProvider, OldStatsOut, ZeroedHist),
        NewHistOut = merge_histograms(
            OldHistOut, CurrentTime, Hist, LastUpdate, TimeWindow
        ),

        NewLinks = case CurrentTime - LastUpdate =< ?TRANSFER_INACTIVITY of
            false ->
                OldActiveLinks;
            true ->
                DestinationProviders = maps:get(SrcProvider, OldActiveLinks, []),
                OldActiveLinks#{SrcProvider => [Provider | DestinationProviders]}
        end,
        {NewHistIn, OldStatsOut#{SrcProvider => NewHistOut}, NewLinks}
    end, {ZeroedHist, StatsOut, ActiveLinks}, Histograms),

    OldStats#space_transfer_stats_cache{
        stats_in = StatsIn#{Provider => HistIn},
        stats_out = NewStatsOut,
        active_links = NewActiveLinks
    };

update_stats(TargetProvider, OldStats, StatsType,
    TargetProvider, TransferStats, CurrentTime
) ->
    Histograms = get_histograms(StatsType, TransferStats),
    LastUpdates = TransferStats#space_transfer_stats.last_update,
    Window = transfer_histograms:type_to_time_window(StatsType),
    OldStats#space_transfer_stats_cache{
        stats_in = transfer_histograms:pad_with_zeroes(
            Histograms, Window, LastUpdates, CurrentTime)
    };

update_stats(TargetProvider, OldStats, StatsType,
    Provider, TransferStats, CurrentTime
) ->
    Histograms = get_histograms(StatsType, TransferStats),
    case maps:find(TargetProvider, Histograms) of
        {ok, Histogram} ->
            LastUpdate = maps:get(TargetProvider,
                TransferStats#space_transfer_stats.last_update),
            Window = transfer_histograms:type_to_time_window(StatsType),
            ShiftSize = max(0, (CurrentTime div Window) - (LastUpdate div Window)),
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
        timestamp = Timestamp,
        stats_in = StatsIn1,
        stats_out = StatsOut1
    } = Stats1,
    #space_transfer_stats_cache{
        expires = Expires,
        timestamp = Timestamp,
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
        expires = Expires,
        timestamp = Timestamp,
        stats_in = maps:fold(MergeFun, StatsIn1, StatsIn2),
        stats_out = maps:fold(MergeFun, StatsOut1, StatsOut2),
        active_links = undefined
    }.


-spec stats_type_to_expiration_timeout(binary()) -> non_neg_integer().
stats_type_to_expiration_timeout(?MINUTE_STAT_TYPE) -> ?MINUTE_STAT_EXPIRATION;
stats_type_to_expiration_timeout(?HOUR_STAT_TYPE) -> ?HOUR_STAT_EXPIRATION;
stats_type_to_expiration_timeout(?DAY_STAT_TYPE) -> ?DAY_STAT_EXPIRATION;
stats_type_to_expiration_timeout(?MONTH_STAT_TYPE) -> ?MONTH_STAT_EXPIRATION.


-spec get_histograms(HistogramsType :: binary(), #space_transfer_stats{}) ->
    transfer_histograms:histograms().
get_histograms(?MINUTE_STAT_TYPE, #space_transfer_stats{min_hist = Histograms}) ->
    Histograms;
get_histograms(?HOUR_STAT_TYPE, #space_transfer_stats{hr_hist = Histograms}) ->
    Histograms;
get_histograms(?DAY_STAT_TYPE, #space_transfer_stats{dy_hist = Histograms}) ->
    Histograms;
get_histograms(?MONTH_STAT_TYPE, #space_transfer_stats{mth_hist = Histograms}) ->
    Histograms.


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
    Seed = op_gui_utils:ids_to_association(TargetProvider, SpaceId),
    Key0 = op_gui_utils:ids_to_association(TransferType, StatsType),
    datastore_utils:gen_key(Seed, Key0).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gather transfer statistics of requested transfer type for given space.
%% @end
%%--------------------------------------------------------------------
-spec get_transfer_stats(RequestedTransferType :: binary(),
    SpaceId :: od_space:id()) -> #{binary() => transfer_stats()}.
get_transfer_stats(RequestedTransferType, SpaceId) ->
    TransferTypesToGet = case RequestedTransferType of
        ?ALL_TRANSFERS_TYPE ->
            [?JOB_TRANSFERS_TYPE, ?ON_THE_FLY_TRANSFERS_TYPE];
        _ ->
            [RequestedTransferType]
    end,
    {ok, #document{value = Space}} = od_space:get(SpaceId),
    SupportingProviders = maps:keys(Space#od_space.providers),

    lists:foldl(fun(TransferType, TransferStatsMap) ->
        TransferStats = lists:foldl(fun(Provider, OldTransferStats) ->
            case space_transfer_stats:get(Provider, TransferType, SpaceId) of
                {ok, TransferStat} ->
                    OldTransferStats#{Provider => TransferStat};
                {error, not_found} ->
                    OldTransferStats;
                {error, Error} ->
                    ?error("Failed to retrieve Space Transfer Stats Document
                            for space ~p, provider ~p and transfer type ~p
                            due to: ~p", [
                        SpaceId, Provider, TransferType, Error
                    ]),
                    OldTransferStats
            end
        end, #{}, SupportingProviders),
        TransferStatsMap#{TransferType => TransferStats}
    end, #{}, TransferTypesToGet).
