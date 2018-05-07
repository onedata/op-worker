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
    save/4,
    get/3, get_active_links/1,
    update/4,
    delete/3
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
%% Saves transfer statistics of given type for given space and transfer type.
%% @end
%%-------------------------------------------------------------------
-spec save(TransferType :: binary(), StatsType :: binary(),
    SpaceId :: od_space:id(), Stats :: space_transfer_stats_cache()
) ->
    ok | {error, term()}.
save(TransferType, StatsType, SpaceId, Stats) ->
    Key = key(TransferType, StatsType, SpaceId),
    case datastore_model:save(?CTX, #document{key = Key, value = Stats}) of
        {ok, _} -> ok;
        Error -> Error
    end.


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfer statistics of requested type for given space.
%% @end
%%-------------------------------------------------------------------
-spec get(TransferType :: binary(), StatsType :: binary(),
    SpaceId :: od_space:id()) -> space_transfer_stats_cache() | {error, term()}.
get(TransferType, StatsType, SpaceId) ->
    Now = time_utils:system_time_millis(),
    Key = key(TransferType, StatsType, SpaceId),
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
            TransferStatsMap = get_transfer_stats(TransferType, SpaceId),
            CurrentTime = provider_logic:zone_time_seconds(),
            prepare_aggregated_stats(
                TransferType, StatsType, SpaceId, TransferStatsMap, CurrentTime
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
    case get(?JOB_TRANSFERS_TYPE, ?MINUTE_STAT_TYPE, SpaceId) of
        #space_transfer_stats_cache{active_links = ActiveLinks} ->
            {ok, ActiveLinks};
        Error ->
            Error
    end.


%%-------------------------------------------------------------------
%% @doc
%% Update transfer statistics of given type for given space. If previously
%% cached stats has not expired yet do not overwrite it. It helps prevent
%% situation when the same stats are cached 2 times longer than they should
%% (e.g. when counting minute stats, entire time slots making up to recent
%% n-seconds are trimmed meaning that stats counted for timestamps:
%% 61 and 64 are the same).
%% @end
%%-------------------------------------------------------------------
-spec update(TransferType :: binary(), StatsType :: binary(),
    SpaceId :: od_space:id(), Stats :: space_transfer_stats_cache()
) ->
    ok | {error, term()}.
update(TransferType, StatsType, SpaceId, Stats) ->
    Key = key(TransferType, StatsType, SpaceId),
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
%% @end
%%-------------------------------------------------------------------
-spec delete(TransferType :: binary(), StatsType :: binary(),
    SpaceId :: od_space:id()) -> ok | {error, term()}.
delete(TransferType, StatsType, SpaceId) ->
    Key = key(TransferType, StatsType, SpaceId),
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
%% for given space. Pad them with zeroes to current time and erase recent
%% n-seconds to avoid fluctuations on charts. To do that for type
%% other than minute one, it is required to calculate also minute stats
%% (otherwise it is not possible to trim histograms of other types).
%% @end
%%--------------------------------------------------------------------
-spec prepare_aggregated_stats(TransferType :: binary(), StatsType :: binary(),
    SpaceId :: od_space:id(), TransferStatsMap :: #{binary() => transfer_stats()},
    CurrentTime :: timestamp()
) ->
    space_transfer_stats_cache().
prepare_aggregated_stats(?ALL_TRANSFERS_TYPE, StatsType, SpaceId,
    TransferStatsMap, CurrentTime
) ->
    JobStats = prepare_aggregated_stats(?JOB_TRANSFERS_TYPE,
        StatsType, SpaceId, TransferStatsMap, CurrentTime
    ),
    OnTheFlyStats = prepare_aggregated_stats(?ON_THE_FLY_TRANSFERS_TYPE,
        StatsType, SpaceId, TransferStatsMap, CurrentTime
    ),
    AllStats = merge_stats(JobStats, OnTheFlyStats),
    update(?ALL_TRANSFERS_TYPE, StatsType, SpaceId, AllStats),
    AllStats;

prepare_aggregated_stats(TransferType, ?MINUTE_STAT_TYPE, SpaceId,
    TransferStatsMap, CurrentTime
) ->
    TransferStats = maps:get(TransferType, TransferStatsMap),
    [MinStats] = aggregate_stats(
        TransferStats, [?MINUTE_STAT_TYPE], CurrentTime
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
    update(TransferType, ?MINUTE_STAT_TYPE, SpaceId, NewMinStats),
    NewMinStats;

prepare_aggregated_stats(TransferType, RequestedStatsType, SpaceId,
    TransferStatsMap, CurrentTime
) ->
    TransferStats = maps:get(TransferType, TransferStatsMap),
    [MinStats, RequestedStats] = aggregate_stats(
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

    update(TransferType, ?MINUTE_STAT_TYPE, SpaceId, NewMinStats),
    update(TransferType, RequestedStatsType, SpaceId, NewRequestedStats),
    NewRequestedStats.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Aggregate statistics of specified type from given transfer statistics.
%% @end
%%--------------------------------------------------------------------
-spec aggregate_stats(transfer_stats(), HistogramTypes :: [binary()],
    CurrentTime :: timestamp()) -> [space_transfer_stats_cache()].
aggregate_stats(TransferStats, RequestedStatsTypes, CurrentTime) ->
    LocalTime = time_utils:system_time_millis(),
    EmptyStats = [
        #space_transfer_stats_cache{
            expires = LocalTime + stats_type_to_expiration_timeout(StatsType),
            timestamp = CurrentTime
        } || StatsType <- RequestedStatsTypes
    ],

    maps:fold(fun(Provider, TransferStat, AggregatedStats) ->
        lists:map(fun({Stats, StatsType}) ->
            update_stats(Stats, StatsType, TransferStat, CurrentTime, Provider)
        end, lists:zip(AggregatedStats, RequestedStatsTypes))
    end, EmptyStats, TransferStats).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Update given aggregated statistics for provider based on given
%% space transfer record.
%% @end
%%--------------------------------------------------------------------
-spec update_stats(OldStats :: space_transfer_stats_cache(),
    StatsType :: binary(), ST :: #space_transfer_stats{},
    CurrentTime :: timestamp(), Provider :: od_provider:id()
) ->
    space_transfer_stats_cache().
update_stats(OldStats, StatsType, ST, CurrentTime, Provider) ->
    #space_transfer_stats_cache{
        stats_in = StatsIn,
        stats_out = StatsOut,
        active_links = ActiveLinks
    } = OldStats,

    LastUpdates = ST#space_transfer_stats.last_update,
    {Histograms, TimeWindow} = case StatsType of
        ?MINUTE_STAT_TYPE -> {ST#space_transfer_stats.min_hist, ?FIVE_SEC_TIME_WINDOW};
        ?HOUR_STAT_TYPE -> {ST#space_transfer_stats.hr_hist, ?MIN_TIME_WINDOW};
        ?DAY_STAT_TYPE -> {ST#space_transfer_stats.dy_hist, ?HOUR_TIME_WINDOW};
        ?MONTH_STAT_TYPE -> {ST#space_transfer_stats.mth_hist, ?DAY_TIME_WINDOW}
    end,
    HistLen = transfer_histograms:type_to_hist_length(StatsType),
    ZeroedHist = time_slot_histogram:new(0,
        CurrentTime, TimeWindow, histogram:new(HistLen)
    ),

    {HistIn, HistsOut, SrcProviders} = maps:fold(fun(SrcProvider, Hist, Acc) ->
        {OldHistIn, OldHistsOut, OldSrcProviders} = Acc,
        LastUpdate = maps:get(SrcProvider, LastUpdates),
        TimeSlotHist = time_slot_histogram:new(0, LastUpdate, TimeWindow, Hist),
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
            HistOut -> time_slot_histogram:new(0, CurrentTime, TimeWindow, HistOut)
        end,
        NewHistOut = time_slot_histogram:merge(OldHistOut, Hist),
        OldStatsOut#{
            SrcProvider => time_slot_histogram:get_histogram_values(NewHistOut)
        }
    end, StatsOut, HistsOut),

    NewActiveLinks = lists:foldl(fun(SrcProvider, OldActiveLinks) ->
        DestinationProviders = maps:get(SrcProvider, OldActiveLinks, []),
        OldActiveLinks#{SrcProvider => [Provider | DestinationProviders]}
    end, ActiveLinks, SrcProviders),

    OldStats#space_transfer_stats_cache{
        stats_in = NewStatsIn,
        stats_out = NewStatsOut,
        active_links = NewActiveLinks
    }.


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


-spec key(TransferType :: binary(), StatsType :: binary(),
    SpaceId :: od_space:id()) -> binary().
key(TransferType, StatsType, SpaceId) ->
    RecordId = op_gui_utils:ids_to_association(TransferType, SpaceId),
    datastore_utils:gen_key(StatsType, RecordId).


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
