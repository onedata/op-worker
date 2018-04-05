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
-module(space_transfer_stats_cache).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([save/3, get/2, get_active_links/1, delete/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type timestamp() :: non_neg_integer().
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
%% Saves transfer statistics of given type for given space.
%% @end
%%-------------------------------------------------------------------
-spec save(SpaceId :: od_space:id(), Stats :: space_transfer_stats_cache(),
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
    space_transfer_stats_cache() | {error, term()}.
get(SpaceId, RequestedStatsType) ->
    Now = time_utils:system_time_millis(),
    Key = datastore_utils:gen_key(RequestedStatsType, SpaceId),
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
            prepare_stats(SpaceId, RequestedStatsType);
        _ ->
            Fetched
    end.


%%-------------------------------------------------------------------
%% @doc
%% Returns active links for given space (providers mapping to providers
%% they recently sent data to).
%% @end
%%-------------------------------------------------------------------
-spec get_active_links(SpaceId :: od_space:id()) ->
    {ok, #{od_provider:id() => [od_provider:id()]}} | {error, term()}.
get_active_links(SpaceId) ->
    case get(SpaceId, ?MINUTE_STAT_TYPE) of
        #space_transfer_stats_cache{active_links = ActiveLinks} ->
            {ok, ActiveLinks};
        Error ->
            Error
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
        {active_links, #{string => [string]}}
    ]}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gather transfer statistics of requested types for given space, pad them
%% with zeroes to current time and erase recent n-seconds to avoid fluctuations
%% on charts (due to synchronization between providers). To do that for type
%% other than minute one, it is required to calculate also mentioned minute stats
%% (otherwise it is not possible to trim histograms of other types).
%% Convert prepared histograms to speed charts and cache them for future requests.
%% @end
%%--------------------------------------------------------------------
-spec prepare_stats(SpaceId :: od_space:id(), StatsType :: binary()) ->
    space_transfer_stats_cache().
prepare_stats(SpaceId, ?MINUTE_STAT_TYPE) ->
    [MinStats] = get_stats(SpaceId, [?MINUTE_STAT_TYPE]),
    #space_transfer_stats_cache{
        timestamp = Timestamp, stats_in = StatsIn, stats_out = StatsOut
    } = MinStats,

    {TrimmedStatsIn, TrimmedTimestamp} = transfer_histograms:trim_min_histograms(
        StatsIn, Timestamp
    ),
    {TrimmedStatsOut, TrimmedTimestamp} = transfer_histograms:trim_min_histograms(
        StatsOut, Timestamp
    ),

    % Filter out zeroed histograms
    Pred = fun(_Provider, Histogram) -> lists:sum(Histogram) > 0 end,
    NewMinStats = MinStats#space_transfer_stats_cache{
        timestamp = TrimmedTimestamp,
        stats_in = maps:filter(Pred, TrimmedStatsIn),
        stats_out = maps:filter(Pred, TrimmedStatsOut)
    },
    SpeedMinStats = stats_to_speed_charts(NewMinStats, ?MINUTE_STAT_TYPE),
    save(SpaceId, SpeedMinStats, ?MINUTE_STAT_TYPE),
    SpeedMinStats;

prepare_stats(SpaceId, RequestedStatsType) ->
    [MinStats, RequestedStats] = get_stats(SpaceId, [
        ?MINUTE_STAT_TYPE, RequestedStatsType
    ]),
    #space_transfer_stats_cache{
        timestamp = Timestamp, stats_in = MinStatsIn, stats_out = MinStatsOut
    } = MinStats,
    #space_transfer_stats_cache{
        timestamp = Timestamp, stats_in = StatsIn, stats_out = StatsOut
    } = RequestedStats,

    Window = transfer_histograms:type_to_time_window(RequestedStatsType),
    {NewMinStatsIn, NewStatsIn, NewTimestamp} = transfer_histograms:trim(
        MinStatsIn, StatsIn, Window, Timestamp
    ),
    {NewMinStatsOut, NewStatsOut, NewTimestamp} = transfer_histograms:trim(
        MinStatsOut, StatsOut, Window, Timestamp
    ),

    % Filter out zeroed histograms
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

    SpeedMinStats = stats_to_speed_charts(NewMinStats, ?MINUTE_STAT_TYPE),
    save(SpaceId, SpeedMinStats, ?MINUTE_STAT_TYPE),
    SpeedRequestedStats = stats_to_speed_charts(NewRequestedStats, RequestedStatsType),
    save(SpaceId, SpeedRequestedStats, RequestedStatsType),
    SpeedRequestedStats.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gather transfer statistics of requested types for given space.
%% @end
%%--------------------------------------------------------------------
-spec get_stats(SpaceId :: od_space:id(), HistogramTypes :: [binary()]) ->
    [space_transfer_stats_cache()].
get_stats(SpaceId, RequestedStatsTypes) ->
    {ok, #document{value = Space}} = od_space:get(SpaceId),
    SpaceTransfers = lists:foldl(fun(Provider, STs) ->
        case space_transfer_stats:get(Provider, SpaceId) of
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
        {#space_transfer_stats_cache{
            expires = LocalTime + stats_type_to_expiration_timeout(StatsType),
            timestamp = CurrentTime
        }, StatsType} || StatsType <- RequestedStatsTypes
    ],

    GatheredStats = maps:fold(fun(Provider, SpaceTransfer, CurrentStats) ->
        lists:map(fun({Stats, StatsType}) ->
            NewStats = update_stats(
                Stats, StatsType, SpaceTransfer, CurrentTime, Provider
            ),
            {NewStats, StatsType}
        end, CurrentStats)
    end, EmptyStats, SpaceTransfers),
    lists:map(fun({Stats, _StatsType}) -> Stats end, GatheredStats).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Update given statistics for provider based on given space transfer record.
%% @end
%%--------------------------------------------------------------------
-spec update_stats(OldStats :: space_transfer_stats_cache(), StatsType :: binary(),
    ST :: space_transfer:space_transfer(), CurrentTime :: timestamp(),
    Provider :: od_provider:id()) -> space_transfer_stats_cache().
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
    ZeroedHist = time_slot_histogram:new(
        CurrentTime, TimeWindow, histogram:new(HistLen)
    ),

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

    NewActiveLinks = lists:foldl(fun(SrcProvider, OldActiveLinks) ->
        DestinationProviders = maps:get(SrcProvider, OldActiveLinks, []),
        OldActiveLinks#{SrcProvider => [Provider | DestinationProviders]}
    end, ActiveLinks, SrcProviders),

    OldStats#space_transfer_stats_cache{
        stats_in = NewStatsIn,
        stats_out = NewStatsOut,
        active_links = NewActiveLinks
    }.


-spec stats_to_speed_charts(Stats :: space_transfer_stats_cache(),
    StatsType :: binary()) -> space_transfer_stats_cache().
stats_to_speed_charts(Stats, StatsType) ->
    Timestamp = Stats#space_transfer_stats_cache.timestamp,
    TimeWindow = transfer_histograms:type_to_time_window(StatsType),
    ToSpeedChart = fun(_ProviderId, Histogram) ->
        transfer_histograms:histogram_to_speed_chart(
            Histogram, 0, Timestamp, TimeWindow)
    end,
    Stats#space_transfer_stats_cache{
        stats_in = maps:map(ToSpeedChart, Stats#space_transfer_stats_cache.stats_in),
        stats_out = maps:map(ToSpeedChart, Stats#space_transfer_stats_cache.stats_out)
    }.


-spec stats_type_to_expiration_timeout(binary()) -> non_neg_integer().
stats_type_to_expiration_timeout(?MINUTE_STAT_TYPE) -> ?MINUTE_STAT_EXPIRATION;
stats_type_to_expiration_timeout(?HOUR_STAT_TYPE) -> ?HOUR_STAT_EXPIRATION;
stats_type_to_expiration_timeout(?DAY_STAT_TYPE) -> ?DAY_STAT_EXPIRATION;
stats_type_to_expiration_timeout(?MONTH_STAT_TYPE) -> ?MONTH_STAT_EXPIRATION.
