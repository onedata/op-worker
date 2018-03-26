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
-export([save/1, get/2, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type space_transfer_cache() :: #space_transfer_cache{}.
-type doc() :: datastore_doc:doc(space_transfer_cache()).
-type timestamp() :: non_neg_integer().

-export_type([space_transfer_cache/0, doc/0]).

-define(INACTIVITY_TIMEOUT, 20).
-define(CACHE_EXPIRATION_TIMEOUT, 5).

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
%% Deletes space transfer document.
%% @end
%%-------------------------------------------------------------------
-spec save(SpaceId :: od_space:id()) -> ok | {error, term()}.
save(SpaceId) ->
    Key = datastore_utils:gen_key(oneprovider:get_id_or_undefined(), SpaceId),
    datastore_model:delete(?CTX, Key).


%%-------------------------------------------------------------------
%% @doc
%% Returns space transfers for given space.
%% @end
%%-------------------------------------------------------------------
-spec get(HistogramType :: binary(), SpaceId :: od_space:id()) ->
    space_transfer_cache() | {error, term()}.
get(HistogramType, SpaceId) ->
    [Stats] = get_stats(SpaceId, [HistogramType]),
    Stats.


%%-------------------------------------------------------------------
%% @doc
%% Deletes space transfer document.
%% @end
%%-------------------------------------------------------------------
-spec delete(SpaceId :: od_space:id()) -> ok | {error, term()}.
delete(SpaceId) ->
    Key = datastore_utils:gen_key(oneprovider:get_id_or_undefined(), SpaceId),
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
    {ok, SpaceDoc} = od_space:get(SpaceId),
    SupportingProviders = maps:keys(SpaceDoc#document.value#od_space.providers),

    SpaceTransfers = lists:foldl(fun(ProviderId, Acc) ->
        case space_transfer:get(ProviderId, SpaceId) of
            {ok, SpaceTransfer} -> Acc#{ProviderId => SpaceTransfer};
            _Error -> Acc
        end
    end, #{}, SupportingProviders),

    CurrentTime = provider_logic:zone_time_seconds(),
    EmptyStats = [#space_transfer_cache{timestamp = CurrentTime} || _ <- HistogramTypes],
    maps:fold(fun(ProviderId, SpaceTransfer, Stats) ->
        lists:map(fun({CurrentStat, HistogramType}) ->
            update_stats(
                CurrentStat, HistogramType, SpaceTransfer, CurrentTime, ProviderId
            )
        end, lists:zip(Stats, HistogramTypes))
    end, EmptyStats, SpaceTransfers).


%%--------------------------------------------------------------------
%% @doc
%% Update statistics for provider based on given space transfer record.
%% @end
%%--------------------------------------------------------------------
-spec update_stats(CurrentStat :: space_transfer_cache(), HistogramType :: binary(),
    SpaceTransfer :: space_transfer:space_transfer(), CurrentTime :: timestamp(),
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
        NewSources = case CurrentTime - LastUpdate =< ?INACTIVITY_TIMEOUT of
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
