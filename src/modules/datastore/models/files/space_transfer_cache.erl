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
-export([get/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type space_transfer_cache() :: #space_transfer_cache{}.
-type doc() :: datastore_doc:doc(space_transfer_cache()).

-export_type([space_transfer_cache/0, doc/0]).

-define(INACTIVITY_TIMEOUT, 30).
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
%% Returns space transfers for given space.
%% @end
%%-------------------------------------------------------------------
-spec get(ResourceType :: binary(), SpaceId :: od_space:id()) ->
    space_transfer_cache() | {error, term()}.
get(ResourceType, SpaceId) ->
    {ok, SpaceDoc} = od_space:get(SpaceId),
    Providers = maps:keys(SpaceDoc#document.value#od_space.providers),

    TimeWindow = case ResourceType of
        ?MINUTE_STAT_TYPE -> ?FIVE_SEC_TIME_WINDOW;
        ?HOUR_STAT_TYPE -> ?MIN_TIME_WINDOW;
        ?DAY_STAT_TYPE -> ?HOUR_TIME_WINDOW;
        ?MONTH_STAT_TYPE -> ?DAY_TIME_WINDOW
    end,
    CurrentTime = provider_logic:zone_time_seconds(),
    ZeroedHist = transfer_histogram:new_time_slot_histogram(CurrentTime, TimeWindow),

    SpaceTransferCache = lists:foldl(fun(ProviderId, Cache) ->
        case space_transfer:get(ProviderId, SpaceId) of
            {ok, SpaceTransfer} ->
                #space_transfer_cache{
                    stats_in = StatsIn,
                    stats_out = StatsOut,
                    mapping = Mapping
                } = Cache,
                Histograms = case ResourceType of
                    ?MINUTE_STAT_TYPE -> SpaceTransfer#space_transfer.min_hist;
                    ?HOUR_STAT_TYPE -> SpaceTransfer#space_transfer.hr_hist;
                    ?DAY_STAT_TYPE -> SpaceTransfer#space_transfer.dy_hist;
                    ?MONTH_STAT_TYPE -> SpaceTransfer#space_transfer.mth_hist
                end,
                LastUpdates = SpaceTransfer#space_transfer.last_update,

                {HistIn, HistsOut, Connections} = maps:fold(fun(Provider, Histogram, Acc) ->
                    {OldHist, OldHistsOut, OldConnections} = Acc,
                    LastUpdate = maps:get(Provider, LastUpdates),
                    Hist = transfer_histogram:new_time_slot_histogram(
                        LastUpdate, TimeWindow, Histogram
                    ),
                    NewHist = time_slot_histogram:merge(OldHist, Hist),
                    NewMapping = case abs(CurrentTime - LastUpdate) =< ?INACTIVITY_TIMEOUT of
                        false -> OldConnections;
                        true -> [{Provider, ProviderId} | OldConnections]
                    end,
                    {NewHist, OldHistsOut#{Provider => Histogram}, NewMapping}
                end, {ZeroedHist, #{}, []}, Histograms),

                NewStatsIn = StatsIn#{
                    ProviderId => time_slot_histogram:get_histogram_values(HistIn)
                },

                NewStatsOut = maps:fold(fun(Provider, Histogram, Acc) ->
                    LastUpdate = maps:get(Provider, LastUpdates),
                    OldHist = case maps:get(Provider, Acc, none) of
                        none ->
                            ZeroedHist;
                        Hist ->
                            transfer_histogram:new_time_slot_histogram(
                                LastUpdate, TimeWindow, Hist)
                    end,
                    ProviderHist = transfer_histogram:new_time_slot_histogram(
                        LastUpdate, TimeWindow, Histogram
                    ),
                    NewHist = time_slot_histogram:merge(OldHist, ProviderHist),
                    Acc#{Provider => time_slot_histogram:get_histogram_values(NewHist)}
                end, StatsOut, HistsOut),

                NewMapping = lists:foldl(fun({FromProvider, ToProvider}, Acc) ->
                    ToProviders = maps:get(FromProvider, Acc, []),
                    Acc#{FromProvider => [ToProvider | ToProviders]}
                end, Mapping, Connections),

                Cache#space_transfer_cache{
                    stats_in = NewStatsIn,
                    stats_out = NewStatsOut,
                    mapping = NewMapping
                };
            _Error ->
                Cache
        end
    end, #space_transfer_cache{timestamp = CurrentTime}, Providers),

    SpaceTransferCache.


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
