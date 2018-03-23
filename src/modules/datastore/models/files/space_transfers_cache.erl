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
-module(space_transfers_cache).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type space_transfers_cache() :: #space_transfers_cache{}.
-type doc() :: datastore_doc:doc(space_transfers_cache()).

-export_type([space_transfers_cache/0, doc/0]).

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
    space_transfers_cache() | {error, term()}.
get(ResourceType, SpaceId) ->
    {ok, SpaceDoc} = od_space:get(SpaceId),
    Providers = maps:keys(SpaceDoc#document.value#od_space.providers),
    CurrentTime = provider_logic:zone_time_seconds(),
    TimeWindow = case ResourceType of
        ?MINUTE_STAT_TYPE -> ?FIVE_SEC_TIME_WINDOW;
        ?HOUR_STAT_TYPE -> ?MIN_TIME_WINDOW;
        ?DAY_STAT_TYPE -> ?HOUR_TIME_WINDOW;
        ?MONTH_STAT_TYPE -> ?DAY_TIME_WINDOW
    end,

    SpaceTransfersCacheRecord = lists:foldl(fun(ProviderId, Acc) ->
        case space_transfers:get(ProviderId, SpaceId) of
            {ok, SpaceTransfer} ->
                LastUpdates = SpaceTransfer#space_transfers.last_update,
                Histograms = case ResourceType of
                    ?MINUTE_STAT_TYPE -> SpaceTransfer#space_transfers.min_hist;
                    ?HOUR_STAT_TYPE -> SpaceTransfer#space_transfers.hr_hist;
                    ?DAY_STAT_TYPE -> SpaceTransfer#space_transfers.dy_hist;
                    ?MONTH_STAT_TYPE -> SpaceTransfer#space_transfers.mth_hist
                end,

                {ProvIn, TransOut, Conn} = maps:fold(fun(Provider, Histogram, Acc) ->
                    {Hist, Out, Connections} = Acc,
                    LastUpdate = maps:get(Provider, LastUpdates),
                    ProviderHist = transfer_histogram:new_time_slot_histogram(LastUpdate, TimeWindow, Histogram),
                    NewHist = time_slot_histogram:merge(Hist, ProviderHist),
                    NewMapping = case abs(CurrentTime - LastUpdate) =< ?INACTIVITY_TIMEOUT of
                        false ->
                            Connections;
                        true ->
                            [{Provider, ProviderId} | Connections]
                    end,
                    {NewHist, Out#{Provider => Histogram}, NewMapping}
                end, {
                    transfer_histogram:new_time_slot_histogram(CurrentTime, TimeWindow), #{}, []
                }, Histograms),

                #space_transfers_cache{
                    stats_in = StatsIn,
                    stats_out = StatsOut,
                    mapping = Mapping
                } = Acc,

                NewStatsOut = maps:fold(fun(Provider, Histogram, Acc) ->
                    LastUpdate = maps:get(Provider, LastUpdates),
                    OldHist = case maps:get(Provider, Acc, none) of
                        none -> transfer_histogram:new_time_slot_histogram(CurrentTime, TimeWindow);
                        Hist -> transfer_histogram:new_time_slot_histogram(LastUpdate, TimeWindow, Hist)
                    end,
                    ProviderHist = transfer_histogram:new_time_slot_histogram(LastUpdate, TimeWindow, Histogram),
                    NewHist = time_slot_histogram:merge(OldHist, ProviderHist),
                    Acc#{Provider => time_slot_histogram:get_histogram_values(NewHist)}
                end, StatsOut, TransOut),

                NewMapping = lists:foldl(fun({FromProvider, ToProvider}, Acc) ->
                    ToProviders = maps:get(FromProvider, Acc, []),
                    Acc#{FromProvider => [ToProvider | ToProviders]}
                end, Mapping, Conn),

                Acc#space_transfers_cache{
                    stats_in = StatsIn#{ProviderId => time_slot_histogram:get_histogram_values(ProvIn)},
                    stats_out = NewStatsOut,
                    mapping = NewMapping
                };
            _Error ->
                Acc
        end
    end, #space_transfers_cache{}, Providers),

    SpaceTransfersCacheRecord#space_transfers_cache{timestamp = CurrentTime}.


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
        {last_update, #{string => integer}},
        {min_hist, #{string => [integer]}},
        {hr_hist, #{string => [integer]}},
        {dy_hist, #{string => [integer]}},
        {mth_hist, #{string => [integer]}}
    ]}.
