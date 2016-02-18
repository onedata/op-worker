%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module for synchronizing file replicas
%%% @end
%%%--------------------------------------------------------------------
-module(replica_synchronizer).
-author("Tomasz Lichon").

-include("modules/dbsync/common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([synchronize/2]).

-define(SYNC_TIMEOUT, timer:seconds(10)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sychronizes File on given range.
%% @end
%%--------------------------------------------------------------------
-spec synchronize(file_meta:uuid(), fslogic_blocks:block()) -> ok.
synchronize(Uuid, Block) ->
    {ok, Locations} = file_meta:get_locations({uuid, Uuid}),
    LocationDocs = lists:map(
        fun(LocationId) ->
            {ok, Loc} = file_location:get(LocationId),
            Loc
        end, Locations),
    ProvidersAndBlocks = get_blocks_for_sync(LocationDocs, [Block]),
    lists:foreach(
        fun({ProviderId, Blocks}) ->
            lists:foreach(
                fun(BlockToSync = #file_block{offset = O, size = S}) ->
                    Ref = rtransfer:prepare_request(ProviderId, Uuid, O, S),
                    NewRef = rtransfer:fetch(Ref, fun notify_fun/3, on_complete_fun()),
                    {ok, Size} = receive_rtransfer_notification(NewRef, ?SYNC_TIMEOUT),
                    replica_updater:update(Uuid, [BlockToSync#file_block{size = Size}], undefined, false) %todo check if multiblock does not cause conflicts
                end, Blocks)
        end, ProvidersAndBlocks),
    fslogic_event:emit_file_location_update({uuid, Uuid}, []).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% RTransfer notify fun
%% @end
%%--------------------------------------------------------------------
-spec notify_fun(any(), any(), any()) -> ok.
notify_fun(_, _, _) -> ok.


%%--------------------------------------------------------------------
%% @doc
%% Rtransfer on complete fun
%% @end
%%--------------------------------------------------------------------
-spec on_complete_fun() -> ok.
on_complete_fun() ->
    Self = self(),
    fun(Ref, Status) ->
        Self ! {Ref, Status}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Wait for Rtransfer notification.
%% @end
%%--------------------------------------------------------------------
-spec receive_rtransfer_notification(rtransfer:ref(), non_neg_integer()) -> term().
receive_rtransfer_notification(Ref, Timeout) ->
    receive
        {Ref, Status} ->
            Status
    after
        Timeout -> {error, timeout}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Check if given blocks are synchronized in local file locations. If not,
%% returns list with tuples informing where to fetch data: {ProviderId, BlocksToFetch}
%% @end
%%--------------------------------------------------------------------
-spec get_blocks_for_sync([file_location:doc()], fslogic_blocks:blocks()) ->
    [{oneprovider:id(), fslogic_blocks:blocks()}].
get_blocks_for_sync(_, []) ->
    [];
get_blocks_for_sync(Locations, Blocks) ->
    LocalProviderId = oneprovider:get_provider_id(),
    LocalLocations = [Loc || Loc = #document{value = #file_location{provider_id = Id}} <- Locations, Id =:= LocalProviderId],
    RemoteLocations = Locations -- LocalLocations,
    [LocalBlocks] = [LocalBlocks || #document{value = #file_location{blocks = LocalBlocks}} <- LocalLocations], %todo allow multi location
    BlocksToSync = fslogic_blocks:invalidate(Blocks, LocalBlocks),

    case BlocksToSync of
        [] -> [];
        _ ->
            [Loc | _] = RemoteLocations,
            RemoteProvider = Loc#document.value#file_location.provider_id,
            [{RemoteProvider, BlocksToSync}] %todo handle multiprovider case
    end.
