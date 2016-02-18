%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Hook executed whenever dbsync changes file_location document.
%%% @end
%%%--------------------------------------------------------------------
-module(replication_dbsync_hook).
-author("Tomasz Lichon").

-include("modules/dbsync/common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([on_file_location_change/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handler for file_location changes which impacts local replicas.
%% @end
%%--------------------------------------------------------------------
-spec on_file_location_change(space_info:id(), file_location:doc()) -> ok.
on_file_location_change(_SpaceId, ChangedLocationDoc =
    #document{key = Key, value = #file_location{uuid = Uuid, provider_id = ProviderId}}) ->
    file_location:run_synchronized(Uuid,
        fun() ->
            ?info("Processing change of file ~p, on provider: ~p", [Uuid, ProviderId]),
            case oneprovider:get_provider_id() =/= ProviderId of
                true ->
                    {ok, Locations} = file_meta:get_locations({uuid, Uuid}),
                    lists:foreach(fun(Id) ->
                        update_location_replica(Id, ChangedLocationDoc) end,
                        [LocationId || LocationId <- Locations, LocationId =/= Key]);
                false ->
                    ok
            end
        end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if given replica is local and updates it according to external changes.
%% @end
%%--------------------------------------------------------------------
-spec update_location_replica(file_location:id(), file_location:doc()) -> ok.
update_location_replica(LocationId, ChangedLocationDoc) ->
    LocalProviderId = oneprovider:get_provider_id(),
    case file_location:get(LocationId) of
        {ok, LocalLocationDoc = #document{value = #file_location{provider_id = LocalProviderId}}} ->
            update_local_location_replica(LocalLocationDoc, ChangedLocationDoc);
        {ok, _} ->
            ok;
        Error ->
            ?error("Cannot get file_location: ~p for replica update, due to ~p", [LocationId, Error])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Update local location replica according to external changes.
%% @end
%%--------------------------------------------------------------------
-spec update_local_location_replica(file_location:doc(), file_location:doc()) -> ok.
update_local_location_replica(LocalDoc = #document{value = #file_location{version_vector = LocalVV}},
    RemoteDoc = #document{value = #file_location{version_vector = RemoteVV}}) ->
    case version_vector:compare(LocalVV, RemoteVV) of
        identical -> ok;
        greater -> ok;
        lesser -> update_outdated_local_location_replica(LocalDoc, RemoteDoc);
        concurrent -> reconcile_replicas(LocalDoc, RemoteDoc)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Update local location replica according to external changes, which are
%% strictly greater in version.
%% @end
%%--------------------------------------------------------------------
-spec update_outdated_local_location_replica(file_location:doc(), file_location:doc()) -> ok.
update_outdated_local_location_replica(LocalDoc = #document{value = #file_location{uuid = Uuid, version_vector = VV1}},
    ExternalDoc = #document{value = #file_location{version_vector = VV2, size = NewSize}}) ->
    ?info("Updating outdated replica ~p, versions: ~p vs ~p", [Uuid, VV1, VV2]),
    LocationDocWithNewVersion = version_vector:merge_location_versions(LocalDoc, ExternalDoc),
    Diff = version_vector:version_diff(LocalDoc, ExternalDoc),
    Changes = fslogic_file_location:get_changes(ExternalDoc, Diff),
    NewDoc = replica_invalidator:invalidate_changes(LocationDocWithNewVersion, Changes, NewSize),
    notify_block_change_if_necessary(LocationDocWithNewVersion, NewDoc),
    notify_size_change_if_necessary(LocationDocWithNewVersion, NewDoc).

%%--------------------------------------------------------------------
%% @doc
%% Reconcile conflicted local replica according to external changes.
%% @end
%%--------------------------------------------------------------------
-spec reconcile_replicas(file_location:doc(), file_location:doc()) -> ok.
reconcile_replicas(LocalDoc = #document{value = LocalLocation = #file_location{uuid = Uuid, version_vector = VV1}},
    #document{value = #file_location{version_vector = VV2, blocks = Blocks2}}) ->
    ?info("Conflicting changes detected on ~p, versions: ~p vs ~p", [Uuid, VV1, VV2]),
    fslogic_blocks:invalidate(LocalDoc#document{value = LocalLocation#file_location{version_vector = version_vector:reconcile(VV1, VV2)}}, Blocks2). %todo reconcile

%%--------------------------------------------------------------------
%% @doc
%% Notify clients if blocks has changed.
%% @end
%%--------------------------------------------------------------------
-spec notify_block_change_if_necessary(file_location:doc(), file_location:doc()) -> ok.
notify_block_change_if_necessary(#document{value = #file_location{blocks = SameBlocks}},
    #document{value = #file_location{blocks = SameBlocks}}) ->
    ok;
notify_block_change_if_necessary(#document{value = #file_location{uuid = FileUuid}}, _) ->
    ok = fslogic_event:emit_file_location_update({uuid, FileUuid}, []).

%%--------------------------------------------------------------------
%% @doc
%% Notify clients if file size has changed.
%% @end
%%--------------------------------------------------------------------
-spec notify_size_change_if_necessary(file_location:doc(), file_location:doc()) -> ok.
notify_size_change_if_necessary(#document{value = #file_location{size = SameSize}},
        #document{value = #file_location{size = SameSize}}) ->
    ok;
notify_size_change_if_necessary(#document{value = #file_location{uuid = FileUuid}}, _) ->
    ok = fslogic_event:emit_file_attr_update({uuid, FileUuid}, []).