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

-include("proto/oneclient/common_messages.hrl").
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
-spec on_file_location_change(space_info:id(), file_location:doc()) ->
    ok | {error, term()}.
on_file_location_change(_SpaceId, ChangedLocationDoc =
    #document{value = #file_location{uuid = Uuid, provider_id = ProviderId}}) ->
    file_location:critical_section(Uuid,
        fun() ->
            case oneprovider:get_provider_id() =/= ProviderId of
                true ->
                    [LocalLocation] = fslogic_utils:get_local_file_locations_once({uuid, Uuid}),
                    update_local_location_replica(LocalLocation, ChangedLocationDoc);
                false ->
                    ok
            end
        end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
    ?info("Updating outdated replica of file ~p, versions: ~p vs ~p", [Uuid, VV1, VV2]),
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
reconcile_replicas(LocalDoc = #document{value = #file_location{uuid = Uuid, version_vector = VV1, blocks = LocalBlocks, size = LocalSize}},
    ExternalDoc = #document{value = #file_location{version_vector = VV2, size = ExternalSize}}) ->
    ?info("Conflicting changes detected on file ~p, versions: ~p vs ~p", [Uuid, VV1, VV2]),
    ExternalChangesNum = version_vector:version_diff(LocalDoc, ExternalDoc),
    LocalChangesNum = version_vector:version_diff(ExternalDoc, LocalDoc),
    {ExternalChanges, ExternalShrink} = fslogic_file_location:get_merged_changes(ExternalDoc, ExternalChangesNum),
    {LocalChanges, LocalShrink} = fslogic_file_location:get_merged_changes(LocalDoc, LocalChangesNum),

    NewSize =
        case {LocalShrink, ExternalShrink} of
            {undefined, undefined} ->
                max(LocalSize, ExternalSize);
            {_, undefined} ->
                max(LocalShrink, LocalSize);
            {undefined, _} ->
                max(ExternalShrink, ExternalSize);
            {_, _} ->
                case version_vector:replica_id_is_greater(LocalDoc, ExternalDoc) of
                    true ->
                        max(LocalShrink, LocalSize);
                    false ->
                        max(ExternalShrink, ExternalSize)
                end
        end,

    NewBlocks =
        case fslogic_blocks:invalidate(LocalChanges, ExternalChanges) of
            LocalChanges ->
                fslogic_blocks:invalidate(LocalBlocks, ExternalChanges);
            IndependentLocalChanges ->
                CommonChanges = fslogic_blocks:invalidate(LocalChanges, IndependentLocalChanges),
                IndependentExternalChanges = fslogic_blocks:invalidate(ExternalChanges, CommonChanges),
                PartiallyInvalidatedLocalBlocks = fslogic_blocks:invalidate(LocalBlocks, IndependentExternalChanges),
                case version_vector:replica_id_is_greater(LocalDoc, ExternalDoc) of
                    true ->
                        PartiallyInvalidatedLocalBlocks;
                    false ->
                        fslogic_blocks:invalidate(PartiallyInvalidatedLocalBlocks, CommonChanges)
                end
        end,

    TruncatedNewBlocks =
        case NewSize < LocalSize of
            true ->
                fslogic_blocks:consolidate(
                    fslogic_blocks:invalidate(NewBlocks, [#file_block{offset = NewSize, size = LocalSize - NewSize}])
                );
            false ->
                fslogic_blocks:consolidate(NewBlocks)
        end,

    NewDoc = version_vector:merge_location_versions(LocalDoc, ExternalDoc),
    NewDoc2 = NewDoc#document{value = NewDoc#document.value#file_location{blocks = TruncatedNewBlocks, size = NewSize}},
    {ok, _} = file_location:save(NewDoc2),
    notify_block_change_if_necessary(LocalDoc, NewDoc2),
    notify_size_change_if_necessary(LocalDoc, NewDoc2).

%%--------------------------------------------------------------------
%% @doc
%% Notify clients if blocks has changed.
%% @end
%%--------------------------------------------------------------------
-spec notify_block_change_if_necessary(file_location:doc(), file_location:doc()) -> ok.
%%notify_block_change_if_necessary(#document{value = #file_location{blocks = SameBlocks}}, %todo VFS-2132
%%    #document{value = #file_location{blocks = SameBlocks}}) ->
%%    ok;
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