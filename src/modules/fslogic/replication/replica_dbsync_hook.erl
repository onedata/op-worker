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
-module(replica_dbsync_hook).
-author("Tomasz Lichon").

-include("proto/oneclient/common_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
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
-spec on_file_location_change(file_ctx:ctx(), file_location:doc()) ->
    ok | {error, term()}.
on_file_location_change(FileCtx, ChangedLocationDoc = #document{
    key = LocId,
    value = #file_location{
        provider_id = ProviderId,
        file_id = FileId,
        space_id = SpaceId
    }}
) ->
    replica_synchronizer:apply(FileCtx, fun() ->
        case oneprovider:is_self(ProviderId) of
            false ->
                % set file_id as the same as for remote file, because
                % computing it requires parent links which may be not here yet.
                FileCtx2 = file_ctx:set_file_id(file_ctx:reset(FileCtx), FileId),
                FileCtx3 = file_ctx:set_is_dir(FileCtx2, false),
                case file_ctx:get_local_file_location_doc(FileCtx3) of
                    {undefined, FileCtx4} ->
                        % If stats are enabled, force creation of local file_location doc
                        % and call the procedure again so that it triggers update_local_location_replica
                        % that internally emits events and reconciles QoS. Otherwise, do not create the
                        % location but still emit events and reconcile QoS.
                        % TODO VFS-8962 - fix getting file distribution in tests not to differentiate
                        % spaces with enabled and disabled stats
                        case dir_stats_service_state:is_active(SpaceId) of
                            true ->
                                try
                                    case fslogic_location:create_doc(FileCtx4, false, false) of
                                        {{ok, _}, FileCtx5} ->
                                            fslogic_event_emitter:emit_file_attr_changed_with_replication_status(
                                                FileCtx5, true, []),
                                            on_file_location_change(FileCtx5, ChangedLocationDoc);
                                        {{error, already_exists}, FileCtx5} ->
                                            fslogic_event_emitter:emit_file_attr_changed_with_replication_status(
                                                FileCtx5, true, []),
                                            on_file_location_change(FileCtx5, ChangedLocationDoc)
                                    end
                                catch
                                    throw:{error, {file_meta_missing, MissingUuid}}  ->
                                        ?debug("~p file_meta_missing: ~p", [?FUNCTION_NAME, MissingUuid]),
                                        file_meta_posthooks:add_hook({file_meta_missing, MissingUuid}, LocId, SpaceId,
                                            ?MODULE, ?FUNCTION_NAME, [file_ctx:reset(FileCtx), ChangedLocationDoc])
                                end;
                            false ->
                                ok = fslogic_event_emitter:emit_file_attr_changed_with_replication_status(FileCtx4, true, []),
                                ok = qos_hooks:reconcile_qos(FileCtx4)
                        end;
                    {#document{deleted = true}, _FileCtx4} ->
                        ok;
                    {LocalLocation, FileCtx4} ->
                        update_local_location_replica(FileCtx4, LocalLocation, ChangedLocationDoc)
                end;
            true ->
                ok
        end
    end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Update local location replica according to external changes.
%% @end
%%--------------------------------------------------------------------
-spec update_local_location_replica(file_ctx:ctx(), file_location:doc(),
    file_location:doc()) -> ok.
update_local_location_replica(FileCtx,
    LocalDoc = #document{value = #file_location{
        version_vector = LocalVV
    }},
    RemoteDoc = #document{value = #file_location{
        version_vector = RemoteVV
    }}
) ->
    case version_vector:compare(LocalVV, RemoteVV) of
        identical -> ok;
        greater -> ok;
        lesser ->
            update_outdated_local_location_replica(FileCtx, LocalDoc, RemoteDoc);
        concurrent ->
            reconcile_replicas(FileCtx, LocalDoc, RemoteDoc)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Update local location replica according to external changes, which are
%% strictly greater in version.
%% @end
%%--------------------------------------------------------------------
-spec update_outdated_local_location_replica(file_ctx:ctx(), file_location:doc(),
    file_location:doc()) -> ok.
update_outdated_local_location_replica(FileCtx,
    LocalDoc = #document{value = #file_location{
        size = OldSize,
        version_vector = VV1
    }},
    ExternalDoc = #document{value = #file_location{
        version_vector = VV2,
        size = NewSize
    }}
) ->
    FirstLocalBlocks = fslogic_location_cache:get_blocks(LocalDoc, #{count => 2}),
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    ?debug("Updating outdated replica of file ~p, versions: ~p vs ~p", [FileGuid, VV1, VV2]),
    LocationDocWithNewVersion = version_vector:merge_location_versions(LocalDoc, ExternalDoc),
    Diff = version_vector:version_diff(LocalDoc, ExternalDoc),
    Changes = replica_changes:get_changes(ExternalDoc, Diff),
    case replica_invalidator:invalidate_changes(FileCtx, LocationDocWithNewVersion, Changes, NewSize, []) of
        {deleted, _FileCtx2, _} ->
            ok;
        {NewDoc, FileCtx2, ChangedBlocks} ->
            {ok, FileCtx3} = maybe_truncate_file_on_storage(FileCtx2, OldSize, NewSize),
            {Location, FileCtx4} = file_ctx:get_file_location_with_filled_gaps(FileCtx3, ChangedBlocks),
            {Offset, Size} = fslogic_location_cache:get_blocks_range(Location, ChangedBlocks),
            ok = fslogic_cache:cache_location_change([], {Location, Offset, Size}), % to use notify_block_change_if_necessary when ready
            notify_attrs_change_if_necessary(FileCtx4, LocationDocWithNewVersion, NewDoc, FirstLocalBlocks)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Reconcile conflicted local replica according to external changes.
%% @end
%%--------------------------------------------------------------------
-spec reconcile_replicas(file_ctx:ctx(), file_location:doc(), file_location:doc()) -> ok.
reconcile_replicas(FileCtx,
    LocalDoc = #document{
        value = #file_location{
            uuid = Uuid,
            version_vector = VV1,
            size = LocalSize
        } = LocalFL},
    ExternalDoc = #document{
        value = #file_location{
            version_vector = VV2,
            size = ExternalSize
        }}
) ->
    LocalBlocks = fslogic_location_cache:get_blocks(LocalDoc),
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    ?debug("Conflicting changes detected on file ~p, versions: ~p vs ~p", [FileGuid, VV1, VV2]),
    ExternalChangesNum = version_vector:version_diff(LocalDoc, ExternalDoc),
    LocalChangesNum = version_vector:version_diff(ExternalDoc, LocalDoc),
    {ExternalChanges, ExternalShrink, ExternalRename} =
        replica_changes:get_merged_changes(ExternalDoc, ExternalChangesNum),
    {LocalChanges, LocalShrink, LocalRename} =
        replica_changes:get_merged_changes(LocalDoc, LocalChangesNum),

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
                CommonChanges =
                    fslogic_blocks:invalidate(LocalChanges, IndependentLocalChanges),
                IndependentExternalChanges =
                    fslogic_blocks:invalidate(ExternalChanges, CommonChanges),
                PartiallyInvalidatedLocalBlocks =
                    fslogic_blocks:invalidate(LocalBlocks, IndependentExternalChanges),
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
                    fslogic_blocks:invalidate(NewBlocks, [#file_block{
                        offset = NewSize,
                        size = LocalSize - NewSize
                    }])
                );
            false ->
                fslogic_blocks:consolidate(NewBlocks)
        end,

    Rename = case {LocalRename, ExternalRename} of
        {undefined, undefined} ->
            skip;
        {_, undefined} ->
            skip;
        {undefined, _} ->
            ExternalRename;
        {{_, LocalNum}, {_, ExternalNum}} when LocalNum > ExternalNum ->
            skip;
        {{_, LocalNum}, {_, ExternalNum}} when LocalNum < ExternalNum ->
            ExternalRename;
        {{_, LocalNum}, {_, ExternalNum}} when LocalNum =:= ExternalNum ->
            case version_vector:replica_id_is_greater(LocalDoc, ExternalDoc) of
                true ->
                    skip;
                false ->
                    ExternalRename
            end
    end,

    NewDoc = version_vector:merge_location_versions(
        LocalDoc#document{value = LocalFL#file_location{
            size = NewSize
        }}, ExternalDoc),
    NewDoc2 = fslogic_location_cache:set_blocks(NewDoc, TruncatedNewBlocks),
    dir_size_stats:report_reg_file_size_changed(file_ctx:get_referenced_guid_const(FileCtx), total, NewSize - LocalSize),

    RenameResult = case Rename of
        skip ->
            {skipped, FileCtx};
        Rename ->
            replica_changes:rename_or_delete(FileCtx, NewDoc2, Rename)
    end,

    case RenameResult of
        {deleted, _} ->
            ok;
        {skipped, FileCtx2} ->
            {ok, _} = fslogic_location_cache:save_location(NewDoc2),
            notify_block_change_if_necessary(file_ctx:reset(FileCtx2), LocalDoc, NewDoc2),
            notify_attrs_change_if_necessary(file_ctx:reset(FileCtx2), LocalDoc, NewDoc2, LocalBlocks);
        {{renamed, RenamedDoc, Uuid, TargetSpaceId}, _} ->
            {ok, _} = fslogic_location_cache:save_location(RenamedDoc),
            RenamedFileCtx = file_ctx:new_by_uuid(Uuid, TargetSpaceId),
            files_to_chown:chown_or_defer(RenamedFileCtx),
            notify_block_change_if_necessary(RenamedFileCtx, LocalDoc, RenamedDoc),
            notify_attrs_change_if_necessary(RenamedFileCtx, LocalDoc, RenamedDoc, LocalBlocks)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Notify clients if blocks has changed.
%% @end
%%--------------------------------------------------------------------
-spec notify_block_change_if_necessary(file_ctx:ctx(), file_location:doc(),
    file_location:doc()) -> ok.
%%notify_block_change_if_necessary(#document{value = #file_location{blocks = SameBlocks}}, %todo VFS-2132
%% %TODO VFS-4412 use fslogic_blocks to get blocks from document
%%    #document{value = #file_location{blocks = SameBlocks}}) ->
%%    ok;
notify_block_change_if_necessary(FileCtx, _, _) ->
    {Location, _FileCtx2} = file_ctx:get_file_location_with_filled_gaps(FileCtx),
    {Offset, Size} = fslogic_location_cache:get_blocks_range(Location),
    ok = fslogic_cache:cache_location_change([], {Location, Offset, Size}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Notify clients if file size has changed.
%% @end
%%--------------------------------------------------------------------
-spec notify_attrs_change_if_necessary(file_ctx:ctx(), file_location:doc(),
    file_location:doc(), fslogic_blocks:blocks()) -> ok.
notify_attrs_change_if_necessary(FileCtx,
    #document{value = #file_location{size = OldSize}},
    #document{value = #file_location{size = NewSize}} = NewDoc,
    FirstLocalBlocksBeforeUpdate
) ->
    FirstLocalBlocks = fslogic_location_cache:get_blocks(NewDoc, #{count => 2}),
    ReplicaStatusChanged = replica_updater:has_replica_status_changed(
        FirstLocalBlocksBeforeUpdate, FirstLocalBlocks, OldSize, NewSize),
    case {ReplicaStatusChanged, OldSize =/= NewSize} of
        {true, SizeChanged} ->
            ok = fslogic_event_emitter:emit_file_attr_changed_with_replication_status(FileCtx, SizeChanged, []),
            ok = qos_hooks:reconcile_qos(FileCtx),
            ok = file_popularity:update_size(FileCtx);
        {false, true} ->
            ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, []),
            ok = qos_hooks:report_synchronization_skipped(FileCtx);
        {false, false} ->
            ok = qos_hooks:report_synchronization_skipped(FileCtx)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Truncates file on storage if its size has changed and if storage
%% is not synced.
%% @end
%%-------------------------------------------------------------------
-spec maybe_truncate_file_on_storage(file_ctx:ctx(), non_neg_integer(),
    non_neg_integer()) -> {ok, file_ctx:ctx()}.
maybe_truncate_file_on_storage(FileCtx, OldSize, NewSize) when OldSize =/= NewSize ->
    {IsImportedStorage, FileCtx2} = file_ctx:is_imported_storage(FileCtx),
    case IsImportedStorage of
        true ->
            {ok, FileCtx2};
        false ->
            case file_ctx:is_readonly_storage(FileCtx2) of
                {true, FileCtx3} ->
                    {ok, FileCtx3};
                {false, FileCtx3} ->
                    % Spawn file truncate on storage to prevent replica_synchronizer blocking.
                    % Although spawning file truncate on storage can result in races,
                    % similar races are possible truncating file via oneclient. Thus,
                    % method of truncating file that does not block synchronizer is preferred.
                    spawn(fun() ->
                        {SDHandle, _FileCtx4} = storage_driver:new_handle(?ROOT_SESS_ID, FileCtx3),
                        case storage_driver:open(SDHandle, write) of
                            {ok, Handle} ->
                                ok = storage_driver:truncate(Handle, NewSize, OldSize);
                            {error, ?ENOENT} ->
                                ok
                        end
                    end),
                    {ok, FileCtx3}
            end
    end;
maybe_truncate_file_on_storage(FileCtx, _OldSize, _NewSize) ->
    {ok, FileCtx}.
