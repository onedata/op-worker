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

-behaviour(file_meta_posthooks_behaviour).

-include("proto/oneclient/common_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([on_file_location_change/2, on_file_location_change/3]).

%% `file_meta_posthooks_behaviour` callbacks
-export([
    encode_file_meta_posthook_args/2,
    decode_file_meta_posthook_args/2
]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handler for file_location changes which impacts local replicas.
%% @end
%%--------------------------------------------------------------------
-spec on_file_location_change(file_ctx:ctx(), file_location:doc() | undefined) ->
    ok | {error, term()}.
on_file_location_change(_FileCtx, ChangedLocationDoc) ->
    on_file_location_change(_FileCtx, ChangedLocationDoc, 0).


-spec on_file_location_change(file_ctx:ctx(), file_location:doc() | undefined, non_neg_integer()) ->
    ok | {error, term()}.
on_file_location_change(_FileCtx, undefined, _QoSCheckSizeLimit) ->
    % can happen, when called as file_meta posthook and file location document was deleted in the meantime
    ok;
on_file_location_change(FileCtx, ChangedLocationDoc = #document{
    value = #file_location{
        provider_id = ProviderId,
        file_id = FileId,
        space_id = SpaceId
    }},
    QoSCheckSizeLimit
) ->
    replica_synchronizer:apply(FileCtx, fun() ->
        case oneprovider:is_self(ProviderId) of
            false ->
                % set file_id as the same as for remote file, because
                % computing it requires parent links which may be not here yet.
                FileCtx2 = file_ctx:set_file_id(file_ctx:reset(FileCtx), FileId),
                FileCtx3 = file_ctx:set_is_dir(FileCtx2, false),
                case fslogic_cache:get_doc_including_deleted(file_location:local_id(file_ctx:get_logical_uuid_const(FileCtx3))) of
                    {error, not_found} ->
                        % If stats are enabled, force creation of local file_location doc
                        % and call the procedure again so that it triggers update_local_location_replica
                        % that internally emits events and reconciles QoS. Otherwise, do not create the
                        % location but still emit events and reconcile QoS.
                        % TODO VFS-8962 - fix getting file distribution in tests not to differentiate
                        % spaces with enabled and disabled stats
                        case dir_stats_service_state:is_active(SpaceId) of
                            true ->
                                try
                                    case fslogic_location:create_doc(FileCtx3, false, false, QoSCheckSizeLimit, true) of
                                        {{ok, #file_location{size = CreatedSize}}, FileCtx5} ->
                                            fslogic_event_emitter:emit_file_attr_changed_with_replication_status(
                                                FileCtx5, true, []),
                                            on_file_location_change(FileCtx5, ChangedLocationDoc, CreatedSize);
                                        {{error, already_exists}, FileCtx5} ->
                                            fslogic_event_emitter:emit_file_attr_changed_with_replication_status(
                                                FileCtx5, true, []),
                                            on_file_location_change(FileCtx5, ChangedLocationDoc, 0)
                                    end
                                catch
                                    throw:{error, {file_meta_missing, MissingUuid}}  ->
                                        ?debug("~tp file_meta_missing: ~tp", [?FUNCTION_NAME, MissingUuid]),
                                        file_meta_posthooks:add_hook({file_meta_missing, MissingUuid}, generator:gen_name(),
                                            SpaceId, ?MODULE, ?FUNCTION_NAME, [FileCtx, ChangedLocationDoc])
                                end;
                            false ->
                                ok = fslogic_event_emitter:emit_file_attr_changed_with_replication_status(FileCtx3, true, []),
                                qos_logic:reconcile_qos(FileCtx)
                        end;
                    #document{deleted = true} ->
                        ok;
                    LocalLocation ->
                        update_local_location_replica(FileCtx3, LocalLocation, ChangedLocationDoc, QoSCheckSizeLimit)
                end;
            true ->
                ok
        end
    end).

%%%===================================================================
%%% `file_meta_posthooks_behaviour` callbacks
%%%===================================================================

-spec encode_file_meta_posthook_args(file_meta_posthooks:function_name(), [term()]) ->
    file_meta_posthooks:encoded_args().
encode_file_meta_posthook_args(on_file_location_change, [FileCtx, LocationDoc]) ->
    Guid = file_ctx:get_logical_guid_const(FileCtx),
    #document{key = LocationId} = LocationDoc,
    term_to_binary([Guid, LocationId]).


-spec decode_file_meta_posthook_args(file_meta_posthooks:function_name(), file_meta_posthooks:encoded_args()) ->
    [term()].
decode_file_meta_posthook_args(on_file_location_change, EncodedArgs) ->
    [Guid, LocationId] = binary_to_term(EncodedArgs),
    LocationDoc = case fslogic_location_cache:get_location(LocationId, file_id:guid_to_uuid(Guid)) of
        {ok, Doc} -> Doc;
        {error, not_found} -> undefined
    end,
    [file_ctx:new_by_guid(Guid), LocationDoc].

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
    file_location:doc(), non_neg_integer()) -> ok.
update_local_location_replica(FileCtx,
    LocalDoc = #document{value = #file_location{
        version_vector = LocalVV
    }},
    RemoteDoc = #document{value = #file_location{
        version_vector = RemoteVV
    }},
    QoSCheckSizeLimit
) ->
    case version_vector:compare(LocalVV, RemoteVV) of
        identical -> ok;
        greater -> ok;
        lesser ->
            update_outdated_local_location_replica(FileCtx, LocalDoc, RemoteDoc, QoSCheckSizeLimit);
        concurrent ->
            reconcile_replicas(FileCtx, LocalDoc, RemoteDoc, QoSCheckSizeLimit)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Update local location replica according to external changes, which are
%% strictly greater in version.
%% @end
%%--------------------------------------------------------------------
-spec update_outdated_local_location_replica(file_ctx:ctx(), file_location:doc(),
    file_location:doc(), non_neg_integer()) -> ok.
update_outdated_local_location_replica(FileCtx,
    LocalDoc = #document{value = #file_location{
        size = OldSize,
        version_vector = VV1
    }},
    ExternalDoc = #document{value = #file_location{
        version_vector = VV2,
        size = NewSize
    }},
    QoSCheckSizeLimit
) ->
    FirstLocalBlocks = fslogic_location_cache:get_blocks(LocalDoc, #{count => 2}),
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    ?debug("Updating outdated replica of file ~tp, versions: ~tp vs ~tp", [FileGuid, VV1, VV2]),
    LocationDocWithNewVersion = version_vector:merge_location_versions(LocalDoc, ExternalDoc),
    Diff = version_vector:version_diff(LocalDoc, ExternalDoc),
    Changes = replica_changes:get_changes(ExternalDoc, Diff),
    case replica_invalidator:invalidate_changes(FileCtx, LocationDocWithNewVersion, Changes, NewSize, []) of
        {deleted, _FileCtx2, _} ->
            ok;
        {NewDoc, FileCtx2, ChangedBlocks} ->
            {ok, FileCtx3} = maybe_truncate_file_on_storage(FileCtx2, OldSize, NewSize),
            case file_ctx:get_file_location_with_filled_gaps(FileCtx3, ChangedBlocks) of
                {undefined, _} ->
                    ok;
                {Location, FileCtx4} ->
                    {Offset, Size} = fslogic_location_cache:get_blocks_range(Location, ChangedBlocks),
                    ok = fslogic_cache:cache_location_change([], {Location, Offset, Size}), % to use notify_block_change_if_necessary when ready
                    notify_attrs_change_if_necessary(FileCtx4, LocationDocWithNewVersion, NewDoc, FirstLocalBlocks, QoSCheckSizeLimit)
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Reconcile conflicted local replica according to external changes.
%% @end
%%--------------------------------------------------------------------
-spec reconcile_replicas(file_ctx:ctx(), file_location:doc(), file_location:doc(), non_neg_integer()) -> ok.
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
        }},
    QoSCheckSizeLimit
) ->
    LocalBlocks = fslogic_location_cache:get_blocks(LocalDoc),
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    ?debug("Conflicting changes detected on file ~tp, versions: ~tp vs ~tp", [FileGuid, VV1, VV2]),
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
    dir_size_stats:report_virtual_size_changed(file_ctx:get_referenced_guid_const(FileCtx), NewSize - LocalSize),

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
            notify_attrs_change_if_necessary(file_ctx:reset(FileCtx2), LocalDoc, NewDoc2, LocalBlocks, QoSCheckSizeLimit);
        {{renamed, RenamedDoc, Uuid, TargetSpaceId}, _} ->
            {ok, _} = fslogic_location_cache:save_location(RenamedDoc),
            RenamedFileCtx = file_ctx:new_by_uuid(Uuid, TargetSpaceId),
            files_to_chown:chown_or_defer(RenamedFileCtx),
            notify_block_change_if_necessary(RenamedFileCtx, LocalDoc, RenamedDoc),
            notify_attrs_change_if_necessary(RenamedFileCtx, LocalDoc, RenamedDoc, LocalBlocks, QoSCheckSizeLimit)
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
    case file_ctx:get_file_location_with_filled_gaps(FileCtx) of
        {undefined, _} ->
            ok;
        {Location, _FileCtx2} ->
            {Offset, Size} = fslogic_location_cache:get_blocks_range(Location),
            ok = fslogic_cache:cache_location_change([], {Location, Offset, Size})
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Notify clients if file size has changed.
%% @end
%%--------------------------------------------------------------------
-spec notify_attrs_change_if_necessary(file_ctx:ctx(), file_location:doc(),
    file_location:doc(), fslogic_blocks:blocks(), non_neg_integer()) -> ok.
notify_attrs_change_if_necessary(FileCtx,
    #document{value = #file_location{size = OldSize}},
    #document{value = #file_location{size = NewSize}} = NewDoc,
    FirstLocalBlocksBeforeUpdate,
    QoSCheckSizeLimit
) ->
    FirstLocalBlocks = fslogic_location_cache:get_blocks(NewDoc, #{count => 2}),
    case replica_updater:is_fully_replicated(FirstLocalBlocks, NewSize) of
        true ->
            ok = qos_logic:report_synchronization_skipped(FileCtx);
        false ->
            case NewSize > QoSCheckSizeLimit of
                true -> ok = qos_logic:reconcile_qos(FileCtx);
                false -> ok
            end
    end,
    ReplicaStatusChanged = replica_updater:has_replica_status_changed(
        FirstLocalBlocksBeforeUpdate, FirstLocalBlocks, OldSize, NewSize),
    case {ReplicaStatusChanged, OldSize =/= NewSize} of
        {true, SizeChanged} ->
            ok = fslogic_event_emitter:emit_file_attr_changed_with_replication_status(FileCtx, SizeChanged, []),
            ok = file_popularity:update_size(FileCtx);
        {false, true} ->
            ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, []);
        {false, false} ->
            ok
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
