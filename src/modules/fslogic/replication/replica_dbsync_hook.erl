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
-export([on_file_location_change/2, update_local_location_replica/3]).

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
    value = #file_location{
        provider_id = ProviderId,
        file_id = FileId
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
                        fslogic_event_emitter:emit_file_attr_changed(FileCtx4, []);
                    {LocalLocation, FileCtx4} ->
                        update_local_location_replica(FileCtx4, LocalLocation, ChangedLocationDoc)
                end;
            true ->
                ok
        end
    end).

%%--------------------------------------------------------------------
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
        lesser -> update_outdated_local_location_replica(FileCtx, LocalDoc, RemoteDoc);
        concurrent -> reconcile_replicas(FileCtx, LocalDoc, RemoteDoc)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
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
    FileGuid = file_ctx:get_guid_const(FileCtx),
    ?debug("Updating outdated replica of file ~p, versions: ~p vs ~p", [FileGuid, VV1, VV2]),
    LocationDocWithNewVersion = version_vector:merge_location_versions(LocalDoc, ExternalDoc),
    Diff = version_vector:version_diff(LocalDoc, ExternalDoc),
    Changes = replica_changes:get_changes(ExternalDoc, Diff),
    case replica_invalidator:invalidate_changes(FileCtx, LocationDocWithNewVersion, Changes, NewSize, []) of
        {deleted, _FileCtx2, _} ->
            ok;
        {NewDoc, FileCtx2, ChangedBlocks} ->
            {ok, FileCtx3} = maybe_truncate_file_on_storage(FileCtx2, OldSize, NewSize),
            ok = fslogic_event_emitter:emit_file_location_changed(FileCtx3, [], ChangedBlocks), % to use notify_block_change_if_necessary when ready
            notify_size_change_if_necessary(FileCtx3, LocationDocWithNewVersion, NewDoc)
    end.

%%--------------------------------------------------------------------
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
        }} = LocalFL,
    ExternalDoc = #document{
        value = #file_location{
            version_vector = VV2,
            size = ExternalSize
        }}
) ->
    LocalBlocks = fslogic_blocks:get_blocks(LocalFL),
    FileGuid = file_ctx:get_guid_const(FileCtx),
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
            %% TODO: resolve conflicts in the same way as in file_meta and links
            case version_vector:replica_id_is_greater(LocalDoc, ExternalDoc) of
                true ->
                    skip;
                false ->
                    ExternalRename
            end
    end,

    #document{value = NewLocation} = NewDoc =
        version_vector:merge_location_versions(LocalDoc, ExternalDoc),
    NewLocatoon2 = fslogic_blocks:set_blocks(NewLocation#file_location{
        size = NewSize
    }, TruncatedNewBlocks),
    NewDoc2 = NewDoc#document{value = NewLocatoon2},

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
            {ok, _} = fslogic_blocks:save_location(NewDoc2),
            notify_block_change_if_necessary(file_ctx:reset(FileCtx2), LocalDoc, NewDoc2),
            notify_size_change_if_necessary(file_ctx:reset(FileCtx2), LocalDoc, NewDoc2);
        {{renamed, RenamedDoc, Uuid, TargetSpaceId}, _} ->
            {ok, _} = fslogic_blocks:save_location(RenamedDoc),
            RenamedFileCtx =
                file_ctx:new_by_guid(fslogic_uuid:uuid_to_guid(Uuid, TargetSpaceId)),
            files_to_chown:chown_file(RenamedFileCtx),
            notify_block_change_if_necessary(RenamedFileCtx, LocalDoc, RenamedDoc),
            notify_size_change_if_necessary(RenamedFileCtx, LocalDoc, RenamedDoc)
    end.

%%--------------------------------------------------------------------
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
    ok = fslogic_event_emitter:emit_file_location_changed(FileCtx, []).

%%--------------------------------------------------------------------
%% @doc
%% Notify clients if file size has changed.
%% @end
%%--------------------------------------------------------------------
-spec notify_size_change_if_necessary(file_ctx:ctx(), file_location:doc(),
    file_location:doc()) -> ok.
notify_size_change_if_necessary(_FileCtx,
    #document{value = #file_location{size = SameSize}},
    #document{value = #file_location{size = SameSize}}
) ->
    ok;
notify_size_change_if_necessary(FileCtx, _, _) ->
    ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, []).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Truncates file on storage if its size has decreased and if storage
%% is not synced.
%% @end
%%-------------------------------------------------------------------
-spec maybe_truncate_file_on_storage(file_ctx:ctx(), non_neg_integer(),
    non_neg_integer()) -> {ok, file_ctx:ctx()}.
maybe_truncate_file_on_storage(FileCtx, OldSize, NewSize) when OldSize > NewSize ->
    {IsImportOn, FileCtx2} = file_ctx:is_import_on(FileCtx),
    case IsImportOn of
        true ->
            {ok, FileCtx2};
        false ->
            {SFMHandle, FileCtx3} = storage_file_manager:new_handle(?ROOT_SESS_ID, FileCtx2),
            case storage_file_manager:open(SFMHandle, write) of
                {ok, Handle} ->
                    ok = storage_file_manager:truncate(Handle, NewSize);
                {error, ?ENOENT} ->
                    ok
            end,
            {ok, FileCtx3}
    end;
maybe_truncate_file_on_storage(FileCtx, _OldSize, _NewSize) ->
    {ok, FileCtx}.