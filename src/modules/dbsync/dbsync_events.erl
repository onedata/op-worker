%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc DBSync hooks.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_events).
-author("Rafal Slota").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([change_replicated/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for change_replicated_internal, ignoring unsupported spaces.
%% @end
%%--------------------------------------------------------------------
-spec change_replicated(SpaceId :: binary(), undefined | datastore:doc()) ->
    any().
change_replicated(_SpaceId, undefined) ->
    ok;
change_replicated(SpaceId, Change) ->
    true = dbsync_utils:is_supported(SpaceId, [oneprovider:get_id()]),
    change_replicated_internal(SpaceId, Change).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Hook that runs just after change was replicated from remote provider.
%% Return value and any errors are ignored.
%% @end
%%--------------------------------------------------------------------
-spec change_replicated_internal(od_space:id(), datastore:doc()) ->
    any() | no_return().
change_replicated_internal(SpaceId, #document{
    key = FileUuid,
    value = #file_meta{type = ?LINK_TYPE}
} = LinkDoc) ->
    ?debug("hardlink replicated ~p", [FileUuid]),
    case file_meta:get_including_deleted(fslogic_file_id:ensure_referenced_uuid(FileUuid)) of
        {ok, ReferencedDoc} ->
            {ok, MergedDoc} = file_meta_hardlinks:merge_link_and_file_doc(LinkDoc, ReferencedDoc),
            FileCtx = file_ctx:new_by_doc(MergedDoc, SpaceId),
            hardlink_replicated(LinkDoc, FileCtx);
        Error ->
            % TODO VFS-7531 - Handle dbsync events for hardlinks when referenced file_meta is missing
            ?warning("hardlink replicated ~p - posthook failed with error ~p",
                [FileUuid, Error])
    end,
    ok = file_meta_posthooks:execute_hooks(FileUuid, doc);
change_replicated_internal(SpaceId, #document{
    key = FileUuid,
    value = #file_meta{mode = CurrentMode}
} = FileDoc) ->
    ?debug("file_meta_change_replicated: ~p", [FileUuid]),
    FileCtx = file_ctx:new_by_doc(FileDoc, SpaceId),
    {ok, FileCtx2} = sd_utils:chmod(FileCtx, CurrentMode),
    file_meta_change_replicated(FileDoc, FileCtx2),
    ok = file_meta_posthooks:execute_hooks(FileUuid, doc);
change_replicated_internal(SpaceId, #document{
    deleted = false,
    value = #file_location{uuid = FileUuid}
} = Doc) ->
    ?debug("change_replicated_internal: changed file_location ~p", [FileUuid]),
    FileCtx = file_ctx:new_by_uuid(FileUuid, SpaceId),
    ok = replica_dbsync_hook:on_file_location_change(FileCtx, Doc);
change_replicated_internal(SpaceId, #document{
    key = FileUuid,
    value = #times{} = Record,
    deleted = true
}) ->
    ?debug("change_replicated_internal: deleted times ~p", [FileUuid]),
    FileCtx = file_ctx:new_by_uuid(FileUuid, SpaceId),
    dir_update_time_stats:report_update_of_nearest_dir(file_ctx:get_logical_guid_const(FileCtx), Record),
    % Emmit event in case of changed times / deleted file_meta propagation race
    (catch fslogic_event_emitter:emit_file_removed(FileCtx, []));
change_replicated_internal(SpaceId, #document{
    key = FileUuid,
    value = #times{} = Record
}) ->
    ?debug("change_replicated_internal: changed times ~p", [FileUuid]),
    FileCtx = file_ctx:new_by_uuid(FileUuid, SpaceId),
    dir_update_time_stats:report_update_of_nearest_dir(file_ctx:get_logical_guid_const(FileCtx), Record),
    (catch fslogic_event_emitter:emit_sizeless_file_attrs_changed(FileCtx));
change_replicated_internal(_SpaceId, #document{
    key = FileUuid,
    value = #custom_metadata{}
}) ->
    ?debug("change_replicated_internal: changed custom_metadata ~p", [FileUuid]);
change_replicated_internal(_SpaceId, Transfer = #document{
    key = TransferId,
    value = #transfer{}
}) ->
    ?debug("change_replicated_internal: changed transfer ~p", [TransferId]),
    transfer_changes:handle(Transfer);
change_replicated_internal(_SpaceId, ReplicaDeletion = #document{
    key = MsgId,
    value = #replica_deletion{}
}) ->
    ?debug("change_replicated_internal: changed replica_deletion ~p", [MsgId]),
    replica_deletion_changes:handle(ReplicaDeletion);
change_replicated_internal(_SpaceId, Index = #document{
    key = IndexId,
    value = #index{}
}) ->
    ?debug("change_replicated_internal: changed index ~p", [IndexId]),
    view_changes:handle(Index);
change_replicated_internal(_SpaceId, #document{value = #traverse_task{}} = Task) ->
    traverse:on_task_change(Task, oneprovider:get_id_or_undefined());
change_replicated_internal(_SpaceId, #document{key = JobId, value = #tree_traverse_job{}} = Doc) ->
    case tree_traverse:get_job(Doc) of
        {ok, Job, PoolName, TaskId} ->
            traverse:on_job_change(Job, JobId, PoolName, TaskId, oneprovider:get_id_or_undefined());
        ?ERROR_NOT_FOUND ->
            % TODO VFS-6391 fix race with file_meta
            ok
    end;
change_replicated_internal(SpaceId, QosEntry = #document{
    key = QosEntryId,
    value = #qos_entry{}
}) ->
    ?debug("change_replicated_internal: qos_entry ~p", [QosEntryId]),
    qos_logic:handle_qos_entry_change(SpaceId, QosEntry);
change_replicated_internal(SpaceId, ArchiveRecallDetails = #document{
    key = RecallId,
    value = #archive_recall_details{}
}) ->
    ?debug("change_replicated_internal: archive_recall_details ~p", [RecallId]),
    archive_recall_details:handle_remote_change(SpaceId, ArchiveRecallDetails);
change_replicated_internal(SpaceId, DatasetDoc = #document{
    key = DatasetId,
    value = #dataset{}
}) ->
    ?debug("change_replicated_internal: dataset ~p", [DatasetId]),
    dataset_api:handle_remote_change(SpaceId, DatasetDoc);
change_replicated_internal(SpaceId, #document{value = #links_forest{key = LinkKey, model = Model}}) ->
    ?debug("change_replicated_internal: links_forest ~p", [LinkKey]),
   link_replicated(Model, LinkKey, SpaceId);
change_replicated_internal(SpaceId, #document{value = #links_node{key = LinkKey, model = Model}}) ->
    ?debug("change_replicated_internal: links_node ~p", [LinkKey]),
   link_replicated(Model, LinkKey, SpaceId);
change_replicated_internal(SpaceId, #document{value = #links_mask{key = LinkKey, model = Model}}) ->
    ?debug("change_replicated_internal: links_mask ~p", [LinkKey]),
   link_replicated(Model, LinkKey, SpaceId);
change_replicated_internal(_SpaceId, _Change) ->
    ok.


%% @private
-spec hardlink_replicated(datastore:doc(), file_ctx:ctx()) -> ok | no_return().
hardlink_replicated(#document{
    value = #file_meta{deleted = Del1},
    deleted = Del2
}, FileCtx) when Del1 or Del2 ->
    fslogic_delete:handle_remotely_deleted_file(FileCtx);
hardlink_replicated(_, FileCtx) ->
    % TODO VFS-7914 - Do not invalidate cache, when it is not needed
    ok = qos_logic:invalidate_cache_and_reconcile(FileCtx),
    ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, []).


%% @private
-spec file_meta_change_replicated(datastore:doc(), file_ctx:ctx()) -> ok | no_return().
file_meta_change_replicated(#document{
    value = #file_meta{deleted = Del1},
    deleted = Del2
}, FileCtx) when Del1 or Del2 ->
    fslogic_delete:handle_remotely_deleted_file(FileCtx);
file_meta_change_replicated(_, FileCtx) ->
    ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, []).


%% @private
-spec link_replicated(module(), datastore:key(), od_space:id()) ->
    any() | no_return().
link_replicated(file_meta, LinkKey, SpaceId) ->
    dir_size_stats:report_remote_links_change(LinkKey, SpaceId),
    case datastore_model:get_generic_key(file_meta, LinkKey) of
        undefined ->
            % Legacy keys are not supported as it is impossible to retrieve GenericKey
            ok;
        GenericKey ->
            file_meta_posthooks:execute_hooks(GenericKey, link)
    end;
link_replicated(_Model, _LinkKey_, _SpaceId) ->
    ok.
