%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions concerning QoS management.
%%% There are two triggers for QoS transfer:
%%%     * new qos_entry is synchronized (see qos_entry for more details) - traverse is started based on traverse_req.
%%%     * file_location of file changed - this triggers reconciliation procedure described below
%%%
%%% In general reconciliation procedure of given file is as follows:
%%% 1. check if all documents on path to space root are synced. There are 3 possible outcomes:
%%%   * some file_meta document is missing - go to 2.
%%%   * some link on path is missing - go to 4.
%%%   * everything is synced - check effective qos entries for given file and go to 6.
%%% 2. add file meta posthook for missing file - this posthook will execute this procedure from 1. for this missing file
%%% 3. check if there is any missing link between given file and missing one - if so go to 4. otherwise 5.
%%% 4. add file meta posthook for this missing link - this posthook will execute this procedure from 1.
%%%    for the file missing link was pointing to
%%% 5. check effective qos between given file and missing one
%%% 6. if there is any entry start traverse from given file
%%% @end
%%%-------------------------------------------------------------------
-module(qos_logic).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/qos.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    handle_qos_entry_change/2,
    handle_entry_delete/1,
    reconcile_qos/1, invalidate_cache_and_reconcile/1,
    report_synchronization_skipped/1,
    reevaluate_all_impossible_qos_in_space/1,
    retry_failed_files/1
]).

%% file_meta posthooks
-export([
    missing_file_meta_posthook/2,
    missing_link_posthook/3
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec handle_qos_entry_change(od_space:id(), qos_entry:doc()) -> ok.
handle_qos_entry_change(_SpaceId, #document{deleted = true} = QosEntryDoc) ->
    handle_entry_delete(QosEntryDoc);
handle_qos_entry_change(SpaceId, #document{key = QosEntryId, value = QosEntry} = QosEntryDoc) ->
    {ok, FileUuid} = qos_entry:get_file_uuid(QosEntry),
    ok = ?ok_if_exists(qos_entry_audit_log:create(QosEntryId)),
    ok = file_qos:add_qos_entry_id(SpaceId, FileUuid, QosEntryId),
    ok = qos_transfer_stats:ensure_exists(QosEntryId),
    case qos_entry:is_possible(QosEntry) of
        true ->
            {ok, AllTraverseReqs} = qos_entry:get_traverse_reqs(QosEntry),
            ok = qos_entry:remove_from_impossible_list(SpaceId, QosEntryId),
            ok = qos_traverse_req:start_applicable_traverses(QosEntryId, SpaceId, AllTraverseReqs);
        false ->
            ok = qos_entry:add_to_impossible_list(SpaceId, QosEntryId),
            ok = reevaluate_qos(QosEntryDoc)
    end.


-spec handle_entry_delete(qos_entry:id() | qos_entry:doc()) -> ok.
handle_entry_delete(QosEntryId) when is_binary(QosEntryId) ->
    {ok, QosEntryDoc} = qos_entry:get(QosEntryId),
    handle_entry_delete(QosEntryDoc);
handle_entry_delete(#document{key = QosEntryId, scope = SpaceId} = QosEntryDoc) ->
    {ok, FileUuid} = qos_entry:get_file_uuid(QosEntryDoc),
    ok = ?ok_if_not_found(file_qos:remove_qos_entry_id(SpaceId, FileUuid, QosEntryId)),
    ok = qos_entry:remove_from_impossible_list(SpaceId, QosEntryId),
    ok = qos_traverse:report_entry_deleted(QosEntryDoc),
    ok = qos_status:report_entry_deleted(SpaceId, QosEntryId),
    ok = qos_entry_audit_log:destroy(QosEntryId),
    ok = qos_transfer_stats:delete(QosEntryId).


-spec invalidate_cache_and_reconcile(file_ctx:ctx()) -> ok.
invalidate_cache_and_reconcile(FileCtx) ->
    ok = qos_bounded_cache:invalidate_on_all_nodes(file_ctx:get_space_id_const(FileCtx)),
    ok = qos_logic:reconcile_qos(FileCtx).


-spec reconcile_qos(file_ctx:ctx()) -> ok.
reconcile_qos(FileCtx) ->
    try
        reconcile_qos_insecure(FileCtx)
    catch Class:Error:Stacktrace ->
        ?critical_stacktrace("Unexpected error during qos reconciliation for file ~p: ~p",
            [file_ctx:get_logical_uuid_const(FileCtx), {Class, Error}], Stacktrace)
    end.


-spec report_synchronization_skipped(file_ctx:ctx()) -> ok.
report_synchronization_skipped(FileCtx) ->
    InodeUuid = file_ctx:get_referenced_uuid_const(FileCtx),
    case file_qos:get_effective(InodeUuid) of
        {ok, EffFileQos} ->
            LocalQosEntries = file_qos:get_locally_required_qos_entries(EffFileQos),
            {FileLogicalPath, FileCtx1} = file_ctx:get_logical_path(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
            FileGuid = file_ctx:get_logical_guid_const(FileCtx1),
            lists:foreach(fun(QosEntryId) ->
                ok = qos_entry_audit_log:report_file_synchronization_skipped(
                    QosEntryId, FileGuid, FileLogicalPath, reconciliation_already_in_progress)
            end, LocalQosEntries);
        _ ->
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% For each impossible QoS entry in given space recalculates target storages
%% and if entry is now possible to fulfill adds appropriate traverse requests.
%% @end
%%--------------------------------------------------------------------
-spec reevaluate_all_impossible_qos_in_space(od_space:id()) -> ok.
reevaluate_all_impossible_qos_in_space(SpaceId) ->
    % TODO VFS-6005 Use traverse to list and reevaluate impossible qos
    qos_entry:apply_to_all_impossible_in_space(SpaceId, fun reevaluate_qos/1).


-spec retry_failed_files(od_space:id()) -> ok.
retry_failed_files(SpaceId) ->
    qos_entry:apply_to_all_in_failed_files_list(SpaceId, fun(FileUuid) ->
        FileCtx = file_ctx:new_by_uuid(FileUuid, SpaceId),
        ok = qos_entry:remove_from_failed_files_list(SpaceId, FileUuid),
        ok = reconcile_qos(FileCtx)
    end).


%%%===================================================================
%%% file_meta posthooks
%%%===================================================================

-spec missing_file_meta_posthook(file_meta:uuid(), od_space:id()) -> ok.
missing_file_meta_posthook(FileUuid, SpaceId) ->
    % recreate file_ctx, as it can be outdated at the moment of hook execution
    FileCtx = file_ctx:new_by_uuid(FileUuid, SpaceId),
    reconcile_qos(FileCtx).


-spec missing_link_posthook(file_meta:uuid(), file_meta:name(), od_space:id()) -> ok.
missing_link_posthook(ParentUuid, MissingName, SpaceId) ->
    case file_meta_forest:get(ParentUuid, all, MissingName) of
        {ok, [#link{target = Uuid}]} ->
            % recreate file_ctx, as it can be outdated at the moment of hook execution
            reconcile_qos(file_ctx:new_by_uuid(Uuid, SpaceId));
        {error, _} ->
            % hook was triggered by one of links document synchronization, but there is still a missing one
            repeat
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec reconcile_qos_insecure(file_ctx:ctx()) -> ok.
reconcile_qos_insecure(FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case file_ctx:list_references_ctx_const(FileCtx) of
        {error, not_found} ->
            % Inode not synchronized yet, add posthook for it
            add_missing_file_meta_posthook(SpaceId, {file_meta_missing, file_ctx:get_referenced_uuid_const(FileCtx)});
        ReferencesList when is_list(ReferencesList) ->
            FinalEffQos = lists:foldl(fun(ReferenceCtx, EffQosAcc) ->
                file_qos:merge_file_qos(EffQosAcc, get_eff_qos_for_reference(ReferenceCtx))
            end, undefined, ReferencesList),
            start_traverse_for_effective_qos(FileCtx, FinalEffQos)
    end.


%% @private
-spec get_eff_qos_for_reference(file_ctx:ctx()) -> file_qos:effective_file_qos().
get_eff_qos_for_reference(ReferenceCtx) ->
    SpaceId = file_ctx:get_space_id_const(ReferenceCtx),
    ReferenceUuid = file_ctx:get_logical_uuid_const(ReferenceCtx),
    HighestSyncedFileParentUuid = case file_meta_sync_status_cache:get(SpaceId, ReferenceUuid) of
        {ok, synced} ->
            <<>>;
        {error, {file_meta_missing, _} = MissingElement} ->
            handle_missing_file_meta(ReferenceCtx, MissingElement);
        {error, {link_missing, _, _} = MissingElement} ->
            handle_missing_link(ReferenceCtx, MissingElement)
    end,
    case HighestSyncedFileParentUuid of
        ReferenceUuid ->
            undefined;
        _ ->
            {FileDoc, _} = file_ctx:get_file_doc(ReferenceCtx),
            % NOTE: calling with calculation_root_parent option will calculate effective value only for given reference
            case file_qos:get_effective(FileDoc, #{calculation_root_parent => HighestSyncedFileParentUuid}) of
                {ok, EffQos} -> EffQos;
                undefined -> undefined
            end
    end.


%% @private
-spec handle_missing_file_meta(file_ctx:ctx(), file_meta_posthooks:missing_element()) -> file_meta:uuid().
handle_missing_file_meta(FileCtx, {file_meta_missing, MissingUuid} = MissingElementFileMeta) ->
    ?debug("[~p] Missing file meta: ~p", [?MODULE, MissingUuid]),
    Uuid = file_ctx:get_logical_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    add_missing_file_meta_posthook(SpaceId, MissingElementFileMeta),
    MissingUuid =/= Uuid andalso
        case file_meta_sync_status_cache:get(SpaceId, Uuid, #{calculation_root_parent => MissingUuid}) of
            {ok, synced} -> ok;
            {error, MissingElementLink} -> add_missing_link_posthook(SpaceId, MissingElementLink)
        end,
    MissingUuid.


%% @private
-spec handle_missing_link(file_ctx:ctx(), file_meta_posthooks:missing_element()) -> file_meta:uuid().
handle_missing_link(FileCtx, {link_missing, MissingParentUuid, MissingLinkName} = MissingElement) ->
    % Because of file_meta_sync_status_cache there is guarantee that all file_meta documents on path are synced,
    % as well as there is no another missing link between given file and given missing one.
    ?debug("[~p] Missing link: ~p", [?MODULE, {MissingParentUuid, MissingLinkName}]),
    add_missing_link_posthook(file_ctx:get_space_id_const(FileCtx), MissingElement),
    MissingParentUuid.


%% @private
-spec start_traverse_for_effective_qos(file_ctx:ctx(), file_qos:effective_file_qos() | undefined) -> ok.
start_traverse_for_effective_qos(_FileCtx, undefined) ->
    ok;
start_traverse_for_effective_qos(FileCtx, EffFileQos) ->
    case file_qos:is_in_trash(EffFileQos) of
        false ->
            {StorageId, FileCtx2} = file_ctx:get_storage_id(FileCtx),
            QosEntriesToUpdate = file_qos:get_assigned_entries_for_storage(EffFileQos, StorageId),
            ok = qos_traverse:start(FileCtx2, QosEntriesToUpdate, datastore_key:new());
        true ->
            LocalQosEntries = file_qos:get_locally_required_qos_entries(EffFileQos),
            {FileLogicalPath, FileCtx1} = file_ctx:get_logical_path(FileCtx, user_ctx:new(?ROOT_SESS_ID)),
            FileGuid = file_ctx:get_logical_guid_const(FileCtx1),
            lists:foreach(fun(QosEntryId) ->
                ok = qos_entry_audit_log:report_file_synchronization_skipped(
                    QosEntryId, FileGuid, FileLogicalPath, file_deleted_locally)
            end, LocalQosEntries)
    end.


%% @private
-spec add_missing_link_posthook(od_space:id(), file_meta_posthooks:missing_element()) -> ok.
add_missing_link_posthook(SpaceId, {link_missing, ParentUuid, MissingName} = MissingElement) ->
    ok = file_meta_posthooks:add_hook(MissingElement, <<"qos_missing_link_", MissingName/binary>>,
        SpaceId, ?MODULE, missing_link_posthook, [ParentUuid, MissingName, SpaceId]).


%% @private
-spec add_missing_file_meta_posthook(od_space:id(), file_meta_posthooks:missing_element()) -> ok.
add_missing_file_meta_posthook(SpaceId, {file_meta_missing, MissingUuid} = MissingElement) ->
    ok = file_meta_posthooks:add_hook(MissingElement, <<"qos_missing_file_meta">>,
        SpaceId, ?MODULE, missing_file_meta_posthook, [MissingUuid, SpaceId]).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Recalculates target storages for given QoS entry and if it is now possible
%% to fulfill adds appropriate traverse requests.
%% @end
%%--------------------------------------------------------------------
-spec reevaluate_qos(qos_entry:id() | qos_entry:doc()) -> ok.
reevaluate_qos(#document{key = QosEntryId} = QosEntryDoc) ->
    {ok, FileGuid} = qos_entry:get_file_guid(QosEntryDoc),
    {ok, ReplicasNum} = qos_entry:get_replicas_num(QosEntryDoc),
    {ok, QosExpression} = qos_entry:get_expression(QosEntryDoc),
    
    FileCtx = file_ctx:new_by_guid(FileGuid),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    
    case qos_expression:try_assigning_storages(SpaceId, QosExpression, ReplicasNum) of
        {true, AssignedStorages} ->
            AllTraverseReqs = qos_traverse_req:build_traverse_reqs(
                file_ctx:get_logical_uuid_const(FileCtx), AssignedStorages
            ),
            qos_entry:mark_possible(QosEntryId, SpaceId, AllTraverseReqs),
            qos_traverse_req:start_applicable_traverses(
                QosEntryId, SpaceId, AllTraverseReqs
            );
        false -> ok
    end;
reevaluate_qos(QosEntryId) when is_binary(QosEntryId) ->
    {ok, QosEntryDoc} = qos_entry:get(QosEntryId),
    reevaluate_qos(QosEntryDoc).