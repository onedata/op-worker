%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides hooks concerning QoS management.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_hooks).
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
    reconcile_qos/1, reconcile_qos/2,
    try_assigning_storages/3,
    reevaluate_all_impossible_qos_in_space/1,
    retry_failed_files/1
]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Callback called when qos_entry document is changed.
%% @end
%%--------------------------------------------------------------------
-spec handle_qos_entry_change(od_space:id(), qos_entry:doc()) -> ok.
handle_qos_entry_change(SpaceId, #document{deleted = true, key = QosEntryId} = QosEntryDoc) ->
    {ok, FileUuid} = qos_entry:get_file_uuid(QosEntryDoc),
    ok = ?ok_if_not_found(file_qos:remove_qos_entry_id(SpaceId, FileUuid, QosEntryId)),
    handle_entry_delete(QosEntryDoc);
handle_qos_entry_change(SpaceId, #document{key = QosEntryId, value = QosEntry} = QosEntryDoc) ->
    {ok, FileUuid} = qos_entry:get_file_uuid(QosEntry),
    ok = file_qos:add_qos_entry_id(SpaceId, FileUuid, QosEntryId),
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
    ok = qos_entry:remove_from_impossible_list(SpaceId, QosEntryId),
    ok = qos_traverse:report_entry_deleted(QosEntryDoc),
    ok = qos_status:report_entry_deleted(SpaceId, QosEntryId).


%%--------------------------------------------------------------------
%% @doc
%% @equiv reconcile_qos(file_ctx:new_by_guid(FileCtx, false).
%% @end
%%--------------------------------------------------------------------
-spec reconcile_qos(file_ctx:ctx()) -> ok.
reconcile_qos(FileCtx) ->
    reconcile_qos_internal(FileCtx, []).


%%--------------------------------------------------------------------
%% @doc
%% @equiv reconcile_qos(file_ctx:new_by_guid(FileGuid, SpaceId))
%% This function is used as file_meta_posthook and recreates file_ctx
%% as previous one could be outdated.
%% @end
%%--------------------------------------------------------------------
-spec reconcile_qos(file_meta:uuid(), od_space:id()) -> ok.
reconcile_qos(FileUuid, SpaceId) ->
    FileCtx = file_ctx:new_by_guid(file_id:pack_guid(FileUuid, SpaceId)),
    reconcile_qos(FileCtx).


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Schedules file replication if it is required by effective_file_qos.
%% Uses QoS traverse pool.
%% If `ignore_missing_files` is not provided in Options, file_meta_posthook 
%% is registered for the missing file.
%% @end
%%--------------------------------------------------------------------
-spec reconcile_qos_internal(file_ctx:ctx(), [Option]) -> ok  
    when Option :: ignore_missing_files.
reconcile_qos_internal(FileCtx, Options) when is_list(Options) ->
    {StorageId, FileCtx1} = file_ctx:get_storage_id(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx1),
    SpaceId = file_ctx:get_space_id_const(FileCtx1),
    case file_qos:get_effective(FileUuid) of
        {error, {file_meta_missing, MissingUuid}} ->
            % new file_ctx will be generated when file_meta_posthook
            % will be executed (see function reconcile_qos/2).
            lists:member(ignore_missing_files, Options) orelse 
                file_meta_posthooks:add_hook(
                    MissingUuid, <<"check_qos_", FileUuid/binary>>,
                    ?MODULE, reconcile_qos, [FileUuid, SpaceId]),
            ok;
        {ok, EffFileQos} ->
            QosEntriesToUpdate = file_qos:get_assigned_entries_for_storage(EffFileQos, StorageId),
            ok = qos_traverse:reconcile_file_for_qos_entries(FileCtx1, QosEntriesToUpdate);
        undefined ->
            ok
    end.


-spec try_assigning_storages(od_space:id(), qos_expression:expression(), qos_entry:replicas_num()) ->
    {true, [storage:id()]} | false.
try_assigning_storages(SpaceId, QosExpression, ReplicasNum) ->
    {ok, SpaceStorages} = space_logic:get_all_storage_ids(SpaceId),
    AllStoragesWithParams = lists:foldl(fun(StorageId, Acc) ->
        Acc#{StorageId => storage:fetch_qos_parameters_of_remote_storage(StorageId, SpaceId)}
    end, #{}, SpaceStorages),
    
    MatchingStorages = qos_expression:filter_storages(QosExpression, AllStoragesWithParams),
    CalculatedStorages = lists_utils:random_sublist(MatchingStorages, ReplicasNum, ReplicasNum),
    case CalculatedStorages of
        L when length(L) == ReplicasNum ->
            {true, CalculatedStorages};
        _ ->
            false
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
    
    case try_assigning_storages(SpaceId, QosExpression, ReplicasNum) of
        {true, AssignedStorages} ->
            AllTraverseReqs = qos_traverse_req:build_traverse_reqs(
                file_ctx:get_uuid_const(FileCtx), AssignedStorages
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


-spec retry_failed_files(od_space:id()) -> ok.
retry_failed_files(SpaceId) ->
    qos_entry:apply_to_all_in_failed_files_list(SpaceId, fun(FileUuid) ->
        FileCtx = file_ctx:new_by_guid(file_id:pack_guid(FileUuid, SpaceId)),
        ok = qos_entry:remove_from_failed_files_list(SpaceId, FileUuid),
        ok = reconcile_qos_internal(FileCtx, [ignore_missing_files])
    end).
    