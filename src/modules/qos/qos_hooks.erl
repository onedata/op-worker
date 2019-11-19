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
-include("modules/datastore/qos.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    handle_qos_entry_change/2,
    reconcile_qos_on_storage/1, reconcile_qos_on_storage/2,
    check_traverse_reqs/3,
    revalidate_impossible_qos/0
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
handle_qos_entry_change(_SpaceId, #document{
    deleted = true,
    key = QosEntryId,
    value = QosEntry
}) ->
    FileUuid = qos_entry:get_file_uuid(QosEntry),
    ok = file_qos:remove_qos_entry_id(FileUuid, QosEntryId);
handle_qos_entry_change(SpaceId, #document{
    key = QosEntryId,
    value = QosEntry
}) ->
    FileUuid = qos_entry:get_file_uuid(QosEntry),
    TraverseMap = qos_entry:get_traverse_map(QosEntry),
    file_qos:add_qos_entry_id(FileUuid, SpaceId, QosEntryId),
    qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
    check_traverse_reqs(QosEntryId, SpaceId, TraverseMap).


%%--------------------------------------------------------------------
%% @doc
%% Checks whether any traverse is to be run on this provider
%% and starts appropriate traverses.
%% @end
%%--------------------------------------------------------------------
-spec check_traverse_reqs(qos_entry:id(), od_space:id(), qos_entry:traverse_map()) -> ok.
check_traverse_reqs(QosEntryId, SpaceId, TraverseReqs) ->
    maps:fold(fun(TaskId, TraverseReq, _) ->
        StorageId = qos_entry:traverse_req_get_storage_id(TraverseReq),
        StartFileUuid = qos_entry:traverse_req_get_start_file_uuid(TraverseReq),
        FileCtx = file_ctx:new_by_guid(file_id:pack_guid(StartFileUuid, SpaceId)),

        {ok, StorageProvider} = storage_logic:get_provider(StorageId, SpaceId),
        case oneprovider:get_id() of
            StorageProvider ->
                start_traverse(FileCtx, QosEntryId, StorageId, TaskId);
            _ ->
                ok
        end
    end, ok, TraverseReqs).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executed only by provider to which given storage belongs to.
%% Starts QoS traverse and updates file_qos accordingly.
%% @end
%%--------------------------------------------------------------------
-spec start_traverse(file_ctx:ctx(), qos_entry:id(), od_storage:id(), traverse:id()) -> ok.
start_traverse(FileCtx, QosEntryId, StorageId, TaskId) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    ok = file_qos:add_qos_entry_id(FileUuid, SpaceId, QosEntryId, StorageId),
    ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
    case lookup_file_meta_doc(FileCtx) of
        {ok, FileCtx1} ->
            ok = qos_traverse:start_initial_traverse(FileCtx1, QosEntryId, TaskId);
        {error, not_found} ->
            % There is no need to start traverse as appropriate transfers will be started
            % when file is finally synced. If this is directory, then each child registered
            % file meta posthook that will fulfill QoS after all its ancestors are synced.
            ok = qos_entry:remove_traverse_req(QosEntryId, TaskId)
    end.



%%--------------------------------------------------------------------
%% @doc
%% This function is used to create new file_ctx when file_meta_posthook
%% is executed.
%% @end
%%--------------------------------------------------------------------
-spec reconcile_qos_on_storage(file_meta:uuid(), od_space:id()) -> ok.
reconcile_qos_on_storage(FileUuid, SpaceId) ->
    FileCtx = file_ctx:new_by_guid(file_id:pack_guid(FileUuid, SpaceId)),
    reconcile_qos_on_storage(FileCtx).


%%--------------------------------------------------------------------
%% @doc
%% Schedules file replication to given storage if it is required by
%% effective_file_qos. Uses QoS traverse pool.
%% @end
%%--------------------------------------------------------------------
-spec reconcile_qos_on_storage(file_ctx:ctx()) -> ok.
reconcile_qos_on_storage(FileCtx) ->
    {StorageId, FileCtx1} = file_ctx:get_storage_id(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx1),
    SpaceId  = file_ctx:get_space_id_const(FileCtx1),
    case file_qos:get_effective(FileUuid) of
        {error, {file_meta_missing, MissingUuid}} ->
            % new file_ctx will be generated when file_meta_posthook
            % will be executed (see function above).
            file_meta_posthooks:add_hook(
                MissingUuid, <<"check_qos_", FileUuid/binary>>,
                ?MODULE, ?FUNCTION_NAME, [FileUuid, SpaceId]
            ),
            ok;
        {ok, EffFileQos} ->
            QosToUpdate = file_qos:get_qos_to_update(StorageId, EffFileQos),
            lists:foreach(fun(QosEntryId) ->
                ok = qos_traverse:reconcile_qos_for_entry(FileCtx1, QosEntryId)
            end, QosToUpdate);
        undefined ->
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% For each impossible QoS entry recalculates target storages and if
%% entry is now possible to fulfill adds appropriate traverse requests.
%% @end
%%--------------------------------------------------------------------
-spec revalidate_impossible_qos() -> ok.
revalidate_impossible_qos() ->
    {ok, QosList} = qos_entry:list_impossible_qos(),
    lists:foreach(fun(QosEntryId) ->
        {ok, #document{value = QosEntry} = QosDoc} = qos_entry:get(QosEntryId),
        {ok, FileGuid} = qos_entry:get_file_guid(QosDoc),
        FileCtx = file_ctx:new_by_guid(FileGuid),
        SpaceId = file_ctx:get_space_id_const(FileCtx),
        ReplicasNum = qos_entry:get_replicas_num(QosEntry),
        QosExpression = qos_entry:get_expression(QosEntry),

        case qos_expression:calculate_storages(FileCtx, QosExpression, ReplicasNum) of
            {true, StoragesList} ->
                TraverseReqs = qos_entry:prepare_traverse_map(file_ctx:get_uuid_const(FileCtx), StoragesList),
                qos_entry:mark_entry_possible(QosEntryId, SpaceId, TraverseReqs),
                % QoS cache is invalidated by each provider that should start traverse
                % task (see qos_hooks:start_traverse)
                check_traverse_reqs(QosEntryId, SpaceId, TraverseReqs);
            false -> ok
        end
    end, QosList).


%%%===================================================================
%%% Utility functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Returns file's file_meta document.
%% Returns error if there is no such file doc (it is not synced yet).
%% @end
%%--------------------------------------------------------------------
-spec lookup_file_meta_doc(file_ctx:ctx()) -> {ok, file_ctx:ctx()} | {error, not_found}.
lookup_file_meta_doc(FileCtx) ->
    try
        {_Doc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
        {ok, FileCtx2}
    catch
        _:{badmatch, {error, not_found} = Error}->
            Error
    end.
