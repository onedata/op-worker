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

%% API
-export([
    handle_qos_entry_change/2,
    reconcile_qos_on_storage/2,
    maybe_start_traverse/4
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
    maps:fold(fun(TaskId, #qos_traverse_req{start_file_uuid = StartFileUuid, storage_id = StorageId}, _) ->
        FileCtx = file_ctx:new_by_guid(file_id:pack_guid(StartFileUuid, SpaceId)),
        ok = maybe_start_traverse(FileCtx, QosEntryId, StorageId, TaskId)
    end, ok, TraverseMap).


%%--------------------------------------------------------------------
%% @doc
%% Schedules file replication to given storage if it is required by
%% effective_file_qos. Uses QoS traverse pool.
%% @end
%%--------------------------------------------------------------------
-spec reconcile_qos_on_storage(file_ctx:ctx(), storage:id()) -> ok.
reconcile_qos_on_storage(FileCtx, StorageId) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),

    case file_qos:get_effective(FileUuid) of
        {error, {file_meta_missing, MissingUuid}} ->
            % Generate new file_ctx as it can change before posthook
            % will be executed
            file_meta_posthooks:add_hook(
                MissingUuid, <<"check_qos_", FileUuid/binary>>,
                ?MODULE, ?FUNCTION_NAME, [file_ctx:new_by_guid(FileGuid), StorageId]
            ),
            ok;
        {ok, EffFileQos} ->
            QosToUpdate = file_qos:get_qos_to_update(StorageId, EffFileQos),
            lists:foreach(fun(QosEntryId) ->
                ok = qos_traverse:reconcile_qos_for_entry(FileCtx, QosEntryId)
            end, QosToUpdate);
        undefined ->
            ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Executed only by provider to which given storage belongs to.
%% Starts QoS traverse and updates file_qos accordingly.
%% @end
%%--------------------------------------------------------------------
-spec maybe_start_traverse(file_ctx:ctx(), qos_entry:id(), storage:id(), traverse:id()) -> ok.
maybe_start_traverse(FileCtx, QosEntryId, Storage, TaskId) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    % TODO VFS-5573 use storage id instead of provider
    case oneprovider:get_id() of
        Storage ->
            ok = file_qos:add_qos_entry_id(FileUuid, SpaceId, QosEntryId, Storage),
            ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
            case lookup_file_meta_doc(FileCtx) of
                {ok, FileCtx1} ->
                    ok = qos_traverse:start_initial_traverse(FileCtx1, QosEntryId, TaskId);
                {error, not_found} ->
                    % There is no need to start traverse as appropriate transfers will be started
                    % when file is finally synced. If this is directory, then each child registered
                    % file meta posthook that will fulfill QoS after all its ancestors are synced.
                    ok = qos_entry:remove_traverse_req(QosEntryId, TaskId)
            end;
        _ ->
            ok
    end.


%%%===================================================================
%%% Internal functions
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
