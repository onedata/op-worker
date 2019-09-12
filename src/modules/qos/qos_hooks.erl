%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides hooks concerning QoS.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_hooks).
-author("Michal Stanisz").

-include("modules/datastore/qos.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([
    handle_qos_entry_change/2,
    maybe_update_file_on_storage/2,
    maybe_update_file_on_storage/3,
    maybe_start_traverse/4
]).

%%--------------------------------------------------------------------
%% @doc
%% Callback called when qos_entry document is changed.
%% @end
%%--------------------------------------------------------------------
-spec handle_qos_entry_change(od_space:id(), qos_entry:doc()) -> ok.
handle_qos_entry_change(_SpaceId, #document{
    deleted = true,
    key = QosId,
    value = #qos_entry{
        file_uuid = FileUuid
    }
}) ->
    ok = file_qos:remove_qos_id(FileUuid, QosId),
    ok;
handle_qos_entry_change(SpaceId, #document{
    key = QosId,
    value = #qos_entry{
        file_uuid = FileUuid,
        traverse_reqs = TR
    }
}) ->
    file_qos:add_qos(FileUuid, SpaceId, QosId, []),
    qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
    maps:fold(fun(TaskId, #qos_traverse_req{start_file_uuid = StartFileUuid, target_storage = TS}, _) ->
        FileCtx = file_ctx:new_by_guid(file_id:pack_guid(StartFileUuid, SpaceId)),
        ok = maybe_start_traverse(FileCtx, QosId, TS, TaskId)
    end, ok, TR).


%%--------------------------------------------------------------------
%% @doc
%% @equiv maybe_update_file_on_storage(file_ctx:new_by_guid(FileGuid), StorageId).
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_file_on_storage(od_space:id(), file_meta:uuid(), storage:id()) -> ok.
maybe_update_file_on_storage(SpaceId, FileUuid, StorageId) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    maybe_update_file_on_storage(file_ctx:new_by_guid(FileGuid), StorageId).


%%--------------------------------------------------------------------
%% @doc
%% Schedules file replication to given storage if it is required by QoS.
%% Uses QoS traverse pool.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_file_on_storage(file_ctx:ctx(), storage:id()) -> ok.
maybe_update_file_on_storage(FileCtx, StorageId) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    EffFileQos = file_qos:get_effective(FileUuid,
        fun(MissingUuid, _Args) ->
            delayed_hooks:add_hook(MissingUuid, <<"check_qos_", FileUuid/binary>>,
                ?MODULE, ?FUNCTION_NAME, [file_ctx:get_space_id_const(FileCtx), FileUuid, StorageId])
        end),
    case EffFileQos of
        undefined ->
            ok;
        _ ->
            QosToUpdate = file_qos:get_qos_to_update(StorageId, EffFileQos),
            lists:foreach(fun(QosId) ->
                ok = qos_traverse:restore_qos(FileCtx, QosId, StorageId)
            end, QosToUpdate)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Executed only by provider to which given storage belongs to.
%% Starts QoS traverse and updates file_qos accordingly.
%% @end
%%--------------------------------------------------------------------
-spec maybe_start_traverse(file_ctx:ctx(), qos_entry:id(), storage:id(), traverse:id()) -> ok.
maybe_start_traverse(FileCtx, QosId, Storage, TaskId) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    % TODO VFS-5573 use storage id instead of provider
    case oneprovider:get_id() of
        Storage ->
            ok = file_qos:add_qos(FileUuid, SpaceId, QosId, [Storage]),
            ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
            case file_ctx:get_file_doc_allow_not_existing(FileCtx) of
                {ok, _FileDoc, FileCtx1} ->
                    ok = qos_traverse:start_initial_traverse(FileCtx1, QosId, Storage, TaskId),
                    {ok, _} = qos_entry:mark_traverse_started(QosId, TaskId),
                    ok;
                error ->
                    % There is no need to start traverse as appropriate transfers will be started
                    % when file is finally synced. If this is directory, then each child registered
                    % delayed hook that will fulfill QoS after all its ancestors are synced.
                    {ok, _} = qos_entry:remove_traverse_req(QosId, TaskId),
                    ok
            end;
        _ ->
            ok
    end.
