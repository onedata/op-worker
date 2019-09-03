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
-export([handle_qos_entry_change/2, maybe_update_file_on_storage/2, maybe_start_traverse/5]).

%%--------------------------------------------------------------------
%% @doc
%% Callback called when qos_entry document is changed.
%% @end
%%--------------------------------------------------------------------
-spec handle_qos_entry_change(od_space:id(), qos_entry:doc()) -> ok.
handle_qos_entry_change(SpaceId, #document{
    key = QosId,
    value = #qos_entry{
        file_uuid = FileUuid,
        traverse_reqs = TR
    }
}) ->
    file_qos:add_qos(FileUuid, SpaceId, QosId, []),
    lists:foreach(fun(#qos_traverse_req{task_id = TaskId, start_file_uuid = StartFileUuid,
        qos_origin_file_guid = QosOriginFileGuid, target_storage = TS}) ->
        FileCtx = file_ctx:new_by_guid(file_id:pack_guid(StartFileUuid, SpaceId)),
        maybe_start_traverse(FileCtx, QosId, QosOriginFileGuid, TS, TaskId)
    end, TR).


%%--------------------------------------------------------------------
%% @doc
%% Schedules file replication to given storage if it is required by QoS.
%% Uses QoS traverse pool.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_file_on_storage(file_ctx:ctx(), storage:id()) -> ok.
maybe_update_file_on_storage(FileCtx, StorageId) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    DelayedHook = fun(_AncestorFileDoc) -> maybe_update_file_on_storage(FileCtx, StorageId) end,
    EffFileQos = file_qos:get_effective(FileUuid,
        fun(ParentUuid) -> delayed_hooks:add_hook(ParentUuid, DelayedHook) end),
    case EffFileQos of
        undefined ->
            ok;
        _ ->
            QosToUpdate = file_qos:get_qos_to_update(StorageId, EffFileQos),
            lists:foreach(fun(QosId) ->
                {ok, OriginGuid} = qos_entry:get_file_guid(QosId),
                ok = qos_traverse:fulfill_qos(FileCtx, QosId, OriginGuid, StorageId, restore)
            end, QosToUpdate)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Executed only by provider to which given storage belongs to.
%% Starts QoS traverse and updates file_qos accordingly.
%% Returns true if traverse was started.
%% @end
%%--------------------------------------------------------------------
-spec maybe_start_traverse(file_ctx:ctx(), qos_entry:id(), file_meta:uuid(), storage:id(), traverse:id()) ->
    boolean().
maybe_start_traverse(FileCtx, QosId, OriginFileGuid, Storage, TaskId) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    % TODO VFS-5573 use storage id instead of provider
    case oneprovider:get_id() of
        Storage ->
            ok = file_qos:add_qos(FileUuid, SpaceId, QosId, [Storage]),
            ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
            ok = qos_traverse:fulfill_qos(FileCtx, QosId, OriginFileGuid, Storage, traverse, TaskId),
            ok = qos_entry:add_traverse(SpaceId, QosId, TaskId),
            case qos_entry:remove_traverse_req(QosId, TaskId) of
                {ok, _} -> ok;
                % request is from the same provider and qos_entry is not yet created
                {error, not_found} -> ok;
                {error, _} = Error -> throw(Error)
            end,
            true;
        _ ->
            false
    end.

