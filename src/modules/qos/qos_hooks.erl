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
-export([maybe_update_file_on_storage/2, maybe_update_file_on_storage/3, maybe_start_traverse/4]).

%%--------------------------------------------------------------------
%% @doc
%% Schedules file replication to given storage if it is required by QoS.
%% Uses QoS traverse pool.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_file_on_storage(file_ctx:ctx(), storage:id()) -> ok.
maybe_update_file_on_storage(FileCtx, StorageId) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    EffFileQos = file_qos:get_effective(FileUuid, #hook{
        module = ?MODULE,
        function = maybe_update_file_on_storage,
        args = [SpaceId, FileUuid, StorageId]
    }),
    case EffFileQos of
        undefined ->
            ok;
        _ ->
            QosToUpdate = maps:get(StorageId, EffFileQos#file_qos.target_storages, []),
            lists:foreach(fun(QosId) ->
                {ok, _} = qos_traverse:fulfill_qos(FileCtx, QosId, [StorageId],
                    ?QOS_TRAVERSE_TASK_ID(SpaceId, QosId, restore))
            end, QosToUpdate)
    end.


%%--------------------------------------------------------------------
%% @doc
%% @equiv maybe_update_file_on_storage(file_ctx:new_by_guid(file_id:pack_guid(FileUuid, SpaceId)), StorageId).
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_file_on_storage(od_space:id(), file_meta:uuid(), storage:id()) -> ok.
maybe_update_file_on_storage(SpaceId, FileUuid, StorageId) ->
    maybe_update_file_on_storage(file_ctx:new_by_guid(file_id:pack_guid(FileUuid, SpaceId)), StorageId).


%%--------------------------------------------------------------------
%% @doc
%% Executed only by provider to which given storage belongs to.
%% Starts QoS traverse and updates file_qos accordingly.
%% Returns true if traverse was started.
%% @end
%%--------------------------------------------------------------------
-spec maybe_start_traverse(file_ctx:ctx(), qos_entry:id(), storage:id(), traverse:id()) -> boolean().
maybe_start_traverse(FileCtx, QosId, Storage, TaskId) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    % TODO VFS-5573 use storage id instead of provider
    case oneprovider:get_id() of
        Storage ->
            ok = file_qos:add_qos(FileUuid, SpaceId, QosId, [Storage]),
            ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
            {ok, _} = qos_traverse:fulfill_qos(FileCtx, QosId, [Storage], TaskId),
            true;
        _ ->
            false
    end.

