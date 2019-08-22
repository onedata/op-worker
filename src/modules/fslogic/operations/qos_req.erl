%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on file qos.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_req).
-author("Michal Cwiertnia").

-include("modules/datastore/qos.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([add_qos/4, get_qos_details/3, remove_qos/3, get_file_qos/2,
    check_fulfillment/3, restore_qos_on_storage/2]).

% For test purpose
-export([get_space_storages/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv add_qos/4 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec add_qos(user_ctx:ctx(), file_ctx:ctx(), binary(), qos_entry:replicas_num()) ->
    fslogic_worker:provider_response().
add_qos(UserCtx, FileCtx, Expression, ReplicasNum) ->
    check_permissions:execute(
        [owner],
        [UserCtx, FileCtx, Expression, ReplicasNum],
        fun add_qos_insecure/4
    ).

%%--------------------------------------------------------------------
%% @equiv get_file_qos_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_file_qos(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_file_qos(UserCtx, FileCtx) ->
    check_permissions:execute(
        [owner],
        [UserCtx, FileCtx],
        fun get_file_qos_insecure/2
    ).

%%--------------------------------------------------------------------
%% @equiv get_qos_details_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_qos_details(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
get_qos_details(UserCtx, FileCtx, QosId) ->
    check_permissions:execute(
        [owner],
        [UserCtx, FileCtx, QosId],
        fun get_qos_details_insecure/3
    ).

%%--------------------------------------------------------------------
%% @equiv remove_qos_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec remove_qos(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
remove_qos(UserCtx, FileCtx, QosId) ->
    check_permissions:execute(
        [owner],
        [UserCtx, FileCtx, QosId],
        fun remove_qos_insecure/3
    ).

%%--------------------------------------------------------------------
%% @equiv check_fulfillment_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec check_fulfillment(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
check_fulfillment(UserCtx, FileCtx, QosId) ->
    check_permissions:execute(
        [owner],
        [UserCtx, FileCtx, QosId],
        fun check_fulfillment_insecure/3
    ).

%%--------------------------------------------------------------------
%% @doc
%% Schedules file replication to given storage if it is required by QoS.
%% Uses QoS traverse pool.
%% @end
%%--------------------------------------------------------------------
-spec restore_qos_on_storage(file_ctx:ctx(), storage:id()) -> ok.
restore_qos_on_storage(FileCtx, StorageId) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    EffFileQos = file_qos:get_effective(FileUuid),

    case EffFileQos of
        undefined ->
            ok;
        _ ->
            QosToUpdate = maps:get(StorageId, EffFileQos#file_qos.target_storages, []),
            lists:foreach(fun(QosId) ->
                ok = qos_traverse:fulfill_qos(?ROOT_SESS_ID, FileCtx, QosId, [StorageId])
            end, QosToUpdate)
    end.

%%%===================================================================
%%% Insecure functions
%%%===================================================================
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new qos item document. Transforms expression to RPN form.
%% Starts traverse job responsible for data management for given QoS.
%% @end
%%--------------------------------------------------------------------
-spec add_qos_insecure(user_ctx:ctx(), file_ctx:ctx(), binary(), qos_entry:replicas_num()) ->
    fslogic_worker:provider_response().
add_qos_insecure(UserCtx, FileCtx, QosExpression, ReplicasNum) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    QosExpressionInRPN = qos_expression:transform_to_rpn(QosExpression),

    % create qos_entry document
    QosEntryToCreate = #document{value = #qos_entry{
        expression = QosExpressionInRPN,
        replicas_num = ReplicasNum,
        file_uuid = FileUuid,
        status = ?QOS_IN_PROGRESS_STATUS
    }},
    {ok, #document{key = QosId}} = qos_entry:create(QosEntryToCreate, SpaceId),

    % TODO: VFS-5567 for now target storage for dir is selected here and
    % does not change in qos traverse task. Have to figure out how to
    % choose different storages for subdirs and/or file if multiple storage
    % fulfilling qos are available.
    TargetStoragesList = calculate_target_storages(user_ctx:get_session_id(UserCtx),
        FileCtx, QosExpressionInRPN, ReplicasNum),

    case TargetStoragesList of
        {error, ?ERROR_CANNOT_FULFILL_QOS} ->
            ok = file_qos:add_qos(FileUuid, SpaceId, QosId, []),
            ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
            {ok, _} = qos_entry:add_impossible_qos(QosId, SpaceId);
        _ ->
            ok = file_qos:add_qos(FileUuid, SpaceId, QosId, TargetStoragesList),
            ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
            ok = qos_traverse:fulfill_qos(
                user_ctx:get_session_id(UserCtx), FileCtx, QosId, TargetStoragesList
            )
    end,

    #provider_response{
        status = #status{code = ?OK},
        provider_response = #qos_id{id = QosId}
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets effective qos for given file or directory. Returns empty effective QoS
%% if no QoS requirements are defined for file/directory.
%% @end
%%--------------------------------------------------------------------
-spec get_file_qos_insecure(user_ctx:ctx(), file_ctx:ctx()) -> fslogic_worker:provider_response().
get_file_qos_insecure(_UserCtx, FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),

    Resp = case file_qos:get_effective(FileUuid) of
        undefined ->
            #effective_file_qos{};
        EffQos ->
            #effective_file_qos{
                qos_list = EffQos#file_qos.qos_list,
                target_storages = EffQos#file_qos.target_storages
            }
    end,

    #provider_response{
        status = #status{code = ?OK},
        provider_response = Resp
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets details about QoS.
%% @end
%%--------------------------------------------------------------------
-spec get_qos_details_insecure(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
get_qos_details_insecure(_UserCtx, _FileCtx, QosId) ->
    {ok, #document{key = QosId, value = QosEntry}} = qos_entry:get(QosId),

    #provider_response{status = #status{code = ?OK}, provider_response = #get_qos_resp{
        expression = QosEntry#qos_entry.expression,
        replicas_num = QosEntry#qos_entry.replicas_num,
        status = QosEntry#qos_entry.status
    }}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes Qos ID from file_qos documents then removes qos_entry document
%% for given QoS.
%% @end
%%--------------------------------------------------------------------
-spec remove_qos_insecure(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
remove_qos_insecure(_UserCtx, FileCtx, QosId) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    % TODO: VFS-5567 For now QoS is added only for file or dir
    % for which QoS has been added, so starting traverse is not needed.
    ok = file_qos:remove_qos_id(FileUuid, QosId),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
    ok = qos_entry:delete(QosId),
    #provider_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether given QoS is fulfilled for given file.
%% @end
%%--------------------------------------------------------------------
-spec check_fulfillment_insecure(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
check_fulfillment_insecure(_UserCtx, FileCtx, QosId) ->
    FulfillmentStatus = qos_status:check_fulfilment(QosId, file_ctx:get_guid_const(FileCtx)),

    #provider_response{status = #status{code = ?OK}, provider_response = #qos_fulfillment{
        fulfilled = FulfillmentStatus
    }}.


%%%===================================================================
%%% Internal functions
%%%===================================================================
%%--------------------------------------------------------------------
%% @doc
%% Calculate list of storage id, on which file should be present according to
%% qos requirements and available storage.
%% @end
%%--------------------------------------------------------------------
-spec calculate_target_storages(session:id(), file_ctx:ctx(),
    qos_expression:expression(), pos_integer()) -> [storage:id()].
calculate_target_storages(SessId, FileCtx, Expression, ReplicasNum) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    FileKey = {guid, FileGuid},

    {ok, FileLocations} = lfm:get_file_distribution(SessId, FileKey),

    % TODO: VFS-5574 add check if storage has enough free space
    % call using ?MODULE macro for mocking in tests
    SpaceStorages = ?MODULE:get_space_storages(SessId, FileCtx),
    qos_expression:get_target_storages(
        Expression, ReplicasNum, SpaceStorages, FileLocations
    ).


%%--------------------------------------------------------------------
%% @doc
%% Get list of storage id supporting space in which given file is stored.
%% @end
%%--------------------------------------------------------------------
-spec get_space_storages(session:id(), file_ctx:ctx()) -> [storage:id()].
get_space_storages(Auth, FileCtx) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    SpaceId = file_id:guid_to_space_id(FileGuid),

    % TODO: VFS-5573 use storage qos
    {ok, ProvidersId} = space_logic:get_provider_ids(Auth, SpaceId),
    ProvidersId.
