%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling requests operating on file qos.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_req).
-author("Michal Cwiertnia").

-include("modules/datastore/qos.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([add_qos/4, get_qos_details/3, remove_qos/3, get_file_qos/2,
    check_fulfillment/3]).

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

%%%===================================================================
%%% Insecure functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new qos item document. Transforms expression to RPN form.
%% Delegates traverse jobs responsible for data management for given QoS
%% to appropriate providers. This is done by creating qos_traverse_req for
%% given QoS that holds information for which file and on which storage
%% traverse should be run.
%% @end
%%--------------------------------------------------------------------
-spec add_qos_insecure(user_ctx:ctx(), file_ctx:ctx(), binary(), qos_entry:replicas_num()) ->
    fslogic_worker:provider_response().
add_qos_insecure(UserCtx, FileCtx, QosExpression, ReplicasNum) ->
    try
        {ok, QosExpressionInRPN} = qos_expression:transform_to_rpn(QosExpression),

        % TODO: VFS-5567 for now target storage for dir is selected here and
        % does not change in qos traverse task. Have to figure out how to
        % choose different storages for subdirs and/or file if multiple storage
        % fulfilling qos are available.
        CalculatedStorages = calculate_target_storages(user_ctx:get_session_id(UserCtx),
            FileCtx, QosExpressionInRPN, ReplicasNum),

        QosId = case CalculatedStorages of
            {ok, TargetStoragesList} ->
                add_possible_qos(FileCtx, QosExpressionInRPN, ReplicasNum, TargetStoragesList);
            {error, _} ->
                add_impossible_qos(FileCtx, QosExpressionInRPN, ReplicasNum)
        end,

        #provider_response{
            status = #status{code = ?OK},
            provider_response = #qos_id{id = QosId}
        }
    catch
        _:{badmatch, {error, Reason}} ->
            #provider_response{status = #status{code = Reason}};
        Error ->
            #provider_response{status = #status{code = Error}}
    end.


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
                qos_list = file_qos:get_qos_list(EffQos),
                target_storages = file_qos:get_target_storages(EffQos)
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
    case qos_entry:get(QosId) of
        {ok, #document{value = QosEntry}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #get_qos_resp{
                expression = qos_entry:get_expression(QosEntry),
                replicas_num = qos_entry:get_replicas_num(QosEntry),
                is_possible = qos_entry:is_possible(QosEntry)
            }};
        {error, Error} ->
            #provider_response{status = #status{code = Error}}
    end.


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
    SpaceId = file_ctx:get_space_id_const(FileCtx),

    % TODO: VFS-5567 For now QoS is added only for file or dir
    % for which QoS has been added, so starting traverse is not needed.
    try
        ok = file_qos:remove_qos_id(FileUuid, QosId),
        SpaceId = file_ctx:get_space_id_const(FileCtx),
        ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
        ok = qos_entry:delete(QosId),
        #provider_response{status = #status{code = ?OK}}
    catch
        _:{badmatch, {error, Reason}} ->
            #provider_response{status = #status{code = Reason}};
        Error ->
            #provider_response{status = #status{code = Error}}
    end.


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
%% @private
%% @doc
%% Creates new QoS document with appropriate traverse requests.
%% @end
%%--------------------------------------------------------------------
-spec add_possible_qos(file_ctx:ctx(), qos_expression:expression(), qos_entry:replicas_num(), [storage:id()]) ->
    qos_entry:id().
add_possible_qos(FileCtx, QosExpressionInRPN, ReplicasNum, TargetStoragesList) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    QosId = datastore_utils:gen_key(),

    TraverseReqs = lists:filtermap(fun(Storage) ->
        TaskId = datastore_utils:gen_key(),
        FileGuid = file_ctx:get_guid_const(FileCtx),
        case qos_hooks:maybe_start_traverse(FileCtx, QosId, FileGuid, Storage, TaskId) of
            true -> false;
            false -> {true, #qos_traverse_req{
                task_id = TaskId,
                start_file_uuid = FileUuid,
                qos_origin_file_guid = FileGuid,
                target_storage = Storage
            }}
        end
    end, TargetStoragesList),

    {ok, _} = qos_entry:create(SpaceId, QosId, FileUuid, QosExpressionInRPN,
        ReplicasNum, true, TraverseReqs),
    QosId.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new qos_entry document and adds it to impossible QoS list.
%% @end
%%--------------------------------------------------------------------
-spec add_impossible_qos(file_ctx:ctx(), qos_expression:expression(), qos_entry:replicas_num()) ->
    qos_entry:id().
add_impossible_qos(FileCtx, QosExpressionInRPN, ReplicasNum) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    QosId = datastore_utils:gen_key(),

    {ok, _} = qos_entry:create(SpaceId, QosId, FileUuid, QosExpressionInRPN, ReplicasNum, false),
    ok = file_qos:add_qos(FileUuid, SpaceId, QosId, []),
    ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
    ok = qos_entry:add_impossible_qos(QosId, SpaceId),
    QosId.

%%--------------------------------------------------------------------
%% @doc
%% Calculate list of storage id, on which file should be present according to
%% qos requirements and available storage.
%% @end
%%--------------------------------------------------------------------
-spec calculate_target_storages(session:id(), file_ctx:ctx(),
    qos_expression:expression(), pos_integer()) -> {ok, [storage:id()]} | {error, term()}.
calculate_target_storages(SessId, FileCtx, Expression, ReplicasNum) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    FileKey = {guid, FileGuid},

    {ok, FileLocations} = lfm:get_file_distribution(SessId, FileKey),

    % TODO: VFS-5574 add check if storage has enough free space
    % call using ?MODULE macro for mocking in tests
    SpaceStorages = ?MODULE:get_space_storages(SessId, FileCtx),
    qos_expression:calculate_target_storages(
        Expression, ReplicasNum, SpaceStorages, FileLocations
    ).


%%--------------------------------------------------------------------
%% @doc
%% Get list of storage id supporting space in which given file is stored.
%% @end
%%--------------------------------------------------------------------
-spec get_space_storages(session:id(), file_ctx:ctx()) -> [storage:id()].
get_space_storages(SessionId, FileCtx) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    SpaceId = file_id:guid_to_space_id(FileGuid),

    % TODO: VFS-5573 use storage qos
    {ok, ProvidersId} = space_logic:get_provider_ids(SessionId, SpaceId),
    ProvidersId.
