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

%% API
-export([add_qos/4, get_qos_details/3, remove_qos/3, get_file_qos/2]).

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
%% @equiv get_file_qos/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_file_qos(user_ctx:ctx(), file_ctx:ctx()) -> fslogic_worker:provider_response().
get_file_qos(UserCtx, FileCtx) ->
    check_permissions:execute(
        [owner],
        [UserCtx, FileCtx],
        fun get_file_qos_insecure/2
    ).

%%--------------------------------------------------------------------
%% @equiv get_qos_details/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_qos_details(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) -> fslogic_worker:provider_response().
get_qos_details(UserCtx, FileCtx, QosId) ->
    check_permissions:execute(
        [owner],
        [UserCtx, FileCtx, QosId],
        fun get_qos_details_insecure/3
    ).

%%--------------------------------------------------------------------
%% @equiv remove_qos/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec remove_qos(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) -> fslogic_worker:provider_response().
remove_qos(UserCtx, FileCtx, QosId) ->
    check_permissions:execute(
        [owner],
        [UserCtx, FileCtx, QosId],
        fun remove_qos_insecure/3
    ).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new qos_entry document. Transforms expression to RPN form.
%% Calls add_qos_for_file/5 or add_qos_for_dir/5 appropriately.
%% @end
%%--------------------------------------------------------------------
-spec add_qos_insecure(user_ctx:ctx(), file_ctx:ctx(), binary(), qos_entry:replicas_num()) ->
    fslogic_worker:provider_response().
add_qos_insecure(UserCtx, FileCtx, QosExpression, ReplicasNum) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    QosExpressionInRPN = qos_expression:transform_to_rpn(QosExpression),
    SpaceId = file_ctx:get_space_id_const(FileCtx),

    % create qos_entry document
    QosEntryToCreate = #document{value = #qos_entry{
        expression = QosExpressionInRPN,
        replicas_num = ReplicasNum,
        file_guid = FileGuid,
        status = ?IN_PROGRESS
    }},
    {ok, #document{key = QosId}} = qos_entry:create(QosEntryToCreate, SpaceId),

    case file_ctx:is_dir(FileCtx) of
        {true, _FileCtx} ->
            add_qos_for_dir(UserCtx, FileCtx, QosId, QosExpressionInRPN, ReplicasNum);
        {false, _FileCtx} ->
            add_qos_for_file(UserCtx, FileCtx, QosId, QosExpressionInRPN, ReplicasNum)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds QoS ID to file_qos document. Starts qos_traverse task for file.
%% @end
%%--------------------------------------------------------------------
-spec add_qos_for_file(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id(),
    qos_expression:expression(), qos_entry:replicas_num()) ->
    fslogic_worker:provider_response().
add_qos_for_file(UserCtx, FileCtx, QosId, QosExpression, ReplicasNum) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    SessId = user_ctx:get_session_id(UserCtx),

    create_or_update_file_qos_doc(FileGuid, QosId, []),
    qos_traverse:fulfill_qos(SessId, FileCtx, QosId, QosExpression, ReplicasNum, undefined),

    % invalidate bounded cache
    qos_bounded_cache:invalidate_on_all_nodes(SpaceId),

    #provider_response{
        status = #status{code = ?OK},
        provider_response = #qos_id{id = QosId}
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds QoS ID to file_qos document. Starts qos_traverse task for directory.
%% @end
%%--------------------------------------------------------------------
-spec add_qos_for_dir(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id(),
    qos_expression:expression(), pos_integer()) -> fslogic_worker:provider_response().
add_qos_for_dir(UserCtx, FileCtx, QosId, QosExpression, ReplicasNum) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    SessId = user_ctx:get_session_id(UserCtx),

    % TODO: for now target storage for dir is selected here and does not change
    % in qos traverse task. Have to figure out how to choose different
    % storages for subdirs and/or file if multiple storage fulfilling qos are
    % available.
    TargetStorageList = qos_traverse:get_target_storages(
        SessId, {guid, FileGuid}, QosExpression, ReplicasNum
    ),

    case TargetStorageList of
        ?CANNOT_FULFILL_QOS ->
            create_or_update_file_qos_doc(FileGuid, QosId, []),
            {ok, _} = qos_entry:set_status(QosId, ?IMPOSSIBLE);
        TargetStorageList ->
            create_or_update_file_qos_doc(FileGuid, QosId, TargetStorageList),
            qos_traverse:fulfill_qos(
                SessId, FileCtx, QosId, QosExpression, ReplicasNum, TargetStorageList
            )
    end,

    % invalidate bounded cache
    qos_bounded_cache:invalidate_on_all_nodes(SpaceId),

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
    FileGuid = file_ctx:get_guid_const(FileCtx),

    Resp = case file_qos:get_effective(FileGuid) of
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
        file_guid = QosEntry#qos_entry.file_guid,
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
-spec remove_qos_insecure(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) -> fslogic_worker:provider_response().
remove_qos_insecure(_UserCtx, FileCtx, QosId) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    {ok, _} = file_qos:remove_qos_id(FileGuid, QosId),
    qos_traverse:remove_qos(FileCtx, QosId),
    qos_entry:delete(QosId),
    #provider_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calls file_qos:create_or_update/2 fun. Document and diff function are
%% created using QoS ID and list of target storages for that QoS.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update_file_qos_doc(fslogic_worker:file_guid(), qos_entry:id(),
    file_qos:target_storages()) -> ok.
create_or_update_file_qos_doc(FileGuid, QosId, TargetStoragesList) ->
    NewTargetStorages = file_qos:merge_storage_list_to_target_storages(
        QosId, TargetStoragesList, #{}
    ),
    NewDoc = #document{
        key = FileGuid,
        scope = file_id:guid_to_space_id(FileGuid),
        value = #file_qos{
            qos_list = [QosId],
            target_storages = NewTargetStorages
        }
    },

    Diff = fun(#file_qos{qos_list = CurrFileQos, target_storages = CurrTS}) ->
        UpdatedTS = file_qos:merge_storage_list_to_target_storages(
            QosId, TargetStoragesList, CurrTS
        ),
        {ok, #file_qos{qos_list = [QosId | CurrFileQos], target_storages = UpdatedTS}}
    end,
    {ok, _} = file_qos:create_or_update(NewDoc, Diff),
    ok.
