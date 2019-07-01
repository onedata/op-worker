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
    restore_qos_on_provider_storages/1, replicate_using_effective_qos/2]).

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

%%--------------------------------------------------------------------
%% @doc
%% Replicates file according to it's effective qos.
%% Uses qos traverse pool.
%% @end
%%--------------------------------------------------------------------
-spec replicate_using_effective_qos(file_ctx:ctx(), session:id()) -> ok.
replicate_using_effective_qos(FileCtx, SessId) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    EffQos = file_qos:get(FileUuid),

    case EffQos of
        undefined ->
            ok;
        _ ->
            TargetStorages = maps:keys(EffQos#file_qos.target_storages),
            lists:foreach(fun(QosId) ->
                qos_traverse:fulfill_qos(SessId, FileCtx, QosId, TargetStorages)
            end, EffQos#file_qos.qos_list)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Schedules file replication to all storages of this provider that are required by qos.
%% Uses qos traverse pool.
%% @end
%%--------------------------------------------------------------------
-spec restore_qos_on_provider_storages(file_ctx:ctx()) -> ok.
restore_qos_on_provider_storages(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    EffFileQos = file_qos:get(FileUuid),

    case EffFileQos of
        undefined ->
            ok;
        _ ->
            % TODO get all storages of this provider
            StorageId = oneprovider:get_id_or_undefined(),
            QosToUpdate = maps:get(StorageId, EffFileQos#file_qos.target_storages, []),
            lists:foreach(fun(QosId) ->
                qos_traverse:fulfill_qos(?ROOT_SESS_ID, FileCtx, QosId, [StorageId])
            end, QosToUpdate)
    end.

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


    % TODO: for now target storage for dir is selected here and does not change
    % in qos traverse task. Have to figure out how to choose different
    % storages for subdirs and/or file if multiple storage fulfilling qos are
    % available.
    Guid = file_ctx:get_guid_const(FileCtx),
    TargetStoragesList = qos_traverse:get_target_storages(user_ctx:get_session_id(UserCtx),
        {guid, Guid}, QosExpressionInRPN, ReplicasNum),

    case TargetStoragesList of
        {error, cannot_fulfill_qos} ->
            qos_entry:add_impossible_qos(QosId, SpaceId);
        _ ->
            create_or_update_file_qos_doc(FileCtx, QosId, TargetStoragesList),
            qos_traverse:fulfill_qos(user_ctx:get_session_id(UserCtx), FileCtx, QosId,
                TargetStoragesList)
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
