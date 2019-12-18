%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling requests concerning QoS management.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_req).
-author("Michal Cwiertnia").

-include("modules/datastore/qos.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([add_qos_entry/4, get_qos_entry/3, remove_qos_entry/3, get_effective_file_qos/2,
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
-spec add_qos_entry(user_ctx:ctx(), file_ctx:ctx(), qos_expression:raw(),
    qos_entry:replicas_num()) -> fslogic_worker:provider_response().
add_qos_entry(UserCtx, FileCtx0, Expression, ReplicasNum) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?write_metadata]
    ),
    add_qos_entry_insecure(UserCtx, FileCtx1, Expression, ReplicasNum).


%%--------------------------------------------------------------------
%% @equiv get_file_qos_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_effective_file_qos(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_effective_file_qos(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_metadata]
    ),
    get_effective_file_qos_insecure(UserCtx, FileCtx1).


%%--------------------------------------------------------------------
%% @equiv get_qos_details_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_qos_entry(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
get_qos_entry(UserCtx, FileCtx0, QosEntryId) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_metadata]
    ),
    get_qos_entry_insecure(UserCtx, FileCtx1, QosEntryId).


%%--------------------------------------------------------------------
%% @equiv remove_qos_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec remove_qos_entry(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
remove_qos_entry(UserCtx, FileCtx0, QosEntryId) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?write_metadata]
    ),
    remove_qos_entry_insecure(UserCtx, FileCtx1, QosEntryId).


%%--------------------------------------------------------------------
%% @equiv check_fulfillment_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec check_fulfillment(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
check_fulfillment(UserCtx, FileCtx0, QosEntryId) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_metadata]
    ),
    check_fulfillment_insecure(UserCtx, FileCtx1, QosEntryId).


%%%===================================================================
%%% Insecure functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new qos_entry document. Transforms expression to RPN form.
%% Delegates traverse jobs responsible for data management for given QoS entry
%% to appropriate providers. This is done by creating qos_traverse_req for
%% given QoS entry that holds information for which file and on which storage
%% traverse should be run.
%% @end
%%--------------------------------------------------------------------
-spec add_qos_entry_insecure(user_ctx:ctx(), file_ctx:ctx(), qos_expression:raw(), qos_entry:replicas_num()) ->
    fslogic_worker:provider_response().
add_qos_entry_insecure(UserCtx, FileCtx, QosExpression, ReplicasNum) ->
    case qos_expression:raw_to_rpn(QosExpression) of
        {ok, QosExpressionInRPN} ->
            % TODO: VFS-5567 for now target storage for dir is selected here and
            % does not change in qos traverse task. Have to figure out how to
            % choose different storages for subdirs and/or file if multiple storage
            % fulfilling qos are available.
            CalculatedStorages = calculate_storages(user_ctx:get_session_id(UserCtx),
                FileCtx, QosExpressionInRPN, ReplicasNum),

            case CalculatedStorages of
                {true, Storages} ->
                    add_possible_qos(FileCtx, QosExpressionInRPN, ReplicasNum, Storages);
                false ->
                    add_impossible_qos(FileCtx, QosExpressionInRPN, ReplicasNum);
                ?ERROR_INVALID_QOS_EXPRESSION ->
                    #provider_response{status = #status{code = ?EINVAL}}
            end;
        ?ERROR_INVALID_QOS_EXPRESSION ->
            #provider_response{status = #status{code = ?EINVAL}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets effective QoS for given file or directory. Returns empty effective QoS
%% if there is no QoS entry that influences file/directory.
%% @end
%%--------------------------------------------------------------------
-spec get_effective_file_qos_insecure(user_ctx:ctx(), file_ctx:ctx()) -> fslogic_worker:provider_response().
get_effective_file_qos_insecure(_UserCtx, FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    case file_qos:get_effective(FileUuid) of
        {ok, EffQos} ->
            #provider_response{
                status = #status{code = ?OK},
                provider_response = #effective_file_qos{
                    qos_entries = file_qos:get_qos_entries(EffQos),
                    assigned_entries = file_qos:get_assigned_entries(EffQos)
                }
            };
        undefined ->
            #provider_response{
                status = #status{code = ?OK},
                provider_response = #effective_file_qos{}
            };
        {error, _} ->
            #provider_response{status = #status{code = ?EAGAIN}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets details about QoS entry.
%% @end
%%--------------------------------------------------------------------
-spec get_qos_entry_insecure(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
get_qos_entry_insecure(_UserCtx, _FileCtx, QosEntryId) ->
    case qos_entry:get(QosEntryId) of
        {ok, #document{value = QosEntry}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = QosEntry};
        {error, _} ->
            #provider_response{status = #status{code = ?EAGAIN}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes qos_entry ID from file_qos documents then removes qos_entry document.
%% @end
%%--------------------------------------------------------------------
-spec remove_qos_entry_insecure(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
remove_qos_entry_insecure(_UserCtx, FileCtx, QosEntryId) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),

    % TODO: VFS-5567 For now QoS entry is added only for file or dir
    % for which it has been added, so starting traverse is not needed.
    try
        ok = file_qos:remove_qos_entry_id(FileUuid, QosEntryId),
        SpaceId = file_ctx:get_space_id_const(FileCtx),
        ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
        ok = qos_entry:delete(QosEntryId),
        #provider_response{status = #status{code = ?OK}}
    catch
        _ ->
            #provider_response{status = #status{code = ?EAGAIN}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether requirements defined in given qos_entry are fulfilled.
%% @end
%%--------------------------------------------------------------------
-spec check_fulfillment_insecure(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
check_fulfillment_insecure(_UserCtx, FileCtx, QosEntryId) ->
    FulfillmentStatus = qos_status:check_fulfilment(QosEntryId, file_ctx:get_guid_const(FileCtx)),

    #provider_response{status = #status{code = ?OK}, provider_response = #qos_fulfillment{
        fulfilled = FulfillmentStatus
    }}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new qos_entry document with appropriate traverse requests.
%% @end
%%--------------------------------------------------------------------
-spec add_possible_qos(file_ctx:ctx(), qos_expression:rpn(), qos_entry:replicas_num(), [od_storage:id()]) ->
    fslogic_worker:provider_response().
add_possible_qos(FileCtx, QosExpressionInRPN, ReplicasNum, Storages) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    QosEntryId = datastore_utils:gen_key(),

    TraverseReqs = lists:foldl(fun(Storage, Acc) ->
        TaskId = datastore_utils:gen_key(),
        Acc#{TaskId => #qos_traverse_req{
            start_file_uuid = FileUuid,
            storage_id = Storage
        }}
    end, #{}, Storages),

    case qos_entry:create(SpaceId, QosEntryId, FileUuid, QosExpressionInRPN,
                          ReplicasNum, true, TraverseReqs) of
        {ok, _} ->
            % QoS cache is invalidated by each provider that should start traverse
            % task (see qos_hooks:maybe_start_traverse)
            maps:fold(fun(TaskId, #qos_traverse_req{storage_id = Storage}, _) ->
                ok = qos_hooks:maybe_start_traverse(FileCtx, QosEntryId, Storage, TaskId)
            end, ok, TraverseReqs),
            #provider_response{
                status = #status{code = ?OK},
                provider_response = #qos_entry_id{id = QosEntryId}
            };
        _ ->
            #provider_response{status = #status{code = ?EAGAIN}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new qos_entry document and adds it to impossible list.
%% @end
%%--------------------------------------------------------------------
-spec add_impossible_qos(file_ctx:ctx(), qos_expression:rpn(), qos_entry:replicas_num()) ->
    fslogic_worker:provider_response().
add_impossible_qos(FileCtx, QosExpressionInRPN, ReplicasNum) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    QosEntryId = datastore_utils:gen_key(),

    case qos_entry:create(SpaceId, QosEntryId, FileUuid, QosExpressionInRPN, ReplicasNum) of
        {ok, _} ->
            ok = file_qos:add_qos_entry_id(FileUuid, SpaceId, QosEntryId),
            ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
            ok = qos_entry:add_impossible_qos(QosEntryId, SpaceId),
            #provider_response{
                status = #status{code = ?OK},
                provider_response = #qos_entry_id{id = QosEntryId}
            };
        _ ->
            #provider_response{status = #status{code = ?EAGAIN}}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Calculate list of storage id, on which file should be present according to
%% given expression and replicas number.
%% @end
%%--------------------------------------------------------------------
-spec calculate_storages(session:id(), file_ctx:ctx(), qos_expression:rpn(),
    qos_entry:replicas_num()) -> {true, [od_storage:id()]} | false | {error, term()}.
calculate_storages(SessId, FileCtx, Expression, ReplicasNum) ->
    {FileLocationsDoc, _NewFileCtx} =  file_ctx:get_file_location_docs(FileCtx),
    ProvidersBlocks = lists:map(fun(#document{value = FileLocation}) ->
        #file_location{provider_id = ProviderId, blocks = Blocks} = FileLocation,
        TotalBlocksSize = lists:foldl(fun(#file_block{size = S}, SizeAcc) ->
            SizeAcc + S
        end, 0, Blocks),
        #{
            <<"providerId">> => ProviderId,
            <<"totalBlocksSize">> => TotalBlocksSize
        }
    end, FileLocationsDoc),

    % TODO: VFS-5574 add check if storage has enough free space
    % call using ?MODULE macro for mocking in tests
    SpaceStorages = ?MODULE:get_space_storages(SessId, FileCtx),
    qos_expression:calculate_storages(
        Expression, ReplicasNum, SpaceStorages, ProvidersBlocks
    ).


%%--------------------------------------------------------------------
%% @doc
%% Get list of storage id supporting space in which given file is stored.
%% @end
%%--------------------------------------------------------------------
-spec get_space_storages(session:id(), file_ctx:ctx()) -> [od_storage:id()].
get_space_storages(SessionId, FileCtx) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    SpaceId = file_id:guid_to_space_id(FileGuid),

    % TODO: VFS-5573 use storage qos
    {ok, ProvidersId} = space_logic:get_provider_ids(SessionId, SpaceId),
    ProvidersId.
