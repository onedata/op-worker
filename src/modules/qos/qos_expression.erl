%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains functions operating on QoS expressions.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_expression).
-author("Michal Cwiertnia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/qos.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([raw_to_rpn/1, calculate_storages/3]).


% For test purpose
-export([get_space_storages/1]).

% the raw type stores expression as single binary. It is used to store input
% from user. In the process of adding new qos_entry raw expression is
% parsed to rpn form (list of key-value binaries separated by operators)
-type raw() :: binary(). % e.g. <<"country=FR&type=disk">>
-type rpn() :: [binary()]. % e.g. [<<"country=FR">>, <<"type=disk">>, <<"&">>]

-type operator_stack() :: [operator_or_paren()].
-type operand_stack() :: [binary()].
-type operator_or_paren() :: operator() | paren().
-type paren() :: binary().
-type operator() :: binary().

-export_type([rpn/0, raw/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Transforms QoS expression from infix notation to reverse polish notation.
%% % TODO: VFS-5569 improve handling invalid QoS expressions
%% @end
%%--------------------------------------------------------------------
-spec raw_to_rpn(raw()) -> {ok, rpn()} | ?ERROR_INVALID_QOS_EXPRESSION.
raw_to_rpn(Expression) ->
    OperatorsBin = <<?UNION/binary, ?INTERSECTION/binary, ?COMPLEMENT/binary>>,
    ParensBin = <<?L_PAREN/binary, ?R_PAREN/binary>>,
    Tokens = re:split(Expression, <<"([", ParensBin/binary, OperatorsBin/binary, "])">>),
    try
        {ok, transform_to_rpn_internal(Tokens, [], [])}
    catch
        throw:?ERROR_INVALID_QOS_EXPRESSION ->
            ?ERROR_INVALID_QOS_EXPRESSION
    end.


%%--------------------------------------------------------------------
%% @doc
%% Calculate list of storage id, on which file should be present according to
%% given expression and replicas number.
%% @end
%%--------------------------------------------------------------------
-spec calculate_storages(file_ctx:ctx(), qos_expression:rpn(), qos_entry:replicas_num()) ->
    {true, [od_storage:id()]} | false | {error, term()}.
calculate_storages(FileCtx, Expression, ReplicasNum) ->
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
    SpaceStorages = ?MODULE:get_space_storages(FileCtx),
    calculate_storages(
        Expression, ReplicasNum, SpaceStorages, ProvidersBlocks, file_ctx:get_space_id_const(FileCtx)
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calculate list of storages on which file should be replicated according to given
%% QoS expression and number of replicas.
%% Takes into consideration actual file locations.
%% @end
%%--------------------------------------------------------------------
-spec calculate_storages(rpn(), pos_integer(), [od_storage:id()], [#file_location{}], od_space:id()) ->
    {ture, [od_storage:id()]} | false | ?ERROR_INVALID_QOS_EXPRESSION.
calculate_storages(_Expression, _ReplicasNum, [], _FileLocations, _SpaceId) ->
    false;
calculate_storages(Expression, ReplicasNum, SpaceStorages, FileLocations, SpaceId) ->
    % TODO: VFS-5734 choose storages for dirs according to current files distribution
    try
        StorageList = eval_rpn(Expression, SpaceStorages, SpaceId),
        select(StorageList, ReplicasNum, FileLocations, SpaceId)
    catch
        throw:?ERROR_INVALID_QOS_EXPRESSION ->
            ?ERROR_INVALID_QOS_EXPRESSION
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% internal version of transform_to_rpn/2
%% @end
%%--------------------------------------------------------------------
-spec transform_to_rpn_internal([binary()], operator_stack(), rpn()) -> rpn().
transform_to_rpn_internal([<<>>], [], []) ->
    [];
transform_to_rpn_internal([<<>> | Expression], Stack, RPNExpression) ->
    transform_to_rpn_internal(Expression, Stack, RPNExpression);
transform_to_rpn_internal([], Stack, RPNExpression) ->
    RPNExpression ++ Stack;
transform_to_rpn_internal([Operator | Expression], Stack, RPNExpression) when
        Operator =:= ?INTERSECTION orelse
        Operator =:= ?UNION orelse
        Operator =:= ?COMPLEMENT ->
    {Stack2, RPNExpression2} = handle_operator(Operator, Stack, RPNExpression),
    transform_to_rpn_internal(Expression, Stack2, RPNExpression2);
transform_to_rpn_internal([?L_PAREN | Expression], Stack, RPNExpression) ->
    transform_to_rpn_internal(Expression, [?L_PAREN|Stack], RPNExpression);
transform_to_rpn_internal([?R_PAREN | Expression], Stack, RPNExpression) ->
    {Stack2, RPNExpression2} = handle_right_paren(Stack, RPNExpression),
    transform_to_rpn_internal(Expression, Stack2, RPNExpression2);
transform_to_rpn_internal([Operand | Expression], Stack, RPNExpression) ->
    case binary:split(Operand, [?EQUALITY], [global]) of
        [_Key, _Val] ->
            transform_to_rpn_internal(Expression, Stack, RPNExpression ++ [Operand]);
        _ ->
            throw(?ERROR_INVALID_QOS_EXPRESSION)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles scanned right paren when transforming expression from infix notation to
%% reverse polish notation.
%% When right paren is scanned operators from operator stack should be
%% popped and moved to result expression as long as left paren is not popped.
%% Popped left paren should be removed.
%% @end
%%--------------------------------------------------------------------
-spec handle_right_paren(operator_stack(), rpn()) -> {operator_stack(), rpn()}.
handle_right_paren([?L_PAREN | Stack], RPNExpression) ->
    {Stack, RPNExpression};
handle_right_paren([Op | Stack], RPNExpression) ->
    handle_right_paren(Stack, RPNExpression ++ [Op]);
handle_right_paren([], _RPNExpression) ->
    throw(?ERROR_INVALID_QOS_EXPRESSION).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles scanned operator when transforming expression from infix notation to
%% reverse polish notation.
%% When operator is scanned all operators from operator stack with greater or
%% equal precedence to this operator should be popped and moved to result expression.
%% If paren is encountered on operator stack popping should stop there (paren
%% shouldn't be popped).
%% Then scanned operator should be pushed to the operator stack.
%% @end
%%--------------------------------------------------------------------
-spec handle_operator(operator(), operator_stack(), rpn()) -> {operator_stack(), rpn()}.
handle_operator(ParsedOp, [StackOperator | Stack], RPNExpression) when
        StackOperator =:= ?INTERSECTION orelse
        StackOperator =:= ?UNION orelse
        StackOperator =:= ?COMPLEMENT ->
    handle_operator(ParsedOp, Stack, RPNExpression ++ [StackOperator]);
handle_operator(ParsedOperator, Stack, RPNExpression) ->
    {[ParsedOperator | Stack], RPNExpression}.


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Filters given providers list using QoS expression so that only providers
%% fulfilling QoS left.
%% @end
%%--------------------------------------------------------------------
-spec eval_rpn(rpn(), [od_storage:id()], od_space:id()) -> [od_storage:id()].
eval_rpn(RPNExpression, StorageList, SpaceId) ->
    StorageSet = sets:from_list(StorageList),
    [ResSet] = lists:foldl(
        fun (ExpressionElem, ResPartial) ->
            eval_rpn(ExpressionElem, ResPartial, StorageSet, SpaceId)
        end , [], RPNExpression),
    sets:to_list(ResSet).

-spec eval_rpn(binary(), operand_stack(), sets:set(od_storage:id()), od_space:id()) ->
    [sets:set(od_storage:id())] | no_return().
eval_rpn(?UNION, [Operand1, Operand2 | StackTail], _AvailableStorage, _SpaceId) ->
    [sets:union(Operand1, Operand2)| StackTail];
eval_rpn(?INTERSECTION, [Operand1, Operand2 | StackTail], _AvailableStorage, _SpaceId) ->
    [sets:intersection(Operand1, Operand2)| StackTail];
eval_rpn(?COMPLEMENT, [Operand1, Operand2 | StackTail], _AvailableStorage, _SpaceId) ->
    [sets:subtract(Operand2, Operand1)| StackTail];
eval_rpn(Operand, Stack, AvailableStorage, SpaceId) ->
    case binary:split(Operand, [?EQUALITY], [global]) of
        [Key, Val] ->
            [filter_storage(Key, Val, AvailableStorage, SpaceId) | Stack];
        _ ->
            throw(?ERROR_INVALID_QOS_EXPRESSION)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Filters given storage list so that only storage with key equal
%% to value are left.
%% @end
%%--------------------------------------------------------------------
-spec filter_storage(binary(), binary(), sets:set(od_storage:id()), od_space:id()) -> sets:set(od_storage:id()).
filter_storage(Key, Val, StorageSet, SpaceId) ->
    sets:filter(fun (StorageId) ->
        {ok, StorageQosParameters} = storage:get_qos_parameters_of_remote_storage(StorageId, SpaceId),
        case maps:find(Key, StorageQosParameters) of
            {ok, Val} ->
                true;
            _ ->
                false
        end
    end, StorageSet).


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Selects required number of storage from list of storage.
%% Storage with higher current blocks size are preferred.
%% If there are no enough storages on list returns false otherwise returns
%% {true, StorageList}.
%% @end
%%--------------------------------------------------------------------
-spec select([od_storage:id()], pos_integer(), [#file_location{}], od_space:id()) ->
    {true, [od_storage:id()]} | false.
select([], _ReplicasNum, _FileLocations, _SpaceId) ->
    false;
select(StorageList, ReplicasNum, FileLocations, SpaceId) ->
    StorageListWithBlocksSize = lists:map(fun (StorageId) ->
        {get_storage_blocks_size(StorageId, FileLocations, SpaceId), StorageId}
    end, StorageList),

    SortedStorageListWithBlocksSize = lists:reverse(lists:sort(StorageListWithBlocksSize)),
    StorageSublist = [StorageId ||
        {_, StorageId} <- lists:sublist(SortedStorageListWithBlocksSize, ReplicasNum)],
    case length(StorageSublist) of
        ReplicasNum ->
            {true, StorageSublist};
        _ ->
            false
    end.


%%--------------------------------------------------------------------
%% @doc
%% @private
%% Returns blocks sum for given storage according to file_locations.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_blocks_size(od_storage:id(), [#file_location{}], od_space:id()) -> integer().
get_storage_blocks_size(StorageId, FileLocations, SpaceId) ->
    lists:foldl(fun(FileLocation, PartialStorageBlockSize) ->
        {ok, ProviderId} = storage:get_provider_of_remote_storage(StorageId, SpaceId),
        case maps:get(<<"providerId">>, FileLocation) of
            ProviderId ->
                PartialStorageBlockSize + maps:get(<<"totalBlocksSize">>, FileLocation);
            _ ->
                PartialStorageBlockSize
        end
     end, 0, FileLocations).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get list of storage id supporting space in which given file is stored.
%% @end
%%--------------------------------------------------------------------
-spec get_space_storages(file_ctx:ctx()) -> [od_storage:id()].
get_space_storages(FileCtx) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    SpaceId = file_id:guid_to_space_id(FileGuid),

    {ok, StorageIds} = space_logic:get_all_storage_ids(SpaceId),
    StorageIds.
