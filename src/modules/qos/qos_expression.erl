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
-export([raw_to_rpn/1, calculate_assigned_storages/3]).


% The raw type stores expression as single binary. It is used to store input
% from user. In the process of adding new qos_entry raw expression is
% parsed to rpn form (list of "key=value" binaries separated by operators)
-type raw() :: binary(). % e.g. <<"country=FR&type=disk">>
-type rpn() :: [binary()]. % e.g. [<<"country=FR">>, <<"type=disk">>, <<"&">>]

-export_type([rpn/0, raw/0]).

-type operator_stack() :: [operator_or_paren()].
-type operator_or_paren() :: operator() | paren().
-type paren() :: binary().
-type operator() :: binary().
-type expr_token() :: operator() | binary().
-type storages_with_params() :: #{storage:id() => storage:qos_parameters()}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Transforms QoS expression from infix notation to reverse polish notation.
%% @end
%%--------------------------------------------------------------------
-spec raw_to_rpn(raw()) -> {ok, rpn()} | ?ERROR_INVALID_QOS_EXPRESSION.
raw_to_rpn(Expression) ->
    OperatorsBin = <<?UNION/binary, ?INTERSECTION/binary, ?COMPLEMENT/binary>>,
    ParensBin = <<?L_PAREN/binary, ?R_PAREN/binary>>,
    NormalizedExpression = re:replace(Expression, "\s", "", [global, {return, binary}]),
    Tokens = re:split(NormalizedExpression, <<"([", ParensBin/binary, OperatorsBin/binary, "])">>),
    try
        {ok, raw_to_rpn_internal(Tokens, [], [])}
    catch
        throw:?ERROR_INVALID_QOS_EXPRESSION ->
            ?ERROR_INVALID_QOS_EXPRESSION
    end.


%%--------------------------------------------------------------------
%% @doc
%% Calculate list of storages, on which file should be present according to
%% given QoS expression and number of replicas.
%% @end
%%--------------------------------------------------------------------
-spec calculate_assigned_storages(file_ctx:ctx(), rpn(), qos_entry:replicas_num()) ->
    {true, [storage:id()]} | false | {error, term()}.
calculate_assigned_storages(FileCtx, Expression, ReplicasNum) ->
    % TODO: VFS-5574 add check if storage has enough free space
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {ok, SpaceStorages} = space_logic:get_all_storage_ids(SpaceId),

    AllStoragesWithParams = lists:foldl(fun(StorageId, Acc) ->
        Acc#{StorageId => storage:fetch_qos_parameters_of_remote_storage(StorageId, SpaceId)}
    end, #{}, SpaceStorages),

    try
        EligibleStorages = filter_storages(AllStoragesWithParams, Expression),
        choose_storages(EligibleStorages, ReplicasNum)
    catch
        throw:?ERROR_INVALID_QOS_EXPRESSION ->
            ?ERROR_INVALID_QOS_EXPRESSION
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec raw_to_rpn_internal([expr_token()], operator_stack(), rpn()) -> rpn().
raw_to_rpn_internal([<<>>], [], []) ->
    [];
raw_to_rpn_internal([<<>> | Expression], Stack, RPNExpression) ->
    raw_to_rpn_internal(Expression, Stack, RPNExpression);
raw_to_rpn_internal([], Stack, RPNExpression) ->
    RPNExpression ++ Stack;
raw_to_rpn_internal([Operator | Expression], Stack, RPNExpression) when
    Operator =:= ?INTERSECTION orelse
        Operator =:= ?UNION orelse
        Operator =:= ?COMPLEMENT ->
    {Stack2, RPNExpression2} = handle_operator(Operator, Stack, RPNExpression),
    raw_to_rpn_internal(Expression, Stack2, RPNExpression2);
raw_to_rpn_internal([?L_PAREN | Expression], Stack, RPNExpression) ->
    raw_to_rpn_internal(Expression, [?L_PAREN|Stack], RPNExpression);
raw_to_rpn_internal([?R_PAREN | Expression], Stack, RPNExpression) ->
    {Stack2, RPNExpression2} = handle_right_paren(Stack, RPNExpression),
    raw_to_rpn_internal(Expression, Stack2, RPNExpression2);
raw_to_rpn_internal([?QOS_ANY_STORAGE | Expression], Stack, RPNExpression) ->
    raw_to_rpn_internal(Expression, Stack, RPNExpression ++ [?QOS_ANY_STORAGE]);
raw_to_rpn_internal([Operand | Expression], Stack, RPNExpression) ->
    case binary:split(Operand, [?EQUALITY], [global]) of
        [_Key, _Val] ->
            raw_to_rpn_internal(Expression, Stack, RPNExpression ++ [Operand]);
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
%% @private
%% @doc
%% Filters storages list using QoS expression so that only storages
%% fulfilling QoS are left.
%% @end
%%--------------------------------------------------------------------
-spec filter_storages(storages_with_params(), [expr_token()]) -> [storage:id()].
filter_storages(AllStoragesWithParams, RPNExpression) ->
    FinalStack = lists:foldl(fun(ExprToken, Stack) ->
        case lists:member(ExprToken, ?OPERATORS) of
            true ->
                apply_operator(ExprToken, Stack);
            false ->
                [select_storages_with_param(AllStoragesWithParams, ExprToken) | Stack]
        end
    end , [], RPNExpression),

    case FinalStack of
        [Result] -> Result;
        _ -> throw(?ERROR_INVALID_QOS_EXPRESSION)
    end.


%% @private
-spec apply_operator(operator(), [[storage:id()]]) ->
    [[storage:id()]] | no_return().
apply_operator(?UNION, [StoragesList1, StoragesList2 | StackTail]) ->
    [lists_utils:union(StoragesList1, StoragesList2)| StackTail];
apply_operator(?INTERSECTION, [StoragesList1, StoragesList2 | StackTail]) ->
    [lists_utils:intersect(StoragesList1, StoragesList2) | StackTail];
apply_operator(?COMPLEMENT, [StoragesList1, StoragesList2 | StackTail]) ->
    [lists_utils:subtract(StoragesList2, StoragesList1) | StackTail];
apply_operator(_, _) ->
    throw(?ERROR_INVALID_QOS_EXPRESSION).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Selects all storages with given parameter("key=value" token).
%% @end
%%--------------------------------------------------------------------
-spec select_storages_with_param(storages_with_params(), expr_token()) ->
    [[storage:id()] | expr_token()].
select_storages_with_param(AllStoragesWithParams, ?QOS_ANY_STORAGE) ->
    maps:keys(AllStoragesWithParams);
select_storages_with_param(AllStoragesWithParams, ExprToken) ->
    case binary:split(ExprToken, [?EQUALITY], [global]) of
        [Key, Val] ->
            maps:keys(maps:filter(fun(_StorageId, StorageQosParameters) ->
                case maps:find(Key, StorageQosParameters) of
                    {ok, Val} -> true;
                    _ -> false
                end
            end, AllStoragesWithParams));
        _ ->
            throw(?ERROR_INVALID_QOS_EXPRESSION)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Selects required number of storage from list of storages.
%% If there are no enough storages on list returns false otherwise returns
%% {true, StorageList}.
%% @end
%%--------------------------------------------------------------------
-spec choose_storages([storage:id()], qos_entry:replicas_num()) ->
    {true, [storage:id()]} | false.
choose_storages(EligibleStoragesList, ReplicasNum) ->
    % TODO: VFS-5734 choose storages according to current files distribution
    StorageSublist = lists:sublist(EligibleStoragesList, ReplicasNum),
    case length(StorageSublist) of
        ReplicasNum -> {true, StorageSublist};
        _ -> false
    end.
