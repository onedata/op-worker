%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains functions operating on QoS expression. 
%%% @end
%%%--------------------------------------------------------------------
-module(qos_expression).
-author("Michal Stanisz").

-include("modules/datastore/qos.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([parse/1, filter/2, expression_to_rpn/1, rpn_to_expression/1, rpn_to_infix/1]).
-export([to_json/1, from_json/1]).

-type operator() :: binary(). % all possible listed in macro ?OPERATORS in qos.hrl
-type comparator() :: binary(). % all possible listed in macro ?COMPARATORS in qos.hrl
-type expr_token() :: binary().

% The infix type stores expression as single binary. It is used to store input
% from user. In the process of adding new qos_entry infix expression is parsed to tree form.
% Infix: <<"country=FR & type=disk">>
% RPN: [<<"country">>, <<"FR">>, <<"=">>, <<"type">>, <<"disk">>, <<"=">>, <<"&">>]
% Tree: {<<"&">>, {<<"=">>, <<"country">>, <<"FR">>}, {<<"=">>, <<"type">>, <<"disk">>}}
-type infix() :: binary(). 
-type rpn() :: [expr_token()].
-type tree() :: 
    {operator(), tree(), tree()} | 
    {comparator(), expr_token(), expr_token() | integer()} | 
    expr_token(). % <<"anyStorage">>

-opaque expression() :: tree().

-export_type([expression/0, infix/0]).


%%%===================================================================
%%% API
%%%===================================================================

-spec parse(infix()) -> expression() | no_return().
parse(InfixExpression) ->
    try
        {ok, Tokens, _} = qos_expression_scanner:string(binary_to_list(InfixExpression)),
        {ok, Tree} = qos_expression_parser:parse(Tokens),
        Tree
    catch _:_ ->
        throw(?ERROR_INVALID_QOS_EXPRESSION)
    end.


-spec filter(expression(), #{storage:id() => storage:qos_parameters()}) -> [storage:id()].
filter(?QOS_ANY_STORAGE, SM) ->
    maps:keys(SM);
filter({<<"|">>, Expr1, Expr2}, SM) ->
    lists_utils:union(filter(Expr1, SM), filter(Expr2, SM));
filter({<<"&">>, Expr1, Expr2}, SM) ->
    lists_utils:intersect(filter(Expr1, SM), filter(Expr2, SM));
filter({<<"\\">>, Expr1, Expr2}, SM) ->
    lists_utils:subtract(filter(Expr1, SM), filter(Expr2, SM));
filter({Comparator, ExprKey, ExprValue}, SM) ->
    maps:keys(maps:filter(fun(_StorageId, StorageParams) ->
        case maps:get(ExprKey, StorageParams, undefined) of
            undefined -> false;
            StorageValue -> compare(Comparator, StorageValue, ExprValue)
        end
    end, SM)).


-spec expression_to_rpn(expression()) -> rpn().
expression_to_rpn(?QOS_ANY_STORAGE) ->
    [?QOS_ANY_STORAGE];
expression_to_rpn({Op, Expr1, Expr2}) when is_binary(Expr1) and is_binary(Expr2) -> 
    [Expr1, Expr2, Op];
expression_to_rpn({Op, Expr1, Expr2}) when is_binary(Expr1) and is_integer(Expr2) ->
    [Expr1, Expr2, Op];
expression_to_rpn({Op, Expr1, Expr2}) ->
    expression_to_rpn(Expr1) ++ expression_to_rpn(Expr2) ++ [Op].


-spec rpn_to_infix(rpn()) -> infix().
rpn_to_infix(ExpressionRpn) -> 
    rpn_to_infix(ExpressionRpn, []).


-spec rpn_to_expression(rpn()) -> expression().
rpn_to_expression(RpnExpression) ->
    rpn_to_expression(RpnExpression, []).


-spec to_json(expression()) -> json_utils:json_term().
to_json(Binary) when is_binary(Binary) ->
    Binary;
to_json(Integer) when is_integer(Integer) ->
    Integer;
to_json({Operator, Expr1, Expr2}) ->
    [Operator, to_json(Expr1), to_json(Expr2)].


-spec from_json(json_utils:json_term()) -> expression().
from_json(Binary) when is_binary(Binary) ->
    Binary;
from_json(Integer) when is_integer(Integer) ->
    Integer;
from_json([Operator, Expr1, Expr2]) ->
    {Operator, from_json(Expr1), from_json(Expr2)}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec compare(comparator(), expr_token(), expr_token()) -> boolean().
compare(<<"<">>, A, B) when is_integer(A) -> A < B;
compare(<<">">>, A, B) when is_integer(A) -> A > B;
compare(<<"<=">>, A, B) when is_integer(A) -> A =< B;
compare(<<">=">>, A, B) when is_integer(A) -> A >= B;
compare(<<"=">>, A, B) -> A =:= B;
compare(_, _A, _B) -> false.


%% @private
-spec rpn_to_infix(rpn(), [expr_token()]) -> infix().
rpn_to_infix([ExprToken | ExpressionTail], Stack) ->
    IsOperator = lists:member(ExprToken, ?OPERATORS),
    case lists:member(ExprToken, ?COMPARATORS) or IsOperator of
        true ->
            [Operand1, Operand2 | StackTail] = Stack,
            InfixTokens = [Operand2, ExprToken, Operand1],
            ExtendedInfixTokens = case IsOperator andalso ExpressionTail of
                [_|_] -> [<<"(">> | InfixTokens] ++ [<<")">>];
                _ -> InfixTokens
            end,
            ConvertedTokens = lists:map(fun
                (Integer) when is_integer(Integer) -> integer_to_binary(Integer);
                (Binary) when is_binary(Binary) -> Binary
            end, ExtendedInfixTokens),
            rpn_to_infix(ExpressionTail, [str_utils:join_binary(ConvertedTokens) | StackTail]);
        false ->
            rpn_to_infix(ExpressionTail, [ExprToken | Stack])
    end;
rpn_to_infix([], [Res]) ->
    Res.


%% @private
-spec rpn_to_expression(rpn(), [expr_token()]) -> expression().
rpn_to_expression([ExprToken | ExpressionTail], Stack) ->
    case lists:member(ExprToken, ?COMPARATORS ++ ?OPERATORS) of
        true ->
            [Operand1, Operand2 | StackTail] = Stack,
            rpn_to_expression(ExpressionTail, [{ExprToken, Operand2, Operand1} | StackTail]);
        false ->
            rpn_to_expression(ExpressionTail, [ExprToken | Stack])
    end;
rpn_to_expression([], [Res]) ->
    Res.
