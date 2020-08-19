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
-export([parse/1, to_infix/1, from_rpn/1, to_rpn/1]).
-export([filter_storages/2]).
-export([to_json/1, from_json/1]).
-export([convert_from_old_version_rpn/1]).

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
-type tree(TokenType) :: 
    {operator(), tree(), tree()} | 
    {comparator(), TokenType, TokenType | integer()} | 
    TokenType. % <<"anyStorage">>

-opaque expression() :: tree(expr_token()).

-export_type([expression/0, infix/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec parse(infix()) -> expression() | no_return().
parse(InfixExpression) ->
    CheckResult = fun(Result, Module) ->
        case element(1, Result) of
            error ->
                ErrorDesc = element(2, Result),
                throw(?ERROR_INVALID_QOS_EXPRESSION(
                    str_utils:unicode_list_to_binary(Module:format_error(element(3, ErrorDesc)))));
            ok -> Result
        end
    end,
    
    {ok, Tokens, _} = CheckResult(
        qos_expression_scanner:string(str_utils:binary_to_unicode_list(InfixExpression)),
        qos_expression_scanner
    ),
    {ok, Tree} = CheckResult(
        qos_expression_parser:parse(Tokens),
        qos_expression_parser
    ),
    strings_to_binaries(Tree).


to_infix(?QOS_ANY_STORAGE) ->
    ?QOS_ANY_STORAGE;
to_infix({Op, Expr1, Expr2}) when is_binary(Expr1) and is_binary(Expr2) ->
    <<Expr1/binary, Op/binary, Expr2/binary>>;
to_infix({Op, Expr1, Expr2}) when is_binary(Expr1) and is_integer(Expr2) ->
    <<Expr1/binary, Op/binary, (integer_to_binary(Expr2))/binary>>;
to_infix({Op, Expr1, Expr2}) ->
    <<"(", (to_infix(Expr1))/binary, Op/binary, (to_infix(Expr2))/binary, ")">>.


-spec from_rpn(rpn()) -> expression().
from_rpn(RpnExpression) ->
    from_rpn(RpnExpression, []).

%% @private
-spec from_rpn(rpn(), [expr_token()]) -> expression().
from_rpn([ExprToken | ExpressionTail], Stack) ->
    case lists:member(ExprToken, ?COMPARATORS ++ ?OPERATORS) of
        true ->
            [Operand1, Operand2 | StackTail] = Stack,
            from_rpn(ExpressionTail, [{ExprToken, Operand2, Operand1} | StackTail]);
        false ->
            from_rpn(ExpressionTail, [ExprToken | Stack])
    end;
from_rpn([], [Res]) ->
    Res.


-spec to_rpn(expression()) -> rpn().
to_rpn(?QOS_ANY_STORAGE) ->
    [?QOS_ANY_STORAGE];
to_rpn({Op, Expr1, Expr2}) when is_binary(Expr1) and is_binary(Expr2) -> 
    [Expr1, Expr2, Op];
to_rpn({Op, Expr1, Expr2}) when is_binary(Expr1) and is_integer(Expr2) ->
    [Expr1, Expr2, Op];
to_rpn({Op, Expr1, Expr2}) ->
    to_rpn(Expr1) ++ to_rpn(Expr2) ++ [Op].


-spec filter_storages(expression(), #{storage:id() => storage:qos_parameters()}) -> [storage:id()].
filter_storages(?QOS_ANY_STORAGE, SM) ->
    maps:keys(SM);
filter_storages({<<"|">>, Expr1, Expr2}, SM) ->
    lists_utils:union(filter_storages(Expr1, SM), filter_storages(Expr2, SM));
filter_storages({<<"&">>, Expr1, Expr2}, SM) ->
    lists_utils:intersect(filter_storages(Expr1, SM), filter_storages(Expr2, SM));
filter_storages({<<"\\">>, Expr1, Expr2}, SM) ->
    lists_utils:subtract(filter_storages(Expr1, SM), filter_storages(Expr2, SM));
filter_storages({Comparator, ExprKey, ExprValue}, SM) ->
    maps:keys(maps:filter(fun(_StorageId, StorageParams) ->
        case maps:get(ExprKey, StorageParams, undefined) of
            undefined -> false;
            StorageValue -> compare(Comparator, StorageValue, ExprValue)
        end
    end, SM)).


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


-spec convert_from_old_version_rpn(rpn()) -> expression().
convert_from_old_version_rpn(PreviousExpression) ->
    % split old RPN tokens e.g: <<"a=b">> to [<<"a">>, <<"b">>, <<"=">>]
    % convert <<"-">> to <<"\\">>
    SplitRpnTokens = lists:flatten(lists:map(fun
        (<<"-">>) -> <<"\\">>;
        (RpnToken) ->
            case binary:split(RpnToken, [<<"=">>], [global]) of
                [X,Y] -> [X, Y, <<"=">>];
                _ -> RpnToken
            end
    end, PreviousExpression)
    ),
    % convert RPN to tree form
    from_rpn(SplitRpnTokens).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec strings_to_binaries(tree(string())) -> tree(expr_token()).
strings_to_binaries(?QOS_ANY_STORAGE_STRING) ->
    ?QOS_ANY_STORAGE;
strings_to_binaries({Op, Expr1, Expr2}) when is_list(Expr1) and is_list(Expr2) ->
    {
        str_utils:unicode_list_to_binary(Op),
        str_utils:unicode_list_to_binary(Expr1),
        str_utils:unicode_list_to_binary(Expr2)
    };
strings_to_binaries({Op, Expr1, Expr2}) when is_list(Expr1) and is_integer(Expr2) ->
    {
        str_utils:unicode_list_to_binary(Op),
        str_utils:unicode_list_to_binary(Expr1),
        Expr2
    };
strings_to_binaries({Op, Expr1, Expr2}) ->
    {
        str_utils:unicode_list_to_binary(Op),
        strings_to_binaries(Expr1),
        strings_to_binaries(Expr2)
    }.

%% @private
-spec compare(comparator(), expr_token(), expr_token()) -> boolean().
compare(<<"<">>, A, B) when is_integer(A) -> A < B;
compare(<<">">>, A, B) when is_integer(A) -> A > B;
compare(<<"<=">>, A, B) when is_integer(A) -> A =< B;
compare(<<">=">>, A, B) when is_integer(A) -> A >= B;
compare(<<"=">>, A, B) -> A =:= B;
compare(_, _A, _B) -> false.
