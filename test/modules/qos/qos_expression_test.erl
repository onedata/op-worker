%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for qos_expression module.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_expression_test).
-author("Michal Cwiertnia").

-include("modules/datastore/qos.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("eunit/include/eunit.hrl").


qos_expression_test() ->
    
    % test simple equality
    Expr0 = <<"country=PL">>,
    {ok, RPN0} = qos_expression:raw_to_rpn(Expr0),
    ?assertEqual([<<"country=PL">>], RPN0),
    ?assertEqual({ok, Expr0}, qos_expression:rpn_to_infix(RPN0)),

    % test operators precedence
    Operators = [<<"|">>, <<"&">>, <<"-">>],
    OperatorPairs = [{Op1, Op2} || Op1 <- Operators, Op2 <- Operators],
    lists:foreach(
        fun({Op1, Op2}) ->
            Expr = <<"country=PL", Op1/binary, "type=disk", Op2/binary, "country=FR">>,
            {ok, RPN} = qos_expression:raw_to_rpn(Expr),
            ?assertEqual([<<"country=PL">>, <<"type=disk">>, Op1, <<"country=FR">>, Op2], RPN),
            % Because expression without brackets and expression with brackets are converted to 
            % the same RPN form it is impossible to distinguish them when converting back. 
            % As infix expression with brackets is unambiguous it is returned by `rpn_to_infix`.
            ExprWithBrackets = <<"(country=PL", Op1/binary, "type=disk)", Op2/binary, "country=FR">>,
            ?assertEqual({ok, ExprWithBrackets}, qos_expression:rpn_to_infix(RPN))
        end, OperatorPairs),

    Expr1 = <<"country=PL&type=disk|country=FR">>,
    {ok, RPN1} = qos_expression:raw_to_rpn(Expr1),
    ?assertEqual([<<"country=PL">>, <<"type=disk">>, <<"&">>, <<"country=FR">>, <<"|">>], RPN1),
    
    % test parens
    Expr2 = <<"(country=PL&type=disk)|country=FR">>,
    {ok, RPN2} = qos_expression:raw_to_rpn(Expr2),
    ?assertEqual([<<"country=PL">>, <<"type=disk">>, <<"&">>, <<"country=FR">>, <<"|">>], RPN2),
    ?assertEqual({ok, Expr2}, qos_expression:rpn_to_infix(RPN2)),

    Expr3 = <<"country=PL&(type=disk|country=FR)">>,
    {ok, RPN3} = qos_expression:raw_to_rpn(Expr3),
    ?assertEqual([<<"country=PL">>, <<"type=disk">>, <<"country=FR">>, <<"|">>, <<"&">>], RPN3),
    ?assertEqual({ok, Expr3}, qos_expression:rpn_to_infix(RPN3)),

    Expr4 = <<"(country=PL&type=tape)|(type=disk&country=FR)">>,
    {ok, RPN4} = qos_expression:raw_to_rpn(Expr4),
    ?assertEqual([<<"country=PL">>, <<"type=tape">>, <<"&">>,
         <<"type=disk">>, <<"country=FR">>, <<"&">>, <<"|">>], RPN4),
    ?assertEqual({ok, Expr4}, qos_expression:rpn_to_infix(RPN4)),

    % test invalid expression
    Expr5 = <<"country">>,
    ?assertThrow(
        ?ERROR_INVALID_QOS_EXPRESSION,
        qos_expression:raw_to_rpn(Expr5)
    ),

    Expr6 = <<"country|type">>,
    ?assertThrow(
        ?ERROR_INVALID_QOS_EXPRESSION,
        qos_expression:raw_to_rpn(Expr6)
    ),

    Expr7 = <<"(country=PL">>,
    ?assertThrow(
        ?ERROR_INVALID_QOS_EXPRESSION,
        qos_expression:raw_to_rpn(Expr7)
    ),

    Expr8 = <<"type=disk)">>,
    ?assertThrow(
        ?ERROR_INVALID_QOS_EXPRESSION,
        qos_expression:raw_to_rpn(Expr8)
    ),

    Expr9 = <<")(country=PL">>,
    ?assertThrow(
        ?ERROR_INVALID_QOS_EXPRESSION,
        qos_expression:raw_to_rpn(Expr9)
    ),
    
    Expr10 = <<"country=PL&-type-disk">>,
    ?assertThrow(
        ?ERROR_INVALID_QOS_EXPRESSION,
        qos_expression:raw_to_rpn(Expr10)
    ),
    
    Expr11 = <<"(type=disk|tier=t2&(country=PL)">>,
    ?assertThrow(
        ?ERROR_INVALID_QOS_EXPRESSION,
        qos_expression:raw_to_rpn(Expr11)
    ).

