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


parse_to_rpn_test() ->
    % test empty
    ?assertEqual({ok, []}, qos_expression:raw_to_rpn(<<"">>)),

    % test simple equality
    ?assertEqual({ok, [<<"country=PL">>]}, qos_expression:raw_to_rpn(<<"country=PL">>)),

    % test operators precedence
    Operators = [<<"|">>, <<"&">>, <<"-">>],
    OperatorPairs = [{Op1, Op2} || Op1 <- Operators, Op2 <- Operators],
    lists:foreach(
        fun({Op1, Op2}) ->
            ?assertEqual(
                {ok, [<<"country=PL">>, <<"type=disk">>, Op1, <<"country=FR">>, Op2]},
                qos_expression:raw_to_rpn(<<"country=PL", Op1/binary,
                    "type=disk", Op2/binary, "country=FR">>)
            )
        end, OperatorPairs),

    % test parens
    Expr1 = <<"country=PL&type=disk|country=FR">>,
    ?assertEqual(
        {ok, [<<"country=PL">>, <<"type=disk">>, <<"&">>, <<"country=FR">>, <<"|">>]},
        qos_expression:raw_to_rpn(Expr1)
    ),

    Expr2 = <<"(country=PL&type=disk)|country=FR">>,
    ?assertEqual(
        {ok, [<<"country=PL">>, <<"type=disk">>, <<"&">>, <<"country=FR">>, <<"|">>]},
        qos_expression:raw_to_rpn(Expr2)
    ),

    Expr3 = <<"country=PL&(type=disk|country=FR)">>,
    ?assertEqual(
        {ok, [<<"country=PL">>, <<"type=disk">>, <<"country=FR">>, <<"|">>, <<"&">>]},
        qos_expression:raw_to_rpn(Expr3)
    ),

    Expr4 = <<"(country=PL&type=tape)|(type=disk&country=FR)">>,
    ?assertEqual(
        {ok, [<<"country=PL">>, <<"type=tape">>, <<"&">>,
         <<"type=disk">>, <<"country=FR">>, <<"&">>, <<"|">>]},
        qos_expression:raw_to_rpn(Expr4)
    ),

    % test invalid
    Expr5 = <<"country">>,
    ?assertMatch(
        ?ERROR_INVALID_QOS_EXPRESSION,
        qos_expression:raw_to_rpn(Expr5)
    ),

    Expr6 = <<"country|type">>,
    ?assertMatch(
        ?ERROR_INVALID_QOS_EXPRESSION,
        qos_expression:raw_to_rpn(Expr6)
    ),

%% TODO: VFS-5569 improve handling invalid QoS expressions
%%    Expr7 = <<"(country=PL">>,
%%    ?assertMatch(
%%        ?ERROR_INVALID_QOS_EXPRESSION,
%%        qos_expression:transform_to_rpn(Expr7)
%%    ),

    Expr8 = <<"type=disk)">>,
    ?assertMatch(
        ?ERROR_INVALID_QOS_EXPRESSION,
        qos_expression:raw_to_rpn(Expr8)
    ),

    Expr9 = <<")(country=PL">>,
    ?assertMatch(
        ?ERROR_INVALID_QOS_EXPRESSION,
        qos_expression:raw_to_rpn(Expr9)
    ).

