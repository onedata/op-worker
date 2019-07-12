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
-include_lib("eunit/include/eunit.hrl").

parse_to_rpn_test_() ->
    % test empty
    ?_assertEqual([], qos_expression:transform_to_rpn(<<"">>)),

    % test simple equality
    ?_assertEqual([<<"country=PL">>], qos_expression:transform_to_rpn(<<"country=PL">>)),

    % test operators precedence
    OperatorPairs = [{Op1, Op2} || Op1 <- ?OPERATORS, Op2 <- ?OPERATORS],
    lists:foreach(
        fun({Op1, Op2}) ->
            ?_assertEqual(
                [<<"country=PL">>, <<"type=disk">>, Op1, <<"country=FR">>, Op2],
                qos_expression:transform_to_rpn(<<"country=PL", Op1/binary,
                    "type=disk", Op2/binary, "country=FR">>)
            )
        end, OperatorPairs),

    % test parens
    Expr1 = <<"country=PL", ?INTERSECTION/binary, "type=disk", ?UNION/binary, "country=FR">>,
    ?_assertEqual(
        [<<"country=PL">>, <<"type=disk">>, ?INTERSECTION, <<"country=FR">>, ?UNION],
        qos_expression:transform_to_rpn(Expr1)
    ),

    Expr2 = <<?L_PAREN/binary, "country=PL", ?INTERSECTION/binary, "type=disk",
        ?R_PAREN/binary, ?UNION/binary, "country=FR">>,
    ?_assertEqual(
        [<<"country=PL">>, <<"type=disk">>, ?INTERSECTION, <<"country=FR">>, ?UNION],
        qos_expression:transform_to_rpn(Expr2)
    ),

    Expr3 = <<"country=PL", ?INTERSECTION/binary, ?L_PAREN/binary, "type=disk",
        ?UNION/binary, "country=FR", ?R_PAREN/binary>>,
    ?_assertEqual(
        [<<"country=PL">>, <<"type=disk">>, <<"country=FR">>, ?UNION, ?INTERSECTION],
        qos_expression:transform_to_rpn(Expr3)
    ),

    Expr4 = <<?L_PAREN/binary, "country=PL", ?INTERSECTION/binary, "type=tape", ?R_PAREN/binary,
        ?UNION/binary, ?L_PAREN/binary, "type=disk", ?INTERSECTION/binary, "country=FR", ?R_PAREN/binary>>,
    ?_assertEqual(
        [<<"country=PL">>, <<"type=tape">>, ?INTERSECTION,
         <<"type=disk">>, <<"country=FR">>, ?INTERSECTION, ?UNION],
        qos_expression:transform_to_rpn(Expr4)
    ).

