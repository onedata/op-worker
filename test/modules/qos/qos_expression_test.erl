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

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/errors.hrl").
-include("modules/datastore/qos.hrl").


valid_qos_expression_test() ->
    check_valid_expression(<<"country=PL">>, [<<"country">>, <<"PL">>, <<"=">>]),
    check_valid_expression(<<"latency=8">>, [<<"latency">>, 8, <<"=">>]),
    check_valid_expression(<<"latency>8">>, [<<"latency">>, 8, <<">">>]),
    check_valid_expression(<<"latency<8">>, [<<"latency">>, 8, <<"<">>]),
    check_valid_expression(<<"latency>=8">>, [<<"latency">>, 8, <<">=">>]),
    check_valid_expression(<<"latency<=8">>, [<<"latency">>, 8, <<"<=">>]),
    
    Operators = [<<"|">>, <<"&">>, <<"\\">>],
    OperatorPairs = [{Op1, Op2} || Op1 <- Operators, Op2 <- Operators],
    lists:foreach(
        fun({Op1, Op2}) ->
            Expr = <<"country=PL", Op1/binary, "type=disk", Op2/binary, "country=FR">>,
            Rpn = [<<"country">>, <<"PL">>, <<"=">>, <<"type">>, <<"disk">>, <<"=">>, Op1, <<"country">>, <<"FR">>, <<"=">>, Op2],
            check_valid_expression(Expr, Rpn),
            % Expression without brackets and expression with brackets are converted to the same RPN form. 
            ExprWithBrackets = <<"(country=PL", Op1/binary, "type=disk)", Op2/binary, "country=FR">>,
            check_valid_expression(ExprWithBrackets, Rpn)
        end, OperatorPairs),

    check_valid_expression(
        <<"country=PL&type=disk|latency<8">>, 
        [<<"country">>,<<"PL">>,<<"=">>,<<"type">>,<<"disk">>, <<"=">>,<<"&">>,<<"latency">>,8,<<"<">>,<<"|">>]
    ),

    check_valid_expression(
        <<"(country=PL&type=disk)|latency>8">>,
        [<<"country">>,<<"PL">>,<<"=">>,<<"type">>,<<"disk">>, <<"=">>,<<"&">>,<<"latency">>,8,<<">">>,<<"|">>]
    ),

    check_valid_expression(
        <<"country=PL&(type=disk|latency=8)">>, 
        [<<"country">>,<<"PL">>,<<"=">>,<<"type">>,<<"disk">>, <<"=">>,<<"latency">>,8,<<"=">>,<<"|">>,<<"&">>] 
    ),
    
    check_valid_expression(
        <<"(country=PL&type=tape)|(type=disk&latency<=8)">>,
        [<<"country">>,<<"PL">>,<<"=">>,<<"type">>,<<"tape">>, <<"=">>,<<"&">>,<<"type">>,<<"disk">>,<<"=">>,
            <<"latency">>, 8,<<"<=">>,<<"&">>,<<"|">>]
    ),
    
    check_valid_expression(<<"a a a a = 8">>, [<<"a a a a">>, 8, <<"=">>]),
    check_valid_expression(<<"a = a a a a">>, [<<"a">>, <<"a a a a">>, <<"=">>]),
    check_valid_expression(<<"a-a < 8">>, [<<"a-a">>, 8, <<"<">>]),
    check_valid_expression(<<"a-a > 8">>, [<<"a-a">>, 8, <<">">>]),
    check_valid_expression(<<"_-_ <= 8">>, [<<"_-_">>, 8, <<"<=">>]),
    check_valid_expression(<<"a8 <= 8">>, [<<"a8">>, 8, <<"<=">>]),
    check_valid_expression(<<"a = a">>, [<<"a">>, <<"a">>, <<"=">>]),
    check_valid_expression(<<"aa = aa">>, [<<"aa">>, <<"aa">>, <<"=">>]),
    check_valid_expression(<<"8a <= 8">>, [<<"8a">>, 8, <<"<=">>]),
    check_valid_expression(<<"a8a >= 8">>, [<<"a8a">>, 8, <<">=">>]),
    check_valid_expression(<<"a = a8">>, [<<"a">>, <<"a8">>, <<"=">>]),
    check_valid_expression(<<"a = 8a">>, [<<"a">>, <<"8a">>, <<"=">>]),
    check_valid_expression(<<"a = a8a">>, [<<"a">>, <<"a8a">>, <<"=">>]),
    check_valid_expression(<<"a = a-a">>, [<<"a">>, <<"a-a">>, <<"=">>]),
    check_valid_expression(<<"a_a = _a_">>, [<<"a_a">>, <<"_a_">>, <<"=">>]),
    check_valid_expression(<<"   \"  a  \t \n  = a     ">>, [<<"a">>, <<"a">>, <<"=">>]),
    check_valid_expression(<<"anyStorage">>, [<<"anyStorage">>]),
    check_valid_expression(<<"anyStorage \\ (a=b)">>, [<<"anyStorage">>, <<"a">>, <<"b">>, <<"=">>, <<"\\">>]),
    check_valid_expression(
        <<"(a=b & anyStorage) \\ anyStorage">>, 
        [<<"a">>, <<"b">>, <<"=">>, <<"anyStorage">>, <<"&">>, <<"anyStorage">>, <<"\\">>]
    ),
    ok.
    

invalid_qos_expression_test() ->    
    check_invalid_expression(<<"country">>),
    check_invalid_expression(<<"country|type">>),
    check_invalid_expression(<<"(country=PL">>),
    check_invalid_expression(<<"type=disk)">>),
    check_invalid_expression(<<")(country=PL">>),
    check_invalid_expression(<<"country=PL&\\type\\disk">>),
    check_invalid_expression(<<"(type=disk|tier=t2&(country=PL)">>),
    check_invalid_expression(<<"country=PL&">>),
    check_invalid_expression(<<"geo=PL | (geo=DE & latency) <= 8">>),
    check_invalid_expression(<<"()">>),
    check_invalid_expression(<<"a<b">>),
    check_invalid_expression(<<"a>b">>),
    check_invalid_expression(<<"8=a">>),
    check_invalid_expression(<<"8888=a">>),
    check_invalid_expression(<<"-a-=a">>),
    check_invalid_expression(<<"a=a=a">>),
    check_invalid_expression(<<"a|a|a">>),
    check_invalid_expression(<<"a=a;">>),
    ok.


filter_storages_test() ->
    StoragesMap = #{
        <<"0">> => #{<<"geo">> => <<"PL">>, <<"latency">> => 8},
        <<"1">> => #{<<"geo">> => <<"FR">>, <<"latency">> => 2131},
        <<"2">> => #{<<"geo">> => <<"PT">>, <<"latency">> => 0},
        <<"3">> => #{<<"geo">> => <<"US">>, <<"latency">> => 123},
        <<"4">> => #{<<"geo">> => <<"DE">>, <<"latency">> => 321},
        <<"5">> => #{<<"latency">> => <<"not_integer">>}
    },
    check_filter_storages(<<"anyStorage">>, maps:keys(StoragesMap), StoragesMap),
    check_filter_storages(<<"geo = PL">>, [<<"0">>], StoragesMap),
    check_filter_storages(<<"latency = not_integer">>, [<<"5">>], StoragesMap),
    check_filter_storages(<<"latency = 8">>, [<<"0">>], StoragesMap),
    check_filter_storages(<<"latency > 8">>, [<<"1">>, <<"3">>, <<"4">>], StoragesMap),
    check_filter_storages(<<"latency < 8">>, [<<"2">>], StoragesMap),
    check_filter_storages(<<"latency >= 8">>, [<<"0">>, <<"1">>, <<"3">>, <<"4">>], StoragesMap),
    check_filter_storages(<<"latency <= 8">>, [<<"0">>, <<"2">>], StoragesMap),
    check_filter_storages(<<"geo=PL | geo=DE">>, [<<"0">>, <<"4">>], StoragesMap),
    check_filter_storages(<<"geo=PL & geo=DE">>, [], StoragesMap),
    check_filter_storages(<<"(geo=PL | geo=DE) & latency <= 8">>, [<<"0">>], StoragesMap),
    check_filter_storages(<<"(geo=PL | geo=DE) | latency <= 8">>, [<<"0">>, <<"2">>, <<"4">>], StoragesMap),
    check_filter_storages(<<"(geo=PL | geo=DE) \\ latency <= 8">>, [<<"4">>], StoragesMap),
    check_filter_storages(<<"geo=PL | (geo=DE & latency <= 8)">>, [<<"0">>], StoragesMap),
    check_filter_storages(<<"latency < 8 | latency > 320">>, [<<"1">>, <<"2">>, <<"4">>], StoragesMap),
    check_filter_storages(<<"latency > 320 \\ latency < 400">>, [<<"1">>], StoragesMap),
    check_filter_storages(<<"(geo=PL | geo=DE) & latency <= 8 | (latency > 320 \\ latency < 400)">>, [<<"0">>, <<"1">>], StoragesMap),
    check_filter_storages(<<"((geo=PL | geo=DE) & latency <= 8 ) | (latency > 320 \\ latency < 400)">>, [<<"0">>, <<"1">>], StoragesMap),
    check_filter_storages(<<"(geo=PL | geo=DE) | (latency > 320 \\ latency < 400) & latency <= 8 ">>, [<<"0">>], StoragesMap),
    check_filter_storages(
        <<"(((geo=PL | geo=DE) & latency <= 8 ) | (latency > 320 \\ latency < 400) 
        | ((geo=PL | geo=DE) & latency <= 8 ) | (latency > 320 \\ latency < 400))
        \\ ((geo=PL | geo=DE) | (latency > 320 \\ latency < 400) & latency <= 8) ">>, 
        [<<"1">>], StoragesMap
    ),
    ok.


convert_from_old_version_rpn_test() ->
    ?assertEqual({<<"=">>, <<"a">>, <<"b">>}, 
        qos_expression:convert_from_old_version_rpn([<<"a=b">>])),
    ?assertEqual({<<"\\">>, {<<"=">>, <<"a">>, <<"b">>}, {<<"=">>, <<"c">>, <<"d">>}}, 
        qos_expression:convert_from_old_version_rpn([<<"a=b">>, <<"c=d">>, <<"-">>])),
    ?assertEqual({<<"|">>, {<<"=">>, <<"a">>, <<"b">>}, {<<"=">>, <<"c">>, <<"d">>}},
        qos_expression:convert_from_old_version_rpn([<<"a=b">>, <<"c=d">>, <<"|">>])),
    ?assertEqual({<<"&">>, {<<"=">>, <<"a">>, <<"b">>}, {<<"=">>, <<"c">>, <<"d">>}},
        qos_expression:convert_from_old_version_rpn([<<"a=b">>, <<"c=d">>, <<"&">>])).

%%%===================================================================
%%% Internal functions
%%%===================================================================

check_valid_expression(Expression, ExpectedRpn) ->
    Tree = qos_expression:parse(Expression),
    % Check that to_rpn works correctly.
    ?assertEqual(ExpectedRpn, qos_expression:to_rpn(Tree)),
    % Check that from_rpn works correctly.
    ?assertEqual(Tree, qos_expression:from_rpn(ExpectedRpn)),
    % Check that to_infix works correctly.
    % Because of whitespaces and parens unambiguity, instead of comparing strings 
    % check that resulting expression is equivalent.
    ?assertEqual(Tree, qos_expression:parse(qos_expression:to_infix(Tree))),
    % Check to_json and from_json functions.
    ?assertEqual(Tree, qos_expression:from_json(qos_expression:to_json(Tree))).
    

check_invalid_expression(Expression) ->
    ?assertThrow(?ERROR_INVALID_QOS_EXPRESSION(_), qos_expression:parse(Expression)).


check_filter_storages(Expression, ExpectedStorages, StoragesMap) ->
    Tree = qos_expression:parse(Expression),
    ?assertEqual(ExpectedStorages, qos_expression:filter_storages(Tree, StoragesMap)).

-endif.
