%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests of functions from transfer_links module
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_links_test).
-author("Jakub Kudzia").

-include_lib("eunit/include/eunit.hrl").

merge_two_empty_lists_test() ->
    ?assertEqual([], transfer_links:umerge([], [], 0, 0)),
    ?assertEqual([], transfer_links:umerge([], [], 0, all)),
    ?assertEqual([], transfer_links:umerge([], [], 0, 100)).

merge_not_empty_and_empty_lists_test() ->
    Size = 10,
    List = lists:map(fun(X) -> {X, X} end, lists:seq(1, 10)),
    ResultList = [X || {X, _Y} <- List],
    ?assertEqual([], transfer_links:umerge(List, [], 0, 0)),
    ?assertEqual([], transfer_links:umerge([], List, 0, 0)),
    ?assertEqual([1], transfer_links:umerge(List, [], 0, 1)),
    ?assertEqual([1], transfer_links:umerge([], List, 0, 1)),
    ?assertEqual(lists:sublist(ResultList, 2, 7), transfer_links:umerge(List, [], 1, 7)),
    ?assertEqual(lists:sublist(ResultList, 2, 7), transfer_links:umerge([], List, 1, 7)),
    ?assertEqual(ResultList, transfer_links:umerge(List, [], 0, all)),
    ?assertEqual(ResultList, transfer_links:umerge([], List, 0, all)),
    ?assertEqual(lists:sublist(ResultList, 5, 10), transfer_links:umerge(List, [], 4, all)),
    ?assertEqual(lists:sublist(ResultList, 5, 10), transfer_links:umerge([], List, 4, all)),
    ?assertEqual(ResultList, transfer_links:umerge(List, [], 0, Size)),
    ?assertEqual(ResultList, transfer_links:umerge([], List, 0, Size)),
    ?assertEqual(ResultList, transfer_links:umerge(List, [], 0, Size + 1)),
    ?assertEqual(ResultList, transfer_links:umerge([], List, 0, Size + 1)).

merge_not_empty_sorted_lists_test() ->
    List1 = [{<<"A5">>, <<"0">>}, {<<"A1">>, <<"2">>}, {<<"A0">>, <<"4">>}],
    List2 = [{<<"B0">>, <<"0">>}, {<<"B8">>, <<"1">>}, {<<"B2">>, <<"2">>}, {<<"B0">>, <<"5">>}, {<<"B9">>, <<"10">>}],
    Result = [<<"A5">>, <<"B8">>, <<"A1">>, <<"A0">>, <<"B0">>, <<"B9">>],
    ?assertEqual(Result, transfer_links:umerge(List1, List2, 0, all)),
    ?assertEqual(lists:sublist(Result, 3, 10), transfer_links:umerge(List1, List2, 2, 100123413)),
    ?assertEqual(lists:sublist(Result, 6, 10), transfer_links:umerge(List1, List2, 5, all)),
    ?assertEqual(lists:sublist(Result, 1, 2), transfer_links:umerge(List1, List2, 0, 2)).