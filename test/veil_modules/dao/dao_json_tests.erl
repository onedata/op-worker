%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao_json module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(dao_json_tests).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/couch_db.hrl").

-endif.

-ifdef(TEST).

mk_doc_test() ->
    ?assertEqual(#doc{id = <<"Test">>}, dao_json:mk_doc("Test")).

mk_obj_test() ->
    ?assertEqual({[]}, dao_json:mk_obj()).

mk_str_test() ->
    ?assertEqual(<<"test">>, dao_json:mk_str("test")),
    ?assertEqual(<<"test">>, dao_json:mk_str(test)).

mk_bin_test() ->
    T = dao_json:mk_bin({test, 1, ["test", 2.0]}),
    ?assertMatch(T, term_to_binary({test, 1, ["test", 2.0]})).

mk_field_test() ->
    Obj1 = dao_json:mk_field({[]}, "Int", 1),
    ?assertEqual(Obj1, {[{<<"Int">>, 1}]}),
    Obj2 = dao_json:mk_field(Obj1, "Bool", true),
    ?assertEqual(Obj2, {[{<<"Bool">>, true}, {<<"Int">>, 1}]}),
    Obj3 = dao_json:mk_field(Obj1, "Bool", true),
    ?assertEqual(Obj3, {[{<<"Bool">>, true}, {<<"Int">>, 1}]}),
    Obj4 = dao_json:mk_field(dao_json:mk_doc("Test"), "Int", 1),
    ?assertEqual(Obj4, #doc{id = <<"Test">>, body = {[{<<"Int">>, 1}]}}).

mk_fields_test() ->
    Obj1 = dao_json:mk_fields(dao_json:mk_doc("Test"), ["Int", "Bool", "List"], [1, true, [1, 2, 3]]),
    ?assertMatch(Obj1, #doc{id = <<"Test">>, body = {[{<<"Int">>, 1}, {<<"Bool">>, true}, {<<"List">>, [1, 2, 3]}]}}).

rm_field_test() ->
    Doc1 = dao_json:rm_field(#doc{body = {[{<<"test">>, 1}, {<<"test2">>, 1}, {<<"test2">>, 1}]}}, "test2"),
    ?assertMatch(Doc1, #doc{body = {[{<<"test">>, 1}]}}).

rm_fields_test() ->
    Doc1 = dao_json:rm_fields(#doc{body = {[{<<"test">>, 1}, {<<"test2">>, 1}, {<<"test2">>, 1}, {<<"test3">>, 1}]}}, ["test2", "test3"]),
    ?assertMatch(Doc1, #doc{body = {[{<<"test">>, 1}]}}).

get_field_test() ->
    ?assertEqual(4, dao_json:get_field(#doc{body = {[{<<"test">>, 1}, {<<"test2">>, 2}, {<<"test2">>, 3}, {<<"test3">>, 4}]}}, "test3")).

get_fields_test() ->
    ?assertMatch([{"test", 1}, {"test2", 2}, {"test3", 4}], dao_json:get_fields(#doc{body = {[{<<"test">>, 1}, {<<"test2">>, 2}, {<<"test3">>, 4}]}})).

reverse_fields_test() ->
    ?assertMatch(#doc{body = {[{<<"f1">>, 5}, {<<"f2">>, 5}]}}, dao_json:reverse_fields(#doc{body = {[{<<"f2">>, 5}, {<<"f1">>, 5}]}})).

-endif.