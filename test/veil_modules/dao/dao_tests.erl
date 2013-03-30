%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(dao_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-endif.

-ifdef(TEST).

main_test_() ->
    {inorder, [fun init/0, fun handle/0, fun cleanup/0]}.

init() ->
    ?assert(dao:init([]) =:= ok).

handle() ->
    ?assertNot({error, wrong_args} =:= dao:handle(1, {helper, test, []})),
    ?assertNot({error, wrong_args} =:= dao:handle(1, {hosts, test, []})),
    ?assertNot({error, wrong_args} =:= dao:handle(1, {test, []})),
    ?assert({error, wrong_args} =:= dao:handle(1, {"wrong", test, []})),
    ?assert({error, undef} =:= dao:handle(1, {wrong, test, []})),
    meck:new(dao_vfs),
    meck:expect(dao_vfs, list_dir, fun(_, _) -> ok end),
    ok = dao:handle(1, {vfs, list_dir, ["", test]}),
    meck:unload(dao_vfs).

cleanup() ->
    ok = dao:cleanup().


save_record_test() ->
    ?assertException(throw, unsupported_record, dao:save_record(whatever, {a, b, c})),
    ?assertException(throw, invalid_record, dao:save_record(whatever, {some_record, a, c})).

get_record_test() ->
    ?assertException(throw, unsupported_record, dao:get_record(test, whatever)),
    ?assertException(throw, unsupported_record, dao:get_record(test, {whatever, a, c})).


term_to_doc_test() ->
    15 = dao:term_to_doc(15),
        -15.43 = dao:term_to_doc(-15.43),
    true = dao:term_to_doc(true),
    false = dao:term_to_doc(false),
    null = dao:term_to_doc(null),
    <<"test:test2 ]test4{test 5=+ 6">> = dao:term_to_doc("test:test2 ]test4{test 5=+ 6"),
    Ans = {[{<<"_record">>, <<"some_record">>},
        {<<"field1">>,
            {[{<<"tuple_field_1">>, <<"rec1">>},
                {<<"tuple_field_2">>,
                    {[{<<"_record">>, <<"some_record">>},
                        {<<"field1">>, true},
                        {<<"field2">>, 5},
                        {<<"field3">>, <<>>}]}}]}},
        {<<"field2">>, <<"test string">>},
        {<<"field3">>,
            [1,
                {[{<<"tuple_field_1">>, 6.53},
                    {<<"tuple_field_2">>,
                        [true,
                            {[{<<"tuple_field_1">>, <<"test1">>}, {<<"tuple_field_2">>, false}]}]}]},
                5.4, false,
                [1, 0, <<"test">>]]}]},
    Ans = dao:term_to_doc(#some_record{field1 = {"rec1", #some_record{field1 = true, field2 = 5}}, field2 = "test string", field3 = [1, {6.53, [true, {"test1", false}]}, 5.4, false, [1, 0, "test"]]}).

doc_to_term_test() ->
    Ans = {[{<<"_record">>, <<"some_record">>},
        {<<"field1">>,
            {[{<<"tuple_field_1">>, <<"rec1">>},
                {<<"tuple_field_2">>,
                    {[{<<"_record">>, <<"some_record">>},
                        {<<"field1">>, true},
                        {<<"field2">>, 5},
                        {<<"field3">>, <<>>}]}}]}},
        {<<"field2">>, <<"test string">>},
        {<<"field3">>,
            [1,
                {[{<<"tuple_field_1">>, 6.53},
                    {<<"tuple_field_2">>,
                        [true,
                            {[{<<"tuple_field_1">>, <<"test1">>}, {<<"tuple_field_2">>, false}]}]}]},
                5.4, false,
                [1, 0, <<"test">>]]}]},
    {some_record, {"rec1", {some_record, true, 5, []}},
        "test string", [1, {6.53, [true, {"test1", false}]}, 5.4, false, [1, 0, "test"]]} = dao:doc_to_term(Ans).

-endif.