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
-include_lib("veil_modules/dao/couch_db.hrl").
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
    ?assertException(throw, unsupported_record, dao:save_record({a, b, c})),
    ?assertException(throw, unsupported_record, dao:save_record({some_record, a, c})),
    ToInsert = {some_record, a, b, c},
    DbObj = dao:term_to_doc(ToInsert),
    meck:new(dao_helper),
    meck:expect(dao_helper, ensure_db_exists, fun(?SYSTEM_DB_NAME) -> ok end),
    meck:expect(dao_helper, open_doc, fun(?SYSTEM_DB_NAME, "test_id") -> {error, not_found} end),
    meck:expect(dao_helper, insert_doc, fun(?SYSTEM_DB_NAME, #doc{id = Id, body = DbObj}) -> {ok, Id} end),
    meck:expect(dao_helper, name, fun(Arg) -> meck:passthrough([Arg]) end),
    {ok, "test_id"} = dao:save_record(ToInsert, "test_id"),
    {ok, "test_id"} = dao:save_record(ToInsert, "test_id", update),
    meck:expect(dao_helper, open_doc, fun(?SYSTEM_DB_NAME, "test_id") -> {ok, #doc{revs = {1, [<<"123">>]}}} end),
    meck:expect(dao_helper, insert_doc, fun(?SYSTEM_DB_NAME, #doc{id = Id, revs = {1, [<<"123">>]}, body = DbObj}) -> {ok, Id} end),
    {ok, "test_id"} = dao:save_record(ToInsert, "test_id", update),
    true = meck:validate(dao_helper),
    meck:unload(dao_helper),
    pass.

get_record_test() ->
    DbObj = {[{<<?RECORD_META_FIELD_NAME>>, <<"some_record">>}, {<<"field1">>, 1}, {<<"field3">>, true}]},
    meck:new(dao_helper),
    meck:expect(dao_helper, ensure_db_exists, fun(?SYSTEM_DB_NAME) -> ok end),
    meck:expect(dao_helper, open_doc, fun(?SYSTEM_DB_NAME, "test_id") -> {ok, #doc{body = DbObj}} end),
    meck:expect(dao_helper, name, fun(Arg) -> meck:passthrough([Arg]) end),
    #some_record{field1 = 1, field3 = true} = dao:get_record("test_id"),
    true = meck:validate(dao_helper),
    meck:unload(dao_helper),
    pass.

remove_record_test() ->
    meck:new(dao_helper),
    meck:expect(dao_helper, ensure_db_exists, fun(?SYSTEM_DB_NAME) -> ok end),
    meck:expect(dao_helper, delete_doc, fun(?SYSTEM_DB_NAME, "test_id") -> ok end),
    meck:expect(dao_helper, name, fun(Arg) -> meck:passthrough([Arg]) end),
    ok = dao:remove_record("test_id"),
    true = meck:validate(dao_helper),
    meck:unload(dao_helper),
    pass.


is_valid_record_test() ->
    true = dao:is_valid_record(#some_record{}),
    true = dao:is_valid_record(some_record),
    true = dao:is_valid_record("some_record"),
    false = dao:is_valid_record({some_record, field1}).

term_to_doc_test() ->
    15 = dao:term_to_doc(15),
    -15.43 = dao:term_to_doc(-15.43),
    true = dao:term_to_doc(true),
    false = dao:term_to_doc(false),
    AtomRes = list_to_binary(string:concat(?RECORD_FIELD_ATOM_PREFIX, "test_atom")),
    AtomRes = dao:term_to_doc(test_atom),
    null = dao:term_to_doc(null),
    <<"test:test2 ]test4{test 5=+ 6">> = dao:term_to_doc("test:test2 ]test4{test 5=+ 6"),
    Ans = {[{<<?RECORD_META_FIELD_NAME>>, <<"some_record">>},
        {<<"field1">>,
            {[{<<"tuple_field_1">>, <<"rec1">>},
                {<<"tuple_field_2">>,
                    {[{<<?RECORD_META_FIELD_NAME>>, <<"some_record">>},
                        {<<"field1">>, list_to_binary(string:concat(?RECORD_FIELD_ATOM_PREFIX, "test_atom"))},
                        {<<"field2">>, 5},
                        {<<"field3">>, <<>>}]}}]}},
        {<<"field2">>, <<"test string">>},
        {<<"field3">>,
            [1,
                {[{<<"tuple_field_1">>, 6.53},
                    {<<"tuple_field_2">>,
                        [true,
                            {[{<<"tuple_field_1">>, <<"test1">>}, {<<"tuple_field_2">>, false}]}]}]},
                5.4, <<<<?RECORD_FIELD_BINARY_PREFIX>>/binary, <<1,2,3>>/binary>>,
                [1, 0, <<"test">>]]}]},
    Ans = dao:term_to_doc(#some_record{field1 = {"rec1", #some_record{field1 = test_atom, field2 = 5}}, field2 = "test string", field3 = [1, {6.53, [true, {"test1", false}]}, 5.4, <<1,2,3>>, [1, 0, "test"]]}).

doc_to_term_test() ->
    test_atom = dao:doc_to_term(list_to_binary(string:concat(?RECORD_FIELD_ATOM_PREFIX, "test_atom"))),
    Ans = {[{<<?RECORD_META_FIELD_NAME>>, <<"some_record">>},
        {<<"field1">>,
            {[{<<"tuple_field_1">>, <<"rec1">>},
                {<<"tuple_field_2">>,
                    {[{<<?RECORD_META_FIELD_NAME>>, <<"some_record">>},
                        {<<"field1">>, list_to_binary(string:concat(?RECORD_FIELD_ATOM_PREFIX, "test_atom"))},
                        {<<"field2">>, 5},
                        {<<"field4">>, <<"tt">>}]}}]}},
        {<<"field2">>, <<"test string">>},
        {<<"field3">>,
            [1,
                {[{<<"tuple_field_1">>, 6.53},
                    {<<"tuple_field_2">>,
                        [{[{<<?RECORD_META_FIELD_NAME>>, <<"unknown_record">>}, {<<"f2">>, 1}, {<<"f1">>, 5}]},
                            {[{<<"tuple_field_2">>, false}, {<<"tuple_field_1">>, <<"test1">>}]}]}]},
                5.4, <<<<?RECORD_FIELD_BINARY_PREFIX>>/binary, <<1,2,3>>/binary>>,
                [1, 0, <<"test">>]]}]},
    {some_record, {"rec1", {some_record, test_atom, 5, []}},
        "test string", [1, {6.53, [{unknown_record, 1, 5}, {"test1", false}]}, 5.4, <<1,2,3>>, [1, 0, "test"]]} = dao:doc_to_term(Ans).

-endif.