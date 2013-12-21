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

%% TODO nie przetestowane widoki (można to zrobić w teście ct) - ogólnie przydałby się jakiś integracyjny test dao w ct
%% TODO nie przetestowane listowanie rekordów (funkcja ogólna)

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/couch_db.hrl").
-endif.

-ifdef(TEST).

main_test_() ->
    {foreach, fun setup/0, fun teardown/1, [fun order_suite/1]}.

records_test_() ->
    {foreach, fun setup/0, fun teardown/1, [fun save_record/0, fun get_record/0, fun remove_record/0]}.

order_suite(_) ->
    {inorder, [fun init/0, fun handle/0, fun cleanup/0]}.

setup() ->
    meck:new([dao_helper, dao_vfs]).

teardown(_) ->
    ok = meck:unload([dao_helper, dao_vfs]).

init() ->
    InitAns1 = dao:init([]),
    ?assert(is_record(InitAns1, initial_host_description)),
    ?assertEqual(ok, InitAns1#initial_host_description.plug_in_state),
    worker_host:stop_all_sub_proc(InitAns1#initial_host_description.sub_procs),

    InitAns2 = dao:init([]),
    ?assert(is_record(InitAns2, initial_host_description)),
    ?assertEqual(ok, InitAns2#initial_host_description.plug_in_state),
    worker_host:stop_all_sub_proc(InitAns2#initial_host_description.sub_procs).

handle() ->
    ?assertNotEqual({error, wrong_args}, dao:handle(1, {helper, test, []})),
    ?assertNotEqual({error, wrong_args}, dao:handle(1, {hosts, test, []})),
    ?assertNotEqual({error, wrong_args}, dao:handle(1, {test, []})),
    ?assertEqual({error, wrong_args}, dao:handle(1, {"wrong", test, []})),
    ?assertEqual({error, undef}, dao:handle(1, {wrong, test, []})),
    meck:expect(dao_vfs, list_dir, fun(_, _, _) -> ok end),
    ?assertEqual(ok, dao:handle(1, {vfs, list_dir, [test, test, test]})),
    ?assert(meck:validate(dao_vfs)).

cleanup() ->
    ?assertEqual(ok, dao:cleanup()).


save_record() ->
    meck:expect(dao_helper, gen_uuid, fun() -> "uuid" end),
    meck:expect(dao_helper, open_doc, fun(?SYSTEM_DB_NAME, "test_id") -> {error, not_found} end),
    meck:expect(dao_helper, name, fun(Arg) -> meck:passthrough([Arg]) end),

    ?assertException(throw, unsupported_record, dao:save_record({a, b, c})),
    ?assertException(throw, unsupported_record, dao:save_record({some_record, a, c})),
    ToInsert = {some_record, a, b, c},
    DbObj = dao:term_to_doc(ToInsert),

    meck:expect(dao_helper, insert_doc, fun(?SYSTEM_DB_NAME, #doc{id = Id, body = DbObj1}) -> DbObj1 = DbObj, {ok, Id} end),

    ?assertMatch({ok, "test_id"}, dao:save_record(#veil_document{record = ToInsert, uuid = "test_id"})),
    ?assertMatch({ok, "test_id"}, dao:save_record(#veil_document{record = ToInsert, uuid = "test_id", force_update = true})),
    ?assert(meck:validate(dao_helper)),

    meck:expect(dao_helper, open_doc, fun(?SYSTEM_DB_NAME, "test_id") -> {ok, #doc{revs = {1, [<<"123">>]}}} end),
    meck:expect(dao_helper, insert_doc, fun(?SYSTEM_DB_NAME, #doc{id = Id, revs = {1, [<<"123">>]}, body = DbObj1}) -> DbObj1 = DbObj, {ok, Id} end),
    ?assertMatch({ok, "test_id"}, dao:save_record(#veil_document{record = ToInsert, uuid = "test_id", force_update = true})),
    ?assert(meck:validate(dao_helper)).

get_record() ->
    DbObj = {[{<<?RECORD_META_FIELD_NAME>>, <<"some_record">>}, {<<"field1">>, 1}, {<<"field3">>, true}]},
    meck:expect(dao_helper, open_doc, fun(?SYSTEM_DB_NAME, "test_id") -> {ok, #doc{body = DbObj}} end),
    meck:expect(dao_helper, name, fun(Arg) -> meck:passthrough([Arg]) end),
    ?assertMatch({ok, #veil_document{record = #some_record{field1 = 1, field3 = true}}}, dao:get_record("test_id")),
    ?assert(meck:validate(dao_helper)).

remove_record() ->
    meck:expect(dao_helper, delete_doc, fun(?SYSTEM_DB_NAME, "test_id") -> ok end),
    meck:expect(dao_helper, name, fun(Arg) -> meck:passthrough([Arg]) end),
    ?assertEqual(ok, dao:remove_record("test_id")),
    ?assert(meck:validate(dao_helper)).


is_valid_record_test() ->
    ?assert(dao:is_valid_record(#some_record{})),
    ?assert(dao:is_valid_record(some_record)),
    ?assert(dao:is_valid_record("some_record")),
    ?assertNot(dao:is_valid_record({some_record, field1})).

term_to_doc_test() ->
    ?assertEqual(15, dao:term_to_doc(15)),
    ?assertEqual(-15.43, dao:term_to_doc(-15.43)),
    ?assert(dao:term_to_doc(true)),
    ?assertNot(dao:term_to_doc(false)),
    AtomRes = list_to_binary(string:concat(?RECORD_FIELD_ATOM_PREFIX, "test_atom")),
    ?assertMatch(AtomRes, dao:term_to_doc(test_atom)),
    ?assertEqual(null, dao:term_to_doc(null)),
    ?assertMatch(<<"test:test2 ]test4{test 5=+ 6">>, dao:term_to_doc("test:test2 ]test4{test 5=+ 6")),
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
                [1, list_to_binary(?RECORD_FIELD_PID_PREFIX ++ pid_to_list(self())), <<"test">>]]}]},
    ?assertMatch(Ans, dao:term_to_doc(#some_record{field1 = {"rec1", #some_record{field1 = test_atom, field2 = 5}},
                                                   field2 = "test string", field3 = [1, {6.53, [true, {"test1", false}]}, 5.4, <<1,2,3>>, [1, self(), "test"]]})).

doc_to_term_test() ->
    ?assertEqual(test_atom, dao:doc_to_term(list_to_binary(string:concat(?RECORD_FIELD_ATOM_PREFIX, "test_atom")))),
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
                [1, list_to_binary(?RECORD_FIELD_PID_PREFIX ++ pid_to_list(self())), <<"test">>]]}]},
    SPid = self(),
    Out = {some_record, {"rec1", {some_record, test_atom, 5, []}},
        "test string", [1, {6.53, [{unknown_record, 1, 5}, {"test1", false}]}, 5.4, <<1,2,3>>, [1, SPid, "test"]]},
    ?assertMatch(Out, dao:doc_to_term(Ans)).


doc_to_term_wrong_data_test() ->
  Ans = try
    dao:doc_to_term({some_atom})
  catch
    Ex -> Ex
  end,
  ?assertEqual(Ans, invalid_document).

term_to_doc_wrong_data_test() ->
  Field = fun() ->
    ok
  end,
  Ans = try
    dao:term_to_doc(Field)
  catch
    Ex -> Ex
  end,
  ?assertEqual(Ans, {unsupported_field, Field}).

get_set_db_test() ->
    ?assertEqual(?DEFAULT_DB, dao:get_db()),
    dao:set_db("db"),
    ?assertEqual("db", dao:get_db()).

-endif.