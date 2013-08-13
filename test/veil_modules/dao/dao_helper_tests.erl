%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao_helper module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(dao_helper_tests).

%% TODO słabo przetestowane sytuacje wyjątkowe (większość lini odpowiadających
%% za obsługę błędu nie użyta w czasie testów) - poprawić

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/couch_db.hrl").
-endif.

-ifdef(TEST).

name_test() ->
    ?assertEqual(<<"test">>, dao_helper:name("test")),
    ?assertEqual(<<"test">>, dao_helper:name(<<"test">>)),
    ?assertEqual(<<"test">>, dao_helper:name(test)).

normalizer_test() ->
    ?assertEqual(ok, dao_helper:normalize_return_term(ok)),
    ?assertEqual({error, err}, dao_helper:normalize_return_term(err)),
    ?assertEqual({error, err}, dao_helper:normalize_return_term({error, err})),
    ?assertEqual({ok, "ret"}, dao_helper:normalize_return_term({ok, "ret"})),
    ?assertEqual({error, {exit_error, ret}}, dao_helper:normalize_return_term({badrpc, {'EXIT', {ret, moar_details}}})),
    ?assertEqual({error, {exit, ret}}, dao_helper:normalize_return_term({badrpc, {'EXIT', ret}})).


%% Fixtures
main_test_() ->
    {foreach, fun setup/0, fun teardown/1, [fun list_dbs/0, fun list_dbs_error/0, fun get_doc_count/0,
        fun get_doc_count_error/0, fun get_db_info/0, fun get_db_info_error/0,fun create_db1/0, fun create_db2/0,
        fun create_db_error/0, fun delete_db/0, fun open_doc/0, fun insert_doc/0, fun delete_doc/0,
        fun delete_doc_error/0,fun create_view/0, fun query_view/0]}.

setup() ->
    meck:new(dao_hosts).

teardown(_) ->
    ok = meck:unload(dao_hosts).

list_dbs() ->
    meck:expect(dao_hosts, call, fun(all_dbs, [<<"Name">>]) -> {ok, [<<"test1">>, <<"test2">>]} end),
    ?assertEqual({ok, ["test1", "test2"]}, dao_helper:list_dbs("Name")),
    ?assert(meck:validate(dao_hosts)).

list_dbs_error() ->
  meck:expect(dao_hosts, call, fun(all_dbs, [<<"Name">>]) -> some_error end),
  ?assertEqual({error, some_error}, dao_helper:list_dbs("Name")),
  ?assert(meck:validate(dao_hosts)).

get_db_info() ->
    meck:expect(dao_hosts, call, fun(get_db_info, [<<"Name">>]) -> {ok, []} end),
    ?assertEqual({ok, []}, dao_helper:get_db_info("Name")).

get_db_info_error() ->
  meck:expect(dao_hosts, call, fun(get_db_info, [<<"Name">>]) ->  {error, {exit_error, database_does_not_exist}} end),
  ?assertEqual({error, database_does_not_exist}, dao_helper:get_db_info("Name")).

get_doc_count() ->
    meck:expect(dao_hosts, call, fun(get_doc_count, [<<"Name">>]) -> {ok, 5} end),
    ?assertEqual({ok, 5}, dao_helper:get_doc_count("Name")),
    ?assert(meck:validate(dao_hosts)).

get_doc_count_error() ->
  meck:expect(dao_hosts, call, fun(get_doc_count, [<<"Name">>]) -> {error, {exit_error, database_does_not_exist}} end),
  ?assertEqual({error, database_does_not_exist}, dao_helper:get_doc_count("Name")),
  ?assert(meck:validate(dao_hosts)).

create_db1() ->
    meck:expect(dao_hosts, call, fun(create_db, [<<"Name">>, []]) -> accepted end),
    ?assertEqual(ok, dao_helper:create_db("Name")),
    ?assert(meck:validate(dao_hosts)).

create_db2() ->
    meck:expect(dao_hosts, call, fun(create_db, [<<"Name">>, _]) -> accepted end),
    ?assertEqual(ok, dao_helper:create_db("Name", [{q, "5"}])),
    ?assert(meck:validate(dao_hosts)).

create_db_error() ->
  meck:expect(dao_hosts, call, fun(create_db, [<<"Name">>, _]) -> {error, file_exists} end),
  ?assertEqual(ok, dao_helper:create_db("Name", [{q, "5"}])),
  ?assert(meck:validate(dao_hosts)).

delete_db() ->
    meck:expect(dao_hosts, call, fun(delete_db, [<<"Name">>, []]) -> {badrpc, {'EXIT', {database_does_not_exist, test}}} end),
    ?assertEqual({error, database_does_not_exist}, dao_helper:delete_db("Name")),
    ?assert(meck:validate(dao_hosts)).

open_doc() ->
    meck:expect(dao_hosts, call, fun(open_doc, [<<"Name">>, <<"ID">>, []]) -> {ok, {some, document, from, db}} end),
    ?assertEqual({ok, {some, document, from, db}}, dao_helper:open_doc("Name", "ID")),
    ?assert(meck:validate(dao_hosts)).

insert_doc() ->
    meck:sequence(dao_hosts, call, 2, [{ok, {1, <<3, 4, 5>>}}, conflict]),
    ?assertEqual({ok, {1, <<3, 4, 5>>}}, dao_helper:insert_doc("Name", #doc{})),
    ?assertEqual({error, conflict}, dao_helper:insert_doc("Name", #doc{})),
    ?assert(meck:validate(dao_hosts)).

delete_doc() ->
    meck:expect(dao_hosts, call, fun(open_doc, [<<"Name">>, <<"ID">>, []]) ->
        {ok, #doc{id = <<"ID">>}};
        (update_doc, [<<"Name">>, #doc{id = <<"ID">>, deleted = true}, []]) ->
            accepted end),
    ?assertEqual(ok, dao_helper:delete_doc(<<"Name">>, <<"ID">>)),
    meck:expect(dao_hosts, call, fun(open_doc, [<<"Name">>, <<"ID">>, []]) ->
        {not_found, missing};
        (update_doc, [<<"Name">>, #doc{id = <<"ID">>, deleted = true}, []]) ->
            accepted end),
    ?assertEqual({error, missing}, dao_helper:delete_doc(<<"Name">>, <<"ID">>)),
    ?assert(meck:validate(dao_hosts)).

delete_doc_error() ->
  meck:expect(dao_hosts, call, fun(open_doc, [<<"Name">>, <<"ID">>, []]) ->
    {error, missing} end),
  ?assertEqual({error, missing}, dao_helper:delete_doc(<<"Name">>, <<"ID">>)).

create_view() ->
    meck:expect(dao_hosts, call, fun(open_doc, [<<"Name">>, <<"_design/des">>, []]) ->
        {not_found, missing};
        (update_doc, [<<"Name">>, #doc{id = <<"_design/des">>}, [{user_ctx, {user_ctx, null, [<<"_admin">>], undefined}}]]) ->
            accepted end),
    ?assertEqual(ok, dao_helper:create_view("Name", "des", "view", "map func", "red_func", 1)),
    ?assert(meck:validate(dao_hosts)).

query_view() ->
    TmpRes = {ok, [{total_and_offset, 3, 0},
        {row, {[{id, <<"9948b33efb7089c7896254f7b20004ce">>},
            {key, null},
            {value, {[{<<"_id">>, <<"9948b33efb7089c7896254f7b20004ce">>},
                {<<"_rev">>,
                    <<"1-967a00dff5e02add41819138abb3284d">>}]}}]}},
        {row, {[{id, <<"9948b33efb7089c7896254f7b20008fc">>},
            {key, null},
            {value, {[{<<"_id">>, <<"9948b33efb7089c7896254f7b20008fc">>},
                {<<"_rev">>,
                    <<"1-967a00dff5e02add41819138abb3284d">>}]}}]}},
        {row, {[{id, <<"9948b33efb7089c7896254f7b2000a13">>},
            {key, null},
            {value, {[{<<"_id">>, <<"9948b33efb7089c7896254f7b2000a13">>},
                {<<"_rev">>,
                    <<"2-7051cbe5c8faecd085a3fa619e6e6337">>}]}}]}}]},
    meck:expect(dao_hosts, call, fun(query_view, [<<"Name">>, <<"des">>, <<"view">>, #view_query_args{view_type = map}]) -> TmpRes end),
    ?assertMatch(TmpRes, dao_helper:query_view("Name", "des", "view", #view_query_args{})),
    ?assert(meck:validate(dao_hosts)).

gen_uuid_test() ->
    ?assertEqual(32, length(dao_helper:gen_uuid())).

-endif.