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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/couch_db.hrl").
-endif.

-ifdef(TEST).

name_test() ->
    <<"test">> = dao_helper:name("test"),
    <<"test">> = dao_helper:name(<<"test">>),
    <<"test">> = dao_helper:name(test).

normalizer_test() ->
    case node() of
        nonode@nohost ->
            {ok, _Pid} = net_kernel:start([master, longnames]);
        _ -> ok
    end,
    ok = dao_helper:normalize_return_term(ok),
    {error, err} = dao_helper:normalize_return_term(err),
    {error, err} = dao_helper:normalize_return_term({error, err}),
    {ok, "ret"} = dao_helper:normalize_return_term({ok, "ret"}),
    {error, {exit, ret}} = dao_helper:normalize_return_term(rpc:call(node(), erlang, exit, [ret])),
    {error, {exit_error, ret}} = dao_helper:normalize_return_term(rpc:call(node(), erlang, error, [ret])),
    net_kernel:stop().


%% Fixtures
main_test_() ->
    {setup, local, fun setup/0, fun teardown/1, [fun list_dbs/0, fun get_doc_count/0, fun get_db_info/0,
        fun create_db1/0, fun create_db2/0, fun delete_db/0, fun open_doc/0, fun insert_doc/0, fun delete_doc/0,
        fun create_view/0, fun query_view/0]}.

setup() ->
    meck:new(dao_hosts, [unstick, passthrough]),
    put(db_host, undefined),
    Pid = spawn(dao_hosts, init, []),
    register(db_host_store_proc, Pid),
    receive after 20 -> Pid end.

teardown(Pid) ->
    meck:unload(dao_hosts),
    Pid ! {self(), shutdown}.

list_dbs() ->
    case dao_helper:list_dbs() of
        {ok, List} when is_list(List) -> ok;
        {error, _} -> ok
    end,
    meck:expect(dao_hosts, call, fun(all_dbs, [<<"Name">>]) ->
        {ok, [<<"test1">>, <<"test2">>]};
        (_, _) -> meck:passthrough() end),
    {ok, ["test1", "test2"]} = dao_helper:list_dbs("Name"),

    ?assert(meck:validate(dao_hosts)).

get_db_info() ->
    case dao_helper:get_db_info("test") of
        {ok, List} when is_list(List) -> ok;
        {error, _} -> ok
    end.

get_doc_count() ->
    case dao_helper:get_doc_count("test") of
        {ok, Count} when is_integer(Count) -> ok;
        {error, _} -> ok
    end,
    meck:expect(dao_hosts, call, fun(get_doc_count, [<<"Name">>]) ->
        {ok, 5};
        (_, _) -> meck:passthrough() end),
    {ok, 5} = dao_helper:get_doc_count("Name"),
    ?assert(meck:validate(dao_hosts)).

create_db1() ->
    case dao_helper:create_db("test") of
        ok -> ok;
        {error, _} -> ok
    end,
    meck:expect(dao_hosts, call, fun(create_db, [<<"Name">>, []]) ->
        accepted;
        (_, _) -> meck:passthrough() end),
    ok = dao_helper:create_db("Name"),
    ?assert(meck:validate(dao_hosts)).

create_db2() ->
    case dao_helper:create_db("test", [{q, "1"}]) of
        ok -> ok;
        {error, _} -> ok
    end,
    meck:expect(dao_hosts, call, fun(create_db, [<<"Name">>, _]) ->
        accepted;
        (_, _) -> meck:passthrough() end),
    ok = dao_helper:create_db("Name", [{q, "5"}]),
    ?assert(meck:validate(dao_hosts)).

delete_db() ->
    case dao_helper:delete_db("test") of
        ok -> ok;
        {error, _} -> ok
    end,
    meck:expect(dao_hosts, call, fun(delete_db, [<<"Name">>, []]) ->
        {badrpc, {'EXIT', {database_does_not_exist, test}}};
        (_, _) -> meck:passthrough() end),
    {error, database_does_not_exist} = dao_helper:delete_db("Name"),
    ?assert(meck:validate(dao_hosts)).

open_doc() ->
    meck:expect(dao_hosts, call, fun(open_doc, [<<"Name">>, <<"ID">>, []]) ->
        {ok, {some, document, from, db}};
        (_, _) -> meck:passthrough() end),
    {ok, {some, document, from, db}} = dao_helper:open_doc("Name", "ID"),
    ?assert(meck:validate(dao_hosts)).

insert_doc() ->
    meck:sequence(dao_hosts, call, 2, [{ok, {1, <<3, 4, 5>>}}, conflict]),
    {ok, {1, <<3, 4, 5>>}} = dao_helper:insert_doc("Name", #doc{}),
    {error, conflict} = dao_helper:insert_doc("Name", #doc{}),
    ?assert(meck:validate(dao_hosts)).

delete_doc() ->
    meck:expect(dao_hosts, call, fun(open_doc, [<<"Name">>, <<"ID">>, []]) ->
        {ok, #doc{id = <<"ID">>}};
        (update_doc, [<<"Name">>, #doc{id = <<"ID">>, deleted = true}, []]) ->
            accepted end),
    ok = dao_helper:delete_doc(<<"Name">>, <<"ID">>),
    meck:expect(dao_hosts, call, fun(open_doc, [<<"Name">>, <<"ID">>, []]) ->
        {not_found, missing};
        (update_doc, [<<"Name">>, #doc{id = <<"ID">>, deleted = true}, []]) ->
            accepted end),
    {error, missing} = dao_helper:delete_doc(<<"Name">>, <<"ID">>),
    ?assert(meck:validate(dao_hosts)).

create_view() ->
    meck:expect(dao_hosts, call, fun(open_doc, [<<"Name">>, <<"_design/des">>, []]) ->
        {not_found, missing};
        (update_doc, [<<"Name">>, #doc{id = <<"_design/des">>}, [{user_ctx, {user_ctx, null, [<<"_admin">>], undefined}}]]) ->
            accepted end),
    ok = dao_helper:create_view("Name", "des", "view", "map func", "red_func"),
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
    TmpRes = dao_helper:query_view("Name", "des", "view", #view_query_args{}),
    ?assert(meck:validate(dao_hosts)).


-endif.