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
-endif.

-ifdef(TEST).

main_test_() ->
    {setup, local, fun init/0, fun cleanUp/1, [fun list_dbs/0, fun get_doc_count/0, fun get_db_info/0,
					       fun create_db1/0, fun create_db2/0, fun delete_db/0]}.

name_test() ->
    <<"test">> = dao_helper:name("test"),
    <<"test">> = dao_helper:name(<<"test">>).

init() ->
    put(db_host, undefined),
    Pid = spawn(dao_hosts, init, []),
    register(db_host_store_proc, Pid),
    receive after 20 -> Pid end.

cleanUp(Pid) ->
    Pid ! {self(), shutdown}.

list_dbs() ->
    case dao_helper:list_dbs() of
        {ok, List} when is_list(List) -> ok;
        {error, _} -> ok
    end.

get_db_info() ->
    case dao_helper:get_db_info("test") of
        {ok, List} when is_list(List) -> ok;
        {error, _} -> ok
    end.

get_doc_count() ->
    case dao_helper:get_doc_count("test") of
        {ok, Count} when is_integer(Count) -> ok;
        {error, _} -> ok
    end.

create_db1() ->
    case dao_helper:create_db("test") of
        ok -> ok;
        {error, _} -> ok
    end.

create_db2() ->
    case dao_helper:create_db("test", [{q, "1"}]) of
        ok -> ok;
        {error, _} -> ok
    end.

delete_db() ->
    case dao_helper:delete_db("test") of
        ok -> ok;
        {error, _} -> ok
    end.


-endif.