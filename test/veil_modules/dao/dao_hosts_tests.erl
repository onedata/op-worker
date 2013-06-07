%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of dao_hosts module.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================
-module(dao_hosts_tests).


-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/dao/common.hrl").
-include("supervision_macros.hrl").
-include("registered_names.hrl").

-endif.

-ifdef(TEST).

host_management_test_() ->
    {setup, fun setup/0, fun teardown/1,
        {inorder, [fun delete_hosts/0, fun insert_hosts/0, fun get_host/0, fun delete_hosts/0, fun ban_host/0, fun reactivate_host/0]}}.

call_test_() ->
    {foreach, fun setup/0, fun teardown/1,
        [fun call/0, fun ping/0]}.

setup() ->
    process_flag(trap_exit, true),
    meck:new(rpc, [unstick, passthrough]),
    meck:new(net_adm, [unstick, passthrough]),
    put(db_host, undefined),
    worker_host:start_link(dao, [], 10).

teardown({ok, Pid}) ->
    Unload = meck:unload([rpc, net_adm]),
    exit(Pid, shutdown),
    Shutdown = receive {'EXIT', Pid,shutdown} -> ok after 100 -> teardown_timeout end,
    ?assertEqual([ok, ok], [Unload, Shutdown]);
teardown({error, {already_started, Pid}}) ->
    Unload = meck:unload([rpc, net_adm]),
    exit(Pid, shutdown),
    Shutdown = receive {'EXIT', Pid,shutdown} -> ok after 100 -> teardown_timeout end,
    ?assertEqual([ok, ok], [Unload, Shutdown]);
teardown(_) ->
    ok = meck:unload([rpc, net_adm]).


ping() ->
    meck:expect(net_adm, ping, fun('test@host1.lan') -> receive after 10000 -> pang end;
        ('real@host.lan') -> pong end),
    ?assertEqual(pang, dao_hosts:ping('test@host1.lan', 50)),
    ?assertEqual(pong, dao_hosts:ping('real@host.lan', 50)),
    ?assert(meck:validate(net_adm)).

insert_hosts() ->
    ?assertEqual(ok, dao_hosts:insert('test@host1.lan')),
    ?assertEqual(ok, dao_hosts:insert('test@host2.lan')),
    ?assertEqual(ok, dao_hosts:insert('test@host3.lan')),
    ?assertEqual(ok, dao_hosts:insert('test@host3.lan')),
    ?assertEqual(3, length(ets:lookup(db_host_store, host))).

delete_hosts() ->
    ?assertEqual(ok, dao_hosts:delete('test@host1.lan')),
    ?assertEqual(ok, dao_hosts:delete('test@host2.lan')),
    ?assertEqual(ok, dao_hosts:delete('test@host3.lan')),
    delete_host(dao_hosts:get_host()).

delete_host({error, no_db_host_found}) ->
    ok;
delete_host(Host) ->
    dao_hosts:delete(Host),
    delete_host(dao_hosts:get_host()).

get_host() ->
    case dao_hosts:get_random() of
        'test@host1.lan' -> ok;
        'test@host2.lan' -> ok;
        'test@host3.lan' -> ok
    end,
    Db1 = dao_hosts:get_host(),
    ?assertEqual(Db1, dao_hosts:get_host()),
    ?assertEqual(Db1, dao_hosts:get_host()),
    dao_hosts:ban(Db1),
    Db2 = dao_hosts:get_host(),
    ?assertNot(Db1 =:= Db2),
    dao_hosts:ban(Db2),
    Db3 = dao_hosts:get_host(),
    ?assertNot(Db1 =:= Db3 orelse Db2 =:= Db3),
    dao_hosts:ban(Db3),
    case dao_hosts:get_random() of
        'test@host1.lan' -> ok;
        'test@host2.lan' -> ok;
        'test@host3.lan' -> ok
    end,
    dao_hosts:reactivate(Db2),
    ?assertEqual(Db2, dao_hosts:get_host()).

call() ->
    delete_hosts(),
    ?assertEqual(ok, dao_hosts:insert('test@host1.lan')),
    ?assertEqual(ok, dao_hosts:insert('test@host2.lan')),
    meck:expect(net_adm, ping, fun('real@host.lan') -> pong;
        (_) -> pang end),
    meck:expect(rpc, call, fun('real@host.lan', ?MODULE, call_resp, []) -> ok;
        (_, ?MODULE, call_resp, []) -> {badrpc, nodedown} end),
    ?assertEqual({error, rpc_retry_limit_exceeded}, dao_hosts:call(?MODULE, call_resp, [])),
    dao_hosts:insert('real@host.lan'),
    ?assertEqual(ok, dao_hosts:call(?MODULE, call_resp, [])),
    ?assert(meck:validate(rpc)),
    ?assert(meck:validate(net_adm)).

ban_host() ->
    ?assertEqual({error, no_host}, dao_hosts:ban('test@host1.lan')),
    ?assertEqual(ok, dao_hosts:insert('test@host1.lan')),
    ?assertEqual(ok, dao_hosts:insert('test@host2.lan')),
    ?assertEqual(ok, dao_hosts:ban('test@host1.lan')),
    ?assertEqual(ok, dao_hosts:ban('test@host2.lan', 10)),
    ?assertEqual(ok, dao_hosts:ban('test@host2.lan', 10)),
    receive
    after 20 ->
        ?assertEqual('test@host2.lan', dao_hosts:get_host())
    end.

reactivate_host() ->
    ?assertEqual({error, no_host}, dao_hosts:ban('test@host3.lan')),
    dao_hosts:ban('test@host2.lan'),
    dao_hosts:ban('test@host2.lan'),
    dao_hosts:reactivate('test@host1.lan'),
    ?assertEqual('test@host1.lan', dao_hosts:get_host()).

-endif.