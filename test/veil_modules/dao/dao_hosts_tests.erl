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
-export([call_resp/0]).

-endif.

-ifdef(TEST).

init_test() ->
    Pid = spawn(fun dao_hosts:init/0),
    receive after 10 -> ok end,
    Info = ets:info(db_host_store),
    Pid ! {self(), shutdown},
    ?assert(is_list(Info)).

host_management_test_() ->
    {setup, local, fun start_link/0, fun cleanUp/1,
     [fun delete_hosts/0, fun insert_hosts/0, fun get_host/0, fun delete_hosts/0, fun ban_host/0, fun reactivate_host/0]}.

call_test_() ->
    case node() of
        nonode@nohost ->
            {ok, _Pid} = net_kernel:start([master, longnames]);
        _ -> ok
    end,
    {setup, local, fun start_link/0, fun cleanUp/1,
     [fun call/0, fun ping/0]}.

ping() ->
    pang = dao_hosts:ping('test@host1.lan', 50),
    pong = dao_hosts:ping(node(), 50).

insert_hosts() ->
    ok = dao_hosts:insert('test@host1.lan'),
    ok = dao_hosts:insert('test@host2.lan'),
    ok = dao_hosts:insert('test@host3.lan'),
    ok = dao_hosts:insert('test@host3.lan'),
    3 = length(ets:lookup(db_host_store, host)).

delete_hosts() ->
    ok = dao_hosts:delete('test@host1.lan'),
    ok = dao_hosts:delete('test@host2.lan'),
    ok = dao_hosts:delete('test@host3.lan'),
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
    Db1 = dao_hosts:get_host(),
    Db1 = dao_hosts:get_host(),
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
    Db2 = dao_hosts:get_host().

call() ->
    delete_hosts(),
    ok = dao_hosts:insert('test@host1.lan'),
    ok = dao_hosts:insert('test@host2.lan'),
    {error, rpc_retry_limit_exceeded} = dao_hosts:call(?MODULE, call_resp, []),
    dao_hosts:insert(node()),
    ok = dao_hosts:call(?MODULE, call_resp, []).

ban_host() ->
    {error, no_host} = dao_hosts:ban('test@host1.lan'),
    ok = dao_hosts:insert('test@host1.lan'),
    ok = dao_hosts:insert('test@host2.lan'),
    ok = dao_hosts:ban('test@host1.lan'),
    ok = dao_hosts:ban('test@host2.lan', 10),
    ok = dao_hosts:ban('test@host2.lan', 10),
    receive
    after 20 ->
	    'test@host2.lan' = dao_hosts:get_host()
    end.

reactivate_host() ->
    {error, no_host} = dao_hosts:ban('test@host3.lan'),
    dao_hosts:ban('test@host2.lan'),
    dao_hosts:ban('test@host2.lan'),
    dao_hosts:reactivate('test@host1.lan'),
    'test@host1.lan' = dao_hosts:get_host().

call_resp() ->
    ok.


start_link() ->
    put(db_host, undefined),
    {ok, Pid} = dao_hosts:start_link([]),
    Pid.

cleanUp(Pid) ->
    net_kernel:stop(),
    monitor(process, Pid),
    Pid ! {self(), shutdown},
    receive {'DOWN', _Ref, process, Pid, normal} -> ok after 1000 -> error(timeout) end.


%% start_node(Name) ->
%%     [_, HostStr] = string:tokens(atom_to_list(node()), "@"),
%%     Host = list_to_atom(HostStr),
%%     Node =
%%     case slave:start_link(Host, Name) of
%%         {ok, N} -> N;
%%         {error, {already_running, N}} -> N
%%     end,
%%     ?LOAD_TEST_NODE(Node),
%%     Node.

-endif.