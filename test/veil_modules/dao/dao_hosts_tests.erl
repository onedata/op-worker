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

main_test_() ->
    case node() of
        nonode@nohost ->
            {ok, _Pid} = net_kernel:start([master, shortnames]);
        _ -> ok
    end,
    {setup, local, fun start_link/0, fun cleanUp/1, [fun insert_hosts/0, fun get_host/0, fun call/0]}.

insert_hosts() ->
    ok = dao_hosts:insert('test@host1'),
    ok = dao_hosts:insert('test@host2'),
    ok = dao_hosts:insert('test@host3').

get_host() ->
    {error, no_db_host_found} = dao_hosts:get_random(),
    Self = node(),
    ok = dao_hosts:insert(Self),
    Self = dao_hosts:get_random(),
    lists:foreach(fun(X) -> ok = dao_hosts:insert(start_node(list_to_atom("test" ++ integer_to_list(X)))) end, lists:seq(1,3)),
    Db = dao_hosts:get_host(),
    Db = dao_hosts:get_host(),
    Db = dao_hosts:get_host().

call() ->
    ok = dao_hosts:call(?MODULE, call_resp, []).

call_resp() ->
    ok.


start_link() ->
    {ok, Pid} = dao_hosts:start_link([]),
    Pid.

cleanUp(Pid) ->
    net_kernel:stop(),
    monitor(process, Pid),
    Pid ! {self(), shutdown},
    receive {'DOWN', _Ref, process, Pid, normal} -> ok after 1000 -> error(timeout) end.


start_node(Name) ->
    [_, HostStr] = string:tokens(atom_to_list(node()), "@"),
    Host = list_to_atom(HostStr),
    Node =
    case slave:start_link(Host, Name) of
        {ok, N} -> N;
        {error, {already_running, N}} -> N
    end,
    ?LOAD_TEST_NODE(Node),
    Node.

-endif.