%% ===================================================================
%% @author Michał Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc:This test checks how many simple messages may be sent and received.
%% It uses very fast Erlang ping so it measures the limits of the infrastructure.
%% @end
%% ===================================================================

-module(veilcluster_driver_limit).
-export([new/1, run/4]).

-include("basho_bench.hrl").

new(Id) ->
    ?DEBUG("limitlog: test started with id: ~p~n", [Id]),
    Hosts = basho_bench_config:get(cluster_erlang_nodes),
    try
      ?DEBUG("limitlog: hosts: ~p~n", [Hosts]),
      H1 = os:cmd("hostname"),
      H2 = string:substr(H1, 1, length(H1) - 1),
      ?DEBUG("limitlog: host name: ~p~n", [H2]),
      Ans1 = net_kernel:start([list_to_atom("tester@" ++ H2), longnames]),
      ?DEBUG("limitlog: net_kernel ans: ~p~n", [Ans1]),
      Ans2 = erlang:set_cookie(node(), veil_cluster_node),
      ?DEBUG("limitlog: set_cookie ans: ~p~n", [Ans2])
    catch
      E1:E2 -> ?DEBUG("limitlog: init error: ~p:~p~n", [E1, E2])
    end,
    {ok, Hosts}.

run(_Action, KeyGen, _ValueGen, Hosts) ->
    Host = lists:nth((KeyGen() rem length(Hosts)) + 1 , Hosts),
%%     ?DEBUG("limitlog: run func started for host ~p~n", [Host]),
    NewState = Hosts,
%%     Ans = try
    try
      case net_adm:ping(Host) of
        pong -> {ok, NewState};
        pang -> {error, pang, NewState}
      end
    catch
      E1:E2 -> {error, E1, E2, NewState}
    end.
%%     ?DEBUG("limitlog: ping ans: ~p~n", [Ans]),
%%     Ans.