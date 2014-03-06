#!/usr/bin/env escript

%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% ===================================================================
%% @doc: This script is used to start and stop load logging on each node.
%%       It's meant to be used by Bamboo for stress test load logging.
%% ===================================================================

-define(default_cookie, veil_cluster_node).
-define(default_ccm_name, "ccm").
-define(default_worker_name, "worker").

set_up_net_kernel() ->
    {A, B, C} = erlang:now(),
    NodeName = "setup_node_" ++ integer_to_list(A, 32) ++ integer_to_list(B, 32) ++ integer_to_list(C, 32) ++ "@127.0.0.1",
    net_kernel:start([list_to_atom(NodeName), longnames]),
    erlang:set_cookie(node(), ?default_cookie).

main([Command | Args]) ->
    HostName = "@" ++ os:cmd("hostname -f") -- "\n",
    put(hostname, HostName),
    set_up_net_kernel(),
    TargetNode = list_to_atom(lists:nth(1, Args)  ++ HostName),
    case Command of
        "start" ->
            start_load_logging(TargetNode);
        "stop" ->
            stop_load_logging(TargetNode)
    end.

start_load_logging(Node) ->
    io:format("Starting load logging on node: ~p~n", [Node]),
    {ok, Dir} = file:get_cwd(),
    io:format("Dir: ~p", [Dir]),
    ok = rpc:call(Node, gen_server, cast, [{global, central_cluster_manager}, {start_load_logging, Dir}], 1000).

stop_load_logging(Node) ->
    io:format("Stopping load logging on node: ~p~n", [Node]),
    ok = rpc:call(Node, gen_server, cast, [{global, central_cluster_manager}, stop_load_logging], 1000).
