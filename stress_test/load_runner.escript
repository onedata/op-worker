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

%% set up net_kernel, which must be running for distributed Erlang to work,
%% and to provide monitoring of the network
set_up_net_kernel() ->
    {A, B, C} = erlang:now(),
    NodeName = "setup_node_" ++ integer_to_list(A, 32) ++ integer_to_list(B, 32) ++ integer_to_list(C, 32) ++ "@127.0.0.1",
    net_kernel:start([list_to_atom(NodeName), longnames]),
    erlang:set_cookie(node(), ?default_cookie).

%% main script function, which processes given arguments
main([Command | Args]) ->
    HostName = "@" ++ os:cmd("hostname -f") -- "\n",
    set_up_net_kernel(),
    put(hostname, HostName),
    NodeType = lists:nth(1, Args),
    TargetNode = list_to_atom(NodeType ++ HostName),
    case Command of
        "start" -> start_load_logging(NodeType, TargetNode);
        "stop" -> stop_load_logging(NodeType, TargetNode);
        _ -> ok
    end.

%% cast message to ccm/worker node to start load logging on all nodes/this node
start_load_logging("ccm", Node) ->
    io:format("Start load logging on node: ~p~n", [Node]),
    {ok, Dir} = file:get_cwd(),
    io:format("Dir: ~p~n", [Dir]),
    ok = rpc:call(Node, gen_server, cast, [{global, central_cluster_manager}, {start_load_logging, Dir}], 1000);
start_load_logging("worker", Node) ->
    io:format("Start load logging on node: ~p~n", [Node]),
    {ok, Dir} = file:get_cwd(),
    io:format("Dir: ~p~n", [Dir]),
    ok = rpc:call(Node, gen_server, cast, [node_manager, {start_load_logging, Dir}], 1000);
start_load_logging(Other, _) ->
    io:format("Unknown node type: ~p~n", [Other]).

%% cast message to ccm/worker node to stop load logging on all nodes/this node
stop_load_logging("ccm", Node) ->
    io:format("Stop load logging on node: ~p~n", [Node]),
    ok = rpc:call(Node, gen_server, cast, [{global, central_cluster_manager}, stop_load_logging], 1000);
stop_load_logging("worker", Node) ->
    io:format("Stop load logging on node: ~p~n", [Node]),
    ok = rpc:call(Node, gen_server, cast, [node_manager, stop_load_logging], 1000);
stop_load_logging(Other, _) ->
    io:format("Unknown node type: ~p~n", [Other]).