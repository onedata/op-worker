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
    NodeType = lists:nth(1, Args),
    TargetNode = list_to_atom(NodeType ++ HostName),
    case Command of
        "start" ->
            info(TargetNode, "Starting load logging ...~n", []),
            {ok, Dir} = file:get_cwd(),
            info(TargetNode, "Logs output directory: ~p.~n", [Dir]),
            start_load_logging(NodeType, TargetNode, Dir);
        "stop" ->
            stop_load_logging(NodeType, TargetNode);
        Other ->
            info("Unknown command: ~p.~n", [Other])
    end.

%% cast message to ccm/worker node to start load logging on all nodes/this node
start_load_logging("ccm", Node, Dir) ->
    case rpc:call(Node, gen_server, cast, [{global, central_cluster_manager}, {start_load_logging, Dir}], 1000) of
        ok -> info(Node, "Load logging has been started.~n", []);
        Other -> info(Node, "Unable to start load logging. Error: ~p.~n", [Other])
    end;
start_load_logging("worker", Node, Dir) ->
    case rpc:call(Node, gen_server, cast, [node_manager, {start_load_logging, Dir}], 1000) of
        ok -> info(Node, "Load logging has been started.~n", []);
        Other -> info(Node, "Unable to start load logging. Error: ~p.~n", [Other])
    end;
start_load_logging(Other, _, _) ->
    info("Unknown node type: ~p.~n", [Other]).

%% cast message to ccm/worker node to stop load logging on all nodes/this node
stop_load_logging("ccm", Node) ->
    case rpc:call(Node, gen_server, cast, [{global, central_cluster_manager}, stop_load_logging], 1000) of
        ok -> info(Node, "Load logging has been stopped.~n", []);
        _ -> ok
    end;
stop_load_logging("worker", Node) ->
    case rpc:call(Node, gen_server, cast, [node_manager, stop_load_logging], 1000) of
        ok -> info(Node, "Load logging has been stopped.~n", []);
        _ -> ok
    end;
stop_load_logging(Other, _) ->
    info("Unknown node type: ~p.~n", [Other]).

info(Format, Data) ->
    io:format("---> ", []),
    io:format(Format, Data).

info(Node, Format, Data) ->
    io:format("---> [Node: ~p] ", [Node]),
    io:format(Format, Data).