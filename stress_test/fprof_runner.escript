#!/usr/bin/env escript

%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% ===================================================================
%% @doc: This script is used to start and stop fprof profiling module
%%       on given erlang node. It meant to be used by Bamboo for stress test
%%       profiling.
%% ===================================================================

-define(default_cookie, veil_cluster_node).
-define(default_ccm_name, "ccm").
-define(default_worker_name, "worker").

-define(trace_procs, [rule_manager,rtransfer,request_dispatcher,node_manager,dns_worker,gateway,fslogic,dao,control_panel,cluster_rengine,central_logger]).

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
            start_trace(TargetNode, lists:nth(2, Args));
        "stop" ->
            stop_trace(TargetNode, lists:nth(1, Args), lists:nth(2, Args))
    end.    

start_trace(Node, "mode_totals_only") ->
    start_trace(Node, "mode_full");    
start_trace(Node, "mode_full") ->
    ProcsToTrace = lists:foldl(fun(Elem, AccIn) -> AccIn ++ get_pid(Elem, Node) end, [], ?trace_procs), 
    io:format("Starting trace of procs: ~p~n", [ProcsToTrace]),

    ok = rpc:call(Node, fprof, trace, [[start, {procs, ProcsToTrace}]], 1000);
start_trace(_Node, _) ->
    ok.

stop_trace(Node, NodeType, Mode) ->
    {ok, Dir} = file:get_cwd(),
    FilePrefix = Dir ++ "/" ++ NodeType ++ "__",
    FileSuffix = ".analysis",
    stop_trace(Node, Mode, FilePrefix, FileSuffix).

stop_trace(Node, "mode_full", FilePrefix, FileSuffix) ->  
    io:format("Stopping full trace on node ~p~n", [Node]), 
    ok = call(Node, fprof, trace, [[stop]]),
    ok = call(Node, fprof, profile, []),
    io:format("Full analyse 'no_callers, {totals, true}, no_details' on node ~p~n", [Node]), 
    ok = call(Node, fprof, analyse, [[no_callers, {totals, true}, no_details, {dest, FilePrefix ++ "totals" ++ FileSuffix}]]), 
    io:format("Full analyse '{totals, true}, no_details' on node ~p~n", [Node]), 
    ok = call(Node, fprof, analyse, [[{totals, true}, no_details, {dest, FilePrefix ++ "totals_with_callers" ++ FileSuffix}]]), 
    io:format("Full analyse 'no_callers, {totals, true}' on node ~p~n", [Node]), 
    ok = call(Node, fprof, analyse, [[no_callers, {totals, true}, {dest, FilePrefix ++ "totals_per_proc" ++ FileSuffix}]]),
    io:format("Full analyse '' on node ~p~n", [Node]), 
    ok = call(Node, fprof, analyse, [[{dest, FilePrefix ++ "callers_per_proc" ++ FileSuffix}]]),
    ok;
stop_trace(Node, "mode_totals_only", FilePrefix, FileSuffix) -> 
    io:format("Stopping totals_only trace on node ~p~n", [Node]),   
    ok = call(Node, fprof, trace, [[stop]]),
    ok = call(Node, fprof, profile, []),
    ok = call(Node, fprof, analyse, [[no_callers, {totals, true}, no_details, {dest, FilePrefix ++ "totals" ++ FileSuffix}]]), 
    ok;
stop_trace(_, _, _, _) ->
    ok.

get_pid(undefined, _) ->
    [];
get_pid(ProcName, Node) when is_atom(ProcName) ->
    get_pid(rpc:call(Node, erlang, whereis, [ProcName], 1000), Node);
get_pid(ProcPid, _) when is_pid(ProcPid) ->
    [ProcPid].

call(Node, Module, Method, Args) ->
    Self = self(),
    Pid = spawn(Node, fun() -> Self ! {self(), apply(Module, Method, Args)} end),
    receive 
        {Pid, Ans} ->
            Ans
    after 600000 ->
        {error, timeout}
    end.
