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

-define(trace_procs, [rule_manager,rtransfer,request_dispatcher,node_manager,dns_worker,gateway,fslogic,dao_worker,control_panel,cluster_rengine,central_logger]).


set_up_net_kernel() ->
    {A, B, C} = erlang:now(),
    NodeName = "setup_node_" ++ integer_to_list(A, 32) ++ integer_to_list(B, 32) ++ integer_to_list(C, 32) ++ "@127.0.0.1",
    net_kernel:start([list_to_atom(NodeName), longnames]),
    erlang:set_cookie(node(), ?default_cookie).

main([Command, NodeType, Mode, PidList, PidBlkList, PidIList | _Args]) ->
    HostName = "@" ++ os:cmd("hostname -f") -- "\n",
    put(hostname, HostName),
    set_up_net_kernel(),
    {_, PidList1} = string_to_term(PidList),
    {_, PidBlkList1} = string_to_term(PidBlkList),
    {_, PidIList1} = string_to_term(PidIList),
    TargetNode = list_to_atom(NodeType  ++ HostName),
    DefProcs = ?trace_procs ++ PidList1 -- PidBlkList1 -- [undefined],
    case Command of 
        "start" -> 
            start_trace(TargetNode, Mode, DefProcs, PidIList1);
        "stop" ->
            stop_trace(TargetNode, NodeType, Mode, DefProcs, PidIList1)
    end.    

start_trace(Node, "mode_totals_only", PidList, PidIList) ->
    start_trace(Node, "mode_full", PidList, PidIList);
start_trace(Node, "mode_trace_only", PidList, PidIList) ->
    start_trace(Node, "mode_full", PidList, PidIList);
start_trace(Node, "mode_full", PidList, PidIList) ->

    ProcsToTrace = get_pid(PidList, Node),
    ProcsToTraceI = get_pid(PidIList, Node),
    Zipped = lists:zip(PidIList, ProcsToTraceI),
    io:format("Starting trace of procs: ~p and groups~p~n", [ProcsToTrace, Zipped]),

    ok = rpc:call(Node, fprof, trace, [[start, {procs, ProcsToTrace}]], 1000);
    %lists:foreach(
    %    fun({undefined, _}) ->
    %        ok;
    %    ({Name, Pid}) ->
    %        FName = string:join(lists:map(fun(Elem) -> atom_to_list(Elem) end, lists:flatten([Name])), "_"),
    %        ok = rpc:call(Node, fprof, trace, [[start, {procs, lists:flatten([Pid])}, {file, FName ++ ".trace"}]], 1000)
    %    end, Zipped);
start_trace(_Node, _, _, _) ->
    ok.

stop_trace(Node, NodeType, Mode, _DefProcs, PidIList) ->
    {ok, Dir} = file:get_cwd(),
    FilePrefix = Dir ++ "/" ++ NodeType ++ "__",
    FileSuffix = ".analysis",
    Procs0 = rpc:call(Node, erlang, registered, [], 1000),
    Procs1 =
        lists:map(fun
            (Elem) ->
                {Elem, rpc:call(Node, erlang, whereis, [Elem], 1000)}
            end, Procs0),
    file:write_file(NodeType ++ ".pid.list", io_lib:format("~p.\n", [Procs1])),
    ok = call(Node, fprof, trace, [[stop]]),
    stop_trace1(Node, Mode, FilePrefix, FileSuffix, "fprof").
    %lists:foreach(
    %    fun(Elem) ->
    %        FName = string:join(lists:map(fun(E) -> atom_to_list(E) end, lists:flatten([Elem])), "_"),
    %        stop_trace1(Node, Mode, FilePrefix, FileSuffix, FName)
    %    end, [fprof] ++ PidIList).

stop_trace1(_Node, "mode_trace_only", _FilePrefix, _FileSuffix, _FName) ->
    ok;

stop_trace1(Node, "mode_full", FilePrefix, FileSuffix, FName) ->
    io:format("Stopping full trace on node ~p~n", [Node]),
    ok = call(Node, fprof, profile, [{file, FName ++ ".trace"}]),
    io:format("Full analyse 'no_callers, {totals, true}, no_details' on node ~p~n", [Node]), 
    ok = call(Node, fprof, analyse, [[no_callers, {totals, true}, no_details, {dest, FilePrefix ++ FName ++ "_" ++ "totals" ++ FileSuffix}]]),
    io:format("Full analyse '{totals, true}, no_details' on node ~p~n", [Node]), 
    ok = call(Node, fprof, analyse, [[{totals, true}, no_details, {dest, FilePrefix ++ FName ++ "_" ++ "totals_with_callers" ++ FileSuffix}]]),
    io:format("Full analyse 'no_callers, {totals, true}' on node ~p~n", [Node]), 
    ok = call(Node, fprof, analyse, [[no_callers, {totals, true}, {dest, FilePrefix ++ FName ++ "_" ++ "totals_per_proc" ++ FileSuffix}]]),
    io:format("Full analyse '' on node ~p~n", [Node]),
    ok = call(Node, fprof, analyse, [[{dest, FilePrefix ++ FName ++ "_" ++ "callers_per_proc" ++ FileSuffix}]]),
    ok;
stop_trace1(Node, "mode_totals_only", FilePrefix, FileSuffix, FName) ->
    io:format("Stopping totals_only trace on node ~p~n", [Node]),
    ok = call(Node, fprof, profile, [{file, FName ++ ".trace"}]),
    ok = call(Node, fprof, analyse, [[no_callers, {totals, true}, no_details, {dest, FilePrefix ++ FName ++ "_" ++ "totals" ++ FileSuffix}]]),
    ok;
stop_trace1(_, _, _, _, _) ->
    ok.

get_pid(undefined, _) ->
    [];
get_pid(ProcName, Node) when is_atom(ProcName) ->
    get_pid(rpc:call(Node, erlang, whereis, [ProcName], 1000), Node);
get_pid(ProcPid, _) when is_pid(ProcPid) ->
    [ProcPid];
get_pid([Pid | PidList], Node) when is_list(Pid) ->
    [get_pid(Pid, Node)] ++ get_pid(PidList, Node);
get_pid([Pid | PidList], Node) ->
    get_pid(Pid, Node) ++ get_pid(PidList, Node);
get_pid([], _Node) ->
    [].

call(Node, Module, Method, Args) ->
    call(Node, Module, Method, Args, 28800000).
call(Node, Module, Method, Args, Timeout) ->
    Self = self(),
    Pid = spawn(Node, fun() -> Self ! {self(), apply(Module, Method, Args)} end),
    receive 
        {Pid, Ans} ->
            Ans
    after Timeout ->
        {error, timeout}
    end.


string_to_term(Input) ->
    Input0 =
        case lists:reverse(Input) of
            [$. | _ ]   -> Input;
            _           -> Input ++ "."
        end,
    {ok, Tokens, _EndLine} = erl_scan:string(Input0),
    {ok, AbsForm} = erl_parse:parse_exprs(Tokens),
    {value, Value, _Bs} = erl_eval:exprs(AbsForm, erl_eval:new_bindings()),
    Value.


