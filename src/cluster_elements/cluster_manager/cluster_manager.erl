%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @author Tomasz Lichon
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module coordinates central cluster.
%%% @end
%%%-------------------------------------------------------------------
-module(cluster_manager).
-author("Michal Wrzeszcz").
-author("Tomasz Lichon").

-behaviour(gen_server).

-include("registered_names.hrl").
-include("supervision_macros.hrl").
-include("modules_and_args.hrl").
-include("cluster_elements/cluster_manager/cluster_manager.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0, stop/0]).

%% gen_event callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% TEST API
-ifdef(TEST).
-export([update_dns_state/3, update_dispatcher_state/6, calculate_node_load/2]).
-endif.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts cluster manager
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> Result when
    Result :: {ok, Pid}
    | ignore
    | {error, Error},
    Pid :: pid(),
    Error :: {already_started, Pid} | term().
start_link() ->
    case gen_server:start_link(?MODULE, [], []) of
        {ok, Pid} ->
            global:re_register_name(?CCM, Pid),
            {ok, Pid};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Stops the server
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok.
stop() ->
    gen_server:cast(?CCM, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State}
    | {ok, State, Timeout}
    | {ok, State, hibernate}
    | {stop, Reason :: term()}
    | ignore,
    State :: term(),
    Timeout :: non_neg_integer() | infinity.
%% ====================================================================
init(_) ->
    process_flag(trap_exit, true),
    {ok, Interval} = application:get_env(?APP_NAME, initialization_time),
    erlang:send_after(Interval * 1000, self(), {timer, init_cluster}),
    {ok, #cm_state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result when
    Result :: {reply, Reply, NewState}
    | {reply, Reply, NewState, Timeout}
    | {reply, Reply, NewState, hibernate}
    | {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason, Reply, NewState}
    | {stop, Reason, NewState},
    Reply :: term(),
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity,
    Reason :: term().
handle_call(get_state_num, _From, State) ->
    {reply, State#cm_state.state_num, State};

handle_call(get_nodes, _From, State) ->
    {reply, State#cm_state.nodes, State};

handle_call(get_workers, _From, State) ->
    WorkersList = get_workers_list(State),
    {reply, {WorkersList, State#cm_state.state_num}, State};

handle_call(_Request, _From, State) ->
    ?warning("Wrong call: ~p", [_Request]),
    {reply, wrong_request, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
handle_cast({heart_beat, Node}, State) ->
    NewState = heart_beat(State, Node),
    {noreply, NewState};

handle_cast(init_cluster, State) ->
    NewState = init_cluster(State),
    {noreply, NewState};

handle_cast({register_dispatcher_map, Module, Map, AnsPid}, State = #cm_state{dispatcher_maps = Maps}) ->
    case proplists:get_value(Module, Maps) =:= Map of
        true ->
            % debug, because it is ok when more than one instance of worker exists
            ?debug("Registration of existing disp map for module ~p", [Module]),
            AnsPid ! dispatcher_map_registered,
            {noreply, State};
        false ->
            ?info("Registration of disp map for module ~p", [Module]),
            NewMapsList = register_dispatcher_map(Module, Map, Maps),
            NewState = update_dispatchers_and_dns(State#cm_state{dispatcher_maps = NewMapsList}, false, true),
            AnsPid ! dispatcher_map_registered,
            {noreply, NewState}
    end;

handle_cast({stop_worker, Node, Module}, State) ->
    NewState = stop_worker(Node, Module, State),
    {noreply, NewState};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Msg, State) ->
    ?warning("Wrong cast: ~p", [_Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
handle_info({timer, Msg}, State) ->
    gen_server:cast({global, ?CCM}, Msg),
    {noreply, State};

handle_info({nodedown, Node}, State) ->
    NewState = node_down(Node, State),
    {noreply, NewState};

handle_info(_Info, State) ->
    ?warning("CCM wrong info: ~p", [_Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason, State :: term()) -> Any :: term() when
    Reason :: normal
    | shutdown
    | {shutdown, term()}
    | term().
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
    Result :: {ok, NewState :: term()} | {error, Reason :: term()},
    OldVsn :: Vsn | {down, Vsn},
    Vsn :: term().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Receive heart_beat from node_manager
%% @end
%%--------------------------------------------------------------------
-spec heart_beat(State :: #cm_state{}, SenderNode :: node()) -> #cm_state{}.
heart_beat(State = #cm_state{nodes = Nodes}, SenderNode) ->
    ?debug("Heartbeat from node: ~p", [SenderNode]),
    case lists:member(SenderNode, Nodes) orelse SenderNode =:= node() of
        true ->
            gen_server:cast({?NODE_MANAGER_NAME, SenderNode}, {heart_beat_ok, State#cm_state.state_num}),
            State;
        false ->
            ?info("New node: ~p", [SenderNode]),

            %% This case checks if node state was analysed correctly.
            %% If it was, it upgrades state number if necessary (workers
            %% were running on node).
            case catch check_node(SenderNode, State) of
                {ok, {NewState, WorkersFound}} ->
                    erlang:monitor_node(SenderNode, true),
                    case WorkersFound of
                        true -> update_dispatchers_and_dns(NewState, true, true);
                        false -> ok
                    end,
                    gen_server:cast({?NODE_MANAGER_NAME, SenderNode}, {heart_beat_ok, State#cm_state.state_num}),
                    NewState#cm_state{nodes = [SenderNode | Nodes]};
                Error ->
                    ?warning_stacktrace("Checking node ~p, in ccm failed with error: ~p", [SenderNode, Error]),
                    gen_server:cast({?NODE_MANAGER_NAME, SenderNode}, {heart_beat_ok, State#cm_state.state_num}),
                    State
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes cluster - decides at which nodes components should
%% be started (and starts them). Additionally, it sets timer that
%% initiates checking of cluster state.
%% @end
%%--------------------------------------------------------------------
-spec init_cluster(State :: #cm_state{}) -> #cm_state{}.
init_cluster(State = #cm_state{nodes = []}) ->
    {ok, Interval} = application:get_env(?APP_NAME, initialization_time),
    erlang:send_after(1000 * Interval, self(), {timer, init_cluster}),
    State;
init_cluster(State = #cm_state{nodes = Nodes, workers = Workers}) ->
    CreateRunningWorkersList =
        fun({_N, M, _Child}, WorkerList) ->
            [M | WorkerList]
        end,
    RunningWorkers = lists:foldl(CreateRunningWorkersList, [], Workers),

    CreateJobsList =
        fun({Job, A}, {TmpJobs, TmpArgs}) ->
            case lists:member(Job, RunningWorkers) of
                true -> {TmpJobs, TmpArgs};
                false -> {[Job | TmpJobs], [A | TmpArgs]}
            end
        end,
    {Jobs, Args} = lists:foldl(CreateJobsList, {[], []}, ?MODULES_WITH_ARGS),

    case {Jobs, Workers} of
        {[], []} ->
            State;
        {[], _} ->
            update_dispatchers_and_dns(State, true, true);
        {_, _} ->
            ?info("Initialization of jobs ~p using nodes ~p", [Jobs, Nodes]),
            NewState =
                case erlang:length(Nodes) >= erlang:length(Jobs) of
                    true -> init_cluster_nodes_dominance(State, Nodes, Jobs, [], Args, []);
                    false -> init_cluster_jobs_dominance(State, Jobs, Args, Nodes, [])
                end,
            update_dispatchers_and_dns(NewState, true, true)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Chooses node for workers when there are more nodes than workers.
%% @end
%%--------------------------------------------------------------------
-spec init_cluster_nodes_dominance(State :: term(), Nodes :: list(), Jobs1 :: list(),
    Jobs2 :: list(), Args1 :: list(), Args2 :: list()) -> NewState when
    NewState :: term().
init_cluster_nodes_dominance(State, [], _Jobs1, _Jobs2, _Args1, _Args2) ->
    State;
init_cluster_nodes_dominance(State, Nodes, [], Jobs2, [], Args2) ->
    init_cluster_nodes_dominance(State, Nodes, Jobs2, [], Args2, []);
init_cluster_nodes_dominance(State, [N | Nodes], [J | Jobs1], Jobs2, [A | Args1], Args2) ->
    NewState = start_worker(N, J, A, State),
    init_cluster_nodes_dominance(NewState, Nodes, Jobs1, [J | Jobs2], Args1, [A | Args2]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Chooses node for workers when there are more workers than nodes.
%% @end
%%--------------------------------------------------------------------
-spec init_cluster_jobs_dominance(State :: term(), Jobs :: list(),
    Args :: list(), Nodes1 :: list(), Nodes2 :: list()) -> NewState when
    NewState :: term().
init_cluster_jobs_dominance(State, [], [], _Nodes1, _Nodes2) ->
    State;
init_cluster_jobs_dominance(State, Jobs, Args, [], Nodes2) ->
    init_cluster_jobs_dominance(State, Jobs, Args, Nodes2, []);
init_cluster_jobs_dominance(State, [J | Jobs], [A | Args], [N | Nodes1], Nodes2) ->
    NewState = start_worker(N, J, A, State),
    init_cluster_jobs_dominance(NewState, Jobs, Args, Nodes1, [N | Nodes2]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes client request using PlugIn:handle function. Afterwards,
%% it sends the answer to dispatcher and logs info about processing time.
%% @end
%%--------------------------------------------------------------------
-spec start_worker(Node :: atom(), Module :: atom(), WorkerArgs :: term(), State :: term()) -> #cm_state{}.
start_worker(Node, Module, WorkerArgs, State) ->
    try
        {ok, LoadMemorySize} = application:get_env(?APP_NAME, worker_load_memory_size),
        {ok, ChildPid} = supervisor:start_child({?SUPERVISOR_NAME, Node}, ?SUP_CHILD(Module, worker_host, transient, [Module, WorkerArgs, LoadMemorySize])),
        Workers = State#cm_state.workers,
        ?info("Worker: ~s started at node: ~s", [Module, Node]),
        State#cm_state{workers = [{Node, Module, ChildPid} | Workers]}
    catch
        _:Error ->
            ?error_stacktrace("Error: ~p during start of worker: ~s at node: ~s", [Error, Module, Node]),
            State
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes client request using PlugIn:handle function. Afterwards,
%% it sends the answer to dispatcher and logs info about processing time.
%% @end
%%--------------------------------------------------------------------
-spec stop_worker(Node :: atom(), Module :: atom(), State :: #cm_state{}) -> #cm_state{}.
stop_worker(Node, Module, State = #cm_state{workers = Workers}) ->
    CreateNewWorkersList =
        fun({N, M, Child}, {WorkerList, ChosenChild}) ->
            case {N, M} of
                {Node, Module} -> {WorkerList, {N, Child}};
                {_N2, _M2} -> {[{N, M, Child} | WorkerList], ChosenChild}
            end
        end,
    {NewWorkers, ChosenChild} = lists:foldl(CreateNewWorkersList, {[], non}, Workers),
    try
        {ChildNode, _ChildPid} = ChosenChild,
        ok = supervisor:terminate_child({?SUPERVISOR_NAME, ChildNode}, Module),
        ok = supervisor:delete_child({?SUPERVISOR_NAME, ChildNode}, Module),
        ?info("Worker: ~s stopped at node: ~s", [Module, Node]),
        State#cm_state{workers = NewWorkers}
    catch
        _:Error ->
            ?error("Worker: ~s not stopped at node: ~s, error ~p", [Module, Node, {delete_error, Error}]),
            State#cm_state{workers = NewWorkers}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if any workers are running on node.
%% @end
%%--------------------------------------------------------------------
-spec check_node(Node :: atom(), State :: term()) -> {ok, {NewState :: #cm_state{}, WorkersFound :: boolean()}} | no_return().
check_node(Node, State = #cm_state{workers = Workers}) ->
    pong = net_adm:ping(Node),
    Children = supervisor:which_children({?SUPERVISOR_NAME, Node}),
    {ok, NewWorkers} = add_children(Node, Children, Workers, State),
    WorkersFound = length(NewWorkers) > length(Workers),
    {ok, {State#cm_state{workers = NewWorkers}, WorkersFound}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Add workers that run on node to workers list.
%% @end
%%--------------------------------------------------------------------
-spec add_children(Node :: atom(), Children :: list(), Workers :: term(), State :: term()) -> NewWorkersList when
    NewWorkersList :: list().
add_children(_Node, [], Workers, _State) ->
    {ok, Workers};
add_children(Node, [{Id, ChildPid, _Type, _Modules} | Children], Workers, State) ->
    Jobs = ?MODULES,
    case lists:member(Id, Jobs) of
        false -> add_children(Node, Children, Workers, State);
        true ->
            ?info("Worker ~p found at node ~s", [Id, Node]),

            case catch gen_server:call(ChildPid, dispatcher_map_unregistered) of
                ok ->
                    {MapState2, Ans} = add_children(Node, Children, Workers, State),
                    {MapState2, [{Node, Id, ChildPid} | Ans]};
                Error ->
                    ?error_stacktrace("Error: ~p during contact with worker ~p found at node ~s", [Error, Id, Node]),
                    {error, Workers}
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Clears information about workers on node that is down.
%% @end
%%--------------------------------------------------------------------
-spec node_down(Node :: atom(), State :: #cm_state{}) -> #cm_state{}.
node_down(Node, State = #cm_state{workers = Workers, nodes = Nodes}) ->
    ?error("Node down: ~p", [Node]),
    CreateNewWorkersList = fun({N, M, Child}, {WorkerList, Found}) ->
        case N of
            Node -> {Workers, true};
            _N2 -> {[{N, M, Child} | WorkerList], Found}
        end
    end,
    {NewWorkers, WorkersFound} = lists:foldl(CreateNewWorkersList, {[], false}, Workers),

    CreateNewNodesList = fun(N, NodeList) ->
        case N of
            Node -> NodeList;
            _N2 -> [N | NodeList]
        end
    end,
    NewNodes = lists:foldl(CreateNewNodesList, [], Nodes),

    NewState = State#cm_state{workers = NewWorkers, nodes = NewNodes},
    case WorkersFound of
        true -> update_dispatchers_and_dns(NewState, true, true);
        false -> NewState
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function provides the list of all alive workers with information
%% at which nodes they are working.
%% @end
%%--------------------------------------------------------------------
-spec get_workers_list(State :: term()) -> Workers when
    Workers :: list().
get_workers_list(State) ->
    ListWorkers = fun({Node, Module, _ChildPid}, Workers) ->
        [{Node, Module} | Workers]
    end,
    lists:foldl(ListWorkers, [], State#cm_state.workers).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function updates all dispatchers and dnses.
%% @end
%%--------------------------------------------------------------------
-spec update_dispatchers_and_dns(State :: term(), UpdateDNS :: boolean(), IncreaseStateNum :: boolean()) -> NewState when
    NewState :: term().
update_dispatchers_and_dns(State, UpdateDNS, IncreaseStateNum) ->
    ?debug("update_dispatchers_and_dns, state: ~p, dns: ~p, increase state num: ~p", [State, UpdateDNS, IncreaseStateNum]),
    case UpdateDNS of
        true ->
            {NodesLoad, AvgLoad} = calculate_node_load(State#cm_state.nodes, medium),
            update_dns_state(State#cm_state.workers, NodesLoad, AvgLoad);
        false -> ok
    end,

    case IncreaseStateNum of
        true ->
            NewStateNum = State#cm_state.state_num + 1,
            WorkersList = get_workers_list(State),
            {NodesLoad2, AvgLoad2} = calculate_node_load(State#cm_state.nodes, short),
            update_dispatcher_state(WorkersList, State#cm_state.dispatcher_maps, State#cm_state.nodes, NewStateNum, NodesLoad2, AvgLoad2),
            State#cm_state{state_num = NewStateNum};
        false ->
            {NodesLoad2, AvgLoad2} = calculate_node_load(State#cm_state.nodes, short),
            ?debug("updating dispatcher, load info: ~p", [{NodesLoad2, AvgLoad2}]),
            update_dispatcher_state(State#cm_state.nodes, NodesLoad2, AvgLoad2),
            State
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates dispatchers' states.
%% @end
%%--------------------------------------------------------------------
-spec update_dispatcher_state(WorkersList, DispatcherMaps, Nodes, NewStateNum, Loads, AvgLoad) -> ok when
    WorkersList :: list(),
    DispatcherMaps :: list(),
    Nodes :: list(),
    NewStateNum :: integer(),
    Loads :: list(),
    AvgLoad :: integer().
update_dispatcher_state(WorkersList, DispatcherMaps, Nodes, NewStateNum, Loads, AvgLoad) ->
    UpdateNode = fun(Node) ->
        gen_server:cast({?DISPATCHER_NAME, Node}, {update_workers, WorkersList, DispatcherMaps, NewStateNum, proplists:get_value(Node, Loads, 0), AvgLoad})
    end,
    lists:foreach(UpdateNode, Nodes),
    gen_server:cast(?DISPATCHER_NAME, {update_workers, WorkersList, DispatcherMaps, NewStateNum, 0, 0}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates dispatchers' states.
%% @end
%%--------------------------------------------------------------------
-spec update_dispatcher_state(Nodes, Loads, AvgLoad) -> ok when
    Nodes :: list(),
    Loads :: list(),
    AvgLoad :: integer().
update_dispatcher_state(Nodes, Loads, AvgLoad) ->
    UpdateNode = fun(Node) ->
        gen_server:cast({?DISPATCHER_NAME, Node}, {update_loads, proplists:get_value(Node, Loads, 0), AvgLoad})
    end,
    lists:foreach(UpdateNode, Nodes).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates dnses' states.
%% @end
%%--------------------------------------------------------------------
-spec update_dns_state(WorkersList, NodeToLoad, AvgLoad) -> ok when
    WorkersList :: list(),
    NodeToLoad :: list(),
    AvgLoad :: number().
update_dns_state(WorkersList, NodeToLoad, AvgLoad) ->
    MergeByFirstElement = fun(List) ->
        lists:reverse(
            lists:foldl(fun({Key, Value}, []) -> [{Key, [Value]}];
                ({Key, Value}, [{Key, AccValues} | Tail]) -> [{Key, [Value | AccValues]} | Tail];
                ({Key, Value}, Acc) -> [{Key, [Value]} | Acc]
            end, [], lists:keysort(1, List)))
    end,

    NodeToIPWithLogging = fun(Node) ->
        case node_to_ip(Node) of
            {ok, Address} -> Address;
            {error, Error} ->
                ?error("Cannot resolve ip address for node ~p, error: ~p", [Node, Error]),
                unknownaddress
        end
    end,

    ModuleToNode = [{Module, Node} || {Node, Module, _Pid} <- WorkersList],

    MergedByModule = MergeByFirstElement(ModuleToNode),

    ModulesToNodes = lists:map(fun({Module, Nodes}) ->
        GetLoads = fun({Node, NodeLoad, ModulesLoads}, TmpAns) ->
            case lists:member(Node, Nodes) of
                true ->
                    ModuleTmpV = proplists:get_value(Module, ModulesLoads, error),
                    ModuleV = case ModuleTmpV of
                                  error -> 0;
                                  _ -> ModuleTmpV
                              end,
                    NodeV = case NodeLoad of
                                error -> 100000;
                                _ -> NodeLoad
                            end,

                    case ModuleV > 0.5 of
                        true ->
                            case (AvgLoad > 0) and (NodeV >= 2 * AvgLoad) of
                                true ->
                                    V = erlang:min(10, erlang:round(NodeV / AvgLoad)),
                                    [{Node, V} | TmpAns];
                                false -> [{Node, 1} | TmpAns]
                            end;
                        false -> [{Node, 1} | TmpAns]
                    end;
                false -> TmpAns
            end
        end,

        FilteredNodeToLoad = lists:foldl(GetLoads, [], NodeToLoad),

        case FilteredNodeToLoad of
            [] -> {Module, []};
            _ ->
                MinV = lists:min([V || {_Node, V} <- FilteredNodeToLoad]),
                FilteredNodeToLoad2 = case MinV of
                                          1 -> FilteredNodeToLoad;
                                          _ -> [{Node, erlang:round(V / MinV)} || {Node, V} <- FilteredNodeToLoad]
                                      end,

                IPs = [{NodeToIPWithLogging(Node), Param} || {Node, Param} <- FilteredNodeToLoad2],

                FilteredIPs = [{IP, Param} || {IP, Param} <- IPs, IP =/= unknownaddress],
                {Module, FilteredIPs}
        end
    end, MergedByModule),

    FilteredModulesToNodes = [{Module, Nodes} || {Module, Nodes} <- ModulesToNodes, Nodes =/= []],

    NLoads = lists:map(
        fun({Node, NodeLoad, _}) ->
            {NodeToIPWithLogging(Node), NodeLoad}
        end, NodeToLoad),
    UpdateInfo = {update_state, FilteredModulesToNodes, [{N_IP, N_Load} || {N_IP, N_Load} <- NLoads, N_IP =/= unknownaddress], AvgLoad},
    ?debug("updating dns, update message: ~p", [UpdateInfo]),
    UpdateDnsWorker = fun({_Node, Module, Pid}) ->
        case Module of
            dns_worker -> gen_server:cast(Pid, {asynch, 1, UpdateInfo});
            _ -> ok
        end
    end,

    lists:foreach(UpdateDnsWorker, WorkersList),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolve ipv4 address of node.
%% @end
%%--------------------------------------------------------------------
-spec node_to_ip(Node) -> Result when
    Result :: {ok, inet:ip4_address()}
    | {error, inet:posix()},
    Node :: atom().
node_to_ip(Node) ->
    StrNode = atom_to_list(Node),
    AddressWith@ = lists:dropwhile(fun(Char) -> Char =/= $@ end, StrNode),
    Address = lists:dropwhile(fun(Char) -> Char =:= $@ end, AddressWith@),
    inet:getaddr(Address, inet).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calculates load of all nodes in cluster
%% @end
%%--------------------------------------------------------------------
-spec calculate_node_load(Nodes :: list(), Period :: atom()) -> Result when
    Result :: list().
calculate_node_load(_Nodes, _Period) ->
    {[], 0}. %todo remove

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Registers information about map used by dispatcher
%% @end
%%--------------------------------------------------------------------
-spec register_dispatcher_map(Module :: atom(), Map :: term(), MapsList :: list()) -> Result when
    Result :: list().
register_dispatcher_map(Module, Map, MapsList) ->
    ?debug("dispatcher map saved: ~p", [{Module, Map, MapsList}]),
    [{Module, Map} | proplists:delete(Module, MapsList)].
