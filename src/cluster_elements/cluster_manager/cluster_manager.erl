%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module coordinates central cluster.
%% @end
%% ===================================================================

-module(cluster_manager).
-behaviour(gen_server).
-include("registered_names.hrl").
-include("supervision_macros.hrl").
-include("modules_and_args.hrl").
-include("cluster_elements/cluster_manager/cluster_manager.hrl").
-include_lib("ctool/include/logging.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/0, start_link/1, stop/0]).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Test API
%% ====================================================================
-ifdef(TEST).
-export([update_dns_state/3, update_dispatcher_state/6, calculate_node_load/2]).
-endif.

%% ====================================================================
%% API functions
%% ====================================================================

%% start_link/0
%% ====================================================================
%% @doc Starts cluster manager
-spec start_link() -> Result when
    Result :: {ok, Pid}
    | ignore
    | {error, Error},
    Pid :: pid(),
    Error :: {already_started, Pid} | term().
%% ====================================================================
start_link() ->
    start_link(normal).

%% start_link/1
%% ====================================================================
%% @doc Starts cluster manager
-spec start_link(Mode) -> Result when
    Mode :: test | normal,
    Result :: {ok, Pid}
    | ignore
    | {error, Error},
    Pid :: pid(),
    Error :: {already_started, Pid} | term().
%% ====================================================================
start_link(Mode) ->
    case gen_server:start_link(?MODULE, [Mode], []) of
        {ok, Pid} ->
            global:re_register_name(?CCM, Pid),
            {ok, Pid};
        Error ->
            Error
    end.

%% stop/0
%% ====================================================================
%% @doc Stops the server
-spec stop() -> ok.
%% ====================================================================
stop() ->
    gen_server:cast(?CCM, stop).

%% init/1
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:init-1">gen_server:init/1</a>
-spec init(Args :: term()) -> Result when
    Result :: {ok, State}
    | {ok, State, Timeout}
    | {ok, State, hibernate}
    | {stop, Reason :: term()}
    | ignore,
    State :: term(),
    Timeout :: non_neg_integer() | infinity.
%% ====================================================================
init([normal]) ->
    process_flag(trap_exit, true),
    {ok, Interval} = application:get_env(?APP_Name, initialization_time),
    Pid = self(),
    erlang:send_after(Interval * 1000, Pid, {timer, init_cluster}),
    {ok, #cm_state{}};

init([test]) ->
    process_flag(trap_exit, true),
    {ok, #cm_state{nodes = [node()]}}.

%% handle_call/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_call-3">gen_server:handle_call/3</a>
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
%% ====================================================================
handle_call(get_state_num, _From, State) ->
    {reply, State#cm_state.state_num, State};

handle_call(get_nodes, _From, State) ->
    {reply, State#cm_state.nodes, State};

handle_call(get_workers, _From, State) ->
    WorkersList = get_workers_list(State),
    {reply, {WorkersList, State#cm_state.state_num}, State};

handle_call(get_state, _From, State) ->
    {reply, State, State};

handle_call(get_version, _From, State) ->
    {reply, get_version(State), State};

handle_call(get_ccm_node, _From, State) ->
    {reply, node(), State};

handle_call(check, _From, State) ->
    {reply, ok, State};

%% Test call
handle_call({start_worker, Node, Module, WorkerArgs}, _From, State) ->
    handle_test_call({start_worker, Node, Module, WorkerArgs}, _From, State);

%% Test call
handle_call(check_state_loaded, _From, State) ->
    handle_test_call(check_state_loaded, _From, State);

handle_call(_Request, _From, State) ->
    ?warning("Wrong call: ~p", [_Request]),
    {reply, wrong_request, State}.

%% handle_test_call/3
%% ====================================================================
%% @doc Handles calls used during tests
-spec handle_test_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result :: term().
%% ====================================================================
-ifdef(TEST).
handle_test_call({start_worker, Node, Module, WorkerArgs}, _From, State) ->
    {Ans, NewState} = start_worker(Node, Module, WorkerArgs, State),
    NewState2 = case Ans of
                    ok -> update_dispatchers_and_dns(NewState, true, true);
                    error -> NewState
                end,
    {reply, Ans, NewState2};
handle_test_call(check_state_loaded, _From, State) ->
    {reply, State#cm_state.state_loaded, State}.
-else.
handle_test_call(_Request, _From, State) ->
    {reply, not_supported_in_normal_mode, State}.
-endif.

%% handle_cast/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_cast-2">gen_server:handle_cast/2</a>
-spec handle_cast(Request :: term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_cast({node_is_up, Node}, State = #cm_state{nodes = Nodes, state_loaded = StateLoaded, state_monitoring = StateMonitoring}) ->
    ?debug("Heartbeat from node: ~p", [Node]),
    case lists:member(Node, Nodes) orelse Node =:= node() of
        true ->
            gen_server:cast({?Node_Manager_Name, Node}, {heart_beat_ok, State#cm_state.state_num}),
            {noreply, State};
        false ->
            ?info("New node: ~p", [Node]),

            %% This case checks if node state was analysed correctly.
            %% If it was, it upgrades state number if necessary (workers
            %% were running on node).
            case catch check_node(Node, State) of
                {ok, NewState, WorkersFound} ->
                    case StateMonitoring of
                        on -> erlang:monitor_node(Node, true);
                        off -> ok
                    end,
                    case StateLoaded of
                        true -> gen_server:cast({global, ?CCM}, init_cluster_once); %todo check for race with update_dispatchers_and_dns
                        _ -> ok
                    end,
                    case WorkersFound of
                        true -> gen_server:cast({global, ?CCM}, update_dispatchers_and_dns);
                        false -> ok
                    end,
                    gen_server:cast({?Node_Manager_Name, Node}, {heart_beat_ok, State#cm_state.state_num}),
                    {noreply, NewState#cm_state{nodes = [Node | Nodes]}};
                Error ->
                    ?warning_stacktrace("Checking node ~p, in ccm failed with error: ~p", [Node, Error]),
                    gen_server:cast({?Node_Manager_Name, Node}, {heart_beat_ok, State#cm_state.state_num}),
                    {noreply, State}
            end
    end;

handle_cast(init_cluster, State) ->
    NewState = init_cluster(State),
    {noreply, NewState};

handle_cast(init_cluster_once, State) ->
    NewState = init_cluster(State, false),
    {noreply, NewState};

handle_cast(update_dispatchers_and_dns, State) ->
    NewState = update_dispatchers_and_dns(State, true, true),
    {noreply, NewState};

handle_cast({update_dispatchers_and_dns, UpdateDNS, IncreaseNum}, State) ->
    NewState = update_dispatchers_and_dns(State, UpdateDNS, IncreaseNum),
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



handle_cast({node_down, Node}, State) ->
    ?error("Node down: ~p", [Node]),
    {NewState, WorkersFound} = node_down(Node, State),

    %% If workers were running on node that is down,
    %% upgrade state.
    NewState2 = case WorkersFound of
                    true -> update_dispatchers_and_dns(NewState, true, true);
                    false -> NewState
                end,
    {noreply, NewState2};

handle_cast({set_monitoring, Flag}, State = #cm_state{state_monitoring = Flag}) ->
    {noreply, State};
handle_cast({set_monitoring, on}, State = #cm_state{state_monitoring = off, nodes = Nodes}) ->
    change_monitoring(Nodes, true),
    {noreply, State#cm_state{state_monitoring = on}};
handle_cast({set_monitoring, off}, State = #cm_state{state_monitoring = on, nodes = Nodes}) ->
    change_monitoring(Nodes, false),
    {noreply, State#cm_state{state_monitoring = off}};

handle_cast({worker_answer, cluster_state, {ok, SavedState}}, State) ->
    ?debug("State read from DB: ~p", [SavedState]),
    {noreply, merge_state(State, SavedState)};
handle_cast({worker_answer, cluster_state, {error, {not_found, _}}}, State) ->
    {noreply, State#cm_state{state_loaded = true}};
handle_cast({worker_answer, cluster_state, Error}, State) ->
    ?debug("State cannot be read from DB: ~p", [Error]), %% debug logging level because state may not be present in db and it's not an error
    {noreply, State};

handle_cast({stop_worker, Node, Module}, State) ->
    {_Ans, New_State} = stop_worker(Node, Module, State),
    {noreply, New_State};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Msg, State) ->
    ?warning("Wrong cast: ~p", [_Msg]),
    {noreply, State}.


%% handle_info/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:handle_info-2">gen_server:handle_info/2</a>
-spec handle_info(Info :: timeout | term(), State :: term()) -> Result when
    Result :: {noreply, NewState}
    | {noreply, NewState, Timeout}
    | {noreply, NewState, hibernate}
    | {stop, Reason :: term(), NewState},
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity.
%% ====================================================================
handle_info({timer, Msg}, State) ->
    gen_server:cast({global, ?CCM}, Msg),
    {noreply, State};

handle_info({nodedown, Node}, State = #cm_state{state_monitoring = on}) ->
    gen_server:cast({global, ?CCM}, {node_down, Node}),
    {noreply, State};
handle_info({nodedown, _Node}, State = #cm_state{state_monitoring = off}) ->
    {noreply, State};

handle_info(_Info, State) ->
    ?warning("CCM wrong info: ~p", [_Info]),
    {noreply, State}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
    Reason :: normal
    | shutdown
    | {shutdown, term()}
    | term().
%% ====================================================================
terminate(_Reason, _State) ->
    ok.


%% code_change/3
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:code_change-3">gen_server:code_change/3</a>
-spec code_change(OldVsn, State :: term(), Extra :: term()) -> Result when
    Result :: {ok, NewState :: term()} | {error, Reason :: term()},
    OldVsn :: Vsn | {down, Vsn},
    Vsn :: term().
%% ====================================================================
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

%% init_cluster/1
%% ====================================================================
%% @doc Triggers repeated cluster initialization.
-spec init_cluster(State :: term()) -> NewState when
    NewState :: term().
%% ====================================================================
init_cluster(State) ->
    init_cluster(State, true).


%% init_cluster/2
%% ====================================================================
%% @doc Initializes cluster - decides at which nodes components should
%% be started (and starts them). Additionally, it sets timer that
%% initiates checking of cluster state.
-spec init_cluster(State :: term(), Repeat:: boolean()) -> NewState when
    NewState :: term().
%% ====================================================================
init_cluster(State = #cm_state{nodes = []}, false) ->
    State;
init_cluster(State = #cm_state{nodes = []}, true) ->
    {ok, Interval} = application:get_env(?APP_Name, initialization_time),
    erlang:send_after(1000 * Interval, self(), {timer, init_cluster}),
    State;
init_cluster(State = #cm_state{nodes = Nodes, workers = Workers}, _Repeat) ->
    JobsAndArgs = ?MODULES_WITH_ARGS,

    CreateRunningWorkersList = fun({_N, M, _Child}, WorkerList) ->
        [M | WorkerList]
    end,
    RunningWorkers = lists:foldl(CreateRunningWorkersList, [], Workers),

    CreateJobsList = fun({Job, A}, {TmpJobs, TmpArgs}) ->
        case lists:member(Job, RunningWorkers) of
            true -> {TmpJobs, TmpArgs};
            false -> {[Job | TmpJobs], [A | TmpArgs]}
        end
    end,
    {Jobs, Args} = lists:foldl(CreateJobsList, {[], []}, JobsAndArgs),

    case length(Jobs) > 0 of
        true ->
            ?info("Initialization of jobs ~p using nodes ~p", [Jobs, Nodes]),
            NewState = case erlang:length(Nodes) >= erlang:length(Jobs) of
                           true -> init_cluster_nodes_dominance(State , Nodes, Jobs, [], Args, []);
                           false -> init_cluster_jobs_dominance(State , Jobs, Args, Nodes, [])
                       end,

            update_dispatchers_and_dns(NewState, true, true);
        false ->
            case length(Workers) > 0 of
                true -> update_dispatchers_and_dns(State, true, true);
                _ -> State
            end
    end.
%%     case Repeat of
%%         true -> plan_next_cluster_state_check();
%%         _ -> ?debug("Single cluster initialization ~p", [NewState])
%%     end,


%% init_cluster_nodes_dominance/6
%% ====================================================================
%% @doc Chooses node for workers when there are more nodes than workers.
-spec init_cluster_nodes_dominance(State :: term(), Nodes :: list(), Jobs1 :: list(),
    Jobs2 :: list(), Args1 :: list(), Args2 :: list()) -> NewState when
    NewState :: term().
%% ====================================================================
init_cluster_nodes_dominance(State, [], _Jobs1, _Jobs2, _Args1, _Args2) ->
    State;
init_cluster_nodes_dominance(State, Nodes, [], Jobs2, [], Args2) ->
    init_cluster_nodes_dominance(State, Nodes, Jobs2, [], Args2, []);
init_cluster_nodes_dominance(State, [N | Nodes], [J | Jobs1], Jobs2, [A | Args1], Args2) ->
    {_Ans, NewState} = start_worker(N, J, A, State),
    init_cluster_nodes_dominance(NewState, Nodes, Jobs1, [J | Jobs2], Args1, [A | Args2]).

%% init_cluster_jobs_dominance/5
%% ====================================================================
%% @doc Chooses node for workers when there are more workers than nodes.
-spec init_cluster_jobs_dominance(State :: term(), Jobs :: list(),
    Args :: list(), Nodes1 :: list(), Nodes2 :: list()) -> NewState when
    NewState :: term().
%% ====================================================================
init_cluster_jobs_dominance(State, [], [], _Nodes1, _Nodes2) ->
    State;
init_cluster_jobs_dominance(State, Jobs, Args, [], Nodes2) ->
    init_cluster_jobs_dominance(State, Jobs, Args, Nodes2, []);
init_cluster_jobs_dominance(State, [J | Jobs], [A | Args], [N | Nodes1], Nodes2) ->
    {_Ans, NewState} = start_worker(N, J, A, State),
    init_cluster_jobs_dominance(NewState, Jobs, Args, Nodes1, [N | Nodes2]).

%% start_worker/4
%% ====================================================================
%% @doc Processes client request using PlugIn:handle function. Afterwards,
%% it sends the answer to dispatcher and logs info about processing time.
-spec start_worker(Node :: atom(), Module :: atom(), WorkerArgs :: term(), State :: term()) -> Result when
    Result :: {Answer, NewState},
    Answer :: ok | error,
    NewState :: term().
%% ====================================================================
start_worker(Node, Module, WorkerArgs, State) ->
    try
        {ok, LoadMemorySize} = application:get_env(?APP_Name, worker_load_memory_size),
        {ok, ChildPid} = supervisor:start_child({?Supervisor_Name, Node}, ?Sup_Child(Module, worker_host, transient, [Module, WorkerArgs, LoadMemorySize])),
        Workers = State#cm_state.workers,
        ?info("Worker: ~s started at node: ~s", [Module, Node]),
        {ok, State#cm_state{workers = [{Node, Module, ChildPid} | Workers]}}
    catch
        _:Error ->
            ?error_stacktrace("Error: ~p during start of worker: ~s at node: ~s", [Error, Module, Node]),
            {error, State}
    end.

%% stop_worker/3
%% ====================================================================
%% @doc Processes client request using PlugIn:handle function. Afterwards,
%% it sends the answer to dispatcher and logs info about processing time.
-spec stop_worker(Node :: atom(), Module :: atom(), State :: term()) -> Result when
    Result :: {Answer, NewState},
    Answer :: ok | child_does_not_exist | delete_error | termination_error,
    NewState :: term().
%% ====================================================================
stop_worker(Node, Module, State = #cm_state{workers = Workers}) ->
    CreateNewWorkersList = fun({N, M, Child}, {WorkerList, ChosenChild}) ->
        case {N, M} of
            {Node, Module} -> {WorkerList, {N, Child}};
            {_N2, _M2} -> {[{N, M, Child} | WorkerList], ChosenChild}
        end
    end,
    {NewWorkers, ChosenChild} = lists:foldl(CreateNewWorkersList, {[], non}, Workers),
    try
        {ChildNode, _ChildPid} = ChosenChild,
        ok = supervisor:terminate_child({?Supervisor_Name, ChildNode}, Module),
        ok = supervisor:delete_child({?Supervisor_Name, ChildNode}, Module),
        ?info("Worker: ~s stopped at node: ~s", [Module, Node]),
        {ok, State#cm_state{workers = NewWorkers}}
    catch
        _:Error  ->
            ?error("Worker: ~s not stopped at node: ~s, error ~p", [Module, Node, {delete_error, Error}]),
            {Error, State#cm_state{workers = NewWorkers}}
    end.

%% check_node/2
%% ====================================================================
%% @doc Checks if any workers are running on node.
-spec check_node(Node :: atom(), State :: term()) -> NewState when
    NewState :: term().
%% ====================================================================
check_node(Node, State = #cm_state{workers = Workers}) ->
        pong = net_adm:ping(Node),
        Children = supervisor:which_children({?Supervisor_Name, Node}),
        {ok, NewWorkers} = add_children(Node, Children, Workers, State),
        WorkersFound = length(NewWorkers) > length(Workers),
        {ok, State#cm_state{workers = NewWorkers}, WorkersFound}.

%% add_children/3
%% ====================================================================
%% @doc Add workers that run on node to workers list.
-spec add_children(Node :: atom(), Children :: list(), Workers :: term(), State :: term()) -> NewWorkersList when
    NewWorkersList :: list().
%% ====================================================================
add_children(_Node, [], Workers, _State) ->
    {ok, Workers};
add_children(Node, [{Id, ChildPid, _Type, _Modules} | Children], Workers, State) ->
    Jobs = ?MODULES,
    case lists:member(Id, Jobs) of
        false -> add_children(Node, Children, Workers, State);
        true ->
            ?info("Worker ~p found at node ~s", [Id, Node]),

            case catch gen_server:call(ChildPid, dispatcher_map_unregistered, 500) of
                ok ->
                    {MapState2, Ans} = add_children(Node, Children, Workers, State),
                    {MapState2, [{Node, Id, ChildPid} | Ans]};
                Error ->
                    ?error_stacktrace("Error: ~p during contact with worker ~p found at node ~s", [Error, Id, Node]),
                    {error, Workers}
            end
    end.

%% node_down/2
%% ====================================================================
%% @doc Clears information about workers on node that is down.
-spec node_down(Node :: atom(), State :: term()) -> {NewState, WorkersFound} when
    NewState :: term(),
    WorkersFound :: boolean().
%% ====================================================================
node_down(Node, State = #cm_state{workers = Workers, nodes = Nodes, state_loaded = StateLoaded}) ->
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

    case StateLoaded of
        true -> gen_server:cast({global,?CCM}, init_cluster_once);
        _ -> ok
    end,
    {State#cm_state{workers = NewWorkers, nodes = NewNodes}, WorkersFound}.

%% merge_state/2
%% ====================================================================
%% @doc This function updates cluster state on the basis of data read
%% from DB.
-spec merge_state(State :: term(), SavedState :: term()) -> NewState when
    NewState :: term().
%% ====================================================================
merge_state(State, SavedState) ->
    ?debug("Merging state, current: ~p, saved: ~p", [State, SavedState]),
    StateNum = erlang:max(State#cm_state.state_num, SavedState#cm_state.state_num) + 1,
    State1 = State#cm_state{state_num = StateNum, state_loaded = true},

    NewNodes = lists:filter(fun(N) -> not lists:member(N, State1#cm_state.nodes) end, SavedState#cm_state.nodes),

    CreateNewState = fun(Node, {TmpState, TmpWorkersFound}) ->
        case State#cm_state.state_monitoring of
            on ->
                erlang:monitor_node(Node, true);
            off -> ok
        end,
        case catch check_node(Node, TmpState) of
            {ok, NewState, WorkersFound} ->
                case State#cm_state.state_monitoring of
                    on ->
                        erlang:monitor_node(Node, true);
                    off -> ok
                end,
                {NewState#cm_state{nodes = [Node | NewState#cm_state.nodes]}, TmpWorkersFound orelse WorkersFound};
            Error ->
                ?warning_stacktrace("Checking node ~p error: ~p", [Node, Error]),
                {State, TmpWorkersFound}
        end
    end,
    {MergedState, IncrementNum} = lists:foldl(CreateNewState, {State1, false}, NewNodes),

    MergedState2 = case IncrementNum of
                       true -> update_dispatchers_and_dns(MergedState, true, true);
                       false -> MergedState
                   end,
    MergedState2.

%% get_workers_list/1
%% ====================================================================
%% @doc This function provides the list of all alive workers with information
%% at which nodes they are working.
-spec get_workers_list(State :: term()) -> Workers when
    Workers :: list().
%% ====================================================================
get_workers_list(State) ->
    ListWorkers = fun({Node, Module, _ChildPid}, Workers) ->
        [{Node, Module} | Workers]
    end,
    lists:foldl(ListWorkers, [], State#cm_state.workers).

%% update_dispatchers_and_dns/3
%% ====================================================================
%% @doc This function updates all dispatchers and dnses.
-spec update_dispatchers_and_dns(State :: term(), UpdateDNS :: boolean(), IncreaseStateNum :: boolean()) -> NewState when
    NewState :: term().
%% ====================================================================
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

%% update_dispatcher_state/6
%% ====================================================================
%% @doc Updates dispatchers' states.
%% @end
-spec update_dispatcher_state(WorkersList, DispatcherMaps, Nodes, NewStateNum, Loads, AvgLoad) -> ok when
    WorkersList :: list(),
    DispatcherMaps :: list(),
    Nodes :: list(),
    NewStateNum :: integer(),
    Loads :: list(),
    AvgLoad :: integer().
%% ====================================================================
update_dispatcher_state(WorkersList, DispatcherMaps, Nodes, NewStateNum, Loads, AvgLoad) ->
    UpdateNode = fun(Node) ->
        gen_server:cast({?Dispatcher_Name, Node}, {update_workers, WorkersList, DispatcherMaps, NewStateNum, proplists:get_value(Node, Loads, 0), AvgLoad})
    end,
    lists:foreach(UpdateNode, Nodes),
    gen_server:cast(?Dispatcher_Name, {update_workers, WorkersList, DispatcherMaps, NewStateNum, 0, 0}).

%% update_dispatcher_state/3
%% ====================================================================
%% @doc Updates dispatchers' states.
%% @end
-spec update_dispatcher_state(Nodes, Loads, AvgLoad) -> ok when
    Nodes :: list(),
    Loads :: list(),
    AvgLoad :: integer().
%% ====================================================================
update_dispatcher_state(Nodes, Loads, AvgLoad) ->
    UpdateNode = fun(Node) ->
        gen_server:cast({?Dispatcher_Name, Node}, {update_loads, proplists:get_value(Node, Loads, 0), AvgLoad})
    end,
    lists:foreach(UpdateNode, Nodes).

%% update_dns_state/2
%% ====================================================================
%% @doc Updates dnses' states.
%% @end
-spec update_dns_state(WorkersList, NodeToLoad, AvgLoad) -> ok when
    WorkersList :: list(),
    NodeToLoad :: list(),
    AvgLoad :: number().
%% ====================================================================
update_dns_state(WorkersList, NodeToLoad, AvgLoad) ->
    MergeByFirstElement = fun(List) -> lists:reverse(lists:foldl(fun({Key, Value}, []) -> [{Key, [Value]}];
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
                        true -> case (AvgLoad > 0) and (NodeV >= 2 * AvgLoad) of
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

%% node_to_ip/1
%% ====================================================================
%% @doc Resolve ipv4 address of node.
%% @end
-spec node_to_ip(Node) -> Result when
    Result :: {ok, inet:ip4_address()}
    | {error, inet:posix()},
    Node :: atom().
%% ====================================================================
node_to_ip(Node) ->
    StrNode = atom_to_list(Node),
    AddressWith@ = lists:dropwhile(fun(Char) -> Char =/= $@ end, StrNode),
    Address = lists:dropwhile(fun(Char) -> Char =:= $@ end, AddressWith@),
    inet:getaddr(Address, inet).


%% change_monitoring/2
%% ====================================================================
%% @doc Starts or stops monitoring of nodes.
-spec change_monitoring(Nodes, Flag) -> ok when
    Nodes :: list(),
    Flag :: boolean().
%% ====================================================================
change_monitoring([], _Flag) ->
    ok;
change_monitoring([Node | Nodes], Flag) ->
    erlang:monitor_node(Node, Flag),
    change_monitoring(Nodes, Flag).

%% get_version/1
%% ====================================================================
%% @doc Provides list of versions of system elements.
-spec get_version(State :: term()) -> Result when
    Result :: list().
%% ====================================================================
get_version(State) ->
    Workers = get_workers_list(State),
    get_workers_versions(Workers).

%% get_workers_versions/1
%% ====================================================================
%% @doc Provides list of versions of workers.
-spec get_workers_versions(Workers :: list()) -> Result when
    Result :: list().
%% ====================================================================
get_workers_versions(Workers) ->
    get_workers_versions(Workers, []).

%% get_workers_versions/2
%% ====================================================================
%% @doc Provides list of versions of workers.
-spec get_workers_versions(Workers :: list(), TmpAnswer :: list()) -> Result when
    Result :: list().
%% ====================================================================
get_workers_versions([], Versions) ->
    Versions;
get_workers_versions([{Node, Module} | Workers], Versions) ->
    try
        V = gen_server:call({Module, Node}, {test_call, 1, get_version}, 500),
        get_workers_versions(Workers, [{Node, Module, V} | Versions])
    catch
        E1:E2 ->
            ?error_stacktrace("get_workers_versions error: ~p:~p", [E1, E2]),
            get_workers_versions(Workers, [{Node, Module, can_not_connect_with_worker} | Versions])
    end.

%% calculate_node_load/2
%% ====================================================================
%% @doc Calculates load of all nodes in cluster
-spec calculate_node_load(Nodes :: list(), Period :: atom()) -> Result when
    Result :: list().
%% ====================================================================
calculate_node_load(_Nodes, _Period) ->
    {[],0}. %todo

%% register_dispatcher_map/1
%% ====================================================================
%% @doc Registers information about map used by dispatcher
-spec register_dispatcher_map(Module :: atom(), Map :: term(), MapsList :: list()) -> Result when
    Result :: list().
%% ====================================================================
register_dispatcher_map(Module, Map, MapsList) ->
    ?debug("dispatcher map saved: ~p", [{Module, Map, MapsList}]),
    [{Module, Map} | proplists:delete(Module, MapsList)].
