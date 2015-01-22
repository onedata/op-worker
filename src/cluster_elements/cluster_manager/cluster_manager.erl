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
-include("cluster_elements/worker_host/worker_proxy.hrl").
-include_lib("ctool/include/logging.hrl").

%% This record is used by ccm (it contains its state). It describes
%% nodes, dispatchers and workers in cluster. It also contains reference
%% to process used to monitor if nodes are alive.
-record(cm_state, {nodes = [], workers = [], state_num = 1}).

%% API
-export([start_link/0, stop/0]).

%% gen_event callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

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
            case catch join_new_node(SenderNode, State) of
                {ok, {NewState, WorkersFound}} ->
                    erlang:monitor_node(SenderNode, true),
                    case WorkersFound of
                        true -> update_dispatchers_and_dns(NewState);
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
            update_dispatchers_and_dns(State);
        {_, _} ->
            ?info("Initialization of jobs ~p using nodes ~p", [Jobs, Nodes]),
            NewState =
                case erlang:length(Nodes) >= erlang:length(Jobs) of
                    true -> init_cluster_nodes_dominance(State, Nodes, Jobs, [], Args, []);
                    false -> init_cluster_jobs_dominance(State, Jobs, Args, Nodes, [])
                end,
            update_dispatchers_and_dns(NewState)
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
        NewState = State#cm_state{workers = NewWorkers},
        {ChildNode, _ChildPid} = ChosenChild,
        update_dispatchers_and_dns(NewState),
        ok = supervisor:terminate_child({?SUPERVISOR_NAME, ChildNode}, Module),
        ok = supervisor:delete_child({?SUPERVISOR_NAME, ChildNode}, Module),
        ?info("Worker: ~s stopped at node: ~s", [Module, Node]),
        NewState
    catch
        _:Error ->
            ?error("Worker: ~s not stopped at node: ~s, error ~p", [Module, Node, {delete_error, Error}]),
            State#cm_state{workers = NewWorkers}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if node can be connected and if any workers are running on it.
%% Returns updated ccm state and a flag that indicates if new node
%% contains workers.
%% @end
%%--------------------------------------------------------------------
-spec join_new_node(Node :: atom(), State :: term()) -> {ok, {NewState :: #cm_state{}, WorkersFound :: boolean()}} | no_return().
join_new_node(Node, State = #cm_state{workers = Workers}) ->
    pong = net_adm:ping(Node),
    Children = supervisor:which_children({?SUPERVISOR_NAME, Node}),
    NewWorkers = add_children(Node, Children, Workers, State),
    WorkersFound = length(NewWorkers) > length(Workers),
    {ok, {State#cm_state{workers = NewWorkers}, WorkersFound}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Add workers that run on node to workers list.
%% @end
%%--------------------------------------------------------------------
-spec add_children(Node :: atom(), Children :: list(), Workers :: term(), State :: term()) -> list().
add_children(_Node, [], Workers, _State) ->
    Workers;
add_children(Node, [{Id, ChildPid, _Type, _Modules} | Children], Workers, State) ->
    case lists:member(Id, ?MODULES) of
        false -> add_children(Node, Children, Workers, State);
        true ->
            ?info("Worker ~p found at node ~s", [Id, Node]),
            [{Node, Id, ChildPid} | add_children(Node, Children, Workers, State)]
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
        true -> update_dispatchers_and_dns(NewState);
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
-spec update_dispatchers_and_dns(State :: term()) -> NewState when
    NewState :: term().
update_dispatchers_and_dns(State) ->
    ?debug("update_dispatchers_and_dns, state: ~p", [State]),
    NewStateNum = State#cm_state.state_num + 1,
    WorkersList = get_workers_list(State),
    update_dns_state(State#cm_state.workers),
    update_dispatcher_state(WorkersList, State#cm_state.nodes, NewStateNum),
    State#cm_state{state_num = NewStateNum}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates dispatchers' states.
%% @end
%%--------------------------------------------------------------------
-spec update_dispatcher_state(WorkersList :: list(), Nodes :: list(), NewStateNum :: integer()) -> ok.
update_dispatcher_state(WorkersList, Nodes, NewStateNum) ->
    UpdateNode = fun(Node) ->
        gen_server:cast({?DISPATCHER_NAME, Node}, {update_state, WorkersList, NewStateNum})
    end,
    lists:foreach(UpdateNode, Nodes),
    gen_server:cast(?DISPATCHER_NAME, {update_state, WorkersList, NewStateNum}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates dnses' states.
%% @end
%%--------------------------------------------------------------------
-spec update_dns_state(WorkersList :: list()) -> ok.
update_dns_state(WorkersList) ->
    %prepare worker IPs with loads info
    Nodes = [Node || {Node, _, _} <- WorkersList],
    UniqueNodes = sets:to_list(sets:from_list(Nodes)),
    UniqueNodesIpToLoad = [{cluster_manager_utils:node_to_ip(Node), node_monitoring:node_load(Node)} || Node <- UniqueNodes],
    FilteredUniqueNodesIpToLoad = [{IP, Load} || {IP, Load} <- UniqueNodesIpToLoad, IP =/= unknownaddress],

    %prepare modules with their nodes and loads info
    ModuleToNode = [{Module, Node} || {Node, Module, _Pid} <- WorkersList],
    ModuleToNodeList = cluster_manager_utils:aggregate_over_first_element(ModuleToNode),
    ModuleToNodeListWithLoad = lists:map(
        fun
            ({Module, []}) ->
                {Module, []};
            ({Module, NodeList}) ->
                IPToLoad = [{cluster_manager_utils:node_to_ip(Node), node_monitoring:node_load(Node)} || Node <- NodeList],
                FilteredIPs = [{IP, Param} || {IP, Param} <- IPToLoad, IP =/= unknownaddress],
                {Module, FilteredIPs}
        end, ModuleToNodeList),
    FilteredModuleToNodeListWithLoad = [{Module, NodeList} || {Module, NodeList} <- ModuleToNodeListWithLoad, NodeList =/= []],

    % prepare average load
    LoadAverage = cluster_manager_utils:average([Load || {_, Load} <- FilteredUniqueNodesIpToLoad]),

    UpdateInfo = {update_state, FilteredModuleToNodeListWithLoad, FilteredUniqueNodesIpToLoad, LoadAverage},
    ?debug("updating dns, update message: ~p", [UpdateInfo]),
    [gen_server:cast(Pid, #worker_request{req = UpdateInfo}) || {_, dns_worker, Pid} <- WorkersList],
    ok.