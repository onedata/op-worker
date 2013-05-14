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
-include("records.hrl").
-include("supervision_macros.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/0]).
-export([monitoring_loop/1, monitoring_loop/2, start_monitoring_loop/2]).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Test API
%% ====================================================================
%%-ifdef(TEST).
-export([start_worker/4, stop_worker/3]).
%%-endif.

%% ====================================================================
%% API functions
%% ====================================================================

%% start_link/0
%% ====================================================================
%% @doc Starts cluster manager
-spec start_link() -> Result when
  Result ::  {ok,Pid}
  | ignore
  | {error,Error},
  Pid :: pid(),
  Error :: {already_started,Pid} | term().
%% ====================================================================
start_link() ->
  Ans = gen_server:start_link(?MODULE, [], []),
  case Ans of
    {ok, Pid} -> global:re_register_name(?CCM, Pid);
    _A -> error
  end,
  Ans.

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
init([]) ->
  {ok, Interval} = application:get_env(veil_cluster_node, initialization_time),
  timer:apply_after(Interval * 1000, gen_server, cast, [{global, ?CCM}, init_cluster]),
  timer:apply_after(50, gen_server, cast, [{global, ?CCM}, {set_monitoring, on}]),
  timer:apply_after(100, gen_server, cast, [{global, ?CCM}, get_state_from_db]),
  {ok, #cm_state{}}.

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
handle_call({node_is_up, Node}, _From, State) ->
  Nodes = State#cm_state.nodes,
  Reply = State#cm_state.state_num,
  case lists:member(Node, Nodes) of
    true -> {reply, Reply, State};
    false ->
      case whereis(?Monitoring_Proc) of
        undefined -> ok;
        MPid -> MPid ! {monitor_node, Node}
      end,

      {Ans, NewState, WorkersFound} = check_node(Node, State),

      %% This case checks if node state was analysed correctly.
      %% If it was, it upgrades state number if necessary (workers
      %% were running on node).
      case Ans of
        ok ->
          NewState2 = NewState#cm_state{nodes = [Node | Nodes]},

          NewState3 = case WorkersFound of
            true -> increase_state_num(NewState2);
            false -> NewState2
          end,

          save_state(NewState3),
          {reply, Reply, NewState3};
        _Other -> {reply, Reply, NewState}
      end
  end;

handle_call(get_state_num, _From, State) ->
  {reply, State#cm_state.state_num, State};

handle_call(get_nodes, _From, State) ->
  {reply, State#cm_state.nodes, State};

handle_call(get_workers, _From, State) ->
  {reply, {get_workers_list(State), State#cm_state.state_num}, State};

handle_call(get_state, _From, State) ->
  {reply, State, State};

handle_call(get_version, _From, State) ->
  {reply, get_version(State), State};

handle_call(_Request, _From, State) ->
  {reply, wrong_request, State}.


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
handle_cast(init_cluster, State) ->
  NewState = init_cluster(State),
  {noreply, NewState};

handle_cast(get_state_from_db, State) ->
  NewState = get_state_from_db(State),
  {noreply, NewState};

handle_cast(check_cluster_state, State) ->
  NewState = check_cluster_state(State),
  {noreply, NewState};

handle_cast({node_down, Node}, State) ->
  {NewState, WorkersFound} = node_down(Node, State),

  %% If workers were running on node that is down,
  %% upgrade state.
  NewState2 = case WorkersFound of
    true -> increase_state_num(NewState);
    false -> NewState
  end,

  save_state(NewState2),
  {noreply, NewState2};

handle_cast({set_monitoring, Flag}, State) ->
  case whereis(?Monitoring_Proc) of
    undefined ->
      {ok, _ChildPid} = supervisor:start_child(?Supervisor_Name, ?Sup_Child(monitor_process, ?MODULE, start_monitoring_loop, permanent, [Flag, State#cm_state.nodes]));
    MPid ->
      MPid ! {Flag, State#cm_state.nodes}
  end,
  {noreply, State};

handle_cast(update_monitoring_loop, State) ->
  whereis(?Monitoring_Proc) ! switch_code,
  {noreply, State};

handle_cast({worker_answer, cluster_state, Response}, State) ->
  NewState = case Response of
    {ok, SavedState} ->
      lager:info([{mod, ?MODULE}], "State read from DB"),
      merge_state(State, SavedState);
    {error, Error} ->
      lager:info([{mod, ?MODULE}], "State cannot be read from DB: ~s", [Error]), %% info logging level because state may not be present in db and it's not an error
      State
  end,
  {noreply, NewState};

handle_cast(_Msg, State) ->
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
handle_info(_Info, State) ->
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
terminate(_Reason, State) ->
  whereis(?Monitoring_Proc) ! exit,
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
  timer:apply_after(10000, gen_server, cast, [{global, ?CCM}, update_monitoring_loop]),
  {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

%% init_cluster/1
%% ====================================================================
%% @doc Initializes cluster - decides at which nodes components should
%% be started (and starts them). Additionally, it sets timer that
%% initiates checking of cluster state.
-spec init_cluster(State :: term()) -> NewState when
  NewState ::  term().
%% ====================================================================
init_cluster(State) ->
  Nodes = State#cm_state.nodes,
  %%Jobs = [cluster_rengine, control_panel, dao, fslogic, gateway, rtransfer, rule_manager],
  JobsAndArgs = [{cluster_rengine, []}, {control_panel, []}, {dao, []}, {fslogic, []}, {gateway, []}, {rtransfer, []}, {rule_manager, []}],

  CreateRunningWorkersList = fun({_N, M, _Child}, Workers) ->
    [M | Workers]
  end,
  Workers = State#cm_state.workers,
  RunningWorkers = lists:foldl(CreateRunningWorkersList, [], Workers),

  CreateJobsList = fun({Job, A}, {TmpJobs, TmpArgs}) ->
     case lists:member(Job, RunningWorkers) of
       true -> {TmpJobs, TmpArgs};
       false -> {[Job | TmpJobs], [A | TmpArgs]}
     end
  end,
  {Jobs, Args} = lists:foldl(CreateJobsList, {[], []}, JobsAndArgs),

  NewState3 = case length(Jobs) > 0 of
    true ->
      NewState = case erlang:length(Nodes) >= erlang:length(Jobs) of
        true -> init_cluster_nodes_dominance(State, Nodes, Jobs, [], Args, []);
        false -> init_cluster_jobs_dominance(State, Jobs, Args, Nodes, [])
      end,

      NewState2 = increase_state_num(NewState),
      save_state(NewState2),
      NewState2;
    false -> State
  end,

  plan_next_cluster_state_check(),
  NewState3.

%% init_cluster_nodes_dominance/6
%% ====================================================================
%% @doc Chooses node for workers when there are more nodes than workers.
-spec init_cluster_nodes_dominance(State :: term(), Nodes :: list(), Jobs1 :: list(),
    Jobs2 :: list(), Args1 :: list(), Args2 :: list()) -> NewState when
  NewState ::  term().
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
  NewState ::  term().
%% ====================================================================
init_cluster_jobs_dominance(State, [], [], _Nodes1, _Nodes2) ->
  State;
init_cluster_jobs_dominance(State, Jobs, Args, [], Nodes2) ->
  init_cluster_jobs_dominance(State, Jobs, Args, Nodes2, []);
init_cluster_jobs_dominance(State, [J | Jobs], [A | Args], [N | Nodes1], Nodes2) ->
  {_Ans, NewState} = start_worker(N, J, A, State),
  init_cluster_jobs_dominance(NewState, Jobs, Args, Nodes1, [N | Nodes2]).

%% check_cluster_state/1
%% ====================================================================
%% @doc Checks cluster state and decides if any new component should
%% be started (currently running ones are overloaded) or stopped.
-spec check_cluster_state(State :: term()) -> NewState when
  NewState ::  term().
%% ====================================================================
%% TODO zaproponować algorytm
check_cluster_state(State) ->
  plan_next_cluster_state_check(),
  State.

%% plan_next_cluster_state_check/0
%% ====================================================================
%% @doc Decides when cluster state should be checked next time and sets
%% the timer (cluster_clontrol_period environment variable is used).
-spec plan_next_cluster_state_check() -> Result when
  Result :: {ok, TRef} | {error, Reason},
  TRef :: term(),
  Reason :: term().
%% ====================================================================
plan_next_cluster_state_check() ->
  {ok, Interval} = application:get_env(veil_cluster_node, cluster_clontrol_period),
  timer:apply_after(Interval * 1000, gen_server, cast, [{global, ?CCM}, check_cluster_state]).

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
    {ok, LoadMemorySize} = application:get_env(veil_cluster_node, worker_load_memory_size),
    {ok, ChildPid} = supervisor:start_child({?Supervisor_Name, Node}, ?Sup_Child(Module, worker_host, transient, [Module, WorkerArgs, LoadMemorySize])),
    Workers = State#cm_state.workers,
    lager:info([{mod, ?MODULE}], "Worker: ~s started at node: ~s", [Module, Node]),
    {ok, State#cm_state{workers = [{Node, Module, ChildPid} | Workers]}}
  catch
    _:_ ->
      lager:error([{mod, ?MODULE}], "Error during start of worker: ~s started at node: ~s", [Module, Node]),
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
stop_worker(Node, Module, State) ->
  CreateNewWorkersList = fun({N, M, Child}, {Workers, ChosenChild}) ->
    case {N, M} of
      {Node, Module} -> {Workers, {N, Child}};
      {_N2, _M2} -> {[{N, M, Child} | Workers], ChosenChild}
    end
  end,
  Workers = State#cm_state.workers,
  {NewWorkers, ChosenChild} = lists:foldl(CreateNewWorkersList, {[], non}, Workers),
  Ans = case ChosenChild of
    non -> child_does_not_exist;
    {ChildNode, _ChildPid} ->
      Ans2 = supervisor:terminate_child({?Supervisor_Name, ChildNode}, Module),
      case Ans2 of
        ok -> Ans3 = supervisor:delete_child({?Supervisor_Name, ChildNode}, Module),
          case Ans3 of
            ok -> ok;
            {error, _Error} -> delete_error
          end;
        {error, _Error} -> termination_error
      end
  end,
  {Ans, State#cm_state{workers = NewWorkers}}.

%% check_node/2
%% ====================================================================
%% @doc Checks if any workers are running on node.
-spec check_node(Node :: atom(), State :: term()) -> NewState when
  NewState :: term().
%% ====================================================================
check_node(Node, State) ->
  try
    Ans = net_adm:ping(Node),
    case Ans of
      pong ->
        Children = supervisor:which_children({?Supervisor_Name, Node}),
        Workers = State#cm_state.workers,
        NewWorkers = add_children(Node, Children, Workers),
        WorkersFound = length(NewWorkers) > length(Workers),
        {ok, State#cm_state{workers = NewWorkers}, WorkersFound};
      pang -> {error, State, false}
    end
  catch
    _:_ -> {error, State, false}
  end.

%% add_children/3
%% ====================================================================
%% @doc Add workers that run on node to workers list.
-spec add_children(Node :: atom(), Childern :: list(), Workers :: term()) -> NewWorkersList when
  NewWorkersList :: list().
%% ====================================================================
add_children(_Node, [], Workers) ->
  Workers;

add_children(Node, [{Id, ChildPid, _Type, _Modules} | Children], Workers) ->
  case Id of
    node_manager -> add_children(Node, Children, Workers);
    cluster_manager -> add_children(Node, Children, Workers);
    request_dispatcher -> add_children(Node, Children, Workers);
    _Other -> [{Node, Id, ChildPid} | add_children(Node, Children, Workers)]
  end.

%% node_down/2
%% ====================================================================
%% @doc Clears information about workers on node that is down.
-spec node_down(Node :: atom(), State :: term()) -> NewState when
  NewState :: term().
%% ====================================================================
node_down(Node, State) ->
  CreateNewWorkersList = fun({N, M, Child}, {Workers, Found}) ->
    case N of
      Node -> {Workers, true};
      _N2 -> {[{N, M, Child} | Workers], Found}
    end
  end,
  Workers = State#cm_state.workers,
  {NewWorkers, WorkersFound} = lists:foldl(CreateNewWorkersList, {[], false}, Workers),

  CreateNewNodesList = fun(N, Nodes) ->
    case N of
      Node -> Nodes;
      _N2 -> [N | Nodes]
    end
  end,
  Nodes = State#cm_state.nodes,
  NewNodes = lists:foldl(CreateNewNodesList, [], Nodes),

  {State#cm_state{workers = NewWorkers, nodes = NewNodes}, WorkersFound}.

%% get_state_from_db/1
%% ====================================================================
%% @doc This function starts DAO and gets cluster state from DB.
-spec get_state_from_db(State :: term()) -> NewState when
  NewState :: term().
%% ====================================================================
get_state_from_db(State) ->
  {Ans, NewState} = start_worker(node(), dao, [], State),
  NewState2 = case Ans of
    ok ->
      gen_server:cast(dao, {synch, 1, {get_state, []}, cluster_state, {gen_serv, {global, ?CCM}}}),
      increase_state_num(NewState);
    error -> NewState
  end,
  NewState2.

%% save_state/1
%% ====================================================================
%% @doc This function saves cluster state in DB.
-spec save_state(State :: term()) -> ok.
%% ====================================================================
save_state(State) ->
  gen_server:cast(dao, {asynch, 1, {save_state, [State]}}).

%% merge_state/2
%% ====================================================================
%% @doc This function updates cluster state on the basis of data read
%% from DB.
-spec merge_state(State :: term(), SavedState:: term()) -> NewState when
  NewState :: term().
%% ====================================================================
merge_state(State, SavedState) ->
  StateNum = erlang:max(State#cm_state.state_num, SavedState#cm_state.state_num),
  State1 = State#cm_state{state_num = StateNum},

  NewNodes = lists:filter(fun(N) -> not lists:member(N, State1#cm_state.nodes) end, SavedState#cm_state.nodes),

  CreateNewState = fun(Node, {TmpState, TmpWorkersFound}) ->
    whereis(?Monitoring_Proc) ! {monitor_node, Node},
    {Ans, NewState, WorkersFound} = check_node(Node, TmpState),
    case Ans of
      ok ->
        whereis(?Monitoring_Proc) ! {monitor_node, Node},
        NewState2 = NewState#cm_state{nodes = [Node | NewState#cm_state.nodes]},
        case WorkersFound of
          true -> {NewState2, true};
          false -> {NewState2, TmpWorkersFound}
        end;
      _Other -> {NewState, TmpWorkersFound}
    end
  end,
  {MergedState, IncrementNum} = lists:foldl(CreateNewState, {State1, false}, NewNodes),

  MergedState2 = case IncrementNum of
    true -> increase_state_num(MergedState);
    false -> MergedState
  end,

  save_state(MergedState2),
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

%% increase_state_num/1
%% ====================================================================
%% @doc This function increases the cluster state value and informs all
%% dispatchers about it.
-spec increase_state_num(State :: term()) -> NewState when
  NewState :: term().
%% ====================================================================
increase_state_num(State) ->
  NewStateNum = State#cm_state.state_num + 1,
  WorkersList = get_workers_list(State),
  UpdateNode = fun(Node) ->
    gen_server:cast({?Dispatcher_Name, Node}, {update_workers, WorkersList, NewStateNum})
  end,
  lists:foreach(UpdateNode, State#cm_state.nodes),

  State#cm_state{state_num = NewStateNum}.

%% start_monitoring_loop/2
%% ====================================================================
%% @doc Starts loop that monitors if nodes are alive.
-spec start_monitoring_loop(Flag, Nodes) -> ok when
  Flag :: on | off,
  Nodes :: list().
%% ====================================================================
start_monitoring_loop(Flag, Nodes) ->
  Pid = spawn_link(?MODULE, monitoring_loop, [Flag, Nodes]),
  register(?Monitoring_Proc, Pid),
  {ok, Pid}.

%% monitoring_loop/2
%% ====================================================================
%% @doc Beginning of loop that monitors if nodes are alive.
-spec monitoring_loop(Flag, Nodes) -> ok when
  Flag :: on | off,
  Nodes :: list().
%% ====================================================================
monitoring_loop(Flag, Nodes) ->
  case Flag of
    on ->
      change_monitoring(Nodes, true);
    off -> ok
  end,
  monitoring_loop(on).

%% monitoring_loop/1
%% ====================================================================
%% @doc Loop that monitors if nodes are alive.
-spec monitoring_loop(Flag) -> ok when
  Flag :: on | off.
%% ====================================================================
monitoring_loop(Flag) ->
  receive
    {nodedown, Node} ->
      case Flag of
        on ->
          erlang:monitor_node(Node, false),
          gen_server:cast({global, ?CCM}, {node_down, Node});
        off -> ok
      end,
      monitoring_loop(Flag);
    {monitor_node, Node} ->
      case Flag of
        on ->
          erlang:monitor_node(Node, true);
        off -> ok
      end,
      monitoring_loop(Flag);
    {off, Nodes} ->
      case Flag of
        on ->
          change_monitoring(Nodes, false);
        off -> ok
      end,
      monitoring_loop(off);
    {on, Nodes} ->
      case Flag of
        off ->
          change_monitoring(Nodes, true);
        on -> ok
      end,
      monitoring_loop(on);
    {get_version, Reply_Pid} ->
      Reply_Pid ! {monitor_process_version, node_manager:check_vsn()},
      monitoring_loop(Flag);
    switch_code ->
      ?MODULE:monitoring_loop(Flag);
    exit ->
      ok
  end.

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
  Versions = get_workers_versions(Workers),
  Pid = self(),
  whereis(?Monitoring_Proc) ! {get_version, Pid},
  receive
    {monitor_process_version, V} -> [{node(), monitor_process, V} | Versions]
  after 500 ->
    [{node(), monitor_process, error} | Versions]
  end.

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
  V = gen_server:call({Module, Node}, {test_call, 1, get_version}),
  get_workers_versions(Workers, [{Node, Module, V} | Versions]).