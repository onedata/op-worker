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
-include("modules_and_args.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/0, start_link/1, stop/0]).
-export([monitoring_loop/1, monitoring_loop/2, start_monitoring_loop/2]).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Test API
%% ====================================================================
-ifdef(TEST).
-export([update_dns_state/3, update_dispatcher_state/5, calculate_load/2, calculate_worker_load/1, calculate_node_load/2]).
-endif.

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
  Error :: {already_started, Pid} | term().
%% ====================================================================
start_link() ->
  start_link(normal).

%% start_link/1
%% ====================================================================
%% @doc Starts cluster manager
-spec start_link(Mode) -> Result when
  Mode :: test | normal,
  Result ::  {ok,Pid}
  | ignore
  | {error,Error},
  Pid :: pid(),
  Error :: {already_started,Pid} | term().
%% ====================================================================
start_link(Mode) ->
  Args = case Mode of
    test -> [test];
    _Other -> []
  end,
  Ans = gen_server:start_link(?MODULE, Args, []),
  case Ans of
    {ok, Pid} -> global:re_register_name(?CCM, Pid);
    _A -> error
  end,
  Ans.

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
init([]) ->
  process_flag(trap_exit, true),
  {ok, Interval} = application:get_env(veil_cluster_node, initialization_time),
  Pid = self(),
  erlang:send_after(Interval * 1000, Pid, {timer, init_cluster}),
  erlang:send_after(50, Pid, {timer, {set_monitoring, on}}),
  erlang:send_after(100, Pid, {timer, start_central_logger}),
  erlang:send_after(150, Pid, {timer, get_state_from_db}),
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
handle_call({node_is_up, Node}, _From, State) ->
  Reply = State#cm_state.state_num,
  case Node =:= node() of
    true -> {reply, Reply, State};
    false ->
      Nodes = State#cm_state.nodes,
      case lists:member(Node, Nodes) of
        true -> {reply, Reply, State};
        false ->
          {Ans, NewState, WorkersFound} = check_node(Node, State),

          %% This case checks if node state was analysed correctly.
          %% If it was, it upgrades state number if necessary (workers
          %% were running on node).
          case Ans of
            ok ->
              case whereis(?Monitoring_Proc) of
                undefined -> ok;
                MPid -> MPid ! {monitor_node, Node}
              end,

              NewState2 = NewState#cm_state{nodes = [Node | Nodes]},

              NewState3 = case WorkersFound of
                true -> gen_server:cast({global, ?CCM}, update_dispatchers_and_dns);
                false -> NewState2
              end,

              save_state(NewState3),
              {reply, Reply, NewState3};
            _Other -> {reply, Reply, NewState}
          end
      end
  end;

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

handle_call({stop_worker, Node, Module}, _From, State) ->
  {Ans, New_State} = stop_worker(Node, Module, State),
  {reply, Ans, New_State};

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

handle_cast(update_dispatchers_and_dns, State) ->
  NewState = update_dispatchers_and_dns(State, true, true),
  {noreply, NewState};

handle_cast(get_state_from_db, State) ->
  case State#cm_state.nodes =:= [] of
    true ->
      Pid = self(),
      erlang:send_after(1000, Pid, {timer, get_state_from_db}),
      {noreply, State};
    false ->
      NewState = get_state_from_db(State),
      {noreply, NewState}
  end;

handle_cast(start_central_logger, State) ->
  case State#cm_state.nodes =:= [] of
    true ->
      Pid = self(),
      erlang:send_after(1000, Pid, {timer, start_central_logger}),
      {noreply, State};
    false ->
      NewState = start_central_logger(State),
      {noreply, NewState}
  end;

handle_cast(check_cluster_state, State) ->
  NewState = check_cluster_state(State),
  {noreply, NewState};

handle_cast({node_down, Node}, State) ->
  {NewState, WorkersFound} = node_down(Node, State),

  %% If workers were running on node that is down,
  %% upgrade state.
  NewState2 = case WorkersFound of
    true -> update_dispatchers_and_dns(NewState, true, true);
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

handle_cast(stop, State) ->
  {stop, normal, State};

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
handle_info({timer, Msg}, State) ->
  gen_server:cast({global, ?CCM}, Msg),
  {noreply, State};

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
terminate(_Reason, _State) ->
  catch whereis(?Monitoring_Proc) ! exit,
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
  {ok, Interval} = application:get_env(veil_cluster_node, hot_swapping_time),
  erlang:send_after(Interval, self(), {timer, update_monitoring_loop}),
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
  case length(Nodes) > 0 of
    true ->
      JobsAndArgs = ?Modules_With_Args,

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

          NewState2 = update_dispatchers_and_dns(NewState, true, true),
          save_state(NewState2),
          NewState2;
        false -> State
      end,

      plan_next_cluster_state_check(),
      NewState3;
    false ->
      Pid = self(),
      {ok, Interval} = application:get_env(veil_cluster_node, initialization_time),
      erlang:send_after(1000 * Interval, Pid, {timer, init_cluster}),
      State
  end.

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
check_cluster_state(State) ->
  CheckNum = (State#cm_state.cluster_check_num + 1) rem 15,

  NewState = case CheckNum of
    0 ->
      case length(State#cm_state.nodes) > 1 of
        true ->
          {NodesLoad, AvgLoad} = calculate_node_load(State#cm_state.nodes, long),
          WorkersLoad = calculate_worker_load(State#cm_state.workers),
          Load = calculate_load(NodesLoad, WorkersLoad),

          MinV = lists:min([NodeLoad || {_Node, NodeLoad, _ModulesLoads} <- Load, NodeLoad =/= error]),
          MaxV = lists:max([NodeLoad || {_Node, NodeLoad, _ModulesLoads} <- Load, NodeLoad =/= error]),
          case (MinV > 0) and (((MaxV >= 2* MinV) and (MaxV >= 1.5 * AvgLoad)) or (MaxV >= 5* MinV)) of
            true ->
              [{MaxNode, MaxNodeModulesLoads} | _] = [{Node, ModulesLoads} || {Node, NodeLoad, ModulesLoads} <- Load, NodeLoad == MaxV],
              MaxMV = lists:max([MLoad || {_Module, MLoad} <- MaxNodeModulesLoads, MLoad =/= error]),
              case MaxMV >= 0.5 of
                true ->
                  [MaxModule | _] = [Module || {Module, MLoad} <- MaxNodeModulesLoads, MLoad == MaxMV],
                  [{MinNode, MinNodeModulesLoads} | _] = [{Node, ModulesLoads} || {Node, NodeLoad, ModulesLoads} <- Load, NodeLoad == MinV],
                  MinWorkers = [Module || {Module, _MLoad} <- MinNodeModulesLoads, Module == MaxModule],
                  case MinWorkers =:= [] of
                    true ->
                      lager:info([{mod, ?MODULE}], "Worker: ~s will be started at node: ~s", [MaxModule, MinNode]),
                      {WorkerRuns, TmpState} = start_worker(MinNode, MaxModule, proplists:get_value(MaxModule, ?Modules_With_Args, []), State),
                      case WorkerRuns of
                        true ->
                          save_state(TmpState),
                          update_dispatchers_and_dns(TmpState, true, true);
                        false -> TmpState
                      end;
                    false ->
                      lager:info([{mod, ?MODULE}], "Worker: ~s will be stopped at node", [MaxModule, MaxNode]),
                      {WorkerStopped, TmpState2} = stop_worker(MaxNode, MaxModule, State),
                      case WorkerStopped of
                        true ->
                          save_state(TmpState2),
                          update_dispatchers_and_dns(TmpState2, true, true);
                        false -> TmpState2
                      end
                  end;
                false -> State
              end;
            false -> State
          end;
        false -> State
      end;
    _ ->
      case CheckNum rem 5 of
        0 -> update_dispatchers_and_dns(State, true, false);
        _ -> update_dispatchers_and_dns(State, false, false)
      end
  end,

  lager:info([{mod, ?MODULE}], "Cluster state ok"),
  NewState2 = init_cluster(NewState), %% if any worker is down and some worker type has no running instances, new worker should be started anywhere
  NewState2#cm_state{cluster_check_num = CheckNum}.

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
  erlang:send_after(Interval * 1000, self(), {timer, check_cluster_state}).

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
-spec add_children(Node :: atom(), Children :: list(), Workers :: term()) -> NewWorkersList when
  NewWorkersList :: list().
%% ====================================================================
add_children(_Node, [], Workers) ->
  Workers;

add_children(Node, [{Id, ChildPid, _Type, _Modules} | Children], Workers) ->
  Jobs = ?Modules,
  case lists:member(Id, Jobs) of
    false -> add_children(Node, Children, Workers);
    true -> [{Node, Id, ChildPid} | add_children(Node, Children, Workers)]
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

%% start_central_logger/1
%% ====================================================================
%% @doc This function starts the central_logger before other modules.
-spec start_central_logger(State :: term()) -> NewState when
  NewState :: term().
%% ====================================================================
start_central_logger(State) ->
  [LoggerNode | _] = State#cm_state.nodes,

  CreateRunningWorkersList = fun({_N, M, _Child}, Workers) ->
    [M | Workers]
  end,
  Workers = State#cm_state.workers,
  RunningWorkers = lists:foldl(CreateRunningWorkersList, [], Workers),
  case lists:member(central_logger, RunningWorkers) of
    true -> State;
    false ->
      {Ans, NewState} = start_worker(LoggerNode, central_logger, [], State),
      case Ans of
        ok -> update_dispatchers_and_dns(NewState, true, true);
        error -> NewState
      end
  end.

%% get_state_from_db/1
%% ====================================================================
%% @doc This function starts DAO and gets cluster state from DB.
-spec get_state_from_db(State :: term()) -> NewState when
  NewState :: term().
%% ====================================================================
get_state_from_db(State) ->
  [DaoNode | _] = State#cm_state.nodes,

  CreateRunningWorkersList = fun({_N, M, _Child}, Workers) ->
    [M | Workers]
  end,
  Workers = State#cm_state.workers,
  RunningWorkers = lists:foldl(CreateRunningWorkersList, [], Workers),
  case lists:member(dao, RunningWorkers) of
    true ->
      gen_server:cast({dao, DaoNode}, {synch, 1, {get_state, []}, cluster_state, {gen_serv, {global, ?CCM}}}),
      State;
    false ->
      {Ans, NewState} = start_worker(DaoNode, dao, [], State),
      case Ans of
        ok ->
          gen_server:cast({dao, DaoNode}, {synch, 1, {get_state, []}, cluster_state, {gen_serv, {global, ?CCM}}}),
          update_dispatchers_and_dns(NewState, true, true);
        error -> NewState
      end
  end.

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
    true -> update_dispatchers_and_dns(MergedState, true, true);
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

%% update_dispatchers_and_dns/3
%% ====================================================================
%% @doc This function updates all dispatchers and dnses.
-spec update_dispatchers_and_dns(State :: term(), UpdateDNS :: boolean(), IncreaseStateNum :: boolean()) -> NewState when
  NewState :: term().
%% ====================================================================
update_dispatchers_and_dns(State, UpdateDNS, IncreaseStateNum) ->
  case UpdateDNS of
    true ->
      {NodesLoad, AvgLoad} = calculate_node_load(State#cm_state.nodes, medium),
      WorkersLoad = calculate_worker_load(State#cm_state.workers),
      Load = calculate_load(NodesLoad, WorkersLoad),
      update_dns_state(State#cm_state.workers, Load, AvgLoad);
    false -> ok
  end,

  case IncreaseStateNum of
    true ->
      NewStateNum = State#cm_state.state_num + 1,
      WorkersList = get_workers_list(State),
      {NodesLoad2, AvgLoad2} = calculate_node_load(State#cm_state.nodes, short),
      update_dispatcher_state(WorkersList, State#cm_state.nodes, NewStateNum, NodesLoad2, AvgLoad2),
      State#cm_state{state_num = NewStateNum};
    false ->
      {NodesLoad2, AvgLoad2} = calculate_node_load(State#cm_state.nodes, short),
      update_dispatcher_state(State#cm_state.nodes, NodesLoad2, AvgLoad2),
      State
  end.

%% update_dispatcher_state/5
%% ====================================================================
%% @doc Updates dispatchers' states.
%% @end
-spec update_dispatcher_state(WorkersList, Nodes, NewStateNum, Loads, AvgLoad) -> ok when
	WorkersList :: list(),
	Nodes :: list(),
	NewStateNum :: integer(),
  Loads :: list(),
  AvgLoad :: integer().
%% ====================================================================
update_dispatcher_state(WorkersList, Nodes, NewStateNum, Loads, AvgLoad) ->
	UpdateNode = fun(Node) ->
		gen_server:cast({?Dispatcher_Name, Node}, {update_workers, WorkersList, NewStateNum, proplists:get_value(Node, Loads, 0), AvgLoad})
	end,
	lists:foreach(UpdateNode, Nodes).

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
	MergeByFirstElement = fun (List) -> lists:reverse(lists:foldl(fun({Key, Value}, []) ->  [{Key, [Value]}];
			({Key, Value}, [{Key, AccValues} | Tail]) -> [{Key, [Value | AccValues]} | Tail];
			({Key, Value}, Acc) -> [{Key, [Value]} | Acc]
		end, [], lists:keysort(1, List)))
	end,

	NodeToIPWithLogging = fun (Node) ->
		case node_to_ip(Node) of
			{ok, Address} -> Address;
			{error, Error} ->
				lager:error("Cannot resolve ip address for node ~p, error: ~p", [Node, Error]),
				unknownaddress
		end
	end,

  ModuleToNode = [{Module, Node} || {Node, Module, _Pid} <- WorkersList],
  MergedByModule = MergeByFirstElement(ModuleToNode),

	ModulesToNodes = lists:map(fun ({Module, Nodes}) ->
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
              true -> case NodeV >= 2*AvgLoad of
                  true->
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

      MinV = lists:min([V || {_Node, V} <- FilteredNodeToLoad]),
      FilteredNodeToLoad2 = case MinV of
        1 -> FilteredNodeToLoad;
        _ -> [{Node, erlang:round(V/MinV) } || {Node, V} <- FilteredNodeToLoad]
      end,

			IPs = [{NodeToIPWithLogging(Node), Param} || {Node, Param} <- FilteredNodeToLoad2],

			FilteredIPs = [{IP, Param} || {IP, Param} <- IPs, IP =/= unknownaddress],
			{Module, FilteredIPs}
		end, MergedByModule),

	FilteredModulesToNodes = [{Module, Nodes} || {Module, Nodes} <- ModulesToNodes, Nodes =/= []],
	UpdateDnsWorker = fun ({_Node, Module, Pid}) ->
    case Module of
      dns_worker -> gen_server:cast(Pid, {asynch, 1, {update_state, FilteredModulesToNodes}});
      _ -> ok
    end
	end,

	lists:foreach(UpdateDnsWorker, WorkersList),
  ok.

%% check_load/1
%% ====================================================================
%% @doc Checks load of worker plugin.
%% @end
-spec check_load(WorkerPlugin) -> {ok, float()} | {error, term()} when
	WorkerPlugin :: pid().
%% ====================================================================
check_load(WorkerPlugin) ->
	BeforeCall = os:timestamp(),
	try
		{LastLoadInfo, Load} = gen_server:call(WorkerPlugin, getLoadInfo),
		TimeDiff = timer:now_diff(BeforeCall, LastLoadInfo),
		Load / TimeDiff
	catch
		_:_ ->
      lager:error([{mod, ?MODULE}], "Can not get status of worker plugin"),
      error
	end.


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
	AddressWith@ = lists:dropwhile(fun (Char) -> Char =/= $@ end, StrNode),
	Address = lists:dropwhile(fun (Char) -> Char =:= $@ end, AddressWith@),
	inet:getaddr(Address, inet).


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
  monitoring_loop(Flag).

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

%% calculate_node_load/2
%% ====================================================================
%% @doc Calculates load of all nodes in cluster
-spec calculate_node_load(Nodes :: list(), Period :: atom()) -> Result when
  Result :: list().
%% ====================================================================
calculate_node_load(Nodes, Period) ->
  GetNodeInfo = fun(Node) ->
    Info = try
      gen_server:call({?Node_Manager_Name, Node}, {get_node_stats, Period})
    catch
      _:_ ->
        lager:error([{mod, ?MODULE}], "Can not get status of node: ~s", [Node]),
        {error, error, {error, error}}
    end,
    {Node, Info}
  end,
  NodesInfo = lists:map(GetNodeInfo, Nodes),
  GetNetMax = fun({_Node, {_Proc, _MemAvg, {InSum, OutSum}}}, {InTmp, OutTmp}) ->
    case InSum of
      error -> {InTmp, OutTmp};
      _ -> {erlang:max(InSum, InTmp), erlang:max(OutSum, OutTmp)}
    end
  end,
  {InMax, OutMax} = lists:foldl(GetNetMax, {0, 0}, NodesInfo),
  InMax2 = erlang:max(InMax, 1),
  OutMax2 = erlang:max(OutMax, 1),
  CalculateNodeValue = fun({Node, {Proc, MemAvg, {InSum, OutSum}}}, {TmpList, Sum, Count}) ->
    case Proc of
      error -> {[{Node, error} | TmpList], Sum, Count};
      _ ->
        V = Proc + MemAvg + InSum/InMax2 + OutSum/OutMax2,
        {[{Node, V} | TmpList], Sum + V, Count + 1}
    end
  end,
  {Ans, Sum, Count} = lists:foldl(CalculateNodeValue, {[], 0, 0}, NodesInfo),
  case Count of
    0 -> {Ans, 0};
    _ -> {Ans, Sum / Count}
  end.

%% calculate_worker_load/1
%% ====================================================================
%% @doc Calculates load of all workers in cluster
-spec calculate_worker_load(Workers :: list()) -> Result when
  Result :: list().
%% ====================================================================
calculate_worker_load(Workers) ->
  WorkersLoad = [{Node, {Module, check_load(Pid)}} || {Node, Module, Pid} <- Workers],

  MergeByFirstElement = fun (List) -> lists:reverse(lists:foldl(fun({Key, Value}, []) ->  [{Key, [Value]}];
    ({Key, Value}, [{Key, AccValues} | Tail]) -> [{Key, [Value | AccValues]} | Tail];
    ({Key, Value}, Acc) -> [{Key, [Value]} | Acc]
  end, [], lists:keysort(1, List)))
  end,

  MergedByNode = MergeByFirstElement(WorkersLoad),

  EvaluateLoad = fun ({Node, Modules}) ->
    GetLoadsSum = fun({Module, Value}, TmpAns) ->
      case Value of
        error -> TmpAns;
        _ -> TmpAns + Value
      end
    end,
    Sum = lists:foldl(GetLoadsSum, 0, Modules),

    case Sum == 0 of
      true -> {Node, Modules};
      false ->
        GetNewLoads = fun({Module, Value}) ->
          case Value of
            error -> {Module, Value};
            _ ->
              {Module, Value / Sum}
          end
        end,
        {Node, lists:map(GetNewLoads, Modules)}
    end
  end,
  lists:map(EvaluateLoad, MergedByNode).

%% calculate_load/2
%% ====================================================================
%% @doc Merges nodes' and workers' loads to more useful form
-spec calculate_load(NodesLoad :: list(), WorkersLoad :: list()) -> Result when
  Result :: list().
%% ====================================================================
calculate_load(NodesLoad, WorkersLoad) ->
  Merge = fun ({Node, Modules}) ->
    {Node, proplists:get_value(Node, NodesLoad, error), Modules}
  end,
  lists:map(Merge, WorkersLoad).

