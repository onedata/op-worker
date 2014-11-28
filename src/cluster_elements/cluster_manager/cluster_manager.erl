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
-include_lib("ctool/include/logging.hrl").

-define(STATS_WEIGHTS, [{"cpu", 100}, {"mem", 100}]).
-define(CALLBACKS_TABLE, callbacks_table).

-define(ACTION_START_WORKER, start_worker).
-define(ACTION_STOP_WORKER, stop_worker).

-record(cluster_stats, {cpu = {0, 0}, memory = {0, 0}, net_rx_b = {0, 0}, net_tx_b = {0, 0}, net_rx_pps = {0, 0},
  net_tx_pps = {0, 0}, ports_rx_b = {0, 0}, ports_tx_b = {0, 0}}).

%% Minimal message ID that can be used - it is limited by type of Answer.message_id in protocol buffers
%% TODO: change type of message_id to sint32, more at https://developers.google.com/protocol-buffers/docs/proto#scalar
-define(MIN_MSG_ID, -1073741824). %% 1073741824 = 2^30

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
-export([update_dns_state/3, update_dispatcher_state/6, calculate_load/2, calculate_worker_load/1, calculate_node_load/2,
  merge_nodes_stats/1, map_node_stats_to_load/1, required_permanent_workers/4]).
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
  ets:new(?CALLBACKS_TABLE, [named_table, set]),
  process_flag(trap_exit, true),
  {ok, Interval} = application:get_env(?APP_Name, initialization_time),
  Pid = self(),
  erlang:send_after(Interval * 1000, Pid, {timer, init_cluster}),

  {ok, Interval2} = application:get_env(?APP_Name, heart_beat),
  LoggerAndDAOInterval = case Interval > (2 * Interval2 + 1) of
                           true -> 2 * Interval2;
                           false -> Interval - 1
                         end,
  erlang:send_after(LoggerAndDAOInterval * 1000 + 100, Pid, {timer, start_central_logger}),
  erlang:send_after(LoggerAndDAOInterval * 1000 + 200, Pid, {timer, get_state_from_db}),
  {ok, MonitoringInitialization} = application:get_env(?APP_Name, cluster_monitoring_initialization),
  erlang:send_after(1000 * MonitoringInitialization, self(), {timer, start_cluster_monitoring}),
  {ok, #cm_state{}};

init([test]) ->
  ets:new(?CALLBACKS_TABLE, [named_table, set]),
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

handle_call({addCallback, FuseId, Node, Pid}, _From, State) ->
  try
    gen_server:call({?Node_Manager_Name, Node}, {addCallback, FuseId, Pid}, 500),
    CallbacksNum = case add_callback(Node, FuseId, State#cm_state.nodes, State#cm_state.callbacks_num) of
                     updated ->
                       save_state(),
                       State#cm_state.callbacks_num + 1;
                     _ -> State#cm_state.callbacks_num
                   end,
    {reply, ok, State#cm_state{callbacks_num = CallbacksNum}}
  catch
    _:_ ->
      ?error("Can not add callback of node: ~p, pid ~p, fuseId ~p", [Node, Pid, FuseId]),
      {reply, error, State}
  end;

handle_call({delete_callback, FuseId, Node, Pid}, _From, State) ->
  try
    Ans = gen_server:call({?Node_Manager_Name, Node}, {delete_callback, FuseId, Pid}, 500),
    CallbacksNum = case Ans of
                     fuse_deleted ->
                       case delete_callback(Node, FuseId, State#cm_state.nodes, State#cm_state.callbacks_num, true) of
                         updated ->
                           save_state(),
                           State#cm_state.callbacks_num + 1;
                         deleted ->
                           save_state(),
                           State#cm_state.callbacks_num + 1;
                         _ -> State#cm_state.callbacks_num
                       end;
                     _ -> State#cm_state.callbacks_num
                   end,
    {reply, ok, State#cm_state{callbacks_num = CallbacksNum}}
  catch
    _:_ ->
      ?error("Can not delete callback of node: ~p, pid ~p, fuseId ~p", [Node, Pid, FuseId]),
      {reply, error, State}
  end;

handle_call(get_callbacks, _From, State) ->
  {reply, {get_callbacks(), State#cm_state.callbacks_num}, State};

handle_call(check, _From, State) ->
  {reply, ok, State};

%% TODO: add generic mechanism that do the same thing
handle_call({update_cluster_rengines, EventType, EventHandlerItem}, _From, State) ->
  Workers = State#cm_state.workers,
  UpdateClusterRengine = fun ({Node, Module, Pid}) ->
    case Module of
      cluster_rengine ->
        ?debug("Update of rengine at node ~p initialized by CCM. Params: ~p", [Node, {update_cluster_rengine, EventType, EventHandlerItem}]),
        gen_server:cast(Pid, {asynch, 1, {update_cluster_rengine, EventType, EventHandlerItem}});
      _ -> ok
    end
  end,

  lists:foreach(UpdateClusterRengine, Workers),
  {reply, ok, State};

handle_call({get_cluster_stats, TimeWindow}, _From, State) ->
  Reply = get_cluster_stats(TimeWindow),
  {reply, Reply, State};

handle_call({get_cluster_stats, StartTime, EndTime}, _From, State) ->
  Reply = get_cluster_stats(StartTime, EndTime),
  {reply, Reply, State};

handle_call({get_cluster_stats, StartTime, EndTime, Columns}, _From, State) ->
  Reply = get_cluster_stats(StartTime, EndTime, Columns),
  {reply, Reply, State};

handle_call({set_provider_id, ProviderId}, _From, State) ->
  {reply, ok, State#cm_state{provider_id = ProviderId}};

handle_call(get_provider_id, _From, #cm_state{provider_id = ProviderId} = State) ->
  {reply, {ok, ProviderId}, State};

%% TODO if callbacks with ack will be used intensively, information should be forwarded to nodes without ccm usage
%% (e.g. request dispatchers can have nodes list)
handle_call({node_for_ack, NodeForAck}, _From, State) ->
  MsgID = case get(callback_msg_ID) of
            ID when is_integer(ID) and (ID > ?MIN_MSG_ID) ->
              put(callback_msg_ID, ID - 1),
              ID - 1;
            _ ->
              put(callback_msg_ID, -2),
              -2
          end,

  SendToNodes = fun(Node) ->
    gen_server:cast({?Node_Manager_Name, Node}, {node_for_ack, MsgID, NodeForAck})
  end,
  lists:foreach(SendToNodes, State#cm_state.nodes),

  {reply, MsgID, State};

%% Test call
handle_call({start_worker, Node, Module, WorkerArgs}, _From, State) ->
  handle_test_call({start_worker, Node, Module, WorkerArgs}, _From, State);

%% Test call
handle_call(check_state_loaded, _From, State) ->
  handle_test_call(check_state_loaded, _From, State);

handle_call({lifecycle_notification, Node, Module, Action}, _From, State) ->
  Res = lifecycle_notification(Node, Module, Action, State#cm_state.workers, State),
  {reply, Res, State};

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
handle_cast({node_is_up, Node}, State) ->
  ?debug("Heartbeat from node: ~p", [Node]),
  case Node =:= node() of
    true ->
      gen_server:cast({?Node_Manager_Name, Node}, {heart_beat_ok, State#cm_state.state_num, State#cm_state.callbacks_num}),
      {noreply, State};
    false ->
      Nodes = State#cm_state.nodes,
      case lists:member(Node, Nodes) of
        true ->
          gen_server:cast({?Node_Manager_Name, Node}, {heart_beat_ok, State#cm_state.state_num, State#cm_state.callbacks_num}),
          {noreply, State};
        false ->
          ?info("New node: ~p", [Node]),
          {Ans, NewState, WorkersFound} = check_node(Node, State),

          %% This case checks if node state was analysed correctly.
          %% If it was, it upgrades state number if necessary (workers
          %% were running on node).
          case Ans of
            ok ->
              case State#cm_state.state_loaded of
                true ->
                  Pid = self(),
                  erlang:send_after(50, Pid, {timer, init_cluster_once});
                _ -> ok
              end,

              case State#cm_state.state_monitoring of
                on ->
                  erlang:monitor_node(Node, true);
                off -> ok
              end,

              NewState2 = NewState#cm_state{nodes = [Node | Nodes]},

              case WorkersFound of
                true -> gen_server:cast({global, ?CCM}, update_dispatchers_and_dns);
                false -> ok
              end,
              save_state(),

              gen_server:cast({?Node_Manager_Name, Node}, {heart_beat_ok, State#cm_state.state_num, State#cm_state.callbacks_num}),
              {noreply, NewState2};
            _Other ->
              gen_server:cast({?Node_Manager_Name, Node}, {heart_beat_ok, State#cm_state.state_num, State#cm_state.callbacks_num}),
              {noreply, NewState}
          end
      end
  end;

handle_cast({register_module_listener, Module, Listener}, State) ->
  ?debug("Registering module lifecycle listeners. ~p", [{Module, Listener}]),
  NewState = add_module_lifecycle_listener(Module, Listener, State),
  save_state(),
  {noreply, NewState};

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

handle_cast({register_dispatcher_map, Module, Map, AnsPid}, State) ->
  Maps = State#cm_state.dispatcher_maps,
  case proplists:get_value(Module, Maps, not_found) =:= Map of
    true ->
      % debug, because it is ok when more than one instance of worker exists
      ?debug("Registration of existing disp map for module ~p", [Module]),
      AnsPid ! dispatcher_map_registered,
      {noreply, State};
    false ->
      ?info("Registration of disp map for module ~p", [Module]),
      NewMapsList = register_dispatcher_map(Module, Map, Maps),
      NewState = State#cm_state{dispatcher_maps = NewMapsList},
      NewState2 = update_dispatchers_and_dns(NewState, false, true),
      AnsPid ! dispatcher_map_registered,
      save_state(),
      {noreply, NewState2}
  end;

handle_cast(get_state_from_db, State) ->
  case State#cm_state.state_loaded of
    true ->
      {noreply, State};
    false ->
      NewState2 = case State#cm_state.nodes =:= [] of
                    true ->
                      State;
                    false ->
                      get_state_from_db(State)
                  end,

      Pid = self(),
      erlang:send_after(1000, Pid, {timer, get_state_from_db}),

      {noreply, NewState2}
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

handle_cast(save_state, State) ->
  case State#cm_state.state_loaded of
    true ->
      try
        Ans = gen_server:call(?Dispatcher_Name, {dao_worker, 1, {save_state, [State#cm_state{dispatcher_maps = []}]}}, 500),
        case Ans of
          ok -> ?debug("Save state message sent");
          _ -> ?error("Save state error: ~p", [Ans])
        end
      catch
        E1:E2 ->
          ?error("Save state error: ~p:~p", [E1, E2])
      end;
    false ->
      ok
  end,
  {noreply, State};

handle_cast(check_cluster_state, State) ->
  NewState = check_cluster_state(State),
  {noreply, NewState};

handle_cast({node_down, Node}, State) ->
  ?error("Node down: ~p", [Node]),
  {NewState, WorkersFound} = node_down(Node, State),

  %% If workers were running on node that is down,
  %% upgrade state.
  NewState2 = case WorkersFound of
                true -> update_dispatchers_and_dns(NewState, true, true);
                false -> NewState
              end,

  save_state(),
  {noreply, NewState2};

handle_cast({set_monitoring, Flag}, State) ->
  ?info("Change of monitoring settings, new flag ~p", [Flag]),
  case Flag of
    on ->
      case State#cm_state.state_monitoring of
        off ->
          change_monitoring(State#cm_state.nodes, true);
        on -> ok
      end;
    off ->
      case State#cm_state.state_monitoring of
        on ->
          change_monitoring(State#cm_state.nodes, false);
        off -> ok
      end
  end,
  {noreply, State#cm_state{state_monitoring = Flag}};

handle_cast({worker_answer, cluster_state, Response}, State) ->
  NewState = case Response of
               {ok, SavedState} ->
                 ?debug("State read from DB: ~p", [SavedState]),
                 merge_state(State, SavedState);
               {error, {not_found, _}} ->
                 save_state(),
                 State#cm_state{state_loaded = true};
               Error ->
                 ?debug("State cannot be read from DB: ~p", [Error]), %% info logging level because state may not be present in db and it's not an error
                 State
             end,
  {noreply, NewState};

handle_cast({stop_worker, Node, Module}, State) ->
  {_Ans, New_State} = stop_worker(Node, Module, State),
  {noreply, New_State};

handle_cast(stop, State) ->
  {stop, normal, State};

%% TODO if cashes will be cleared more frequently, it should be done without ccm usage
handle_cast({clear_cache, Cache, ReturnPid}, State) ->
  ?debug("Cache clearing ~p", [{clear_cache, Cache, ReturnPid}]),
  ReturnPid ! {cache_cleared, Cache},
  New_State = clear_cache(State, Cache),
  {noreply, New_State};

handle_cast({synch_cache_clearing, Cache, ReturnPid}, State) ->
  ?debug("Cache clearing ~p", [{synch_cache_clearing, Cache, ReturnPid}]),
  New_State = clear_cache(State, Cache),
  ReturnPid ! {cache_cleared, Cache},
  {noreply, New_State};

%% this handler notify all logical_files_manager that event production of EventType should be enabled/disabled
handle_cast({notify_lfm, EventType, Enabled}, State) ->
  ?debug("Sending notification to all logical files managers ~p", [{notify_lfm, EventType, Enabled}]),
  NotifyFn = fun(Node) ->
    gen_server:cast({?Node_Manager_Name, Node}, {notify_lfm, EventType, Enabled})
  end,

  lists:foreach(NotifyFn, State#cm_state.nodes),
  {noreply, State};

handle_cast({update_user_write_enabled, UserDn, Enabled}, State) ->
  ?debug("Sending notification to update user's write permitions ~p", [{update_user_write_enabled, UserDn, Enabled}]),
  NotifyFn = fun(Node) ->
    gen_server:cast({?Node_Manager_Name, Node}, {update_user_write_enabled, UserDn, Enabled})
  end,

  lists:foreach(NotifyFn, State#cm_state.nodes),
  {noreply, State};

handle_cast({start_load_logging, Path}, State) ->
  ?info("Start load logging on nodes: ~p", State#cm_state.nodes),
  lists:map(fun(Node) ->
    gen_server:cast({?Node_Manager_Name, Node}, {start_load_logging, Path}) end, State#cm_state.nodes),
  {noreply, State};

handle_cast(stop_load_logging, State) ->
  ?info("Stop load logging on nodes: ~p", State#cm_state.nodes),
  lists:map(fun(Node) -> gen_server:cast({?Node_Manager_Name, Node}, stop_load_logging) end, State#cm_state.nodes),
  {noreply, State};

handle_cast(start_cluster_monitoring, State) ->
  ?debug("start_cluster_monitoring"),
  spawn(fun() -> create_cluster_stats_rrd() end),
  {noreply, State};

handle_cast(monitor_cluster, State) ->
  {ok, Period} = application:get_env(?APP_Name, cluster_monitoring_period),
  GetNodeStats = fun(Node) ->
    try gen_server:call({?Node_Manager_Name, Node}, {get_node_stats, Period}, 500) of
      Stats when is_list(Stats) -> {Node, Stats};
      _ -> {Node, undefined}
    catch
      _:Reason ->
        ?error("Can not get statistics of node ~s: ~p", [Node, Reason]),
        {Node, undefined}
    end
  end,
  NodesStats = lists:map(GetNodeStats, State#cm_state.nodes),
  spawn(fun() -> save_cluster_stats_to_rrd(NodesStats, State) end),
  erlang:send_after(1000 * Period, self(), {timer, monitor_cluster}),
  {noreply, State#cm_state{storage_stats = #storage_stats{}}};

handle_cast({update_storage_write_b, Bytes}, #cm_state{storage_stats = #storage_stats{write_bytes = WB} = Stats} = State) ->
  {noreply, State#cm_state{storage_stats = Stats#storage_stats{write_bytes = WB + Bytes}}};

handle_cast({update_storage_read_b, Bytes}, #cm_state{storage_stats = #storage_stats{read_bytes = RB} = Stats} = State) ->
  {noreply, State#cm_state{storage_stats = Stats#storage_stats{read_bytes = RB + Bytes}}};

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

handle_info({nodedown, Node}, State) ->
  case State#cm_state.state_monitoring of
    on ->
      gen_server:cast({global, ?CCM}, {node_down, Node});
    off -> ok
  end,
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
init_cluster(State, Repeat) ->
  ?debug("Checking if initialization is needed ~p ~p", [State, Repeat]),
  Nodes = State#cm_state.nodes,
  case length(Nodes) > 0 of
    true ->
      JobsAndArgs = ?MODULES_WITH_ARGS,
      PermanentModules = ?PERMANENT_MODULES,

      %% Every node is supposed to have a complete set of permament workes.
      {_PermamentWorkers, RequiredPermamentWorkers} = required_permanent_workers(PermanentModules, JobsAndArgs, State, Nodes),

      NewStatePermament2 = init_permament_nodes(RequiredPermamentWorkers, State),

      CreateRunningWorkersList = fun({_N, M, _Child}, Workers) ->
        [M | Workers]
      end,
      Workers = NewStatePermament2 #cm_state.workers,
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
                      ?info("Initialization of jobs ~p using nodes ~p", [Jobs, Nodes]),
                      NewState = case erlang:length(Nodes) >= erlang:length(Jobs) of
                                   true -> init_cluster_nodes_dominance(NewStatePermament2 , Nodes, Jobs, [], Args, []);
                                   false -> init_cluster_jobs_dominance(NewStatePermament2 , Jobs, Args, Nodes, [])
                                 end,

                      NewState2 = update_dispatchers_and_dns(NewState, true, true),
                      save_state(),
                      NewState2;
                    false ->
                      case length(RequiredPermamentWorkers) > 0 of
                        true -> update_dispatchers_and_dns(NewStatePermament2, true, true);
                        _ -> NewStatePermament2
                      end
                  end,

      case Repeat of
        true -> plan_next_cluster_state_check();
        _ -> ?debug("Single cluster initialization ~p", [NewState3])
      end,
      NewState3;
    false ->
      case Repeat of
        true ->
          Pid = self(),
          {ok, Interval} = application:get_env(?APP_Name, initialization_time),
          erlang:send_after(1000 * Interval, Pid, {timer, init_cluster});
        _ -> ok
      end,
      State
  end.



%% required_permanent_workers/4
%% ====================================================================
%% @doc Creates a list of permanent workers, which are going to be initialized.
required_permanent_workers(PermanentModules, JobsAndArgs, State, Nodes) ->
  PermamentWorkers = [{Worker, Module} || {Worker, Module, _Child} <- State#cm_state.workers,
                      lists:member(Module, PermanentModules)],

  RequiredPermamentWorkers = [{Node, Module, Args} || Node <- Nodes, Module <- PermanentModules,
                              {Module2, Args} <- JobsAndArgs,
                              not lists:member({Node, Module}, PermamentWorkers),
                              Module =:= Module2],

  {PermamentWorkers, RequiredPermamentWorkers}.



%% init_permament_nodes/2
%% ====================================================================
%% @doc Starts list of workers.
init_permament_nodes([], State) ->
  State;
init_permament_nodes([{Node, Job, Args}| InitList], State) ->
  {_Ans, NewState} = start_worker(Node, Job, Args, State),
  init_permament_nodes(InitList, NewState).

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

%% check_cluster_state/1
%% ====================================================================
%% @doc Checks cluster state and decides if any new component should
%% be started (currently running ones are overloaded) or stopped.
-spec check_cluster_state(State :: term()) -> NewState when
  NewState :: term().
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

                     MinV = case [NodeLoad || {_Node, NodeLoad, _ModulesLoads} <- Load, NodeLoad =/= error] of
                              [] -> 0;
                              NonEmptyMinList -> lists:min(NonEmptyMinList)
                            end,
                     MaxV = case [NodeLoad || {_Node, NodeLoad, _ModulesLoads} <- Load, NodeLoad =/= error] of
                              [] -> 0;
                              NonEmptyMaxList -> lists:max(NonEmptyMaxList)
                            end,

                     ?debug("Load ~p", [Load]),
                     case (MinV > 0) and (((MaxV >= 2 * MinV) and (MaxV >= 1.5 * AvgLoad)) or (MaxV >= 5 * MinV)) of
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
                                 ?info("Worker: ~s will be started at node: ~s", [MaxModule, MinNode]),
                                 {WorkerRuns, TmpState} = start_worker(MinNode, MaxModule, proplists:get_value(MaxModule, ?MODULES_WITH_ARGS, []), State),
                                 case WorkerRuns of
                                   ok ->
                                     save_state(),
                                     update_dispatchers_and_dns(TmpState, true, true);
                                   error -> TmpState
                                 end;
                               false ->
                                 ?info("Worker: ~s will be stopped at node: ~s", [MaxModule, MaxNode]),
                                 {WorkerStopped, TmpState2} = stop_worker(MaxNode, MaxModule, State),
                                 case WorkerStopped of
                                   ok ->
                                     save_state(),
                                     update_dispatchers_and_dns(TmpState2, true, true);
                                   _ -> TmpState2
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

  ?debug("Cluster state ok"),
  NewState2 = init_cluster(NewState), %% if any worker is down and some worker type has no running instances, new worker should be started anywhere
  NewState2#cm_state{cluster_check_num = CheckNum}.

%% plan_next_cluster_state_check/0
%% ====================================================================
%% @doc Decides when cluster state should be checked next time and sets
%% the timer (cluster_monitoring_period environment variable is used).
-spec plan_next_cluster_state_check() -> Result when
  Result :: {ok, TRef} | {error, Reason},
  TRef :: term(),
  Reason :: term().
%% ====================================================================
plan_next_cluster_state_check() ->
  {ok, Interval} = application:get_env(?APP_Name, cluster_monitoring_period),
  erlang:send_after(Interval * 1000, self(), {timer, check_cluster_state}).




%% add_module_lifecycle_listener/3
%% ====================================================================
%% @doc Adds new lifcycle listener to the list of registered listeners
-spec add_module_lifecycle_listener(Module :: list(), Listener :: term(), State :: term()) -> Result when
  Result :: term(),
  Module :: ModuleName,
  Module :: {module, ModuleName} | {module, ModuleName, Node} | {all_modules},
  ModuleName :: atom(),
  Node :: list().
%% ====================================================================
add_module_lifecycle_listener(Module, Listener, State) ->
  Listeners = State#cm_state.worker_lifecycle_listeners,
  ModuleListeners = [{Module2, Listeners2} || {Module2, Listeners2} <- Listeners, Module =:= Module2],
  RestListeners = [{Module2, Listeners2} || {Module2, Listeners2} <- Listeners, Module =/= Module2],

  NewModuleListeners = case ModuleListeners of
                         [{M, L}] ->
                           case lists:member(Listener, L) of
                             true -> {M, L};
                             false -> {M, [Listeners | L]}
                           end;
                         [] -> {Module, [Listener]}
                       end,

  State#cm_state{worker_lifecycle_listeners = [NewModuleListeners | RestListeners]}.


%% lifecycle_notification/5
%% ====================================================================
%% @doc Sends notification to modules that are registered for certain lifecycle acions
-spec lifecycle_notification(Node :: list(), Module :: atom(), Action :: atom(), Workers :: term(), State :: term()) -> ok.
%% ====================================================================
lifecycle_notification(Node, Module, Action, Workers, State) ->
  Listeners = State#cm_state.worker_lifecycle_listeners,
  ModuleListeners = [{Module2, Listeners2} || {Module2, Listeners2} <- Listeners, Module =:= Module2],
  case ModuleListeners of
    [{_M, L}] -> send_notifications(Node, Module, Action, Workers, L);
    _ -> ok
  end.

%% send_notifications/5
%% ====================================================================
%% @doc Sends notifications to modules that are registered for certain lifecycle acions
-spec send_notifications(Node :: list(), Module :: atom(), Action :: atom(), Workers :: term(), Listeners ::list()) -> ok.
%% ====================================================================
send_notifications(Node, Module, Action, Workers,  Listeners) ->
  ?debug("Notification ~p ~p ~p ~p ~p", [Node, Module, Listeners, Action, Workers]),
  [{ gen_server:cast({Module2, Node2}, {asynch, 1, {node_lifecycle_notification, Node, Module, Action, Pid}})} || {Node2, Module2, Pid} <- Workers ,
    lists:member({all_modules}, Listeners)
      or lists:member({module, Module2}, Listeners)
      or lists:member({module, Module2, Node2}, Listeners),
    ((Node =/= Node2) or (Module =/= Module2))
  ],
  ok.



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
    lifecycle_notification(Node, Module, ?ACTION_START_WORKER, Workers, State),
    {ok, State#cm_state{workers = [{Node, Module, ChildPid} | Workers]}}
  catch
    _:_ ->
      ?error("Error during start of worker: ~s at node: ~s", [Module, Node]),
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
                  ok ->
                    ?info("Worker: ~s stopped at node: ~s", [Module, Node]),
                    lifecycle_notification(Node, Module, ?ACTION_STOP_WORKER, NewWorkers, State),
                    ok;
                  {error, Error1} ->
                    ?error("Worker: ~s not stopped at node: ~s, error ~p", [Module, Node, {delete_error, Error1}]),
                    delete_error
                end;
              {error, Error2} ->
                ?error("Worker: ~s not stopped at node: ~s, error ~p", [Module, Node, {termination_error, Error2}]),
                termination_error
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
        {AddState, NewWorkers} = add_children(Node, Children, Workers, State),
        case AddState of
          ok ->
            WorkersFound = length(NewWorkers) > length(Workers),

            try
              Callbacks = gen_server:call({?Node_Manager_Name, Node}, get_fuses_list, 500),
              Changed = lists:foldl(fun(Fuse, TmpAns) ->
                case add_callback(Node, Fuse, State#cm_state.nodes, State#cm_state.callbacks_num) of
                  updated -> true;
                  _ -> TmpAns
                end
              end, false, Callbacks),

              {CallbacksNum, WorkersFound2} = case Changed of
                                                true -> {State#cm_state.callbacks_num + 1, true};
                                                false -> {State#cm_state.callbacks_num, WorkersFound}
                                              end,
              {ok, State#cm_state{workers = NewWorkers, callbacks_num = CallbacksNum}, WorkersFound2}
            catch
              _:_ ->
                ?error("Cannot get fuses of node: ~s", [Node]),
                {error, State, false}
            end;
          _ -> {error, State, false}
        end;
      pang ->
        ?error("Pang during check of node: ~s", [Node]),
        {error, State, false}
    end
  catch
    E1:E2 ->
      ?error("Error: ~p:~p during check of node: ~s", [E1, E2, Node]),
      {error, State, false}
  end.

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

      MapState = try
        ok = gen_server:call(ChildPid, dispatcher_map_unregistered, 500)
                 catch
                   _:_ ->
          ?error("Error during contact with worker ~p found at node ~s", [Id, Node]),
                     error
                 end,

      case MapState of
        ok ->
          {MapState2, Ans} = add_children(Node, Children, Workers, State),
          lifecycle_notification(Node, Id, ?ACTION_START_WORKER, Workers, State),
          {MapState2, [{Node, Id, ChildPid} | Ans]};
        _ -> {error, Workers}
      end
  end.

%% node_down/2
%% ====================================================================
%% @doc Clears information about workers on node that is down.
-spec node_down(Node :: atom(), State :: term()) -> {NewState, WorkersFound} when
  NewState :: term(),
  WorkersFound :: boolean().
%% ====================================================================
node_down(Node, State) ->
  CreateNewWorkersList = fun({N, M, Child}, {Workers, Found}) ->
    case N of
      Node ->
        lifecycle_notification(N, M, ?ACTION_STOP_WORKER, State#cm_state.workers, State),
        {Workers, true};
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

  {CallbacksNum, WorkersFound2} = case delete_all_callbacks(Node, State#cm_state.nodes, State#cm_state.callbacks_num) of
                                    true -> {State#cm_state.callbacks_num + 1, true};
                                    false -> {State#cm_state.callbacks_num, WorkersFound}
                                  end,

  case State#cm_state.state_loaded of
    true ->
      Pid = self(),
      erlang:send_after(50, Pid, {timer, init_cluster_once});
    _ -> ok
  end,
  {State#cm_state{workers = NewWorkers, nodes = NewNodes, callbacks_num = CallbacksNum}, WorkersFound2}.

%% start_central_logger/1
%% ====================================================================
%% @doc This function starts the central_logger before other modules.
-spec start_central_logger(State :: term()) -> NewState when
  NewState :: term().
%% ====================================================================
start_central_logger(State) ->
  CreateRunningWorkersList = fun({_N, M, _Child}, Workers) ->
    [M | Workers]
  end,
  Workers = State#cm_state.workers,
  RunningWorkers = lists:foldl(CreateRunningWorkersList, [], Workers),
  case lists:member(central_logger, RunningWorkers) of
    true -> State;
    false ->
      [LoggerNode | _] = State#cm_state.nodes,
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
  CreateRunningWorkersList = fun({_N, M, _Child}, Workers) ->
    [M | Workers]
  end,
  Workers = State#cm_state.workers,
  RunningWorkers = lists:foldl(CreateRunningWorkersList, [], Workers),
  case lists:member(dao_worker, RunningWorkers) of
    true ->
      FindDAO = fun({N, M, _Child}, TmpAns) ->
        case M of
          dao_worker -> N;
          _ -> TmpAns
        end
      end,
      DaoNode = lists:foldl(FindDAO, [], Workers),
      gen_server:cast({dao_worker, DaoNode}, {synch, 1, {get_state, []}, cluster_state, {gen_serv, {global, ?CCM}}}),
      State;
    false ->
      [DaoNode | _] = State#cm_state.nodes,
      {Ans, NewState} = start_worker(DaoNode, dao_worker, [], State),
      case Ans of
        ok ->
          gen_server:cast({dao_worker, DaoNode}, {synch, 1, {get_state, []}, cluster_state, {gen_serv, {global, ?CCM}}}),
          update_dispatchers_and_dns(NewState, true, true);
        error -> NewState
      end
  end.

%% save_state/0
%% ====================================================================
%% @doc This function saves cluster state in DB.
-spec save_state() -> ok.
%% ====================================================================
save_state() ->
  Pid = self(),
  erlang:send_after(100, Pid, {timer, save_state}).

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
  CallbacksNum = erlang:max(State#cm_state.callbacks_num, SavedState#cm_state.callbacks_num) + 1,
  State1 = State#cm_state{state_num = StateNum, callbacks_num = CallbacksNum, state_loaded = true},

  NewNodes = lists:filter(fun(N) -> not lists:member(N, State1#cm_state.nodes) end, SavedState#cm_state.nodes),

  CreateNewState = fun(Node, {TmpState, TmpWorkersFound}) ->
    case State#cm_state.state_monitoring of
      on ->
        erlang:monitor_node(Node, true);
      off -> ok
    end,
    {Ans, NewState, WorkersFound} = check_node(Node, TmpState),
    case Ans of
      ok ->
        case State#cm_state.state_monitoring of
          on ->
            erlang:monitor_node(Node, true);
          off -> ok
        end,
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

  save_state(),
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
      WorkersLoad = calculate_worker_load(State#cm_state.workers),
      Load = calculate_load(NodesLoad, WorkersLoad),
      ?debug("updating dns, load info: ~p", [Load]),
      update_dns_state(State#cm_state.workers, Load, AvgLoad);
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
    {LastLoadInfo, Load} = gen_server:call(WorkerPlugin, getLoadInfo, 500),
    TimeDiff = timer:now_diff(BeforeCall, LastLoadInfo),
    Load / TimeDiff
  catch
    _:_ ->
      ?error("Can not get status of worker plugin"),
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
      ?error("get_workers_versions error: ~p:~p", [E1, E2]),
      get_workers_versions(Workers, [{Node, Module, can_not_connect_with_worker} | Versions])
  end.

%% calculate_node_load/2
%% ====================================================================
%% @doc Calculates load of all nodes in cluster
-spec calculate_node_load(Nodes :: list(), Period :: atom()) -> Result when
  Result :: list().
%% ====================================================================
calculate_node_load(Nodes, Period) ->
  GetNodeStats = fun(Node) ->
    try gen_server:call({?Node_Manager_Name, Node}, {get_node_stats, Period}, 500) of
      Stats when is_list(Stats) -> {Node, Stats};
      _ -> {Node, undefined}
    catch
      _:Reason ->
        ?error("Can not get statistics of node ~s: ~p", [Node, Reason]),
        {Node, undefined}
    end
  end,
  NodesStats = lists:map(GetNodeStats, Nodes),
  map_node_stats_to_load(NodesStats).

%% map_node_stats_to_load/1
%% ====================================================================
%% @doc Maps node statistical data to one value for each node
-spec map_node_stats_to_load(NodesStats :: list()) -> Result when
  Result :: {[{Node :: term(), Load}], AvgLoad},
  Load :: float() | undefined,
  AvgLoad :: float() | undefined.
%% ====================================================================
map_node_stats_to_load(NodesStats) ->
  %% Unify diffrent nodes stats to common pattern
  UnifiedNodesStats = lists:map(fun
    ({Node, undefined}) -> {Node, undefined};
    ({Node, NodeStats}) ->
      {Node, lists:zip(["cpu", "mem", "net_rx_b", "net_tx_b", "net_rx_pps", "net_tx_pps", "ports_rx_b", "ports_tx_b"],
        merge_nodes_stats([{Node, NodeStats}]))}
  end, NodesStats),

  %% Create list of maximum values for each statistical data
  MaxStats = lists:foldl(fun
    ({_, undefined}, Maxs) -> Maxs;
    ({_, Stats}, undefined) -> Stats;
    ({_, Stats}, Maxs) ->
      lists:zipwith(fun({Name, X}, {Name, Y}) -> {Name, max(X, Y)} end, Stats, Maxs)
  end, undefined, UnifiedNodesStats),

  %% Define weigth of statistical data (default weight equals 1)
  StatsWeight = dict:from_list(?STATS_WEIGHTS),
  GetStatWeight = fun(Name) ->
    case dict:find(Name, StatsWeight) of
      {ok, Value} -> Value;
      _ -> 1
    end
  end,

  %% Convert list of statistical data base on maximal values and weigth of each entry
  StatsToLoadMap = fun(Stats) ->
    ScaledStats = lists:zipwith(fun
      ({Name, _}, {Name, 0.0}) -> 0.0;
      ({Name, Stat}, {Name, MaxStat}) -> GetStatWeight(Name) * Stat / MaxStat
    end, Stats, MaxStats),
    lists:foldl(fun(ScaledStat, Acc) -> ScaledStat + Acc end, 0, ScaledStats)
  end,

  %% Get load for each node
  NodesLoad = lists:map(fun
    ({Node, undefined}) -> {Node, error};
    ({Node, Stats}) -> {Node, StatsToLoadMap(Stats)}
  end, UnifiedNodesStats),

  %% Calculate average load
  {SumLoad, Counter} = lists:foldl(fun
    ({_, error}, {SumLoad, Counter}) -> {SumLoad, Counter};
    ({_, Load}, {SumLoad, Counter}) -> {Load + SumLoad, Counter + 1}
  end, {0, 0}, NodesLoad),

  case Counter of
    0 -> {NodesLoad, 0};
    _ -> {NodesLoad, SumLoad / Counter}
  end.

%% calculate_worker_load/1
%% ====================================================================
%% @doc Calculates load of all workers in cluster
-spec calculate_worker_load(Workers :: list()) -> Result when
  Result :: list().
%% ====================================================================
calculate_worker_load(Workers) ->
  WorkersLoad = [{Node, {Module, check_load(Pid)}} || {Node, Module, Pid} <- Workers,
                 not lists:member(Module, ?PERMANENT_MODULES),
                 not lists:member(Module, ?SINGLETON_MODULES)],

  MergeByFirstElement = fun(List) -> lists:reverse(lists:foldl(fun({Key, Value}, []) -> [{Key, [Value]}];
    ({Key, Value}, [{Key, AccValues} | Tail]) -> [{Key, [Value | AccValues]} | Tail];
    ({Key, Value}, Acc) -> [{Key, [Value]} | Acc]
  end, [], lists:keysort(1, List)))
  end,

  MergedByNode = MergeByFirstElement(WorkersLoad),

  EvaluateLoad = fun({Node, Modules}) ->
    GetLoadsSum = fun({_Module, Value}, TmpAns) ->
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
  Merge = fun({Node, NLoad}) ->
    {Node, NLoad, proplists:get_value(Node, WorkersLoad, [])}
  end,
  lists:map(Merge, NodesLoad).

%% add_callback/4
%% ====================================================================
%% @doc Registers callback
-spec add_callback(Node :: term(), Fuse :: string(), Nodes :: list(), CallbacksNum :: integer()) -> Result when
  Result :: ok | updated.
%% ====================================================================
add_callback(Node, Fuse, Nodes, CallbacksNum) ->
  OldCallbacks = ets:lookup(?CALLBACKS_TABLE, Fuse),
  case OldCallbacks of
    [{Fuse, OldCallbacksList}] ->
      case lists:member(Node, OldCallbacksList) of
        true -> ok;
        false ->
          ets:insert(?CALLBACKS_TABLE, {Fuse, [Node | OldCallbacksList]}),
          update_dispatcher_callback(addCallback, Nodes, Fuse, Node, CallbacksNum + 1),
          updated
      end;
    _ ->
      ets:insert(?CALLBACKS_TABLE, {Fuse, [Node]}),
      update_dispatcher_callback(addCallback, Nodes, Fuse, Node, CallbacksNum + 1),
      updated
  end.

%% delete_callback/5
%% ====================================================================
%% @doc Deletes callback
-spec delete_callback(Node :: term(), Fuse :: string(), Nodes :: list(), CallbacksNum :: integer(), ClearETS :: boolean()) -> Result when
  Result :: updated | not_exists.
%% ====================================================================
delete_callback(Node, Fuse, Nodes, CallbacksNum, ClearETS) ->
  OldCallbacks = ets:lookup(?CALLBACKS_TABLE, Fuse),
  case OldCallbacks of
    [{Fuse, OldCallbacksList}] ->
      case lists:member(Node, OldCallbacksList) of
        true ->
          case length(OldCallbacksList) of
            1 ->
              case ClearETS of
                true -> ets:delete(?CALLBACKS_TABLE, Fuse);
                _ -> ok
              end,
              update_dispatcher_callback(delete_callback, Nodes, Fuse, Node, CallbacksNum + 1),
              ?debug("delete_callback with params: ~p, ans: deleted", [{Node, Fuse, Nodes, CallbacksNum, ClearETS}]),
              deleted;
            _ ->
              ets:insert(?CALLBACKS_TABLE, {Fuse, lists:delete(Node, OldCallbacksList)}),
              update_dispatcher_callback(delete_callback, Nodes, Fuse, Node, CallbacksNum + 1),
              ?debug("delete_callback with params: ~p, ans: updated", [{Node, Fuse, Nodes, CallbacksNum, ClearETS}]),
              updated
          end;
        false ->
          ?debug("delete_callback with params: ~p, ans: not_exists", [{Node, Fuse, Nodes, CallbacksNum, ClearETS}]),
          not_exists
      end;
    _ ->
      ?debug("delete_callback with params: ~p, ans: not_exists", [{Node, Fuse, Nodes, CallbacksNum, ClearETS}]),
      not_exists
  end.

%% delete_all_callbacks/3
%% ====================================================================
%% @doc Deletes all callbacks at node
-spec delete_all_callbacks(Node :: term(), Nodes :: list(), CallbacksNum :: integer()) -> Result when
  Result :: boolean().
%% ====================================================================
delete_all_callbacks(Node, Nodes, CallbacksNum) ->
  delete_all_callbacks(Node, ets:first(?CALLBACKS_TABLE), false, Nodes, CallbacksNum, []).

%% delete_all_callbacks/6
%% ====================================================================
%% @doc Deletes all callbacks at node (helper function)
-spec delete_all_callbacks(Node :: term(), CurrentElement :: term(), Updated :: boolean(), Nodes :: list(), CallbacksNum :: integer(), ToBeDeleted :: list()) -> Result when
  Result :: boolean().
%% ====================================================================
delete_all_callbacks(_Node, '$end_of_table', Updated, _Nodes, _CallbacksNum, ToBeDeleted) ->
  UpdateETS = fun(Fuse) ->
    ets:delete(?CALLBACKS_TABLE, Fuse)
  end,
  lists:foreach(UpdateETS, ToBeDeleted),
  Updated;
delete_all_callbacks(Node, CurrentElement, Updated, Nodes, CallbacksNum, ToBeDeleted) ->
  case delete_callback(Node, CurrentElement, Nodes, CallbacksNum, false) of
    updated ->
      delete_all_callbacks(Node, ets:next(?CALLBACKS_TABLE, CurrentElement), true, Nodes, CallbacksNum, ToBeDeleted);
    deleted ->
      delete_all_callbacks(Node, ets:next(?CALLBACKS_TABLE, CurrentElement), true, Nodes, CallbacksNum, [CurrentElement | ToBeDeleted]);
    _ ->
      delete_all_callbacks(Node, ets:next(?CALLBACKS_TABLE, CurrentElement), Updated, Nodes, CallbacksNum, ToBeDeleted)
  end.

%% update_dispatcher_callback/5
%% ====================================================================
%% @doc Updates info about callbacks in all dispatchers
-spec update_dispatcher_callback(Action :: atom(), Nodes :: list(), FuseId :: string(), Node :: term(), CallbacksNum :: integer()) -> ok.
%% ====================================================================
update_dispatcher_callback(Action, Nodes, FuseId, Node, CallbacksNum) ->
  UpdateNode = fun(UpdatedNode) ->
    gen_server:cast({?Dispatcher_Name, UpdatedNode}, {Action, FuseId, Node, CallbacksNum})
  end,
  lists:foreach(UpdateNode, Nodes),
  gen_server:cast(?Dispatcher_Name, {Action, FuseId, Node, CallbacksNum}).

%% get_callbacks/0
%% ====================================================================
%% @doc Gets information about all callbacks
-spec get_callbacks() -> Result when
  Result :: list().
%% ====================================================================
get_callbacks() ->
  get_callbacks(ets:first(?CALLBACKS_TABLE)).

%% get_callbacks/1
%% ====================================================================
%% @doc Gets information about all callbacks (helper function)
-spec get_callbacks(Fuse :: string()) -> Result when
  Result :: list().
%% ====================================================================
get_callbacks('$end_of_table') ->
  [];
get_callbacks(Fuse) ->
  [Value] = ets:lookup(?CALLBACKS_TABLE, Fuse),
  [Value | get_callbacks(ets:next(?CALLBACKS_TABLE, Fuse))].

%% register_dispatcher_map/1
%% ====================================================================
%% @doc Registers information about map used by dispatcher
-spec register_dispatcher_map(Module :: atom(), Map :: term(), MapsList :: list()) -> Result when
  Result :: list().
%% ====================================================================
register_dispatcher_map(Module, Map, MapsList) ->
  ?debug("dispatcher map saved: ~p", [{Module, Map, MapsList}]),
  [{Module, Map} | proplists:delete(Module, MapsList)].

%% clear_cache/2
%% ====================================================================
%% @doc Clears chosen caches on all nodes
-spec clear_cache(State :: term(), Cache :: term()) -> NewState when
  NewState :: term().
%% ====================================================================
clear_cache(State, Cache) ->
  Clear = fun(Node, {TmpState, TmpWorkersFound}) ->
    try
      ok = gen_server:call({?Node_Manager_Name, Node}, {clear_cache, Cache}, 500),
      {TmpState, TmpWorkersFound}
    catch
      E1:E2 ->
        ?error("Can not clear cache ~p of node: ~p, error: ~p:~p", [Cache, Node, E1, E2]),
        {NewState, WorkersFound} = node_down(Node, State),
        {NewState, TmpWorkersFound or WorkersFound}
    end
  end,

  {State2, WF} = lists:foldl(Clear, {State, false}, State#cm_state.nodes),

  %% If workers were running on node that is down,
  %% upgrade state.
  ?debug("caches cleared witch workers found param: ~p", [WF]),
  State3 = case WF of
             true -> update_dispatchers_and_dns(State2, true, true);
             false -> State2
           end,

  save_state(),
  State3.

%% create_cluster_stats_rrd/1
%% ====================================================================
%% @doc Creates cluster stats Round Robin Database
-spec create_cluster_stats_rrd() -> Result when
  Result :: ok | {error, Error :: term()}.
%% ====================================================================
create_cluster_stats_rrd() ->
  {ok, Timeout} = application:get_env(?APP_Name, rrd_timeout),
  {ok, Period} = application:get_env(?APP_Name, cluster_monitoring_period),
  {ok, Steps} = application:get_env(?APP_Name, rrd_steps),
  {ok, RRDSize} = application:get_env(?APP_Name, rrd_size),
  Heartbeat = 2 * Period,
  RRASize = round(RRDSize / Period),
  BinaryPeriod = integer_to_binary(Period),
  BinaryHeartbeat = integer_to_binary(Heartbeat),
  RRASizeBinary = integer_to_binary(RRASize),
  Options = <<"--step ", BinaryPeriod/binary>>,

  DSs = [
    <<"DS:cpu:GAUGE:", BinaryHeartbeat/binary, ":0:100">>,
    <<"DS:mem:GAUGE:", BinaryHeartbeat/binary, ":0:100">>,
    <<"DS:net_rx_b:GAUGE:", BinaryHeartbeat/binary, ":0:U">>,
    <<"DS:net_tx_b:GAUGE:", BinaryHeartbeat/binary, ":0:U">>,
    <<"DS:net_rx_pps:GAUGE:", BinaryHeartbeat/binary, ":0:U">>,
    <<"DS:net_tx_pps:GAUGE:", BinaryHeartbeat/binary, ":0:U">>,
    <<"DS:ports_rx_b:GAUGE:", BinaryHeartbeat/binary, ":0:U">>,
    <<"DS:ports_tx_b:GAUGE:", BinaryHeartbeat/binary, ":0:U">>,
    <<"DS:storage_read_b:GAUGE:", BinaryHeartbeat/binary, ":0:U">>,
    <<"DS:storage_write_b:GAUGE:", BinaryHeartbeat/binary, ":0:U">>
  ],
  RRAs = lists:map(fun(Step) -> BinaryStep = integer_to_binary(Step),
    <<"RRA:AVERAGE:0.5:", BinaryStep/binary, ":", RRASizeBinary/binary>> end, Steps),

  case gen_server:call(?RrdErlang_Name, {create, ?Cluster_Stats_RRD_Name, Options, DSs, RRAs}, Timeout) of
    {error, Error} ->
      ?error("Can not create cluster stats RRD: ~p", [Error]),
      {error, Error};
    _ ->
      gen_server:cast({global, ?CCM}, monitor_cluster),
      ?debug("Cluster RRD created"),
      ok
  end.

%% save_cluster_stats_to_rrd/2
%% ====================================================================
%% @doc Saves cluster stats to Round Robin Database
-spec save_cluster_stats_to_rrd(NodesStats :: term(), State :: term()) -> Result when
  Result :: ok | {error, Error :: term()}.
%% ====================================================================
save_cluster_stats_to_rrd(NodesStats, #cm_state{storage_stats = StorageStats}) ->
  {ok, Timeout} = application:get_env(?APP_Name, rrd_timeout),
  {MegaSecs, Secs, _} = erlang:now(),
  Timestamp = integer_to_binary(MegaSecs * 1000000 + Secs),
  Values = merge_nodes_stats(NodesStats) ++ get_storage_stats(StorageStats),
  case gen_server:call(?RrdErlang_Name, {update, ?Cluster_Stats_RRD_Name, <<>>, Values, Timestamp}, Timeout) of
    {error, Error} ->
      ?error("Can not save node stats to RRD: ~p", [Error]),
      {error, Error};
    _ -> ok
  end.

%% merge_nodes_stats/1
%% ====================================================================
%% @doc Checks storage usage
-spec merge_nodes_stats(NodesStats :: term()) -> Result when
  Result :: [{Name, Value}],
  Name :: string(),
  Value :: non_neg_integer().
%% ====================================================================
merge_nodes_stats(NodesStats) ->
  merge_nodes_stats(NodesStats, #cluster_stats{}).

%% merge_nodes_stats/2
%% ====================================================================
%% @doc Checks storage usage
-spec merge_nodes_stats(NodesStats :: term(), SumStats :: term()) -> Result when
  Result :: [{Name, Value}],
  Name :: string(),
  Value :: non_neg_integer().
%% ====================================================================
merge_nodes_stats([], SumStats) ->
  AvgFun = fun
    ({_, 0}) -> 0.0;
    ({Sum, Counter}) -> Sum / Counter
  end,
  [
    AvgFun(SumStats#cluster_stats.cpu), AvgFun(SumStats#cluster_stats.memory),
    AvgFun(SumStats#cluster_stats.net_rx_b), AvgFun(SumStats#cluster_stats.net_tx_b),
    AvgFun(SumStats#cluster_stats.net_rx_pps), AvgFun(SumStats#cluster_stats.net_tx_pps),
    AvgFun(SumStats#cluster_stats.ports_rx_b), AvgFun(SumStats#cluster_stats.ports_tx_b)
  ];
merge_nodes_stats([{_, undefined} | NodeStats], SumStats) ->
  merge_nodes_stats(NodeStats, SumStats);
merge_nodes_stats([{_, Stats} | NodeStats], SumStats) ->
  merge_nodes_stats(
    NodeStats,
    lists:foldl(fun
      ({"cpu", Value}, #cluster_stats{cpu = {V, C}} = S) ->
        S#cluster_stats{cpu = {V + Value, C + 1}};
      ({"mem", Value}, #cluster_stats{memory = {V, C}} = S) ->
        S#cluster_stats{memory = {V + Value, C + 1}};
      ({"net_rx_b_" ++ _, Value}, #cluster_stats{net_rx_b = {V, C}} = S) ->
        S#cluster_stats{net_rx_b = {V + Value, C + 1}};
      ({"net_tx_b_" ++ _, Value}, #cluster_stats{net_tx_b = {V, C}} = S) ->
        S#cluster_stats{net_tx_b = {V + Value, C + 1}};
      ({"net_rx_pps_" ++ _, Value}, #cluster_stats{net_rx_pps = {V, C}} = S) ->
        S#cluster_stats{net_rx_pps = {V + Value, C + 1}};
      ({"net_tx_pps_" ++ _, Value}, #cluster_stats{net_tx_pps = {V, C}} = S) ->
        S#cluster_stats{net_tx_pps = {V + Value, C + 1}};
      ({"ports_rx_b", Value}, #cluster_stats{ports_rx_b = {V, C}} = S) ->
        S#cluster_stats{ports_rx_b = {V + Value, C + 1}};
      ({"ports_tx_b", Value}, #cluster_stats{ports_tx_b = {V, C}} = S) ->
        S#cluster_stats{ports_tx_b = {V + Value, C + 1}};
      (_, S) -> S
    end, SumStats, Stats)
  ).

%% get_storage_stats/0
%% ====================================================================
%% @doc Checks storage usage
-spec get_storage_stats(StorageStats :: term()) -> Result when
  Result :: [{Name, Value}],
  Name :: string(),
  Value :: non_neg_integer().
%% ====================================================================
get_storage_stats(#storage_stats{read_bytes = RB, write_bytes = WB}) ->
  [RB, WB].

%% get_cluster_stats/1
%% ====================================================================
%% @doc Get statistics about cluster load
-spec get_cluster_stats(TimeWindow :: short | medium | long | integer()) -> Result when
  Result :: [{Name :: atom(), Value :: float()}] | {error, term()}.
%% ====================================================================
get_cluster_stats(TimeWindow) ->
  {ok, Interval} = case TimeWindow of
                     short -> application:get_env(?APP_Name, short_monitoring_time_window);
                     medium -> application:get_env(?APP_Name, medium_monitoring_time_window);
                     long -> application:get_env(?APP_Name, long_monitoring_time_window);
                     _ -> {ok, TimeWindow}
                   end,
  {MegaSecs, Secs, _} = erlang:now(),
  EndTime = MegaSecs * 1000000 + Secs,
  StartTime = EndTime - Interval,
  get_cluster_stats(StartTime, EndTime).

%% get_cluster_stats/2
%% ====================================================================
%% @doc Get statistics about cluster load
-spec get_cluster_stats(StartTime :: integer(), EndTime :: integer()) -> Result when
  Result :: [{Name :: string(), Value :: float()}] | {error, term()}.
%% ====================================================================
get_cluster_stats(StartTime, EndTime) ->
  {ok, Timeout} = application:get_env(?APP_Name, rrd_timeout),
  BinaryEndTime = integer_to_binary(EndTime),
  BinaryStartTime = integer_to_binary(StartTime),
  Options = <<"--start ", BinaryStartTime/binary, " --end ", BinaryEndTime/binary>>,
  case gen_server:call(?RrdErlang_Name, {fetch, ?Cluster_Stats_RRD_Name, Options, <<"AVERAGE">>}, Timeout) of
    {ok, {Header, Data}} ->
      HeaderList = lists:map(fun(Elem) -> binary_to_list(Elem) end, Header),
      HeaderLen = length(Header),
      Values = lists:foldl(fun({_, Values}, Acc) ->
        lists:zipwith(fun
          (nan, {Y, C}) -> {Y, C};
          (X, {Y, C}) -> {X + Y, C + 1}
        end, Values, Acc)
      end, lists:duplicate(HeaderLen, {0, 0}), Data),

      AvgValues = lists:map(fun
        ({_, 0}) -> 0.0;
        ({Value, Counter}) -> Value / Counter
      end, Values),

      lists:zip(HeaderList, AvgValues);
    Other ->
      Other
  end.

%% get_cluster_stats/3
%% ====================================================================
%% @doc Fetch specified columns from cluster statistics Round Robin Database.
-spec get_cluster_stats(StartTime :: integer(), EndTime :: integer(), Columns) -> Result when
  Columns :: all | {name, [binary()]} | {starts_with, [binary()]} | {index, [integer()]},
  Result :: {ok, {Header, Body}} | {error, Error :: term()},
  Header :: [ColumnNames :: binary()],
  Body :: [Row],
  Row :: [{Timestamp, Values}],
  Timestamp :: integer(),
  Values :: [integer() | float()].
%% ====================================================================
get_cluster_stats(StartTime, EndTime, Columns) ->
  {ok, Timeout} = application:get_env(?APP_Name, rrd_timeout),
  BinaryStartTime = integer_to_binary(StartTime),
  BinaryEndTime = integer_to_binary(EndTime),
  Options = <<"--start ", BinaryStartTime/binary, " --end ", BinaryEndTime/binary>>,
  gen_server:call(?RrdErlang_Name, {fetch, ?Cluster_Stats_RRD_Name, Options, <<"AVERAGE">>, Columns}, Timeout).
