%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module is a gen_server that coordinates the 
%% life cycle of node. It starts/stops appropriate services (according
%% to node type) and communicates with ccm (if node works as worker).
%%
%% Node can be ccm or worker. However, worker_hosts can be also
%% started at ccm nodes.
%% @end
%% ===================================================================

-module(node_manager).
-behaviour(gen_server).
-include("registered_names.hrl").
-include("records.hrl").
-include("logging.hrl").
-include("supervision_macros.hrl").

%% Dispatcher cowboy listener ID
-define(DISPATCHER_LISTENER_REF, dispatcher_listener).

%% Path (relative to domain) on which cowboy expects client's requests
-define(VEILCLIENT_URI_PATH, "/veilclient").

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/1, stop/0]).
-export([check_vsn/0]).
-export([start_load_logging_loop/2, load_logging_loop/3]).

%% ====================================================================
%% Test API
%% ====================================================================
%% TODO zmierzyć czy bardziej się opłaca przechowywać dane o callbackach
%% jako stan (jak teraz) czy jako ets i ewentualnie przejść na ets
-ifdef(TEST).
-export([get_callback/2, addCallback/3, delete_callback/3]).
-endif.

%% ====================================================================
%% gen_server callbacks
%% ====================================================================
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================

%% start_link/1
%% ====================================================================
%% @doc Starts the server
-spec start_link(Type) -> Result when
  Type :: test_worker | worker | ccm | ccm_test,
  Result ::  {ok,Pid}
  | ignore
  | {error,Error},
  Pid :: pid(),
  Error :: {already_started,Pid} | term().
%% ====================================================================

start_link(Type) ->
  gen_server:start_link({local, ?Node_Manager_Name}, ?MODULE, [Type], []).

%% stop/0
%% ====================================================================
%% @doc Stops the server
-spec stop() -> ok.
%% ====================================================================

stop() ->
  gen_server:cast(?Node_Manager_Name, stop).

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
init([Type]) when Type =:= worker ; Type =:= ccm ; Type =:= ccm_test ->
  case Type =/= ccm of
    true ->
      try
        cowboy:stop_listener(?DISPATCHER_LISTENER_REF),
        ok
      catch
        _:_ -> ok
      end,

      {ok, Port} = application:get_env(?APP_Name, dispatcher_port),
      {ok, DispatcherPoolSize} = application:get_env(?APP_Name, dispatcher_pool_size),
      {ok, CertFile} = application:get_env(?APP_Name, ssl_cert_path),

      Dispatch = cowboy_router:compile([{'_', [{?VEILCLIENT_URI_PATH, ws_handler, []}]}]),

      {ok, _} = cowboy:start_https(?DISPATCHER_LISTENER_REF, DispatcherPoolSize,
        [
          {port, Port},
          {certfile, atom_to_list(CertFile)},
          {cacerts, gsi_handler:strip_self_signed_ca(gsi_handler:get_ca_certs())},
          {keyfile, atom_to_list(CertFile)},
          {password, ""},
          {verify, verify_peer}, {verify_fun, {fun gsi_handler:verify_callback/3, []}}
        ],
        [
          {env, [{dispatch, Dispatch}]}
        ]);
    false -> ok
  end,

  %% TODO: replace with permanent cache
  ets:new(?LFM_EVENT_PRODUCTION_ENABLED_ETS, [set, named_table, public]),
  ets:new(?WRITE_DISABLED_USERS, [set, named_table, public]),
  ets:new(?ACK_HANDLERS, [set, named_table, public]),

  process_flag(trap_exit, true),
  erlang:send_after(10, self(), {timer, do_heart_beat}),
  erlang:send_after(100, self(), {timer, monitor_mem_net}),

  {ok, Period} = application:get_env(veil_cluster_node, node_monitoring_period),
  LoadMemorySize = round(15 * 60 / Period + 1),
  {ok, #node_state{node_type = Type, ccm_con_status = not_connected, memory_and_network_info = {[], [], 0, LoadMemorySize}}};

init([Type]) when Type =:= test_worker ->
  process_flag(trap_exit, true),
  erlang:send_after(10, self(), {timer, do_heart_beat}),
  erlang:send_after(100, self(), {timer, monitor_mem_net}),

  {ok, Period} = application:get_env(veil_cluster_node, node_monitoring_period),
  LoadMemorySize = round(15 * 60 / Period + 1),
  {ok, #node_state{node_type = worker, ccm_con_status = not_connected, memory_and_network_info = {[], [], 0, LoadMemorySize}}};

init([_Type]) ->
  {stop, wrong_type}.

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
handle_call(getNodeType, _From, State) ->
  Reply = State#node_state.node_type,
  {reply, Reply, State};

handle_call(getNode, _From, State) ->
  Reply = node(),
  {reply, Reply, State};

handle_call(get_ccm_connection_status, _From, State) ->
  {reply, State#node_state.ccm_con_status, State};

handle_call({get_node_stats, Window}, _From, State) ->
  Reply = get_node_stats(Window, State#node_state.memory_and_network_info),
  {reply, Reply, State};

handle_call(get_fuses_list, _From, State) ->
  {reply, get_fuses_list(State), State};

handle_call({get_all_callbacks, Fuse}, _From, State) ->
  {reply, get_all_callbacks(Fuse, State), State};

handle_call({addCallback, FuseId, Pid}, _From, State) ->
  NewState = addCallback(State, FuseId, Pid),
  {reply, ok, NewState};

handle_call({delete_callback, FuseId, Pid}, _From, State) ->
  {NewState, DeleteAns} = delete_callback(State, FuseId, Pid),
  {reply, DeleteAns, NewState};

handle_call({get_callback, FuseId}, _From, State) ->
  {Callback, NewState} = get_callback(State, FuseId),
  {reply, Callback, NewState};

handle_call(get_callback_and_state_num, _From, State) ->
  Reply = {State#node_state.callbacks_num, State#node_state.state_num},
  {reply, Reply, State};

handle_call({clear_cache, Cache}, _From, State) ->
  Ans = clear_cache(Cache, State#node_state.simple_caches),
  {reply, Ans, State};

handle_call(check, _From, State) ->
  {reply, ok, State};

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
handle_cast(do_heart_beat, State) ->
  {noreply, heart_beat(State#node_state.ccm_con_status, State)};

handle_cast({heart_beat_ok, StateNum, CallbacksNum}, State) ->
  {noreply, heart_beat_response(StateNum, CallbacksNum, State)};

handle_cast(reset_ccm_connection, State) ->
  {noreply, heart_beat(not_connected, State)};

handle_cast({dispatcher_updated, DispState, DispCallbacksNum}, State) ->
  NewState = State#node_state{ dispatcher_state = DispState, callbacks_state = DispCallbacksNum},
  {noreply, NewState};

handle_cast(stop, State) ->
  {stop, normal, State};

handle_cast(monitor_mem_net, State) ->
  Info = get_memory_and_net_info(),
  NewInfo = save_progress(Info, State#node_state.memory_and_network_info),
  {ok, Period} = application:get_env(veil_cluster_node, node_monitoring_period),
  erlang:send_after(1000 * Period, self(), {timer, monitor_mem_net}),
  {noreply, State#node_state{memory_and_network_info = NewInfo}};

handle_cast({delete_callback_by_pid, Pid}, State) ->
  Fuse = get_fuse_by_callback_pid(State, Pid),
  case Fuse of
    not_found -> ok;
    _ ->
      spawn(fun() ->
        try
          gen_server:call({global, ?CCM}, {delete_callback, Fuse, node(), Pid}, 2000)
        catch
          _:_ ->
            lager:error("delete_callback - error during contact with CCM"),
            error
        end
      end)
  end,
  {noreply, State};

handle_cast({register_simple_cache, Cache, ReturnPid}, State) ->
  Caches = State#node_state.simple_caches,
  NewCaches = case lists:member(Cache, Caches) of
    true -> Caches;
    false -> [Cache | Caches]
  end,
  ReturnPid ! simple_cache_registered,
  {noreply, State#node_state{simple_caches = NewCaches}};

handle_cast({start_load_logging, Path}, State) ->
  lager:info("Start load logging on node: ~p", [node()]),
  {ok, Interval} = application:get_env(?APP_Name, node_load_logging_period),
  {MegaSecs, Secs, MicroSecs} = os:timestamp(),
  case whereis(?Load_Logging_Proc) of
    undefined -> supervisor:start_child(?Supervisor_Name, ?Sup_Child(?Load_Logging_Proc, ?MODULE, start_load_logging_loop, permanent, [Path, 1000000 * MegaSecs + Secs + MicroSecs / 1000000]));
    Pid -> Pid ! {log, Interval}
  end,
  {noreply, State};

handle_cast(stop_load_logging, State) ->
  lager:info("Stop load logging on node: ~p", [node()]),
  case whereis(?Load_Logging_Proc) of
    undefined -> ok;
    Pid -> Pid ! stop
  end,
  {noreply, State};

handle_cast({notify_lfm, EventType, Enabled}, State) ->
  case Enabled of
    true -> ets:insert(?LFM_EVENT_PRODUCTION_ENABLED_ETS, {EventType, true});
    _ -> ets:delete(?LFM_EVENT_PRODUCTION_ENABLED_ETS, EventType)
  end,
  {noreply, State};

handle_cast({update_user_write_enabled, UserDn, Enabled}, State) ->
  case Enabled of
    false -> ets:insert(?WRITE_DISABLED_USERS, {UserDn, true});
    _ -> ets:delete(?WRITE_DISABLED_USERS, UserDn)
  end,
  {noreply, State};

handle_cast({node_for_ack, MsgID, Node}, State) ->
  ets:insert(?ACK_HANDLERS, {{chosen_node, MsgID}, Node}),
  {ok, Interval} = application:get_env(veil_cluster_node, callback_ack_time),
  Pid = self(),
  erlang:send_after(Interval * 1000, Pid, {delete_node_for_ack, MsgID}),
  {noreply, State};

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
  gen_server:cast(?Node_Manager_Name, Msg),
  {noreply, State};

handle_info({delete_node_for_ack, MsgID}, State) ->
  ets:delete(?ACK_HANDLERS, {chosen_node, MsgID}),
  {noreply, State};

handle_info({nodedown, _Node}, State) ->
  lager:error("Connection to CCM lost"),
  {noreply, State#node_state{ccm_con_status = not_connected}};

handle_info(_Info, State) ->
  ?error("Error: wrong info: ~p", [_Info]),
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
  try
		cowboy:stop_listener(?DISPATCHER_LISTENER_REF),
    ok
  catch
    _:_ -> ok
  end.

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

%% heart_beat/2
%% ====================================================================
%% @doc Connects with ccm and tells that the node is alive.
%% First it establishes network connection, next sends message to ccm.
-spec heart_beat(Conn_status :: atom(), State::term()) -> NewStatus when
  NewStatus ::  term().
%% ====================================================================
heart_beat(Conn_status, State) ->
  New_conn_status = case Conn_status of
                                    not_connected ->
                                      {ok, CCM_Nodes} = application:get_env(veil_cluster_node, ccm_nodes),
                                      Ans = init_net_connection(CCM_Nodes),
                                      case Ans of
                                        ok -> connected;
                                        error -> not_connected
                                      end;
                                    Other -> Other
                                  end,

  {ok, Interval} = application:get_env(veil_cluster_node, heart_beat),
  case New_conn_status of
    connected ->
      gen_server:cast({global, ?CCM}, {node_is_up, node()}),
      erlang:send_after(Interval * 1000, self(), {timer, do_heart_beat});
    _ -> erlang:send_after(500, self(), {timer, do_heart_beat})
  end,

  lager:info([{mod, ?MODULE}], "Heart beat on node: ~p: sent; connection: ~p, old conn_status: ~p,  state_num: ~b, callback_num: ~b,  disp dispatcher_state: ~b, callbacks_state: ~b",
    [node(), New_conn_status, Conn_status, State#node_state.state_num, State#node_state.callbacks_num, State#node_state.dispatcher_state, State#node_state.callbacks_state]),
  State#node_state{ccm_con_status = New_conn_status}.

%% heart_beat_response/2
%% ====================================================================
%% @doc Saves information about ccm connection when ccm answers to its request
-spec heart_beat_response(New_state_num :: integer(), CallbacksNum :: integer(), State::term()) -> NewStatus when
  NewStatus ::  term().
%% ====================================================================
heart_beat_response(New_state_num, CallbacksNum, State) ->
  lager:info([{mod, ?MODULE}], "Heart beat on node: ~p: answered, new state_num: ~b, new callback_num: ~b", [node(), New_state_num, CallbacksNum]),

  case (New_state_num == State#node_state.state_num) of
    true -> ok;
    false ->
      %% TODO find a method which do not force clearing of all simple caches at all nodes when only one worker/node is added/deleted
      %% Now all caches are canceled because we do not know if state number change is connected with network problems (so cache of node may be not valid)
      %% TODO during refactoring integrate simple and permanent cache (cache clearing can be triggered as CacheCheckFun)
      clear_simple_caches(State#node_state.simple_caches)
  end,

  case (New_state_num == State#node_state.state_num) and (New_state_num == State#node_state.dispatcher_state) and (CallbacksNum == State#node_state.callbacks_num) and (CallbacksNum == State#node_state.callbacks_state)
  of
    true -> State;
    false ->
      update_dispatcher(New_state_num, CallbacksNum, State#node_state.node_type),
      State#node_state{state_num = New_state_num, callbacks_num = CallbacksNum}
  end.

%% update_dispatcher/2
%% ====================================================================
%% @doc Tells dispatcher that cluster state has changed.
-spec update_dispatcher(New_state_num :: integer(), CallbacksNum :: integer(), Type :: atom()) -> Result when
  Result ::  atom().
%% ====================================================================
update_dispatcher(New_state_num, CallbacksNum, Type) ->
  case Type =:= ccm of
    true -> ok;
    false -> gen_server:cast(?Dispatcher_Name, {update_state, New_state_num, CallbacksNum})
  end.

%% init_net_connection/1
%% ====================================================================
%% @doc Initializes network connection with cluster that contains nodes
%% given in argument.
-spec init_net_connection(Nodes :: list()) -> Result when
  Result ::  atom().
%% ====================================================================
init_net_connection([]) ->
  error;

init_net_connection([Node | Nodes]) ->
  try
    Ans = net_adm:ping(Node),
    case Ans of
      pong ->
        erlang:monitor_node(Node, true),
        global:sync(),
        ok;
      pang -> init_net_connection(Nodes)
    end
  catch
    _:_ -> error
  end.

%% check_vsn/0
%% ====================================================================
%% @doc Checks application version
-spec check_vsn() -> Result when
  Result :: term().
%% ====================================================================
check_vsn() ->
  check_vsn(application:which_applications()).

%% check_vsn/1
%% ====================================================================
%% @doc Checks application version
-spec check_vsn(ApplicationData :: list()) -> Result when
  Result :: term().
%% ====================================================================
check_vsn([]) ->
  non;

check_vsn([{Application, _Description, Vsn} | Apps]) ->
  case Application of
    ?APP_Name -> Vsn;
    _Other -> check_vsn(Apps)
  end.

%% start_load_logging_loop/2
%% ====================================================================
%% @doc Start loop that logs current load of node
-spec start_load_logging_loop(Path :: string(), StartTime :: float()) -> no_return().
%% ====================================================================
start_load_logging_loop(Path, StartTime) ->
  {ok, Interval} = application:get_env(?APP_Name, node_load_logging_period),
  case file:open(Path ++ "/load_log.csv", [append]) of
    {ok, Fd} ->
      Pid = spawn_link(?MODULE, load_logging_loop, [Fd, StartTime, StartTime]),
      register(?Load_Logging_Proc, Pid),
      case file:position(Fd, eof) of
        {ok, 0} ->
          io:fwrite(Fd, "elapsed, window, cpu, mem, input, output~n", []),
          erlang:send_after(Interval * 1000, Pid, {log, Interval});
        _ -> Pid ! {log, Interval}
      end;
    Other -> lager:error("Error while openning file: ~p", [Other])
  end.

%% TODO consider integration of load logging loop with node_manager callbacks after development of monitorig function
%% load_logging_loop/3
%% ====================================================================
%% @doc Loop that logs current load of node
-spec load_logging_loop(Fd :: pid(), StartTime :: float(), PrevTime :: float()) -> no_return().
%% ====================================================================
load_logging_loop(Fd, StartTime, PrevTime) ->
  receive
    {log, Interval} ->
      {MegaSecs, Secs, MicroSecs} = os:timestamp(),
      CurrTime = 1000000 * MegaSecs + Secs + MicroSecs / 1000000,
      {Proc, Mem, {In, Out}} = gen_server:call({?Node_Manager_Name, node()}, {get_node_stats, short}, 500),
      io:fwrite(Fd, "~.6f, ~.6f, ~.6f, ~.6f, ~p, ~p~n", [CurrTime - StartTime, CurrTime - PrevTime, Proc, Mem, In, Out]),
      erlang:send_after(Interval * 1000, self(), {log, Interval}),
      load_logging_loop(Fd, StartTime, CurrTime);
    stop ->
      file:close(Fd);
    Other ->
      lager:error("Load logging loop got unknown message: ~p", [Other]),
      load_logging_loop(Fd, StartTime, PrevTime)
  end.

%% get_node_stats/2
%% ====================================================================
%% @doc Get statistics about node load
-spec get_node_stats(Window :: atom(), Stats :: term()) -> Result when
  Result :: term().
%% ====================================================================

get_node_stats(Window, {New, Old, NewListSize, _Max}) ->
  {ok, Period} = application:get_env(veil_cluster_node, node_monitoring_period),
  {ProcTmp, MemAndNetSize}  = case Window of
                                short -> {cpu_sup:avg1(), round(60 / Period + 1)};
                                medium -> {cpu_sup:avg5(), round(5 * 60 / Period + 1)};
                                long -> {cpu_sup:avg15(), round(15 * 60 / Period + 1)};
                                _W -> wrong_window
                              end,
  Proc = ProcTmp / 256,

  MemAndNet = case NewListSize >= MemAndNetSize of
                true -> lists:sublist(New, MemAndNetSize);
                false -> lists:flatten([New, lists:sublist(Old, MemAndNetSize-NewListSize)])
              end,

  CalculateMemAndNet = fun({Mem, {In, Out}}, {MemTmpSum, {InTmpSum, OutTmpSum}, {LastIn, LastOut}}) ->
    case LastIn of
      non -> {MemTmpSum + Mem, {InTmpSum, OutTmpSum}, {In, Out}};
      _ -> {MemTmpSum + Mem, {InTmpSum + erlang:max(LastIn - In, 0), OutTmpSum + erlang:max(LastOut - Out, 0)}, {In, Out}}
    end
  end,
  {MemSum, {InSum, OutSum}, _} = lists:foldl(CalculateMemAndNet, {0, {0, 0}, {non, non}}, MemAndNet),
  MemAndNetListSize = length(MemAndNet),
  MemAvg = case MemAndNetListSize of
             0 -> 0;
             _ -> MemSum / MemAndNetListSize
           end,
  {Proc, MemAvg, {InSum, OutSum}}.

%% get_memory_and_net_info/0
%% ====================================================================
%% @doc Checks memory and network usage
-spec get_memory_and_net_info() -> Result when
  Result :: term().
%% ====================================================================

get_memory_and_net_info() ->
  {Total, Allocated, _Worst} = memsup:get_memory_data(),
  Mem = case Total of
          0 -> 0;
          _ -> Allocated / Total
        end,
  Ports = erlang:ports(),
  GetNetInfo = fun(Port, {InTmp, OutTmp}) ->
    In = case erlang:port_info(Port, input) of
           {input, V} -> V;
           _Other -> 0
         end,
    Out = case erlang:port_info(Port, output) of
            {output, V2} -> V2;
            _Other2 -> 0
          end,
    {InTmp + In, OutTmp + Out}
  end,
  Net = lists:foldl(GetNetInfo, {0, 0}, Ports),
  {Mem, Net}.

%% save_progress/2
%% ====================================================================
%% @doc Saves information about node load
-spec save_progress(Report :: atom(), Stats :: term()) -> Result when
  Result :: term().
%% ====================================================================

save_progress(Report, {New, Old, NewListSize, Max}) ->
  case NewListSize + 1 of
    Max ->
      {[], [Report | New], 0, Max};
    S ->
      {[Report | New], Old, S, Max}
  end.

%% get_callback/2
%% ====================================================================
%% @doc Gets callback to fuse (if there are more than one callback it
%% chooses one).
-spec get_callback(State :: term(), FuseId :: string()) -> Result when
  Result :: non | pid().
%% ====================================================================
get_callback(State, FuseId) ->
  {Callback, NewCallbacks} = get_pid_list(FuseId, State#node_state.callbacks, []),
  {Callback, State#node_state{callbacks = NewCallbacks}}.

%% get_pid_list/3
%% ====================================================================
%% @doc Helper function that sets callback to fuse (if there are more t
%% han one callback itchooses one).
-spec get_pid_list(FuseId :: string(), CallbacksList :: list(), NewCallbacksList :: list()) -> Result when
  Result :: {non | pid(), NewCallbacks},
  NewCallbacks :: list().
%% ====================================================================
get_pid_list(_FuseId, [], NewList) ->
  {non, NewList};
get_pid_list(FuseId, [{F, {CList1, CList2}} | T], NewList) ->
  case F =:= FuseId of
    true ->
      {Callback, NewLists} = choose_callback(CList1, CList2),
      {Callback, [{F, NewLists} | T] ++ NewList};
    false -> get_pid_list(FuseId, T, [{F, {CList1, CList2}} | NewList])
  end.

%% get_all_callbacks/2
%% ====================================================================
%% @doc Gets all callbacks to fuse.
-spec get_all_callbacks(State :: term(), FuseId :: string()) -> Result when
  Result :: list().
%% ====================================================================
get_all_callbacks(Fuse, State) ->
  {L1, L2} = proplists:get_value(Fuse, State#node_state.callbacks, {[],[]}),
  lists:flatten(L1, L2).

%% addCallback/3
%% ====================================================================
%% @doc Adds callback to fuse.
-spec addCallback(State :: term(), FuseId :: string(), Pid :: pid()) -> NewState when
  NewState :: list().
%% ====================================================================
addCallback(State, FuseId, Pid) ->
  NewCallbacks = update_pid_list(FuseId, Pid, State#node_state.callbacks, []),
  State#node_state{callbacks = NewCallbacks}.

%% update_pid_list/4
%% ====================================================================
%% @doc Helper function that adds callback to fuse.
-spec update_pid_list(FuseId :: string(), NewPid :: pid(), CallbacksList :: list(), NewCallbacksList :: list()) -> Result when
  Result :: list().
%% ====================================================================
update_pid_list(FuseId, NewPid, [], Ans) ->
  [{FuseId, {[NewPid], []}} | Ans];
update_pid_list(FuseId, NewPid, [{F, {CList1, CList2}} | T], Ans) ->
  case F =:= FuseId of
    true ->
      case lists:member(NewPid, CList1) or lists:member(NewPid, CList2) of
        true ->
          [{F, {CList1, CList2}} | Ans] ++ T;
        false ->
          [{F, {[NewPid | CList1], CList2}} | Ans] ++ T
      end;
    false -> update_pid_list(FuseId, NewPid, T, [{F, {CList1, CList2}} | Ans])
  end.

%% choose_callback/2
%% ====================================================================
%% @doc Helper function that chooses callback to use.
-spec choose_callback(List1 :: list(), List2 :: list()) -> Result when
  Result :: {non | pid(), {list(), list()}}.
%% ====================================================================
choose_callback([], []) ->
  {non, {[], []}};
choose_callback([], L2) ->
  choose_callback(L2, []);
choose_callback([Callback | L1], L2) ->
  {Callback, {L1, [Callback | L2]}}.

%% delete_callback/3
%% ====================================================================
%% @doc Deletes callback
-spec delete_callback(State :: term(), FuseId :: string(), Pid :: pid()) -> Result when
  Result :: {NewState, fuse_not_found | fuse_deleted | pid_not_found | pid_deleted},
  NewState :: term().
%% ====================================================================
delete_callback(State, FuseId, Pid) ->
  {NewCallbacks, DeleteAns} = delete_pid_from_list(FuseId, Pid, State#node_state.callbacks, []),
  {State#node_state{callbacks = NewCallbacks}, DeleteAns}.

%% delete_pid_from_list/4
%% ====================================================================
%% @doc Helper function that deletes callback
-spec delete_pid_from_list(FuseId :: string(), Pid :: pid(), CallbacksList :: list(), NewCallbacksList :: list()) -> Result when
  Result :: {NewList, fuse_not_found | fuse_deleted | pid_not_found | pid_deleted},
  NewList :: term().
%% ====================================================================
delete_pid_from_list(_FuseId, _Pid, [], Ans) ->
  {Ans, fuse_not_found};
delete_pid_from_list(FuseId, Pid, [{F, {CList1, CList2}} | T], Ans) ->
  case F =:= FuseId of
    true ->
      Length1 = length(CList1) + length(CList2),
      NewCList1 = lists:delete(Pid, CList1),
      NewCList2 = lists:delete(Pid, CList2),
      case length(NewCList1) + length(NewCList2) of
        0 -> {Ans ++ T, fuse_deleted};
        Length1 -> {[{F, {NewCList1, NewCList2}} | Ans] ++ T, pid_not_found};
        _ -> {[{F, {NewCList1, NewCList2}} | Ans] ++ T, pid_deleted}
      end;
    false -> delete_pid_from_list(FuseId, Pid, T, [{F, {CList1, CList2}} | Ans])
  end.

%% get_fuses_list/1
%% ====================================================================
%% @doc Get all fuses that have callbacka at this node
-spec get_fuses_list(State :: term()) -> Result when
  Result :: list().
%% ====================================================================
get_fuses_list(State) ->
  lists:map(fun({Fuse, _Pids}) ->
    Fuse
  end, State#node_state.callbacks).

%% get_fuse_by_callback_pid/2
%% ====================================================================
%% @doc Gets fuseId with which callback is connected
-spec get_fuse_by_callback_pid(State :: term(), Pid :: pid()) -> Result when
  Result :: not_found | string().
%% ====================================================================
get_fuse_by_callback_pid(State, Pid) ->
  get_fuse_by_callback_pid_helper(Pid, State#node_state.callbacks).

%% get_fuse_by_callback_pid_helper/2
%% ====================================================================
%% @doc Helper function that gets fuseId with which callback is connected
-spec get_fuse_by_callback_pid_helper(Pid :: pid(), Callbacks :: list()) -> Result when
  Result :: not_found | string().
%% ====================================================================
get_fuse_by_callback_pid_helper(_Pid, []) ->
  not_found;
get_fuse_by_callback_pid_helper(Pid, [{F, {CList1, CList2}} | T]) ->
  case lists:member(Pid, CList1) or lists:member(Pid, CList2) of
    true -> F;
    false -> get_fuse_by_callback_pid_helper(Pid, T)
  end.

%% clear_simple_caches/1
%% ====================================================================
%% @doc Clears all caches at node
-spec clear_simple_caches(Caches :: list()) -> ok.
%% ====================================================================
clear_simple_caches(Caches) ->
  lists:foreach(fun
    ({sub_proc_cache, Cache}) ->
      worker_host:clear_sub_procs_cache(Cache);
    ({permanent_cache, _Cache}) -> ok;
    ({permanent_cache, _Cache, CacheCheckFun}) -> CacheCheckFun();
    (Cache) -> ets:delete_all_objects(Cache)
  end, Caches).

%% clear_cache/1
%% ====================================================================
%% @doc Clears chosen caches at node
-spec clear_cache(Cache :: term(), Caches :: list()) -> ok.
%% ====================================================================
clear_cache(Cache, Caches) ->
  Method = case Cache of
    CacheName when is_atom(CacheName) ->
      {Cache, all, Cache};
    {permanent_cache, CacheName2} ->
      {Cache, all, CacheName2};
    {permanent_cache, CacheName3, _} ->
      {Cache, all, CacheName3};
    {sub_proc_cache, SubProcCache} ->
      {Cache, sub_proc, SubProcCache};
    {{sub_proc_cache, SubProcCache2}, SubProcKey} ->
      {{sub_proc_cache, SubProcCache2}, sub_proc, {SubProcCache2, SubProcKey}};
    {{permanent_cache, CacheName4}, Keys} when is_list(Keys) ->
      {{permanent_cache, CacheName4}, list, {CacheName4, Keys}};
    {{permanent_cache, CacheName5}, Key} ->
      {{permanent_cache, CacheName5}, simple, {CacheName5, Key}};
    {{permanent_cache, CacheName6, _}, Keys2} when is_list(Keys2) ->
      {{permanent_cache, CacheName6}, list, {CacheName6, Keys2}};
    {{permanent_cache, CacheName7, _}, Key2} ->
      {{permanent_cache, CacheName7}, simple, {CacheName7, Key2}};
    {CacheName8, Keys3} when is_list(Keys3) ->
      {CacheName8, list, {CacheName8, Keys3}};
    {CacheName9, Key3} ->
      {CacheName9, simple, {CacheName9, Key3}};
    [] ->
      ok;
    [H | T] ->
      Ans1 = clear_cache(H, Caches),
      Ans2 = clear_cache(T, Caches),
      case {Ans1, Ans2} of
        {ok, ok} -> ok;
        _ -> error
      end
  end,
  case Method of
    {CName, ClearingMethod, ClearingMethodAttr} ->
      case lists:member(CName, Caches) of
        true ->
          case ClearingMethod of
            simple ->
              {EtsName, EtsKey} = ClearingMethodAttr,
              ets:delete(EtsName, EtsKey),
              ok;
            all ->
              ets:delete_all_objects(ClearingMethodAttr),
              ok;
            sub_proc ->
              worker_host:clear_sub_procs_cache(ClearingMethodAttr);
            list ->
              {EtsName2, KeysToDel} = ClearingMethodAttr,
              lists:foreach(fun(K) -> ets:delete(EtsName2, K) end, KeysToDel),
              ok
          end;
        false ->
          ok
      end;
    _ -> Method
  end.
