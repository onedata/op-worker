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
-export([create_node_stats_rrd/1, save_node_stats_to_rrd/0, log_load/4]).

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
  Result :: {ok, Pid}
  | ignore
  | {error, Error},
  Pid :: pid(),
  Error :: {already_started, Pid} | term().
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
init([Type]) when Type =:= worker; Type =:= ccm; Type =:= ccm_test ->
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
          {keyfile, atom_to_list(CertFile)},
          {password, ""},
          {verify, verify_peer}, {verify_fun, {fun gsi_handler:verify_callback/3, []}}
        ],
        [
          {env, [{dispatch, Dispatch}]}
        ]),
      spawn(?MODULE, create_node_stats_rrd, [self()]);
    false -> ok
  end,

  process_flag(trap_exit, true),
  erlang:send_after(10, self(), {timer, do_heart_beat}),

  {ok, #node_state{node_type = Type, ccm_con_status = not_connected}};

init([Type]) when Type =:= test_worker ->
  process_flag(trap_exit, true),
  spawn(?MODULE, create_node_stats_rrd, [self()]),
  erlang:send_after(10, self(), {timer, do_heart_beat}),

  {ok, #node_state{node_type = worker, ccm_con_status = not_connected}};

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

handle_call({get_node_stats, TimeWindow}, _From, State) ->
  Reply = get_node_stats(TimeWindow),
  {reply, Reply, State};

handle_call({get_node_stats, StartTime, EndTime}, _From, State) ->
  Reply = get_node_stats(StartTime, EndTime),
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
  NewState = State#node_state{dispatcher_state = DispState, callbacks_state = DispCallbacksNum},
  {noreply, NewState};

handle_cast(stop, State) ->
  {stop, normal, State};

handle_cast(monitor_node, State) ->
  spawn(?MODULE, save_node_stats_to_rrd, []),
  {ok, Period} = application:get_env(?APP_Name, node_monitoring_period),
  erlang:send_after(1000 * Period, self(), {timer, monitor_node}),
  {noreply, State};

handle_cast({delete_callback_by_pid, Pid}, State) ->
  Fuse = get_fuse_by_callback_pid(State, Pid),
  case Fuse of
    not_found -> ok;
    _ ->
      spawn(fun() ->
        try
          gen_server:call({global, ?CCM}, {delete_callback, Fuse, node(), Pid}, 1000)
        catch
          _:_ ->
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
  {MegaSecs, Secs, MicroSecs} = erlang:now(),
  StartTime = MegaSecs * 1000000 + Secs + MicroSecs / 1000000,
  case file:open(Path ++ "/load_log.csv", [write]) of
    {ok, Fd} ->
      NodeStats = get_node_stats(0),
      Header = string:join(["elapsed", "window" | lists:map(fun({Name, _}) -> atom_to_list(Name) end, NodeStats)], ", ") ++ "\n",
      io:fwrite(Fd, Header, []),
      erlang:send_after(Interval * 1000, self(), {timer, {log_load, StartTime, StartTime}}),
      {noreply, State#node_state{load_logging_fd = Fd}};
    _ -> {noreply, State}
  end;

handle_cast({log_load, StartTime, PrevTime}, State) ->
  spawn(?MODULE, log_load, [self(), State#node_state.load_logging_fd, StartTime, PrevTime]),
  {noreply, State};

handle_cast(stop_load_logging, State) ->
  lager:info("Stop load logging on node: ~p", [node()]),
  case State#node_state.load_logging_fd of
    undefined -> ok;
    Fd -> file:close(Fd)
  end,
  {noreply, State#node_state{load_logging_fd = undefined}};

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

handle_info({nodedown, _Node}, State) ->
  lager:error("Connection to CCM lost"),
  {noreply, State#node_state{ccm_con_status = not_connected}};

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
-spec heart_beat(Conn_status :: atom(), State :: term()) -> NewStatus when
  NewStatus :: term().
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
-spec heart_beat_response(New_state_num :: integer(), CallbacksNum :: integer(), State :: term()) -> NewStatus when
  NewStatus :: term().
%% ====================================================================
heart_beat_response(New_state_num, CallbacksNum, State) ->
  lager:info([{mod, ?MODULE}], "Heart beat on node: ~p: answered, new state_num: ~b, new callback_num: ~b", [node(), New_state_num, CallbacksNum]),

  case (New_state_num == State#node_state.state_num) of
    true -> ok;
    false ->
      %% TODO find a method which do not force clearing of all simple caches at all nodes when only one worker/node is added/deleted
      %% Now all caches are canceled because we do not know if state number change is connected with network problems (so cache of node may be not valid)
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
  Result :: atom().
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
  Result :: atom().
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

%% log_load/4
%% ====================================================================
%% @doc Writes node load to file
-spec log_load(Pid :: pid(), Fd :: pid(), StartTime :: integer(), PrevTime :: integer()) -> Result when
  Result :: ok.
%% ====================================================================
log_load(Pid, Fd, StartTime, PrevTime) ->
  {ok, Interval} = application:get_env(?APP_Name, node_load_logging_period),
  {MegaSecs, Secs, MicroSecs} = erlang:now(),
  CurrTime = MegaSecs * 1000000 + Secs + MicroSecs / 1000000,
  case Fd of
    undefined -> ok;
    _ ->
      NodeStats = get_node_stats(short),
      Values = [CurrTime - StartTime, CurrTime - PrevTime | lists:map(fun({_, Value}) ->
        Value end, NodeStats)],
      Format = string:join(lists:duplicate(length(Values), "~.6f"), ", ") ++ "\n",
      io:fwrite(Fd, Format, Values),
      erlang:send_after(Interval * 1000, Pid, {timer, {log_load, StartTime, CurrTime}}),
      ok
  end.

%% create_node_stats_rrd/1
%% ====================================================================
%% @doc Creates node stats Round Robin Database
  -spec create_node_stats_rrd(Pid :: pid()) -> Result when
Result :: {ok, Data :: binary()} | {error, Error :: term()}.
%% ====================================================================
create_node_stats_rrd(Pid) ->
  {ok, Period} = application:get_env(?APP_Name, node_monitoring_period),
  Heartbeat = 2 * Period,
  RRASize = round(24 * 60 * 60 / Period), % one day in seconds devided by monitoring period
  Steps = [1, 7, 30, 365], % defines how many primary data points are used to build a consolidated data point which then goes into the archive, it is an equivalent of [1 day, 7 days, 30 days, 365 days]
  BinaryPeriod = integer_to_binary(Period),
  BinaryHeartbeat = integer_to_binary(Heartbeat),
  RRASizeBinary = integer_to_binary(RRASize),
  Options = <<"--step ", BinaryPeriod/binary>>,

  DSs = lists:map(fun({Name, _}) -> BinaryName = atom_to_binary(Name, utf8),
    <<"DS:", BinaryName/binary, ":GAUGE:", BinaryHeartbeat/binary, ":0:100">> end, get_cpu_stats()) ++
    lists:map(fun({Name, _}) -> BinaryName = atom_to_binary(Name, utf8),
      <<"DS:", BinaryName/binary, ":GAUGE:", BinaryHeartbeat/binary, ":0:100">> end, get_memory_stats()) ++
    lists:map(fun({Name, _}) -> BinaryName = atom_to_binary(Name, utf8),
      <<"DS:", BinaryName/binary, ":COUNTER:", BinaryHeartbeat/binary, ":U:U">> end, get_storage_stats()) ++
    lists:map(fun({Name, _}) -> BinaryName = atom_to_binary(Name, utf8),
      <<"DS:", BinaryName/binary, ":COUNTER:", BinaryHeartbeat/binary, ":0:U">> end, get_network_stats()) ++
    lists:map(fun({Name, _}) -> BinaryName = atom_to_binary(Name, utf8),
      <<"DS:", BinaryName/binary, ":COUNTER:", BinaryHeartbeat/binary, ":0:U">> end, get_port_stats()),

  RRAs = lists:map(fun(Step) -> BinaryStep = integer_to_binary(Step),
    <<"RRA:AVERAGE:0.5:", BinaryStep/binary, ":", RRASizeBinary/binary>> end, Steps),

  case rrderlang:create(?Node_Stats_RRD_Name, Options, DSs, RRAs) of
    {error, Error} -> lager:error([{mod, ?MODULE}], "Can not create node stats RRD: ~p", [Error]);
    _ -> erlang:send_after(100, Pid, {timer, monitor_node})
  end.

%% get_node_stats/1
%% ====================================================================
%% @doc Get statistics about node load
-spec get_node_stats(TimeWindow :: short | medium | long | integer()) -> Result when
  Result :: [{Name :: atom(), Value :: float()}] | {error, term()}.
%% ====================================================================
get_node_stats(TimeWindow) ->
  Interval = case TimeWindow of
               short -> 1 * 60;
               medium -> 5 * 60;
               long -> 15 * 60;
               _ -> TimeWindow
             end,
  {MegaSecs, Secs, _} = erlang:now(),
  EndTime = MegaSecs * 1000000 + Secs,
  StartTime = EndTime - Interval,
  get_node_stats(StartTime, EndTime).

%% get_node_stats/2
%% ====================================================================
%% @doc Get statistics about node load
-spec get_node_stats(StartTime :: integer(), EndTime :: integer()) -> Result when
  Result :: [{Name :: atom(), Value :: float()}] | {error, term()}.
%% ====================================================================
get_node_stats(StartTime, EndTime) ->
  BinaryEndTime = integer_to_binary(EndTime),
  BinaryStartTime = integer_to_binary(StartTime),
  Options = <<"--start ", BinaryStartTime/binary, " --end ", BinaryEndTime/binary>>,
  case rrderlang:fetch(?Node_Stats_RRD_Name, Options, <<"AVERAGE">>) of
    {ok, {Header, Data}} ->
      HeaderAtom = lists:map(fun(Elem) -> binary_to_atom(Elem, utf8) end, Header),
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

      lists:zip(HeaderAtom, AvgValues);
    Other -> Other
  end.

%% save_node_stats_to_rrd/0
%% ====================================================================
%% @doc Saves node stats to Round Robin Database
-spec save_node_stats_to_rrd() -> Result when
  Result :: ok | {error, Error :: term()}.
%% ====================================================================
save_node_stats_to_rrd() ->
  {MegaSecs, Secs, _} = erlang:now(),
  Timestamp = MegaSecs * 1000000 + Secs,
  Stats = get_cpu_stats() ++ get_memory_stats() ++ get_storage_stats() ++ get_network_stats() ++ get_port_stats(),
  Values = lists:map(fun({_, Value}) -> Value end, Stats),
  case rrderlang:update(?Node_Stats_RRD_Name, <<>>, Values, Timestamp) of
    {error, Error} ->
      lager:error([{mod, ?MODULE}], "Can not save node stats to RRD: ~p", [Error]),
      {error, Error};
    _ -> ok
  end.

%% get_network_stats/0
%% ====================================================================
%% @doc Checks network usage
-spec get_network_stats() -> Result when
  Result :: [{network_rx_ops, Value} | {network_tx_ops, Value} |
  {network_rx_bps, Value} | {network_tx_bps, Value}],
  Value :: float() | undefined.
%% ====================================================================
get_network_stats() ->
  {ok, Period} = application:get_env(?APP_Name, node_monitoring_period),
  case file:list_dir("/sys/class/net") of
    {ok, Interfaces} ->
      SumInterfacesStats = fun(Type) ->
        lists:foldl(fun(Interface, Acc) ->
          case {get_interface_stats(Interface, Type), Acc} of
            {undefined, _} -> Acc;
            {InterfaceStat, undefined} -> InterfaceStat;
            {InterfaceStat, _} -> InterfaceStat + Acc
          end
        end, undefined, Interfaces)
      end,
      RxBytes = SumInterfacesStats("rx_bytes"),
      TxBytes = SumInterfacesStats("tx_bytes"),
      RxPackets = SumInterfacesStats("rx_packets"),
      TxPackets = SumInterfacesStats("tx_packets"),

      lists:map(fun
        ({Name, undefined}) -> {Name, undefined};
        ({Name, Value}) -> {Name, round(Value / Period)}
      end, [{network_rx_bps, RxBytes}, {network_tx_bps, TxBytes},
        {network_rx_pps, RxPackets}, {network_tx_pps, TxPackets}]);
    _ ->
      [{network_rx_bps, undefined}, {network_tx_bps, undefined},
        {network_rx_pps, undefined}, {network_tx_pps, undefined}]
  end.

%% get_interface_stats/2
%% ====================================================================
%% @doc Checks interface usage, where Interface is a name (e.g. eth0)
%% and Type is name of collecting statistics (e.g. rx_bytes)
-spec get_interface_stats(Interface :: string(), Type :: string()) -> Result when
  Result :: non_neg_integer() | undefined.
%% ====================================================================
get_interface_stats(Interface, Type) ->
  Filename = "/sys/class/net/" ++ Interface ++ "/statistics/" ++ Type,
  case file:open(Filename, [raw]) of
    {ok, Fd} ->
      case file:read_line(Fd) of
        {ok, Value} ->
          list_to_integer(string:strip(Value, right, $\n));
        _ -> undefined
      end;
    _ -> undefined
  end.

%% get_cpu_stats/0
%% ====================================================================
%% @doc Checks cpu usage
-spec get_cpu_stats() -> Result when
  Result :: [{cpu, Value} | {cpu_avg1, Value} | {cpu_avg5, Value} | {cpu_avg15, Value}],
  Value :: float() | undefined.
%% ====================================================================
get_cpu_stats() ->
  Try = fun(Fun) ->
    case Fun() of
      {error, _} -> undefined;
      Result -> Result
    end
  end,
  [
    {cpu, Try(fun() -> cpu_sup:util() end)},
    {cpu_avg1, Try(fun() -> cpu_sup:avg1() end)},
    {cpu_avg5, Try(fun() -> cpu_sup:avg5() end)},
    {cpu_avg15, Try(fun() -> cpu_sup:avg15() end)}
  ].

%% get_port_stats/0
%% ====================================================================
%% @doc Checks port usage
-spec get_port_stats() -> Result when
  Result :: [{port_rx_bps, Value} | {port_tx_bps, Value}],
  Value :: float() | undefined.
%% ====================================================================
get_port_stats() ->
  {ok, Period} = application:get_env(?APP_Name, node_monitoring_period),
  Ports = erlang:ports(),
  lists:map(fun
    ({Name, undefinde}) -> {Name, undefined};
    ({Name, Value}) -> {Name, round(Value / Period)} end,
    [
      {port_rx_bps, lists:foldl(
        fun(_, undefined) -> undefined;
          (Port, Acc) -> case erlang:port_info(Port, input) of
                           {input, Bytes} -> Acc + Bytes;
                           undefined -> undefined
                         end
        end, 0, Ports)},
      {port_tx_bps, lists:foldl(
        fun(_, undefined) -> undefined;
          (Port, Acc) -> case erlang:port_info(Port, output) of
                           {output, Bytes} -> Acc + Bytes;
                           undefined -> undefined
                         end
        end, 0, Ports)}
    ]).

%% get_memory_stats/0
%% ====================================================================
%% @doc Checks memory usage
-spec get_memory_stats() -> Result when
  Result :: [{mem, Value}],
  Value :: float() | undefined.
%% ====================================================================
get_memory_stats() ->
  MemDataList = memsup:get_system_memory_data(),
  {FreeMem, TotalMem} = lists:foldl(
    fun({free_memory, Size}, {_, TotalMem}) -> {Size, TotalMem};
      ({total_memory, Size}, {FreeMem, _}) -> {FreeMem, Size};
      ({_, _}, {FreeMem, TotalMem}) -> {FreeMem, TotalMem}
    end,
    {undefined, undefined}, MemDataList),
  case FreeMem of
    undefined -> [{mem, undefined}];
    _ -> case TotalMem of
           undefined -> [{mem, undefined}];
           _ -> [{mem, 100 * (TotalMem - FreeMem) / TotalMem}]
         end
  end.

%% get_storage_stats/0
%% ====================================================================
%% @doc Checks storage usage
-spec get_storage_stats() -> Result when
  Result :: [{storage_rx_ops, Value} | {storage_tx_ops, Value} |
  {storage_rx_bps, Value} | {storage_tx_bps, Value}],
  Value :: float() | undefined.
%% ====================================================================
get_storage_stats() ->
  [{storage_rx_ops, undefined}, {storage_tx_ops, undefined}, {storage_rx_bps, undefined}, {storage_tx_bps, undefined}].

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
  {L1, L2} = proplists:get_value(Fuse, State#node_state.callbacks, {[], []}),
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
    (Cache) -> ets:delete_all_objects(Cache)
  end, Caches).

%% clear_cache/1
%% ====================================================================
%% @doc Clears chosen caches at node
-spec clear_cache(Cache :: term(), Caches :: list()) -> ok.
%% ====================================================================
clear_cache(Cache, Caches) ->
  case Cache of
    CacheName when is_atom(CacheName) ->
      case lists:member(Cache, Caches) of
        true ->
          ets:delete_all_objects(Cache),
          ok;
        false ->
          ok
      end;
    {sub_proc_cache, SubProcCache} ->
      case lists:member(Cache, Caches) of
        true -> worker_host:clear_sub_procs_cache(SubProcCache);
        false -> ok
      end;
    {{sub_proc_cache, SubProcCache2}, SubProcKey} ->
      case lists:member({sub_proc_cache, SubProcCache2}, Caches) of
        true -> worker_host:clear_sub_procs_cache(SubProcCache2, SubProcKey);
        false -> ok
      end;
    {CacheName3, Keys} when is_list(Keys) ->
      case lists:member(CacheName3, Caches) of
        true ->
          lists:foreach(fun(K) -> ets:delete(CacheName3, K) end, Keys),
          ok;
        false ->
          ok
      end;
    {CacheName2, Key} ->
      case lists:member(CacheName2, Caches) of
        true ->
          ets:delete(CacheName2, Key),
          ok;
        false ->
          ok
      end;
    [] ->
      ok;
    [H | T] ->
      Ans1 = clear_cache(H, Caches),
      Ans2 = clear_cache(T, Caches),
      case {Ans1, Ans2} of
        {ok, ok} -> ok;
        _ -> error
      end
  end.
