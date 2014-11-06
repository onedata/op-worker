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
-include("oneprovider_modules/control_panel/common.hrl").
-include_lib("ctool/include/logging.hrl").

%% Path (relative to domain) on which cowboy expects client's requests
-define(ONECLIENT_URI_PATH, "/oneclient").

%% ------------
% GUI and cowboy related defines

% Paths in gui static directory
-define(static_paths, ["/common/", "/css/", "/flatui/", "/fonts/", "/images/", "/js/", "/n2o/"]).

% Session logic module
-define(session_logic_module, session_logic).

% GUI routing module
-define(gui_routing_module, gui_routes).

% Custom cowboy bridge module
-define(cowboy_bridge_module, n2o_handler).

% Cowboy listener references
-define(dispatcher_listener, dispatcher).
-define(https_listener, https).
-define(rest_listener, rest).
-define(http_redirector_listener, http).

%% Reload time for storage UIDs and GIDs.
-define(storage_ids_reload_time, timer:seconds(20)).

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/1, stop/0]).
-export([check_vsn/0]).

%% ====================================================================
%% Test API
%% ====================================================================
%% TODO zmierzyć czy bardziej się opłaca przechowywać dane o callbackach
%% jako stan (jak teraz) czy jako ets i ewentualnie przejść na ets
-ifdef(TEST).
-export([get_callback/2, addCallback/3, delete_callback/3]).
-export([calculate_network_stats/3, get_interface_stats/2, get_cpu_stats/1, calculate_ports_transfer/4,
    get_memory_stats/0, is_valid_name/1, is_valid_name/2, is_valid_character/1]).
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
                cowboy:stop_listener(?dispatcher_listener),
                ok
            catch
                _:_ -> ok
            end,
            start_dispatcher_listener(),
            erlang:send_after(0, self(), {timer, init_listeners}),
            {ok, MonitoringInitialization} = application:get_env(?APP_Name, node_monitoring_initialization),
            erlang:send_after(1000 * MonitoringInitialization, self(), {timer, start_node_monitoring});
        false -> ok
    end,

    %% TODO: replace with permanent cache
    ets:new(?TOKEN_AUTHENTICATION_CACHE, [set, named_table, public]),
    ets:new(?LFM_EVENT_PRODUCTION_ENABLED_ETS, [set, named_table, public]),
    ets:new(?WRITE_DISABLED_USERS, [set, named_table, public]),
    ets:new(?ACK_HANDLERS, [set, named_table, public]),
    ets:new(?STORAGE_USER_IDS_CACHE, [set, named_table, protected, {read_concurrency, true}]),
    ets:new(?STORAGE_GROUP_IDS_CACHE, [set, named_table, protected, {read_concurrency, true}]),

    erlang:send_after(10, self(), {timer, do_heart_beat}),
    erlang:send_after(0, self(), {timer, reload_storage_users}),
    erlang:send_after(0, self(), {timer, reload_storage_groups}),

    {ok, #node_state{node_type = Type, ccm_con_status = not_connected}};

init([Type]) when Type =:= test_worker ->
    {ok, MonitoringInitialization} = application:get_env(?APP_Name, node_monitoring_initialization),
    erlang:send_after(1000 * MonitoringInitialization, self(), {timer, start_node_monitoring}),
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

handle_call({get_node_stats, StartTime, EndTime, Columns}, _From, State) ->
    Reply = get_node_stats(StartTime, EndTime, Columns),
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

handle_call({check_storage, FilePath, Content}, _From, State) ->
    {reply, fslogic_storage:check_storage_on_node(FilePath, Content), State};

handle_call(_Request, _From, State) ->
    ?warning("Wrong call: ~p", [_Request]),
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

handle_cast(start_node_monitoring, State) ->
    spawn(fun() -> create_node_stats_rrd(State) end),
    {noreply, State};

handle_cast(init_listeners, State) ->
    try
        start_gui_listener(),
        start_rest_listener(),
        start_redirector_listener()
    catch T:M ->
        ?error_stacktrace("Error while initializing cowboy listeners - ~p:~p", [T, M])
    end,
    {noreply, State};

handle_cast(monitor_node, State) ->
    spawn(fun() -> save_node_stats_to_rrd(State) end),
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
                    gen_server:call({global, ?CCM}, {delete_callback, Fuse, node(), Pid}, 2000)
                catch
                    _:_ ->
                        ?error("delete_callback - error during contact with CCM"),
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
    ?info("simple_cache_registered: ~p", [Cache]),
    ReturnPid ! simple_cache_registered,
    {noreply, State#node_state{simple_caches = NewCaches}};

handle_cast({start_load_logging, Path}, #node_state{load_logging_fd = undefined} = State) ->
    ?info("Start load logging on node: ~p", [node()]),
    {ok, Interval} = application:get_env(?APP_Name, node_load_logging_period),
    {MegaSecs, Secs, MicroSecs} = erlang:now(),
    StartTime = MegaSecs * 1000000 + Secs + MicroSecs / 1000000,
    case file:open(Path, [write]) of
        {ok, Fd} ->
            case get_node_stats(short) of
                NodeStats when is_list(NodeStats) ->
                    Header = string:join(["elapsed", "window" | lists:map(fun({Name, _}) ->
                        Name
                    end, NodeStats)], ", ") ++ "\n",
                    io:fwrite(Fd, Header, []),
                    erlang:send_after(Interval * 1000, self(), {timer, {log_load, StartTime, StartTime}}),
                    {noreply, State#node_state{load_logging_fd = Fd}};
                Other ->
                    ?error("Can not get node stats: ~p", [Other]),
                    {noreply, State}
            end;
        _ -> {noreply, State}
    end;
handle_cast({start_load_logging, _}, State) ->
    ?warning("Load logging already started on node: ~p", [node()]),
    {noreply, State};

handle_cast({log_load, _, _}, #node_state{load_logging_fd = undefined} = State) ->
    ?warning("Can not log load: file descriptor is undefined."),
    {noreply, State};
handle_cast({log_load, StartTime, PrevTime}, #node_state{load_logging_fd = Fd} = State) ->
    {ok, Interval} = application:get_env(?APP_Name, node_load_logging_period),
    {MegaSecs, Secs, MicroSecs} = erlang:now(),
    CurrTime = MegaSecs * 1000000 + Secs + MicroSecs / 1000000,
    spawn(fun() -> log_load(Fd, StartTime, PrevTime, CurrTime) end),
    erlang:send_after(Interval * 1000, self(), {timer, {log_load, StartTime, CurrTime}}),
    {noreply, State};

handle_cast(stop_load_logging, #node_state{load_logging_fd = undefined} = State) ->
    ?warning("Load logging already stopped on node: ~p", [node()]),
    {noreply, State};
handle_cast(stop_load_logging, #node_state{load_logging_fd = Fd} = State) ->
    ?info("Stop load logging on node: ~p", [node()]),
    file:close(Fd),
    {noreply, State#node_state{load_logging_fd = undefined}};

handle_cast({notify_lfm, EventType, Enabled}, State) ->
    ?debug("notify_lfm message: ~p", [{EventType, Enabled}]),
    case Enabled of
        true -> ets:insert(?LFM_EVENT_PRODUCTION_ENABLED_ETS, {EventType, true});
        _ -> ets:delete(?LFM_EVENT_PRODUCTION_ENABLED_ETS, EventType)
    end,
    {noreply, State};

handle_cast({update_user_write_enabled, UserDn, Enabled}, State) ->
    ?debug("update_user_write_enabled message: ~p", [{UserDn, Enabled}]),
    case Enabled of
        false -> ets:insert(?WRITE_DISABLED_USERS, {UserDn, true});
        _ -> ets:delete(?WRITE_DISABLED_USERS, UserDn)
    end,
    {noreply, State};

handle_cast({node_for_ack, MsgID, Node}, State) ->
    ?debug("Node for ack chosen: ~p", [{MsgID, Node}]),
    ets:insert(?ACK_HANDLERS, {{chosen_node, MsgID}, Node}),
    {ok, Interval} = application:get_env(?APP_Name, callback_ack_time),
    Pid = self(),
    erlang:send_after(Interval * 1000, Pid, {delete_node_for_ack, MsgID}),
    {noreply, State};

handle_cast({update_cpu_stats, CpuStats}, State) ->
    {noreply, State#node_state{cpu_stats = CpuStats}};

handle_cast({update_network_stats, NetworkStats}, State) ->
    {noreply, State#node_state{network_stats = NetworkStats}};

handle_cast({update_ports_stats, PortsStats}, State) ->
    {noreply, State#node_state{ports_stats = PortsStats}};


handle_cast(reload_storage_users, State) ->
    {ok, Data} = file:read_file("/etc/passwd"),
    UserTokens = binary:split(Data, [<<10>>, <<13>>], [global, trim]),
    EtsMap = maps:from_list(ets:tab2list(?STORAGE_USER_IDS_CACHE)),
    UnusedMap =
        lists:foldl(
            fun(Elem, CurrMap) ->
                [UserName, _, UID | _] = binary:split(Elem, [<<":">>], [global, trim]),
                ets:insert(?STORAGE_USER_IDS_CACHE, {UserName, binary_to_integer(UID)}),
                maps:remove(UserName, CurrMap)
            end, EtsMap, UserTokens),

    [ets:delete(?STORAGE_USER_IDS_CACHE, Key) || Key <- maps:keys(UnusedMap)],

    erlang:send_after(timer:seconds(20), self(), {timer, reload_storage_users}),
    {noreply, State};

handle_cast(reload_storage_groups, State) ->
    {ok, Data} = file:read_file("/etc/group"),
    GroupTokens = binary:split(Data, [<<10>>, <<13>>], [global, trim]),
    EtsMap = maps:from_list(ets:tab2list(?STORAGE_GROUP_IDS_CACHE)),
    UnusedMap =
        lists:foldl(
            fun(Elem, CurrMap) ->
                [GroupName, _, GID | _] = binary:split(Elem, [<<":">>], [global, trim]),
                ets:insert(?STORAGE_GROUP_IDS_CACHE, {GroupName, binary_to_integer(GID)}),
                maps:remove(GroupName, CurrMap)
            end, EtsMap, GroupTokens),

    [ets:delete(?STORAGE_GROUP_IDS_CACHE, Key) || Key <- maps:keys(UnusedMap)],

    erlang:send_after(timer:seconds(20), self(), {timer, reload_storage_groups}),
    {noreply, State};

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
    gen_server:cast(?Node_Manager_Name, Msg),
    {noreply, State};

handle_info({delete_node_for_ack, MsgID}, State) ->
    ?debug("Node for ack deleted: ~p", [MsgID]),
    ets:delete(?ACK_HANDLERS, {chosen_node, MsgID}),
    {noreply, State};

handle_info({nodedown, _Node}, State) ->
    ?error("Connection to CCM lost"),
    {noreply, State#node_state{ccm_con_status = not_connected}};

handle_info(_Info, State) ->
    ?warning("Wrong info: ~p", [_Info]),
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
    % Stop all listeners
    catch cowboy:stop_listener(?dispatcher_listener),
    catch cowboy:stop_listener(?http_redirector_listener),
    catch cowboy:stop_listener(?rest_listener),
    catch cowboy:stop_listener(?https_listener),

    catch ets:delete(?TOKEN_AUTHENTICATION_CACHE),

    % Clean up after n2o.
    catch gui_utils:cleanup_n2o(?session_logic_module),
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

%% heart_beat/2
%% ====================================================================
%% @doc Connects with ccm and tells that the node is alive.
%% First it establishes network connection, next sends message to ccm.
-spec heart_beat(Conn_status :: atom(), State :: term()) -> NewStatus when
    NewStatus :: term().
%% ====================================================================
%% TODO Check why first heart beat is not successful
heart_beat(Conn_status, State) ->
    New_conn_status = case Conn_status of
                          not_connected ->
                              {ok, CCM_Nodes} = application:get_env(?APP_Name, ccm_nodes),
                              Ans = init_net_connection(CCM_Nodes),
                              case Ans of
                                  ok -> connected;
                                  error -> not_connected
                              end;
                          Other -> Other
                      end,

    {ok, Interval} = application:get_env(?APP_Name, heart_beat),
    case New_conn_status of
        connected ->
            gen_server:cast({global, ?CCM}, {node_is_up, node()}),
            erlang:send_after(Interval * 1000, self(), {timer, do_heart_beat});
        _ -> erlang:send_after(500, self(), {timer, do_heart_beat})
    end,

    ?debug("Heart beat on node: ~p: sent; connection: ~p, old conn_status: ~p,  state_num: ~b, callback_num: ~b,  disp dispatcher_state: ~b, callbacks_state: ~b",
        [node(), New_conn_status, Conn_status, State#node_state.state_num, State#node_state.callbacks_num, State#node_state.dispatcher_state, State#node_state.callbacks_state]),
    State#node_state{ccm_con_status = New_conn_status}.

%% heart_beat_response/2
%% ====================================================================
%% @doc Saves information about ccm connection when ccm answers to its request
-spec heart_beat_response(New_state_num :: integer(), CallbacksNum :: integer(), State :: term()) -> NewStatus when
    NewStatus :: term().
%% ====================================================================
heart_beat_response(New_state_num, CallbacksNum, State) ->
    ?debug("Heart beat on node: ~p: answered, new state_num: ~b, new callback_num: ~b", [node(), New_state_num, CallbacksNum]),

    case (New_state_num == State#node_state.state_num) of
        true -> ok;
        false ->
            %% TODO find a method which do not force clearing of all simple caches at all nodes when only one worker/node is added/deleted
            %% Now all caches are canceled because we do not know if state number change is connected with network problems (so cache of node may be not valid)
            %% TODO during refactoring integrate simple and permanent cache (cache clearing can be triggered as CacheCheckFun)
            ?debug("Simple caches cleared"),
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
        false ->
            ?debug("Message sent to update dispatcher, state and callbacks num: ~p", [{New_state_num, CallbacksNum}]),
            gen_server:cast(?Dispatcher_Name, {update_state, New_state_num, CallbacksNum})
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
                ?debug("Connection to nodes ~p initialized", [Nodes]),
                ok;
            pang ->
                ?error("Cannot connect to node ~p", [Node]),
                init_net_connection(Nodes)
        end
    catch
        E1:E2 ->
            ?error("Error ~p:~p during initialization of cennection to nodes: ~p", [E1, E2, Nodes]),
            error
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
-spec log_load(Fd :: pid(), StartTime :: integer(), PrevTime :: integer(), CurrTime :: integer()) -> Result when
    Result :: ok | {error, Reason :: term()}.
%% ====================================================================
log_load(Fd, StartTime, PrevTime, CurrTime) ->
    case get_node_stats(short) of
        NodeStats when is_list(NodeStats) ->
            Values = [CurrTime - StartTime, CurrTime - PrevTime | lists:map(fun({_, Value}) -> Value end, NodeStats)],
            Format = string:join(lists:duplicate(length(Values), "~.6f"), ", ") ++ "\n",
            io:fwrite(Fd, Format, Values),
            ok;
        Other ->
            ?error("Can not get node stats: ~p", [Other]),
            {error, Other}
    end.

%% create_node_stats_rrd/1
%% ====================================================================
%% @doc Creates node stats Round Robin Database
-spec create_node_stats_rrd(State :: term()) -> Result when
    Result :: ok | {error, Error :: term()}.
%% ====================================================================
create_node_stats_rrd(#node_state{cpu_stats = CpuStats, network_stats = NetworkStats, ports_stats = PortsStats}) ->
    {ok, Timeout} = application:get_env(?APP_Name, rrd_timeout),
    {ok, Period} = application:get_env(?APP_Name, node_monitoring_period),
    {ok, Steps} = application:get_env(?APP_Name, rrd_steps),
    {ok, RRDSize} = application:get_env(?APP_Name, rrd_size),
    Heartbeat = 2 * Period,
    RRASize = round(RRDSize / Period),
    BinaryPeriod = integer_to_binary(Period),
    BinaryHeartbeat = integer_to_binary(Heartbeat),
    RRASizeBinary = integer_to_binary(RRASize),
    Options = <<"--step ", BinaryPeriod/binary>>,

    DSs = lists:map(fun({Name, _}) ->
        <<"DS:", Name/binary, ":GAUGE:", BinaryHeartbeat/binary, ":0:100">> end, get_cpu_stats(CpuStats)) ++
        lists:map(fun({Name, _}) ->
            <<"DS:", Name/binary, ":GAUGE:", BinaryHeartbeat/binary, ":0:100">> end, get_memory_stats()) ++
        lists:map(fun({Name, _}) ->
            <<"DS:", Name/binary, ":GAUGE:", BinaryHeartbeat/binary, ":0:U">> end, get_network_stats(NetworkStats)) ++
        lists:map(fun({Name, _}) ->
            <<"DS:", Name/binary, ":GAUGE:", BinaryHeartbeat/binary, ":0:U">> end, get_ports_stats(PortsStats)),

    RRAs = lists:map(fun(Step) -> BinaryStep = integer_to_binary(Step),
        <<"RRA:AVERAGE:0.5:", BinaryStep/binary, ":", RRASizeBinary/binary>> end, Steps),

    case gen_server:call(?RrdErlang_Name, {create, ?Node_Stats_RRD_Name, Options, DSs, RRAs}, Timeout) of
        {error, Error} ->
            ?error("Can not create node stats RRD: ~p", [Error]),
            {error, Error};
        _ ->
            gen_server:cast(?Node_Manager_Name, monitor_node),
            ?debug("Node RRD created"),
            ok
    end.

%% get_node_stats/2
%% ====================================================================
%% @doc Get statistics about node load
%% TimeWindow - time in seconds since now, that defines interval for which statistics will be fetched
-spec get_node_stats(TimeWindow :: short | medium | long | integer()) -> Result when
    Result :: [{Name :: string(), Value :: float()}] | {error, term()}.
%% ====================================================================
get_node_stats(TimeWindow) ->
    {ok, Interval} = case TimeWindow of
                         short -> application:get_env(?APP_Name, short_monitoring_time_window);
                         medium -> application:get_env(?APP_Name, medium_monitoring_time_window);
                         long -> application:get_env(?APP_Name, long_monitoring_time_window);
                         _ when is_integer(TimeWindow) -> {ok, TimeWindow};
                         _ ->
                             ?warning("Wrong statistics time window: ~p", [TimeWindow]),
                             application:get_env(?APP_Name, short_monitoring_time_window)
                     end,
    {MegaSecs, Secs, _} = erlang:now(),
    EndTime = 1000000 * MegaSecs + Secs,
    StartTime = EndTime - Interval,
    get_node_stats(StartTime, EndTime).

%% get_node_stats/2
%% ====================================================================
%% @doc Get statistics about node load
%% StartTime, EndTime - time in seconds since epoch (1970-01-01), that defines interval
%% for which statistics will be fetched
-spec get_node_stats(StartTime :: integer(), EndTime :: integer()) -> Result when
    Result :: [{Name :: string(), Value :: float()}] | {error, term()}.
%% ====================================================================
get_node_stats(StartTime, EndTime) ->
    {ok, Timeout} = application:get_env(?APP_Name, rrd_timeout),
    BinaryStartTime = integer_to_binary(StartTime),
    BinaryEndTime = integer_to_binary(EndTime),
    Options = <<"--start ", BinaryStartTime/binary, " --end ", BinaryEndTime/binary>>,
    case gen_server:call(?RrdErlang_Name, {fetch, ?Node_Stats_RRD_Name, Options, <<"AVERAGE">>}, Timeout) of
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

%% get_node_stats/3
%% ====================================================================
%% @doc Fetch specified columns from node statistics Round Robin Database.
-spec get_node_stats(StartTime :: integer(), EndTime :: integer(), Columns) -> Result when
    Columns :: all | {name, [binary()]} | {starts_with, [binary()]} | {index, [integer()]},
    Result :: {ok, {Header, Body}} | {error, Error :: term()},
    Header :: [ColumnNames :: binary()],
    Body :: [Row],
    Row :: [{Timestamp, Values}],
    Timestamp :: integer(),
    Values :: [integer() | float()].
%% ====================================================================
get_node_stats(StartTime, EndTime, Columns) ->
    {ok, Timeout} = application:get_env(?APP_Name, rrd_timeout),
    BinaryStartTime = integer_to_binary(StartTime),
    BinaryEndTime = integer_to_binary(EndTime),
    Options = <<"--start ", BinaryStartTime/binary, " --end ", BinaryEndTime/binary>>,
    gen_server:call(?RrdErlang_Name, {fetch, ?Node_Stats_RRD_Name, Options, <<"AVERAGE">>, Columns}, Timeout).

%% save_node_stats_to_rrd/1
%% ====================================================================
%% @doc Saves node stats to Round Robin Database
-spec save_node_stats_to_rrd(State :: term()) -> Result when
    Result :: ok | {error, Error :: term()}.
%% ====================================================================
save_node_stats_to_rrd(#node_state{cpu_stats = CpuStats, network_stats = NetworkStats, ports_stats = PortsStats}) ->
    {ok, Timeout} = application:get_env(?APP_Name, rrd_timeout),
    {MegaSecs, Secs, _} = erlang:now(),
    Timestamp = integer_to_binary(MegaSecs * 1000000 + Secs),
    Stats = get_cpu_stats(CpuStats) ++ get_memory_stats() ++ get_network_stats(NetworkStats) ++
        get_ports_stats(PortsStats),
    Values = lists:map(fun({_, Value}) -> Value end, Stats),
    case gen_server:call(?RrdErlang_Name, {update, ?Node_Stats_RRD_Name, <<>>, Values, Timestamp}, Timeout) of
        {error, Error} ->
            ?error("Can not save node stats to RRD: ~p", [Error]),
            {error, Error};
        _ -> ok
    end.

%% get_network_stats/0
%% ====================================================================
%% @doc Checks network usage
-spec get_network_stats(NetworkStats :: [{Name, Value}]) -> Result when
    Result :: [{Name, Value}],
    Name :: string(),
    Value :: non_neg_integer().
%% ====================================================================
get_network_stats(NetworkStats) ->
    {ok, Period} = application:get_env(?APP_Name, node_monitoring_period),
    Dir = "/sys/class/net/",
    case file:list_dir(Dir) of
        {ok, Interfaces} ->
            ValidInterfaces = lists:filter(fun(Interface) ->
                is_valid_name(Interface, 11)
            end, Interfaces),
            CurrentNetworkStats = lists:foldl(
                fun(Interface, Stats) -> [
                    {<<"net_rx_b_", (list_to_binary(Interface))/binary>>, get_interface_stats(Interface, "rx_bytes")},
                    {<<"net_tx_b_", (list_to_binary(Interface))/binary>>, get_interface_stats(Interface, "tx_bytes")},
                    {<<"net_rx_pps_", (list_to_binary(Interface))/binary>>, get_interface_stats(Interface, "rx_packets") / Period},
                    {<<"net_tx_pps_", (list_to_binary(Interface))/binary>>, get_interface_stats(Interface, "tx_packets") / Period} |
                    Stats
                ] end, [], ValidInterfaces),
            gen_server:cast(?Node_Manager_Name, {update_network_stats, CurrentNetworkStats}),
            calculate_network_stats(CurrentNetworkStats, NetworkStats, []);
        _ -> []
    end.

calculate_network_stats([], _, Stats) ->
    lists:reverse(Stats);
calculate_network_stats([Stat | CurrentNetworkStats], [], Stats) ->
    calculate_network_stats(CurrentNetworkStats, [], [Stat | Stats]);
calculate_network_stats([{Name, StatA} | CurrentNetworkStats], [{Name, StatB} | NetworkStats], Stats) ->
    calculate_network_stats(CurrentNetworkStats, NetworkStats, [{Name, StatA - StatB} | Stats]);
calculate_network_stats([{NameA, StatA} | CurrentNetworkStats], [{NameB, _} | NetworkStats], Stats) when NameA < NameB ->
    calculate_network_stats(CurrentNetworkStats, NetworkStats, [{NameA, StatA} | Stats]);
calculate_network_stats([{NameA, _} | CurrentNetworkStats], [{NameB, _} | NetworkStats], Stats) when NameA > NameB ->
    calculate_network_stats(CurrentNetworkStats, NetworkStats, Stats);
calculate_network_stats(_, _, Stats) ->
    lists:reverse(Stats).

%% get_interface_stats/2
%% ====================================================================
%% @doc Checks interface usage, where Interface is a name (e.g. eth0)
%% and Type is name of collecting statistics (e.g. rx_bytes)
-spec get_interface_stats(Interface :: string(), Type :: string()) -> Result when
    Result :: non_neg_integer().
%% ====================================================================
get_interface_stats(Interface, Type) ->
    Filename = "/sys/class/net/" ++ Interface ++ "/statistics/" ++ Type,
    case file:open(Filename, [raw]) of
        {ok, Fd} ->
            InterfaceStats = case file:read_line(Fd) of
                                 {ok, Value} -> list_to_integer(string:strip(Value, right, $\n));
                                 _ -> 0
                             end,
            file:close(Fd),
            InterfaceStats;
        _ -> 0
    end.

%% get_cpu_stats/1
%% ====================================================================
%% @doc Checks cpu usage
-spec get_cpu_stats(CpuStats :: [{Name, WorkJiffies, TotalJiffies}]) -> Result when
    WorkJiffies :: non_neg_integer(),
    TotalJiffies :: non_neg_integer(),
    Result :: [{Name, Value}],
    Name :: string(),
    Value :: float().
%% ====================================================================
get_cpu_stats(CpuStats) ->
    case file:open("/proc/stat", [read]) of
        {ok, Fd} ->
            CurrentCpuStats = read_cpu_stats(Fd, []),
            gen_server:cast(?Node_Manager_Name, {update_cpu_stats, CurrentCpuStats}),
            case calculate_cpu_stats(CurrentCpuStats, CpuStats, []) of
                [Main, _OneCore] -> [Main];
                MoreCores -> MoreCores
            end;
        _ -> []
    end.

read_cpu_stats(Fd, Stats) ->
    case file:read_line(Fd) of
        {ok, "cpu" ++ _ = Line} ->
            ["cpu" ++ ID | Values] = string:tokens(string:strip(Line, right, $\n), " "),
            Name = case ID of
                       "" -> <<"cpu">>;
                       _ -> <<"core", (list_to_binary(ID))/binary>>
                   end,
            case is_valid_name(Name, 0) of
                true ->
                    [User, Nice, System | Rest] = lists:map(fun(Value) -> list_to_integer(Value) end, Values),
                    WorkJiffies = User + Nice + System,
                    TotalJiffies = WorkJiffies + lists:foldl(fun(Value, Sum) -> Value + Sum end, 0, Rest),
                    read_cpu_stats(Fd, [{Name, WorkJiffies, TotalJiffies} | Stats]);
                _ -> read_cpu_stats(Fd, Stats)
            end;
        {ok, _} ->
            read_cpu_stats(Fd, Stats);
        _ ->
            file:close(Fd),
            Stats
    end.

calculate_cpu_stats([{Name, _, _} | CurrentCpuStats], [], Stats) ->
    calculate_cpu_stats(CurrentCpuStats, [], [{Name, 0} | Stats]);
calculate_cpu_stats([{Name, _, T} | CurrentCpuStats], [{Name, _, T} | CpuStats], Stats) ->
    calculate_cpu_stats(CurrentCpuStats, CpuStats, [{Name, 0} | Stats]);
calculate_cpu_stats([{Name, WA, TA} | CurrentCpuStats], [{Name, WB, TB} | CpuStats], Stats) ->
    calculate_cpu_stats(CurrentCpuStats, CpuStats, [{Name, 100 * (WA - WB) / (TA - TB)} | Stats]);
calculate_cpu_stats(_, _, Stats) ->
    Stats.

%% get_port_stats/1
%% ====================================================================
%% @doc Checks port usage
-spec get_ports_stats(PortsStats :: [{Port, Input, Output}]) -> Result when
    Port :: port(),
    Input :: non_neg_integer(),
    Output :: non_neg_integer(),
    Result :: [{Name, Value}],
    Name :: string(),
    Value :: non_neg_integer().
%% ====================================================================
get_ports_stats(PortsStats) ->
    GetPortInfo = fun(Port, Item) ->
        case erlang:port_info(Port, Item) of
            {Item, Value} -> Value;
            _ -> 0
        end
    end,
    ExistingPorts = lists:sort(erlang:ports()),
    ExistingPortsStats = lists:map(
        fun(Port) -> {Port, GetPortInfo(Port, input), GetPortInfo(Port, output)}
        end, ExistingPorts),
    gen_server:cast(?Node_Manager_Name, {update_ports_stats, ExistingPortsStats}),
    calculate_ports_transfer(PortsStats, ExistingPortsStats, 0, 0).

calculate_ports_transfer([], [{_, In, Out} | Ports], Input, Output) ->
    calculate_ports_transfer([], Ports, Input + In, Output + Out);
calculate_ports_transfer([{Port, InA, OutA} | PortsA], [{Port, InB, OutB} | PortsB], Input, Output) ->
    calculate_ports_transfer(PortsA, PortsB, Input + InB - InA, Output + OutB - OutA);
calculate_ports_transfer([{PortA, _, _} | PortsA], [{PortB, InB, OutB} | PortsB], Input, Output) when PortA < PortB ->
    calculate_ports_transfer(PortsA, [{PortB, InB, OutB} | PortsB], Input, Output);
calculate_ports_transfer([{PortA, _, _} | PortsA], [{PortB, InB, OutB} | PortsB], Input, Output) when PortA > PortB ->
    calculate_ports_transfer(PortsA, PortsB, Input + InB, Output + OutB);
calculate_ports_transfer(_, _, Input, Output) ->
    [{<<"ports_rx_b">>, Input}, {<<"ports_tx_b">>, Output}].

%% get_memory_stats/0
%% ====================================================================
%% @doc Checks memory usage
-spec get_memory_stats() -> Result when
    Result :: [{Name, Value}],
    Name :: string(),
    Value :: float().
%% ====================================================================
get_memory_stats() ->
    case file:open("/proc/meminfo", [read]) of
        {ok, Fd} -> read_memory_stats(Fd, undefined, undefined, 0);
        _ -> 0
    end.

read_memory_stats(Fd, MemFree, MemTotal, 2) ->
    file:close(Fd),
    [{<<"mem">>, 100 * (MemTotal - MemFree) / MemTotal}];
read_memory_stats(Fd, MemFree, MemTotal, Counter) ->
    GetValue = fun(Line) ->
        [Value | _] = string:tokens(Line, " "),
        list_to_integer(Value)
    end,
    case file:read_line(Fd) of
        {ok, "MemTotal:" ++ Line} -> read_memory_stats(Fd, MemFree, GetValue(Line), Counter + 1);
        {ok, "MemFree:" ++ Line} -> read_memory_stats(Fd, GetValue(Line), MemTotal, Counter + 1);
        eof -> file:close(Fd), [];
        {error, _} -> [];
        _ -> read_memory_stats(Fd, MemFree, MemTotal, Counter)
    end.

%% is_valid_name/2
%% ====================================================================
%% @doc Checks whether string contains only following characters a-zA-Z0-9_
%% and with some prefix is not longer than 19 characters. This is a requirement
%% for a valid column name in Round Robin Database.
-spec is_valid_name(Name :: string() | binary(), PrefixLength :: integer()) -> Result when
    Result :: true | false.
%% ====================================================================
is_valid_name(Name, PrefixLength) when is_list(Name) ->
    case length(Name) =< 19 - PrefixLength of
        true -> is_valid_name(Name);
        _ -> false
    end;
is_valid_name(Name, PrefixLength) when is_binary(Name) ->
    is_valid_name(binary_to_list(Name), PrefixLength);
is_valid_name(_, _) ->
    false.

%% is_valid_name/1
%% ====================================================================
%% @doc Checks whether string contains only following characters a-zA-Z0-9_
-spec is_valid_name(Name :: string() | binary()) -> Result when
    Result :: true | false.
%% ====================================================================
is_valid_name([]) ->
    false;
is_valid_name([Character]) ->
    is_valid_character(Character);
is_valid_name([Character | Characters]) ->
    is_valid_character(Character) andalso is_valid_name(Characters);
is_valid_name(Name) when is_binary(Name) ->
    is_valid_name(binary_to_list(Name));
is_valid_name(_) ->
    false.

%% is_valid_character/1
%% ====================================================================
%% @doc Checks whether character belongs to a-zA-Z0-9_ range
-spec is_valid_character(Character :: char()) -> Result when
    Result :: true | false.
%% ====================================================================
is_valid_character($_) -> true;
is_valid_character(Character) when Character >= $0 andalso Character =< $9 -> true;
is_valid_character(Character) when Character >= $A andalso Character =< $Z -> true;
is_valid_character(Character) when Character >= $a andalso Character =< $z -> true;
is_valid_character(_) -> false.

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
    ?debug("Callback added: ~p", [{FuseId, Pid}]),
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
    ?debug("Deleting callback, ans ~p", [DeleteAns]),
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
%% @doc Get all fuses that have callbacks at this node
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
    ?debug("Clearing caches: ~p", [Caches]),
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


%% ====================================================================
%% Cowboy listeners starting
%% ====================================================================


%% start_dispatcher_listener/0
%% ====================================================================
%% @doc Starts a cowboy listener for request_dispatcher.
%% @end
-spec start_dispatcher_listener() -> ok | no_return().
%% ====================================================================
start_dispatcher_listener() ->
    {ok, Port} = application:get_env(?APP_Name, dispatcher_port),
    {ok, DispatcherPoolSize} = application:get_env(?APP_Name, dispatcher_pool_size),
    {ok, CertFile} = application:get_env(?APP_Name, fuse_ssl_cert_path),

    LocalPort = oneproxy:get_local_port(Port),
    Pid = spawn_link(fun() -> oneproxy:start(Port, LocalPort, CertFile, verify_peer) end),
    register(?ONEPROXY_DISPATCHER, Pid),

    Dispatch = cowboy_router:compile([{'_', [{?ONECLIENT_URI_PATH, ws_handler, []}]}]),

    {ok, _} = cowboy:start_http(?dispatcher_listener, DispatcherPoolSize,
        [
            {ip, {127, 0, 0, 1}},
            {port, LocalPort}
        ],
        [
            {env, [{dispatch, Dispatch}]}
        ]),
    ok.


%% start_gui_listener/0
%% ====================================================================
%% @doc Starts a cowboy listener for n2o GUI.
%% @end
-spec start_gui_listener() -> ok | no_return().
%% ====================================================================
start_gui_listener() ->
    % Get params from env for gui
    {ok, DocRoot} = application:get_env(?APP_Name, control_panel_static_files_root),

    {ok, Cert} = application:get_env(?APP_Name, web_ssl_cert_path),

    {ok, GuiPort} = application:get_env(?APP_Name, control_panel_port),
    {ok, GuiNbAcceptors} = application:get_env(?APP_Name, control_panel_number_of_acceptors),
    {ok, MaxKeepAlive} = application:get_env(?APP_Name, control_panel_max_keepalive),
    {ok, Timeout} = application:get_env(?APP_Name, control_panel_socket_timeout),

    LocalPort = oneproxy:get_local_port(GuiPort),
    spawn_link(fun() -> oneproxy:start(GuiPort, LocalPort, Cert, verify_none) end),

    % Setup GUI dispatch opts for cowboy
    GUIDispatch = [
        % Matching requests will be redirected to the same address without leading 'www.'
        % Cowboy does not have a mechanism to match every hostname starting with 'www.'
        % This will match hostnames with up to 6 segments
        % e. g. www.seg2.seg3.seg4.seg5.com
        {"www.:_[.:_[.:_[.:_[.:_]]]]", [{'_', opn_cowboy_bridge,
            [
                {delegation, true},
                {handler_module, redirect_handler},
                {handler_opts, []}
            ]}
        ]},
        % Proper requests are routed to handler modules
        {'_', static_dispatches(DocRoot, ?static_paths) ++ [
            {"/nagios/[...]", opn_cowboy_bridge,
                [
                    {delegation, true},
                    {handler_module, nagios_handler},
                    {handler_opts, []}
                ]},
            {?user_content_download_path ++ "/:path", opn_cowboy_bridge,
                [
                    {delegation, true},
                    {handler_module, file_download_handler},
                    {handler_opts, [{type, ?user_content_request_type}]}
                ]},
            {?shared_files_download_path ++ "/:path", opn_cowboy_bridge,
                [
                    {delegation, true},
                    {handler_module, file_download_handler},
                    {handler_opts, [{type, ?shared_files_request_type}]}
                ]},
            {?file_upload_path, opn_cowboy_bridge,
                [
                    {delegation, true},
                    {handler_module, file_upload_handler},
                    {handler_opts, []}
                ]},
            {"/ws/[...]", opn_cowboy_bridge,
                [
                    {delegation, true},
                    {handler_module, opn_bullet_handler},
                    {handler_opts, [{handler, n2o_bullet}]}
                ]},
            {'_', opn_cowboy_bridge,
                [
                    {delegation, true},
                    {handler_module, n2o_handler},
                    {handler_opts, []}
                ]}
        ]}
    ],

    % Create ets tables and set envs needed by n2o
    gui_utils:init_n2o_ets_and_envs(GuiPort, ?gui_routing_module, ?session_logic_module, ?cowboy_bridge_module),

    % Start the listener for web gui and nagios handler
    {ok, _} = cowboy:start_http(?https_listener, GuiNbAcceptors,
        [
            {ip, {127, 0, 0, 1}},
            {port, LocalPort}
        ],
        [
            {env, [{dispatch, cowboy_router:compile(GUIDispatch)}]},
            {max_keepalive, MaxKeepAlive},
            {timeout, Timeout},
            % On every request, add headers that improve security to the response
            {onrequest, fun gui_utils:onrequest_adjust_headers/1}
        ]).


%% start_redirector_listener/0
%% ====================================================================
%% @doc Starts a cowboy listener that will redirect all requests of http to https.
%% @end
-spec start_redirector_listener() -> ok | no_return().
%% ====================================================================
start_redirector_listener() ->
    {ok, RedirectPort} = application:get_env(?APP_Name, control_panel_redirect_port),
    {ok, RedirectNbAcceptors} = application:get_env(?APP_Name, control_panel_number_of_http_acceptors),
    {ok, Timeout} = application:get_env(?APP_Name, control_panel_socket_timeout),

    RedirectDispatch = [
        {'_', [
            {'_', opn_cowboy_bridge,
                [
                    {delegation, true},
                    {handler_module, opn_redirect_handler},
                    {handler_opts, []}
                ]}
        ]}
    ],

    {ok, _} = cowboy:start_http(?http_redirector_listener, RedirectNbAcceptors,
        [
            {port, RedirectPort}
        ],
        [
            {env, [{dispatch, cowboy_router:compile(RedirectDispatch)}]},
            {max_keepalive, 1},
            {timeout, Timeout}
        ]).


%% start_rest_listener/0
%% ====================================================================
%% @doc Starts a cowboy listener for REST requests.
%% @end
-spec start_rest_listener() -> ok | no_return().
%% ====================================================================
start_rest_listener() ->
    {ok, NbAcceptors} = application:get_env(?APP_Name, control_panel_number_of_acceptors),
    {ok, Timeout} = application:get_env(?APP_Name, control_panel_socket_timeout),

    {ok, Cert} = application:get_env(?APP_Name, web_ssl_cert_path),

    % Get REST port from env and setup dispatch opts for cowboy
    {ok, RestPort} = application:get_env(?APP_Name, rest_port),

    LocalPort = oneproxy:get_local_port(RestPort),
    Pid = spawn_link(fun() -> oneproxy:start(RestPort, LocalPort, Cert, verify_peer) end),
    register(oneproxy_rest, Pid),

    RestDispatch = [
        {'_', [
            {"/rest/:version/[...]", opn_cowboy_bridge,
                [
                    {delegation, true},
                    {handler_module, rest_handler},
                    {handler_opts, []}
                ]},
            {"/cdmi/[...]", opn_cowboy_bridge,
                [
                    {delegation, true},
                    {handler_module, cdmi_handler},
                    {handler_opts, []}
                ]}
        ]}
    ],

    % Start the listener for REST handler
    {ok, _} = cowboy:start_http(?rest_listener, NbAcceptors,
        [
            {ip, {127, 0, 0, 1}},
            {port, LocalPort}
        ],
        [
            {env, [{dispatch, cowboy_router:compile(RestDispatch)}]},
            {max_keepalive, 1},
            {timeout, Timeout}
        ]),

    ok.


%% static_dispatches/2
%% ====================================================================
%% @doc Generates static file routing rules for cowboy.
%% @end
-spec static_dispatches(DocRoot :: string(), StaticPaths :: [string()]) -> [term()].
%% ====================================================================
static_dispatches(DocRoot, StaticPaths) ->
    _StaticDispatches = lists:map(fun(Dir) ->
        {Dir ++ "[...]", opn_cowboy_bridge,
            [
                {delegation, true},
                {handler_module, cowboy_static},
                {handler_opts, {dir, DocRoot ++ Dir}}
            ]}
    end, StaticPaths).
