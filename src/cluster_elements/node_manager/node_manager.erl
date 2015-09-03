%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @author Tomasz Lichon
%%% @author Lukasz Opiola
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is a gen_server that coordinates the
%%% life cycle of node. It starts/stops appropriate services and communicates
%%% with ccm.
%%% @end
%%%-------------------------------------------------------------------
-module(node_manager).
-author("Michal Wrzeszcz").
-author("Tomasz Lichon").
-author("Lukasz Opiola").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules_and_args.hrl").
-include("cluster_elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_definitions.hrl").

%% This record is used by node_manager (it contains its state).
%% It hold the status of connection to ccm.
-record(state, {
    node_ip = {127, 0, 0, 1} :: {A :: byte(), B :: byte(), C :: byte(), D :: byte()},
    ccm_con_status = not_connected :: not_connected | connected | registered,
    monitoring_state = undefined :: monitoring:node_monitoring_state(),
    cache_control = true,
    last_cache_cleaning = {0,0,0}
}).


%% API
-export([start_link/0, stop/0, get_ip_address/0, refresh_ip_address/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts node manager
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> Result when
    Result :: {ok, Pid}
    | ignore
    | {error, Error},
    Pid :: pid(),
    Error :: {already_started, Pid} | term().
start_link() ->
    gen_server:start_link({local, ?NODE_MANAGER_NAME}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Stops node manager
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok.
stop() ->
    gen_server:cast(?NODE_MANAGER_NAME, stop).


%%--------------------------------------------------------------------
%% @doc
%% Returns node's IP address.
%% @end
%%--------------------------------------------------------------------
get_ip_address() ->
    gen_server:call(?NODE_MANAGER_NAME, get_ip_address).


%%--------------------------------------------------------------------
%% @doc
%% Tries to contact GR and refresh node's IP Address.
%% @end
%%--------------------------------------------------------------------
refresh_ip_address() ->
    gen_server:cast(?NODE_MANAGER_NAME, refresh_ip_address).

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
init([]) ->
    process_flag(trap_exit,true),
    try
        ensure_correct_hostname(),
        listener_starter:start_protocol_listener(),
        listener_starter:start_gui_listener(),
        listener_starter:start_rest_listener(),
        listener_starter:start_redirector_listener(),
        listener_starter:start_dns_listeners(),
        gen_server:cast(self(), connect_to_ccm),
        next_mem_check(),

        %% Load NIFs
        ok = helpers_nif:init(),

        NodeIP = check_node_ip_address(),
        MonitoringState = monitoring:start(NodeIP),
        {ok, #state{node_ip = NodeIP,
            ccm_con_status = not_connected,
            monitoring_state = MonitoringState}}
    catch
        _:Error ->
            ?error_stacktrace("Cannot start node_manager: ~p", [Error]),
            {stop, cannot_start_node_manager}
    end.

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
    Reply :: nagios_handler:healthcheck_response() | term(),
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity,
    Reason :: term().

handle_call(healthcheck, _From, State = #state{ccm_con_status = ConnStatus}) ->
    Reply = case ConnStatus of
                registered -> ok;
                _ -> out_of_sync
            end,
    {reply, Reply, State};

handle_call(get_ip_address, _From, State = #state{node_ip = IPAddress}) ->
    {reply, IPAddress, State};

% only for tests
handle_call(check_mem_synch, _From, State) ->
    Ans = case monitoring:get_memory_stats() of
        [{<<"mem">>, MemUsage}] ->
            case caches_controller:should_clear_cache(MemUsage) of
                true ->
                    free_memory(MemUsage);
                _ ->
                    ok
            end;
        _ ->
            cannot_check_mem_usage
    end,
    {reply, Ans, State};

% only for tests
handle_call(clear_mem_synch, _From, State) ->
    caches_controller:delete_old_keys(locally_cached, 0),
    caches_controller:delete_old_keys(globally_cached, 0),
    {reply, ok, State};

handle_call(disable_cache_control, _From, State) ->
    {reply, ok, State#state{cache_control = false}};

handle_call(enable_cache_control, _From, State) ->
    {reply, ok, State#state{cache_control = true}};

handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
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
handle_cast(connect_to_ccm, State) ->
    NewState = connect_to_ccm(State),
    {noreply, NewState};

handle_cast(ccm_conn_ack, State) ->
    NewState = ccm_conn_ack(State),
    {noreply, NewState};

handle_cast(check_mem, #state{monitoring_state = MonState, cache_control = CacheControl,
    last_cache_cleaning = Last} = State) when CacheControl =:= true ->
    MemUsage = monitoring:mem_usage(MonState),
    % Check if memory cleaning of oldest docs should be started
    % even when memory utilization is low (e.g. once a day)
    NewState = case caches_controller:should_clear_cache(MemUsage) of
                   true ->
                       spawn(fun() -> free_memory(MemUsage) end),
                       State#state{last_cache_cleaning = os:timestamp()};
                   _ ->
                       Now = os:timestamp(),
                       {ok, CleaningPeriod} = application:get_env(?APP_NAME, clear_cache_max_period_ms),
                       case timer:now_diff(Now, Last) >= 1000000*CleaningPeriod of
                           true ->
                               spawn(fun() -> free_memory() end),
                               State#state{last_cache_cleaning = Now};
                           _ ->
                               State
                       end
               end,
    next_mem_check(),
    {noreply, NewState};

handle_cast(check_mem, State) ->
    next_mem_check(),
    {noreply, State};

handle_cast(do_heartbeat, State) ->
    NewState = do_heartbeat(State),
    {noreply, NewState};

handle_cast({update_lb_advices, Advices}, State) ->
    NewState = update_lb_advices(State, Advices),
    {noreply, NewState};

handle_cast(refresh_ip_address, #state{monitoring_state = MonState} = State) ->
    NodeIP = check_node_ip_address(),
    NewMonState = monitoring:refresh_ip_address(NodeIP, MonState),
    {noreply, State#state{node_ip = NodeIP, monitoring_state = NewMonState}};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {reply, wrong_request, State}.

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
    gen_server:cast(?NODE_MANAGER_NAME, Msg),
    {noreply, State};

handle_info({nodedown, Node}, State) ->
    {ok, CCMNodes} = application:get_env(?APP_NAME, ccm_nodes),
    case lists:member(Node, CCMNodes) of
        false ->
            ?warning("Node manager received unexpected nodedown msg: ~p", [{nodedown, Node}]);
        true ->
            ok
            % TODO maybe node_manager should be restarted along with all workers to
            % avoid desynchronization of modules between nodes.
%%             ?error("Connection to CCM lost, restarting node"),
%%             throw(connection_to_ccm_lost)
    end,
    {noreply, State};

handle_info(_Request, State) ->
    ?log_bad_request(_Request),
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
    ?info("Shutting down ~p due to ~p", [?MODULE, _Reason]),
    listener_starter:stop_listeners().

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
%% Connects with ccm and tells that the node is alive.
%% First it establishes network connection, next sends message to ccm.
%% @end
%%--------------------------------------------------------------------
-spec connect_to_ccm(State :: #state{}) -> #state{}.
connect_to_ccm(State = #state{ccm_con_status = registered}) ->
    % Already registered, do nothing
    State;
connect_to_ccm(State = #state{ccm_con_status = connected}) ->
    % Connected, but not registered (workers did not start), check again in some time
    {ok, Interval} = application:get_env(?APP_NAME, ccm_connection_retry_period),
    gen_server:cast({global, ?CCM}, {ccm_conn_req, node()}),
    erlang:send_after(Interval, self(), {timer, connect_to_ccm}),
    State;
connect_to_ccm(State = #state{ccm_con_status = not_connected}) ->
    % Not connected to CCM, try and automatically schedule the next try
    {ok, CcmNodes} = application:get_env(?APP_NAME, ccm_nodes),
    {ok, Interval} = application:get_env(?APP_NAME, ccm_connection_retry_period),
    erlang:send_after(Interval, self(), {timer, connect_to_ccm}),
    case (catch init_net_connection(CcmNodes)) of
        ok ->
            ?info("Initializing connection to CCM"),
            gen_server:cast({global, ?CCM}, {ccm_conn_req, node()}),
            State#state{ccm_con_status = connected};
        Err ->
            ?debug("No connection with CCM: ~p, retrying in ~p ms", [Err, Interval]),
            State#state{ccm_con_status = not_connected}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves information about ccm connection when ccm answers to its request
%% @end
%%--------------------------------------------------------------------
-spec ccm_conn_ack(State :: term()) -> #state{}.
ccm_conn_ack(State = #state{ccm_con_status = connected}) ->
    ?info("Successfully connected to CCM"),
    init_node(),
    ?info("Node initialized"),
    gen_server:cast({global, ?CCM}, {init_ok, node()}),
    {ok, Interval} = application:get_env(?APP_NAME, heartbeat_interval),
    erlang:send_after(Interval, self(), {timer, do_heartbeat}),
    State#state{ccm_con_status = registered};
ccm_conn_ack(State) ->
    % Already registered or not connected, do nothing
    ?warning("node_manager received redundant ccm_conn_ack"),
    State.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Performs calls to CCM with heartbeat. The heartbeat message consists of
%% current monitoring data. The data is updated directly before sending.
%% The CCM will perform an 'update_lb_advices' cast perodically, using
%% newest node states from node managers for calculations.
%% @end
%%--------------------------------------------------------------------
-spec do_heartbeat(State :: #state{}) -> #state{}.
do_heartbeat(#state{ccm_con_status = registered, monitoring_state = MonState} = State) ->
    {ok, Interval} = application:get_env(?APP_NAME, heartbeat_interval),
    erlang:send_after(Interval, self(), {timer, do_heartbeat}),
    NewMonState = monitoring:update(MonState),
    NodeState = monitoring:get_node_state(NewMonState),
    ?debug("Sending heartbeat to CCM"),
    gen_server:cast({global, ?CCM}, {heartbeat, NodeState}),
    State#state{monitoring_state = NewMonState};

% Stop heartbeat if node_manager is not registered in CCM
do_heartbeat(State) ->
    State.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Receives lb advices update from CCM and follows it to DNS worker and dispatcher.
%% @end
%%--------------------------------------------------------------------
-spec update_lb_advices(State :: #state{}, LBAdvices) -> #state{} when
    LBAdvices :: {load_balancing:dns_lb_advice(), load_balancing:dispatcher_lb_advice()}.
update_lb_advices(State, {DNSAdvice, DispatcherAdvice}) ->
    gen_server:cast(?DISPATCHER_NAME, {update_lb_advice, DispatcherAdvice}),
    worker_proxy:call({dns_worker, node()}, {update_lb_advice, DNSAdvice}),
    State.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes network connection with cluster that contains nodes
%% given in argument.
%% @end
%%--------------------------------------------------------------------
-spec init_net_connection(Nodes :: list()) -> ok | {error, not_connected}.
init_net_connection([]) ->
    {error, not_connected};
init_net_connection([Node | Nodes]) ->
    case net_adm:ping(Node) of
        pong ->
            global:sync(),
            case global:whereis_name(?CCM) of
                undefined ->
                    ?error("Connection to node ~p global_synch error", [Node]),
                    rpc:eval_everywhere(erlang, disconnect_node, [node()]),
                    {error, global_synch};
                _ ->
                    ?debug("Connection to node ~p initialized", [Node]),
                    erlang:monitor_node(Node, true),
                    ok
            end;
        pang ->
            ?error("Cannot connect to node ~p", [Node]),
            init_net_connection(Nodes)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Init node as worker of oneprovider cluster
%% @end
%%--------------------------------------------------------------------
-spec init_node() -> ok.
init_node() ->
    {ok, NodeToSync} = gen_server:call({global, ?CCM}, get_node_to_sync),
    ok = datastore:ensure_state_loaded(NodeToSync),
    ?info("Datastore synchronized"),
    init_workers().

%%--------------------------------------------------------------------
%% @doc
%% Starts all workers defined in "modules_and_args.hrl" on node, and notifies
%% ccm about successfull init
%% @end
%%--------------------------------------------------------------------
-spec init_workers() -> ok.
init_workers() ->
    lists:foreach(fun({Module, Args}) -> ok = start_worker(Module, Args) end, ?MODULES_WITH_ARGS),
    ?info("All workers started"),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Starts worker node with dedicated supervisor as brother. Both entities
%% are started under MAIN_WORKER_SUPERVISOR supervision.
%% @end
%%--------------------------------------------------------------------
-spec start_worker(Module :: atom(), Args :: term()) -> ok | {error, term()}.
start_worker(Module, Args) ->
    try
        {ok, LoadMemorySize} = application:get_env(?APP_NAME, worker_load_memory_size),
        WorkerSupervisorName = ?WORKER_HOST_SUPERVISOR_NAME(Module),
        {ok, _} = supervisor:start_child(
            ?MAIN_WORKER_SUPERVISOR_NAME,
            {Module, {worker_host, start_link, [Module, Args, LoadMemorySize]}, transient, 5000, worker, [worker_host]}
        ),
        {ok, _} = supervisor:start_child(
            ?MAIN_WORKER_SUPERVISOR_NAME,
            {WorkerSupervisorName, {worker_host_sup, start_link, [WorkerSupervisorName, Args]}, transient, infinity, supervisor, [worker_host_sup]}
        ),
        ?info("Worker: ~s started", [Module])
    catch
        _:Error ->
            ?error_stacktrace("Error: ~p during start of worker: ~s", [Error, Module]),
            {error, Error}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks IP address of this node by asking GR. If the check cannot be performed,
%% it assumes a 127.0.0.1 address and logs an alert.
%% @end
%%--------------------------------------------------------------------
-spec check_node_ip_address() -> IPV4Addr :: {A :: byte(), B :: byte(), C :: byte(), D :: byte()}.
check_node_ip_address() ->
    try
        {ok, IPBin} = gr_providers:check_ip_address(provider),
        {ok, IP} = inet_parse:ipv4_address(binary_to_list(IPBin)),
        IP
    catch T:M ->
        ?alert_stacktrace("Cannot check external IP of node, defaulting to 127.0.0.1 - ~p:~p", [T, M]),
        {127, 0, 0, 1}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Clears memory caches.
%% @end
%%--------------------------------------------------------------------
-spec free_memory(NodeMem :: number()) -> ok | mem_usage_too_high | cannot_check_mem_usage | {error, term()}.
free_memory(NodeMem) ->
    try
        AvgMem = gen_server:call({global, ?CCM}, get_avg_mem_usage),
        ClearingOrder = case NodeMem >= AvgMem of
            true ->
                [{false, locally_cached}, {false, globally_cached}, {true, locally_cached}, {true, globally_cached}];
            _ ->
                [{false, globally_cached}, {false, locally_cached}, {true, globally_cached}, {true, locally_cached}]
        end,
        ?info("Clearing memory in order: ~p", [ClearingOrder]),
        lists:foldl(fun
            ({_Aggressive, _StoreType}, ok) ->
                ok;
            ({Aggressive, StoreType}, _) ->
                Ans = caches_controller:clear_cache(NodeMem, Aggressive, StoreType),
                case Ans of
                    mem_usage_too_high ->
                        ?warning("Not able to free enough memory clearing cache ~p with param ~p", [StoreType, Aggressive]);
                    _ ->
                        ok
                end,
                Ans
        end, start, ClearingOrder)
    catch
        E1:E2 ->
            ?error_stacktrace("Error during caches cleaning ~p:~p", [E1, E2]),
            {error, E2}
    end.

free_memory() ->
    try
        ClearingOrder = [{false, globally_cached}, {false, locally_cached}],
        lists:foreach(fun
            ({Aggressive, StoreType}) ->
                caches_controller:clear_cache(100, Aggressive, StoreType)
        end, ClearingOrder)
    catch
        E1:E2 ->
            ?error_stacktrace("Error during caches cleaning ~p:~p", [E1, E2]),
            {error, E2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Plans next memory checking.
%% @end
%%--------------------------------------------------------------------
-spec next_mem_check() -> TimerRef :: reference().
next_mem_check() ->
    {ok, IntervalMin} = application:get_env(?APP_NAME, check_mem_interval_minutes),
    Interval = timer:minutes(IntervalMin),
    % random to reduce probability that two nodes clear memory simultanosly
    erlang:send_after(crypto:rand_uniform(round(0.8 * Interval), round(1.2 * Interval)), self(), {timer, check_mem}).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Makes sure node hostname belongs to provider domain.
%% @end
%%--------------------------------------------------------------------
-spec ensure_correct_hostname() -> ok | no_return().
ensure_correct_hostname() ->
    Hostname = oneprovider:get_node_hostname(),
    Domain = oneprovider:get_provider_domain(),
    case string:join(tl(string:tokens(Hostname, ".")), ".") of
        Domain ->
            ok;
        _ ->
            ?error("Node hostname must be in provider domain. Check env conf. "
            "Current configuration:~nHostname: ~p~nDomain: ~p",
                [Hostname, Domain]),
            throw(wrong_hostname)
    end.

