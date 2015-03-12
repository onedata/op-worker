%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @author Tomasz Lichon
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is a gen_server that coordinates the
%%% life cycle of node. It starts/stops appropriate services (according
%%% to node type) and communicates with ccm (if node works as worker).
%%%
%%% Node can be ccm or worker. However, worker_hosts can be also
%%% started at ccm nodes.
%%% @end
%%%-------------------------------------------------------------------
-module(node_manager).
-author("Michal Wrzeszcz").
-author("Tomasz Lichon").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("modules_and_args.hrl").
-include("cluster_elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/logging.hrl").

%% This record is used by node_manager (it contains its state).
%% It describes node type (ccm or worker) and status of connection
%% with ccm (connected or not_connected).
-record(node_state, {
    ccm_con_status = not_connected :: not_connected | connected | registered,
    state_num = 0 :: integer(),
    dispatcher_state = 0 :: integer()
}).

%% API
-export([start_link/0, stop/0]).

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
    Type :: worker | ccm,
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
init(_Opts) ->
    try
        listener_starter:start_protocol_listener(),
        listener_starter:start_gui_listener(),
        listener_starter:start_rest_listener(),
        listener_starter:start_redirector_listener(),
        listener_starter:start_dns_listeners(),
        gen_server:cast(self(), do_heartbeat),
        {ok, #node_state{ccm_con_status = not_connected}}
    catch
        _:Error ->
            ?error_stacktrace("Cannot initialize listeners: ~p", [Error]),
            {stop, cannot_initialize_listeners}
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
    Reply :: nagios_handler:healthcheck_reponse() | term(),
    NewState :: term(),
    Timeout :: non_neg_integer() | infinity,
    Reason :: term().
handle_call(get_state_num, _From, State = #node_state{state_num = StateNum}) ->
    {reply, StateNum, State};

handle_call(healthcheck, _From, State = #node_state{state_num = StateNum}) ->
    {reply, {ok, StateNum}, State};

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
handle_cast(do_heartbeat, State) ->
    NewState = do_heartbeat(State),
    {noreply, NewState};

handle_cast({heartbeat_ok, StateNum}, State) ->
    NewState = on_ccm_state_change(StateNum, State),
    {noreply, NewState};

handle_cast({update_state, NewStateNum}, State) ->
    ?info("Node manager state updated, state num: ~p", [NewStateNum]),
    NewState = on_ccm_state_change(NewStateNum, State),
    {noreply, NewState};

handle_cast({dispatcher_up_to_date, DispState}, State) ->
    NewState = State#node_state{dispatcher_state = DispState},
    {noreply, NewState};

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

handle_info({nodedown, _Node}, State) ->
    ?warning("Connection to CCM lost, node~p", [node()]),
    {noreply, State#node_state{ccm_con_status = not_connected}};

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
-spec do_heartbeat(State :: #node_state{}) -> #node_state{}.
do_heartbeat(State = #node_state{ccm_con_status = registered}) ->
    {ok, Interval} = application:get_env(?APP_NAME, heartbeat_success_interval_seconds),
    gen_server:cast({global, ?CCM}, {heartbeat, node()}),
    erlang:send_after(timer:seconds(Interval), self(), {timer, do_heartbeat}),
    State;
do_heartbeat(State = #node_state{ccm_con_status = connected}) ->
    {ok, Interval} = application:get_env(?APP_NAME, heartbeat_fail_interval_seconds),
    gen_server:cast({global, ?CCM}, {heartbeat, node()}),
    erlang:send_after(timer:seconds(Interval), self(), {timer, do_heartbeat}),
    State;
do_heartbeat(State = #node_state{ccm_con_status = not_connected}) ->
    {ok, CcmNodes} = application:get_env(?APP_NAME, ccm_nodes),
    {ok, Interval} = application:get_env(?APP_NAME, heartbeat_fail_interval_seconds),
    erlang:send_after(timer:seconds(Interval), self(), {timer, do_heartbeat}),
    case (catch init_net_connection(CcmNodes)) of
        ok ->
            gen_server:cast({global, ?CCM}, {heartbeat, node()}),

            %% Initialize datastore
            datastore:ensure_state_loaded(),

            State#node_state{ccm_con_status = connected};
        Err ->
            ?debug("No connection with CCM: ~p, retrying in ~p s", [Err, Interval]),
            State#node_state{ccm_con_status = not_connected}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves information about ccm connection when ccm answers to its request
%% @end
%%--------------------------------------------------------------------
-spec on_ccm_state_change(NewStateNum :: integer(), State :: term()) -> #node_state{}.
on_ccm_state_change(NewStateNum, State = #node_state{ccm_con_status = connected}) ->
    ?debug("Node ~p successfully connected to ccm, starting workers...", [node(), NewStateNum]),
    init_workers(),
    update_dispatcher_and_dns(NewStateNum),
    State#node_state{state_num = NewStateNum, ccm_con_status = registered};
on_ccm_state_change(NewStateNum, State = #node_state{state_num = NewStateNum, dispatcher_state = NewStateNum}) ->
    ?debug("heartbeat on node: ~p: answered, new state_num: ~p", [node(), NewStateNum]),
    State#node_state{ccm_con_status = registered};
on_ccm_state_change(NewStateNum, State) ->
    ?debug("heartbeat on node: ~p: answered, new state_num: ~p", [node(), NewStateNum]),
    update_dispatcher_and_dns(NewStateNum),
    State#node_state{state_num = NewStateNum, ccm_con_status = registered}.

update_dispatcher_and_dns(NewStateNum) ->
    update_dispatcher(NewStateNum),
    update_dns().

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tells dispatcher that cluster state has changed.
%% @end
%%--------------------------------------------------------------------
-spec update_dispatcher(NewStateNum :: integer()) -> atom().
update_dispatcher(NewStateNum) ->
    ?debug("Message sent to update dispatcher, state num: ~p", [NewStateNum]),
    gen_server:cast(?DISPATCHER_NAME, {check_state, NewStateNum}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates dnses' states.
%% @end
%%--------------------------------------------------------------------
-spec update_dns() -> ok | {error, ip_resolving}.
update_dns() ->
    %prepare worker IPs with loads info
    Nodes = gen_server:call({global, ?CCM}, get_nodes),
    NodesIpToLoad = [{node_to_ip(Node), node_monitoring:node_load(Node)} || Node <- Nodes],
    FilteredNodesIpToLoad = [{IP, Load} || {IP, Load} <- NodesIpToLoad, IP =/= unknownaddress],

    %prepare modules with their nodes and loads info
    ModuleToNodeList = [{Module, NodesIpToLoad} || Module <- ?MODULES],

    case FilteredNodesIpToLoad of
        [] -> ok;
        _ ->
            % prepare average load
            LoadAverage = utils:average([Load || {_, Load} <- FilteredNodesIpToLoad]),

            UpdateInfo = {update_state, ModuleToNodeList, FilteredNodesIpToLoad, LoadAverage},
            ?debug("updating dns, update message: ~p", [UpdateInfo]),
            gen_server:cast(dns_worker, #worker_request{req = UpdateInfo})
    end,

    case length(FilteredNodesIpToLoad) == length(Nodes) of
        true ->
            ok;
        false ->
            {error, ip_resolving}
    end.

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
%% Starts all workers defined in "modules_and_args.hrl" on node, and notifies
%% ccm about successfull init
%% @end
%%--------------------------------------------------------------------
-spec init_workers() -> ok.
init_workers() ->
    lists:foreach(fun({Module, Args}) -> ok = start_worker(Module, Args) end, ?MODULES_WITH_ARGS),
    gen_server:cast({global, ?CCM}, {init_ok, node()}).

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
%% Resolve ipv4 address of node.
%% @end
%%--------------------------------------------------------------------
-spec node_to_ip(Node :: atom()) -> inet:ip4_address() | unknownaddress.
node_to_ip(Node) ->
    StrNode = atom_to_list(Node),
    AddressWith@ = lists:dropwhile(fun(Char) -> Char =/= $@ end, StrNode),
    Address = lists:dropwhile(fun(Char) -> Char =:= $@ end, AddressWith@),
    case inet:getaddr(Address, inet) of
        {ok, Ip} -> Ip;
        {error, Error} ->
            ?error("Cannot resolve ip address for node ~p, error: ~p", [Node, Error]),
            unknownaddress
    end.