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

-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% This record is used by node_manager (it contains its state).
%% It describes node type (ccm or worker) and status of connection
%% with ccm (connected or not_connected).
-record(node_state, {
    node_type = worker :: worker | ccm,
    ccm_con_status = not_connected :: not_connected | connected,
    state_num = 0 :: integer(),
    dispatcher_state = 0 :: integer()
}).

%% API
-export([start_link/1, stop/0]).

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
-spec start_link(Type) -> Result when
    Type :: worker | ccm,
    Result :: {ok, Pid}
    | ignore
    | {error, Error},
    Pid :: pid(),
    Error :: {already_started, Pid} | term().
start_link(Type) ->
    gen_server:start_link({local, ?NODE_MANAGER_NAME}, ?MODULE, [Type], []).

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
init([worker]) ->
    try
        listener_starter:start_dispatcher_listener(),
        listener_starter:start_gui_listener(),
        listener_starter:start_rest_listener(),
        listener_starter:start_redirector_listener(),
        listener_starter:start_dns_listeners(),
        gen_server:cast(self(), do_heartbeat),
        {ok, #node_state{node_type = worker, ccm_con_status = not_connected}}
    catch
        _:Error ->
            ?error_stacktrace("Cannot initialize listeners: ~p", [Error]),
            {stop, cannot_initialize_listeners}
    end;
init([ccm]) ->
    gen_server:cast(self(), do_heartbeat),
    {ok, #node_state{node_type = ccm, ccm_con_status = not_connected}};
init([_Type]) ->
    {stop, wrong_type}.

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
handle_call(get_node_type, _From, State = #node_state{node_type = NodeType}) ->
    {reply, NodeType, State};

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
    NewState = heartbeat_ok(StateNum, State),
    {noreply, NewState};

handle_cast({update_state, NewStateNum}, State) ->
    ?info("Node manager state updated, state num: ~p", [NewStateNum]),
    {noreply, State#node_state{state_num = NewStateNum}};

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
do_heartbeat(State = #node_state{ccm_con_status = connected}) ->
    {ok, Interval} = application:get_env(?APP_NAME, heartbeat_success_interval),
    gen_server:cast({global, ?CCM}, {heartbeat, node()}),
    erlang:send_after(Interval, self(), {timer, do_heartbeat}),
    State#node_state{ccm_con_status = connected};
do_heartbeat(State = #node_state{ccm_con_status = not_connected}) ->
    {ok, CcmNodes} = application:get_env(?APP_NAME, ccm_nodes),
    case (catch init_net_connection(CcmNodes)) of
        ok ->
            {ok, Interval} = application:get_env(?APP_NAME, heartbeat_success_interval),
            gen_server:cast({global, ?CCM}, {heartbeat, node()}),
            erlang:send_after(Interval * 1000, self(), {timer, do_heartbeat}),
            State#node_state{ccm_con_status = connected};
        Err ->
            {ok, Interval} = application:get_env(?APP_NAME, heartbeat_fail_interval),
            ?debug("No connection with CCM: ~p, retrying in ~p s", [Err, Interval]),
            erlang:send_after(Interval * 1000, self(), {timer, do_heartbeat}),
            State#node_state{ccm_con_status = not_connected}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves information about ccm connection when ccm answers to its request
%% @end
%%--------------------------------------------------------------------
-spec heartbeat_ok(NewStateNum :: integer(), State :: term()) -> #node_state{}.
heartbeat_ok(NewStateNum, State = #node_state{state_num = NewStateNum, dispatcher_state = NewStateNum}) ->
    ?debug("heartbeat on node: ~p: answered, new state_num: ~p, new callback_num: ~p", [node(), NewStateNum]),
    State;
heartbeat_ok(NewStateNum, State) ->
    ?debug("heartbeat on node: ~p: answered, new state_num: ~p, new callback_num: ~p", [node(), NewStateNum]),
    update_dispatcher(NewStateNum),
    State#node_state{state_num = NewStateNum}.

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
            erlang:monitor_node(Node, true),
            global:sync(),
            ?debug("Connection to node ~p initialized", [Node]),
            ok;
        pang ->
            ?error("Cannot connect to node ~p", [Node]),
            init_net_connection(Nodes)
    end.