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

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0, stop/0]).

%% gen_event callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% This record is used by ccm (it contains its state). It describes
%% nodes, dispatchers and workers in cluster. It also contains reference
%% to process used to monitor if nodes are alive.
-record(cm_state, {
    nodes = [] :: [Node :: node()],
    uninitialized_nodes = [] :: [Node :: node()],
    workers = [] :: [{Node :: node(), Module :: module(), Args :: term()}],
    state_num = 1 :: integer()
}).

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
    gen_server:cast({global, ?CCM}, stop).

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

handle_call(get_nodes_and_state_num, _From, State) ->
    {reply, {State#cm_state.nodes, State#cm_state.state_num}, State};

handle_call(healthcheck, _From, State) ->
    Ans = healthcheck(State),
    {reply, Ans, State};

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
handle_cast({heartbeat, Node}, State) ->
    NewState = heartbeat(State, Node),
    {noreply, NewState};

handle_cast({init_ok, Node}, State) ->
    NewState = init_ok(State, Node),
    {noreply, NewState};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
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
%% Receive heartbeat from node_manager
%% @end
%%--------------------------------------------------------------------
-spec heartbeat(State :: #cm_state{}, SenderNode :: node()) ->
    NewState :: #cm_state{}.
heartbeat(State = #cm_state{nodes = Nodes, uninitialized_nodes = InitNodes}, SenderNode) ->
    ?debug("Heartbeat from node: ~p", [SenderNode]),
    case lists:member(SenderNode, Nodes) or lists:member(SenderNode, InitNodes) of
        true ->
            gen_server:cast({?NODE_MANAGER_NAME, SenderNode}, {heartbeat_ok, State#cm_state.state_num}),
            State;
        false ->
            ?info("New node: ~p", [SenderNode]),
            try
                pong = net_adm:ping(SenderNode),
                erlang:monitor_node(SenderNode, true),
                NewInitNodes = [SenderNode | lists:delete(SenderNode, InitNodes)],
                gen_server:cast({?NODE_MANAGER_NAME, SenderNode}, {heartbeat_ok, State#cm_state.state_num}),
                State#cm_state{uninitialized_nodes = NewInitNodes}
            catch
                _:Error ->
                    ?warning_stacktrace("Checking node ~p, in ccm failed with error: ~p", [SenderNode, Error]),
                    State
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Receive acknowledgement of successfull worker initialization on node
%% @end
%%--------------------------------------------------------------------
-spec init_ok(State :: #cm_state{}, SenderNode :: node()) -> #cm_state{}.
init_ok(State = #cm_state{nodes = Nodes, uninitialized_nodes = InitNodes, state_num = StateNum}, SenderNode) ->
    NewInitNodes = lists:delete(SenderNode, InitNodes),
    NewNodes = [SenderNode | lists:delete(SenderNode, Nodes)],
    NewStateNum = StateNum + 1,
    update_node_managers(NewNodes, NewStateNum),
    State#cm_state{nodes = NewNodes, uninitialized_nodes = NewInitNodes, state_num = NewStateNum}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends information to all nodes, that the state of cluster has changed and
%% they are supposed to synchronize
%% @end
%%--------------------------------------------------------------------
-spec update_node_managers(State :: #cm_state{}, SenderNode :: node()) -> #cm_state{}.
update_node_managers(Nodes, NewStateNum) ->
    UpdateNode = fun(Node) ->
        gen_server:cast({?NODE_MANAGER_NAME, Node}, {update_state, NewStateNum})
    end,
    lists:foreach(UpdateNode, Nodes).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Clears information about workers on node that is down.
%% @end
%%--------------------------------------------------------------------
-spec node_down(Node :: atom(), State :: #cm_state{}) -> #cm_state{}.
node_down(Node, State = #cm_state{nodes = Nodes, uninitialized_nodes = InitNodes, state_num = StateNum}) ->
    ?error("Node down: ~p", [Node]),
    NewNodes = Nodes -- [Node],
    NewInitNodes = InitNodes -- [Node],
    NewStateNum = StateNum + 1,
    update_node_managers(NewNodes, NewStateNum),
    State#cm_state{nodes = NewNodes, uninitialized_nodes = NewInitNodes, state_num = NewStateNum}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles healthcheck request
%% @end
%%--------------------------------------------------------------------
-spec healthcheck(State :: #cm_state{}) ->
    {ok, {Nodes, StateNum}} | {error, invalid_worker_num}.
healthcheck(#cm_state{nodes = Nodes, state_num = StateNum}) ->
    case application:get_env(?APP_NAME, worker_num) of
        {ok, N} when N =:= length(Nodes)->
            {ok, {Nodes, StateNum}};
        {ok, undefined} ->
            {ok, {Nodes, StateNum}};
        _ -> {error, invalid_worker_num}
    end.