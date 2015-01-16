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
-include("supervision_macros.hrl").
-include("cluster_elements/node_manager/node_manager.hrl").
-include("cluster_elements/node_manager/node_manager_listeners.hrl").
-include_lib("ctool/include/logging.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/1, stop/0]).
-export([check_vsn/0]).

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
start_link(Type) ->
    gen_server:start_link({local, ?Node_Manager_Name}, ?MODULE, [Type], []).

%% stop/0
%% ====================================================================
%% @doc Stops the server
-spec stop() -> ok.
stop() ->
    gen_server:cast(?Node_Manager_Name, stop).


%% check_vsn/0
%% ====================================================================
%% @doc Checks application version
-spec check_vsn() -> Result when
    Result :: term().
%% ====================================================================
check_vsn() ->
    check_vsn(application:which_applications()).

%% ====================================================================
%% gen_server callbacks functions
%% ====================================================================

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
            node_manager_listener_starter:start_dispatcher_listener(),
            erlang:send_after(0, self(), {timer, init_listeners});
        false -> ok
    end,
    erlang:send_after(10, self(), {timer, do_heart_beat}),
    {ok, #node_state{node_type = Type, ccm_con_status = not_connected}};
init([test_worker]) ->
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

handle_call(get_ccm_connection_status, _From, State) ->
    {reply, State#node_state.ccm_con_status, State};

handle_call(get_state_num, _From, State) ->
    Reply = State#node_state.state_num,
    {reply, Reply, State};

handle_call(check, _From, State) ->
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    ?warning("Wrong node_manager call: ~p", [_Request]),
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

handle_cast({heart_beat_ok, StateNum}, State) ->
    {noreply, heart_beat_response(StateNum, State)};

handle_cast(reset_ccm_connection, State) ->
    {noreply, heart_beat(not_connected, State)};

handle_cast({dispatcher_updated, DispState}, State) ->
    NewState = State#node_state{dispatcher_state = DispState},
    {noreply, NewState};

handle_cast(init_listeners, State) ->
    try
        node_manager_listener_starter:start_gui_listener(),
        node_manager_listener_starter:start_rest_listener(),
        node_manager_listener_starter:start_redirector_listener(),
        node_manager_listener_starter:start_dns_listeners()
    catch
        _:Error  ->
            ?error_stacktrace("Cannot initialize listeners: ~p", [Error])
    end,
    {noreply, State};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Msg, State) ->
    ?warning("Wrong node_manager cast: ~p", [_Msg]),
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
    ?warning("Connection to CCM lost, node~p", [node()]),
    {noreply, State#node_state{ccm_con_status = not_connected}};

handle_info(_Info, State) ->
    ?warning("Wrong node_manager info: ~p", [_Info]),
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
heart_beat(Conn_status, State) ->
    New_conn_status = case Conn_status of
                          not_connected ->
                              {ok, CCM_Nodes} = application:get_env(?APP_Name, ccm_nodes),
                              case catch init_net_connection(CCM_Nodes) of
                                  ok -> connected;
                                  _ -> not_connected
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

    ?debug("Heart beat on node: ~p: sent; connection: ~p, old conn_status: ~p,  state_num: ~p, disp dispatcher_state: ~p",
        [node(), New_conn_status, Conn_status, State#node_state.state_num, State#node_state.dispatcher_state]),
    State#node_state{ccm_con_status = New_conn_status}.

%% heart_beat_response/2
%% ====================================================================
%% @doc Saves information about ccm connection when ccm answers to its request
-spec heart_beat_response(New_state_num :: integer(), State :: term()) -> NewStatus when
    NewStatus :: term().
%% ====================================================================
heart_beat_response(New_state_num, State) when (New_state_num == State#node_state.state_num) and (New_state_num == State#node_state.dispatcher_state) ->
    ?debug("Heart beat on node: ~p: answered, new state_num: ~p, new callback_num: ~p", [node(), New_state_num]),
    State;
heart_beat_response(New_state_num, State) ->
    ?debug("Heart beat on node: ~p: answered, new state_num: ~p, new callback_num: ~p", [node(), New_state_num]),
    update_dispatcher(New_state_num, State#node_state.node_type),
    State#node_state{state_num = New_state_num}.

%% update_dispatcher/2
%% ====================================================================
%% @doc Tells dispatcher that cluster state has changed.
-spec update_dispatcher(New_state_num :: integer(), Type :: atom()) -> Result when
    Result :: atom().
%% ====================================================================
update_dispatcher(_New_state_num, ccm) ->
    ok;
update_dispatcher(New_state_num, _Type) ->
    ?debug("Message sent to update dispatcher, state num: ~p", [New_state_num]),
    gen_server:cast(?Dispatcher_Name, {update_state, New_state_num}).

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