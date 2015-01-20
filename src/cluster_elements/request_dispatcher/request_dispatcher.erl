%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module forwards client's requests to appropriate worker_hosts.
%%% @end %todo dispatcher should keep modules in ETS, and all clients should be able to read this ETS directly
%%%-------------------------------------------------------------------
-module(request_dispatcher).
-author("Michal Wrzeszcz").

-behaviour(gen_server).

-include("registered_names.hrl").
-include("modules_and_args.hrl").
-include("cluster_elements/request_dispatcher/request_dispatcher.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts request_dispatcher
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> Result when
    Result :: {ok, Pid}
    | ignore
    | {error, Error},
    Pid :: pid(),
    Error :: {already_started, Pid} | term().
start_link() ->
    gen_server:start_link({local, ?DISPATCHER_NAME}, ?MODULE, ?MODULES, []).

%%--------------------------------------------------------------------
%% @doc
%% Stops request_dispatcher
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok.
stop() ->
    gen_server:cast(?DISPATCHER_NAME, stop).

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
init(Modules) ->
    process_flag(trap_exit, true),
    catch gsi_handler:init(),   %% Failed initialization of GSI should not disturb dispacher's startup
    {ok, initState(Modules)}.

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
    {reply, State#dispatcher_state.state_num, State};

handle_call({Task, ProtocolVersion, AnsPid, MsgId, Request}, _From, State) ->
    case handle_generic_request(Task, Request, State) of
        {Response, undefined} -> Response;
        {Response, Node} ->
            gen_server:cast({Task, Node}, {synch, ProtocolVersion, Request, MsgId, {proc, AnsPid}}),
            Response
    end;

handle_call({Task, ProtocolVersion, AnsPid, Request}, _From, State) ->
    case handle_generic_request(Task, Request, State) of
        {Response, undefined} -> Response;
        {Response, Node} ->
            gen_server:cast({Task, Node}, {synch, ProtocolVersion, Request, {proc, AnsPid}}),
            Response
    end;

handle_call({Task, ProtocolVersion, Request}, _From, State) ->
    case handle_generic_request(Task, Request, State) of
        {Response, undefined} -> Response;
        {Response, Node} ->
            gen_server:cast({Task, Node}, {asynch, ProtocolVersion, Request}),
            Response
    end;

handle_call({node_chosen, {Task, ProtocolVersion, AnsPid, Request}}, _From, State) ->
    case get_worker_node_prefering_local(Task, State) of
        {Node, NewState} ->
            case Node of
                non ->
                    case Task of
                        central_logger -> ok;
                        _ ->
                            ?warning("Worker not found, dispatcher state: ~p, task: ~p, request: ~p", [State, Task, Request])
                    end,
                    {reply, worker_not_found, State};
                _N ->
                    gen_server:cast({Task, Node}, {synch, ProtocolVersion, Request, {proc, AnsPid}}),
                    {reply, ok, NewState}
            end;
        Other -> {reply, Other, State}
    end;

handle_call({node_chosen, {Task, ProtocolVersion, AnsPid, MsgId, Request}}, _From, State) ->
    case get_worker_node_prefering_local(Task, State) of
        {Node, NewState} ->
            case Node of
                non ->
                    case Task of
                        central_logger -> ok;
                        _ ->
                            ?warning("Worker not found, dispatcher state: ~p, task: ~p, request: ~p", [State, Task, Request])
                    end,
                    {reply, worker_not_found, State};
                _N ->
                    gen_server:cast({Task, Node}, {synch, ProtocolVersion, Request, MsgId, {proc, AnsPid}}),
                    {reply, ok, NewState}
            end;
        Other -> {reply, Other, State}
    end;

handle_call({node_chosen, {Task, ProtocolVersion, Request}}, _From, State) ->
    case get_worker_node_prefering_local(Task, State) of
        {Node, NewState} ->
            case Node of
                non ->
                    case Task of
                        central_logger -> ok;
                        _ ->
                            ?warning("Worker not found, dispatcher state: ~p, task: ~p, request: ~p", [State, Task, Request])
                    end,
                    {reply, worker_not_found, State};
                _N ->
                    gen_server:cast({Task, Node}, {asynch, ProtocolVersion, Request}),
                    {reply, ok, NewState}
            end;
        Other -> {reply, Other, State}
    end;

handle_call(_Request, _From, State) ->
    ?warning("Wrong call: ~p", [_Request]),
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
handle_cast({update_state, NewStateNum}, State) ->
    NewState = update_state(State, NewStateNum),
    {noreply, NewState};

handle_cast({update_workers, WorkersList, NewStateNum}, _) ->
    ?info("Dispatcher state updated, state num: ~p", [NewStateNum]),
    NewState = update_workers(WorkersList, NewStateNum, ?MODULES),
    {noreply, NewState};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Msg, State) ->
    ?warning("Wrong cast: ~p", [_Msg]),
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
    spawn(fun() -> gen_server:call(?DISPATCHER_NAME, Msg) end),
    {noreply, State};
handle_info(_Info, State) ->
    ?warning("Dispatcher wrong info: ~p", [_Info]),
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
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec update_state(State :: #dispatcher_state{}, NewStateNum :: integer()) -> #dispatcher_state{}.
update_state(State = #dispatcher_state{state_num = StateNum}, NewStateNum) when StateNum >= NewStateNum ->
    gen_server:cast(?NODE_MANAGER_NAME, {dispatcher_updated, NewStateNum}),
    State;
update_state(State, _) ->
    ?info("Dispatcher had old state number, starting update"),
    {WorkersList, StateNum} = gen_server:call({global, ?CCM}, get_workers),
    NewState =
        case WorkersList of
            non ->
                State;
            error ->
                ?error("Dispatcher had old state number but could not update data"),
                State;
            _ ->
                TmpState = update_workers(WorkersList, State#dispatcher_state.state_num, ?MODULES),
                ?info("Dispatcher state updated, state num: ~p", [StateNum]),
                TmpState#dispatcher_state{state_num = StateNum}
        end,
    gen_server:cast(?NODE_MANAGER_NAME, {dispatcher_updated, NewState#dispatcher_state.state_num}),
    NewState.

handle_generic_request(Task, Request, State) ->
    case get_worker_node(Task, State) of
        {non, _} ->
            case Task of % todo beautify
                central_logger -> ok;
                _ -> ?warning("Worker not found, dispatcher state: ~p, task: ~p, request: ~p", [State, Task, Request])
            end,
            {{reply, worker_not_found, State}, undefined};
        {Node, NewState} ->
            {{reply, ok, NewState}, Node};
        Other ->
            {{reply, Other, State}, undefined}
    end.

%% get_nodes/2
%% ====================================================================
%% @doc Gets nodes where module is working.
-spec get_nodes(Module :: atom(), State :: term()) -> {list(), list()}.
%% ====================================================================
get_nodes(Module, #dispatcher_state{modules = Modules}) ->
    get_nodes(Module, Modules);
get_nodes(_, []) ->
    wrong_worker_type;
get_nodes(Module, [{M, N} | T]) ->
    case M =:= Module of
        true -> N;
        false -> get_nodes(Module, T)
    end.

%% update_nodes/3
%% ====================================================================
%% @doc Updates information about nodes (were modules are working).
-spec update_nodes(Module :: atom(), NewNodes :: term(), State :: term()) -> Result when
    Result :: term().
%% ====================================================================
update_nodes(Module, NewNodes, State) ->
    Modules = State#dispatcher_state.modules,
    NewModules = update_nodes_list(Module, NewNodes, Modules, []),
    State#dispatcher_state{modules = NewModules}.

%% update_nodes_list/4
%% ====================================================================
%% @doc Updates information about nodes (were modules are working).
-spec update_nodes_list(Module :: atom(), NewNodes :: term(), Modules :: list(), TmpList :: list()) -> Result when
    Result :: term().
%% ====================================================================
update_nodes_list(_Module, _NewNodes, [], Ans) ->
    Ans;
update_nodes_list(Module, NewNodes, [{M, N} | T], Ans) ->
    case M =:= Module of
        true -> update_nodes_list(Module, NewNodes, T, [{M, NewNodes} | Ans]);
        false -> update_nodes_list(Module, NewNodes, T, [{M, N} | Ans])
    end.

%% ====================================================================
%% @doc Chooses one from nodes where module is working.
-spec get_worker_node(Module :: atom(), State :: term()) -> Result when
    Result :: term().
get_worker_node(Module, State) ->
    case get_nodes(Module, State) of
        {L1, L2} ->
            {N, NewLists} = choose_worker(L1, L2),
            {N, update_nodes(Module, NewLists, State)};
        Other -> Other
    end.

%% ====================================================================
%% @doc Checks if module is working on this node. If not, chooses other
%% node from nodes where module is working.
-spec get_worker_node_prefering_local(Module :: atom(), State :: term()) -> Result when
    Result :: term().
get_worker_node_prefering_local(Module, State) ->
    case get_nodes(Module, State) of
        {L1, L2} ->
            case lists:member(node(), lists:flatten([L1, L2])) of
                true ->
                    {node(), State};
                false ->
                    {N, NewLists} = choose_worker(L1, L2),
                    {N, update_nodes(Module, NewLists, State)}
            end;
        Other -> Other
    end.

%% ====================================================================
%% @doc Helper function used by get_worker_node/3. It chooses node and
%% put it on recently used nodes list.
-spec choose_worker(L1 :: term(), L2 :: term()) -> Result when
    Result :: term().
%% ====================================================================
choose_worker([], []) ->
    {non, {[], []}};
choose_worker([], L2) ->
    choose_worker(L2, []);
choose_worker([N | L1], L2) ->
    {N, {L1, [N | L2]}}.

%% add_worker/3
%% ====================================================================
%% @doc Adds nodes to list that describes module.
-spec add_worker(Module :: atom(), Node :: term(), State :: term()) -> Result when
    Result :: {ok, NewState} | Error,
    Error :: term(),
    NewState :: term().
%% ====================================================================
add_worker(Module, Node, State) ->
    case get_nodes(Module, State) of
        {L1, L2} ->
            {ok, update_nodes(Module, {[Node | L1], L2}, State)};
        Other -> Other
    end.

%% update_workers/6
%% ====================================================================
%% @doc Updates dispatcher state when new workers list appears.
-spec update_workers(WorkersList :: term(), SNum :: integer(), Modules :: list()) -> #dispatcher_state{}.
update_workers(WorkersList, SNum, Modules) ->
    Update =
        fun({Node, Module}, TmpState) ->
            case add_worker(Module, Node, TmpState) of
                {ok, NewState} -> NewState;
                _Other -> TmpState
            end
        end,
    lists:foldl(Update, initState(SNum, Modules), WorkersList).

%% initState/1
%% ====================================================================
%% @doc Initializes new record #dispatcher_state
-spec initState(Modules :: list()) -> Result when
    Result :: record().
%% ====================================================================
initState(Modules) ->
    CreateModules = fun(Module, TmpList) ->
        [{Module, {[], []}} | TmpList]
    end,
    NewModules = lists:foldl(CreateModules, [], Modules),
    #dispatcher_state{modules = NewModules}.

%% initState/5
%% ====================================================================
%% @doc Initializes new record #dispatcher_state
-spec initState(SNum :: integer(), Modules :: list()) -> Result when
    Result :: record().
%% ====================================================================
initState(SNum, Modules) ->
    NewState = initState(Modules),
    NewState#dispatcher_state{state_num = SNum}.