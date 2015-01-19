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
    NewState = initState(Modules),
    ModulesConstList = lists:map(fun({M, {L1, L2}}) ->
        {M, lists:append(L1, L2)}
    end, NewState#dispatcher_state.modules),
    {ok, NewState#dispatcher_state{modules_const_list = ModulesConstList}}.

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
handle_call({get_workers, Module}, _From, State) ->
    {reply, get_workers(Module, State), State};

handle_call({Task, ProtocolVersion, AnsPid, MsgId, Request}, _From, State = #dispatcher_state{asnych_mode = true}) ->
    forward_request(false, Task, Request, {synch, ProtocolVersion, Request, MsgId, {proc, AnsPid}}, State);

handle_call({Task, ProtocolVersion, AnsPid, Request}, _From, State = #dispatcher_state{asnych_mode = true}) ->
    forward_request(false, Task, Request, {synch, ProtocolVersion, Request, {proc, AnsPid}}, State);

handle_call({Task, ProtocolVersion, Request}, _From, State = #dispatcher_state{asnych_mode = true}) ->
    forward_request(false, Task, Request, {asynch, ProtocolVersion, Request}, State);

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
    case State#dispatcher_state.asnych_mode of
        true ->
            forward_request(true, Task, Request, {synch, ProtocolVersion, Request, {proc, AnsPid}}, State);
        false ->
            Ans = check_worker_node(Task, Request, State),
            case Ans of
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
            end
    end;

handle_call({node_chosen, {Task, ProtocolVersion, AnsPid, MsgId, Request}}, _From, State) ->
    case State#dispatcher_state.asnych_mode of
        true ->
            forward_request(true, Task, Request, {synch, ProtocolVersion, Request, MsgId, {proc, AnsPid}}, State);
        false ->
            Ans = check_worker_node(Task, Request, State),
            case Ans of
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
            end
    end;

handle_call({node_chosen, {Task, ProtocolVersion, Request}}, _From, State) ->
    case State#dispatcher_state.asnych_mode of
        true ->
            forward_request(true, Task, Request, {asynch, ProtocolVersion, Request}, State);
        false ->
            Ans = check_worker_node(Task, Request, State),
            case Ans of
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
            end
    end;

handle_call({set_asnych_mode, AsnychMode}, _From, State) ->
    ?info("Asynch mode new value ~p", [AsnychMode]),
    {reply, ok, State#dispatcher_state{asnych_mode = AsnychMode}};

handle_call(get_state_num, _From, State) ->
    {reply, State#dispatcher_state.state_num, State};

%% test call
handle_call({get_worker_node, {Request, Module}}, _From, State) ->
    handle_test_call({get_worker_node, {Request, Module}}, _From, State);

%% test call
handle_call({get_worker_node, Module}, _From, State) ->
    handle_test_call({get_worker_node, Module}, _From, State);

%% test call
handle_call({check_worker_node, Module}, _From, State) ->
    handle_test_call({check_worker_node, Module}, _From, State);

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
    case State#dispatcher_state.state_num >= NewStateNum of
        true ->
            gen_server:cast(?NODE_MANAGER_NAME, {dispatcher_updated, NewStateNum});
        false ->
            spawn(fun() ->
                ?info("Dispatcher had old state number, starting update"),
                {WorkersList, StateNum} = pull_state(State#dispatcher_state.state_num),
                gen_server:cast(?DISPATCHER_NAME, {update_pulled_state, WorkersList, StateNum})
            end)
    end,
    {noreply, State};

handle_cast({update_pulled_state, WorkersList, StateNum}, State) ->
    NewState = case WorkersList of
                   non ->
                       State;
                   error ->
                       ?error("Dispatcher had old state number but could not update data"),
                       State;
                   _ ->
                       TmpState = update_workers(WorkersList, State#dispatcher_state.state_num, State#dispatcher_state.current_load, State#dispatcher_state.avg_load, ?MODULES),
                       ?info("Dispatcher state updated, state num: ~p", [StateNum]),
                       TmpState#dispatcher_state{state_num = StateNum}
               end,

    gen_server:cast(?NODE_MANAGER_NAME, {dispatcher_updated, NewState#dispatcher_state.state_num}),
    {noreply, NewState};

handle_cast({update_workers, WorkersList, RequestMap, NewStateNum, CurLoad, AvgLoad}, _) ->
    NewState = update_workers(WorkersList, NewStateNum, CurLoad, AvgLoad, ?MODULES),
    ?info("Dispatcher state updated, state num: ~p", [NewStateNum]),
    {noreply, NewState#dispatcher_state{request_map = RequestMap}};

handle_cast({update_workers, WorkersList, RequestMap, NewStateNum, CurLoad, AvgLoad, Modules}, _) ->
    NewState = update_workers(WorkersList, NewStateNum, CurLoad, AvgLoad, Modules),
    ?info("Dispatcher state updated, state num: ~p", [NewStateNum]),
    {noreply, NewState#dispatcher_state{request_map = RequestMap}};

handle_cast({update_loads, CurLoad, AvgLoad}, State) ->
    {noreply, State#dispatcher_state{current_load = CurLoad, avg_load = AvgLoad}};

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
%% todo REFACTOR ALL

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles calls used during tests
%% @end
%%--------------------------------------------------------------------
-spec handle_test_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result :: term().
-ifdef(TEST).
handle_test_call({get_worker_node, {Request, Module}}, _From, State) ->
    Ans = get_worker_node(Module, Request, State),
    case Ans of
        {Node, NewState} -> {reply, Node, NewState};
        Other -> {reply, Other, State}
    end;

handle_test_call({get_worker_node, Module}, _From, State) ->
    Ans = get_worker_node(Module, non, State),
    case Ans of
        {Node, NewState} -> {reply, Node, NewState};
        Other -> {reply, Other, State}
    end;

handle_test_call({check_worker_node, Module}, _From, State) ->
    Ans = check_worker_node(Module, non, State),
    case Ans of
        {Node, NewState} -> {reply, Node, NewState};
        Other -> {reply, Other, State}
    end.
-else.
handle_test_call(_Request, _From, State) ->
    {reply, not_supported_in_normal_mode, State}.
-endif.

handle_generic_request(Task, Request, State) ->
    case get_worker_node(Task, Request, State) of
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
%% @doc Gets nodes were module is working.
-spec get_nodes(Module :: atom(), State :: term()) -> Result when
    Result :: term().
%% ====================================================================
get_nodes(_, #dispatcher_state{modules = []}) ->
    wrong_worker_type;
get_nodes(Module, #dispatcher_state{modules = [{M, N} | T]}) ->
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

%% get_worker_node/3
%% ====================================================================
%% @doc Chooses one from nodes where module is working.
-spec get_worker_node(Module :: atom(), Request :: term(), State :: term()) -> Result when
    Result :: term().
%% ====================================================================
get_worker_node(Module, Request, State) ->
    case choose_node_by_map(Module, Request, State) of
        use_standard_mode ->
            Nodes = get_nodes(Module, State),
            case Nodes of
                {L1, L2} ->
                    {N, NewLists} = choose_worker(L1, L2),
                    {N, update_nodes(Module, NewLists, State)};
                Other -> Other
            end;
        Other2 ->
            Other2
    end.

%% check_worker_node/3
%% ====================================================================
%% @doc Checks if module is working on this node. If not, chooses other
%% node from nodes where module is working.
-spec check_worker_node(Module :: atom(), Request :: term(), State :: term()) -> Result when
    Result :: term().
%% ====================================================================
check_worker_node(Module, Request, State) ->
    case choose_node_by_map(Module, Request, State) of
        use_standard_mode ->
            Nodes = get_nodes(Module, State),
            case Nodes of
                {L1, L2} ->
                    ThisNode = node(),
                    Check = (lists:member(ThisNode, lists:flatten([L1, L2]))),
                    case Check of
                        true ->
                            Check2 = ((State#dispatcher_state.current_load =< 3) and (State#dispatcher_state.current_load =< 2 * State#dispatcher_state.avg_load)),
                            case Check2 of
                                true -> {ThisNode, State};
                                false ->
                                    {N, NewLists} = choose_worker(L1, L2),
                                    {N, update_nodes(Module, NewLists, State)}
                            end;
                        false ->
                            {N, NewLists} = choose_worker(L1, L2),
                            {N, update_nodes(Module, NewLists, State)}
                    end;
                Other -> Other
            end;
        Other2 ->
            Other2
    end.

%% choose_worker/2
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
    Nodes = get_nodes(Module, State),
    case Nodes of
        {L1, L2} ->
            {ok, update_nodes(Module, {[Node | L1], L2}, State)};
        Other -> Other
    end.

%% update_workers/6
%% ====================================================================
%% @doc Updates dispatcher state when new workers list appears.
-spec update_workers(WorkersList :: term(), SNum :: integer(), CLoad :: number(), ALoad :: number(), Modules :: list()) -> Result when
    Result :: term().
%% ====================================================================
update_workers(WorkersList, SNum, CLoad, ALoad, Modules) ->
    Update = fun({Node, Module}, TmpState) ->
        Ans = add_worker(Module, Node, TmpState),
        case Ans of
            {ok, NewState} -> NewState;
            _Other -> TmpState
        end
    end,
    NewState = lists:foldl(Update, initState(SNum, CLoad, ALoad, Modules), WorkersList),
    ModulesConstList = lists:map(fun({M, {L1, L2}}) ->
        {M, lists:append(L1, L2)}
    end, NewState#dispatcher_state.modules),
    NewState#dispatcher_state{modules_const_list = ModulesConstList}.

%% pull_state/1
%% ====================================================================
%% @doc Pulls workers list from cluster manager.
-spec pull_state(CurrentStateNum :: integer()) -> Result when
    Result :: {WorkersList, StateNum} | {error, StateNum},
    StateNum :: integer(),
    WorkersList :: list().
%% ====================================================================
pull_state(CurrentStateNum) ->
    try
        gen_server:call({global, ?CCM}, get_workers, 1000)
    catch
        _:_ ->
            ?error("Dispatcher on node: ~p: can not pull workers list", [node()]),
            {error, CurrentStateNum}
    end.

%% get_workers/2
%% ====================================================================
%% @doc Returns information about nodes were module is working according
%% to dispatcher knowledge.
-spec get_workers(Module :: atom, State :: term()) -> Result when
    Result :: term().
%% ====================================================================
get_workers(Module, State) ->
    case get_nodes(Module, State) of
        {L1, L2} -> lists:flatten([L1, L2]);
        _Other -> []
    end.

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
-spec initState(SNum :: integer(), CLoad :: number(), ALoad :: number(), Modules :: list()) -> Result when
    Result :: record().
%% ====================================================================
initState(SNum, CLoad, ALoad, Modules) ->
    NewState = initState(Modules),
    NewState#dispatcher_state{state_num = SNum, current_load = CLoad, avg_load = ALoad}.


%% choose_node_by_map/3
%% ====================================================================
%% @doc Chooses node for a request
-spec choose_node_by_map(Module :: atom(), Msg :: term(), State :: term()) -> Result when
    Result :: wrong_worker_type | use_standard_mode | {non, NewState} | {Node, NewState},
    Node :: atom(),
    NewState :: term().
%% ====================================================================
choose_node_by_map(Module, Msg, State) ->
    ModulesConstList = proplists:get_value(Module, State#dispatcher_state.modules_const_list, wrong_worker_type),
    case ModulesConstList of
        wrong_worker_type -> wrong_worker_type;
        _ ->
            ModulesConstListLength = length(ModulesConstList),
            case ModulesConstListLength of
                0 -> {non, State};
                1 ->
                    [M | _] = ModulesConstList,
                    {M, State};
                _ ->
                    RequestMapList = State#dispatcher_state.request_map,
                    RequestMap = proplists:get_value(Module, RequestMapList, non),
                    case RequestMap of
                        non ->
                            use_standard_mode;
                        _ ->
                            try
                                RequestNum = RequestMap(Msg),
                                case RequestNum of
                                    Num when is_integer(Num) ->
                                        {lists:nth(Num rem ModulesConstListLength + 1, ModulesConstList), State};
                                    _ ->
                                        use_standard_mode
                                end
                            catch
                                Type:Error ->
                                    ?error("Dispatcher error for module ~p and request ~p: ~p:~p ~n ~p", [Module, Msg, Type, Error, erlang:get_stacktrace()]),
                                    use_standard_mode
                            end
                    end
            end
    end.

%% forward_request/5
%% ====================================================================
%% @doc Forwards request. Uses asynchronous process if RequestMap is used.
-spec forward_request(NodeChosen :: boolean(), Task :: atom(), Request :: term(), Message :: term(), State) -> Result when
    Result :: {reply, Ans, State},
    Ans :: wrong_worker_type | worker_not_found | ok,
    State :: term().
%% ====================================================================
forward_request(NodeChosen, Task, Request, Message, State) ->
    ModulesList = proplists:get_value(Task, State#dispatcher_state.modules_const_list, wrong_worker_type),
    Ans = case ModulesList of
              wrong_worker_type -> wrong_worker_type;
              _ ->
                  ModulesListLength = length(ModulesList),
                  case ModulesListLength of
                      0 -> worker_not_found;
                      1 ->
                          [Node | _] = ModulesList,
                          gen_server:cast({Task, Node}, Message),
                          ok;
                      _ ->
                          RequestMapList = State#dispatcher_state.request_map,
                          RequestMap = proplists:get_value(Task, RequestMapList, non),
                          case RequestMap of
                              non ->
                                  Action = case NodeChosen of
                                               true ->
                                                   ThisNode = node(),
                                                   case lists:member(ThisNode, ModulesList) of
                                                       true ->
                                                           gen_server:cast({Task, ThisNode}, Message),
                                                           forwarded;
                                                       false ->
                                                           ok
                                                   end;
                                               false ->
                                                   ok
                                           end,
                                  case Action of
                                      forwarded ->
                                          ok;
                                      _ ->
                                          random:seed(now()),
                                          Node2 = lists:nth(random:uniform(ModulesListLength), ModulesList),
                                          gen_server:cast({Task, Node2}, Message)
                                  end;
                              _ ->
                                  spawn(fun() ->
                                      try
                                          RequestNum = RequestMap(Message),
                                          case RequestNum of
                                              Num when is_integer(Num) ->
                                                  Node3 = lists:nth(Num rem ModulesListLength + 1, ModulesList),
                                                  gen_server:cast({Task, Node3}, Message);
                                              WrongAns ->
                                                  ?error("Dispatcher (request map) wrong answer for module ~p and request ~p: ~p", [Task, Message, WrongAns]),
                                                  random:seed(now()),
                                                  Node4 = lists:nth(random:uniform(ModulesListLength), ModulesList),
                                                  gen_server:cast({Task, Node4}, Message)
                                          end
                                      catch
                                          Type:Error ->
                                              ?error_stacktrace("Dispatcher (request map) error for module ~p and request ~p: ~p:~p", [Task, Message, Type, Error]),
                                              random:seed(now()),
                                              Node5 = lists:nth(random:uniform(ModulesListLength), ModulesList),
                                              gen_server:cast({Task, Node5}, Message)
                                      end
                                  end)
                          end,
                          ok
                  end
          end,
    case Ans of
        ok ->
            {reply, ok, State};
        Other ->
            case Task of
                central_logger -> ok;
                _ -> ?warning("Worker not found, dispatcher state: ~p, task: ~p, request: ~p", [State, Task, Request])
            end,
            {reply, Other, State}
    end.
