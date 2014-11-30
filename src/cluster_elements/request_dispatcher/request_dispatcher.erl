%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module forwards client's requests to appropriate worker_hosts.
%% @end
%% ===================================================================

-module(request_dispatcher).
-behaviour(gen_server).
-include("registered_names.hrl").
-include("records.hrl").
-include("modules_and_args.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include("oneprovider_modules/dao/dao_cluster.hrl").
-include_lib("ctool/include/logging.hrl").

-define(CALLBACKS_TABLE, dispatcher_callbacks_table).

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/0, stop/0, get_connected_fuses/0, send_to_fuse/3, send_to_fuse_ack/4]).

%% TODO zmierzyć czy bardziej się opłaca przechowywać dane o modułach
%% jako stan (jak teraz) czy jako ets i ewentualnie przejść na ets

%% ====================================================================
%% Test API
%% ====================================================================
-ifdef(TEST).
-export([start_link/1]).
-endif.

%% ====================================================================
%% gen_server callbacks
%% ====================================================================
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================

%% start_link/0
%% ====================================================================
%% @doc Starts the server
-spec start_link() -> Result when
  Result :: {ok, Pid}
  | ignore
  | {error, Error},
  Pid :: pid(),
  Error :: {already_started, Pid} | term().
%% ====================================================================

start_link() ->
  gen_server:start_link({local, ?Dispatcher_Name}, ?MODULE, ?MODULES, []).

-ifdef(TEST).
%% start_link/1
%% ====================================================================
%% @doc Starts the server
-spec start_link(Modules :: list()) -> Result when
  Result :: {ok, Pid}
  | ignore
  | {error, Error},
  Pid :: pid(),
  Error :: {already_started, Pid} | term().
%% ====================================================================

start_link(Modules) ->
  gen_server:start_link({local, ?Dispatcher_Name}, ?MODULE, Modules, []).
-endif.

%% stop/0
%% ====================================================================
%% @doc Stops the server
-spec stop() -> ok.
%% ====================================================================

stop() ->
  gen_server:cast(?Dispatcher_Name, stop).

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
init(Modules) ->
  ets:new(?CALLBACKS_TABLE, [named_table, set]),
  process_flag(trap_exit, true),
  try gsi_handler:init() of     %% Failed initialization of GSI should not disturb dispacher's startup
    ok -> ok;
    {error, Error} -> ?error("GSI Handler init failed. Error: ~p", [Error])
  catch
    throw:ccm_node -> ?info("GSI Handler init interrupted due to wrong node type (CCM)");
    _:Except -> ?error_stacktrace("GSI Handler init failed. Exception: ~p", [Except])
  end,
  NewState = initState(Modules),
  ModulesConstList = lists:map(fun({M, {L1, L2}}) ->
    {M, lists:append(L1, L2)}
  end, NewState#dispatcher_state.modules),
  {ok, NewState#dispatcher_state{modules_const_list = ModulesConstList}}.

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
handle_call({get_workers, Module}, _From, State) ->
  {reply, get_workers(Module, State), State};

handle_call({Task, ProtocolVersion, AnsPid, MsgId, Request}, _From, State) ->
  case State#dispatcher_state.asnych_mode of
    true ->
      forward_request(false, Task, Request, {synch, ProtocolVersion, Request, MsgId, {proc, AnsPid}}, State);
    false ->
      Ans = get_worker_node(Task, Request, State),
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

handle_call({Task, ProtocolVersion, AnsPid, Request}, _From, State) ->
  case State#dispatcher_state.asnych_mode of
    true ->
      forward_request(false, Task, Request, {synch, ProtocolVersion, Request, {proc, AnsPid}}, State);
    false ->
      Ans = get_worker_node(Task, Request, State),
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

handle_call({Task, ProtocolVersion, Request}, _From, State) ->
  case State#dispatcher_state.asnych_mode of
    true ->
      forward_request(false, Task, Request, {asynch, ProtocolVersion, Request}, State);
    false ->
      Ans = get_worker_node(Task, Request, State),
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

handle_call({node_chosen_for_ack, {Task, ProtocolVersion, Request, MsgId, FuseId}}, _From, State) ->
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
              gen_server:cast({Task, Node}, {asynch, ProtocolVersion, Request, MsgId, FuseId}),
              {reply, ok, NewState}
          end;
        Other -> {reply, Other, State}
      end
  end;

handle_call({set_asnych_mode, AsnychMode}, _From, State) ->
  ?info("Asynch mode new value ~p", [AsnychMode]),
  {reply, ok, State#dispatcher_state{asnych_mode = AsnychMode}};

handle_call({get_callback, Fuse}, _From, State) ->
  {reply, get_callback(Fuse), State};

handle_call(get_callbacks, _From, State) ->
  {reply, {get_callbacks(), State#dispatcher_state.callbacks_num}, State};

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

%% handle_test_call/3
%% ====================================================================
%% @doc Handles calls used during tests
-spec handle_test_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result :: term().
%% ====================================================================
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
%% TODO: check dispatchers map
handle_cast({update_state, NewStateNum, NewCallbacksNum}, State) ->
  case {State#dispatcher_state.state_num >= NewStateNum, State#dispatcher_state.callbacks_num >= NewCallbacksNum} of
    {true, true} ->
      gen_server:cast(?Node_Manager_Name, {dispatcher_updated, NewStateNum, NewCallbacksNum});
    {false, true} ->
      spawn(fun() ->
        ?info("Dispatcher had old state number, starting update"),
        {WorkersList, StateNum} = pull_state(State#dispatcher_state.state_num),
        gen_server:cast(?Dispatcher_Name, {update_pulled_state, WorkersList, StateNum, non, non})
      end);
    {true, false} ->
      spawn(fun() ->
        ?info("Dispatcher had old callbacks number, starting update"),
        {AnsList, AnsNum} = pull_callbacks(State#dispatcher_state.callbacks_num),
        gen_server:cast(?Dispatcher_Name, {update_pulled_state, non, non, AnsList, AnsNum})
      end);
    {false, false} ->
      spawn(fun() ->
        ?info("Dispatcher had old state number, starting update"),
        {WorkersList, StateNum} = pull_state(State#dispatcher_state.state_num),
        ?info("Dispatcher had old callbacks number, starting update"),
        {AnsList, AnsNum} = pull_callbacks(State#dispatcher_state.callbacks_num),
        gen_server:cast(?Dispatcher_Name, {update_pulled_state, WorkersList, StateNum, AnsList, AnsNum})
      end)
  end,
  {noreply, State};

handle_cast({update_pulled_state, WorkersList, StateNum, CallbacksList, CallbacksNum}, State) ->
  NewState = case WorkersList of
               non ->
                 State;
               error ->
                 ?error("Dispatcher had old state number but could not update data"),
                 State;
               _ ->
                 TmpState = update_workers(WorkersList, State#dispatcher_state.state_num, State#dispatcher_state.callbacks_num, State#dispatcher_state.current_load, State#dispatcher_state.avg_load, ?MODULES),
                 ?info("Dispatcher state updated, state num: ~p", [StateNum]),
                 TmpState#dispatcher_state{state_num = StateNum}
             end,

  NewState2 = case CallbacksList of
                non ->
                  NewState;
                error ->
                  ?error("Dispatcher had old callbacks number but could not update data"),
                  NewState;
                _ ->
                  UpdateCallbacks = fun({Fuse, NodesList}) ->
                    ets:insert(?CALLBACKS_TABLE, {Fuse, NodesList})
                  end,
                  lists:foreach(UpdateCallbacks, CallbacksList),
                  ?info("Dispatcher callbacks updated, new num: ~p", [CallbacksNum]),
                  NewState#dispatcher_state{callbacks_num = CallbacksNum}
              end,

  gen_server:cast(?Node_Manager_Name, {dispatcher_updated, NewState2#dispatcher_state.state_num, NewState2#dispatcher_state.callbacks_num}),
  {noreply, NewState2};

handle_cast({update_workers, WorkersList, RequestMap, NewStateNum, CurLoad, AvgLoad}, State) ->
  NewState = update_workers(WorkersList, NewStateNum, State#dispatcher_state.callbacks_num, CurLoad, AvgLoad, ?MODULES),
  ?info("Dispatcher state updated, state num: ~p", [NewStateNum]),
  {noreply, NewState#dispatcher_state{request_map = RequestMap}};

handle_cast({update_workers, WorkersList, RequestMap, NewStateNum, CurLoad, AvgLoad, Modules}, State) ->
  NewState = update_workers(WorkersList, NewStateNum, State#dispatcher_state.callbacks_num, CurLoad, AvgLoad, Modules),
  ?info("Dispatcher state updated, state num: ~p", [NewStateNum]),
  {noreply, NewState#dispatcher_state{request_map = RequestMap}};

handle_cast({update_loads, CurLoad, AvgLoad}, State) ->
  {noreply, State#dispatcher_state{current_load = CurLoad, avg_load = AvgLoad}};

handle_cast(stop, State) ->
  {stop, normal, State};

handle_cast({addCallback, FuseId, Node, CallbacksNum}, State) ->
  add_callback(Node, FuseId),
  ?debug("Callback added to dispatcher: ~p", [{FuseId, Node, CallbacksNum}]),
  {noreply, State#dispatcher_state{callbacks_num = CallbacksNum}};

handle_cast({delete_callback, FuseId, Node, CallbacksNum}, State) ->
  delete_callback(Node, FuseId),
  ?debug("Callback deleted from dispatcher: ~p", [{FuseId, Node, CallbacksNum}]),
  {noreply, State#dispatcher_state{callbacks_num = CallbacksNum}};

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
    spawn(fun() -> gen_server:call(?Dispatcher_Name, Msg) end),
    {noreply, State};
handle_info(_Info, State) ->
  ?warning("Dispatcher wrong info: ~p", [_Info]),
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

%% get_nodes/2
%% ====================================================================
%% @doc Gets nodes were module is working.
-spec get_nodes(Module :: atom(), State :: term()) -> Result when
  Result :: term().
%% ====================================================================
get_nodes(Module, State) ->
  get_nodes_list(Module, State#dispatcher_state.modules).

%% get_nodes_list/2
%% ====================================================================
%% @doc Gets nodes were module is working.
-spec get_nodes_list(Module :: atom(), Modules :: list()) -> Result when
  Result :: term().
%% ====================================================================

get_nodes_list(_Module, []) ->
  wrong_worker_type;
get_nodes_list(Module, [{M, N} | T]) ->
  case M =:= Module of
    true -> N;
    false -> get_nodes_list(Module, T)
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
%%                ?debug("Load of node too high", [Module]),
                  {N, NewLists} = choose_worker(L1, L2),
                  {N, update_nodes(Module, NewLists, State)}
              end;
            false ->
%%            ?debug("Module ~p does not work at this node", [Module]),
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
-spec update_workers(WorkersList :: term(), SNum :: integer(), CNum :: integer(), CLoad :: number(), ALoad :: number(), Modules :: list()) -> Result when
  Result :: term().
%% ====================================================================
update_workers(WorkersList, SNum, CNum, CLoad, ALoad, Modules) ->
  Update = fun({Node, Module}, TmpState) ->
    Ans = add_worker(Module, Node, TmpState),
    case Ans of
      {ok, NewState} -> NewState;
      _Other -> TmpState
    end
  end,
  NewState = lists:foldl(Update, initState(SNum, CNum, CLoad, ALoad, Modules), WorkersList),
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
  Nodes = get_nodes(Module, State),
  case Nodes of
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
-spec initState(SNum :: integer(), CNum :: integer(), CLoad :: number(), ALoad :: number(), Modules :: list()) -> Result when
  Result :: record().
%% ====================================================================
initState(SNum, CNum, CLoad, ALoad, Modules) ->
  NewState = initState(Modules),
  NewState#dispatcher_state{state_num = SNum, callbacks_num = CNum, current_load = CLoad, avg_load = ALoad}.

%% add_callback/2
%% ====================================================================
%% @doc Adds info about callback to fuse.
-spec add_callback(Node :: term(), Fuse :: string()) -> Result when
  Result :: ok | updated.
%% ====================================================================
add_callback(Node, Fuse) ->
  OldCallbacks = ets:lookup(?CALLBACKS_TABLE, Fuse),
  case OldCallbacks of
    [{Fuse, OldCallbacksList}] ->
      case lists:member(Node, OldCallbacksList) of
        true ->
          ?debug("Adding callback ~p to dispatcher: already exists", [{Node, Fuse}]),
          ok;
        false ->
          ets:insert(?CALLBACKS_TABLE, {Fuse, [Node | OldCallbacksList]}),
          ?debug("Adding callback ~p to dispatcher: updated", [{Node, Fuse}]),
          updated
      end;
    _ ->
      ets:insert(?CALLBACKS_TABLE, {Fuse, [Node]}),
      ?debug("Adding callback ~p to dispatcher: updated", [{Node, Fuse}]),
      updated
  end.

%% delete_callback/2
%% ====================================================================
%% @doc Deletes info about callback
-spec delete_callback(Node :: term(), Fuse :: string()) -> Result when
  Result :: updated | not_exists.
%% ====================================================================
delete_callback(Node, Fuse) ->
  OldCallbacks = ets:lookup(?CALLBACKS_TABLE, Fuse),
  case OldCallbacks of
    [{Fuse, OldCallbacksList}] ->
      case lists:member(Node, OldCallbacksList) of
        true ->
          case length(OldCallbacksList) of
            1 ->
              ets:delete(?CALLBACKS_TABLE, Fuse),
              ?debug("Deleting callback ~p from dispatcher: updated", [{Node, Fuse}]),
              updated;
            _ ->
              ets:insert(?CALLBACKS_TABLE, {Fuse, lists:delete(Node, OldCallbacksList)}),
              ?debug("Deleting callback ~p from dispatcher: updated", [{Node, Fuse}]),
              updated
          end;
        false ->
          ?debug("Deleting callback ~p from dispatcher: not_exists", [{Node, Fuse}]),
          not_exists
      end;
    _ ->
      ?debug("Deleting callback ~p from dispatcher: not_exists", [{Node, Fuse}]),
      not_exists
  end.

%% get_callback/1
%% ====================================================================
%% @doc Gets info on which node callback to fuse can be found
%% (if there are many nodes, one is chosen).
-spec get_callback(Fuse :: string()) -> Result when
  Result :: not_found | term().
%% ====================================================================
get_callback(Fuse) ->
  Callbacks = ets:lookup(?CALLBACKS_TABLE, Fuse),
  case Callbacks of
    [{Fuse, CallbacksList}] ->
      Num = random:uniform(length(CallbacksList)), %% if it will be moved to other proc than dispatcher the seed shoud be initialized
      lists:nth(Num, CallbacksList);
    _ ->
      not_found
  end.

%% pull_callbacks/1
%% ====================================================================
%% @doc Pulls info about callbacks from CCM
-spec pull_callbacks(OldCallbacksNum :: integer()) -> Result when
  Result :: error | CallbacksNum,
  CallbacksNum :: integer().
%% ====================================================================
pull_callbacks(OldCallbacksNum) ->
  try
    gen_server:call({global, ?CCM}, get_callbacks, 1000)
  catch
    _:_ ->
      ?error("Dispatcher on node: ~p: can not pull callbacks list", [node()]),
      {error, OldCallbacksNum}
  end.

%% get_callbacks/0
%% ====================================================================
%% @doc Gets information about all callbacks
-spec get_callbacks() -> Result when
  Result :: list().
%% ====================================================================
get_callbacks() ->
  get_callbacks(ets:first(?CALLBACKS_TABLE)).

%% get_callbacks/1
%% ====================================================================
%% @doc Gets information about all callbacks (helper function)
-spec get_callbacks(Fuse :: string()) -> Result when
  Result :: list().
%% ====================================================================
get_callbacks('$end_of_table') ->
  [];
get_callbacks(Fuse) ->
  [Value] = ets:lookup(?CALLBACKS_TABLE, Fuse),
  [Value | get_callbacks(ets:next(?CALLBACKS_TABLE, Fuse))].

%% get_connected_fuses/0
%% ====================================================================
%% @doc Gets information about all callbacks (helper function)
-spec get_connected_fuses() -> Result when
  Result :: {ok, no_fuses_connected} | {ok, ClientList :: [{Login :: string(), UUID :: uuid()}]} | {error, Reason :: term()}.
%% ====================================================================
get_connected_fuses() ->
  try
    Infinity = 10000000000000000000,
    {ok, List} = dao_lib:apply(dao_cluster, list_fuse_sessions, [{by_valid_to, Infinity}], 1),
    ClientList = lists:foldl(
      fun(#db_document{uuid = UUID, record = #fuse_session{uid = UserID}} = SessionDoc, Acc) ->
        case dao_cluster:check_session(SessionDoc) of
          ok ->
            {ok, UserDoc} = user_logic:get_user({uuid, UserID}),
            Acc ++ [{user_logic:get_login(UserDoc), UUID}];
          _ ->
            Acc
        end
      end, [], List),
    case ClientList of
      [] -> {ok, no_fuses_connected};
      _ -> {ok, ClientList}
    end
  catch Type:Reason ->
    ?error_stacktrace("Cannot list fuse sessions: ~p:~p", [Type, Reason]),
    {error, Reason}
  end.

%% send_to_fuse/3
%% ====================================================================
%% @doc Sends message to fuse
-spec send_to_fuse(FuseId :: string(), Message :: term(), MessageDecoder :: string()) -> Result when
  Result :: callback_node_not_found | node_manager_error | dispatcher_error | ok | term().
%% ====================================================================
send_to_fuse(FuseId, Message, MessageDecoder) ->
  send_to_fuse(FuseId, Message, MessageDecoder, 3).

%% send_to_fuse/4
%% ====================================================================
%% @doc Sends message to fuse
-spec send_to_fuse(FuseId :: string(), Message :: term(), MessageDecoder :: string(), SendNum :: integer()) -> Result when
  Result :: callback_node_not_found | node_manager_error | dispatcher_error | ok | term().
%% ====================================================================
send_to_fuse(FuseId, Message, MessageDecoder, SendNum) ->
  case fslogic_context:is_global_fuse_id(FuseId) of
    true ->
      provider_proxy:reroute_push_message(fslogic_context:read_global_fuse_id(FuseId), Message, MessageDecoder);
    false ->
      send_to_fuse1(FuseId, Message, MessageDecoder, SendNum)
  end.
send_to_fuse1(FuseId, Message, MessageDecoder, SendNum) ->
  Ans = try
    Node = gen_server:call(?Dispatcher_Name, {get_callback, FuseId}, 500),
    case Node of
      not_found ->
        ?warning("Sending message ~p to fuse ~p: callback_node_not_found", [Message, FuseId]),
        callback_node_not_found;
      _ ->
        try
          Callback = gen_server:call({?Node_Manager_Name, Node}, {get_callback, FuseId}, 1000),
          case Callback of
            non ->
              ?warning("Sending message ~p to fuse ~p: channel_not_found", [Message, FuseId]),
              channel_not_found;
            _ ->
              Callback ! {self(), Message, MessageDecoder, -1},
              receive
                {Callback, -1, Response} -> Response
              after 1000 ->
                ?error("Sending message ~p to fuse ~p: socket_error", [Message, FuseId]),
                socket_error
              end
          end
        catch
          E1:E2 ->
            ?error("Can not get callback from node: ~p to fuse ~p, error: ~p:~p", [Node, FuseId, E1, E2]),
            node_manager_error
        end
    end
        catch
          _:_ ->
            ?error("Can not get callback node of fuse: ~p", [FuseId]),
            dispatcher_error
        end,
  case Ans of
    ok -> ok;
    _ ->
      case SendNum of
        0 -> Ans;
        _ ->
          send_to_fuse(FuseId, Message, MessageDecoder, SendNum - 1)
      end
  end.

%% send_to_fuse_ack/4
%% ====================================================================
%% @doc Sends message to fuse with ack
-spec send_to_fuse_ack(FuseId :: string(), Message :: term(), MessageDecoder :: string(), MessageId :: integer()) -> Result when
  Result :: callback_node_not_found | node_manager_error | dispatcher_error | ok | term().
%% ====================================================================
send_to_fuse_ack(FuseId, Message, MessageDecoder, MessageId) ->
  send_to_fuse_ack(FuseId, Message, MessageDecoder, MessageId, 3).

%% send_to_fuse_ack/5
%% ====================================================================
%% @doc Sends message to fuse with ack
-spec send_to_fuse_ack(FuseId :: string(), Message :: term(), MessageDecoder :: string(), MessageId :: integer(), SendNum :: integer()) -> Result when
  Result :: callback_node_not_found | node_manager_error | dispatcher_error | ok | term().
%% ====================================================================
%% TODO how sender can get ack from fuse?
send_to_fuse_ack(FuseId, Message, MessageDecoder, MessageId, SendNum) ->
  Ans = try
    Node = gen_server:call(?Dispatcher_Name, {get_callback, FuseId}, 500),
    case Node of
      not_found ->
        ?warning("Sending message ~p to fuse ~p: callback_node_not_found", [Message, FuseId]),
        callback_node_not_found;
      _ ->
        try
          Callback = gen_server:call({?Node_Manager_Name, Node}, {get_callback, FuseId}, 1000),
          case Callback of
            non ->
              ?warning("Sending message ~p to fuse ~p: channel_not_found", [Message, FuseId]),
              channel_not_found;
            _ ->
              Callback ! {with_ack, self(), Message, MessageDecoder, MessageId},
              receive
                {Callback, MessageId, Response} -> Response
              after 500 ->
                ?error("Sending message ~p to fuse ~p: socket_error", [Message, FuseId]),
                socket_error
              end
          end
        catch
          E1:E2 ->
            ?error("Can not get callback from node: ~p to fuse ~p, error: ~p:~p", [Node, FuseId, E1, E2]),
            node_manager_error
        end
    end
        catch
          _:_ ->
            ?error("Can not get callback node of fuse: ~p", [FuseId]),
            dispatcher_error
        end,
  case Ans of
    ok -> ok;
    _ ->
      case SendNum of
        0 -> Ans;
        _ ->
          send_to_fuse_ack(FuseId, Message, MessageDecoder, MessageId, SendNum - 1)
      end
  end.

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
