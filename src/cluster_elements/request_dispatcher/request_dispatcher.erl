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
-include("logging.hrl").

-define(CALLBACKS_TABLE, dispatcher_callbacks_table).

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/0, stop/0, send_to_fuse/3]).

%% TODO zmierzyć czy bardziej się opłaca przechowywać dane o modułach
%% jako stan (jak teraz) czy jako ets i ewentualnie przejść na ets
%% TODO - sprawdzić czy dispatcher wytrzyma obciążenie - alternatywą jest
%% przejście z round robin na losowość co wyłacza updatowanie stanu
%% Można też wyłączyć potwierdzenie znajdowania noda co
%% da jeszcze większe przyspieszenie

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
  Result ::  {ok,Pid}
  | ignore
  | {error,Error},
  Pid :: pid(),
  Error :: {already_started,Pid} | term().
%% ====================================================================

start_link() ->
  gen_server:start_link({local, ?Dispatcher_Name}, ?MODULE, ?Modules, []).

-ifdef(TEST).
%% start_link/1
%% ====================================================================
%% @doc Starts the server
-spec start_link(Modules :: list()) -> Result when
  Result ::  {ok,Pid}
  | ignore
  | {error,Error},
  Pid :: pid(),
  Error :: {already_started,Pid} | term().
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
    {error, Error} -> lager:error("GSI Handler init failed. Error: ~p", [Error])
  catch
    throw:ccm_node -> lager:info("GSI Handler init interrupted due to wrong node type (CCM)");
    _:Except -> lager:error("GSI Handler init failed. Exception: ~p ~n ~p", [Except, erlang:get_stacktrace()])
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
                _ -> ?warning("Worker not found, dispatcher state: ~p, task: ~p, request: ~p", [State, Task, Request])
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
                _ -> ?warning("Worker not found, dispatcher state: ~p, task: ~p, request: ~p", [State, Task, Request])
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
                _ -> ?warning("Worker not found, dispatcher state: ~p, task: ~p, request: ~p", [State, Task, Request])
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
                _ -> ?warning("Worker not found, dispatcher state: ~p, task: ~p, request: ~p", [State, Task, Request])
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
                _ -> ?warning("Worker not found, dispatcher state: ~p, task: ~p, request: ~p", [State, Task, Request])
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
                _ -> ?warning("Worker not found, dispatcher state: ~p, task: ~p, request: ~p", [State, Task, Request])
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
  ?error("Error: wrong call: ~p", [_Request]),
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
handle_cast({update_state, NewStateNum, NewCallbacksNum}, State) ->
  case {State#dispatcher_state.state_num == NewStateNum, State#dispatcher_state.callbacks_num == NewCallbacksNum} of
    {true, true} ->
      gen_server:cast(?Node_Manager_Name, {dispatcher_updated, NewStateNum, NewCallbacksNum});
    {false, true} ->
      spawn(fun() ->
        lager:info([{mod, ?MODULE}], "Dispatcher had old state number, starting update"),
        {Ans, NewState} = pull_state(State),
        gen_server:cast(?Dispatcher_Name, {update_state, Ans, non, NewState})
      end);
    {true, false} ->
      spawn(fun() ->
        lager:info([{mod, ?MODULE}], "Dispatcher had old callbacks number, starting update"),
        {AnsList, AnsNum} = pull_callbacks(State#dispatcher_state.callbacks_num),
        gen_server:cast(?Dispatcher_Name, {update_state, non, AnsList, State#dispatcher_state{callbacks_num = AnsNum}})
      end);
    {false, false} ->
      spawn(fun() ->
        lager:info([{mod, ?MODULE}], "Dispatcher had old state number, starting update"),
        {AnsState, NewState} = pull_state(State),
        lager:info([{mod, ?MODULE}], "Dispatcher had old callbacks number, starting update"),
        {AnsList, AnsNum} = pull_callbacks(State#dispatcher_state.callbacks_num),
        gen_server:cast(?Dispatcher_Name, {update_state, AnsState, AnsList, NewState#dispatcher_state{callbacks_num = AnsNum}})
      end)
  end,
  {noreply, State};

handle_cast({update_pulled_state, NewStateStatus, CallbacksList, NewState}, State) ->
  case NewStateStatus of
    non ->
      ok;
    ok ->
      lager:info([{mod, ?MODULE}], "Dispatcher state updated");
    _ ->
      lager:error([{mod, ?MODULE}], "Dispatcher had old state number but could not update data")
  end,

  case CallbacksList of
    non ->
      ok;
    error ->
      lager:error([{mod, ?MODULE}], "Dispatcher had old callbacks number but could not update data");
    _ ->
      UpdateCallbacks = fun({Fuse, NodesList}) ->
        ets:insert(?CALLBACKS_TABLE, {Fuse, NodesList})
      end,
      lists:foreach(UpdateCallbacks, CallbacksList),
      lager:info([{mod, ?MODULE}], "Dispatcher callbacks updated")
  end,

  gen_server:cast(?Node_Manager_Name, {dispatcher_updated, NewState#dispatcher_state.state_num, NewCallbacksNum}),
  {noreply, NewState};

handle_cast({update_workers, WorkersList, RequestMap, NewStateNum, CurLoad, AvgLoad}, _State) ->
  NewState = update_workers(WorkersList, NewStateNum, CurLoad, AvgLoad, ?Modules),
  {noreply, NewState#dispatcher_state{request_map = RequestMap}};

handle_cast({update_workers, WorkersList, RequestMap, NewStateNum, CurLoad, AvgLoad, Modules}, _State) ->
  NewState = update_workers(WorkersList, NewStateNum, CurLoad, AvgLoad, Modules),
  {noreply, NewState#dispatcher_state{request_map = RequestMap}};

handle_cast({update_loads, CurLoad, AvgLoad}, State) ->
  {noreply, State#dispatcher_state{current_load = CurLoad, avg_load = AvgLoad}};

handle_cast(stop, State) ->
  {stop, normal, State};

handle_cast({addCallback, FuseId, Node, CallbacksNum}, State) ->
  add_callback(Node, FuseId),
  {noreply, State#dispatcher_state{callbacks_num = CallbacksNum}};

handle_cast({delete_callback, FuseId, Node, CallbacksNum}, State) ->
  delete_callback(Node, FuseId),
  {noreply, State#dispatcher_state{callbacks_num = CallbacksNum}};

handle_cast(_Msg, State) ->
  ?error("Error: wrong cast: ~p", [_Msg]),
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
handle_info(_Info, State) ->
  ?error("Error: wrong info: ~p", [_Info]),
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
  Result ::  term().
%% ====================================================================
get_nodes(Module, State) ->
  get_nodes_list(Module, State#dispatcher_state.modules).

%% get_nodes_list/2
%% ====================================================================
%% @doc Gets nodes were module is working.
-spec get_nodes_list(Module :: atom(), Modules :: list()) -> Result when
  Result ::  term().
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
-spec update_nodes(Module :: atom(), NewNodes:: term(), State :: term()) -> Result when
  Result ::  term().
%% ====================================================================
update_nodes(Module, NewNodes, State) ->
  Modules = State#dispatcher_state.modules,
  NewModules = update_nodes_list(Module, NewNodes, Modules, []),
  State#dispatcher_state{modules = NewModules}.

%% update_nodes_list/4
%% ====================================================================
%% @doc Updates information about nodes (were modules are working).
-spec update_nodes_list(Module :: atom(), NewNodes:: term(), Modules :: list(), TmpList :: list()) -> Result when
  Result ::  term().
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
  Result ::  term().
%% ====================================================================
get_worker_node(Module, Request, State) ->
  case choose_node_by_map(Module, Request, State) of
    use_standard_mode ->
      Nodes = get_nodes(Module,State),
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
  Result ::  term().
%% ====================================================================
check_worker_node(Module, Request, State) ->
  case choose_node_by_map(Module, Request, State) of
    use_standard_mode ->
      Nodes = get_nodes(Module,State),
      case Nodes of
        {L1, L2} ->
          ThisNode = node(),
          Check = (lists:member(ThisNode, lists:flatten([L1, L2]))),
          case Check of
            true ->
              Check2 = ((State#dispatcher_state.current_load =< 3) and (State#dispatcher_state.current_load =< 2*State#dispatcher_state.avg_load)),
              case Check2 of
                true -> {ThisNode, State};
                false ->
%%               lager:warning([{mod, ?MODULE}], "Load of node too high", [Module]),
                  {N, NewLists} = choose_worker(L1, L2),
                  {N, update_nodes(Module, NewLists, State)}
              end;
            false ->
%%           lager:warning([{mod, ?MODULE}], "Module ~p does not work at this node", [Module]),
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
  Result ::  term().
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
  Result ::  {ok, NewState} | Error,
  Error :: term(),
  NewState :: term().
%% ====================================================================
add_worker(Module, Node, State) ->
  Nodes = get_nodes(Module,State),
  case Nodes of
    {L1, L2} ->
      {ok, update_nodes(Module, {[Node | L1], L2}, State)};
    Other -> Other
  end.

%% update_workers/5
%% ====================================================================
%% @doc Updates dispatcher state when new workers list appears.
-spec update_workers(WorkersList :: term(), SNum :: integer(), CLoad :: number(), ALoad :: number(), Modules :: list()) -> Result when
  Result ::  term().
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
-spec pull_state(State :: term()) -> Result when
  Result :: {ok, NewState} | {error, State},
  State :: term(),
  NewState :: term().
%% ====================================================================
pull_state(State) ->
  try
    {WorkersList, StateNum} = gen_server:call({global, ?CCM}, get_workers, 1000),
    NewState = update_workers(WorkersList, State#dispatcher_state.state_num, State#dispatcher_state.current_load, State#dispatcher_state.avg_load, ?Modules),
    {ok, NewState#dispatcher_state{state_num = StateNum}}
  catch
    _:_ ->
      lager:error([{mod, ?MODULE}], "Dispatcher on node: ~s: can not pull workers list", [node()]),
      {error, State}
  end.

%% get_workers/2
%% ====================================================================
%% @doc Returns information about nodes were module is working according
%% to dispatcher knowledge.
-spec get_workers(Module :: atom, State :: term()) -> Result when
  Result :: term().
%% ====================================================================
get_workers(Module, State) ->
  Nodes = get_nodes(Module,State),
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
    [{Module, {[],[]}} | TmpList]
  end,
  NewModules = lists:foldl(CreateModules, [], Modules),
  #dispatcher_state{modules = NewModules}.

%% initState/4
%% ====================================================================
%% @doc Initializes new record #dispatcher_state
-spec initState(SNum :: integer(), CLoad :: number(), ALoad :: number(), Modules :: list()) -> Result when
  Result :: record().
%% ====================================================================
initState(SNum, CLoad, ALoad, Modules) ->
  NewState = initState(Modules),
  NewState#dispatcher_state{state_num = SNum, current_load = CLoad, avg_load = ALoad}.

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
        true -> ok;
        false ->
          ets:insert(?CALLBACKS_TABLE, {Fuse, [Node | OldCallbacksList]}),
          updated
      end;
    _ ->
      ets:insert(?CALLBACKS_TABLE, {Fuse, [Node]}),
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
              updated;
            _ ->
              ets:insert(?CALLBACKS_TABLE, {Fuse, lists:delete(Node, OldCallbacksList)}),
              updated
          end;
        false -> not_exists
      end;
    _ -> not_exists
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
      lager:error([{mod, ?MODULE}], "Dispatcher on node: ~s: can not pull callbacks list", [node()]),
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

%% send_to_fuse/3
%% ====================================================================
%% @doc Sends message to fuse
-spec send_to_fuse(FuseId :: string(), Message :: term(), MessageDecoder :: string()) -> Result when
  Result :: callback_node_not_found | node_manager_error | dispatcher_error | ok | term().
%% ====================================================================
send_to_fuse(Fuse, Message, MessageDecoder) ->
  send_to_fuse(Fuse, Message, MessageDecoder, 3).

%% send_to_fuse/4
%% ====================================================================
%% @doc Sends message to fuse
  -spec send_to_fuse(FuseId :: string(), Message :: term(), MessageDecoder :: string(), SendNum :: integer()) -> Result when
Result :: callback_node_not_found | node_manager_error | dispatcher_error | ok | term().
%% ====================================================================
send_to_fuse(Fuse, Message, MessageDecoder, SendNum) ->
  Ans = try
    Node = gen_server:call(?Dispatcher_Name, {get_callback, Fuse}, 500),
    case Node of
      not_found ->
        callback_node_not_found;
      _ ->
        try
          Callback = gen_server:call({?Node_Manager_Name, Node}, {get_callback, Fuse}, 1000),
          case Callback of
            non -> channel_not_found;
            _ ->
              case get(callback_msg_ID) of
                ID when is_integer(ID) ->
                  put(callback_msg_ID, ID + 1);
                _ -> put(callback_msg_ID, 0)
              end,
              MsgID = get(callback_msg_ID),
              Callback ! {self(), Message, MessageDecoder, MsgID},
              receive
                {Callback, MsgID, Response} -> Response
              after 500 ->
                socket_error
              end
          end
        catch
          E1:E2 ->
            lager:error([{mod, ?MODULE}], "Can not get callback from node: ~p to fuse ~p, error: ~p:~p", [Node, Fuse, E1, E2]),
            node_manager_error
        end
    end
  catch
    _:_ ->
      lager:error([{mod, ?MODULE}], "Can not get callback node of fuse: ~p", [Fuse]),
      dispatcher_error
  end,
  case Ans of
    ok -> ok;
    _ ->
      case SendNum of
        0 -> Ans;
        _ ->
          send_to_fuse(Fuse, Message, MessageDecoder, SendNum - 1)
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
                  lager:error("Dispatcher error for module ~p and request ~p: ~p:~p ~n ~p", [Module, Msg, Type, Error, erlang:get_stacktrace()]),
                  use_standard_mode
              end
          end
      end
  end.

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
                      lager:error("Dispatcher (request map) wrong answer for module ~p and request ~p: ~p", [Task, Message, WrongAns]),
                      random:seed(now()),
                      Node4 = lists:nth(random:uniform(ModulesListLength), ModulesList),
                      gen_server:cast({Task, Node4}, Message)
                  end
                catch
                  Type:Error ->
                    lager:error("Dispatcher (request map) error for module ~p and request ~p: ~p:~p ~n ~p", [Task, Message, Type, Error, erlang:get_stacktrace()]),
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
