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

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/0]).

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
-spec start_link() -> Result when
  Result ::  {ok,Pid}
  | ignore
  | {error,Error},
  Pid :: pid(),
  Error :: {already_started,Pid} | term().
%% ====================================================================

start_link() ->
  gen_server:start_link({local, ?Dispatcher_Name}, ?MODULE, [], []).

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
init([]) ->
  {ok, #dispatcher_state{}}.

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
handle_call(update_state, _From, State) ->
  {Ans, NewState} = pull_state(State),
  {reply, Ans, NewState};

handle_call({Task, ProtocolVersion, AnsPid, Request}, _From, State) ->
  Ans = get_worker_node(Task, State),
  case Ans of
    {Node, NewState} ->
      case Node of
        non -> {reply, worker_not_found, State};
        N ->
          gen_server:cast({Task, Node}, {synch, ProtocolVersion, Request, disp_call, {proc, AnsPid}}),
          {reply, ok, NewState}
      end;
    Other -> {reply, Other, State}
   end;

handle_call({Task, ProtocolVersion, Request}, _From, State) ->
  Ans = get_worker_node(Task, State),
  case Ans of
    {Node, NewState} ->
      case Node of
        non -> {reply, worker_not_found, State};
        N ->
          gen_server:cast({Task, Node}, {asynch, ProtocolVersion, Request}),
          {reply, ok, NewState}
      end;
    Other -> {reply, Other, State}
  end;

handle_call(_Request, _From, State) ->
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
handle_cast({update_workers, WorkersList}, State) ->
  {noreply, update_workers(WorkersList, State)};

handle_cast(_Msg, State) ->
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

get_nodes(cluster_rengine, State) ->
  {L1, L2} = ?get_workers(cluster_rengine, State);
get_nodes(control_panel, State) ->
  {L1, L2} = ?get_workers(control_panel, State);
get_nodes(dao, State) ->
  {L1, L2} = ?get_workers(dao, State);
get_nodes(fslogic, State) ->
  {L1, L2} = ?get_workers(fslogic, State);
get_nodes(gateway, State) ->
  {L1, L2} = ?get_workers(gateway, State);
get_nodes(rtransfer, State) ->
  {L1, L2} = ?get_workers(rtransfer, State);
get_nodes(rule_manager, State) ->
  {L1, L2} = ?get_workers(rule_manager, State);
get_nodes(_Other, _State) ->
  wrong_worker_type.

update_nodes(cluster_rengine, NewNodes, State) ->
  ?update_workers(cluster_rengine, NewNodes, State);
update_nodes(control_panel, NewNodes, State) ->
  ?update_workers(control_panel, NewNodes, State);
update_nodes(dao, NewNodes, State) ->
  ?update_workers(dao, NewNodes, State);
update_nodes(fslogic, NewNodes, State) ->
  ?update_workers(fslogic, NewNodes, State);
update_nodes(gateway, NewNodes, State) ->
  ?update_workers(gateway, NewNodes, State);
update_nodes(rtransfer, NewNodes, State) ->
  ?update_workers(rtransfer, NewNodes, State);
update_nodes(rule_manager, NewNodes, State) ->
  ?update_workers(rule_manager, NewNodes, State);
update_nodes(_Other, _NewNodes, State) ->
  State.

get_worker_node(Module, State) ->
  Nodes = get_nodes(Module,State),
  case Nodes of
    {L1, L2} ->
      {N, NewLists} = choose_worker(L1, L2),
      {N, update_nodes(Module, NewLists, State)};
    Other -> Other
  end.

choose_worker([], []) ->
  {non, {[], []}};
choose_worker([], L2) ->
  choose_worker(L2, []);
choose_worker([N | L1], L2) ->
  {N, {L1, [N, L2]}}.

add_worker(Module, Node, State) ->
  Nodes = get_nodes(Module,State),
  case Nodes of
    {L1, L2} ->
      {ok, update_nodes(Module, {[Node, L1], L2}, State)};
    Other -> Other
  end.

update_workers(WorkersList, _State) ->
  Update = fun({Node, Module}, TmpState) ->
    Ans = add_worker(Module, Node, TmpState),
      case Ans of
      {ok, NewState} -> NewState;
      _Other -> TmpState
    end
  end,
  lists:foldl(Update, #dispatcher_state{}, WorkersList).

pull_state(State) ->
  try
    WorkersList = gen_server:call({global, ?CCM}, get_workers),
    {ok, update_workers(WorkersList, State)}
  catch
    _:_ ->
      lager:error([{mod, ?MODULE}], "Dispatcher on node: ~s: can not pull workers list", [node()]),
      {error, State}
  end.