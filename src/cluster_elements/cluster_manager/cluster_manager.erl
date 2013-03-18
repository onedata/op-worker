%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module coordinates central cluster.
%% @end
%% ===================================================================

-module(cluster_manager).
-behaviour(gen_server).
-include("registered_names.hrl").
-include("records.hrl").
-include("supervision_macros.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/0]).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% Test API
%% ====================================================================
%%-ifdef(TEST).
-export([start_worker/4, stop_worker/3]).
%%-endif.

%% ====================================================================
%% API functions
%% ====================================================================

%% start_link/0
%% ====================================================================
%% @doc Starts cluster manager
-spec start_link() -> Result when
	Result ::  {ok,Pid} 
			| ignore 
			| {error,Error},
	Pid :: pid(),
	Error :: {already_started,Pid} | term().
%% ====================================================================
start_link() ->
    Ans = gen_server:start_link(?MODULE, [], []),
	case Ans of
		{ok, Pid} -> global:re_register_name(?CCM, Pid);
		_A -> error
	end,
	Ans.

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
	{ok, Interval} = application:get_env(veil_cluster_node, initialization_time),
	timer:apply_after(Interval * 1000, gen_server, cast, [{global, ?CCM}, init_cluster]),
	{ok, #cm_state{}}.

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
handle_call({node_is_up, Node}, _From, State) ->
	Nodes = State#cm_state.nodes,
	case lists:member(Node, Nodes) of
		true -> {reply, ok, State};
		false -> {reply, ok, State#cm_state{nodes = [Node | Nodes]}}
	end;

handle_call(get_nodes, _From, State) ->
	{reply, State#cm_state.nodes, State};

handle_call(get_state, _From, State) ->
	{reply, State, State};

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
handle_cast(init_cluster, State) ->
	NewState = init_cluster(State),
	{noreply, NewState};

handle_cast(check_cluster_state, State) ->
	NewState = check_cluster_state(State),
	{noreply, NewState};

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

%% init_cluster/1
%% ====================================================================
%% @doc Initializes cluster - decides at which nodes components should
%% be started (and starts them). Additionally, it sets timer that
%% initiates checking of cluster state.
-spec init_cluster(State :: term()) -> NewState when
	NewState ::  term(). 
%% ====================================================================
init_cluster(State) ->
	plan_next_cluster_state_check(),
	State.

%% check_cluster_state/1
%% ====================================================================
%% @doc Checks cluster state and decides if any new component should
%% be started (currently running ones are overloaded) or stopped.
-spec check_cluster_state(State :: term()) -> NewState when
	NewState ::  term(). 
%% ====================================================================
check_cluster_state(State) ->
	plan_next_cluster_state_check(),
	State.

%% plan_next_cluster_state_check/0
%% ====================================================================
%% @doc Decides when cluster state should be checked next time and sets
%% the timer (cluster_clontrol_period environment variable is used).
-spec plan_next_cluster_state_check() -> Result when
	Result :: {ok, TRef} | {error, Reason},
	TRef :: term(),
	Reason :: term().
%% ====================================================================
plan_next_cluster_state_check() ->
	{ok, Interval} = application:get_env(veil_cluster_node, cluster_clontrol_period),
	timer:apply_after(Interval * 1000, gen_server, cast, [{global, ?CCM}, check_cluster_state]).

%% start_worker/4
%% ====================================================================
%% @doc Processes client request using PlugIn:handle function. Afterwards,
%% it sends the answer to dispatcher and logs info about processing time.
-spec start_worker(Node :: atom(), Module :: atom(), WorkerArgs :: term(), State :: term()) -> Result when
	Result :: {Answer, NewState},
	Answer :: ok | error,
	NewState :: term().
%% ====================================================================
start_worker(Node, Module, WorkerArgs, State) ->
	try
		{ok, LoadMemorySize} = application:get_env(veil_cluster_node, worker_load_memory_size),
		{ok, ChildPid} = supervisor:start_child({?Supervisor_Name, Node}, ?Sup_Child(Module, worker_host, transient, [Module, WorkerArgs, LoadMemorySize])),
		Workers = State#cm_state.workers,
		{ok, State#cm_state{workers = [{Node, Module, ChildPid} | Workers]}}
	catch
		_:_ -> {error, State}
	end.

%% stop_worker/3
%% ====================================================================
%% @doc Processes client request using PlugIn:handle function. Afterwards,
%% it sends the answer to dispatcher and logs info about processing time.
-spec stop_worker(Node :: atom(), Module :: atom(), State :: term()) -> Result when
	Result :: {Answer, NewState},
	Answer :: ok | child_does_not_exist | delete_error | termination_error,
	NewState :: term().
%% ====================================================================
stop_worker(Node, Module, State) ->
	CreateNewWorkersList = fun({N, M, Child}, {Workers, ChosenChild}) ->
		case {N, M} of
			{Node, Module} -> {Workers, {N, Child}};
			{_N2, _M2} -> {[{N, M, Child} | Workers], ChosenChild}
		end
	end,
	Workers = State#cm_state.workers,
	{NewWorkers, ChosenChild} = lists:foldl(CreateNewWorkersList, {[], non}, Workers),
	Ans = case ChosenChild of
		non -> child_does_not_exist;
		{ChildNode, _ChildPid} -> 
			Ans2 = supervisor:terminate_child({?Supervisor_Name, ChildNode}, Module),
			case Ans2 of
				ok -> Ans3 = supervisor:delete_child({?Supervisor_Name, ChildNode}, Module),
					case Ans3 of
						ok -> ok;
						{error, _Error} -> delete_error
					end;
				{error, _Error} -> termination_error
			end
	end,
	{Ans, State#cm_state{workers = NewWorkers}}.
