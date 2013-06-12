%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module hosts all VeilFS modules (fslogic, cluster_rengin etc.).
%% It makes it easy to manage modules and provides some basic functionality
%% for its plug-ins (VeilFS modules) e.g. requests management.
%% @end
%% ===================================================================

-module(worker_host).
-behaviour(gen_server).
-include("records.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/3, stop/1]).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================

%% start_link/3
%% ====================================================================
%% @doc Starts host with apropriate plug-in
-spec start_link(PlugIn, PlugInArgs, LoadMemorySize) -> Result when
	PlugIn :: atom(),
	PlugInArgs :: any(),
	LoadMemorySize :: integer(),
	Result ::  {ok,Pid} 
			| ignore 
			| {error,Error},
	Pid :: pid(),
	Error :: {already_started,Pid} | term().
%% ====================================================================
start_link(PlugIn, PlugInArgs, LoadMemorySize) ->
    gen_server:start_link({local, PlugIn}, ?MODULE, [PlugIn, PlugInArgs, LoadMemorySize], []).

%% stop/1
%% ====================================================================
%% @doc Stops the server
-spec stop(PlugIn) -> ok when
  PlugIn :: atom().
%% ====================================================================

stop(PlugIn) ->
  gen_server:cast(PlugIn, stop).

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
init([PlugIn, PlugInArgs, LoadMemorySize]) ->
    process_flag(trap_exit, true),
    {ok, #host_state{plug_in = PlugIn, plug_in_state = PlugIn:init(PlugInArgs), load_info = {[], [], 0, LoadMemorySize}}}.

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
handle_call(getPlugIn, _From, State) ->
    Reply = State#host_state.plug_in,
    {reply, Reply, State};

handle_call({updatePlugInState, NewPlugInState}, _From, State) ->
    {reply, ok, State#host_state{plug_in_state = NewPlugInState}};

handle_call(getPlugInState, _From, State) ->
    {reply, State#host_state.plug_in_state, State};

handle_call(getLoadInfo, _From, State) ->
    Reply = load_info(State#host_state.load_info),
    {reply, Reply, State};

handle_call(getFullLoadInfo, _From, State) ->
    {reply, State#host_state.load_info, State};

handle_call(clearLoadInfo, _From, State) ->
	{_New, _Old, _NewListSize, Max} = State#host_state.load_info,
    Reply = ok,
    {reply, Reply, State#host_state{load_info = {[], [], 0, Max}}};

handle_call({test_call, ProtocolVersion, Msg}, _From, State) ->
  PlugIn = State#host_state.plug_in,
  Reply = PlugIn:handle(ProtocolVersion, Msg),
  {reply, Reply, State};

handle_call(Request, _From, State) when is_tuple(Request) -> %% Proxy call. Each cast can be achieved by instant proxy-call which ensures
                                                             %% that request was made, unlike cast because cast ignores state of node/gen_server
    {reply, gen_server:cast(State#host_state.plug_in, Request), State};

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
handle_cast({synch, ProtocolVersion, Msg, MsgId, ReplyTo}, State) ->
	PlugIn = State#host_state.plug_in,
	spawn(fun() -> proc_request(PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo) end),
	{noreply, State};

handle_cast({asynch, ProtocolVersion, Msg}, State) ->
	PlugIn = State#host_state.plug_in,
	spawn(fun() -> proc_request(PlugIn, ProtocolVersion, Msg, non, non) end),	
	{noreply, State};

handle_cast({sequential_synch, ProtocolVersion, Msg, MsgId, ReplyTo}, State) ->
    PlugIn = State#host_state.plug_in,
    Job = fun() -> proc_request(PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo) end,
    gen_server:cast(State#host_state.plug_in, {sequential, job_check}), %% Process run queue
    {noreply, State#host_state{seq_queue = State#host_state.seq_queue ++ [Job]}};

handle_cast({sequential_asynch, ProtocolVersion, Msg}, State) ->
    PlugIn = State#host_state.plug_in,
    Job = fun() -> proc_request(PlugIn, ProtocolVersion, Msg, non, non) end,
    gen_server:cast(State#host_state.plug_in, {sequential, job_check}), %% Process run queue
    {noreply, State#host_state{seq_queue = State#host_state.seq_queue ++ [Job]}};

handle_cast({sequential, job_end}, State) ->
    gen_server:cast(State#host_state.plug_in, {sequential, job_check}), %% Process run queue
    {noreply, State#host_state{current_seq_job = none}};  %% Clear current task info

handle_cast({sequential, job_check}, State) ->
    {CurrentJob, _RunQueue} = SeqState = {State#host_state.current_seq_job, State#host_state.seq_queue},
    IsAlive = is_pid(CurrentJob) andalso is_process_alive(CurrentJob),
    case SeqState of
        {_Value, _Queue} when IsAlive -> %% We're running some process right now -> we should just return and let it finish
            {noreply, State};
        {_, []} -> %% There is no process currently running, but the run queue is empty -> we should let it be as it is
            {noreply, State};
        {_, [ Job | Queue ]} when is_function(Job) -> %% There is no process currently running, we can start queued job
            Pid = spawn(fun() -> Job(), gen_server:cast(State#host_state.plug_in, {sequential, job_end}) end),
            {noreply, State#host_state{current_seq_job = Pid, seq_queue = Queue}};
        {Value, Queue}->  %% Unknown state
            lager:error([{mod, ?MODULE}], "Unknown worker sequential run queue state: current job: ~p, run queue: ~p", [Value, Queue]),
            {noreply, State}
    end;

handle_cast({progress_report, Report}, State) ->
	NewLoadInfo = save_progress(Report, State#host_state.load_info),
    {noreply, State#host_state{load_info = NewLoadInfo}};

handle_cast(stop, State) ->
  {stop, normal, State};

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
handle_info(Info, {PlugIn, State}) ->
	PlugIn = State#host_state.plug_in,
    {_Reply, NewPlugInState} = PlugIn:handle(Info, State#host_state.plug_in_state),
    {noreply, State#host_state{plug_in_state = NewPlugInState}}.


%% terminate/2
%% ====================================================================
%% @doc <a href="http://www.erlang.org/doc/man/gen_server.html#Module:terminate-2">gen_server:terminate/2</a>
-spec terminate(Reason, State :: term()) -> Any :: term() when
	Reason :: normal
			| shutdown
			| {shutdown, term()}
			| term().
%% ====================================================================
terminate(_Reason, #host_state{plug_in = PlugIn}) ->
    PlugIn:cleanup(),
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

%% proc_request/5
%% ====================================================================
%% @doc Processes client request using PlugIn:handle function. Afterwards,
%% it sends the answer to dispatcher and logs info about processing time.
-spec proc_request(PlugIn :: atom(), ProtocolVersion :: integer(), Msg :: term(), MsgId :: integer(), ReplyDisp :: term()) -> Result when
	Result ::  atom(). 
%% ====================================================================
proc_request(PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo) ->
	{Megaseconds,Seconds,Microseconds} = os:timestamp(),
	Response = 	try
		PlugIn:handle(ProtocolVersion, Msg)
	catch
		_:_ -> wrongTask
	end,

	case ReplyTo of
		non -> ok;
    {gen_serv, Disp} -> gen_server:cast(Disp, {worker_answer, MsgId, Response});
    {proc, Pid} -> Pid ! Response;
    Other -> lagger:error("Wrong reply type: ~s", [Other])
	end,
	
	{Megaseconds2,Seconds2,Microseconds2} = os:timestamp(),
	Time = 1000000*1000000*(Megaseconds2-Megaseconds) + 1000000*(Seconds2-Seconds) + Microseconds2-Microseconds,
	gen_server:cast(PlugIn, {progress_report, {{Megaseconds,Seconds,Microseconds}, Time}}).

%% save_progress/2
%% ====================================================================
%% @doc Adds information about ended request to host memory (ccm uses
%% it to control cluster load).
-spec save_progress(Report :: term(), LoadInfo :: term()) -> NewLoadInfo when
	NewLoadInfo ::  term(). 
%% ====================================================================
save_progress(Report, {New, Old, NewListSize, Max}) ->
	case NewListSize + 1 of
		Max ->
			{[], [Report | New], 0, Max};
		S ->
			{[Report | New], Old, S, Max}
	end.

%% load_info/1
%% ====================================================================
%% @doc Provides averaged information about last requests.
-spec load_info(LoadInfo :: term()) -> Result when
	Result ::  term(). 
%% ====================================================================
load_info({New, Old, NewListSize, Max}) ->
	Load = lists:sum(lists:map(fun({_Time, Load}) -> Load end, New)) + lists:sum(lists:map(fun({_Time, Load}) -> Load end, lists:sublist(Old, Max-NewListSize))),
	{Time, _Load} = case {New, Old} of
		{[], []} -> {os:timestamp(), []};
		{_O, []} -> lists:last(New);
		_L -> case NewListSize of
			Max -> lists:last(New);
			_S -> lists:nth(Max-NewListSize, Old)
		end	
	end,
	{Time, Load}.