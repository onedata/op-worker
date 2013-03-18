%% @author Michal
%% @doc @todo Add description to worker_host.

-module(worker_host).
-behaviour(gen_server).
-include("records.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/3]).

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
    Reply = loadInfo(State#host_state.load_info),
    {reply, Reply, State};

handle_call(clearLoadInfo, _From, State) ->
	{_New, _Old, _NewListSize, Max} = State#host_state.load_info,
    Reply = ok,
    {reply, Reply, State#host_state{load_info = {[], [], 0, Max}}};

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
handle_cast({synch, ProtocolVersion, Msg, MsgId, ReplyDisp}, State) ->
	PlugIn = State#host_state.plug_in,
	spawn(fun() -> procRequest(PlugIn, ProtocolVersion, Msg, MsgId, ReplyDisp) end),	
	{noreply, State};

handle_cast({asynch, ProtocolVersion, Msg}, State) ->
	PlugIn = State#host_state.plug_in,
	spawn(fun() -> procRequest(PlugIn, ProtocolVersion, Msg, non, non) end),	
	{noreply, State};

handle_cast({progress_report, Report}, State) ->
	NewLoadInfo = saveProgress(Report, State#host_state.load_info),
    {noreply, State#host_state{load_info = NewLoadInfo}};

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

%% procRequest/5
%% ====================================================================
%% @doc Processes client request using PlugIn:handle function. Afterwards,
%% it sends the answer to dispatcher and logs info about processing time.
-spec procRequest(PlugIn :: atom(), ProtocolVersion :: integer(), Msg :: term(), MsgId :: integer(), ReplyDisp :: term()) -> Result when
	Result ::  atom(). 
%% ====================================================================
procRequest(PlugIn, ProtocolVersion, Msg, MsgId, ReplyDisp) ->
	{Megaseconds,Seconds,Microseconds} = os:timestamp(),
	Response = 	try
		PlugIn:handle(ProtocolVersion, Msg)
	catch
		_:_ -> wrongTask
	end,
	
	case ReplyDisp of
		non -> ok;
		Disp -> gen_server:cast(Disp, {worker_answer, MsgId, Response})
	end,
	
	{Megaseconds2,Seconds2,Microseconds2} = os:timestamp(),
	Time = 1000000*1000000*(Megaseconds2-Megaseconds) + 1000000*(Seconds2-Seconds) + Microseconds2-Microseconds,
	gen_server:cast({local, PlugIn}, {progress_report, {{Megaseconds,Seconds,Microseconds}, Time}}),
	ok.

%% saveProgress/2
%% ====================================================================
%% @doc Adds information about ended request to host memory (ccm uses
%% it to control cluster load).
-spec saveProgress(Report :: term(), LoadInfo :: term()) -> NewLoadInfo when
	NewLoadInfo ::  term(). 
%% ====================================================================
saveProgress(Report, {New, Old, NewListSize, Max}) ->
	case NewListSize + 1 of
		Max ->
			{[], [Report | New], 0, Max};
		S ->
			{[Report | New], Old, S, Max}
	end.

%% loadInfo/1
%% ====================================================================
%% @doc Provides averaged information about last requests.
-spec loadInfo(LoadInfo :: term()) -> Result when
	Result ::  term(). 
%% ====================================================================
loadInfo({New, Old, NewListSize, Max}) ->
	Load = lists:sum(lists:map(fun({_Time, Load}) -> Load end, New)) + lists:sum(lists:map(fun({_Time, Load}) -> Load end, lists:sublist(Old, Max-NewListSize))),
	{Time, _Load} = case {New, Old} of
		{[], []} -> {os:timestamp(), []};
		{[], _O} -> [H | _R] = Old,
			H;
		_L -> [H | _R] = New,
			H
	end,
	{Time, Load}.