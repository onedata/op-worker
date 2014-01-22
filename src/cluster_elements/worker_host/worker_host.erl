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
-include("registered_names.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").

-define(BORTER_CHILD_WAIT_TIME, 10000).
-define(MAX_CHILD_WAIT_TIME, 60000000).
-define(MAX_CALCULATION_WAIT_TIME, 10000000).

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/3, stop/1, start_sub_proc/5, generate_sub_proc_list/1, generate_sub_proc_list/5]).
-export([create_simple_cache/3, create_simple_cache/4, create_simple_cache/5]).

%% ====================================================================
%% Test API
%% ====================================================================
-ifdef(TEST).
-export([stop_all_sub_proc/1]).
-endif.

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
    InitAns = PlugIn:init(PlugInArgs),
    case InitAns of
      IDesc when is_record(IDesc, initial_host_description) ->
        DispatcherRequestMapState = case InitAns#initial_host_description.request_map of
          non -> true;
          _ ->
            Pid = self(),
            erlang:send_after(200, Pid, {timer, register_disp_map}),
            false
        end,
        {ok, #host_state{plug_in = PlugIn, request_map = InitAns#initial_host_description.request_map, sub_procs = InitAns#initial_host_description.sub_procs,
        dispatcher_request_map = InitAns#initial_host_description.dispatcher_request_map, dispatcher_request_map_ok = DispatcherRequestMapState, plug_in_state = InitAns#initial_host_description.plug_in_state, load_info = {[], [], 0, LoadMemorySize}}};
      _ -> {ok, #host_state{plug_in = PlugIn, plug_in_state = InitAns, load_info = {[], [], 0, LoadMemorySize}}}
    end.

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

handle_call(dispatcher_map_unregistered, _From, State) ->
  Pid = self(),
  DMapState = case State#host_state.request_map of
                non -> true;
                _ ->
                  erlang:send_after(200, Pid, {timer, register_disp_map}),
                  false
              end,
  {reply, ok, State#host_state{dispatcher_request_map_ok = DMapState}};

%% For tests
handle_call({register_sub_proc, Name, MaxDepth, MaxWidth, ProcFun, MapFun, RM, DM}, _From, State) ->
  Pid = self(),
  SubProcList = worker_host:generate_sub_proc_list(Name, MaxDepth, MaxWidth, ProcFun, MapFun),
  erlang:send_after(200, Pid, {timer, register_disp_map}),
  {reply, ok, State#host_state{request_map = RM, sub_procs = SubProcList,
  dispatcher_request_map = DM, dispatcher_request_map_ok = false}};

handle_call(Request, _From, State) when is_tuple(Request) -> %% Proxy call. Each cast can be achieved by instant proxy-call which ensures
                                                             %% that request was made, unlike cast because cast ignores state of node/gen_server
    {reply, gen_server:cast(State#host_state.plug_in, Request), State};

%% Test call
handle_call(check, _From, State) ->
  {reply, ok, State};

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
  NewSubProcList = proc_standard_request(State#host_state.request_map, State#host_state.sub_procs, PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo),
	{noreply, State#host_state{sub_procs = NewSubProcList}};

handle_cast({synch, ProtocolVersion, Msg, ReplyTo}, State) ->
  PlugIn = State#host_state.plug_in,
  NewSubProcList = proc_standard_request(State#host_state.request_map, State#host_state.sub_procs, PlugIn, ProtocolVersion, Msg, non, ReplyTo),
  {noreply, State#host_state{sub_procs = NewSubProcList}};

handle_cast({asynch, ProtocolVersion, Msg}, State) ->
	PlugIn = State#host_state.plug_in,
  NewSubProcList = proc_standard_request(State#host_state.request_map, State#host_state.sub_procs, PlugIn, ProtocolVersion, Msg, non, non),
	{noreply, State#host_state{sub_procs = NewSubProcList}};

handle_cast({sequential_synch, ProtocolVersion, Msg, MsgId, ReplyTo}, State) ->
    PlugIn = State#host_state.plug_in,
    Job = fun() -> proc_request(PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo) end,
    gen_server:cast(State#host_state.plug_in, {sequential, job_check}), %% Process run queue
    {noreply, State#host_state{seq_queue = State#host_state.seq_queue ++ [Job]}};

handle_cast({sequential_synch, ProtocolVersion, Msg, ReplyTo}, State) ->
  PlugIn = State#host_state.plug_in,
  Job = fun() -> proc_request(PlugIn, ProtocolVersion, Msg, non, ReplyTo) end,
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

handle_cast(register_disp_map, State) ->
  case State#host_state.dispatcher_request_map_ok of
    true -> ok;
    _ ->
     Pid = self(),
     erlang:send_after(10000, Pid, {timer, register_disp_map}),
     gen_server:cast({global, ?CCM}, {register_dispatcher_map, State#host_state.plug_in, State#host_state.dispatcher_request_map, Pid})
  end,
  {noreply, State};

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
handle_info({timer, Msg}, State) ->
  PlugIn = State#host_state.plug_in,
  gen_server:cast(PlugIn, Msg),
  {noreply, State};

handle_info({clear_sipmle_cache, LoopTime, Fun, StrongCacheConnection}, State) ->
  Pid = self(),
  erlang:send_after(LoopTime, Pid, {clear_sipmle_cache, LoopTime, Fun}),
  case StrongCacheConnection of
    true ->
      spawn_link(fun() -> Fun() end);
    _ ->
      spawn(fun() -> Fun() end)
  end,
  {noreply, State};

handle_info(dispatcher_map_registered, State) ->
  {noreply, State#host_state{dispatcher_request_map_ok = true}};

handle_info(Info, State) ->
	PlugIn = State#host_state.plug_in,
    {_Reply, NewPlugInState} = PlugIn:handle(Info, State#host_state.plug_in_state), %% TODO: fix me ! There's no such callback in worker_plugin
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
terminate(_Reason, #host_state{plug_in = PlugIn, sub_procs = SubProcs}) ->
    PlugIn:cleanup(),
    stop_all_sub_proc(SubProcs),
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
-spec proc_request(PlugIn :: atom(), ProtocolVersion :: integer(), Msg :: term(), MsgId :: term(), ReplyDisp :: term()) -> Result when
	Result ::  atom(). 
%% ====================================================================
proc_request(PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo) ->
	BeforeProcessingRequest = os:timestamp(),
  Request = preproccess_msg(Msg),
	Response = 	try
    PlugIn:handle(ProtocolVersion, Request)
	catch
    Type:Error ->
      lager:error("Worker plug-in ~p error: ~p:~p ~n ~p", [PlugIn, Type, Error, erlang:get_stacktrace()]),
      worker_plug_in_error
	end,
  send_response(PlugIn, BeforeProcessingRequest, Response, MsgId, ReplyTo).

%% proc_request/7
%% ====================================================================
%% @doc Processes client request using PlugIn:handle function or delegates it request to sub proccess. Afterwards,
%% it sends the answer to dispatcher and logs info about processing time.
-spec proc_standard_request(RequestMap :: term(), SubProcs :: list(), PlugIn :: atom(), ProtocolVersion :: integer(), Msg :: term(), MsgId :: term(), ReplyDisp :: term()) -> Result when
  Result ::  list().
%% ====================================================================
proc_standard_request(RequestMap, SubProcs, PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo) ->
  case RequestMap of
    non ->
      spawn(fun() -> proc_request(PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo) end),
      SubProcs;
    _ ->
      try
        {SubProcArgs, SubProcPid} = proplists:get_value(RequestMap(Msg), SubProcs, {not_found, not_found}),
        case SubProcArgs of
          not_found ->
            spawn(fun() -> proc_request(PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo) end),
            SubProcs;
          _ ->
            SubProcPid ! {PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo},
            %% check if chosen proc did not time out before message was delivered
            case process_info(SubProcPid) of
              undefined->
                {Name, MaxDepth, MaxWidth, NewProcFun, NewMapFun} = SubProcArgs,
                SubProcPid2 = start_sub_proc(Name, MaxDepth, MaxWidth, NewProcFun, NewMapFun),
                SubProcPid2 ! {PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo},
                SubProc2Desc = {Name, {SubProcArgs, SubProcPid2}},
                [SubProc2Desc | proplists:delete(Name, SubProcs)];
              _ ->
                SubProcs
            end
        end
      catch
        Type:Error ->
          spawn(fun() ->
            BeforeProcessingRequest = os:timestamp(),
            lager:error("Worker plug-in ~p sub proc error: ~p:~p ~n ~p", [PlugIn, Type, Error, erlang:get_stacktrace()]),
            send_response(PlugIn, BeforeProcessingRequest, sub_proc_error, MsgId, ReplyTo)
          end),
          SubProcs
      end
  end.

%% preproccess_msg/1
%% ====================================================================
%% @doc Preproccesses client request.
-spec preproccess_msg(Msg :: term()) -> Result when
  Result ::  term().
%% ====================================================================
preproccess_msg(Msg) ->
    case Msg of
      #veil_request{subject = Subj, request = Msg1, fuse_id = FuseID} ->
        put(user_id, Subj),
        put(fuse_id, FuseID),
        Msg1;
      NotWrapped -> NotWrapped
    end.

%% send_response/5
%% ====================================================================
%% @doc Sends responce to client
-spec send_response(PlugIn :: atom(), BeforeProcessingRequest :: term(), Response :: term(), MsgId :: term(), ReplyDisp :: term()) -> Result when
  Result ::  atom().
%% ====================================================================
send_response(PlugIn, BeforeProcessingRequest, Response, MsgId, ReplyTo) ->
  case ReplyTo of
    non -> ok;
    {gen_serv, Serv} ->
      case MsgId of
        non -> gen_server:cast(Serv, Response);
        Id -> gen_server:cast(Serv, {worker_answer, Id, Response})
      end;
    {proc, Pid} ->
      case MsgId of
        non -> Pid ! Response;
        Id -> Pid ! {worker_answer, Id, Response}
      end;
    Other -> lagger:error("Wrong reply type: ~s", [Other])
  end,

  AfterProcessingRequest = os:timestamp(),
  Time = timer:now_diff(AfterProcessingRequest, BeforeProcessingRequest),
  gen_server:cast(PlugIn, {progress_report, {BeforeProcessingRequest, Time}}).

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

%% update_wait_time/2
%% ====================================================================
%% @doc Updates information about sub proc average waiting time.
-spec update_wait_time(WaitFrom :: term(), AvgWaitTime :: term()) -> Result when
  Result ::  {Now, NewAvgWaitTime},
  Now ::  term(),
  NewAvgWaitTime ::  term().
%% ====================================================================
update_wait_time(WaitFrom, AvgWaitTime) ->
  Now = os:timestamp(),
  TimeDifTmp = timer:now_diff(Now, WaitFrom),
  TimeDif = case TimeDifTmp > ?MAX_CALCULATION_WAIT_TIME of
    true -> ?MAX_CALCULATION_WAIT_TIME;
    false -> TimeDifTmp
  end,
  NewAvgWaitTime = TimeDif / 10 + AvgWaitTime * 9 / 10,
  {Now, NewAvgWaitTime}.

%% start_sub_proc/5
%% ====================================================================
%% @doc Starts sub proc
-spec start_sub_proc(Name :: atom(), MaxDepth :: integer(), MaxWidth :: integer(), ProcFun :: term(), MapFun :: term()) -> Result when
  Result ::  pid().
%% ====================================================================
start_sub_proc(Name, MaxDepth, MaxWidth, ProcFun, MapFun) ->
  spawn(fun() -> start_sub_proc(Name, 1, MaxDepth, MaxWidth, ProcFun, MapFun) end).

%% start_sub_proc/6
%% ====================================================================
%% @doc Starts sub proc
-spec start_sub_proc(Name :: atom(), SubProcDepth :: integer(), MaxDepth :: integer(), MaxWidth :: integer(), ProcFun :: term(), MapFun :: term()) -> Result when
  Result ::  pid().
%% ====================================================================
start_sub_proc(Name, SubProcDepth, MaxDepth, MaxWidth, ProcFun, MapFun) ->
  process_flag(trap_exit, true),
  ets:new(Name, [named_table, set, private]),
  sub_proc(Name, proc, SubProcDepth, MaxDepth, MaxWidth, os:timestamp(), ?MAX_CALCULATION_WAIT_TIME, ProcFun, MapFun).

%% sub_proc/9
%% ====================================================================
%% @doc Sub proc function
-spec sub_proc(Name :: atom(), ProcType:: atom(), SubProcDepth :: integer(), MaxDepth :: integer(), MaxWidth :: integer(),
    WaitFrom :: term(), AvgWaitTime :: term(), ProcFun :: term(), MapFun :: term()) -> Result when
  Result ::  ok | end_sub_proc.
%% ====================================================================
%% TODO Add memory clearing for old data
sub_proc(Name, ProcType, SubProcDepth, MaxDepth, MaxWidth, WaitFrom, AvgWaitTime, ProcFun, MapFun) ->
  receive
    {sub_proc_management, stop} ->
      del_sub_procs(ets:first(Name), Name);

    {'EXIT', ChildPid, _} ->
      ChildDesc = ets:lookup(Name, ChildPid),
      case ChildDesc of
        [{ChildPid, ChildNum}] ->
          ets:delete(Name, ChildPid),
          ets:delete(Name, ChildNum);
        _ ->
          lager:error([{mod, ?MODULE}], "Exit of unknown sub proc"),
          error
      end,
      sub_proc(Name, ProcType, SubProcDepth, MaxDepth, MaxWidth, WaitFrom, AvgWaitTime, ProcFun, MapFun);
    Request ->
      {Now, NewAvgWaitTime} = update_wait_time(WaitFrom, AvgWaitTime),
      NewProcType = case ProcType of
                      proc ->
                        case NewAvgWaitTime < ?BORTER_CHILD_WAIT_TIME of
                          true ->
                            case SubProcDepth < MaxDepth of
                              true ->
                                map;
                              false ->
                                proc
                            end;
                          false ->
                            proc
                        end;
                      map ->
                        case NewAvgWaitTime > ?BORTER_CHILD_WAIT_TIME * 10 of
                          true ->
                            proc;
                          false ->
                            map
                        end
                    end,

      case NewProcType of
        map ->
          {ForwardNum, ForwardPid} = map_to_sub_proc(Name, SubProcDepth, MaxDepth, MaxWidth, ProcFun, MapFun, Request),
          case ForwardNum of
            error -> error;
            _ ->
              ForwardPid ! Request,
              %% check if chosen proc did not time out before message was delivered
              case process_info(ForwardPid) of
                undefined->
                  ets:delete(Name, ForwardPid),
                  ets:delete(Name, ForwardNum),
                  ForwardPid2 = map_to_sub_proc(Name, SubProcDepth, MaxDepth, MaxWidth, ProcFun, MapFun, Request),
                  ForwardPid2 ! Request;
                _ ->
                  ok
              end
          end;
        proc ->
          ProcFun(Request)
      end,

      sub_proc(Name, NewProcType, SubProcDepth, MaxDepth, MaxWidth, Now, NewAvgWaitTime, ProcFun, MapFun)
  after ?MAX_CHILD_WAIT_TIME ->
    end_sub_proc
  end.

%% map_to_sub_proc/7
%% ====================================================================
%% @doc Maps request to sub proc pid
-spec map_to_sub_proc(Name :: atom(), SubProcDepth :: integer(), MaxDepth :: integer(), MaxWidth :: integer(),
   ProcFun :: term(), MapFun :: term(), Request :: term()) -> Result when
  Result ::  {SubProcNum, SubProcPid},
  SubProcNum :: integer(),
  SubProcPid :: term().
%% ====================================================================
map_to_sub_proc(Name, SubProcDepth, MaxDepth, MaxWidth, ProcFun, MapFun, Request) ->
  try
    RequestValue = calculate_proc_vale(SubProcDepth, MaxWidth, MapFun(Request)),
    RequestProc = ets:lookup(Name, RequestValue),
    case RequestProc of
      [] ->
        NewName = list_to_atom(atom_to_list(Name) ++ "_" ++ integer_to_list(RequestValue)),
        NewPid = spawn(fun() -> start_sub_proc(NewName, SubProcDepth + 1, MaxDepth, MaxWidth, ProcFun, MapFun) end),
        ets:insert(Name, {RequestValue, NewPid}),
        ets:insert(Name, {NewPid, RequestValue}),
        {RequestValue, NewPid};
      [{RequestValue, RequestProcPid}] ->
        {RequestValue, RequestProcPid};
      _ ->
        lager:error([{mod, ?MODULE}], "Sub proc error for request ~p", [Request]),
        {error, error}
    end
  catch
    _:_ ->
      lager:error([{mod, ?MODULE}], "Sub proc error for request ~p", [Request]),
      {error, error}
  end.

%% calculate_proc_vale/10
%% ====================================================================
%% @doc Calculates value used to identify sub proc
-spec calculate_proc_vale(TmpDepth :: integer(), MaxWidth :: integer(), CurrentValue :: integer()) -> Result when
  Result ::  integer().
%% ====================================================================
calculate_proc_vale(1, MaxWidth, CurrentValue) ->
  trunc(CurrentValue) rem MaxWidth;

calculate_proc_vale(TmpDepth, MaxWidth, CurrentValue) ->
  calculate_proc_vale(TmpDepth - 1, MaxWidth, CurrentValue  / MaxWidth).

%% generate_sub_proc_list/5
%% ====================================================================
%% @doc Generates the list that describes sub procs.
-spec generate_sub_proc_list(Name :: atom(), MaxDepth :: integer(), MaxWidth :: integer(), ProcFun :: term(), MapFun :: term()) -> Result when
  Result ::  list().
%% ====================================================================
generate_sub_proc_list(Name, MaxDepth, MaxWidth, ProcFun, MapFun) ->
  generate_sub_proc_list([{Name, MaxDepth, MaxWidth, ProcFun, MapFun}]).

%% generate_sub_proc_list/5
%% ====================================================================
%% @doc Generates the list that describes sub procs.
-spec generate_sub_proc_list([{Name :: atom(), MaxDepth :: integer(), MaxWidth :: integer(), ProcFun :: term(), MapFun :: term()}]) -> Result when
  Result ::  list().
%% ====================================================================
generate_sub_proc_list([]) ->
  [];

generate_sub_proc_list([{Name, MaxDepth, MaxWidth, ProcFun, MapFun} | Tail]) ->
  NewProcFun = fun({PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo}) ->
    lager:info("Processing in sub proc: ~p ~n", [Name]),
    BeforeProcessingRequest = os:timestamp(),
    Request = preproccess_msg(Msg),
    Response = 	try
      ProcFun(ProtocolVersion, Request)
                catch
                  Type:Error ->
                    lager:error("Worker plug-in ~p error: ~p:~p ~n ~p", [PlugIn, Type, Error, erlang:get_stacktrace()]),
                    worker_plug_in_error
                end,
    send_response(PlugIn, BeforeProcessingRequest, Response, MsgId, ReplyTo)
  end,

  NewMapFun = fun({_, _, Msg2, _, _}) ->
    Request = preproccess_msg(Msg2),
    MapFun(Request)
  end,

  StartArgs = {Name, MaxDepth, MaxWidth, NewProcFun, NewMapFun},
  SubProc = {Name, {StartArgs, start_sub_proc(Name, MaxDepth, MaxWidth, NewProcFun, NewMapFun)}},
  [SubProc | generate_sub_proc_list(Tail)].

%% stop_all_sub_proc/10
%% ====================================================================
%% @doc Stops all sub procs
-spec stop_all_sub_proc(SubProcs :: list()) -> ok.
%% ====================================================================
stop_all_sub_proc(SubProcs) ->
  Keys = proplists:get_keys(SubProcs),
  lists:foreach(fun(K) ->
    {_, SubProcPid} = proplists:get_value(K, SubProcs),
    SubProcPid ! {sub_proc_management, stop}
  end, Keys).

%% del_sub_procs/10
%% ====================================================================
%% @doc Sends stop signal to all processes found in ets table.
-spec del_sub_procs(Key :: term(), Name :: atom()) -> ok.
%% ====================================================================
del_sub_procs('$end_of_table', _Name) ->
  ok;
del_sub_procs(Key, Name) ->
  V = ets:lookup(Name, Key),
  case V of
    [{Int, EndPid}] when is_integer(Int) -> EndPid ! {sub_proc_management, stop};
    _ -> ok
  end,
  del_sub_procs(ets:next(Name, Key), Name).

create_simple_cache(Name, CacheLoop, ClearFun) ->
  create_simple_cache(Name, CacheLoop, ClearFun, true).

create_simple_cache(Name, CacheLoop, ClearFun, StrongCacheConnection) ->
  Pid = self(),
  create_simple_cache(Name, CacheLoop, ClearFun, StrongCacheConnection, Pid).

create_simple_cache(Name, CacheLoop, ClearFun, StrongCacheConnection, Pid) ->
  %% Init Cache-ETS. Ignore the fact that other DAO worker could have created this table. In this case, this call will
  %% fail, but table is present anyway, so everyone is happy.
  case ets:info(Name) of
    undefined   -> ets:new(Name, [named_table, public, set, {read_concurrency, true}]);
    [_ | _]     -> ok
  end,

  LoopTime = case CacheLoop of
    non -> non;
    Atom when is_atom(Atom) ->
      case application:get_env(veil_cluster_node, CacheLoop) of
        {ok, Interval1} -> Interval1;
        _               -> loop_time_load_error
      end;
    _ -> CacheLoop
  end,

  case LoopTime of
    Time when is_integer(Time) ->
      erlang:send_after(1000 * Time, Pid, {clear_sipmle_cache, 1000 * Time, ClearFun, StrongCacheConnection}),
      ok;
    _ -> loop_time_not_a_number_error
  end.