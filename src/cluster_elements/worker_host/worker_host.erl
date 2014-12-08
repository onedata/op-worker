%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module hosts all oneprovider modules (fslogic, cluster_rengin etc.).
%% It makes it easy to manage modules and provides some basic functionality
%% for its plug-ins (oneprovider modules) e.g. requests management.
%% @end
%% ===================================================================

-module(worker_host).
-behaviour(gen_server).

-include("records.hrl").
-include("registered_names.hrl").
-include("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include_lib("ctool/include/logging.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").

-define(BORTER_CHILD_WAIT_TIME, 10000).
-define(MAX_CHILD_WAIT_TIME, 60000000).
-define(MAX_CALCULATION_WAIT_TIME, 10000000).
-define(SUB_PROC_CACHE_CLEAR_TIME, 2000).

%% ====================================================================
%% API
%% ====================================================================
-export([start_link/3, stop/1, start_sub_proc/5, start_sub_proc/6, generate_sub_proc_list/1, generate_sub_proc_list/5, generate_sub_proc_list/6, send_to_user/4, send_to_user_with_ack/5, send_to_fuses_with_ack/4]).
-export([create_permanent_cache/1, create_permanent_cache/2, create_simple_cache/1, create_simple_cache/3, create_simple_cache/4, create_simple_cache/5, register_simple_cache/5, clear_cache/1, synch_cache_clearing/1, clear_sub_procs_cache/1, clear_simple_cache/3]).

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
  ?debug("Plugin ~p initialized with args ~p and result ~p", [PlugIn, PlugInArgs, InitAns]),
  case InitAns of
    IDesc when is_record(IDesc, initial_host_description) ->
      Pid = self(),
      DispatcherRequestMapState = case InitAns#initial_host_description.request_map of
        non -> true;
        _ ->
          erlang:send_after(200, Pid, {timer, register_disp_map}),
          false
      end,
      erlang:send_after(200, Pid, {timer, register_sub_proc_caches}),
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

handle_call(getSubProcs, _From, State) ->
    handle_test_call(getSubProcs, _From, State);

handle_call(clearLoadInfo, _From, State) ->
	{_New, _Old, _NewListSize, Max} = State#host_state.load_info,
    Reply = ok,
    {reply, Reply, State#host_state{load_info = {[], [], 0, Max}}};

handle_call({test_call, ProtocolVersion, Msg}, _From, State) ->
  PlugIn = State#host_state.plug_in,
  Reply = PlugIn:handle(ProtocolVersion, Msg),
  {reply, Reply, State};

handle_call(dispatcher_map_unregistered, _From, State) ->
  ?info("dispatcher_map_unregistered for plugin ~p", [State#host_state.plug_in]),
  Pid = self(),
  DMapState = case State#host_state.request_map of
                non -> true;
                _ ->
                  erlang:send_after(200, Pid, {timer, register_disp_map}),
                  false
              end,
  {reply, ok, State#host_state{dispatcher_request_map_ok = DMapState}};

%% If sub_proc with Name already existed then returns exists. Otherwise register new sub_proc
handle_call({register_new_sub_proc, Name, MaxDepth, MaxWidth, ProcFun, MapFun, RM, DM, Cache}, _From, State) ->
  case lists:keyfind(Name, 1, State#host_state.sub_procs) of
    false ->
      SubProcList = worker_host:generate_sub_proc_list(Name, MaxDepth, MaxWidth, ProcFun, MapFun, Cache) ++ State#host_state.sub_procs,
      NewState = upgrade_sub_proc_list(SubProcList, RM, DM, Cache, State),
      ?info("register_new_sub_proc: ~p, ok", [{Name, Cache}]),
      {reply, ok, NewState};
    _ ->
      ?warning("register_new_sub_proc: ~p, exists", [{Name, Cache}]),
      {reply, exists, State}
  end;

handle_call({register_new_sub_proc, Name, MaxDepth, MaxWidth, ProcFun, MapFun, RM, DM}, _From, State) ->
  handle_call({register_new_sub_proc, Name, MaxDepth, MaxWidth, ProcFun, MapFun, RM, DM, non}, _From, State);

%% If sub_proc with Name has not existed before than returns not_exists. Otherwise overwrite existing sub_proc
%% with newly added sub_proc
handle_call({update_sub_proc, Name, MaxDepth, MaxWidth, ProcFun, MapFun, RM, DM, Cache}, _From, State) ->
  case lists:keyfind(Name, 1, State#host_state.sub_procs) of
    false ->
      ?warning("update_sub_proc: ~p, not_exists", [{Name, Cache}]),
      {reply, not_exists, State};
    OldSubProcTuple ->
        {_, _, SubProcPid} = proplists:get_value(Name, State#host_state.sub_procs),
        SubProcPid ! {sub_proc_management, self(), stop},
        case is_process_alive(SubProcPid) of
          false ->
            WithoutOldSubProc = lists:delete(OldSubProcTuple, State#host_state.sub_procs),
            SubProcList = worker_host:generate_sub_proc_list(Name, MaxDepth, MaxWidth, ProcFun, MapFun, Cache) ++ WithoutOldSubProc,
            NewState = upgrade_sub_proc_list(SubProcList, RM, DM, Cache, State),
            {reply, ok, NewState};
          true ->
            receive
                sub_proc_stopped ->
                    WithoutOldSubProc2 = lists:delete(OldSubProcTuple, State#host_state.sub_procs),
                    SubProcList2 = worker_host:generate_sub_proc_list(Name, MaxDepth, MaxWidth, ProcFun, MapFun, Cache) ++ WithoutOldSubProc2,
                    NewState2 = upgrade_sub_proc_list(SubProcList2, RM, DM, Cache, State),
                    {reply, ok, NewState2}
            after
                1000 ->
                    {reply, cannot_stop_old_proc, State}
            end
        end
  end;

handle_call({update_sub_proc, Name, MaxDepth, MaxWidth, ProcFun, MapFun, RM, DM}, _From, State) ->
  handle_call({update_sub_proc, Name, MaxDepth, MaxWidth, ProcFun, MapFun, RM, DM, non}, _From, State);

%% Register sub_proc, may overwrite existing for the same Name.
handle_call({register_or_update_sub_proc, Name, MaxDepth, MaxWidth, ProcFun, MapFun, RM, DM, Cache}, _From, State) ->
    case lists:keyfind(Name, 1, State#host_state.sub_procs) of
        false ->
            SubProcList = worker_host:generate_sub_proc_list(Name, MaxDepth, MaxWidth, ProcFun, MapFun, Cache) ++ State#host_state.sub_procs,
            NewState = upgrade_sub_proc_list(SubProcList, RM, DM, Cache, State),
            {reply, ok, NewState};
    OldSubProcTuple ->
        {_, _, SubProcPid} = proplists:get_value(Name, State#host_state.sub_procs),
        SubProcPid ! {sub_proc_management, self(), stop},
        case is_process_alive(SubProcPid) of
          false ->
            WithoutOldSubProc = lists:delete(OldSubProcTuple, State#host_state.sub_procs),
            SubProcList = worker_host:generate_sub_proc_list(Name, MaxDepth, MaxWidth, ProcFun, MapFun, Cache) ++ WithoutOldSubProc,
            NewState = upgrade_sub_proc_list(SubProcList, RM, DM, Cache, State),
            {reply, ok, NewState};
          true ->
            receive
                sub_proc_stopped ->
                    WithoutOldSubProc2 = lists:delete(OldSubProcTuple, State#host_state.sub_procs),
                    SubProcList2 = worker_host:generate_sub_proc_list(Name, MaxDepth, MaxWidth, ProcFun, MapFun, Cache) ++ WithoutOldSubProc2,
                    NewState2 = upgrade_sub_proc_list(SubProcList2, RM, DM, Cache, State),
                    {reply, ok, NewState2}
            after
                1000 ->
                    {reply, cannot_stop_old_proc, State}
            end
        end
    end;

handle_call({register_or_update_sub_proc, Name, MaxDepth, MaxWidth, ProcFun, MapFun, RM, DM}, _From, State) ->
  handle_call({register_or_update_sub_proc, Name, MaxDepth, MaxWidth, ProcFun, MapFun, RM, DM, non}, _From, State);

handle_call({link_process, Pid}, _From, State) ->
    LinkAns = try
                  link(Pid),
                  ok
              catch
                  _:Reason ->
                      {error, Reason}
              end,
    {reply, LinkAns, State};

handle_call(Request, _From, State) when is_tuple(Request) -> %% Proxy call. Each cast can be achieved by instant proxy-call which ensures
                                                             %% that request was made, unlike cast because cast ignores state of node/gen_server
    {reply, gen_server:cast(State#host_state.plug_in, Request), State};

handle_call(check, _From, State) ->
  {reply, ok, State};

handle_call(_Request, _From, State) ->
  ?warning("Wrong call: ~p", [_Request]),
  {reply, wrong_request, State}.

%% handle_test_call/3
%% ====================================================================
%% @doc Handles calls used during tests
-spec handle_test_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> Result :: term().
%% ====================================================================
-ifdef(TEST).
handle_test_call(getSubProcs, _From, State) ->
  {reply, State#host_state.sub_procs, State}.
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

% Ack from FUSE client
handle_cast({asynch, _ProtocolVersion, _Msg, MsgId, FuseID}, State) ->
  OldCallbacks = ets:lookup(?ACK_HANDLERS, {chosen_node, MsgId}),
  case OldCallbacks of
    [{{chosen_node, MsgId}, Node}] ->
      ThisNode = node(),
      case Node of
        ThisNode -> handle_fuse_ack(MsgId, FuseID);
        _ -> gen_server:cast({State#host_state.plug_in, Node}, {asynch, _ProtocolVersion, _Msg, MsgId, FuseID})
      end;
    _ ->
      handle_fuse_ack(MsgId, FuseID)
  end,
  {noreply, State};

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
            ?error("Unknown worker sequential run queue state: current job: ~p, run queue: ~p", [Value, Queue]),
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

handle_cast(register_sub_proc_caches, State) ->
  case State#host_state.sub_proc_caches_ok of
    true ->
      {noreply, State};
    _ ->
      PlugIn = State#host_state.plug_in,
      RegCache = fun({Name, {_StartArgs, CacheType, _Pid}}, TmpAns) ->
        CacheRegAns = case CacheType of
          simple ->
            ClearingPid = self(),
            register_sub_proc_simple_cache({PlugIn, Name}, non, non, ClearingPid);
          {simple, CacheLoop2, ClearFun2} ->
            ClearingPid2 = self(),
            register_sub_proc_simple_cache({PlugIn, Name}, CacheLoop2, ClearFun2, ClearingPid2);
          {simple, CacheLoop4, ClearFun4, ClearingPid4} ->
            register_sub_proc_simple_cache({PlugIn, Name}, CacheLoop4, ClearFun4, ClearingPid4);
          _ ->
            ?debug("Use of non simple cache ~p", [{Name, CacheType}]),
            ok
        end,

        case CacheRegAns of
          ok -> TmpAns;
          _ -> error
        end
      end,
      RegAns = lists:foldl(RegCache, ok, State#host_state.sub_procs),

      ?debug("register_sub_proc_caches ans: ~p", [RegAns]),
      case RegAns of
        ok -> {noreply, State#host_state{sub_proc_caches_ok = true}};
        _ ->
          Pid = self(),
          erlang:send_after(10000, Pid, {timer, register_sub_proc_caches}),
          {noreply, State}
      end
  end;

handle_cast({clear_sub_procs_cache, AnsPid, Cache}, State) ->
  CacheName = case Cache of
    C when is_atom(C) ->
      Cache;
    {C2, _} ->
      C2
  end,

  SubProcs = State#host_state.sub_procs,
  {_SubProcArgs, _SubProcCache, SubProcPid} = proplists:get_value(CacheName, SubProcs, {not_found, not_found, not_found}),
  Ans = case SubProcPid of
    not_found ->
      false;
    _ ->
      case Cache of
        CName when is_atom(CName) ->
          SubProcPid ! {sub_proc_management, self(), clear_cache};
        {_, Keys} ->
          SubProcPid ! {sub_proc_management, self(), {clear_cache, Keys}}
      end,
      receive
        {sub_proc_cache_cleared, ClearAns} ->
          ?debug("clear_sub_procs_cache ~p ans: ~p", [Cache, ClearAns]),
          ClearAns
      after ?SUB_PROC_CACHE_CLEAR_TIME ->
        ?warning("clear_sub_procs_cache ~p timeout", [Cache]),
        false
      end
  end,
  AnsPid ! {sub_proc_cache_cleared, Ans},
  {noreply, State};

handle_cast(stop, State) ->
  {stop, normal, State};

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
handle_info({'EXIT', Pid, Reason}, State) ->
  PlugIn = State#host_state.plug_in,
  gen_server:cast(PlugIn, {asynch, 1, {'EXIT', Pid, Reason}}),
  {noreply, State};

handle_info({timer, Msg}, State) ->
  PlugIn = State#host_state.plug_in,
  gen_server:cast(PlugIn, Msg),
  {noreply, State};

handle_info({clear_simple_cache, LoopTime, Fun, StrongCacheConnection}, State) ->
  clear_simple_cache(LoopTime, Fun, StrongCacheConnection),
  {noreply, State};

handle_info({clear_sub_proc_simple_cache, Name, LoopTime, Fun}, State) ->
  clear_sub_proc_simple_cache(Name, LoopTime, Fun, State#host_state.sub_procs),
  {noreply, State};

handle_info(dispatcher_map_registered, State) ->
  ?debug("dispatcher_map_registered"),
  {noreply, State#host_state{dispatcher_request_map_ok = true}};

handle_info(Msg, State) ->
  handle_cast({asynch, 1, Msg}, State).


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
      ?error_stacktrace("Worker plug-in ~p error: ~p:~p", [PlugIn, Type, Error]),
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
        {SubProcArgs, SubProcCache, SubProcPid} = proplists:get_value(RequestMap(Msg), SubProcs, {not_found, not_found, not_found}),
        case SubProcArgs of
          not_found ->
            spawn(fun() -> proc_request(PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo) end),
            SubProcs;
          _ ->
            SubProcPid ! {PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo},
            %% check if chosen proc did not time out before message was delivered
            case is_process_alive(SubProcPid) of
              false ->
                {Name, MaxDepth, MaxWidth, NewProcFun, NewMapFun} = SubProcArgs,
                CacheType = case SubProcCache of
                  non -> non;
                  _ -> use_cache
                end,
                SubProcPid2 = start_sub_proc(Name, CacheType, MaxDepth, MaxWidth, NewProcFun, NewMapFun),
                SubProcPid2 ! {PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo},
                SubProc2Desc = {Name, {SubProcArgs, SubProcCache, SubProcPid2}},
                [SubProc2Desc | proplists:delete(Name, SubProcs)];
              true ->
                SubProcs
            end
        end
      catch
        Type:Error ->
          spawn(fun() ->
            BeforeProcessingRequest = os:timestamp(),
            ?error_stacktrace("Worker plug-in ~p sub proc error: ~p:~p", [PlugIn, Type, Error]),
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
      #worker_request{peer_id = PeerId, subject = Subj, request = Msg1, fuse_id = FuseID, access_token = AccessTokenTuple} ->
        fslogic_context:set_user_dn(Subj),
        put(peer_id, PeerId),
        case AccessTokenTuple of
            {UserID, AccessToken} ->
                fslogic_context:set_gr_auth(UserID, AccessToken);
            _ -> undefined
        end,
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
    Other -> ?error("Wrong reply type: ~p", [Other])
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

%% upgrade_sub_proc_list/5
%% ====================================================================
%% @doc Starts sub proc
-spec upgrade_sub_proc_list(SubProcList :: list(), RM :: fun(), DM :: fun(), Cache :: atom(), State :: #host_state{}) -> Result when
  Result ::  pid().
%% ====================================================================
upgrade_sub_proc_list(SubProcList, RM, DM, Cache, State) ->
  Pid = self(),
  erlang:send_after(200, Pid, {timer, register_disp_map}),
  case Cache of
    non -> ok;
    _ -> erlang:send_after(200, Pid, {timer, register_sub_proc_caches})
  end,
  State#host_state{request_map = RM, sub_procs = SubProcList,
  dispatcher_request_map = DM, dispatcher_request_map_ok = false}.

%% start_sub_proc/5
%% ====================================================================
%% @doc Starts sub proc
-spec start_sub_proc(Name :: atom(), MaxDepth :: integer(), MaxWidth :: integer(), ProcFun :: term(), MapFun :: term()) -> Result when
  Result ::  pid().
%% ====================================================================
start_sub_proc(Name, MaxDepth, MaxWidth, ProcFun, MapFun) ->
  spawn(fun() -> start_sub_proc(Name, non, 1, MaxDepth, MaxWidth, ProcFun, MapFun) end).

%% start_sub_proc/6
%% ====================================================================
%% @doc Starts sub proc
-spec start_sub_proc(Name :: atom(), CacheType :: term(), MaxDepth :: integer(), MaxWidth :: integer(), ProcFun :: term(), MapFun :: term()) -> Result when
  Result ::  pid().
%% ====================================================================
start_sub_proc(Name, CacheType, MaxDepth, MaxWidth, ProcFun, MapFun) ->
  spawn(fun() -> start_sub_proc(Name, CacheType, 1, MaxDepth, MaxWidth, ProcFun, MapFun) end).

%% start_sub_proc/7
%% ====================================================================
%% @doc Starts sub proc
-spec start_sub_proc(Name :: atom(), CacheType :: term(), SubProcDepth :: integer(), MaxDepth :: integer(), MaxWidth :: integer(), ProcFun :: term(), MapFun :: term()) -> Result when
  Result ::  pid().
%% ====================================================================
start_sub_proc(Name, CacheType, SubProcDepth, MaxDepth, MaxWidth, ProcFun, MapFun) ->
  process_flag(trap_exit, true),
  ets:new(Name, [named_table, set, private]),

  CacheName = case CacheType of
    non ->
      non;
    use_cache ->
      CName = get_cache_name(Name),
      ets:new(CName, [named_table, set, private]),
      CName
  end,

  ?debug("Starting sub proc ~p with cache type: ~p", [Name, CacheType]),
  sub_proc(Name, CacheName, proc, SubProcDepth, MaxDepth, MaxWidth, os:timestamp(), ?MAX_CALCULATION_WAIT_TIME, ProcFun, MapFun).

%% sub_proc/9
%% ====================================================================
%% @doc Sub proc function
-spec sub_proc(Name :: atom(), CacheName :: atom(), ProcType:: atom(), SubProcDepth :: integer(), MaxDepth :: integer(), MaxWidth :: integer(),
    WaitFrom :: term(), AvgWaitTime :: term(), ProcFun :: term(), MapFun :: term()) -> Result when
  Result ::  ok | end_sub_proc.
%% ====================================================================
sub_proc(Name, CacheName, ProcType, SubProcDepth, MaxDepth, MaxWidth, WaitFrom, AvgWaitTime, ProcFun, MapFun) ->
  receive
    {sub_proc_management, stop} ->
      ?debug("Stoping sub proc ~p", [Name]),
      del_sub_procs(ets:first(Name), Name);

    {sub_proc_management, ReturnPid, stop} ->
      del_sub_procs(ets:first(Name), Name),
      ets:delete(CacheName),
      ReturnPid ! sub_proc_stopped;

    {sub_proc_management, ReturnPid, clear_cache} ->
      case CacheName of
          non ->
            ReturnPid ! {sub_proc_cache_cleared, false};
        _ ->
          ?debug("Clearing cache of sub proc ~p", [Name]),
          ets:delete_all_objects(CacheName),
          ReturnPid ! {sub_proc_cache_cleared, clear_sub_procs_caches(Name, clear_cache)}
      end,
      sub_proc(Name, CacheName, ProcType, SubProcDepth, MaxDepth, MaxWidth, WaitFrom, AvgWaitTime, ProcFun, MapFun);

    {sub_proc_management, ReturnPid, {clear_cache, Keys}} ->
      case CacheName of
          non ->
            ReturnPid ! {sub_proc_cache_cleared, false};
        _ ->
          ?debug("Clearing cache of sub proc ~p with key ~p", [Name, Keys]),
          case Keys of
            KList when is_list(KList) ->
              lists:foreach(fun(K) -> ets:delete(CacheName, K) end, KList);
            Key ->
              ets:delete(CacheName, Key)
          end,
          ReturnPid ! {sub_proc_cache_cleared, clear_sub_procs_caches(Name, {clear_cache, Keys})}
      end,
      sub_proc(Name, CacheName, ProcType, SubProcDepth, MaxDepth, MaxWidth, WaitFrom, AvgWaitTime, ProcFun, MapFun);

    {sub_proc_management, sub_proc_automatic_cache_clearing, ClearFun} ->
      ?debug("Automatic cache clearing of sub proc ~p", [Name]),
      ClearFun(CacheName),
      clear_sub_procs_caches_by_fun(Name, ClearFun),
      sub_proc(Name, CacheName, ProcType, SubProcDepth, MaxDepth, MaxWidth, WaitFrom, AvgWaitTime, ProcFun, MapFun);

    {'EXIT', ChildPid, _} ->
      ChildDesc = ets:lookup(Name, ChildPid),
      case ChildDesc of
        [{ChildPid, ChildNum}] ->
          ?debug("Exit of child num ~p of sub proc ~p", [ChildNum, Name]),
          ets:delete(Name, ChildPid),
          ets:delete(Name, ChildNum);
        _ ->
          ?error("Exit of unknown sub proc"),
          error
      end,
      sub_proc(Name, CacheName, ProcType, SubProcDepth, MaxDepth, MaxWidth, WaitFrom, AvgWaitTime, ProcFun, MapFun);
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
                        case NewAvgWaitTime > ?BORTER_CHILD_WAIT_TIME * 1000 of
                          true ->
                            proc;
                          false ->
                            map
                        end
                    end,

      case NewProcType of
        map ->
          {ForwardNum, ForwardPid} = map_to_sub_proc(Name, CacheName, SubProcDepth, MaxDepth, MaxWidth, ProcFun, MapFun, Request),
          case ForwardNum of
            error -> error;
            _ ->
              ForwardPid ! Request,
              %% check if chosen proc did not time out before message was delivered
              case is_process_alive(ForwardPid) of
                false ->
                  ets:delete(Name, ForwardPid),
                  ets:delete(Name, ForwardNum),
                  ForwardPid2 = map_to_sub_proc(Name, CacheName, SubProcDepth, MaxDepth, MaxWidth, ProcFun, MapFun, Request),
                  ForwardPid2 ! Request;
                true ->
                  ok
              end
          end;
        proc ->
          case CacheName of
            non -> ProcFun(Request);
            _ ->
              ProcFun(Request, CacheName)
          end
      end,

      sub_proc(Name, CacheName, NewProcType, SubProcDepth, MaxDepth, MaxWidth, Now, NewAvgWaitTime, ProcFun, MapFun)
  after ?MAX_CHILD_WAIT_TIME ->
    ?debug("End of sub proc ~p (max waiting time)", [Name]),
    end_sub_proc
  end.

%% map_to_sub_proc/8
%% ====================================================================
%% @doc Maps request to sub proc pid
-spec map_to_sub_proc(Name :: atom(), CacheName:: atom(), SubProcDepth :: integer(), MaxDepth :: integer(), MaxWidth :: integer(),
   ProcFun :: term(), MapFun :: term(), Request :: term()) -> Result when
  Result ::  {SubProcNum, SubProcPid},
  SubProcNum :: integer(),
  SubProcPid :: term().
%% ====================================================================
map_to_sub_proc(Name, CacheName, SubProcDepth, MaxDepth, MaxWidth, ProcFun, MapFun, Request) ->
  try
    RequestValue = calculate_proc_vale(SubProcDepth, MaxWidth, MapFun(Request)),
    RequestProc = ets:lookup(Name, RequestValue),
    case RequestProc of
      [] ->
        NewName = list_to_atom(atom_to_list(Name) ++ "_" ++ integer_to_list(RequestValue)),
        NewCacheType = case CacheName of
          non -> non;
          _ -> use_cache
        end,
        NewPid = spawn(fun() -> start_sub_proc(NewName, NewCacheType, SubProcDepth + 1, MaxDepth, MaxWidth, ProcFun, MapFun) end),
        ets:insert(Name, {RequestValue, NewPid}),
        ets:insert(Name, {NewPid, RequestValue}),
        {RequestValue, NewPid};
      [{RequestValue, RequestProcPid}] ->
        {RequestValue, RequestProcPid};
      _ ->
        ?error("Sub proc error for request ~p", [Request]),
        {error, error}
    end
  catch
    _:_ ->
      ?error("Sub proc error for request ~p", [Request]),
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

%% generate_sub_proc_list/6
%% ====================================================================
%% @doc Generates the list that describes sub procs.
-spec generate_sub_proc_list(Name :: atom(), MaxDepth :: integer(), MaxWidth :: integer(), ProcFun :: term(), MapFun :: term(), CacheType :: term()) -> Result when
  Result ::  list().
%% ====================================================================
generate_sub_proc_list(Name, MaxDepth, MaxWidth, ProcFun, MapFun, CacheType) ->
  generate_sub_proc_list([{Name, MaxDepth, MaxWidth, ProcFun, MapFun, CacheType}]).

%% generate_sub_proc_list/1
%% ====================================================================
%% @doc Generates the list that describes sub procs.
-spec generate_sub_proc_list([]) -> Result when
  Result ::  list().
%% ====================================================================
generate_sub_proc_list([]) ->
  [];

generate_sub_proc_list([{Name, MaxDepth, MaxWidth, ProcFun, MapFun} | Tail]) ->
  generate_sub_proc_list([{Name, MaxDepth, MaxWidth, ProcFun, MapFun, non} | Tail]);

generate_sub_proc_list([{Name, MaxDepth, MaxWidth, ProcFun, MapFun, CacheType} | Tail]) ->
  NewProcFun = case CacheType of
    non ->
      fun({PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo}) ->
        ?debug("Processing in sub proc: ~p ~n", [Name]),
        BeforeProcessingRequest = os:timestamp(),
        Request = preproccess_msg(Msg),
        Response = 	try
          ProcFun(ProtocolVersion, Request)
                    catch
                      Type:Error ->
                        ?error_stacktrace("Worker plug-in ~p error: ~p:~p", [PlugIn, Type, Error]),
                        worker_plug_in_error
                    end,
        send_response(PlugIn, BeforeProcessingRequest, Response, MsgId, ReplyTo)
      end;
    _ ->
      fun({PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo}, CacheName) ->
        ?debug("Processing in sub proc: ~p with cache ~p~n", [Name, CacheName]),
        BeforeProcessingRequest = os:timestamp(),
        Request = preproccess_msg(Msg),
        Response = 	try
          ProcFun(ProtocolVersion, Request, CacheName)
                    catch
                      Type:Error ->
                        ?error_stacktrace("Worker plug-in ~p error: ~p:~p", [PlugIn, Type, Error]),
                        worker_plug_in_error
                    end,
        send_response(PlugIn, BeforeProcessingRequest, Response, MsgId, ReplyTo)
      end
  end,

  NewMapFun = fun({_, _, Msg2, _, _}) ->
    Request = preproccess_msg(Msg2),
    MapFun(Request)
  end,

  StartArgs = {Name, MaxDepth, MaxWidth, NewProcFun, NewMapFun},
  CacheType2 = case CacheType of
    non -> non;
    _ -> use_cache
  end,
  SubProc = {Name, {StartArgs, CacheType, start_sub_proc(Name, CacheType2, MaxDepth, MaxWidth, NewProcFun, NewMapFun)}},
  [SubProc | generate_sub_proc_list(Tail)].

%% stop_all_sub_proc/1
%% ====================================================================
%% @doc Stops all sub procs
-spec stop_all_sub_proc(SubProcs :: list()) -> ok.
%% ====================================================================
stop_all_sub_proc(SubProcs) ->
  ?debug("Stoping all sub procs ~p", [SubProcs]),
  Keys = proplists:get_keys(SubProcs),
  lists:foreach(fun(K) ->
    {_, _, SubProcPid} = proplists:get_value(K, SubProcs),
    SubProcPid ! {sub_proc_management, stop}
  end, Keys).

%% del_sub_procs/2
%% ====================================================================
%% @doc Sends stop signal to all processes found in ets table.
-spec del_sub_procs(Key :: term(), Name :: atom()) -> ok.
%% ====================================================================
del_sub_procs('$end_of_table', _Name) ->
  ok;
del_sub_procs(Key, Name) ->
  V = ets:lookup(Name, Key),
  case V of
    [{Int, EndPid}] when is_integer(Int) ->
      ?debug("Stoping sub proc ~p", [Key]),
      EndPid ! {sub_proc_management, stop};
    _ -> ok
  end,
  del_sub_procs(ets:next(Name, Key), Name).

%% send_to_user/4
%% ====================================================================
%% @doc Sends message to all active fuses belonging to user.
-spec send_to_user(UserKey :: user_key(), Message :: term(), MessageDecoder :: string(), ProtocolVersion :: integer()) -> Result when
  Result :: ok | {error, Error :: term()}.
%% ====================================================================
%% TODO: move to request_dispatcher
send_to_user(UserKey, Message, MessageDecoder, ProtocolVersion) ->
  case user_logic:get_user(UserKey) of
    {ok, UserDoc} ->
      case dao_lib:apply(dao_cluster, get_sessions_by_user, [UserDoc#db_document.uuid], ProtocolVersion) of
        {ok, FuseIds} ->
          lists:foreach(fun(FuseId) -> request_dispatcher:send_to_fuse(FuseId, Message, MessageDecoder) end, FuseIds),
          ok;
        {error, Error} ->
          ?warning("cannot get fuse ids for user, error: ~p", [Error]),
          {error, Error}
      end;
    {error, Error} ->
      ?warning("cannot get user in send_to_user: ~p", [Error]),
      {error, Error}
  end.

%% send_to_user_with_ack/5
%% ====================================================================
%% @doc Sends message with ack to all active fuses belonging to user. OnCompleteCallback function will be called in
%% newly created process when all acks will come back or timeout is reached.
-spec send_to_user_with_ack(UserKey :: user_key(), Message :: term(), MessageDecoder :: string(),
    OnCompleteCallback :: fun((SuccessFuseIds :: list(), FailFuseIds :: list()) -> term()), ProtocolVersion :: integer()) -> Result when
    Result :: ok | {error, Error :: term()}.
%% ====================================================================
send_to_user_with_ack(UserKey, Message, MessageDecoder, OnCompleteCallback, ProtocolVersion) ->
  case user_logic:get_user(UserKey) of
    {ok, UserDoc} ->
      case dao_lib:apply(dao_cluster, get_sessions_by_user, [UserDoc#db_document.uuid], ProtocolVersion) of
        {ok, FuseIds} ->
          send_to_fuses_with_ack(FuseIds, Message, MessageDecoder, OnCompleteCallback);
        {error, Error} ->
          ?warning("cannot get fuse ids for user"),
          {error, Error}
      end;
    {error, Error} ->
      ?warning("cannot get user in send_to_user"),
      {error, Error}
  end.

%% send_to_fuses_with_ack/4
%% ====================================================================
%% @doc Sends message with ack to all fuses from FuseIds list.
%% OnComplete function will be called in newly created process when all acks will come back or timeout is reached.
-spec send_to_fuses_with_ack(FuseIds :: list(string()), Message :: term(), MessageDecoder :: string(),
    OnCompleteCallback :: fun((SuccessFuseIds :: list(), FailFuseIds :: list()) -> term())) -> Result when
  Result :: ok | {error, Error :: term()}.
%% ====================================================================
send_to_fuses_with_ack(FuseIds, Message, MessageDecoder, OnCompleteCallback) ->
  try
    ThisNode = node(),
    MsgId = gen_server:call({global, ?CCM}, {node_for_ack, ThisNode}),

    Pid = spawn(fun() ->
      receive
        {call_on_complete_callback} ->
          [{_Key, {_, SucessFuseIds, FailFuseIds, _Length, _Pid}}] = ets:lookup(?ACK_HANDLERS, MsgId),
          ets:delete(?ACK_HANDLERS, MsgId),
          OnCompleteCallback(SucessFuseIds, FailFuseIds)
      end
    end),

    % we need to create and insert to ets ack handler before sending a push message
    ets:insert(?ACK_HANDLERS, {MsgId, {OnCompleteCallback, [], FuseIds, length(FuseIds), Pid}}),

    % now we can send messages to fuses
    lists:foreach(fun(FuseId) -> request_dispatcher:send_to_fuse_ack(FuseId, Message, MessageDecoder, MsgId) end, FuseIds),

    %% TODO change to receive .. after
    {ok, CallbackAckTimeSeconds} = application:get_env(?APP_Name, callback_ack_time),
    erlang:send_after(CallbackAckTimeSeconds * 1000, Pid, {call_on_complete_callback}),
    ok
  catch
    E1:E2 ->
      ?warning("cannot get callback message id, Error: ~p:~p", [E1, E2]),
      {error, E2}
  end.


%% create_simple_cache/1
%% ====================================================================
%% @doc Creates simple cache.
-spec create_simple_cache(Name :: CacheName) -> Result when
  CacheName :: atom() | {permanent_cache, atom()} | {permanent_cache, atom(), term()},
  Result :: ok | error_during_cache_registration.
%% ====================================================================
create_simple_cache(Name) ->
  create_simple_cache(Name, non, non).

%% create_simple_cache/3
%% ====================================================================
%% @doc Creates simple cache.
-spec create_simple_cache(Name :: CacheName, CacheLoop, ClearFun :: term()) -> Result when
  CacheName :: atom() | {permanent_cache, atom()} | {permanent_cache, atom(), term()},
  Result :: ok | error_during_cache_registration | loop_time_not_a_number_error,
  CacheLoop :: integer() | atom().
%% ====================================================================
create_simple_cache(Name, CacheLoop, ClearFun) ->
  create_simple_cache(Name, CacheLoop, ClearFun, true).

%% create_simple_cache/4
%% ====================================================================
%% @doc Creates simple cache.
-spec create_simple_cache(Name :: CacheName, CacheLoop, ClearFun :: term(), StrongCacheConnection :: boolean()) -> Result when
  CacheName :: atom() | {permanent_cache, atom()} | {permanent_cache, atom(), term()},
  Result :: ok | error_during_cache_registration | loop_time_not_a_number_error,
  CacheLoop :: integer() | atom().
%% ====================================================================
create_simple_cache(Name, CacheLoop, ClearFun, StrongCacheConnection) ->
  Pid = self(),
  create_simple_cache(Name, CacheLoop, ClearFun, StrongCacheConnection, Pid).

%% create_simple_cache/5
%% ====================================================================
%% @doc Creates simple cache.
-spec create_simple_cache(Name :: CacheName, CacheLoop, ClearFun :: term(), StrongCacheConnection :: boolean(), Pid :: pid()) -> Result when
  CacheName :: atom() | {permanent_cache, atom()} | {permanent_cache, atom(), term()},
  Result :: ok | error_during_cache_registration | loop_time_not_a_number_error,
  CacheLoop :: integer() | atom().
%% ====================================================================
create_simple_cache(Name, CacheLoop, ClearFun, StrongCacheConnection, ClearingPid) ->
  %% Init Cache-ETS. Ignore the fact that other DAO worker could have created this table. In this case, this call will
  %% fail, but table is present anyway, so everyone is happy.
  EtsName = case Name of
    {permanent_cache, PCache} -> PCache;
    {permanent_cache, PCache2, _} -> PCache2;
    _ -> Name
  end,

  case ets:info(EtsName) of
    undefined   -> ets:new(EtsName, [named_table, public, set, {read_concurrency, true}]);
    [_ | _]     -> ok
  end,

  worker_host:register_simple_cache(Name, CacheLoop, ClearFun, StrongCacheConnection, ClearingPid).

%% register_simple_cache/5
%% ====================================================================
%% @doc Registers simple cache.
-spec register_simple_cache(Name :: CacheName, CacheLoop, ClearFun :: term(), StrongCacheConnection :: boolean(), ClearingPid :: pid()) -> Result when
  CacheName :: atom() | {permanent_cache, atom()} | {permanent_cache, atom(), term()} | {sub_proc_cache, {PlugIn, SubProcName}},
  PlugIn :: atom(),
  SubProcName :: atom(),
  Result :: ok | error_during_cache_registration | loop_time_not_a_number_error,
  CacheLoop :: integer() | atom().
%% ====================================================================
register_simple_cache(Name, CacheLoop, ClearFun, StrongCacheConnection, ClearingPid) ->
  ?debug("register_simple_cache: ~p with loop ~p and strong connection ~p", [Name, CacheLoop, ClearFun]),

  Pid = self(),
  gen_server:cast(?Node_Manager_Name, {register_simple_cache, Name, Pid}),
  receive
    simple_cache_registered ->
      LoopTime = case CacheLoop of
                   non -> non;
                   Atom when is_atom(Atom) ->
                     case application:get_env(?APP_Name, CacheLoop) of
                       {ok, Interval1} -> Interval1;
                       _               -> loop_time_load_error
                     end;
                   _ -> CacheLoop
                 end,

      case LoopTime of
        Time when is_integer(Time) ->
          case Name of
            {sub_proc_cache, {_PlugIn, SubProcName}} ->
              erlang:send_after(1000 * Time, ClearingPid, {clear_sub_proc_simple_cache, SubProcName, 1000 * Time, ClearFun});
            _ ->
              erlang:send_after(1000 * Time, ClearingPid, {clear_simple_cache, 1000 * Time, ClearFun, StrongCacheConnection})
          end,
          ok;
        non -> ok;
        _ ->
          ?error("register_simple_cache: ~p: loop_time_not_a_number_error"),
          loop_time_not_a_number_error
      end
  after 500 ->
    ?error("register_simple_cache: ~p: timeout"),
    error_during_cache_registration
  end.

%% create_permanent_cache/1
%% ====================================================================
%% @doc Creates permanent cache.
-spec create_permanent_cache(Name :: atom()) -> Result when
  Result :: ok | error_during_cache_registration.
%% ====================================================================
create_permanent_cache(Name) ->
  create_simple_cache({permanent_cache, Name}).

%% create_permanent_cache/2
%% ====================================================================
%% @doc Creates permanent cache.
-spec create_permanent_cache(Name :: atom(), CacheCheckFun :: term()) -> Result when
  Result :: ok | error_during_cache_registration.
%% ====================================================================
create_permanent_cache(Name, CacheCheckFun) ->
  create_simple_cache({permanent_cache, Name, CacheCheckFun}).

%% clear_cache/1
%% ====================================================================
%% @doc Clears chosen caches at all nodes
-spec clear_cache(Cache :: term()) -> ok | error_during_contact_witch_ccm.
%% ====================================================================
clear_cache(Cache) ->
  Pid = self(),
  gen_server:cast({global, ?CCM}, {clear_cache, Cache, Pid}),
  receive
    {cache_cleared, Cache} -> ok
  after 500 ->
    error_during_contact_witch_ccm
  end.

%% synch_cache_clearing/1
%% ====================================================================
%% @doc Clears chosen caches at all nodes
-spec synch_cache_clearing(Cache :: term()) -> ok | error_during_contact_witch_ccm.
%% ====================================================================
synch_cache_clearing(Cache) ->
  Pid = self(),
  gen_server:cast({global, ?CCM}, {synch_cache_clearing, Cache, Pid}),
  receive
    {cache_cleared, Cache} -> ok
  after 5000 ->
    error_during_contact_witch_ccm
  end.

%% lclear_sub_procs_caches/2
%% ====================================================================
%% @doc Clears caches of all sub processes (used during cache clearing for request)
-spec clear_sub_procs_caches(EtsName :: atom(), Message :: term()) -> integer().
%% ====================================================================
clear_sub_procs_caches(EtsName, Message) ->
  clear_sub_procs_caches(EtsName, ets:first(EtsName), Message, 0).

%% clear_sub_procs_caches/4
%% ====================================================================
%% @doc Clears caches of all sub processes
-spec clear_sub_procs_caches(EtsName :: atom(), CurrentElement :: term(), Message :: term(), SendNum :: integer()) -> integer().
%% ====================================================================
clear_sub_procs_caches(_EtsName, '$end_of_table', _Message, SendNum) ->
  count_answers(SendNum);
clear_sub_procs_caches(EtsName, CurrentElement, Message, SendNum) ->
  NewSendNum = case CurrentElement of
    Pid when is_pid(Pid) ->
      Pid ! {sub_proc_management, self(), Message},
      SendNum + 1;
    _ -> SendNum
  end,
  clear_sub_procs_caches(EtsName, ets:next(EtsName, CurrentElement), Message, NewSendNum).

%% count_answers/1
%% ====================================================================
%% @doc Gethers answers from sub processes (about cache clearing).
-spec count_answers(ExpectedNum :: integer()) -> boolean().
%% ====================================================================
count_answers(ExpectedNum) ->
  count_answers(ExpectedNum, true).

%% count_answers/1
%% ====================================================================
%% @doc Gethers answers from sub processes (about cache clearing).
-spec count_answers(ExpectedNum :: integer(), TmpAns :: boolean()) -> boolean().
%% ====================================================================
count_answers(0, TmpAns) ->
  TmpAns;

count_answers(ExpectedNum, TmpAns) ->
  receive
    {sub_proc_cache_cleared, TmpAns2} ->
      count_answers(ExpectedNum - 1, TmpAns and TmpAns2)
  after ?SUB_PROC_CACHE_CLEAR_TIME ->
    false
  end.

%% clear_sub_procs_caches_by_fun/2
%% ====================================================================
%% @doc Clears caches of all sub processes (used during automatic cache clearing)
-spec clear_sub_procs_caches_by_fun(EtsName :: atom(), Fun :: term()) -> integer().
%% ====================================================================
clear_sub_procs_caches_by_fun(EtsName, Fun) ->
  clear_sub_procs_caches_by_fun(EtsName, ets:first(EtsName), Fun).

%% clear_sub_procs_caches_by_fun/3
%% ====================================================================
%% @doc Clears caches of all sub processes (used during automatic cache clearing)
-spec clear_sub_procs_caches_by_fun(EtsName :: atom(), CurrentElement::term(), Fun :: term()) -> ok.
%% ====================================================================
clear_sub_procs_caches_by_fun(_EtsName, '$end_of_table', _) ->
  ok;
clear_sub_procs_caches_by_fun(EtsName, CurrentElement, Fun) ->
  case CurrentElement of
    Pid when is_pid(Pid) ->
      Pid ! {sub_proc_management, sub_proc_automatic_cache_clearing, Fun};
    _ ->
      ok
  end,
  clear_sub_procs_caches_by_fun(EtsName, ets:next(EtsName, CurrentElement), Fun).

%% get_cache_name/1
%% ====================================================================
%% @doc Returns ets name for sub_proc
-spec get_cache_name(SupProcName :: atom()) -> atom().
%% ====================================================================
get_cache_name(SupProcName) ->
  list_to_atom(atom_to_list(SupProcName) ++ "_cache").

%% register_sub_proc_simple_cache/5
%% ====================================================================
%% @doc Registers sub_proc simple cache
-spec register_sub_proc_simple_cache(Name :: atom(), CacheLoop, ClearFun :: term(), ClearingPid :: pid()) -> Result when
  Result :: ok | error,
  CacheLoop :: integer() | atom().
%% ====================================================================
register_sub_proc_simple_cache(Name, CacheLoop, ClearFun, ClearingPid) ->
  RegAns = worker_host:register_simple_cache({sub_proc_cache, Name}, CacheLoop, ClearFun, false, ClearingPid),
  case RegAns of
    ok ->
      ok;
    _ ->
      ?error("Error of register_sub_proc_simple_cache, error: ~p, args: ~p", [RegAns, {Name, CacheLoop, ClearFun, ClearingPid}]),
      error
  end.

%% clear_sub_procs_cache/1
%% ====================================================================
%% @doc Clears caches of sub_proc
-spec clear_sub_procs_cache(Cache :: CacheDesc) -> ok | error when
  CacheDesc :: {PlugIn :: atom(), Cache :: atom()} | {{PlugIn :: atom(), Cache :: atom()}, Key :: term()}.
%% ====================================================================
clear_sub_procs_cache({{PlugIn, Cache}, Key}) ->
  gen_server:cast(PlugIn, {clear_sub_procs_cache, self(), {Cache, Key}}),
  receive
    {sub_proc_cache_cleared, ClearAns} ->
      case ClearAns of
        true -> ok;
        _ -> error
      end
  after ?SUB_PROC_CACHE_CLEAR_TIME ->
    error
  end;

clear_sub_procs_cache({PlugIn, Cache}) ->
  gen_server:cast(PlugIn, {clear_sub_procs_cache, self(), Cache}),
  receive
    {sub_proc_cache_cleared, ClearAns} ->
      case ClearAns of
        true -> ok;
        _ -> error
      end
  after ?SUB_PROC_CACHE_CLEAR_TIME ->
    error
  end.


%% clear_simple_cache/3
%% ====================================================================
%% @doc Clears simple cache. Returns clearing process pid.
-spec clear_simple_cache(LoopTime :: integer(), Fun :: term(), StrongCacheConnection :: boolean()) -> pid().
%% ====================================================================
clear_simple_cache(LoopTime, Fun, StrongCacheConnection) ->
  Pid = self(),
  erlang:send_after(LoopTime, Pid, {clear_simple_cache, LoopTime, Fun, StrongCacheConnection}),
  case StrongCacheConnection of
    true ->
      spawn_link(fun() -> Fun() end);
    _ ->
      spawn(fun() -> Fun() end)
  end.

%% clear_sub_proc_simple_cache/4
%% ====================================================================
%% @doc Clears sub proc simple cache.
-spec clear_sub_proc_simple_cache(Name :: atom(), LoopTime :: integer(), Fun :: term(), SubProcs :: atom()) -> boolean().
%% ====================================================================
clear_sub_proc_simple_cache(Name, LoopTime, Fun, SubProcs) ->
  Pid = self(),
  erlang:send_after(LoopTime, Pid, {clear_sub_proc_simple_cache, Name, LoopTime, Fun}),
  {_SubProcArgs, _SubProcCache, SubProcPid} = proplists:get_value(Name, SubProcs, {not_found, not_found, not_found}),
  case SubProcPid of
    not_found ->
      false;
    _ ->
      SubProcPid ! {sub_proc_management, sub_proc_automatic_cache_clearing, Fun}
  end.

handle_fuse_ack(MsgId, FuseID) ->
  HandlerTuple = ets:lookup(?ACK_HANDLERS, MsgId),
  case HandlerTuple of
    [{_, {Callback, SuccessFuseIds, FailFuseIds, Length, Pid}}] ->
      NewSuccessFuseIds = [FuseID | SuccessFuseIds],
      NewFailFuseIds = lists:delete(FuseID, FailFuseIds),

      %% TODO: refactoring needed - Length not needed, we can replace NewFailFuseIds with AllFuseIds and then we will always have Length and we will not have to delete anything from this list
      ets:insert(?ACK_HANDLERS, {MsgId, {Callback, NewSuccessFuseIds, NewFailFuseIds, Length, Pid}}),
      case length(NewSuccessFuseIds) of
        Length ->
          % we dont need to cancel timer set with send_after which sends the same message - receiver process should be dead after call_on_complete_callback
          Pid ! {call_on_complete_callback};
        _ -> ok
      end;
    _ -> ok
  end.
