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

-include("registered_names.hrl").
-include("cluster_elements/worker_host/worker_host.hrl").
-include_lib("ctool/include/logging.hrl").

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
    InitAns = PlugIn:init(PlugInArgs),
    ?debug("Plugin ~p initialized with args ~p and result ~p", [PlugIn, PlugInArgs, InitAns]),
    case InitAns of
        #initial_host_description{request_map = RequestMap, dispatcher_request_map = DispReqMap} ->
            DispatcherRequestMapState =
                case RequestMap of
                    non -> true;
                    _ ->
                        erlang:send_after(200, self, {timer, register_disp_map}),
                        false
                end,
            {ok, #host_state{plug_in = PlugIn, request_map = RequestMap, dispatcher_request_map = DispReqMap,
                dispatcher_request_map_ok = DispatcherRequestMapState,
                plug_in_state = InitAns#initial_host_description.plug_in_state, load_info = {[], [], 0, LoadMemorySize}}};
        _ ->
            {ok, #host_state{plug_in = PlugIn, plug_in_state = InitAns, load_info = {[], [], 0, LoadMemorySize}}}
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
    ?info("dispatcher_map_unregistered for plugin ~p", [State#host_state.plug_in]),
    DMapState = case State#host_state.request_map of
                    non -> true;
                    _ ->
                        erlang:send_after(200, self(), {timer, register_disp_map}),
                        false
                end,
    {reply, ok, State#host_state{dispatcher_request_map_ok = DMapState}};

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

handle_cast({synch, ProtocolVersion, Msg, ReplyTo}, State) ->
    PlugIn = State#host_state.plug_in,
    spawn(fun() -> proc_request(PlugIn, ProtocolVersion, Msg, non, ReplyTo) end),
    {noreply, State};

handle_cast({asynch, ProtocolVersion, Msg}, State) ->
    PlugIn = State#host_state.plug_in,
    spawn(fun() -> proc_request(PlugIn, ProtocolVersion, Msg, non, non) end),
    {noreply, State};

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
-spec proc_request(PlugIn :: atom(), ProtocolVersion :: integer(), Msg :: term(), MsgId :: term(), ReplyDisp :: term()) -> Result when
    Result ::  atom().
%% ====================================================================
proc_request(PlugIn, ProtocolVersion, Msg, MsgId, ReplyTo) ->
    BeforeProcessingRequest = os:timestamp(),
    Response = 	try
        PlugIn:handle(ProtocolVersion, Msg)
                  catch
                      Type:Error ->
                          ?error_stacktrace("Worker plug-in ~p error: ~p:~p", [PlugIn, Type, Error]),
                          worker_plug_in_error
                  end,
    send_response(PlugIn, BeforeProcessingRequest, Response, MsgId, ReplyTo).

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
