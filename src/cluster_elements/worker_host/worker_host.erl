%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module hosts all oneprovider modules (fslogic, cluster_rengin etc.).
%%% It makes it easy to manage modules and provides some basic functionality
%%% for its plug-ins (oneprovider modules) e.g. requests management.
%%% @end
%%%-------------------------------------------------------------------
-module(worker_host).
-author("Michal Wrzeszcz").

-behaviour(gen_server).

-include("registered_names.hrl").
-include("cluster_elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/logging.hrl").

%% This record is used by worker_host (it contains its state). It describes
%% plug_in that is used and state of this plug_in. It contains also
%% information about time of requests processing (used by ccm during
%% load balancing).
-record(host_state, {plug_in = non, plug_in_state = [], load_info = []}).

%% API
-export([start_link/3, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts host with apropriate plug-in
%% @end
%%--------------------------------------------------------------------
-spec start_link(PlugIn, PlugInArgs, LoadMemorySize) -> Result when
    PlugIn :: atom(),
    PlugInArgs :: any(),
    LoadMemorySize :: integer(),
    Result :: {ok, Pid}
    | ignore
    | {error, Error},
    Pid :: pid(),
    Error :: {already_started, Pid} | term().
start_link(PlugIn, PlugInArgs, LoadMemorySize) ->
    gen_server:start_link({local, PlugIn}, ?MODULE, [PlugIn, PlugInArgs, LoadMemorySize], []).

%%--------------------------------------------------------------------
%% @doc
%% Stops the server
%% @end
%%--------------------------------------------------------------------
-spec stop(PlugIn) -> ok when
    PlugIn :: atom().
stop(PlugIn) ->
    gen_server:cast(PlugIn, stop).

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
init([PlugIn, PlugInArgs, LoadMemorySize]) ->
    process_flag(trap_exit, true),
    {ok, InitAns} = PlugIn:init(PlugInArgs),
    ?debug("Plugin ~p initialized with args ~p and result ~p", [PlugIn, PlugInArgs, InitAns]),
    {ok, #host_state{plug_in = PlugIn, plug_in_state = InitAns, load_info = {[], [], 0, LoadMemorySize}}}.

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
handle_call({updatePlugInState, StateTransformFun}, _From, State = #host_state{plug_in_state = PluginState}) when is_function(StateTransformFun) ->
    {reply, ok, State#host_state{plug_in_state =  StateTransformFun(PluginState)}};

handle_call({updatePlugInState, NewPlugInState}, _From, State) ->
    {reply, ok, State#host_state{plug_in_state = NewPlugInState}};

handle_call(getPlugInState, _From, State) ->
    {reply, State#host_state.plug_in_state, State};

handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
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
handle_cast(#worker_request{} = Req, State = #host_state{plug_in = PlugIn, plug_in_state = PluginState}) ->
    spawn(fun() -> proc_request(PlugIn, Req, PluginState) end),
    {noreply, State};

handle_cast({progress_report, Report}, State) ->
    NewLoadInfo = save_progress(Report, State#host_state.load_info),
    {noreply, State#host_state{load_info = NewLoadInfo}};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
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
handle_info({'EXIT', Pid, Reason}, State) ->
    PlugIn = State#host_state.plug_in,
    gen_server:cast(PlugIn, #worker_request{req = {'EXIT', Pid, Reason}}),
    {noreply, State};

handle_info({timer, Msg}, State) ->
    gen_server:cast(self(), Msg),
    {noreply, State};

handle_info(Msg, State) ->
    gen_server:cast(self(), Msg),
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
terminate(_Reason, #host_state{plug_in = PlugIn}) ->
    PlugIn:cleanup(),
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes client request using PlugIn:handle function. Afterwards,
%% it sends the answer to dispatcher and logs info about processing time.
%% @end
%%--------------------------------------------------------------------
-spec proc_request(PlugIn :: atom(), Request :: #worker_request{}, PluginState :: term()) -> Result when
    Result :: atom().
proc_request(PlugIn, Request = #worker_request{req = Msg}, PluginState) ->
    BeforeProcessingRequest = os:timestamp(),
    Response =
        try
            PlugIn:handle(Msg, PluginState)
        catch
            Type:Error ->
                ?error_stacktrace("Worker plug-in ~p error: ~p:~p, on request: ~p", [PlugIn, Type, Error, Request]),
                worker_plug_in_error
        end,
    send_response(PlugIn, BeforeProcessingRequest, Request, Response).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends responce to client
%% @end
%%--------------------------------------------------------------------
-spec send_response(PlugIn :: atom(), BeforeProcessingRequest :: term(), Request :: #worker_request{}, Response :: term()) -> atom().
send_response(PlugIn, BeforeProcessingRequest, #worker_request{id = MsgId, reply_to = ReplyTo}, Response) ->
    case ReplyTo of
        undefined -> ok;
        {gen_serv, Serv} ->
            case MsgId of
                undefined -> gen_server:cast(Serv, Response);
                Id -> gen_server:cast(Serv, #worker_answer{id = Id, response = Response})
            end;
        {proc, Pid} ->
            case MsgId of
                undefined -> Pid ! Response;
                Id -> Pid ! #worker_answer{id = Id, response = Response}
            end;
        Other -> ?error("Wrong reply type: ~p", [Other])
    end,

    AfterProcessingRequest = os:timestamp(),
    Time = timer:now_diff(AfterProcessingRequest, BeforeProcessingRequest),
    gen_server:cast(PlugIn, {progress_report, {BeforeProcessingRequest, Time}}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds information about ended request to host memory (ccm uses
%% it to control cluster load).
%% @end
%%--------------------------------------------------------------------
-spec save_progress(Report :: term(), LoadInfo :: term()) -> NewLoadInfo when
    NewLoadInfo :: term().
save_progress(Report, {New, Old, NewListSize, Max}) ->
    case NewListSize + 1 of
        Max ->
            {[], [Report | New], 0, Max};
        S ->
            {[Report | New], Old, S, Max}
    end.

