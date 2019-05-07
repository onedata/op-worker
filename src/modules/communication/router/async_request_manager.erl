%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module monitors execution of client requests and periodically
%%% send heartbeat messages to inform him that his requests are still being
%%% handled or error messages in case of workers deaths.
%%%
%%% When connection receives a possibly time-consuming request it delegates
%%% execution to appropriate worker and notifies async request manager about
%%% pending request. It is done using cast to avoid deadlock (manager may be
%%% trying to send heartbeat via this connection).
%%% While task is being handled by worker, async request manager periodically
%%% checks its status. If it is still alive heartbeat message is being send
%%% to client informing him that his request is still being processed.
%%% In case it has died, error message is send instead.
%%% Once worker finishes its task, it calls async request manager to withhold
%%% sending heartbeats for this task and tries to send response via the same
%%% connection the request came. If it is not possible, due to some errors,
%%% it tries sending via other connections of peer session. If all goes well
%%% worker informs async request manager about successful sending of response.
%%% Generally it would look like this from sync request manager perspective:
%%%
%%%                            +----------------------------+
%%%                            |    Async request manager   |
%%%  report pending request -->|                            |
%%%       (ReqId, Pid)         | - pending_requests:    #{} |
%%%                            | - withheld_heartbeats: #{} |
%%%                            +----------------------------+
%%%                                         |
%%% ----------------------------------------|---------------------------------
%%%                                         v
%%%                            +----------------------------------------+
%%%                            |         Async request manager          |
%%%    withhold heartbeats --->|                                        |
%%%        (ReqId, Pid)        | - pending_requests:    #{ReqId => Pid} |
%%%                            | - withheld_heartbeats: #{}             |
%%%                            +----------------------------------------+
%%%                                                |
%%% -----------------------------------------------|--------------------------
%%%                                                v
%%%                            +----------------------------------------+
%%%                            |         Async request manager          |
%%%   report response sent --->|                                        |
%%%        (ReqId, Pid)        | - pending_requests:    #{}             |
%%%                            | - withheld_heartbeats: #{ReqId => Pid} |
%%%                            +----------------------------------------+
%%%                                         |
%%% ----------------------------------------|---------------------------------
%%%                                         v
%%%                            +----------------------------+
%%%                            |    Async request manager   |
%%%                            |                            |
%%%                            | - pending_requests:    #{} |
%%%                            | - withheld_heartbeats: #{} |
%%%                            +----------------------------+
%%%
%%%
%%% CAUTION!!!
%%% Because reporting pending requests is done using cast a race when
%%% "withhold heartbeats" call comes earlier then "report pending request" cast
%%% is possible. When the aforementioned cast comes later it would be added to
%%% pending_requests map and error would be send to peer on next workers checkup
%%% (because worker already handled request and responded == it is dead).
%%% To avoid such situation 1 more mapping is needed -> unreported_requests.
%%% When "withhold heartbeats" call comes and no req_id to worker pid mapping
%%% is kept in pending_requests it is added to both withheld_heartbeats and
%%% unreported_requests. When the "report pending request" cast finally comes
%%% unreported_requests map will be checked first. If it contains association
%%% then no new mapping will be added but it will be removed from
%%% unreported_requests.
%%%
%%% Example of this:
%%%
%%%                            +----------------------------+
%%%                            |   Async request manager    |
%%%    withhold heartbeats --->|                            |
%%%        (ReqId, Pid)        | - pending_requests:    #{} |
%%%                            | - unreported_requests: #{} |
%%%                            | - withheld_heartbeats: #{} |
%%%                            +----------------------------+
%%%                                         |
%%% ----------------------------------------|---------------------------------
%%%                                         v
%%%                            +----------------------------------------+
%%%                            |    Async request manager               |
%%%  report pending request -->|                                        |
%%%       (ReqId, Pid)         | - pending_requests:    #{}             |
%%%                            | - unreported_requests: #{ReqId => Pid} |
%%%                            | - withheld_heartbeats: #{ReqId => Pid} |
%%%                            +----------------------------------------+
%%%                                                |
%%% -----------------------------------------------|--------------------------
%%%                                                v
%%%                            +----------------------------------------+
%%%                            |         Async request manager          |
%%%   report response sent --->|                                        |
%%%        (ReqId, Pid)        | - pending_requests:    #{}             |
%%%                            | - unreported_requests: #{}             |
%%%                            | - withheld_heartbeats: #{ReqId => Pid} |
%%%                            +----------------------------------------+
%%%                                         |
%%% ----------------------------------------|---------------------------------
%%%                                         v
%%%                            +----------------------------+
%%%                            |    Async request manager   |
%%%                            |                            |
%%%                            | - pending_requests:    #{} |
%%%                            | - unreported_requests: #{} |
%%%                            | - withheld_heartbeats: #{} |
%%%                            +----------------------------+
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(async_request_manager).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("timeouts.hrl").
-include("modules/communication/connection.hrl").
-include("proto/common/clproto_message_id.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([start_link/1]).
-export([delegate_and_supervise/4]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-record(state, {
    session_id :: session:id(),

    pending_requests = #{} :: #{req_id() => pid() | {pid(), not_alive}},
    unreported_requests = #{} :: #{req_id() => pid()},
    withheld_heartbeats = #{} :: #{req_id() => pid() | {pid(), not_alive}},

    heartbeat_timer = undefined :: undefined | reference()
}).

-type state() :: #state{}.
-type req_id() :: {reference(), clproto_message_id:id()}.
-type server_message() :: #server_message{}.

-type worker_ref() :: proc | module() | {module(), node()}.
-type respond_via() :: {Conn :: pid(), AsyncReqManager :: pid(), session:id()}.

-type error() :: {error, Reason :: term()}.

-define(HEARTBEAT_MSG(__MSG_ID), #server_message{
    message_id = __MSG_ID,
    message_body = #processing_status{code = 'IN_PROGRESS'}
}).
-define(ERROR_MSG(__MSG_ID), #server_message{
    message_id = __MSG_ID,
    message_body = #processing_status{code = 'ERROR'}
}).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts the async_request_manager server for specified session.
%% @end
%%--------------------------------------------------------------------
-spec start_link(session:id()) -> {ok, pid()} | ignore | error().
start_link(SessionId) ->
    gen_server:start_link(?MODULE, [SessionId], []).


%%--------------------------------------------------------------------
%% @doc
%% Delegates handling of request to specified worker or spawned process
%% (if worker_ref is `proc`). When worker finishes it's work,
%% it will send result using given RespondVia info.
%% @end
%%--------------------------------------------------------------------
-spec delegate_and_supervise(worker_ref(), term(), clproto_message_id:id(),
    respond_via()) -> ok | {ok, server_message()}.
delegate_and_supervise(WorkerRef, Req, MsgId, RespondVia) ->
    try
        ReqId = {make_ref(), MsgId},
        ok = delegate_request_insecure(WorkerRef, Req, ReqId, RespondVia)
    catch
        Type:Error ->
            ?error_stacktrace("Failed to delegate request (~p) due to: ~p:~p", [
                MsgId, Type, Error
            ]),
            {ok, ?ERROR_MSG(MsgId)}
    end.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, state()} | {ok, state(), timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([SessionId]) ->
    process_flag(trap_exit, true),
    ok = session_connections:set_async_request_manager(SessionId, self()),
    {ok, #state{session_id = SessionId}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call({withhold_heartbeats, Pid, ReqId}, _From, #state{
    pending_requests = PendingReqs,
    unreported_requests = UnreportedReqs,
    withheld_heartbeats = WithheldHeartbeats
} = State) ->
    {NewPendingReqs, NewUnreportedReqs} = case maps:take(ReqId, PendingReqs) of
        {_, PendingReqs2} ->
            {PendingReqs2, UnreportedReqs};
        error ->
            {PendingReqs, UnreportedReqs#{ReqId => Pid}}
    end,
    NewState = State#state{
        pending_requests = NewPendingReqs,
        unreported_requests = NewUnreportedReqs,
        withheld_heartbeats = WithheldHeartbeats#{ReqId => Pid}
    },
    {reply, ok, schedule_workers_status_checkup(NewState)};
handle_call({response_sent, ReqId}, _From, #state{
    withheld_heartbeats = WithheldHeartbeats
} = State) ->
    {reply, ok, State#state{
        withheld_heartbeats = maps:remove(ReqId, WithheldHeartbeats)
    }};
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, {error, wrong_request}, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast({report_pending_req, Pid, ReqId}, #state{
    pending_requests = PendingReqs,
    unreported_requests = UnreportedReqs
} = State) ->
    NewState = case maps:take(ReqId, UnreportedReqs) of
        {Pid, NewUnreportedReqs} ->
            State#state{unreported_requests = NewUnreportedReqs};
        error ->
            schedule_workers_status_checkup(State#state{
                pending_requests = PendingReqs#{ReqId => Pid}
            })
    end,
    {noreply, NewState};
handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_info(heartbeat, #state{
    pending_requests = PR,
    withheld_heartbeats = WH
} = State) when map_size(PR) == 0 andalso map_size(WH) == 0 ->
    {noreply, State#state{heartbeat_timer = undefined}};
handle_info(heartbeat, #state{
    session_id = SessionId,
    pending_requests = PR,
    withheld_heartbeats = WH
} = State) ->
    NewState = case session_connections:list(SessionId) of
        {ok, Cons} ->
            State#state{
                pending_requests = check_workers_status(PR, Cons, true),
                withheld_heartbeats = check_workers_status(WH, Cons, false),
                heartbeat_timer = undefined
            };
        Error ->
            ?error("Async request manager for session ~p failed to send "
                   "heartbeats due to: ~p", [
                SessionId, Error
            ]),
            State#state{heartbeat_timer = undefined}
    end,
    {noreply, schedule_workers_status_checkup(NewState)};
handle_info(Info, State) ->
    ?log_bad_request(Info),
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
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    state()) -> term().
terminate(_Reason, _State) ->
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, state(), Extra :: term()) ->
    {ok, NewState :: state()} | error().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec delegate_request_insecure(worker_ref(), term(), req_id(), respond_via()) ->
    ok | error().
delegate_request_insecure(proc, HandlerFun, ReqId, RespondVia) ->
    Pid = spawn(fun() ->
        Response = try
            HandlerFun()
        catch
            Type:Error ->
                ?error("Failed to handle delegated request ~p due to ~p:~p", [
                    ReqId, Type, Error
                ]),
                #processing_status{code = 'ERROR'}
        end,
        respond(RespondVia, ReqId, Response)
    end),
    report_pending_request(RespondVia, Pid, ReqId);

delegate_request_insecure(WorkerRef, Req, ReqId, RespondVia) ->
    ReplyFun =
        fun
            ({ok, Response}) ->
                respond(RespondVia, ReqId, Response);
            ({error, Reason}) ->
                ?error("Failed to handle delegated request ~p due to ~p", [
                    ReqId, Reason
                ]),
                respond(RespondVia, ReqId, #processing_status{code = 'ERROR'})
        end,
    case worker_proxy:cast_and_monitor(WorkerRef, Req, ReplyFun, ReqId) of
        Pid when is_pid(Pid) ->
            report_pending_request(RespondVia, Pid, ReqId);
        Error ->
            Error
    end.


%% @private
-spec report_pending_request(respond_via(), pid(), req_id()) -> ok.
report_pending_request({_, AsyncReqManager, _}, Pid, ReqId) ->
    gen_server2:cast(AsyncReqManager, {report_pending_req, Pid, ReqId}).


%% @private
-spec respond(respond_via(), req_id(), term()) -> ok | error().
respond({Conn, AsyncReqManager, SessionId}, {_Ref, MsgId} = ReqId, Response) ->
    case withhold_heartbeats(AsyncReqManager, ReqId) of
        ok ->
            Msg = #server_message{
                message_id = MsgId,
                message_body = Response
            },
            case send_response(Conn, SessionId, Msg) of
                ok ->
                    report_response_sent(AsyncReqManager, ReqId);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


%% @private
-spec withhold_heartbeats(AsyncReqManager :: pid(), req_id()) ->
    ok | error().
withhold_heartbeats(AsyncReqManager, ReqId) ->
    Req = {withhold_heartbeats, self(), ReqId},
    call_async_request_manager(AsyncReqManager, Req).


%% @private
-spec send_response(pid(), session:id(), server_message()) ->
    ok | error().
send_response(Conn, SessionId, Response) ->
    case connection:send_msg(Conn, Response) of
        ok ->
            ok;
        {error, serialization_failed} = SerializationError ->
            SerializationError;
        {error, sending_msg_via_wrong_conn_type} = WrongConnError ->
            WrongConnError;
        _Error ->
            connection_api:send(SessionId, Response, [Conn])
    end.


%% @private
-spec report_response_sent(AsyncReqManager :: pid(), req_id()) ->
    ok | error().
report_response_sent(AsyncReqManager, ReqId) ->
    call_async_request_manager(AsyncReqManager, {response_sent, ReqId}).


%% @private
-spec call_async_request_manager(AsyncReqManager :: pid(), term()) ->
    ok | error().
call_async_request_manager(AsyncReqManager, Msg) ->
    try
        gen_server2:call(AsyncReqManager, Msg, ?DEFAULT_REQUEST_TIMEOUT)
    catch
        exit:{noproc, _} ->
            ?debug("Request manager process ~p does not exist", [
                AsyncReqManager
            ]),
            {error, no_async_req_manager};
        exit:{normal, _} ->
            ?debug("Exit of request manager process ~p", [AsyncReqManager]),
            {error, no_async_req_manager};
        exit:{timeout, _} ->
            ?debug("Timeout of request manager process ~p", [AsyncReqManager]),
            ?ERROR_TIMEOUT;
        Type:Reason ->
            ?error("Cannot call request manager ~p due to ~p:~p", [
                AsyncReqManager, Type, Reason
            ]),
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether workers/processes handling requests are still alive.
%% If they are and `SendHeartbeats` flag is set then informs peer about
%% ongoing requests processing by sending heartbeat messages.
%% Otherwise (dead workers) schedules error messages to be send during
%% next checkup. They are not send immediately to avoid potential race
%% between response and heartbeats.
%% @end
%%--------------------------------------------------------------------
-spec check_workers_status(Workers, [pid()], boolean()) -> Workers when
    Workers :: #{req_id() => pid() | {pid(), not_alive}}.
check_workers_status(Workers, Cons, SendHeartbeats) ->
    maps:fold(
        fun
            ({_Ref, MsgId} = ReqId, {Pid, not_alive}, Acc) ->
                ?error("Async Request Manager: process ~p handling request ~p died",
                    [Pid, ReqId]
                ),
                connection_api:send_via_any(?ERROR_MSG(MsgId), Cons),
                Acc;
            ({_Ref, MsgId} = ReqId, Pid, Acc) ->
                SendHeartbeats andalso connection_api:send_via_any(
                    ?HEARTBEAT_MSG(MsgId), Cons
                ),
                case rpc:call(node(Pid), erlang, is_process_alive, [Pid]) of
                    true ->
                        Acc#{ReqId => Pid};
                    false ->
                        Acc#{ReqId => {Pid, not_alive}}
                end
        end, #{}, Workers
    ).


%% @private
-spec schedule_workers_status_checkup(state()) -> state().
schedule_workers_status_checkup(#state{heartbeat_timer = undefined} = State) ->
    State#state{heartbeat_timer = erlang:send_after(
        ?WORKERS_STATUS_CHECK_INTERVAL, self(), heartbeat
    )};
schedule_workers_status_checkup(State) ->
    State.
