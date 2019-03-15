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
%%% handled.
%%%
%%% When connection receives a possibly time-consuming request it delegates
%%% execution to appropriate worker and notifies async request manager about
%%% pending request. It is done using cast to avoid deadlock (manager may be
%%% trying to send heartbeat via this connection).
%%% While task is being handled by worker, async request manager periodically
%%% checks its status. If it is still alive heartbeat message is being send
%%% to client informing him that his request is still being processed.
%%% In case it is dead, error message is send instead.
%%% Once worker finishes its task, it calls async request manager to withhold
%%% sending heartbeats for this task and tries to send response via the same
%%% connection the request came. If it is not possible, due to some errors,
%%% it tries sending via other connections of client session. If all goes well
%%% worker informs async request manager about successful sending of response.
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

%% API
-export([start_link/2]).
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
-type reply_to() :: session:id() | {Conn :: pid(), AsyncReqManager :: pid(), session:id()}.


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
%% Starts the server and optionally sets keepalive timeout (it should
%% be done only for provider_outgoing sessions).
%% @end
%%--------------------------------------------------------------------
-spec start_link(SessId :: session:id(), SetKeepaliveTimeout :: boolean()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(SessId, SetKeepaliveTimeout) ->
    gen_server:start_link(?MODULE, [SessId, SetKeepaliveTimeout], []).


%%--------------------------------------------------------------------
%% @doc
%% Delegates handling of request to specified worker. When worker finishes
%% it's work, he will send result using given ReplyTo info.
%% @end
%%--------------------------------------------------------------------
-spec delegate_and_supervise(worker_ref(), term(), clproto_message_id:id(),
    reply_to()) -> ok | {ok, server_message()}.
delegate_and_supervise(WorkerRef, Req, MsgId, ReplyTo) ->
    try
        ReqId = {make_ref(), MsgId},
        ok = delegate_request_insecure(WorkerRef, Req, ReqId, ReplyTo)
    catch
        Type:Error ->
            ?error_stacktrace("Router error: ~p:~p for message id ~p", [
                Type, Error, MsgId
            ]),
            {ok, #server_message{
                message_id = MsgId,
                message_body = #processing_status{code = 'ERROR'}
            }}
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
init([SessionId, EnableKeepalive]) ->
    process_flag(trap_exit, true),
    ok = session:set_async_request_manager(SessionId, self()),
    EnableKeepalive andalso schedule_keepalive_msg(),
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
    NewState = case session_connections:get_connections(SessionId) of
        {ok, Cons} ->
            State#state{
                pending_requests = check_workers_status(PR, Cons, true),
                withheld_heartbeats = check_workers_status(WH, Cons, false),
                heartbeat_timer = undefined
            };
        Error ->
            ?error("Failed to fetch connections to send heartbeats because "
                   "of: ~p", [Error]),
            State#state{heartbeat_timer = undefined}
    end,
    {noreply, schedule_workers_status_checkup(NewState)};
handle_info(keepalive, #state{session_id = SessionId} = State) ->
    case session_connections:get_connections(SessionId) of
        {ok, Cons} ->
            lists:foreach(fun(Conn) -> 
                connection:send_keepalive(Conn) 
            end, Cons);
        Error ->
            ?error("Failed to fetch connection to send keepalives because "
                   "of: ~p", [Error])
    end,
    schedule_keepalive_msg(),
    {noreply, State};
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
    {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% If worker_ref is `proc` then spawns new process to handle request.
%% Otherwise delegates it to specified worker via worker_proxy.
%% @end
%%--------------------------------------------------------------------
-spec delegate_request_insecure(worker_ref(), Req :: term(),
    connection_api:req_id(), connection_api:reply_to()) ->
    ok | {error, term()}.
delegate_request_insecure(proc, HandlerFun, ReqId, ReplyTo) ->
    Pid = spawn(fun() ->
        Response = try
            HandlerFun()
        catch
            Type:Error ->
                ?error("Router local delegation error: ~p for request id ~p", [
                    Type, Error, ReqId
                ]),
                #processing_status{code = 'ERROR'}
        end,
        respond(ReplyTo, ReqId, {ok, Response})
    end),
    report_pending_request(ReplyTo, Pid, ReqId);

delegate_request_insecure(WorkerRef, Req, ReqId, ReplyTo) ->
    ReplyFun = fun(Response) -> respond(ReplyTo, ReqId, Response) end,
    case worker_proxy:cast_and_monitor(WorkerRef, Req, ReplyFun, ReqId) of
        Pid when is_pid(Pid) ->
            report_pending_request(ReplyTo, Pid, ReqId);
        Error ->
            Error
    end.


%% @private
-spec report_pending_request(reply_to(), pid(), req_id()) ->
    ok | {error, term()}.
report_pending_request({_, AsyncReqManager, _}, Pid, ReqId) ->
    gen_server2:cast(AsyncReqManager, {report_pending_req, Pid, ReqId});
report_pending_request(SessionId, Pid, ReqId) ->
    case session_connections:get_async_req_manager(SessionId) of
        {ok, AsyncReqManager} ->
            gen_server2:cast(AsyncReqManager, {report_pending_req, Pid, ReqId});
        Error ->
            Error
    end.


%% @private
-spec respond(reply_to(), req_id(), term()) -> ok | {error, term()}.
respond({Conn, AsyncReqManager, SessionId}, {_Ref, MsgId} = ReqId, Ans) ->
    case withhold_heartbeats(AsyncReqManager, ReqId) of
        ok ->
            Response = build_response(MsgId, Ans),
            case respond_via_any_conn(Conn, SessionId, Response) of
                ok ->
                    report_response_sent(AsyncReqManager, ReqId);
                {error, no_connections} = NoConsError ->
                    ?debug("Failed to send response to ~p due to no connections", [
                        SessionId
                    ]),
                    NoConsError;
                Error ->
                    ?error("Failed to send response ~p to peer ~p due to: ~p", [
                        Response, SessionId, Error
                    ]),
                    Error
            end;
        Error ->
            Error
    end;
respond(SessionId, ReqId, Ans) ->
    case session_connections:get_random_conn_and_async_req_manager(SessionId) of
        {ok, Conn, AsyncReqManager} ->
            respond({Conn, AsyncReqManager, SessionId}, ReqId, Ans);
        Error ->
            Error
    end.


%% @private
-spec withhold_heartbeats(AsyncReqManager :: pid(), req_id()) ->
    ok | {error, term()}.
withhold_heartbeats(AsyncReqManager, ReqId) ->
    Req = {withhold_heartbeats, self(), ReqId},
    call_async_request_manager(AsyncReqManager, Req).


%% @private
-spec build_response(clproto_message_id:id(), {ok, term()} | term()) ->
    server_message().
build_response(MsgId, {ok, Ans}) ->
    #server_message{
        message_id = MsgId,
        message_body = Ans
    };
build_response(MsgId, ErrorAns) ->
    ?error("Error while handling request with id ~p due to ~p", [
        MsgId, ErrorAns
    ]),
    #server_message{
        message_id = MsgId,
        message_body = #processing_status{code = 'ERROR'}
    }.


%% @private
-spec respond_via_any_conn(pid(), session:id(), server_message()) ->
    ok | {error, term()}.
respond_via_any_conn(Conn, SessionId, Response) ->
    case connection:send_msg(Conn, Response) of
        ok ->
            ok;
        {error, serialization_failed} = SerializationError ->
            SerializationError;
        {error, sending_msg_via_wrong_connection} = WrongConnError ->
            WrongConnError;
        _Error ->
            connection_utils:send_msg_excluding_connections(
                SessionId, Response, [Conn]
            )
    end.


%% @private
-spec report_response_sent(AsyncReqManager :: pid(), req_id()) ->
    ok | {error, term()}.
report_response_sent(AsyncReqManager, ReqId) ->
    call_async_request_manager(AsyncReqManager, {response_sent, ReqId}).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether workers/processes handling requests are still alive.
%% If they are, optionally (SendHeartbeats flag) informs peer about
%% ongoing requests processing by sending heartbeat messages.
%% Otherwise (dead workers) schedules error messages to be send during
%% next checkup. They are not send immediately to avoid race
%% response/heartbeats.
%% @end
%%--------------------------------------------------------------------
-spec check_workers_status(Workers, [pid()], boolean()) -> Workers when
    Workers :: #{req_id() => pid() | {pid(), not_alive}}.
check_workers_status(Workers, Cons, SendHeartbeats) ->
    maps:fold(
        fun
            ({_Ref, MsgId} = ReqId, {Pid, not_alive}, Acc) ->
                ?error("AsyncManager: process ~p connected with req_id ~p is dead",
                    [Pid, ReqId]
                ),
                send_via_any_conn(?ERROR_MSG(MsgId), Cons),
                Acc;
            ({_Ref, MsgId} = ReqId, Pid, Acc) ->
                SendHeartbeats andalso send_via_any_conn(?HEARTBEAT_MSG(MsgId), Cons),
                case rpc:call(node(Pid), erlang, is_process_alive, [Pid]) of
                    true ->
                        Acc#{ReqId => Pid};
                    false ->
                        Acc#{ReqId => {Pid, not_alive}}
                end
        end, #{}, Workers
    ).


%% @private
-spec send_via_any_conn(#server_message{}, [pid()]) -> ok | {error, term()}.
send_via_any_conn(Msg, Cons) ->
    connection_utils:send_via_any_connection(Msg, Cons).


%% @private
-spec call_async_request_manager(AsyncReqManager :: pid(), term()) ->
    ok | {error, term()}.
call_async_request_manager(AsyncReqManager, Msg) ->
    try
        gen_server2:call(AsyncReqManager, Msg)
    catch
        exit:{noproc, _} ->
            ?debug("Connection manager process ~p does not exist", [
                AsyncReqManager
            ]),
            {error, no_async_req_manager};
        exit:{normal, _} ->
            ?debug("Exit of connection manager process ~p", [AsyncReqManager]),
            {error, no_async_req_manager};
        exit:{timeout, _} ->
            ?debug("Timeout of connection manager process ~p", [AsyncReqManager]),
            {error, timeout};
        Type:Reason ->
            ?error("Cannot call connection manager ~p due to ~p:~p", [
                AsyncReqManager, Type, Reason
            ]),
            {error, Reason}
    end.


%% @private
-spec schedule_workers_status_checkup(state()) -> state().
schedule_workers_status_checkup(#state{heartbeat_timer = undefined} = State) ->
    State#state{heartbeat_timer = erlang:send_after(
        ?WORKERS_STATUS_CHECK_INTERVAL, self(), heartbeat
    )};
schedule_workers_status_checkup(State) ->
    State.


%% @private
-spec schedule_keepalive_msg() -> TimerRef :: reference().
schedule_keepalive_msg() ->
    erlang:send_after(?KEEPALIVE_TIMEOUT, self(), keepalive).
