%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module encapsulates API for communication with given session but also
%%% monitors processing of asynchronous requests and mediates when sending
%%% responses.
%%%
%%% When connection receives more advanced request it delegates execution to
%%% appropriate worker and notifies connection manager about pending request.
%%% It is done using cast to avoid deadlock (connection manager may be trying
%%% to send message via this connection).
%%% While task is being handled by worker, connection manager periodically
%%% checks his status. If it is still alive heartbeat message is being send
%%% to client informing him that his request is still being processed.
%%% In case it is dead, error message is send instead.
%%% Once worker finishes it's task, it calls connection manager to withheld
%%% sending heartbeats for this task and tries to send response via the same
%%% connection the request came. If it is not possible, due to some errors,
%%% it tries sending via other connections of client session. If all goes well
%%% worker informs connection manager about successful sending of response.
%%% @end
%%%-------------------------------------------------------------------
-module(connection_manager).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("timeouts.hrl").
-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2]).
-export([
    communicate/2,
    send/2, send/3,

    assign_request_id/1,
    report_pending_request/3,
    respond/3
]).

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
-type client_message() :: #client_message{}.
-type server_message() :: #server_message{}.
-type message() :: client_message() | server_message().

-type req_id() :: {reference(), message_id:id()}.
-type reply_to() :: {Conn :: pid(), ConnManager :: pid(), session:id()} | session:id().

-define(WORKERS_STATUS_CHECK_INTERVAL, application:get_env(
    ?APP_NAME, router_processes_check_interval, timer:seconds(10)
)).
-define(KEEPALIVE_TIMEOUT, timer:seconds(60)).

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
%% Sends message to peer and awaits answer. If no answer or heartbeat is
%% sent within 3 ?WORKERS_STATUS_CHECK_INTERVAL then timeout error
%% is returned.
%% In case of errors during sending tries other session connections
%% until message is send or no more available connections remains.
%% Exceptions to this are encoding errors which immediately fails call.
%% @end
%%--------------------------------------------------------------------
-spec communicate(session:id(), message()) ->
    {ok, message()} | {error, term()}.
communicate(SessionId, RawMsg) ->
    {ok, MsgId} = message_id:generate(self()),
    Msg = set_msg_id(RawMsg, MsgId),
    case send_msg_internal(SessionId, Msg, []) of
        ok ->
            await_response(Msg);
        Error ->
            ?error("Failed to communicate msg ~p to peer ~p due to: ~p", [
                Msg, SessionId, Error
            ]),
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% @equiv send_sync(SessionId, Msg, []).
%% @end
%%--------------------------------------------------------------------
-spec send(session:id(), message()) ->
    ok | {error, Reason :: term()}.
send(SessionId, Msg) ->
    send(SessionId, Msg, []).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to peer. In case of errors during sending tries other
%% session connections until message is send or no more available
%% connections remains.
%% Exceptions to this are encoding errors which immediately fails call.
%% @end
%%--------------------------------------------------------------------
-spec send(session:id(), message(), ExcludedCons :: [pid()]) ->
    ok | {error, Reason :: term()}.
send(SessionId, Msg, ExcludedCons) ->
    case send_msg_internal(SessionId, Msg, ExcludedCons) of
        ok ->
            ok;
        Error ->
            ?error("Failed to send msg ~p to peer ~p due to: ~p", [
                Msg, SessionId, Error
            ]),
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates unique id for request using specified message id.
%% @end
%%--------------------------------------------------------------------
-spec assign_request_id(message_id:id()) -> req_id().
assign_request_id(MsgId) ->
    Ref = make_ref(),
    {Ref, MsgId}.


%%--------------------------------------------------------------------
%% @doc
%% Informs connection manager about pending request (identifiable by
%% specified request id) being processed by specified process.
%% @end
%%--------------------------------------------------------------------
-spec report_pending_request(reply_to(), pid(), req_id()) ->
    ok | {error, term()}.
report_pending_request({_, ConnManager, _}, Pid, ReqId) ->
    gen_server2:cast(ConnManager, {report_pending_req, Pid, ReqId});
report_pending_request(SessionId, Pid, ReqId) ->
    case session_connections:get_connection_manager(SessionId) of
        {ok, ConnManager} ->
            gen_server2:cast(ConnManager, {report_pending_req, Pid, ReqId});
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Sends response to peer.
%% @end
%%--------------------------------------------------------------------
-spec respond(reply_to(), req_id(), term()) -> ok | {error, term()}.
respond({Conn, ConnManager, SessionId}, {_Ref, MsgId} = ReqId, Ans) ->
    case withheld_heartbeats(ConnManager, ReqId) of
        ok ->
            Response = prepare_response(MsgId, Ans),
            case respond_internal(Conn, SessionId, Response) of
                ok ->
                    report_response_sent(ConnManager, ReqId);
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
    case session_connections:get_random_conn_and_conn_manager(SessionId) of
        {ok, Conn, ConnManager} ->
            respond({Conn, ConnManager, SessionId}, ReqId, Ans);
        Error ->
            Error
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
init([SessId, SetKeepaliveTimeout]) ->
    process_flag(trap_exit, true),
    Self = self(),
    {ok, _} = session:update(SessId, fun(Session = #session{}) ->
        {ok, Session#session{connection_manager = Self}}
    end),
    maybe_set_keepalive_timeout(SetKeepaliveTimeout),
    {ok, #state{session_id = SessId}}.


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
handle_call({withheld_heartbeats, Pid, ReqId}, _From, #state{
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
    {reply, ok, set_heartbeat_timer(NewState)};
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
            set_heartbeat_timer(State#state{
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
    {noreply, set_heartbeat_timer(NewState)};
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
    maybe_set_keepalive_timeout(true),
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


%% @private
-spec send_msg_internal(session:id(), message(), ExcludedCons :: [pid()]) ->
    ok | {error, term()}.
send_msg_internal(SessionId, Msg, ExcludedCons) ->
    case session_connections:get_connections(SessionId) of
        {ok, Cons} ->
            send_in_loop(Msg, utils:random_shuffle(Cons -- ExcludedCons));
        Error ->
            Error
    end.


%% @private
-spec send_in_loop(message(), [pid()]) -> ok | {error, term()}.
send_in_loop(_Msg, []) ->
    {error, no_connections};
send_in_loop(Msg, [Conn]) ->
    connection:send_msg(Conn, Msg);
send_in_loop(Msg, [Conn | Cons]) ->
    case connection:send_msg(Conn, Msg) of
        ok ->
            ok;
        {error, serialization_failed} = SerializationError ->
            SerializationError;
        {error, sending_msg_via_wrong_connection} = WrongConnError ->
            WrongConnError;
        _Error ->
            send_in_loop(Msg, Cons)
    end.


%% @private
-spec await_response(message()) -> {ok, message()} | {error, timeout}.
await_response(#client_message{message_id = MsgId} = Msg) ->
    Timeout = 3 * ?WORKERS_STATUS_CHECK_INTERVAL,
    receive
        #server_message{
            message_id = MsgId,
            message_body = #processing_status{code = 'IN_PROGRESS'}
        } ->
            await_response(Msg);
        #server_message{message_id = MsgId} = ServerMsg ->
            {ok, ServerMsg}
    after Timeout ->
        {error, timeout}
    end;
await_response(#server_message{message_id = MsgId}) ->
    receive
        #client_message{message_id = MsgId} = ClientMsg ->
            {ok, ClientMsg}
    % TODO VFS-4025 - how long should we wait for client answer?
    after ?DEFAULT_REQUEST_TIMEOUT ->
        {error, timeout}
    end.


%% @private
-spec prepare_response(message_id:id(), {ok, term()} | term()) ->
    server_message().
prepare_response(MsgId, {ok, Ans}) ->
    #server_message{
        message_id = MsgId,
        message_body = Ans
    };
prepare_response(MsgId, ErrorAns) ->
    ?error("Error while handling request with id ~p due to ~p", [
        MsgId, ErrorAns
    ]),
    #server_message{
        message_id = MsgId,
        message_body = #processing_status{code = 'ERROR'}
    }.


%% @private
-spec withheld_heartbeats(ConnManager :: pid(), req_id()) ->
    ok | {error, term()}.
withheld_heartbeats(ConnManager, ReqId) ->
    call_conn_manager(ConnManager, {withheld_heartbeats, self(), ReqId}).


%% @private
respond_internal(Conn, SessionId, Response) ->
    case connection:send_msg(Conn, Response) of
        ok ->
            ok;
        {error, serialization_failed} = SerializationError ->
            SerializationError;
        {error, sending_msg_via_wrong_connection} = WrongConnError ->
            WrongConnError;
        _Error ->
            send_msg_internal(SessionId, Response, [Conn])
    end.


%% @private
-spec report_response_sent(ConnManager :: pid(), req_id()) ->
    ok | {error, term()}.
report_response_sent(ConnManager, ReqId) ->
    call_conn_manager(ConnManager, {response_sent, ReqId}).


%% @private
-spec call_conn_manager(ConnManager :: pid(), term()) ->
    ok | {error, term()}.
call_conn_manager(ConnManager, Msg) ->
    try
        gen_server2:call(ConnManager, Msg)
    catch
        exit:{noproc, _} ->
            ?debug("Connection manager process ~p does not exist", [
                ConnManager
            ]),
            {error, no_connection_manager};
        exit:{normal, _} ->
            ?debug("Exit of connection manager process ~p", [ConnManager]),
            {error, no_connection_manager};
        exit:{timeout, _} ->
            ?debug("Timeout of connection manager process ~p", [ConnManager]),
            {error, timeout};
        Type:Reason ->
            ?error("Cannot call connection manager ~p due to ~p:~p", [
                ConnManager, Type, Reason
            ]),
            {error, Reason}
    end.


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
                ?error("ConnManager: process ~p connected with req_id ~p is dead",
                    [Pid, ReqId]
                ),
                send_in_loop(?ERROR_MSG(MsgId), Cons),
                Acc;
            ({_Ref, MsgId} = ReqId, Pid, Acc) ->
                case SendHeartbeats of
                    true ->
                        send_in_loop(?HEARTBEAT_MSG(MsgId), Cons);
                    false ->
                        ok
                end,
                case rpc:call(node(Pid), erlang, is_process_alive, [Pid]) of
                    true ->
                        Acc#{ReqId => Pid};
                    false ->
                        Acc#{ReqId => {Pid, not_alive}}
                end
        end, #{}, Workers
    ).


%% @private
-spec set_msg_id(message(), message_id:id()) -> message().
set_msg_id(#client_message{} = Msg, MsgId) ->
    Msg#client_message{message_id = MsgId};
set_msg_id(#server_message{} = Msg, MsgId) ->
    Msg#server_message{message_id = MsgId}.


%% @private
-spec set_heartbeat_timer(state()) -> state().
set_heartbeat_timer(#state{heartbeat_timer = undefined} = State) ->
    TimerRef = erlang:send_after(?WORKERS_STATUS_CHECK_INTERVAL, self(), heartbeat),
    State#state{heartbeat_timer = TimerRef};
set_heartbeat_timer(State) ->
    State.


%% @private
-spec maybe_set_keepalive_timeout(SetTimeout :: boolean()) ->
    TimerRef :: reference().
maybe_set_keepalive_timeout(true) ->
    erlang:send_after(?KEEPALIVE_TIMEOUT, self(), keepalive);
maybe_set_keepalive_timeout(false) ->
    ok.
