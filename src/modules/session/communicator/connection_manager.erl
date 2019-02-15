%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME
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
-export([start_link/0]).
-export([
    communicate/2,
    send_sync/2, send_sync/3,
    send_async/2,
    respond/3
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-define(SERVER, ?MODULE).

-record(state, {
    pending_requests = #{} :: #{reference() => pid()},
    unreported_requests = #{} :: #{reference() => pid()},
    withheld_heartbeats = #{} :: #{reference() => pid()}
}).

-type state() :: #state{}.
-type server_message() :: #server_message{}.
-type message() :: #client_message{} | server_message().

-type req_id() :: {reference(), message_id:id()}.
-type reply_to() :: {Conn :: pid(), ConnManager :: pid(), session:id()} | session:id().

-define(DEFAULT_PROCESSES_CHECK_INTERVAL, timer:seconds(10)).

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
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to peer and awaits answer.
%% @end
%%--------------------------------------------------------------------
-spec communicate(session:id(), message()) ->
    {ok, message()} | {error, term()}.
communicate(SessionId, Msg0) ->
    {ok, MsgId} = message_id:generate(self()),
    Msg1 = fill_msg_id(Msg0, MsgId),
    case send_sync_internal(SessionId, Msg1) of
        ok ->
            await_response(Msg1);
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% @equiv send_sync(SessionId, Msg, undefined).
%% @end
%%--------------------------------------------------------------------
-spec send_sync(session:id(), message()) ->
    ok | {ok, message_id:id()} | {error, Reason :: term()}.
send_sync(SessionId, Msg) ->
    send_sync(SessionId, Msg, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to peer.
%% @end
%%--------------------------------------------------------------------
-spec send_sync(session:id(), message(), Recipient :: undefined | pid()) ->
    ok | {ok, message_id:id()} | {error, Reason :: term()}.
send_sync(SessionId, Msg0, Recipient) ->
    {MsgId, Msg1} = maybe_fill_msg_id(Msg0, Recipient),
    case {send_sync_internal(SessionId, Msg1), MsgId} of
        {ok, undefined} ->
            ok;
        {ok, _} ->
            {ok, MsgId};
        {Error, _} ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Schedules message to be send to peer.
%% @end
%%--------------------------------------------------------------------
-spec send_async(session:id(), message()) -> ok.
send_async(SessionId, Msg) ->
    case session_connections:get_random_connection(SessionId) of
        {ok, Conn} ->
            connection:send_async(Conn, Msg);
        _Error ->
            ok
    end.


report_pending_request(ReplyTo, Pid, Ref) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Sends response to peer.
%% @end
%%--------------------------------------------------------------------
-spec respond(reply_to(), req_id(), term()) -> ok | {error, term()}.
respond({Conn, ConnManager, SessionId}, {Ref, MsgId}, Ans) ->
    case withheld_heartbeats(ConnManager, Ref) of
        ok ->
            Response = prepare_response(MsgId, Ans),
            case respond_internal(Conn, SessionId, Response) of
                ok ->
                    report_sending_response(ConnManager, Ref);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


respond_internal(Conn, SessionId, Response) ->
    case connection:send_sync(Conn, Response) of
        ok ->
            ok;
        {error, serialization_failed} = SerializationError ->
            SerializationError;
        {error, sending_msg_via_wrong_connection} = WrongConnError ->
            WrongConnError;
        _Error ->
            send_sync_internal(SessionId, Response, [Conn])
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes answer and returns message to be sent.
%% @end
%%--------------------------------------------------------------------
-spec prepare_response(message_id:id(), term()) -> server_message().
prepare_response(MsgId, {process_error, ErrorAns}) ->
    ?error("Router wrong answer: ~p for message id ~p", [ErrorAns, MsgId]),
    #server_message{
        message_id = MsgId,
        message_body = #processing_status{code = 'ERROR'}
    };
prepare_response(MsgId, Ans) ->
    #server_message{
        message_id = MsgId,
        message_body = Ans
    }.


-spec withheld_heartbeats(ConnManager :: pid(), reference()) ->
    ok | {error, term()}.
withheld_heartbeats(ConnManager, Ref) ->
    call(ConnManager, {withheld_heartbeats, self(), Ref}).


-spec report_sending_response(ConnManager :: pid(), reference()) ->
    ok | {error, term()}.
report_sending_response(ConnManager, Ref) ->
    call(ConnManager, {response_sent, Ref}).


-spec call(ConnManager :: pid(), term()) ->
    ok | {error, term()}.
call(ConnManager, Msg) ->
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
            ?error("Cannot call connection manager ~p due to: ~p", [
                ConnManager, {Type, Reason}
            ]),
            {error, Reason}
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
init([]) ->
    {ok, #state{}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call({withheld_heartbeats, Pid, Ref}, _From, #state{
    pending_requests = PendingReqs,
    unreported_requests = UnreportedReqs,
    withheld_heartbeats = WithheldHeartbeats
} = State) ->
    {NewPendingReqs, NewUnreportedReqs} = case maps:take(Ref, PendingReqs) of
        {Pid, PendingReqs2} ->
            {PendingReqs2, UnreportedReqs};
        error ->
            {PendingReqs, UnreportedReqs#{Ref => Pid}}
    end,
    {reply, ok, State#state{
        pending_requests = NewPendingReqs,
        unreported_requests = NewUnreportedReqs,
        withheld_heartbeats = WithheldHeartbeats#{Ref => Pid}
    }};
handle_call({response_sent, Ref}, _From, #state{
    withheld_heartbeats = WithheldHeartbeats
} = State) ->
    {reply, ok, State#state{
        withheld_heartbeats = maps:remove(Ref, WithheldHeartbeats)
    }};
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
    {reply, {error, wrong_request}, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast({report_pending_req, Pid, Ref}, #state{
    pending_requests = PendingReqs,
    unreported_requests = UnreportedReqs
} = State) ->
    NewState = case maps:take(Ref, UnreportedReqs) of
        {Pid, NewUnreportedReqs} ->
            State#state{unreported_requests = NewUnreportedReqs};
        error ->
            State#state{pending_requests = PendingReqs#{Ref => Pid}}
    end,
    {noreply, NewState};
handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
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
%% Convert process state when code is changed
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
-spec maybe_fill_msg_id(message(), Recipient :: undefined | pid()) ->
    {undefined | message_id:id(), message()}.
maybe_fill_msg_id(#client_message{message_id = MsgId} = Msg, undefined) ->
    {MsgId, Msg};
maybe_fill_msg_id(#server_message{message_id = MsgId} = Msg, undefined) ->
    {MsgId, Msg};
maybe_fill_msg_id(Msg, Recipient) ->
    {ok, MsgId} = message_id:generate(Recipient),
    {MsgId, fill_msg_id(Msg, MsgId)}.


-spec fill_msg_id(message(), message_id:id()) -> message().
fill_msg_id(#client_message{} = Msg, MsgId) ->
    Msg#client_message{message_id = MsgId};
fill_msg_id(#server_message{} = Msg, MsgId) ->
    Msg#server_message{message_id = MsgId}.


%% @private
-spec send_sync_internal(session:id(), message()) -> ok | {error, term()}.
send_sync_internal(SessionId, Msg) ->
    send_sync_internal(SessionId, Msg, []).


%% @private
-spec send_sync_internal(session:id(), message(), [pid()]) ->
    ok | {error, term()}.
send_sync_internal(SessionId, Msg, ExcludedCons) ->
    case session_connections:get_connections(SessionId) of
        {ok, Cons} ->
            send_in_loop(Msg, shuffle_connections(Cons -- ExcludedCons));
        Error ->
            Error
    end.


%% @private
shuffle_connections(Cons) ->
    [X || {_, X} <- lists:sort([{random:uniform(), Conn} || Conn <- Cons])].


%% @private
-spec send_in_loop(message(), [pid()]) -> ok | {error, term()}.
send_in_loop(_Msg, []) ->
    {error, no_connections};
send_in_loop(Msg, [Conn]) ->
    connection:send_sync(Conn, Msg);
send_in_loop(Msg, [Conn | Cons]) ->
    case connection:send_sync(Conn, Msg) of
        ok ->
            ok;
        {error, serialization_failed} = SerializationError ->
            SerializationError;
        {error, sending_msg_via_wrong_connection} = WrongConnError ->
            WrongConnError;
        _Error ->
            % TODO is necessary?
            timer:sleep(?SEND_RETRY_DELAY),
            send_in_loop(Msg, Cons)
    end.


%% @private
-spec await_response(message()) -> {ok, message()} | {error, timeout}.
await_response(#client_message{message_id = MsgId} = Msg) ->
    Timeout = 3 * ?DEFAULT_PROCESSES_CHECK_INTERVAL,
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
