%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible for storing
%%% outgoing stream messages and resending them on request.  It is supervised by
%%% sequencer stream supervisor and coordinated by sequencer manager.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_out_stream).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/3, send/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-type stream_id() :: sequencer:stream_id().
-type sequence_number() :: sequencer:sequence_number().
-type messages() :: queue:queue().

%% sequencer stream state:
%% session_id           - ID of a session associated with this sequencer stream
%% sequencer_manager    - pid of a sequencer manager that controls this
%%                        sequencer stream
%% stream_id            - ID of a communication stream associated with this
%%                        sequencer stream
%% sequence_number      - sequence number of next message to be forwarded
%% messages             - queue of forwarded messages
-record(state, {
    session_id :: session:id(),
    sequencer_manager :: pid(),
    stream_id :: stream_id(),
    sequence_number = 0 :: sequencer:sequence_number(),
    inbox = queue:new() :: messages(),
    outbox = queue:new() :: messages()
}).

-define(PROCESS_REQUEST_RETRY_FIRST_BACKOFF, timer:seconds(5)).
-define(PROCESS_REQUEST_RETRY_MAX_BACKOFF, timer:hours(1)).
-define(THROTTLE_LOG(LogId, Log), utils:throttle(LogId, 300, fun() -> Log end)). % log no more often than every 5 minutes
-define(LOG_FAILED_ATTEMPTS_THRESHOLD, 10).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec start_link(SeqMan :: pid(), StmId :: stream_id(), SessId :: session:id()) ->
    {ok, SeqStm :: pid()} | ignore | {error, Reason :: term()}.
start_link(SeqMan, StmId, SessId) ->
    gen_server2:start_link(?MODULE, [SeqMan, StmId, SessId], []).

%%--------------------------------------------------------------------
%% @doc
%% Sends message to sequencer.
%% @end
%%--------------------------------------------------------------------
-spec send(pid(), term()) -> ok.
send(Manager, Message) ->
    gen_server2:cast(Manager, Message).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([SeqMan, StmId, SessId]) ->
    ?debug("Initializing sequencer out stream for session ~tp", [SessId]),
    process_flag(trap_exit, true),
    register_stream(SeqMan, StmId),
    {ok, #state{
        sequencer_manager = SeqMan,
        stream_id = StmId,
        session_id = SessId
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_cast(#message_stream_reset{} = Request, #state{} = State) ->
    {noreply, handle_request(Request, State)};

handle_cast(#message_request{} = Request, #state{} = State) ->
    {noreply, handle_request(Request, State)};

handle_cast(#message_acknowledgement{} = Request, #state{} = State) ->
    {noreply, handle_request(Request, State)};

handle_cast(#server_message{} = Request, #state{session_id = SessionId} = State) ->
    {noreply, handle_request(Request#server_message{effective_session_id = SessionId}, State)};

handle_cast(#client_message{} = Request, State) ->
    {noreply, handle_request(Request, State)};

handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}.
handle_info({process_pending_requests, Backoff}, State) ->
    {noreply, process_pending_requests(State, Backoff)};

handle_info({'EXIT', _, shutdown}, State) ->
    {stop, normal, State};

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
    State :: #state{}) -> term().
terminate(Reason, State) ->
    ?log_terminate(Reason, State),
    unregister_stream(State).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) -> {ok, NewState :: #state{}} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Registers sequencer stream in the sequecner manager.
%% @end
%%--------------------------------------------------------------------
-spec register_stream(SeqMan :: pid(), StmId :: stream_id()) -> ok.
register_stream(SeqMan, StmId) ->
    gen_server2:cast(SeqMan, {register_out_stream, StmId, self()}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Unregisters sequencer stream in the sequencer manager.
%% @end
%%--------------------------------------------------------------------
-spec unregister_stream(State :: #state{}) -> ok.
unregister_stream(#state{sequencer_manager = SeqMan, stream_id = StmId}) ->
    gen_server2:cast(SeqMan, {unregister_out_stream, StmId}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes request if there are no pending requests, otherwise appends it to
%% the queue of pending requests. If request fails with an exception it is saved
%% as a pending request and will be handled again after timeout.
%% @end
%%--------------------------------------------------------------------
-spec handle_request(Request :: term(), State :: #state{}) -> NewState :: #state{}.
handle_request(Request, #state{inbox = Inbox} = State) ->
    case queue:is_empty(Inbox) of
        true ->
            try
                process_request(Request, State)
            catch
                _:Reason ->
                    schedule_retry_of_pending_requests_processing(?PROCESS_REQUEST_RETRY_FIRST_BACKOFF),
                    maybe_log_failure(Request, Reason, 1, ?PROCESS_REQUEST_RETRY_FIRST_BACKOFF),
                    State#state{inbox = queue:in({Request, 1}, Inbox)}
            end;
        false ->
            State#state{inbox = queue:in({Request, 0}, Inbox)}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes all pending requests as long as there are no errors.
%% If an error occurs, processing is stopped and will be restarted after timeout.
%% @end
%%--------------------------------------------------------------------
-spec process_pending_requests(State :: #state{}, time:millis()) -> NewState :: #state{}.
process_pending_requests(#state{inbox = Inbox} = State, Backoff) ->
    case queue:peek(Inbox) of
        {value, {Request, Attempts}} ->
            try
                NewState = process_request(Request, State#state{inbox = queue:drop(Inbox)}),
                process_pending_requests(NewState, ?PROCESS_REQUEST_RETRY_FIRST_BACKOFF)
            catch
                _:Reason ->
                    schedule_retry_of_pending_requests_processing(Backoff),
                    maybe_log_failure(Request, Reason, Attempts + 1, Backoff),
                    State#state{inbox = queue:in_r({Request, Attempts + 1}, queue:drop(Inbox))}
            end;
        empty -> State
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes the request.
%% @end
%%--------------------------------------------------------------------
-spec process_request(Request :: term(), State :: #state{}) -> NewState :: #state{}.
process_request(#message_stream_reset{}, #state{outbox = Msgs,
    session_id = SessId} = State) ->
    {NewSeqNum, NewMsgs} = resend_all_messages(Msgs, SessId),
    State#state{sequence_number = NewSeqNum, outbox = NewMsgs};

process_request(#message_request{lower_sequence_number = LowerSeqNum,
    upper_sequence_number = UpperSeqNum}, #state{outbox = Msgs,
    stream_id = StmId, sequence_number = SeqNum, session_id = SessId} = State) ->
    case LowerSeqNum < SeqNum of
        true ->
            ok = resend_messages(LowerSeqNum, UpperSeqNum, Msgs, StmId, SessId),
            State;
        false ->
            State
    end;

process_request(#message_acknowledgement{sequence_number = SeqNum}, #state{
    outbox = Msgs} = State) ->
    State#state{outbox = remove_messages(SeqNum, Msgs)};

process_request(#server_message{message_stream = MsgStm} = Msg, #state{
    sequence_number = SeqNum, session_id = SessId, outbox = Msgs} = State) ->
    NewMsg = Msg#server_message{message_stream = MsgStm#message_stream{
        sequence_number = SeqNum
    }},
    ok = communicator:send_to_oneclient(SessId, NewMsg),
    State#state{sequence_number = SeqNum + 1, outbox = queue:in(NewMsg, Msgs)};

process_request(#client_message{message_stream = MsgStm} = Msg, #state{
    sequence_number = SeqNum, session_id = SessId, outbox = Msgs} = State) ->
    NewMsg = Msg#client_message{message_stream = MsgStm#message_stream{
        sequence_number = SeqNum
    }},
    ok = communicator:send_to_provider(SessId, NewMsg),
    State#state{sequence_number = SeqNum + 1, outbox = queue:in(NewMsg, Msgs)}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes acknowledged messages.
%% @end
%%--------------------------------------------------------------------
-spec remove_messages(SeqNum :: sequence_number(), Msgs :: messages()) ->
    NewMsgs :: messages().
remove_messages(SeqNum, Msgs) ->
    case queue:peek(Msgs) of
        {value, #server_message{message_body = #end_of_message_stream{}}} ->
            exit(self(), shutdown);
        {value, #server_message{message_stream = #message_stream{
            sequence_number = MsgSeqNum
        }}} when MsgSeqNum =< SeqNum ->
            remove_messages(SeqNum, queue:drop(Msgs));
        _ ->
            Msgs
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @equiv resend_all_messages(Msgs, SessId, 0, queue:new())
%% @end
%%--------------------------------------------------------------------
-spec resend_all_messages(Msgs :: messages(), SessId :: session:id()) ->
    {NewSeqNum :: sequence_number(), NewMsgs :: messages()}.
resend_all_messages(Msgs, SessId) ->
    resend_all_messages(Msgs, SessId, 0, queue:new()).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resends all stored messages. Sequence number of each message is recomputed
%% starting from zero. Returns new sequence number and messages with recomputed
%% sequence number.
%% @end
%%--------------------------------------------------------------------
-spec resend_all_messages(Msgs :: messages(), SessId :: session:id(),
    SeqNum :: sequence_number(), MsgsAcc :: messages()) ->
    {NewSeqNum :: sequence_number(), NewMsgs :: messages()}.
resend_all_messages(Msgs, SessId, SeqNum, MsgsAcc) ->
    case queue:peek(Msgs) of
        {value, #server_message{message_stream = MsgStm} = Msg} ->
            NewMsg = Msg#server_message{
                message_stream = MsgStm#message_stream{sequence_number = SeqNum}
            },
            ok = communicator:send_to_oneclient(SessId, NewMsg),
            resend_all_messages(queue:drop(Msgs), SessId, SeqNum + 1, queue:in(NewMsg, MsgsAcc));
        {value, #client_message{message_stream = MsgStm} = Msg} ->
            NewMsg = Msg#client_message{
                message_stream = MsgStm#message_stream{sequence_number = SeqNum}
            },
            ok = communicator:send_to_provider(SessId, NewMsg),
            resend_all_messages(queue:drop(Msgs), SessId, SeqNum + 1, queue:in(NewMsg, MsgsAcc));
        empty ->
            {SeqNum, MsgsAcc}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resends stored messages in inclusive range [LowerSeqNum, UpperSeqNum].
%% @end
%%--------------------------------------------------------------------
-spec resend_messages(LowerSeqNum :: sequence_number(), UpperSeqNum ::
    sequence_number(), Msgs :: messages(), StmId :: stream_id(),
    SessId :: session:id()) -> ok | {error, Reason :: term()}.
resend_messages(LowerSeqNum, UpperSeqNum, _, _, _) when LowerSeqNum > UpperSeqNum ->
    ok;

resend_messages(LowerSeqNum, UpperSeqNum, Msgs, StmId, SessId) ->
    case queue:peek(Msgs) of
        {value, #server_message{message_stream = #message_stream{
            sequence_number = LowerSeqNum
        }} = Msg} ->
            ok = communicator:send_to_oneclient(SessId, Msg),
            resend_messages(LowerSeqNum + 1, UpperSeqNum, queue:drop(Msgs), StmId, SessId);
        {value, #server_message{message_stream = #message_stream{
            sequence_number = SeqNum
        }}} when SeqNum < LowerSeqNum ->
            resend_messages(LowerSeqNum, UpperSeqNum, queue:drop(Msgs), StmId, SessId);
        _ ->
            ?warning("Received request for messages unavailable in sequencer stream "
            "queue. Stream ID: ~tp, range: [~tp,~tp].", [StmId, LowerSeqNum, UpperSeqNum])
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Logs request failure if processing of the request failed more than
%% 'LOG_FAILED_ATTEMPTS_THRESHOLD' times in a row.
%% @end
%%--------------------------------------------------------------------
-spec maybe_log_failure(Request :: term(), Reason :: term(),
    Attempt :: non_neg_integer(), time:millis()) -> ok.
maybe_log_failure(_, {badmatch, {error, no_connections}}, _, _) ->
    ok;
maybe_log_failure(_, {badmatch, ?ERROR_NO_CONNECTION_TO_PEER_ONEPROVIDER}, _, _) ->
    ok;
maybe_log_failure(_, {badmatch, {error, not_found}}, _, _) ->
    ok;
maybe_log_failure(Request, Reason, AttemptNumber, RetryInMillis) ->
    case AttemptNumber > ?LOG_FAILED_ATTEMPTS_THRESHOLD of
        true ->
            ?THROTTLE_LOG(Request, ?error(?autoformat_with_msg("Cannot process request in ~tp",
                [?MODULE], [Request, Reason, AttemptNumber, RetryInMillis]
            )));
        false ->
            ok
    end.

-spec schedule_retry_of_pending_requests_processing(time:millis()) -> ok.
schedule_retry_of_pending_requests_processing(CurrentBackoff) ->
    NextBackoff = round(min(1.2 * CurrentBackoff, ?PROCESS_REQUEST_RETRY_MAX_BACKOFF)),
    erlang:send_after(CurrentBackoff, self(), {process_pending_requests, NextBackoff}),
    ok.