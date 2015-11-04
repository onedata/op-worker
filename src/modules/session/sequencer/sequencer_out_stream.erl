%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible for storing
%%% outgoing stream messages and resending them on request.
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
-export([start_link/3]).

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
    messages = queue:new() :: messages()
}).

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
    gen_server:start_link(?MODULE, [SeqMan, StmId, SessId], []).

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
handle_cast(#message_stream_reset{}, #state{messages = Msgs,
    session_id = SessId} = State) ->
    {NewSeqNum, NewMsgs} = resend_all_messages(Msgs, SessId),
    {noreply, State#state{sequence_number = NewSeqNum, messages = NewMsgs}};

handle_cast(#message_request{lower_sequence_number = LowerSeqNum,
    upper_sequence_number = UpperSeqNum}, #state{messages = Msgs,
    stream_id = StmId, sequence_number = SeqNum, session_id = SessId} = State) ->
    case LowerSeqNum < SeqNum of
        true -> resend_messages(LowerSeqNum, UpperSeqNum, Msgs, StmId, SessId);
        false -> ok
    end,
    {noreply, State};

handle_cast(#message_acknowledgement{sequence_number = SeqNum}, #state{
    messages = Msgs} = State) ->
    {noreply, State#state{messages = remove_messages(SeqNum, Msgs)}};

handle_cast(#server_message{} = Msg, State) ->
    {noreply, store_and_forward_message(Msg, State)};

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
handle_info({'EXIT', _, shutdown}, State) ->
    {stop, shutdown, State};

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
    gen_server:cast(SeqMan, {register_out_stream, StmId, self()}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Unregisters sequencer stream in the sequencer manager.
%% @end
%%--------------------------------------------------------------------
-spec unregister_stream(State :: #state{}) -> ok.
unregister_stream(#state{sequencer_manager = SeqMan, stream_id = StmId}) ->
    gen_server:cast(SeqMan, {unregister_out_stream, StmId}).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Injects message stream details into the message, pushes it into the queue and
%% forwards to the communicator.
%% @end
%%--------------------------------------------------------------------
-spec store_and_forward_message(Msg :: #server_message{}, State :: #state{}) ->
    NewState :: #state{}.
store_and_forward_message(#server_message{message_stream = MsgStm} = Msg, #state{
    sequence_number = SeqNum, session_id = SessId, messages = Msgs} = State) ->
    NewMsg = Msg#server_message{message_stream = MsgStm#message_stream{
        sequence_number = SeqNum
    }},
    communicator:send(NewMsg, SessId),
    State#state{sequence_number = SeqNum + 1, messages = queue:in(NewMsg, Msgs)}.

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
    case queue:out(Msgs) of
        {{value, #server_message{message_stream = MsgStm} = Msg}, NewMsgs} ->
            NewMsg = Msg#server_message{
                message_stream = MsgStm#message_stream{sequence_number = SeqNum}
            },
            communicator:send(NewMsg, SessId),
            resend_all_messages(NewMsgs, SessId, SeqNum + 1, queue:in(NewMsg, MsgsAcc));
        {empty, _} ->
            {SeqNum, MsgsAcc}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resends stored messages in inclusive range [LowerSeqNum, UpperSeqNum].
%% @end
%%--------------------------------------------------------------------
-spec resend_messages(LowerSeqNum :: sequence_number(), UpperSeqNum :: sequence_number(),
    Msgs :: messages(), StmId :: stream_id(), SessId :: session:id()) -> ok.
resend_messages(LowerSeqNum, UpperSeqNum, _, _, _) when LowerSeqNum > UpperSeqNum ->
    ok;

resend_messages(LowerSeqNum, UpperSeqNum, Msgs, StmId, SessId) ->
    case queue:out(Msgs) of
        {{value, #server_message{message_stream = #message_stream{
            sequence_number = LowerSeqNum
        }} = Msg}, NewMsgs} ->
            communicator:send(Msg, SessId),
            resend_messages(LowerSeqNum + 1, UpperSeqNum, NewMsgs, StmId, SessId);
        {{value, #server_message{message_stream = #message_stream{
            sequence_number = SeqNum}
        }}, NewMsgs} when SeqNum < LowerSeqNum ->
            resend_messages(LowerSeqNum, UpperSeqNum, NewMsgs, StmId, SessId);
        {_, _} ->
            ?warning("Received request for messages unavailable in sequencer stream "
            "queue. Stream ID: ~p, range: [~p,~p].", [StmId, LowerSeqNum, UpperSeqNum])
    end.