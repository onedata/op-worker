%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for sorting messages in ascending order of sequence number and forwarding
%%% them to the router.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_stream).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("global_definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/stream_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% sequencer stream state:
%% stream_id            - stream ID associated with sequencer
%% sequence_number      - sequence number of message that can be processed by
%%                        sequencer stream
%% sequence_number_ack  - sequence number of last acknowledge message
%% messages             - mapping from sequence number to message for messages
%%                        waiting to be processed by sequencer stream
%% sequencer_manager    - pid of sequencer manager
%% messages_ack_window  - amount of messages that have to be processed by
%%                        sequencer stream before emissions of acknowledgement
%%                        message
%% time_ack_window      - amount of seconds that have to elapsed before emission
%%                        of acknowledgement message
-record(state, {
    session_id :: session:id(),
    sequencer_manager :: pid(),
    sequence_number = 0 :: non_neg_integer(),
    sequence_number_ack = -1 :: -1 | non_neg_integer(),
    stream_id :: integer(),
    messages = #{} :: map(),
    messages_ack_window :: non_neg_integer(),
    time_ack_window :: non_neg_integer()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(SeqMan :: pid(), SessId :: session:id(), StmId :: non_neg_integer()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(SeqMan, SessId, StmId) ->
    gen_server:start_link(?MODULE, [SeqMan, SessId, StmId], []).

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
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([SeqMan, SessId, StmId]) ->
    process_flag(trap_exit, true),
    gen_server:cast(self(), initialize),
    {ok, #state{sequencer_manager = SeqMan, session_id = SessId, stream_id = StmId}}.

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
handle_call(_Request, _From, State) ->
    ?log_bad_request(_Request),
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
handle_cast(initialize, #state{sequencer_manager = SeqMan, stream_id = StmId} = State) ->
    {ok, MsgsAckWin} = application:get_env(?APP_NAME,
        sequencer_stream_messages_ack_window),
    {ok, TimeAckWin} = application:get_env(?APP_NAME,
        sequencer_stream_seconds_ack_window),
    erlang:send_after(timer:seconds(TimeAckWin), self(), periodic_ack),
    case gen_server:call(SeqMan, {sequencer_stream_initialized, StmId}) of
        {ok, #state{} = SeqStmState} ->
            ?info("Sequencer stream reinitialized in state: ~p", [SeqStmState]),
            {noreply, SeqStmState#state{messages_ack_window = MsgsAckWin,
                time_ack_window = TimeAckWin}};
        _ ->
            {noreply, State#state{messages_ack_window = MsgsAckWin,
                time_ack_window = TimeAckWin}}
    end;

handle_cast(#client_message{message_stream = #message_stream{sequence_number = SeqNum}} =
    Msg, #state{sequence_number = SeqNum} = State) ->
    process_pending_messages(process_message(Msg, State));

handle_cast(#client_message{message_stream = #message_stream{sequence_number = MsgSeqNum}} =
    Msg, #state{sequence_number = SeqNum} = State) when
    is_integer(MsgSeqNum), MsgSeqNum > SeqNum ->
    {noreply, send_message_request(Msg, store_message(Msg, State))};

handle_cast(#client_message{}, State) ->
    {noreply, State};

handle_cast(_Request, State) ->
    ?log_bad_request(_Request),
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
handle_info(periodic_ack, #state{sequence_number_ack = SeqNumAck,
    sequence_number = SeqNum, time_ack_window = TimeAckWin} = State) ->
    erlang:send_after(timer:seconds(TimeAckWin), self(), periodic_ack),
    case SeqNumAck + 1 =:= SeqNum of
        true -> {noreply, State};
        _ -> {noreply, send_message_ack(State)}
    end;

handle_info(_Info, State) ->
    ?log_bad_request(_Info),
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
terminate(Reason, #state{stream_id = StmId, sequencer_manager = SeqMan} = State) ->
    NewState = send_message_ack(State),
    ?log_terminate(Reason, NewState),
    gen_server:cast(SeqMan, {sequencer_stream_terminated, StmId, Reason, NewState}).

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
%% Forward message to the router and sends periodic acknowledgement messages.
%% Returns modified sequencer state.
%% @end
%%--------------------------------------------------------------------
-spec process_message(Msg :: #client_message{}, State :: #state{}) ->
    {stop, shutdown, NewState :: #state{}} |
    {noreply, NewState :: #state{}}.
process_message(#client_message{message_stream = #message_stream{},
    message_body = #end_of_message_stream{}} = Msg, State) ->
    NewState = send_message(Msg, State),
    {stop, shutdown, NewState};

process_message(#client_message{message_stream = #message_stream{
    sequence_number = MsgSeqNum}} = Msg, #state{sequence_number_ack = SeqNumAck,
    messages_ack_window = MsgsAckWin} = State) ->
    NewState = send_message(Msg, State),
    case MsgSeqNum =:= SeqNumAck + MsgsAckWin of
        true -> {noreply, send_message_ack(NewState)};
        false -> {noreply, NewState}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes pending messages by calling process_message/2 function on messages
%% with sequence number equal to current sequence number. Returns modified
%% sequencer state.
%% @end
%%--------------------------------------------------------------------
-spec process_pending_messages({stop, shutdown, State :: #state{}} | {noreply,
    State :: #state{}}) -> {stop, shutdown, NewState :: #state{}} |
{noreply, NewState :: #state{}}.
process_pending_messages({stop, shutdown, State}) ->
    {stop, shutdown, State};

process_pending_messages({noreply, #state{sequence_number = SeqNum,
    messages = Msgs} = State}) ->
    case maps:find(SeqNum, Msgs) of
        {ok, Msg} ->
            process_pending_messages(process_message(Msg, remove_message(Msg, State)));
        _ ->
            {noreply, State}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Forwards message to the router.
%% @end
%%--------------------------------------------------------------------
-spec send_message(Msg :: #client_message{}, State :: #state{}) ->
    NewState :: #state{}.
send_message(Msg, #state{sequence_number = SeqNum} = State) ->
    router:route_message(Msg),
    State#state{sequence_number = SeqNum + 1}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends acknowledgement to the client informing about sequence number of last
%% successfully processed message.
%% @end
%%--------------------------------------------------------------------
-spec send_message_ack(State :: #state{}) -> NewState :: #state{}.
send_message_ack(#state{stream_id = StmId, session_id = SessId,
    sequence_number = SeqNum} = State) ->
    Msg = #message_acknowledgement{stream_id = StmId, sequence_number = SeqNum - 1},
    ok = communicator:send(Msg, SessId),
    State#state{sequence_number_ack = SeqNum - 1}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends request to the client for messages with sequence number
%% ranging from expected sequence number to sequence number proceeding sequence
%% number of received message.
%% @end
%%--------------------------------------------------------------------
-spec send_message_request(Msg :: #client_message{}, State :: #state{}) ->
    NewState :: #state{}.
send_message_request(#client_message{message_stream = #message_stream{
    stream_id = StmId, sequence_number = MsgSeqNum}}, #state{session_id = SessId,
    sequence_number = SeqNum} = State) ->
    Msg = #message_request{
        stream_id = StmId, lower_sequence_number = SeqNum, upper_sequence_number = MsgSeqNum - 1
    },
    ok = communicator:send(Msg, SessId),
    State.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stores message in sequencer stream state.
%% @end
%%--------------------------------------------------------------------
-spec store_message(Msg :: #client_message{}, State :: #state{}) ->
    NewState :: #state{}.
store_message(#client_message{message_stream = #message_stream{sequence_number = SeqNum}} =
    Msg, #state{messages = Msgs} = State) ->
    State#state{messages = maps:put(SeqNum, Msg, Msgs)}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes message from sequencer stream state.
%% @end
%%--------------------------------------------------------------------
-spec remove_message(Msg :: #client_message{}, State :: #state{}) ->
    NewState :: #state{}.
remove_message(#client_message{message_stream = #message_stream{sequence_number = SeqNum}},
    #state{messages = Msgs} = State) ->
    State#state{messages = maps:remove(SeqNum, Msgs)}.