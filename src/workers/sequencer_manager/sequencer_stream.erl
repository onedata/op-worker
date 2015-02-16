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
-include("proto_internal/oneclient/client_messages.hrl").
-include("proto_internal/oneclient/server_messages.hrl").
-include("proto_internal/oneclient/stream_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% sequencer stream state:
%% stm_id       - stream ID associated with sequencer
%% seq_num      - sequence number of message that can be processed by sequencer
%%                stream
%% seq_num_ack  - sequence number of last acknowledge message
%% msgs         - mapping from sequence number to message for messages waiting to
%%                be processed by sequencer stream
%% seq_disp     - pid of sequencer dispatcher
%% msgs_ack_win - amount of messages that have to be processed by sequencer stream
%%                before emissions of acknowledgement message
%% time_ack_win - amount of seconds that have to elapsed before emission of
%%                acknowledgement message
-record(state, {
    sess_id :: session:id(),
    seq_disp :: pid(),
    seq_num = 1 :: non_neg_integer(),
    seq_num_ack = 0 :: non_neg_integer(),
    stm_id :: integer(),
    msgs = #{} :: map(),
    msgs_ack_win :: non_neg_integer(),
    time_ack_win :: non_neg_integer()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(SeqDisp :: pid(), SessId :: session:id(), StmId :: non_neg_integer()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(SeqDisp, SessId, StmId) ->
    gen_server:start_link(?MODULE, [SeqDisp, SessId, StmId], []).

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
init([SeqDisp, SessId, StmId]) ->
    process_flag(trap_exit, true),
    gen_server:cast(self(), initialize),
    {ok, #state{seq_disp = SeqDisp, sess_id = SessId, stm_id = StmId}}.

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
handle_cast(initialize, #state{seq_disp = SeqDisp, stm_id = StmId} = State) ->
    {ok, MsgsAckWin} = application:get_env(?APP_NAME, sequencer_stream_msgs_ack_win),
    {ok, TimeAckWin} = application:get_env(?APP_NAME, sequencer_stream_time_ack_win),
    erlang:send_after(timer:seconds(TimeAckWin), self(), periodic_ack),
    case gen_server:call(SeqDisp, {sequencer_stream_initialized, StmId}) of
        {ok, #state{} = SeqStmState} ->
            ?info("Sequencer stream reinitialized in state: ~p", [SeqStmState]),
            {noreply, SeqStmState#state{msgs_ack_win = MsgsAckWin, time_ack_win = TimeAckWin}};
        _ ->
            {noreply, State#state{msgs_ack_win = MsgsAckWin, time_ack_win = TimeAckWin}}
    end;

handle_cast(#client_message{message_stream = #message_stream{seq_num = SeqNum}} = Msg,
    #state{seq_num = SeqNum} = State) ->
    process_pending_msgs(process_msg(Msg, State));

handle_cast(#client_message{message_stream = #message_stream{seq_num = MsgSeqNum}} = Msg,
    #state{seq_num = SeqNum} = State) when MsgSeqNum > SeqNum ->
    {noreply, send_msg_req(Msg, store_msg(Msg, State))};

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
handle_info(periodic_ack, #state{seq_num_ack = SeqNumAck,
    seq_num = SeqNum, time_ack_win = TimeAckWin} = State) ->
    erlang:send_after(timer:seconds(TimeAckWin), self(), periodic_ack),
    case SeqNumAck + 1 =:= SeqNum of
        true -> {noreply, State};
        _ -> {noreply, send_msg_ack(State)}
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
terminate(Reason, #state{stm_id = StmId, seq_disp = SeqDisp} = State) ->
    NewState = send_msg_ack(State),
    ?warning("Sequencer stream closed in state ~p due to: ~p", [StmId, NewState, Reason]),
    gen_server:cast(SeqDisp, {sequencer_stream_terminated, StmId, Reason, NewState}).

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
-spec process_msg(Msg :: #client_message{}, State :: #state{}) ->
    {stop, normal, NewState :: #state{}} |
    {noreply, NewState :: #state{}}.
process_msg(#client_message{message_stream = #message_stream{eos = true}} = Msg, State) ->
    NewState = send_msg(Msg, State),
    {stop, normal, NewState};

process_msg(#client_message{message_stream = #message_stream{seq_num = MsgSeqNum}} = Msg,
    #state{seq_num_ack = SeqNumAck, msgs_ack_win = MsgsAckWin} = State) ->
    NewState = send_msg(Msg, State),
    case MsgSeqNum =:= SeqNumAck + MsgsAckWin of
        true -> {noreply, send_msg_ack(NewState)};
        false -> {noreply, NewState}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes pending messages by calling process_msg/2 function on messages
%% with sequence number equal to current sequence number. Returns modified
%% sequencer state.
%% @end
%%--------------------------------------------------------------------
-spec process_pending_msgs({stop, normal, State :: #state{}} |{noreply,
    State :: #state{}}) -> {stop, normal, NewState :: #state{}} |
{noreply, NewState :: #state{}}.
process_pending_msgs({stop, normal, State}) ->
    {stop, normal, State};

process_pending_msgs({noreply, #state{seq_num = SeqNum, msgs = Msgs} = State}) ->
    case maps:find(SeqNum, Msgs) of
        {ok, Msg} ->
            process_pending_msgs(process_msg(Msg, remove_msg(Msg, State)));
        _ ->
            {noreply, State}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Forwards message to the router.
%% @end
%%--------------------------------------------------------------------
-spec send_msg(Msg :: #client_message{}, State :: #state{}) ->
    NewState :: #state{}.
send_msg(Msg, #state{seq_num = SeqNum} = State) ->
    router:route_message(Msg),
    State#state{seq_num = SeqNum + 1}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends acknowledgement to sequencer dispatcher for last processed message.
%% @end
%%--------------------------------------------------------------------
-spec send_msg_ack(State :: #state{}) -> NewState :: #state{}.
send_msg_ack(#state{stm_id = StmId, sess_id = SessId, seq_num = SeqNum} = State) ->
    Msg = #server_message{message_body = #message_acknowledgement{
        stm_id = StmId,
        seq_num = SeqNum - 1
    }},
    ok = client_communicator:send(Msg, SessId),
    State#state{seq_num_ack = SeqNum - 1}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends request to sequencer dispatcher for messages with sequence number
%% ranging expected sequence number to sequence number proceeding sequence
%% number of received message minus one.
%% @end
%%--------------------------------------------------------------------
-spec send_msg_req(Msg :: #client_message{}, State :: #state{}) ->
    NewState :: #state{}.
send_msg_req(#client_message{message_stream = #message_stream{stm_id = StmId,
    seq_num = MsgSeqNum}}, #state{sess_id = SessId, seq_num = SeqNum} = State) ->
    Msg = #server_message{message_body = #message_request{
        stm_id = StmId,
        lower_seq_num = SeqNum,
        upper_seq_num = MsgSeqNum - 1
    }},
    ok = client_communicator:send(Msg, SessId),
    State.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stores message in sequencer stream state.
%% @end
%%--------------------------------------------------------------------
-spec store_msg(Msg :: #client_message{}, State :: #state{}) ->
    NewState :: #state{}.
store_msg(#client_message{message_stream = #message_stream{seq_num = SeqNum}} = Msg,
    #state{msgs = Msgs} = State) ->
    State#state{msgs = maps:put(SeqNum, Msg, Msgs)}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes message from sequencer stream state.
%% @end
%%--------------------------------------------------------------------
-spec remove_msg(Msg :: #client_message{}, State :: #state{}) ->
    NewState :: #state{}.
remove_msg(#client_message{message_stream = #message_stream{seq_num = SeqNum}},
    #state{msgs = Msgs} = State) ->
    State#state{msgs = maps:remove(SeqNum, Msgs)}.