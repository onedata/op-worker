%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% sequencer state:
%% msg_id       - message ID associated with sequencer
%% seq_num      - sequence number of message that can be processed by sequencer
%% seq_num_ack  - sequence number of last acknowledge message
%% msgs         - mapping from sequence number to message for messages waiting to
%%                be processed by sequencer
%% seq_man      - pid of sequencer manager
%% msgs_ack_win - amount of messages that have to be forwarded by sequencer before
%%                emissions of acknowledgement message
%% time_ack_win - amount of seconds that have to elapsed before emission of
%%                acknowledgement message
-record(state, {
    seq_man :: pid(),
    seq_num = 1 :: non_neg_integer(),
    seq_num_ack = 0 :: non_neg_integer(),
    msg_id :: integer(),
    msgs = #{} :: maps:new(),
    msgs_ack_win :: non_neg_integer(),
    time_ack_win :: non_neg_integer()
}).

-record(client_message, {message_id, seq_num, last_message, client_message}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(SeqMan :: pid(), MsgId :: integer()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(SeqMan, MsgId) ->
    gen_server:start_link(?MODULE, [SeqMan, MsgId], []).

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
init([SeqMan, MsgId]) ->
    gen_server:cast(self(), initialize),
    {ok, #state{seq_man = SeqMan, msg_id = MsgId}}.

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
handle_cast(initialize, #state{seq_man = SeqMan, msg_id = MsgId} = State) ->
    {ok, MsgsAckWin} = application:get_env(?APP_NAME, sequencer_msgs_ack_win),
    {ok, TimeAckWin} = application:get_env(?APP_NAME, sequencer_time_ack_win),
    erlang:send_after(timer:seconds(TimeAckWin), self(), periodic_ack),
    case gen_server:call(SeqMan, {sequencer_initialized, MsgId}) of
        {ok, #state{} = SeqState} ->
            ?info("Sequencer reinitialized in state: ~p", [SeqState]),
            {noreply, SeqState#state{msgs_ack_win = MsgsAckWin, time_ack_win = TimeAckWin}};
        _ ->
            {noreply, State#state{msgs_ack_win = MsgsAckWin, time_ack_win = TimeAckWin}}
    end;

handle_cast(#client_message{seq_num = SeqNum} = Msg, #state{seq_num = SeqNum} = State) ->
    process_pending_msgs(process_msg(Msg, State));

handle_cast(#client_message{seq_num = MsgSeqNum} = Msg, #state{seq_num = SeqNum} = State)
    when MsgSeqNum > SeqNum ->
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
terminate(Reason, #state{msg_id = MsgId, seq_man = SeqMan} = State) ->
    ?warning("Sequencer terminated in state ~p due to: ~p", [State, Reason]),
    gen_server:cast(SeqMan, {sequencer_terminated, MsgId, Reason, State}).

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

process_msg(#client_message{last_message = true} = Msg, State) ->
    NewState = send_msg_ack(send_msg(Msg, State)),
    {stop, normal, NewState};

process_msg(#client_message{seq_num = MsgSeqNum} = Msg,
    #state{seq_num_ack = SeqNumAck, msgs_ack_win = MsgsAckWin} = State) ->
    NewState = send_msg(Msg, State),
    case MsgSeqNum =:= SeqNumAck + MsgsAckWin of
        true -> {noreply, send_msg_ack(NewState)};
        false -> {noreply, NewState}
    end.

process_pending_msgs({stop, normal, State}) ->
    {stop, normal, State};

process_pending_msgs({noreply, #state{seq_num = SeqNum, msgs = Msgs} = State}) ->
    case maps:find(SeqNum, Msgs) of
        {ok, Msg} ->
            process_pending_msgs(process_msg(Msg, remove_msg(Msg, State)));
        _ ->
            {noreply, State}
    end.

send_msg(Msg, #state{seq_num = SeqNum} = State) ->
    ?info("Sending msg ~p in state ~p", [Msg, State]),
    State#state{seq_num = SeqNum + 1}.

send_msg_ack(#state{msg_id = MsgId, seq_man = SeqMan, seq_num = SeqNum} = State) ->
    ?info("Sending ack to sequence manager (pid: ~p) for message (id: ~p, seq_num: ~p)",
        [SeqMan, MsgId, SeqNum - 1]),
    State#state{seq_num_ack = SeqNum - 1}.

send_msg_req(#client_message{message_id = MsgId, seq_num = MsgSeqNum}, #state{seq_man = SeqMan, seq_num = SeqNum} = State) ->
    ?info("Sending req to sequencer manager (pid: ~p) for messages (id: ~p, range: [~p, ~p))",
        [SeqMan, MsgId, SeqNum, MsgSeqNum]),
    State.

store_msg(#client_message{seq_num = SeqNum} = Msg, #state{msgs = Msgs} = State) ->
    State#state{msgs = maps:put(SeqNum, Msg, Msgs)}.

remove_msg(#client_message{seq_num = SeqNum}, #state{msgs = Msgs} = State) ->
    State#state{msgs = maps:remove(SeqNum, Msgs)}.