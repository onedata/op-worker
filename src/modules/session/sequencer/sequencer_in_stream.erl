%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_fsm behaviour and is responsible for sorting
%%% incoming stream messages in ascending order of sequence number and
%%% forwarding them to the router. It is supervised by sequencer stream
%%% supervisor and coordinated by sequencer manager.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_in_stream).
-author("Krzysztof Trzepla").

-behaviour(gen_fsm).

-include("global_definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/3]).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3,
    code_change/4]).

%% gen_fsm states
-export([receiving/2, processing/2, requesting/2]).

-type stream_id() :: sequencer:stream_id().
-type sequence_number() :: sequencer:sequence_number().

%% sequencer in stream state:
%% session_id           - ID of a session associated with this sequencer stream
%% sequencer_manager    - pid of a sequencer manager that controls this
%%                        sequencer stream
%% stream_id            - ID of a communication stream associated with this
%%                        sequencer stream
%% sequence_number      - sequence number of message that can be forwarded
%% sequence_number_ack  - sequence number of last acknowledged message
%% messages             - mapping from sequence number to message for messages
%%                        waiting to be forwarded
-record(state, {
    session_id :: session:id(),
    proxy_session_id :: session:id(),
    sequencer_manager :: pid(),
    stream_id :: stream_id(),
    sequence_number = 0 :: sequence_number(),
    sequence_number_ack = -1 :: -1 | sequence_number(),
    messages = #{} :: #{sequence_number() => #client_message{}},
    is_proxy :: boolean()
}).

-define(MSG_ACK_THRESHOLD, application:get_env(?APP_NAME,
    sequencer_stream_msg_ack_threshold, 100)).
-define(RECEIVING_TIMEOUT, timer:seconds(application:get_env(?APP_NAME,
    sequencer_stream_msg_req_long_timeout_seconds, 10))).
-define(REQUESTING_TIMEOUT, timer:seconds(application:get_env(?APP_NAME,
    sequencer_stream_msg_req_short_timeout_seconds, 1))).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the sequencer stream for incoming messages.
%% @end
%%--------------------------------------------------------------------
-spec start_link(SeqMan :: pid(), StmId :: stream_id(), SessId :: session:id()) ->
    {ok, SeqStm :: pid()} | ignore | {error, Reason :: term()}.
start_link(SeqMan, StmId, SessId) ->
    gen_fsm:start_link(?MODULE, [SeqMan, StmId, SessId], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the sequencer stream for incoming messages.
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, StateName :: atom(), StateData :: #state{}} |
    {ok, StateName :: atom(), StateData :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([SeqMan, StmId, SessId]) ->
    ?debug("Initializing sequencer in stream for session ~p", [SessId]),
    process_flag(trap_exit, true),
    register_stream(SeqMan, StmId),
    {ok, #document{value = #session{type = SessionType, proxy_via = ProxyVia}}} = session:get(SessId),
    IsProxy = SessionType =:= provider orelse SessionType =:= provider_outgoing orelse ProxyVia =/= undefined,
    send_message_stream_reset(StmId, SessId, IsProxy),
    {ok, receiving, #state{
        sequencer_manager = SeqMan,
        session_id = SessId,
        stream_id = StmId,
        is_proxy = IsProxy
    }, ?RECEIVING_TIMEOUT}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%% @end
%%--------------------------------------------------------------------
-spec(handle_event(Event :: term(), StateName :: atom(),
    StateData :: #state{}) ->
    {next_state, NextStateName :: atom(), NewStateData :: #state{}} |
    {next_state, NextStateName :: atom(), NewStateData :: #state{},
        timeout() | hibernate} |
    {stop, Reason :: term(), NewStateData :: #state{}}).
handle_event(Event, StateName, State) ->
    ?log_bad_request({Event, StateName}),
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%% @end
%%--------------------------------------------------------------------
-spec(handle_sync_event(Event :: term(), From :: {pid(), Tag :: term()},
    StateName :: atom(), StateData :: term()) ->
    {reply, Reply :: term(), NextStateName :: atom(), NewStateData :: term()} |
    {reply, Reply :: term(), NextStateName :: atom(), NewStateData :: term(),
        timeout() | hibernate} |
    {next_state, NextStateName :: atom(), NewStateData :: term()} |
    {next_state, NextStateName :: atom(), NewStateData :: term(),
        timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewStateData :: term()} |
    {stop, Reason :: term(), NewStateData :: term()}).
handle_sync_event(Event, From, StateName, State) ->
    ?log_bad_request({Event, From, StateName}),
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: term(), StateName :: atom(),
    StateData :: term()) ->
    {next_state, NextStateName :: atom(), NewStateData :: term()} |
    {next_state, NextStateName :: atom(), NewStateData :: term(),
        timeout() | hibernate} |
    {stop, Reason :: normal | term(), NewStateData :: term()}).
handle_info({'EXIT', _, shutdown}, _, State) ->
    {stop, normal, State};

handle_info(Info, StateName, State) ->
    ?log_bad_request({Info, StateName}),
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: normal | shutdown | {shutdown, term()}
| term(), StateName :: atom(), StateData :: term()) -> term()).
terminate(Reason, StateName, #state{stream_id = StmId, sequence_number = SeqNum,
    session_id = SessId, sequencer_manager = SeqMan, is_proxy = IsProxy} = State) ->
    ?log_terminate(Reason, {StateName, State}),
    Msg = #message_acknowledgement{stream_id = StmId, sequence_number = SeqNum - 1},
    CommunicatorModule = communicator_module(IsProxy),
    case CommunicatorModule:send(Msg, SessId) of
        ok -> ok;
        {error, Reason} -> SeqMan ! {send, Msg, SessId}
    end,
    unregister_stream(State).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, StateName :: atom(),
    StateData :: #state{}, Extra :: term()) ->
    {ok, NextStateName :: atom(), NewStateData :: #state{}}).
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% gen_fsm states
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% In this state sequencer stream is waiting for a client message with a specific
%% sequence number. If it arrives, it is forwarded and sequencer stream goes into
%% a 'processing' state. If other client message arrives a request is sent for
%% messages having sequence number ranging from the awaited sequence number up
%% to the sequence number proceeding the one of the received message. Sequencer
%% stream goes into a 'requesting' state. If timeout occurs a request for
%% message with a awaited sequence number is sent and sequencer stream remains
%% in 'receiving' state.
%% @end
%%--------------------------------------------------------------------
-spec receiving(Event :: timeout | #client_message{}, State :: #state{}) ->
    {next_state, NextStateName :: atom(), NextState :: #state{}, timeout()}.
receiving(timeout, #state{sequence_number = SeqNum} = State) ->
    send_message_request(SeqNum, State),
    {next_state, receiving, State, ?RECEIVING_TIMEOUT};

receiving(#client_message{message_stream = #message_stream{
    sequence_number = SeqNum}} = Msg, #state{sequence_number = SeqNum} = State) ->
    {next_state, processing, forward_message(Msg, State), 0};

receiving(#client_message{} = Msg, State) ->
    case store_message(Msg, State) of
        {false, NewState} ->
            {next_state, receiving, NewState, ?RECEIVING_TIMEOUT};
        {SeqNum, NewState} ->
            send_message_request(SeqNum - 1, State),
            {next_state, requesting, NewState, ?REQUESTING_TIMEOUT}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% In this state sequencer stream tries to forward messages that arrived in one
%% of the 'receiving' or 'requesting' states while waiting for a message with a
%% specific sequence number. If none of messages can be forwarded sequencer
%% stream goes into 'requesting' state. It also sends message acknowledgement if
%% threshold is exceeded.
%% @end
%%--------------------------------------------------------------------
-spec processing(Event :: timeout | #client_message{}, State :: #state{}) ->
    {next_state, NextStateName :: atom(), NextState :: #state{}, timeout()}.
processing(timeout, #state{sequence_number = SeqNum, messages = Msgs} = State) ->
    case maps:find(SeqNum, Msgs) of
        {ok, Msg} ->
            {next_state, processing,
                remove_message(SeqNum, forward_message(Msg, State)), 0};
        error ->
            {next_state, requesting, maybe_send_message_acknowledgement(State), 0}
    end;

processing(#client_message{message_stream = #message_stream{
    sequence_number = SeqNum}} = Msg, #state{sequence_number = SeqNum} = State) ->
    {next_state, processing, forward_message(Msg, State), 0};

processing(#client_message{} = Msg, State) ->
    {_, NewState} = store_message(Msg, State),
    {next_state, processing, NewState, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This state is similar to the 'receiving' state, however timeout is different.
%% If it occurres sequencer streams either goes into 'receiving' state if 
%% there are no pending messages or into 'requesting' state sending message
%% request for message with awaited sequence number.
%% @end
%%--------------------------------------------------------------------
-spec requesting(Event :: timeout | #client_message{}, State :: #state{}) ->
    {next_state, NextStateName :: atom(), NextState :: #state{}, timeout()}.
requesting(timeout, #state{messages = #{}} = State) ->
    {next_state, receiving, State, ?RECEIVING_TIMEOUT};

requesting(timeout, #state{sequence_number = SeqNum} = State) ->
    send_message_request(SeqNum, State),
    {next_state, requesting, State, ?REQUESTING_TIMEOUT};

requesting(#client_message{message_stream = #message_stream{
    sequence_number = SeqNum}} = Msg, #state{sequence_number = SeqNum} = State) ->
    {next_state, processing, forward_message(Msg, State), 0};

requesting(#client_message{} = Msg, State) ->
    {_, NewState} = store_message(Msg, State),
    {next_state, requesting, NewState, ?REQUESTING_TIMEOUT}.

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
    gen_server:cast(SeqMan, {register_in_stream, StmId, self()}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Unregisters sequencer stream in the sequencer manager.
%% @end
%%--------------------------------------------------------------------
-spec unregister_stream(State :: #state{}) -> ok.
unregister_stream(#state{sequencer_manager = SeqMan, stream_id = StmId}) ->
    gen_server:cast(SeqMan, {unregister_in_stream, StmId}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends a message stream reset request to the remote client.
%% @end
%%--------------------------------------------------------------------
-spec send_message_stream_reset(StmId :: stream_id(),
    SessId :: session:id(), IsProxy :: boolean()) -> ok.
send_message_stream_reset(StmId, SessId, IsProxy) ->
    CommunicatorModule = communicator_module(IsProxy),
    CommunicatorModule:send(#message_stream_reset{stream_id = StmId}, SessId, infinity).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends a messages acknowledgement to the remote client if necessary, i.e.
%% when previous acknowledgement concerned different sequence number.
%% @end
%%--------------------------------------------------------------------
-spec send_message_acknowledgement(State :: #state{}) -> NewState :: #state{}.
send_message_acknowledgement(#state{sequence_number = SeqNum,
    sequence_number_ack = SeqNumAck} = State) when SeqNum == SeqNumAck + 1 ->
    State;
send_message_acknowledgement(#state{sequence_number = SeqNum} = State) when SeqNum < 1 ->
    State;

send_message_acknowledgement(#state{stream_id = StmId, sequence_number = SeqNum,
    session_id = SessId, is_proxy = IsProxy} = State) ->
    CommunicatorModule = communicator_module(IsProxy),
    CommunicatorModule:send(#message_acknowledgement{
        stream_id = StmId, sequence_number = SeqNum - 1
    }, SessId, infinity),
    State#state{sequence_number_ack = SeqNum - 1}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends a messages acknowledgement to the remote client if required, i.e.
%% when difference between the awaited sequence number and the last acknowledged
%% sequence number exceeds the threshold.
%% @end
%%--------------------------------------------------------------------
-spec maybe_send_message_acknowledgement(State :: #state{}) -> NewState :: #state{}.
maybe_send_message_acknowledgement(#state{sequence_number = SeqNum,
    sequence_number_ack = SeqNumAck} = State) ->
    case SeqNum > SeqNumAck + ?MSG_ACK_THRESHOLD of
        true -> send_message_acknowledgement(State);
        false -> State
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends a message request for messages having sequence number ranging from the
%% awaited sequencer number up to provided sequence number.
%% @end
%%--------------------------------------------------------------------
-spec send_message_request(UpperSeqNum :: sequence_number(),
    State :: #state{}) -> ok.
send_message_request(UpperSeqNum, #state{stream_id = StmId,
    sequence_number = LowerSeqNum, session_id = SessId, is_proxy = IsProxy}) ->
    CommunicatorModule = communicator_module(IsProxy),
    CommunicatorModule:send(#message_request{
        stream_id = StmId,
        lower_sequence_number = LowerSeqNum,
        upper_sequence_number = UpperSeqNum
    }, SessId, infinity).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stores message if its sequence number is greater or equal to the awaited
%% sequence number. Returns tuple which first element is set to the sequence
%% number of the stored message or 'false' if massage was not stored.
%% @end
%%--------------------------------------------------------------------
-spec store_message(Msg :: #client_message{}, State :: #state{}) ->
    {SeqNum :: sequence_number() | false, NewState :: #state{}}.
store_message(#client_message{message_stream = #message_stream{
    sequence_number = MsgSeqNum}} = Msg, #state{sequence_number = SeqNum,
    messages = Msgs} = State) when is_integer(MsgSeqNum), MsgSeqNum >= SeqNum ->
    {MsgSeqNum, State#state{messages = maps:put(MsgSeqNum, Msg, Msgs)}};

store_message(#client_message{}, State) ->
    {false, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes message with the provided sequence number.
%% @end
%%--------------------------------------------------------------------
-spec remove_message(SeqNum :: sequence_number(), State :: #state{}) ->
    NewState :: #state{}.
remove_message(SeqNum, #state{messages = Msgs} = State) ->
    State#state{messages = maps:remove(SeqNum, Msgs)}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Forwards a message to the router. If it is the last message in the stream
%% closes sequencer stream.
%% @end
%%--------------------------------------------------------------------
-spec forward_message(Msg :: #client_message{}, State :: #state{}) ->
    NewState :: #state{}.
forward_message(#client_message{message_body = #end_of_message_stream{}},
    #state{sequence_number = SeqNum} = State) ->
    exit(self(), shutdown),
    State#state{sequence_number = SeqNum + 1};

forward_message(Msg, #state{sequence_number = SeqNum} = State) ->
    router:route_message(Msg),
    State#state{sequence_number = SeqNum + 1}.


communicator_module(false) ->
    communicator;
communicator_module(true) ->
    provider_communicator.