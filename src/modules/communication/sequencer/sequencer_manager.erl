%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for dispatching messages to sequencer streams. When a message arrives it is
%%% forwarded to sequencer stream associated with stream ID. Sequencer manager
%%% is supervised by sequencer manager supervisor and initialized on session
%%% creation.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_manager).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/stream_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").

%% API
-export([start_link/2, handle/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-type stream_id() :: sequencer:stream_id().
-type stream_type() :: sequencer_in_streams | sequencer_out_streams.
-type streams() :: #{stream_id() => pid()}.

%% sequencer manager state:
%% session_id                - ID of session associated with sequencer manager
%% sequencer_manager_sup     - pid of sequencer manager supervisor
%% sequencer_in_stream_sup   - pid of incoming sequencer stream supervisor
%% sequencer_out_stream_sup  - pid of outgoing sequencer stream supervisor
-record(state, {
    session_id :: session:id(),
    sequencer_manager_sup :: pid(),
    sequencer_in_stream_sup :: undefined | pid(),
    sequencer_out_stream_sup :: undefined | pid()
}).

-define(TIMEOUT, timer:minutes(1)).
-define(STATE_ID, session).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the sequencer manager.
%% @end
%%--------------------------------------------------------------------
-spec start_link(SeqManSup :: pid(), SessId :: session:id()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(SeqManSup, SessId) ->
    gen_server2:start_link(?MODULE, [SeqManSup, SessId], []).

%%--------------------------------------------------------------------
%% @doc
%% Handles message (in the calling process) or sends to sequencer_manager.
%% @end
%%--------------------------------------------------------------------
-spec handle(pid(), term()) -> ok.
handle(Manager, #client_message{message_body = #message_stream_reset{
    stream_id = undefined} = Msg}) ->
    ?debug("Handling ~p by sequencer manager", [Msg]),
    {ok, Stms} = get_streams(Manager, sequencer_out_streams),
    maps:map(fun(_, SeqStm) ->
        sequencer_out_stream:send(SeqStm, Msg)
    end, Stms),
    ok;

handle(Manager, #client_message{message_body = #message_stream_reset{
    stream_id = StmId} = Msg}) ->
    ?debug("Handling ~p by sequencer manager", [Msg]),
    forward_to_sequencer_out_stream(Msg, StmId, Manager),
    ok;

handle(Manager, #client_message{message_body = #message_request{
    stream_id = StmId} = Msg}) ->
    ?debug("Handling ~p by sequencer manager", [Msg]),
    forward_to_sequencer_out_stream(Msg, StmId, Manager),
    ok;

handle(Manager, #client_message{message_body = #message_acknowledgement{
    stream_id = StmId} = Msg}) ->
    ?debug("Handling ~p by sequencer manager", [Msg]),
    forward_to_sequencer_out_stream(Msg, StmId, Manager),
    ok;

%% Handle outgoing messages
handle(Manager, #client_message{message_stream = #message_stream{sequence_number = undefined}} = Msg) ->
    {ok, SeqStm} = get_or_create_sequencer_out_stream(Msg, Manager),
    sequencer_out_stream:send(SeqStm, Msg),
    ok;

%% Handle incoming messages
handle(Manager, #client_message{} = Msg) ->
    {ok, SeqStm} = get_or_create_sequencer_in_stream(Msg, Manager),
    sequencer_in_stream:send(SeqStm, Msg),
    ok;

handle(Manager, #server_message{message_stream = #message_stream{
    stream_id = StmId}} = Msg) ->
    forward_to_sequencer_out_stream(Msg, StmId, Manager),
    ok;

% Test call
handle(Manager, {close_stream, StmId}) ->
    ?debug("Closing stream ~p", [StmId]),
    forward_to_sequencer_out_stream(#server_message{
        message_stream = #message_stream{stream_id = StmId},
        message_body = #end_of_message_stream{}
    }, StmId, Manager),
    ok;

handle(Manager, Message) ->
    gen_server2:call(Manager, Message, ?TIMEOUT).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the sequencer manager. Returns timeout equal to zero, so that
%% sequencer manager receives 'timeout' message in handle_info immediately after
%% initialization. This mechanism is introduced in order to avoid deadlock
%% when asking sequencer manager supervisor for sequencer stream supervisor pid
%% during supervision tree creation.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([SeqManSup, SessId]) ->
    ?debug("Initializing sequencer manager for session ~p", [SessId]),
    process_flag(trap_exit, true),
    init_state(),
    Self = self(),
    {ok, SessId} = session:update(SessId, fun(Session = #session{}) ->
        {ok, Session#session{sequencer_manager = Self}}
    end),
    {ok, #state{sequencer_manager_sup = SeqManSup, session_id = SessId}, 0}.

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
% Test call
handle_call(open_stream, _From, #state{session_id = SessId} = State) ->
    StmId = generate_stream_id(),
    ?debug("Opening stream ~p in sequencer manager for session ~p", [StmId, SessId]),
    {ok, _} = create_sequencer_out_stream(StmId, State),
    {reply, {ok, StmId}, State};

handle_call({create_in_stream, StmId}, _From, State) ->
    case get_stream(self(), sequencer_in_streams, StmId) of
        {ok, SeqStm} ->
            {reply, {ok, SeqStm}, State};
        error ->
            {ok, SeqStm} = create_sequencer_in_stream(StmId, State),
            {reply, {ok, SeqStm}, State}
    end;

handle_call({create_out_stream, StmId}, _From, State) ->
    case get_stream(self(), sequencer_out_streams, StmId) of
        {ok, SeqStm} ->
            {reply, {ok, SeqStm}, State};
        error ->
            {ok, SeqStm} = create_sequencer_out_stream(StmId, State),
            {reply, {ok, SeqStm}, State}
    end;

handle_call(Request, From, State) ->
    gen_server2:reply(From, ok),
    handle_cast(Request, State).

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
% Refresh state in case of stream restart
handle_cast({register_in_stream, StmId, Stm}, State) ->
    add_stream(sequencer_in_streams, StmId, Stm),
    {noreply, State};

% Refresh state in case of stream restart
handle_cast({register_out_stream, StmId, Stm}, State) ->
    add_stream(sequencer_out_streams, StmId, Stm),
    {noreply, State};

handle_cast({unregister_in_stream, StmId}, State) ->
    remove_stream(sequencer_in_streams, StmId),
    {noreply, State};

handle_cast({unregister_out_stream, StmId}, State) ->
    remove_stream(sequencer_out_streams, StmId),
    {noreply, State};

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
handle_info({'EXIT', SeqManSup, shutdown}, #state{sequencer_manager_sup = SeqManSup} = State) ->
    {stop, normal, State};

handle_info(timeout, #state{sequencer_manager_sup = SeqManSup} = State) ->
    {ok, SeqInStmSup} = sequencer_manager_sup:get_sequencer_stream_sup(
        SeqManSup, sequencer_in_stream_sup
    ),
    {ok, SeqOutStmSup} = sequencer_manager_sup:get_sequencer_stream_sup(
        SeqManSup, sequencer_out_stream_sup
    ),
    {noreply, State#state{
        sequencer_in_stream_sup = SeqInStmSup,
        sequencer_out_stream_sup = SeqOutStmSup
    }};

handle_info({send, Msg, SessId}, State) ->
    ensure_sent(Msg, SessId),
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
    State :: #state{}) -> term().
terminate(Reason, #state{session_id = SessId} = State) ->
    ?log_terminate(Reason, State),
    delete_state(),
    session:update(SessId, fun(Session = #session{}) ->
        {ok, Session#session{sequencer_manager = undefined}}
    end).

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
%% Sends a message to client associated with session. If an error occurs retries
%% after delay.
%% @end
%%--------------------------------------------------------------------
-spec ensure_sent(Msg :: term(), SessId :: session:id()) -> ok.
ensure_sent(Msg, SessId) ->
    case communicator:send_to_oneclient(SessId, Msg) of
        ok ->
            ok;
        {error, Reason} ->
            ?error("Cannot send message ~p due to: ~p", [Msg, Reason]),
            erlang:send_after(?SEND_RETRY_DELAY, self(), {send, Msg, SessId}),
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns sequencer stream for outgoing messages associated with provided
%% stream ID. If stream does not exist creates one.
%% @see create_sequencer_out_stream/2
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_sequencer_out_stream(Msg :: #client_message{},
    Manager :: pid()) -> {ok, SeqStm :: pid()}.
get_or_create_sequencer_out_stream(#client_message{message_stream = #message_stream{
    stream_id = StmId}}, Manager) ->
    case get_stream(Manager, sequencer_out_streams, StmId) of
        {ok, SeqStm} -> {ok, SeqStm};
        error -> gen_server2:call(Manager, {create_out_stream, StmId}, ?TIMEOUT)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec create_sequencer_out_stream(StmId :: stream_id(), State :: #state{}) ->
    {ok, SeqStm :: pid()}.
create_sequencer_out_stream(StmId, #state{sequencer_out_stream_sup = SeqStmSup,
    session_id = SessId}) ->
    {ok, SeqStm} = sequencer_stream_sup:start_sequencer_stream(
        SeqStmSup, self(), StmId, SessId
    ),
    add_stream(sequencer_out_streams, StmId, SeqStm),
    {ok, SeqStm}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Forwards message to the sequencer out stream associated with provided stream ID.
%% @end
%%--------------------------------------------------------------------
-spec forward_to_sequencer_out_stream(Msg :: term(), StmId :: stream_id(),
    Manager :: pid()) -> ok.
forward_to_sequencer_out_stream(Msg, StmId, Manager) ->
    case get_stream(Manager, sequencer_out_streams, StmId) of
        {ok, SeqStm} -> sequencer_out_stream:send(SeqStm, Msg);
        error -> ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns sequencer stream for incoming messages associated with provided
%% stream ID. If stream does not exist creates one.
%% @see create_sequencer_in_stream/2
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_sequencer_in_stream(Msg :: #client_message{},
    Manager :: pid()) -> {ok, SeqStm :: pid()}.
get_or_create_sequencer_in_stream(#client_message{message_stream = #message_stream{
    stream_id = StmId}}, Manager) ->
    case get_stream(Manager, sequencer_in_streams, StmId) of
        {ok, SeqStm} -> {ok, SeqStm};
        error -> gen_server2:call(Manager, {create_in_stream, StmId}, ?TIMEOUT)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates sequencer stream for incoming messages.
%% @end
%%--------------------------------------------------------------------
-spec create_sequencer_in_stream(StmId :: stream_id(), State :: #state{}) ->
    {ok, SeqStm :: pid()}.
create_sequencer_in_stream(StmId, #state{sequencer_in_stream_sup = SeqStmSup,
    session_id = SessId}) ->
    {ok, SeqStm} = sequencer_stream_sup:start_sequencer_stream(
        SeqStmSup, self(), StmId, SessId
    ),
    add_stream(sequencer_in_streams, StmId, SeqStm),
    {ok, SeqStm}.

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Returns increasing stream IDs based on the monotonic time.
%% Should be used only for temporary subscriptions.
%% @end
%%--------------------------------------------------------------------
-spec generate_stream_id() -> StmId :: stream_id().
generate_stream_id() ->
    erlang:unique_integer([monotonic, positive]) .

%%%===================================================================
%%% Internal functions for streams storing in state
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves stream in state.
%% @end
%%--------------------------------------------------------------------
-spec add_stream(stream_type(), stream_id(), Stream :: pid()) -> ok.
add_stream(StreamType, StmId, SeqStm) ->
    ets_state:add_to_collection(?STATE_ID, StreamType, StmId, SeqStm).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes stream from state.
%% @end
%%--------------------------------------------------------------------
-spec remove_stream(stream_type(), stream_id()) -> ok.
remove_stream(StreamType, StmId) ->
    ets_state:remove_from_collection(?STATE_ID, StreamType, StmId).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets stream from state.
%% @end
%%--------------------------------------------------------------------
-spec get_stream(Manager :: pid(), stream_type(), stream_id()) ->
    {ok, Streams :: pid()} | error.
get_stream(Manager, StreamType, StmId) ->
    ets_state:get_from_collection(?STATE_ID, Manager, StreamType, StmId).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets all streams of given type from state.
%% @end
%%--------------------------------------------------------------------
-spec get_streams(Manager :: pid(), stream_type()) -> {ok, streams()} | error.
get_streams(Manager, StreamType) ->
    ets_state:get_collection(?STATE_ID, Manager, StreamType).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes maps in state.
%% @end
%%--------------------------------------------------------------------
-spec init_state() -> ok.
init_state() ->
    ets_state:init_collection(?STATE_ID, sequencer_in_streams),
    ets_state:init_collection(?STATE_ID, sequencer_out_streams).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes all data from state.
%% @end
%%--------------------------------------------------------------------
-spec delete_state() -> ok.
delete_state() ->
    ets_state:delete_collection(?STATE_ID, sequencer_in_streams),
    ets_state:delete_collection(?STATE_ID, sequencer_out_streams).