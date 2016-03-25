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

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/stream_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-type stream_id() :: sequencer:stream_id().
-type streams() :: #{stream_id() => pid()}.

%% sequencer manager state:
%% session_id                - ID of session associated with sequencer manager
%% sequencer_manager_sup     - pid of sequencer manager supervisor
%% sequencer_in_stream_sup   - pid of incoming sequencer stream supervisor
%% sequencer_out_stream_sup  - pid of outgoing sequencer stream supervisor
%% sequencer_in_streams      - mapping from stream ID to an incoming sequencer stream pid
%% sequencer_out_streams     - mapping from stream ID to an outgoing sequencer stream pid
-record(state, {
    session_id :: session:id(),
    sequencer_manager_sup :: pid(),
    sequencer_in_stream_sup :: pid(),
    sequencer_out_stream_sup :: pid(),
    sequencer_in_streams = #{} :: streams(),
    sequencer_out_streams = #{} :: streams()
}).


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
    gen_server:start_link(?MODULE, [SeqManSup, SessId], []).

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
    {ok, SessId} = session:update(SessId, #{sequencer_manager => self()}),
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
handle_call(open_stream, _From, #state{session_id = SessId} = State) ->
    StmId = generate_stream_id(),
    ?debug("Opening stream ~p in sequencer manager for session ~p", [StmId, SessId]),
    {reply, {ok, StmId}, create_sequencer_out_stream(StmId, State)};

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
handle_cast({register_in_stream, StmId, Stm}, #state{sequencer_in_streams = Stms} = State) ->
    {noreply, State#state{sequencer_in_streams = maps:put(StmId, Stm, Stms)}};

handle_cast({register_out_stream, StmId, Stm}, #state{sequencer_out_streams = Stms} = State) ->
    {noreply, State#state{sequencer_out_streams = maps:put(StmId, Stm, Stms)}};

handle_cast({unregister_in_stream, StmId}, #state{sequencer_in_streams = Stms} = State) ->
    {noreply, State#state{sequencer_in_streams = maps:remove(StmId, Stms)}};

handle_cast({unregister_out_stream, StmId}, #state{sequencer_out_streams = Stms} = State) ->
    {noreply, State#state{sequencer_out_streams = maps:remove(StmId, Stms)}};

handle_cast({close_stream, StmId}, #state{session_id = SessId} = State) ->
    ?debug("Closing stream ~p in sequencer manager for session ~p", [StmId, SessId]),
    forward_to_sequencer_out_stream(#server_message{
        message_stream = #message_stream{stream_id = StmId},
        message_body = #end_of_message_stream{}
    }, StmId, State),
    {noreply, State};

handle_cast(#client_message{message_body = #message_stream_reset{
    stream_id = undefined} = Msg}, #state{sequencer_out_streams = Stms,
    session_id = SessId} = State) ->
    ?debug("Handling ~p in sequencer manager for session ~p", [Msg, SessId]),
    maps:map(fun(_, SeqStm) ->
        gen_server:cast(SeqStm, Msg)
    end, Stms),
    {noreply, State};

handle_cast(#client_message{message_body = #message_stream_reset{
    stream_id = StmId} = Msg}, #state{session_id = SessId} = State) ->
    ?debug("Handling ~p in sequencer manager for session ~p", [Msg, SessId]),
    forward_to_sequencer_out_stream(Msg, StmId, State),
    {noreply, State};

handle_cast(#client_message{message_body = #message_request{
    stream_id = StmId} = Msg}, #state{session_id = SessId} = State) ->
    ?debug("Handling ~p in sequencer manager for session ~p", [Msg, SessId]),
    forward_to_sequencer_out_stream(Msg, StmId, State),
    {noreply, State};

handle_cast(#client_message{message_body = #message_acknowledgement{
    stream_id = StmId} = Msg}, #state{session_id = SessId} = State) ->
    ?debug("Handling ~p in sequencer manager for session ~p", [Msg, SessId]),
    forward_to_sequencer_out_stream(Msg, StmId, State),
    {noreply, State};

handle_cast(#client_message{} = Msg, State) ->
    {ok, SeqStm, NewState} = get_or_create_sequencer_in_stream(Msg, State),
    gen_fsm:send_event(SeqStm, Msg),
    {noreply, NewState};

handle_cast(#server_message{} = Msg, #state{session_id = SessionId} = State) ->
    case get_sequencer_out_stream(Msg#server_message{proxy_session_id = SessionId}, State) of
        {ok, SeqStm} -> gen_server:cast(SeqStm, Msg);
        {error, not_found} -> ok
    end,
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
    session:update(SessId, #{sequencer_manager => undefined}).

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
    case communicator:send(Msg, SessId) of
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
%% Returns sequencer stream for outgoing messages associated with provided stream ID.
%% @end
%%--------------------------------------------------------------------
-spec get_sequencer_out_stream(Ref :: #server_message{} | stream_id(),
    State :: #state{}) -> {ok, SeqStm :: pid()} | {error, not_found}.
get_sequencer_out_stream(#server_message{message_stream = #message_stream{
    stream_id = StmId}}, State) ->
    get_sequencer_out_stream(StmId, State);

get_sequencer_out_stream(StmId, #state{session_id = SessId,
    sequencer_out_streams = Stms}) ->
    case maps:find(StmId, Stms) of
        {ok, SeqStm} ->
            {ok, SeqStm};
        error ->
            ?warning("Sequencer out stream not found for stream ~p and "
            "session: ~p", [StmId, SessId]),
            {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates sequencer stream for outgoing messages.
%% @end
%%--------------------------------------------------------------------
-spec create_sequencer_out_stream(StmId :: stream_id(), State :: #state{}) ->
    NewState :: #state{}.
create_sequencer_out_stream(StmId, #state{sequencer_out_stream_sup = SeqStmSup,
    sequencer_out_streams = Stms, session_id = SessId} = State) ->
    {ok, SeqStm} = sequencer_stream_sup:start_sequencer_stream(
        SeqStmSup, self(), StmId, SessId
    ),
    State#state{sequencer_out_streams = maps:put(StmId, SeqStm, Stms)}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Forwards message to the sequencer out stream associated with provided stream ID.
%% @end
%%--------------------------------------------------------------------
-spec forward_to_sequencer_out_stream(Msg :: term(), StmId :: stream_id(),
    State :: #state{}) -> ok.
forward_to_sequencer_out_stream(Msg, StmId, State) ->
    case get_sequencer_out_stream(StmId, State) of
        {ok, SeqStm} -> gen_server:cast(SeqStm, Msg);
        {error, not_found} -> ok
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
    State :: #state{}) -> {ok, SeqStm :: pid(), NewState :: #state{}}.
get_or_create_sequencer_in_stream(#client_message{message_stream = #message_stream{
    stream_id = StmId}}, #state{sequencer_in_streams = Stms} = State) ->
    case maps:find(StmId, Stms) of
        {ok, SeqStm} -> {ok, SeqStm, State};
        error -> create_sequencer_in_stream(StmId, State)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates sequencer stream for incoming messages.
%% @end
%%--------------------------------------------------------------------
-spec create_sequencer_in_stream(StmId :: stream_id(), State :: #state{}) ->
    {ok, SeqStm :: pid(), NewState :: #state{}}.
create_sequencer_in_stream(StmId, #state{sequencer_in_stream_sup = SeqStmSup,
    sequencer_in_streams = Stms, session_id = SessId} = State) ->
    {ok, SeqStm} = sequencer_stream_sup:start_sequencer_stream(
        SeqStmSup, self(), StmId, SessId
    ),
    {ok, SeqStm, State#state{sequencer_in_streams = maps:put(StmId, SeqStm, Stms)}}.

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

