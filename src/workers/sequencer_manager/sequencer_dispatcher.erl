%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for dispatching messages associated with given session to sequencer streams.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_dispatcher).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("workers/datastore/datastore_models.hrl").
-include("proto_internal/oneclient/client_messages.hrl").
-include("proto_internal/oneclient/server_messages.hrl").
-include("proto_internal/oneclient/stream_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% sequencer dispatcher state:
%% sess_id     - ID of session associated with event dispatcher
%% seq_stm_sup - pid of sequencer stream supervisor
%% seq_stms    - mapping from message ID to sequencer stream
-record(state, {
    sess_id :: session:id(),
    seq_stm_sup :: pid(),
    seq_stms = #{} :: map()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(SeqDispSup :: pid(), SeqStmSup :: pid(), SessId :: session:id()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(SeqDispSup, SeqStmSup, SessId) ->
    gen_server:start_link(?MODULE, [SeqDispSup, SeqStmSup, SessId], []).

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
init([SeqDispSup, SeqStmSup, SessId]) ->
    process_flag(trap_exit, true),
    case sequencer_dispatcher_data:create(#document{key = SessId,
        value = #sequencer_dispatcher_data{node = node(), pid = self(), sup = SeqDispSup}
    }) of
        {ok, SessId} ->
            ok = reset_message_stream(SessId),
            {ok, #state{seq_stm_sup = SeqStmSup, sess_id = SessId}};
        {error, Reason} ->
            {stop, Reason}
    end.

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
handle_call({sequencer_stream_initialized, StmId}, {Pid, _}, #state{seq_stms = SeqStms} = State) ->
    case maps:find(StmId, SeqStms) of
        {ok, {state, SeqStmState, PendingMsgs}} ->
            lists:foreach(fun(Msg) ->
                gen_server:cast(Pid, Msg)
            end, lists:reverse(PendingMsgs)),
            {reply, {ok, SeqStmState}, State#state{seq_stms = maps:put(StmId, {pid, Pid}, SeqStms)}};
        _ ->
            {reply, undefined, State}
    end;

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
handle_cast({sequencer_stream_terminated, StmId, normal, _}, #state{seq_stms = SeqStms} = State) ->
    {noreply, State#state{seq_stms = maps:remove(StmId, SeqStms)}};

handle_cast({sequencer_stream_terminated, StmId, _, SeqStmState}, #state{seq_stms = SeqStms} = State) ->
    {noreply, State#state{seq_stms = maps:put(StmId, {state, SeqStmState, []}, SeqStms)}};

handle_cast(#client_message{message_stream = #message_stream{stm_id = StmId}} = Msg,
    #state{seq_stm_sup = SeqStmSup, seq_stms = SeqStms, sess_id = SessId} = State) ->
    case maps:find(StmId, SeqStms) of
        {ok, {pid, Pid}} ->
            gen_server:cast(Pid, Msg),
            {noreply, State};
        {ok, {state, SeqStmState, PendingMsgs}} ->
            {noreply, State#state{seq_stms = maps:put(StmId, {state, SeqStmState,
                [Msg | PendingMsgs]}, SeqStms)}};
        _ ->
            SeqDisp = self(),
            {ok, _SeqStm} = sequencer_stream_sup:start_sequencer_stream(SeqStmSup,
                SeqDisp, SessId, StmId),
            {noreply, State#state{seq_stms = maps:put(StmId, {state, undefined,
                [Msg]}, SeqStms)}}
    end;

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
terminate(Reason, #state{sess_id = SessId} = State) ->
    ?warning("Sequencer dispatcher terminated in state ~p due to: ~p", [State, Reason]),
    sequencer_dispatcher_data:delete(SessId).

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
%% Resets each client message stream, so that sequence number starts from '1'.
%% @end
%%--------------------------------------------------------------------
-spec reset_message_stream(SessId :: session:id()) ->
    ok | {error, Reason :: term()}.
reset_message_stream(SessId) ->
    Msg = #server_message{message_body = #message_stream_reset{}},
    client_communicator:send(Msg, SessId).