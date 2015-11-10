%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for dispatching messages to sequencer streams.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_manager).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("cluster/worker/modules/datastore/datastore.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/stream_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2, route_message/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% sequencer manager state:
%% session_id           - ID of session associated with event manager
%% sequencer_stream_sup - pid of sequencer stream supervisor
%% sequencer_streams    - mapping from message ID to sequencer stream
-record(state, {
    session_id :: session:id(),
    sequencer_stream_sup :: pid(),
    sequencer_streams = #{} :: map()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(SeqManSup :: pid(), SessId :: session:id()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(SeqManSup, SessId) ->
    gen_server:start_link(?MODULE, [SeqManSup, SessId], []).

%%--------------------------------------------------------------------
%% @doc
%% Routes message through sequencer manager.
%% @end
%%--------------------------------------------------------------------
-spec route_message(Msg :: #client_message{}, SessId :: session:id()) ->
    ok | {error, Reason :: term()}.
route_message(Msg, SessId) ->
    case session:get_sequencer_manager(SessId) of
        {ok, SeqMan} ->
            gen_server:cast(SeqMan, Msg);
        {error, Reason} ->
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
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([SeqManSup, SessId]) ->
    process_flag(trap_exit, true),
    {ok, SessId} = session:update(SessId, #{sequencer_manager => self()}),
    gen_server:cast(self(), {initialize, SeqManSup}),
    {ok, #state{session_id = SessId}}.

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
handle_call({sequencer_stream_initialized, StmId}, {Pid, _},
    #state{sequencer_streams = SeqStms} = State) ->
    case maps:find(StmId, SeqStms) of
        {ok, {state, SeqStmState, PendingMsgs}} ->
            lists:foreach(fun(Msg) ->
                gen_server:cast(Pid, Msg)
            end, lists:reverse(PendingMsgs)),
            {reply, {ok, SeqStmState}, State#state{
                sequencer_streams = maps:put(StmId, {pid, Pid}, SeqStms)}};
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
handle_cast({initialize, SeqManSup}, #state{session_id = SessId} = State) ->
    {ok, SeqStmSup} = get_sequencer_stream_sup(SeqManSup),
    reset_message_stream(SessId),
    {noreply, State#state{sequencer_stream_sup = SeqStmSup}};

handle_cast({sequencer_stream_terminated, StmId, shutdown, _},
    #state{sequencer_streams = SeqStms} = State) ->
    {noreply, State#state{sequencer_streams = maps:remove(StmId, SeqStms)}};

handle_cast({sequencer_stream_terminated, StmId, _, SeqStmState},
    #state{sequencer_streams = SeqStms} = State) ->
    {noreply, State#state{
        sequencer_streams = maps:put(StmId, {state, SeqStmState, []}, SeqStms)}};

handle_cast(#client_message{message_stream = #message_stream{stream_id = StmId}} = Msg,
    #state{sequencer_stream_sup = SeqStmSup, sequencer_streams = SeqStms,
        session_id = SessId} = State) ->
    case maps:find(StmId, SeqStms) of
        {ok, {pid, Pid}} ->
            gen_server:cast(Pid, Msg),
            {noreply, State};
        {ok, {state, SeqStmState, PendingMsgs}} ->
            {noreply, State#state{sequencer_streams = maps:put(StmId, {state,
                SeqStmState, [Msg | PendingMsgs]}, SeqStms)}};
        error ->
            {ok, _SeqStm} = sequencer_stream_sup:start_sequencer_stream(SeqStmSup,
                self(), SessId, StmId),
            {noreply, State#state{sequencer_streams = maps:put(StmId, {state,
                undefined, [Msg]}, SeqStms)}}
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
%% Returns sequencer stream supervisor associated with sequencer manager.
%% @end
%%--------------------------------------------------------------------
-spec get_sequencer_stream_sup(SeqManSup :: pid()) ->
    {ok, SeqStmSup :: pid()} | {error, not_found}.
get_sequencer_stream_sup(SeqManSup) ->
    Id = sequencer_stream_sup,
    Children = supervisor:which_children(SeqManSup),
    case lists:keyfind(Id, 1, Children) of
        {Id, SeqStmSup, _, _} when is_pid(SeqStmSup) -> {ok, SeqStmSup};
        _ -> {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends 'message_stream_reset' request to the client. Sequence numbers
%% of each client stream should by reinitialized and unconfirmed messages
%% should be resent.
%% @end
%%--------------------------------------------------------------------
-spec reset_message_stream(SessId :: session:id()) ->
    ok | {error, Reason :: term()}.
reset_message_stream(SessId) ->
    Msg = #message_stream_reset{},
    ok = communicator:send(Msg, SessId).
