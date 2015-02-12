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

-include("proto_internal/oneclient/client_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% sequencer dispatcher state:
%% seq_stm_sup - pid of sequencer stream supervisor
%% cons        - list of connection pids to client associated with
%%                sequencer dispatcher
%% seq_stms    - mapping from message ID to sequencer stream pid
-record(state, {
    seq_stm_sup,
    cons = [],
    seq_stms = #{}
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(SeqStmSup :: pid(), Con :: pid()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(SeqStmSup, Con) ->
    gen_server:start_link(?MODULE, [SeqStmSup, Con], []).

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
init([SeqStmSup, Con]) ->
    {ok, #state{seq_stm_sup = SeqStmSup, cons = [Con]}}.

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
handle_call({sequencer_stream_initialized, MsgId}, {SeqStm, _}, #state{seq_stms = SeqStms} = State) ->
    case maps:find(MsgId, SeqStms) of
        {ok, {state, SeqStmState, PendingMsgs}} ->
            lists:foreach(fun(Msg) ->
                gen_server:cast(SeqStm, Msg)
            end, lists:reverse(PendingMsgs)),
            {reply, {ok, SeqStmState}, State#state{seq_stms = maps:put(MsgId, {pid, SeqStm}, SeqStms)}};
        _ ->
            {reply, undefined, State}
    end;

handle_call({add_connection, Con}, _From, #state{cons = Cons} = State) ->
    {reply, ok, State#state{cons = [Con | Cons]}};

handle_call({remove_connection, Con}, _From, #state{cons = Cons} = State) ->
    {reply, ok, State#state{cons = Cons -- [Con]}};

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
handle_cast({sequencer_stream_terminated, MsgId, normal, _}, #state{seq_stms = SeqStms} = State) ->
    {noreply, State#state{seq_stms = maps:remove(MsgId, SeqStms)}};

handle_cast({sequencer_stream_terminated, MsgId, _, SeqStmState}, #state{seq_stms = SeqStms} = State) ->
    {noreply, State#state{seq_stms = maps:put(MsgId, {state, SeqStmState, []}, SeqStms)}};

handle_cast(#client_message{message_id = MsgId} = Msg,
    #state{seq_stm_sup = SeqStmSup, seq_stms = SeqStms} = State) ->
    case maps:find(MsgId, SeqStms) of
        {ok, {pid, SeqStm}} ->
            gen_server:cast(SeqStm, Msg),
            {noreply, State};
        {ok, {state, SeqStmState, PendingMsgs}} ->
            {noreply, State#state{seq_stms =
            maps:put(MsgId, {state, SeqStmState, [Msg | PendingMsgs]}, SeqStms)}};
        _ ->
            SeqDisp = self(),
            {ok, _SeqStm} = sequencer_stream_sup:start_sequencer_stream(SeqStmSup, SeqDisp, MsgId),
            {noreply, State#state{seq_stms =
            maps:put(MsgId, {state, undefined, [Msg]}, SeqStms)}}
    end;

handle_cast({send, Msg}, #state{cons = []} = State) ->
    ?warning("~p:~p cannot send message ~p due to: 'connection pool empty'",
        [?MODULE, ?LINE, Msg]),
    {noreply, State};

handle_cast({send, Msg}, #state{cons = [Con | Cons]} = State) ->
    protocol_handler:cast(Con, Msg),
    {noreply, State#state{cons = Cons ++ [Con]}};

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
terminate(_Reason, _State) ->
    ok.

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
