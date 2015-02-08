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
-module(sequencer_manager).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%% sequencer manager state:
%% seq_sup - pid of sequencer supervisor
%% cons   - list of connectionnection pids to FUSE client associated with
%%                 sequencer manager
%% seqs    - mapping from message ID to sequencer pid
-record(state, {
    seq_sup,
    cons = [],
    seqs = #{}
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
-spec start_link(SeqSup :: supervisor:sup_ref(), Connectionnection :: pid()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(SeqSup, Connectionnection) ->
    gen_server:start_link(?MODULE, [SeqSup, Connectionnection], []).

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
init([SeqSup, Connection]) ->
    {ok, #state{seq_sup = SeqSup, cons = [Connection]}}.

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
handle_call({sequencer_initialized, MsgId}, {Seq, _}, #state{seqs = Seqs} = State) ->
    case maps:find(MsgId, Seqs) of
        {ok, {state, SeqState, PendingMsgs}} ->
            lists:foreach(fun(Msg) ->
                gen_server:cast(Seq, Msg)
            end, lists:reverse(PendingMsgs)),
            {reply, {ok, SeqState}, State#state{seqs = maps:put(MsgId, {pid, Seq}, Seqs)}};
        _ ->
            {reply, undefined, State}
    end;

handle_call({add_connection, Connection}, _From, #state{cons = Connections} = State) ->
    {reply, ok, State#state{cons = [Connection | Connections]}};

handle_call({remove_connection, Connection}, _From, #state{cons = Connections} = State) ->
    {reply, ok, State#state{cons = Connections -- [Connection]}};

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
handle_cast({sequencer_terminated, MsgId, normal, _}, #state{seqs = Seqs} = State) ->
    {noreply, State#state{seqs = maps:remove(MsgId, Seqs)}};

handle_cast({sequencer_terminated, MsgId, _, SeqState}, #state{seqs = Seqs} = State) ->
    {noreply, State#state{seqs = maps:put(MsgId, {state, SeqState, []}, Seqs)}};

handle_cast(#client_message{message_id = MsgId} = Msg,
    #state{seq_sup = SeqSup, seqs = Seqs} = State) ->
    case maps:find(MsgId, Seqs) of
        {ok, {pid, Seq}} ->
            gen_server:cast(Seq, Msg),
            {noreply, State};
        {ok, {state, SeqState, PendingMsgs}} ->
            {noreply, State#state{seqs =
            maps:put(MsgId, {state, SeqState, [Msg | PendingMsgs]}, Seqs)}};
        _ ->
            SeqMan = self(),
            {ok, _Seq} = sequencer_sup:start_sequencer(SeqSup, SeqMan, MsgId),
            {noreply, State#state{seqs =
            maps:put(MsgId, {state, undefined, [Msg]}, Seqs)}}
    end;

handle_cast({send, Msg}, #state{cons = []} = State) ->
    ?warning("~p:~p cannot send message ~p due to: 'connection pool empty'", [?MODULE, ?LINE, Msg]),
    {noreply, State};

handle_cast({send, Msg}, #state{cons = [Connection | Connections]} = State) ->
    Connection ! {msg, Msg},
    {noreply, State#state{cons = Connections ++ [Connection]}};

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
%% Connectionverts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) -> {ok, NewState :: #state{}} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
