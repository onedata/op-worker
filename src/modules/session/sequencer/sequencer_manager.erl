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

-include("modules/datastore/datastore.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/stream_messages.hrl").
-include_lib("ctool/include/logging.hrl").

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
%% sequencer_in_stream_sup   - pid of incomming sequencer stream supervisor
%% sequencer_out_stream_sup  - pid of outgoing sequencer stream supervisor
%% sequencer_in_streams      - mapping from stream ID to an incomming sequencer stream pid
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
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([SeqManSup, SessId]) ->
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
handle_call(open_stream, _From, State) ->
    StmId = generate_stream_id(),
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

handle_cast({close_stream, StmId}, State) ->
    forward_to_sequencer_out_stream(#server_message{
        message_body = #end_of_message_stream{}
    }, StmId, State),
    {noreply, State};

handle_cast(#client_message{message_body = #message_stream_reset{
    stream_id = StmId} = Msg}, State) ->
    forward_to_sequencer_out_stream(Msg, StmId, State),
    {noreply, State};

handle_cast(#client_message{message_body = #message_request{
    stream_id = StmId} = Msg}, State) ->
    forward_to_sequencer_out_stream(Msg, StmId, State),
    {noreply, State};

handle_cast(#client_message{message_body = #message_acknowledgement{
    stream_id = StmId} = Msg}, State) ->
    forward_to_sequencer_out_stream(Msg, StmId, State),
    {noreply, State};

handle_cast(#client_message{} = Msg, State) ->
    {ok, SeqStm, NewState} = get_or_create_sequencer_in_stream(Msg, State),
    gen_fsm:send_event(SeqStm, Msg),
    {noreply, NewState};

handle_cast(#server_message{} = Msg, State) ->
    {ok, SeqStm} = get_sequencer_out_stream(Msg, State),
    gen_server:cast(SeqStm, Msg),
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
    {stop, shutdown, State};

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
%% Returns sequencer stream for outgoing messages associated with provided stream ID.
%% @end
%%--------------------------------------------------------------------
-spec get_sequencer_out_stream(Ref :: #server_message{} | stream_id(),
    State :: #state{}) -> {ok, SeqStm :: pid()}.
get_sequencer_out_stream(#server_message{message_stream = #message_stream{
    stream_id = StmId}}, State) ->
    get_sequencer_out_stream(StmId, State);

get_sequencer_out_stream(StmId, #state{sequencer_out_streams = Stms}) ->
    maps:find(StmId, Stms).

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
    {ok, SeqStm} = get_sequencer_out_stream(StmId, State),
    gen_server:cast(SeqStm, Msg).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns sequencer stream for incomming messages associated with provided
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
%% Creates sequencer stream for incomming messages.
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
%% Returns increasing stream IDs based on the timestamp.
%% @end
%%--------------------------------------------------------------------
-spec generate_stream_id() -> StmId :: stream_id().
generate_stream_id() ->
    {MegaSecs, Secs, MicroSecs} = erlang:now(),
    MegaSecs * 1000000000000 + Secs * 1000000 + MicroSecs.
