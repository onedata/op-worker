%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for dispatching events to event streams.
%%% @end
%%%-------------------------------------------------------------------
-module(event_manager).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("modules/events/definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-type streams() :: #{subscription:id() => pid()}.

%% event manager state:
%% session_id        - ID of a session associated with this event manager
%% event_manager_sup - pid of an event manager supervisor
%% event_stream_sup  - pid of an event stream supervisor
%% event_streams     - mapping from a subscription ID to an event stream pid
-record(state, {
    session_id :: session:id(),
    event_manager_sup :: pid(),
    event_stream_sup :: pid(),
    event_streams = #{} :: streams()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the event manager.
%% @end
%%--------------------------------------------------------------------
-spec start_link(EvtManSup :: pid(), SessId :: session:id()) ->
    {ok, EvtMan :: pid()} | ignore | {error, Reason :: term()}.
start_link(EvtManSup, SessId) ->
    gen_server:start_link(?MODULE, [EvtManSup, SessId], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the event manager.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([EvtManSup, SessId]) ->
    process_flag(trap_exit, true),
    {ok, SessId} = session:update(SessId, #{manager => self()}),
    {ok, #state{event_manager_sup = EvtManSup, session_id = SessId}, 0}.

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
handle_cast({register_stream, SubId, EvtStm}, #state{event_streams = Stms} = State) ->
    {noreply, State#state{event_streams = maps:put(SubId, EvtStm, Stms)}};

handle_cast({unregister_stream, SubId}, #state{event_streams = Stms} = State) ->
    {noreply, State#state{event_streams = maps:remove(SubId, Stms)}};

handle_cast(#event{} = Evt, #state{event_streams = Stms} = State) ->
    {noreply, State#state{event_streams = maps:map(fun(_, EvtStm) ->
        gen_server:cast(EvtStm, Evt),
        EvtStm
    end, Stms)}};

handle_cast(#subscription{id = SubId} = Sub, #state{event_stream_sup = EvtStmSup,
    session_id = SessId, event_streams = Stms} = State) ->
    subscription:send(Sub, SessId),
    {ok, EvtStm} = event_stream_sup:start_event_stream(EvtStmSup, self(), Sub, SessId),
    {noreply, State#state{event_streams = maps:put(SubId, EvtStm, Stms)}};

handle_cast(#subscription_cancellation{id = SubId}, #state{event_streams = Stms} = State) ->
    {ok, StmPid} = maps:find(SubId, Stms),
    erlang:exit(StmPid, shutdown),
    {noreply, State#state{event_streams = maps:remove(SubId, Stms)}};

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
handle_info(timeout, #state{event_manager_sup = EvtManSup, session_id = SessId} = State) ->
    {ok, EvtStmSup} = event_manager_sup:get_event_stream_sup(EvtManSup),
    {noreply, State#state{
        event_stream_sup = EvtStmSup,
        event_streams = start_event_streams(EvtStmSup, SessId)
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
    session:update(SessId, #{manager => undefined}).

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
%% Starts event streams for durable subscriptions.
%% @end
%%--------------------------------------------------------------------
-spec start_event_streams(EvtStmSup :: pid(), SessId :: session:id()) ->
    Stms :: streams().
start_event_streams(EvtStmSup, SessId) ->
    {ok, Subs} = subscription:list(),
    lists:foldl(fun(#subscription{id = SubId} = Sub, Stms) ->
        {ok, EvtStm} = event_stream_sup:start_event_stream(EvtStmSup, self(), Sub, SessId),
        maps:put(SubId, EvtStm, Stms)
    end, #{}, Subs).