%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for aggregating incomming events and executing handlers.
%%% @end
%%%-------------------------------------------------------------------
-module(event_stream).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("workers/event_manager/event_stream.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-export_type([event_stream/0, admission_rule/0, aggregation_rule/0,
    transition_rule/0, emission_rule/0, event_handler/0, metadata/0]).

-type event_stream() :: #event_stream{}.
-type admission_rule() :: fun((event_manager:event()) -> true | false).
-type aggregation_rule() :: fun((event_manager:event(), event_manager:event()) ->
    {ok, event_manager:event()} | {error, disparate}).
-type transition_rule() :: fun((event_manager:event(), metadata()) -> metadata()).
-type emission_rule() :: fun((metadata()) -> true | false).
-type event_handler() :: fun(([event_manager:event()]) -> ok).
-type metadata() :: term().

%% event stream state:
%% sub_id   - subscription ID associated with event stream
%% evt_disp - pid of event dispatcher
-record(state, {
    sub_id :: event_manager:subscription_id(),
    evt_disp :: pid(),
    evts = [] :: [event_manager:event()],
    spec :: event_stream(),
    meta :: metadata()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server.
%% @end
%%--------------------------------------------------------------------
-spec start_link(EvtDisp :: pid(), SubId :: event_manager:subscription_id(),
    EvtStmSpec :: event_stream()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(EvtDisp, SubId, EvtStmSpec) ->
    gen_server:start_link(?MODULE, [EvtDisp, SubId, EvtStmSpec], []).

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
init([EvtDisp, SubId, EvtStmSpec]) ->
    process_flag(trap_exit, true),
    gen_server:cast(self(), initialize),
    {ok, #state{sub_id = SubId, evt_disp = EvtDisp, spec = EvtStmSpec,
        meta = reset_metadata(EvtStmSpec)}}.

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
handle_cast(initialize, #state{evt_disp = EvtDisp, spec = SubId} = State) ->
    case gen_server:call(EvtDisp, {event_stream_initialized, SubId}) of
        {ok, #state{} = EvtStmState} ->
            ?info("Event stream reinitialized in state: ~p", [EvtStmState]),
            {noreply, EvtStmState};
        _ ->
            {noreply, State}
    end;

handle_cast({event, Evt}, #state{evts = Evts, meta = Meta, spec = EvtStmSpec} = State) ->
    {NewEvts, NewMeta} = aggregate(Evt, Evts, Meta, EvtStmSpec),
    case is_emission_rule_satisfied(NewMeta, EvtStmSpec) of
        true ->
            emit(NewEvts, EvtStmSpec),
            {noreply, State#state{evts = [], meta = reset_metadata(EvtStmSpec)}};
        false ->
            {noreply, State#state{evts = NewEvts, meta = NewMeta}}
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
terminate(Reason, #state{sub_id = SubId, evt_disp = EvtDisp} = State) ->
    ?warning("Event stream closed in state ~p due to: ~p", [State, Reason]),
    gen_server:cast(EvtDisp, {event_stream_terminated, SubId, Reason, State}).

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
%% @doc
%% Returns initial value of stream metadata.
%% @end
%%--------------------------------------------------------------------
-spec reset_metadata(EvtStmSpec :: event_stream()) -> metadata().
reset_metadata(#event_stream{metadata = Meta}) ->
    Meta.

%%--------------------------------------------------------------------
%% @doc
%% Executes stream event handlers on aggregated events.
%% @end
%%--------------------------------------------------------------------
-spec emit(Evts :: [event_manager:event()], EvtStmSpec :: event_stream()) -> ok.
emit(Evts, #event_stream{handlers = Handlers}) ->
    lists:foreach(fun(Handler) ->
        Handler(Evts)
    end, Handlers).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether emission rule is satisfied for event stream.
%% @end
%%--------------------------------------------------------------------
-spec is_emission_rule_satisfied(Meta :: metadata(), EvtStmSpec :: event_stream()) ->
    true | false.
is_emission_rule_satisfied(Meta, #event_stream{emission_rule = EmsRule}) ->
    EmsRule(Meta).

%%--------------------------------------------------------------------
%% @doc
%% Aggregates an event with first aggregable event stored in the event stream.
%% @equiv aggregate(Evt, Evts, [], Meta, EvtStmSpec)
%% @end
%%--------------------------------------------------------------------
-spec aggregate(Evt :: event_manager:event(), Evts :: [event_manager:event()],
    Meta :: metadata(), EvtStmSpec :: event_stream()) ->
    {NewEvts :: [event_manager:event()], NewMeta :: metadata()}.
aggregate(Evt, Evts, Meta, EvtStmSpec) ->
    aggregate(Evt, Evts, [], Meta, EvtStmSpec).

%%--------------------------------------------------------------------
%% @doc
%% Aggregates an event with first aggregable event stored in the event stream.
%% @end
%%--------------------------------------------------------------------
-spec aggregate(Evt :: event_manager:event(), Evts :: [event_manager:event()],
    DipEvts :: [event_manager:event()], Meta :: metadata(), EvtStmSpec :: event_stream()) ->
    {NewEvts :: [event_manager:event()], NewMeta :: metadata()}.
aggregate(Evt, [], Evts, Meta, #event_stream{transition_rule = TrsRule}) ->
    {[Evt | Evts], TrsRule(Evt, Meta)};

aggregate(Evt, [StmEvt | StmEvts], DisEvts, Meta,
    #event_stream{aggregation_rule = AggRule, transition_rule = TrsRule} = EvtStmSpec) ->
    case AggRule(Evt, StmEvt) of
        {ok, NewEvt} ->
            {[NewEvt | StmEvts] ++ DisEvts, TrsRule(Evt, Meta)};
        {error, disparate} ->
            aggregate(Evt, StmEvts, [StmEvt | DisEvts], Meta, EvtStmSpec)
    end.