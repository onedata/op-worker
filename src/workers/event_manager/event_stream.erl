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
    {ok, event_manager:event()} | {error, different}).
-type transition_rule() :: fun((metadata(), event_manager:event()) -> metadata()).
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
    meta :: metadata(),
    emission_reference :: reference()
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
init([EvtDisp, SubId, #event_stream{metadata = Meta} = EvtStmSpec]) ->
    process_flag(trap_exit, true),
    gen_server:cast(self(), initialize),
    {ok, #state{sub_id = SubId, evt_disp = EvtDisp, spec = EvtStmSpec, meta = Meta}}.

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
handle_cast(initialize, #state{sub_id = SubId, evt_disp = EvtDisp} = State) ->
    case gen_server:call(EvtDisp, {event_stream_initialized, SubId}) of
        {ok, #state{} = EvtStmState} ->
            ?info("Event stream reinitialized in state: ~p", [EvtStmState]),
            {noreply, reset_time_emission(EvtStmState)};
        _ ->
            {noreply, reset_time_emission(State)}
    end;

handle_cast({event, Evt}, #state{evts = Evts, meta = Meta, spec = EvtStmSpec} = State) ->
    NewEvts = apply_aggregation_rule(Evt, Evts, EvtStmSpec),
    NewMeta = apply_transition_rule(Evt, Meta, EvtStmSpec),
    NewState = State#state{evts = NewEvts, meta = NewMeta},
    case apply_emission_rule(NewMeta, EvtStmSpec) of
        true ->
            {noreply, emit(NewState)};
        false ->
            {noreply, NewState}
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
handle_info({periodic_emission, Ref}, #state{emission_reference = Ref} = State) ->
    {noreply, emit(State)};

handle_info({periodic_emission, _}, State) ->
    {noreply, State};

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
%% @private
%% @doc
%% Executes stream event handlers on aggregated events stored in the stream.
%% Resets periodic emission of events.
%% @end
%%--------------------------------------------------------------------
-spec emit(State :: #state{}) -> NewState :: #state{}.
emit(#state{evts = []} = State) ->
    reset_time_emission(State);
emit(#state{evts = Evts, spec = #event_stream{handlers = Handlers,
    metadata = Meta}} = State) ->
    lists:foreach(fun(Handler) ->
        Handler(Evts)
    end, Handlers),
    reset_time_emission(State#state{evts = [], meta = Meta}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resets the time emission by sending 'periodic_emission' message to itself.
%% Message is marked with new reference stored in event stream state, so that
%% event stream can ignore messages with reference different from the one saved
%% in the state.
%% @end
%%--------------------------------------------------------------------
-spec reset_time_emission(State :: #state{}) -> NewState :: #state{}.
reset_time_emission(#state{spec = #event_stream{emission_time = EmTime}} = State)
    when is_integer(EmTime) ->
    Ref = make_ref(),
    erlang:send_after(EmTime, self(), {periodic_emission, Ref}),
    State#state{emission_reference = Ref};
reset_time_emission(#state{} = State) ->
    State.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether emission rule is satisfied for event stream.
%% @end
%%--------------------------------------------------------------------
-spec apply_emission_rule(Meta :: metadata(), EvtStmSpec :: event_stream()) ->
    true | false.
apply_emission_rule(Meta, #event_stream{emission_rule = EmsRule}) ->
    EmsRule(Meta).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies aggregation rule on every event stored in the stream. Returns
%% modified list of events. If none of events already in the stream can be aggregated
%% with the new event it is appended to the list of events.
%% @end
%%--------------------------------------------------------------------
-spec apply_aggregation_rule(NewEvt :: event_manager:event(), Evts :: [event_manager:event()],
    EvtStmSpec :: event_stream()) -> NewEvts :: [event_manager:event()].
apply_aggregation_rule(NewEvt, Evts, #event_stream{aggregation_rule = AggRule}) ->
    {NewEvts, AggCount} = lists:mapfoldl(fun(Evt, AggCount) ->
        case AggRule(Evt, NewEvt) of
            {ok, AggEvt} -> {AggEvt, AggCount + 1};
            {error, different} -> {Evt, AggCount}
        end
    end, 0, Evts),
    case AggCount of
        0 -> [NewEvt | NewEvts];
        _ -> NewEvts
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies transition rule on event stream metadata and new event. Returns
%% new metadata associated with the stream.
%% @end
%%--------------------------------------------------------------------
-spec apply_transition_rule(NewEvt :: event_manager:event(), Meta :: metadata(),
    EvtStmSpec :: event_stream()) -> NewMeta :: metadata().
apply_transition_rule(Evt, Meta, #event_stream{transition_rule = TrsRule}) ->
    TrsRule(Meta, Evt).