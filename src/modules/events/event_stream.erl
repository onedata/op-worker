%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gen_server behaviour and is responsible
%%% for aggregating incoming events and executing handlers. It is supervised by
%%% event_stream_sup supervisor and coordinated by event_manager.
%%% @end
%%%-------------------------------------------------------------------
-module(event_stream).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

-include("modules/events/definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-export_type([id/0, key/0, ctx/0, definition/0, metadata/0, init_handler/0,
    terminate_handler/0, event_handler/0, admission_rule/0, aggregation_rule/0,
    transition_rule/0, emission_rule/0, emission_time/0]).

-type id() :: binary().
-type key() :: binary().
-type ctx() :: #{}.
-type definition() :: #event_stream_definition{}.
-type metadata() :: term().
-type init_handler() :: fun((#subscription{}, session:id(), session:type()) -> ctx()).
-type terminate_handler() :: fun((ctx()) -> term()).
-type event_handler() :: fun(([event:event()], ctx()) -> ok).
-type admission_rule() :: fun((event:event()) -> true | false).
-type aggregation_rule() :: fun((event:event(), event:event()) -> event:event()).
-type transition_rule() :: fun((metadata(), event:event()) -> metadata()).
-type emission_rule() :: fun((metadata()) -> true | false).
-type emission_time() :: timeout().
-type subscriptions() :: #{subscription:id() => #subscription{}}.

-type events() :: #{event:key() => event:event()}.

%% event stream state:
%% subscription_id - ID of an event subscription
%% session_id      - ID of a session associated with this event manager
%% event_manager   - pid of an event manager that controls this event stream
%% definition      - event stream definition
%% ctx             - result of init handler execution
%% metadata        - event stream metadata
%% events          - mapping form an event key to an aggregated event
%% emission_ref    - reference associated with the last 'periodic_emission' message
-record(state, {
    stream_id :: id(),
    session_id :: session:id(),
    session_type :: session:type(),
    event_manager :: pid(),
    definition :: definition(),
    ctx :: term(),
    metadata :: metadata(),
    events = #{} :: events(),
    emission_ref :: undefined | reference(),
    subscriptions = #{} :: subscriptions()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the event stream.
%% @end
%%--------------------------------------------------------------------
-spec start_link(SessType :: session:type(), EvtMan :: pid(), Sub :: #subscription{},
    SessId :: session:id()) -> {ok, Pid :: pid()} | ignore |{error, Reason :: term()}.
start_link(SessType, EvtMan, Sub, SessId) ->
    gen_server:start_link(?MODULE, [SessType, EvtMan, Sub, SessId], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the event stream.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore.
init([SessType, EvtMan, #subscription{event_stream = #event_stream_definition{
    id = StmId} = StmDef} = Sub, SessId]) ->
    ?debug("Initializing event stream ~p in session ~p", [StmId, SessId]),
    process_flag(trap_exit, true),
    register_stream(EvtMan, StmId),
    {ok, #state{
        stream_id = StmId,
        session_id = SessId,
        session_type = SessType,
        event_manager = EvtMan,
        ctx = execute_init_handler(StmDef, Sub, SessId, SessType),
        metadata = get_initial_metadata(StmDef),
        definition = StmDef,
        subscriptions = add_subscription(SessId, Sub, #{})
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}}.
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
handle_cast(#event{} = Evt, #state{stream_id = StmId, session_id = SessId,
    definition = StmDef} = State) ->
    case apply_admission_rule(Evt, StmDef) of
        true ->
            ?debug("Handling event ~p in event stream ~p and session ~p",
                [Evt, StmId, SessId]),
            {noreply, process_event(Evt, State)};
        false -> {noreply, State}
    end;

handle_cast({add_subscription, Sub}, #state{session_id = SessId,
    subscriptions = Subs} = State) ->
    {noreply, State#state{subscriptions = add_subscription(SessId, Sub, Subs)}};

handle_cast({remove_subscription, SubId}, #state{session_id = SessId,
    subscriptions = Subs} = State) ->
    case remove_subscription(SessId, SubId, Subs) of
        {true, NewSubs} -> {stop, normal, State#state{subscriptions = NewSubs}};
        {false, NewSubs} -> {noreply, State#state{subscriptions = NewSubs}}
    end;

handle_cast({flush, NotifyFun}, #state{ctx = Ctx} = State) ->
    #state{ctx = NewCtx} = NewState = execute_event_handler(
        State#state{ctx = Ctx#{notify => NotifyFun}}
    ),
    {noreply, NewState#state{ctx = maps:remove(notify, NewCtx)}};

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
handle_info({'EXIT', _, shutdown}, State) ->
    {stop, normal, State};

handle_info({execute_event_handler, Ref}, #state{emission_ref = Ref} = State) ->
    {noreply, execute_event_handler(State)};

handle_info({execute_event_handler, _}, State) ->
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
terminate(Reason, #state{event_manager = EvtMan, stream_id = StmId,
    definition = StmDef, ctx = Ctx} = State) ->
    ?log_terminate(Reason, State),
    execute_event_handler(State),
    execute_terminate_handler(StmDef, Ctx),
    unregister_stream(EvtMan, StmId).

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
%% Registers event stream in the event manager.
%% @end
%%--------------------------------------------------------------------
-spec register_stream(EvtMan :: pid(), StmId :: id()) -> ok.
register_stream(EvtMan, StmId) ->
    gen_server:cast(EvtMan, {register_stream, StmId, self()}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Unregisters event stream in the event manager.
%% @end
%%--------------------------------------------------------------------
-spec unregister_stream(EvtMan :: pid(), StmId :: id()) -> ok.
unregister_stream(EvtMan, StmId) ->
    gen_server:cast(EvtMan, {unregister_stream, StmId}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds subscription.
%% @end
%%--------------------------------------------------------------------
-spec add_subscription(SessId :: session:id(), Sub :: #subscription{},
    Subs :: subscriptions()) -> NewSubs :: subscriptions().
add_subscription(SessId, #subscription{id = SubId} = Sub, Subs) ->
    file_subscription:add(SessId, Sub),
    maps:put(SubId, Sub, Subs).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes subscription.
%% @end
%%--------------------------------------------------------------------
-spec remove_subscription(SessId :: session:id(), SubId :: subscription:id(),
    Subs :: subscriptions()) -> {LastSub :: boolean(), NewSubs :: subscriptions()}.
remove_subscription(SessId, SubId, Subs) ->
    NewSubs = case maps:find(SubId, Subs) of
        {ok, Sub} ->
            file_subscription:remove(SessId, Sub),
            maps:remove(SubId, Subs);
        error ->
            Subs
    end,
    {maps:size(NewSubs) == 0, NewSubs}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes init handler.
%% @end
%%--------------------------------------------------------------------
-spec execute_init_handler(StmDef :: definition(), Sub :: #subscription{},
    SessId :: session:id(), SessType :: session:type()) -> Ctx :: ctx().
execute_init_handler(#event_stream_definition{init_handler = Handler}, Sub, SessId,
    SessType) ->
    Handler(Sub, SessId, SessType).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes terminate handler.
%% @end
%%--------------------------------------------------------------------
-spec execute_terminate_handler(StmDef :: definition(), Ctx :: ctx()) ->
    term().
execute_terminate_handler(#event_stream_definition{terminate_handler = Handler},
    Ctx) ->
    Handler(Ctx).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes stream event handlers on aggregated events stored in the stream.
%% Resets periodic emission of events.
%% @end
%%--------------------------------------------------------------------
-spec execute_event_handler(State :: #state{}) -> NewState :: #state{}.
execute_event_handler(#state{stream_id = StmId, session_id = SessId,
    events = Evts, definition = #event_stream_definition{event_handler = Handler
    } = StmDef, ctx = Ctx} = State) ->
    ?debug("Executing event handler on events ~p in event stream ~p and session ~p",
        [Evts, StmId, SessId]),
    try Handler(maps:values(Evts), Ctx)
    catch
        Error:Reason ->
            ?error_stacktrace("~p event handler of state ~p failed with ~p:~p",
                              [?MODULE, State, Error, Reason])
    end,
    State#state{
        events = #{},
        metadata = get_initial_metadata(StmDef),
        emission_ref = undefined
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns initial stream metadata.
%% @end
%%--------------------------------------------------------------------
-spec get_initial_metadata(StmDef :: definition()) -> Meta :: metadata().
get_initial_metadata(#event_stream_definition{metadata = Meta}) ->
    Meta.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes event on the event stream.
%% @end
%%--------------------------------------------------------------------
-spec process_event(Evt :: event:event(), State :: #state{}) -> NewState :: #state{}.
process_event(Evt, #state{events = Evts, metadata = Meta,
    definition = StmDef} = State) ->
    NewEvts = apply_aggregation_rule(Evt, Evts, StmDef),
    NewMeta = apply_transition_rule(Evt, Meta, StmDef),
    NewState = State#state{events = NewEvts, metadata = NewMeta},
    case apply_emission_rule(NewMeta, StmDef) of
        true -> execute_event_handler(NewState);
        false -> maybe_schedule_event_handler_execution(NewState)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules sending 'execute_event_handler' message to itself. Message is
%% marked with new reference stored in event stream state, so that event stream
%% can ignore messages with reference different from the one saved in the state.
%% @end
%%--------------------------------------------------------------------
-spec maybe_schedule_event_handler_execution(State :: #state{}) -> NewState :: #state{}.
maybe_schedule_event_handler_execution(#state{emission_ref = undefined,
    definition = #event_stream_definition{emission_time = Time}} = State)
    when is_integer(Time) ->
    Ref = make_ref(),
    erlang:send_after(Time, self(), {execute_event_handler, Ref}),
    State#state{emission_ref = Ref};

maybe_schedule_event_handler_execution(State) ->
    State.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies admission rule.
%% @end
%%--------------------------------------------------------------------
-spec apply_admission_rule(Evt :: event:event(), StmDef :: definition()) -> true | false.
apply_admission_rule(Evt, #event_stream_definition{admission_rule = Rule}) ->
    Rule(Evt).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies emission rule.
%% @end
%%--------------------------------------------------------------------
-spec apply_emission_rule(Meta :: metadata(), StmDef :: definition()) -> true | false.
apply_emission_rule(Meta, #event_stream_definition{emission_rule = Rule}) ->
    Rule(Meta).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies aggregation rule on events with the same key. If there is no event with
%% given key inserts new event into event map.
%% @end
%%--------------------------------------------------------------------
-spec apply_aggregation_rule(Evt :: event:event(), Evts :: events(), StmDef :: definition()) ->
    NewEvts :: events().
apply_aggregation_rule(#event{key = Key} = Evt, Evts, #event_stream_definition{
    aggregation_rule = Rule}) ->
    case maps:find(Key, Evts) of
        {ok, AggEvt} -> maps:put(Key, Rule(AggEvt, Evt), Evts);
        error -> maps:put(Key, Evt, Evts)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies transition rule on the event stream metadata and new event. Returns
%% new metadata associated with the stream.
%% @end
%%--------------------------------------------------------------------
-spec apply_transition_rule(Evt :: event:event(), Meta :: metadata(), StmDef :: definition()) ->
    NewMeta :: metadata().
apply_transition_rule(Evt, Meta, #event_stream_definition{transition_rule = Rule}) ->
    Rule(Meta, Evt).
