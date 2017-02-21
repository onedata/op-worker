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
-export([start_link/3, execute_event_handler/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-export_type([key/0, ctx/0, metadata/0, init_handler/0, terminate_handler/0,
    event_handler/0, aggregation_rule/0, transition_rule/0, emission_rule/0,
    emission_time/0]).

-type key() :: atom().
-type ctx() :: maps:map().
-type metadata() :: term().
-type init_handler() :: fun((subscription:id(), session:id()) -> ctx()).
-type terminate_handler() :: fun((ctx()) -> term()).
-type event_handler() :: fun((Evts :: [event:type()], ctx()) -> ok).
-type aggregation_rule() :: fun((OldEvt :: event:type(), Evt :: event:type()) ->
    NewEvt :: event:type()).
-type transition_rule() :: fun((metadata(), Evt :: event:object()) -> metadata()).
-type emission_rule() :: fun((metadata()) -> true | false).
-type emission_time() :: timeout().
-type subscriptions() :: #{subscription:id() => event_router:key() | local}.
-type events() :: #{event:key() => event:type()}.

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
    key :: key(),
    stream :: event:stream(),
    session_id :: session:id(),
    manager :: pid(),
    ctx :: term(),
    metadata :: metadata(),
    events = #{} :: events(),
    subscriptions = #{} :: subscriptions(),
    emission_ref :: undefined | pending | reference(),
    handler_ref :: undefined | {pid(), reference()}
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the event stream.
%% @end
%%--------------------------------------------------------------------
-spec start_link(Mgr :: pid(), Sub :: #subscription{}, SessId :: session:id()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(Mgr, Sub, SessId) ->
    gen_server2:start_link(?MODULE, [Mgr, Sub, SessId], []).

%%--------------------------------------------------------------------
%% @doc
%% Executes event handler. If another event handler is being executed, waits
%% until it is finished.
%% @end
%%--------------------------------------------------------------------
-spec execute_event_handler(Force :: boolean(), State :: #state{}) -> ok.
execute_event_handler(Force, #state{events = Evts, handler_ref = undefined,
    ctx = Ctx, stream = #event_stream{event_handler = Handler},
    session_id = SessId, key = StmKey} = State) ->
    try
        Start = erlang:monotonic_time(milli_seconds),
        EvtsList = maps:values(Evts),
        case {Force, EvtsList} of
            {true, _} -> Handler(EvtsList, Ctx);
            {false, []} -> ok;
            {_, _} -> Handler(EvtsList, Ctx)
        end,
        Duration = erlang:monotonic_time(milli_seconds) - Start,
        ?debug("Execution of handler on events ~p in event stream ~p and session
        ~p took ~p milliseconds", [EvtsList, StmKey, SessId, Duration])
    catch
        Error:Reason ->
            ?error_stacktrace("~p event handler of state ~p failed with ~p:~p",
                [?MODULE, State, Error, Reason])
    end;

execute_event_handler(Force, #state{handler_ref = {Pid, _}} = State) ->
    MonitorRef = monitor(process, Pid),
    receive
        {'DOWN', MonitorRef, _, _, _} -> ok
    end,
    execute_event_handler(Force, State#state{handler_ref = undefined}).


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
init([Mgr, #subscription{id = SubId} = Sub, SessId]) ->
    Key = subscription_type:get_stream_key(Sub),
    Stm = subscription_type:get_stream(Sub),
    ?debug("Initializing event stream ~p in session ~p", [Key, SessId]),
    process_flag(trap_exit, true),
    gen_server2:cast(Mgr, {register_stream, Key, self()}),
    {ok, #state{
        key = Key,
        session_id = SessId,
        manager = Mgr,
        ctx = exec(Stm#event_stream.init_handler, [SubId, SessId]),
        metadata = Stm#event_stream.metadata,
        stream = Stm,
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
handle_cast(#event{type = Evt}, #state{key = Key, session_id = SessId} = State) ->
    ?debug("Handling event ~p in event stream ~p and session ~p", [Evt, Key, SessId]),
    {noreply, process_event(Evt, State)};

handle_cast({add_subscription, Sub}, #state{} = State) ->
    #state{session_id = SessId, subscriptions = Subs} = State,
    {noreply, State#state{subscriptions = add_subscription(SessId, Sub, Subs)}};

handle_cast({remove_subscription, SubId}, #state{} = State) ->
    #state{session_id = SessId, subscriptions = Subs} = State,
    case remove_subscription(SessId, SubId, Subs) of
        {true, NewSubs} -> {stop, normal, State#state{subscriptions = NewSubs}};
        {false, NewSubs} -> {noreply, State#state{subscriptions = NewSubs}}
    end;

handle_cast({flush, NotifyFun}, #state{ctx = Ctx} = State) ->
    #state{ctx = NewCtx} = NewState = spawn_event_handler(
        true, State#state{ctx = Ctx#{notify => NotifyFun}}
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
handle_info({'DOWN', MonitorRef, _, _, _}, #state{handler_ref = {_, MonitorRef},
    emission_ref = EmissionRef} = State) ->
    NewState = State#state{handler_ref = undefined},
    case EmissionRef of
        pending -> {noreply, maybe_spawn_event_handler(false, NewState)};
        _ -> {noreply, NewState}
    end;

handle_info({'DOWN', _, _, _, _}, State) ->
    {noreply, State};

handle_info({'EXIT', _, shutdown}, State) ->
    {stop, normal, State};

handle_info({periodic_emission, Ref}, #state{emission_ref = Ref} = State) ->
    {noreply, maybe_spawn_event_handler(false, State)};

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
terminate(Reason, #state{manager = Mgr, key = Key, stream = Stm, ctx = Ctx} = State) ->
    ?log_terminate(Reason, State),
    spawn_event_handler(false, State),
    remove_subscriptions(State),
    exec(Stm#event_stream.terminate_handler, [Ctx]),
    gen_server2:cast(Mgr, {unregister_stream, Key}).

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
%% Adds subscription.
%% @end
%%--------------------------------------------------------------------
-spec add_subscription(SessId :: session:id(), Sub :: #subscription{},
    Subs :: subscriptions()) -> NewSubs :: subscriptions().
add_subscription(SessId, #subscription{id = SubId} = Sub, Subs) ->
    case event_router:add_subscriber(Sub, SessId) of
        {ok, Key} -> maps:put(SubId, Key, Subs);
        {error, session_only} -> maps:put(SubId, session_only, Subs)
    end.

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
        {ok, session_only} ->
            maps:remove(SubId, Subs);
        {ok, Key} ->
            event_router:remove_subscriber(Key, SessId),
            maps:remove(SubId, Subs);
        error ->
            Subs
    end,
    {maps:size(NewSubs) == 0, NewSubs}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes subscriptions from the events routing table.
%% @end
%%--------------------------------------------------------------------
-spec remove_subscriptions(State :: #state{}) -> ok.
remove_subscriptions(#state{session_id = SessId, subscriptions = Subs}) ->
    maps:fold(fun
        (session_only, _, _) -> ok;
        (Key, _, _) -> event_router:remove_subscriber(Key, SessId)
    end, ok, Subs).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Spawns event handler execution if no other event handler is currently being
%% executed.
%% @end
%%--------------------------------------------------------------------
-spec maybe_spawn_event_handler(Force :: boolean(), State :: #state{}) ->
    NewState :: #state{}.
maybe_spawn_event_handler(Force, #state{handler_ref = undefined} = State) ->
    spawn_event_handler(Force, State);

maybe_spawn_event_handler(_Force, #state{} = State) ->
    State#state{emission_ref = pending}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Spawns event handler execution and resets stream metadata.
%% @end
%%--------------------------------------------------------------------
-spec spawn_event_handler(Force :: boolean(), State :: #state{}) -> NewState :: #state{}.
spawn_event_handler(Force, #state{stream = Stm} = State) ->
    HandlerRef = spawn_monitor(?MODULE, execute_event_handler, [Force, State]),
    State#state{
        events = #{},
        metadata = Stm#event_stream.metadata,
        emission_ref = undefined,
        handler_ref = HandlerRef
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes event on the event stream.
%% @end
%%--------------------------------------------------------------------
-spec process_event(Evt :: event:object(), State :: #state{}) -> NewState :: #state{}.
process_event(Evt, #state{events = Evts, metadata = Meta, stream = Stm} = State) ->
    EvtKey = event_type:get_aggregation_key(Evt),
    NewEvts = case maps:find(EvtKey, Evts) of
        {ok, OldEvt} ->
            NewEvt = exec(Stm#event_stream.aggregation_rule, [OldEvt, Evt]),
            maps:put(EvtKey, NewEvt, Evts);
        error ->
            maps:put(EvtKey, Evt, Evts)
    end,
    NewMeta = exec(Stm#event_stream.transition_rule, [Meta, Evt]),
    NewState = State#state{events = NewEvts, metadata = NewMeta},
    case exec(Stm#event_stream.emission_rule, [NewMeta]) of
        true -> maybe_spawn_event_handler(false, NewState);
        false -> maybe_schedule_event_handler_execution(NewState)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules sending 'periodic_emission' message to itself. Message is
%% marked with new reference stored in event stream state, so that event stream
%% can ignore messages with reference different from the one saved in the state.
%% @end
%%--------------------------------------------------------------------
-spec maybe_schedule_event_handler_execution(State :: #state{}) -> NewState :: #state{}.
maybe_schedule_event_handler_execution(#state{emission_ref = undefined,
    stream = #event_stream{emission_time = Time}} = State) when is_integer(Time) ->
    Ref = make_ref(),
    erlang:send_after(Time, self(), {periodic_emission, Ref}),
    State#state{emission_ref = Ref};

maybe_schedule_event_handler_execution(State) ->
    State.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes function with provided arguments.
%% @end
%%--------------------------------------------------------------------
-spec exec(Fun :: fun(), Args :: [term()]) -> Result :: term().
exec(Fun, Args) ->
    erlang:apply(Fun, Args).