%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Main monitor for space file events - the primary, long-lived monitor
%%% that streams events from current Couchbase sequence.
%%% 
%%% Responsibilities:
%%%   - Stream live events from Couchbase
%%%   - Accept or reject client subscriptions based on since_seq
%%%   - Accept takeovers from catching monitors
%%%   - Timeout when no observers AND no catching monitors exist
%%% 
%%% NOTE: Only one main monitor exists per actively monitored space.
%%% @end
%%%-------------------------------------------------------------------
-module(space_files_main_monitor).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("http/space_file_events_stream.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    id/0,
    spec/1,
    start_link/1,

    try_subscribe/2,
    verify_inactive/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-record(state, {
    space_id :: od_space:id(),

    changes_stream_pid :: pid() | undefined,

    monitoring :: space_files_monitor_common:monitoring(),

    inactivity_timer = undefined :: undefined | reference()
}).
-type state() :: #state{}.


%% The process is supposed to die after ?INACTIVITY_PERIOD_MS time of idling (no subscribers)
-define(INACTIVITY_PERIOD_MS, op_worker:get_env(space_files_monitor_inactivity_period_ms, 30_000)).
-define(NOTIFY_INACTIVE_REQ, notify_inactive).


%%%===================================================================
%%% API
%%%===================================================================


-spec id() -> ?MODULE.
id() -> ?MODULE.


-spec spec(od_space:id()) -> supervisor:child_spec().
spec(SpaceId) ->
    #{
        id => id(),
        start => {?MODULE, start_link, [SpaceId]},
        restart => permanent,
        shutdown => timer:seconds(10),
        type => worker
    }.


-spec start_link(od_space:id()) -> {ok, pid()} | {error, term()}.
start_link(SpaceId) ->
    gen_server2:start_link(?MODULE, [SpaceId], []).


-spec try_subscribe(pid(), space_files_monitor_common:subscribe_req()) ->
    ok | {error, {main_ahead, couchbase_changes:seq()}} | errors:error().
try_subscribe(MonitorPid, SubscribeReq) ->
    space_files_monitor_common:call_monitor(MonitorPid, SubscribeReq).


-spec verify_inactive(pid()) -> {ok, boolean()} | errors:error().
verify_inactive(MonitorPid) ->
    space_files_monitor_common:call_monitor(MonitorPid, verify_inactive).


%%%===================================================================
%%% gen_server2 callbacks
%%%===================================================================


-spec init([od_space:id()]) -> {ok, state(), non_neg_integer()}.
init([SpaceId]) ->
    process_flag(trap_exit, true),

    ?info("[ space file events ]: Starting main monitor for space '~ts'", [SpaceId]),

    SinceSeq = dbsync_state:get_seq(SpaceId, oneprovider:get_id()),
    ChangesPid = space_files_monitor_common:start_link_changes_stream(SpaceId, SinceSeq),

    State = #state{
        space_id = SpaceId,
        changes_stream_pid = ChangesPid,
        monitoring = #monitoring{current_seq = SinceSeq}
    },
    {ok, State, ?INACTIVITY_PERIOD_MS}.


-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), state()} |
    {noreply, state()}.
handle_call(#subscribe_req{since_seq = SinceSeq}, _From, State = #state{monitoring = #monitoring{
    current_seq = CurrentSeq
}}) when is_integer(SinceSeq) andalso CurrentSeq > SinceSeq ->
    %% Client is behind - reject and tell to start catching
    reply({error, {main_ahead, CurrentSeq}}, State);

handle_call(SubscribeReq = #subscribe_req{}, _From, State) ->
    case space_files_monitor_common:add_observer(State#state.monitoring, SubscribeReq) of
        {ok, NewMonitoring} ->
            reply(ok, State#state{monitoring = NewMonitoring});
        {error, _} = Error ->
            reply(Error, State)
    end;

handle_call(verify_inactive, _From, State) ->
    reply({ok, is_inactive(State)}, State);

handle_call(#docs_change_notification{docs = ChangedDocs}, From, State) ->
    gen_server2:reply(From, ok),

    noreply(State#state{
        monitoring = space_files_monitor_common:process_docs(
            ChangedDocs, State#state.monitoring
        )
    });

handle_call(#seq_advancement_notification{seq = NewSpaceSeq}, From, State = #state{monitoring = Monitoring}) ->
    gen_server2:reply(From, ok),

    noreply(State#state{
        monitoring = space_files_monitor_common:send_heartbeats_if_needed(
            Monitoring#monitoring{current_seq = NewSpaceSeq}
        )
    });

handle_call(Request, _From, #state{} = State) ->
    ?log_bad_request(Request),
    reply({error, unknown_request}, State).


-spec handle_cast(Request :: term(), state()) ->
    {noreply, state()}.
handle_cast(Request, #state{} = State) ->
    ?log_bad_request(Request),
    noreply(State).


-spec handle_info(timeout() | term(), state()) ->
    {noreply, state()} |
    {stop, term(), state()}.
handle_info({'EXIT', ObserverPid, _Reason}, State = #state{}) ->
    NewState = State#state{
        monitoring = space_files_monitor_common:remove_observer(State#state.monitoring, ObserverPid)
    },
    noreply(NewState);

handle_info(stream_ended, State = #state{}) ->
    ?error(
        "[ space file events ]: Couchbase changes stream ended for main monitor space '~ts'",
        [State#state.space_id]
    ),
    {stop, {shutdown, stream_ended}, State};

handle_info(?NOTIFY_INACTIVE_REQ, State = #state{space_id = SpaceId}) ->
    is_inactive(State) andalso files_monitoring_manager:notify_inactive(SpaceId),
    noreply(State#state{inactivity_timer = undefined});

handle_info(Info, #state{} = State) ->
    ?log_bad_request(Info),
    noreply(State).


-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()), state()) ->
    term().
terminate(Reason, State = #state{changes_stream_pid = ChangesStreamPid}) ->
    couchbase_changes:cancel_stream(ChangesStreamPid),
    ?log_terminate(Reason, State).


-spec code_change(OldVsn :: term() | {down, term()}, state(), Extra :: term()) ->
    {ok, state()} | {error, Reason :: term()}.
code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec reply(Response, state()) -> {reply, Response, state()} when Response :: term().
reply(Response, State) ->
    {reply, Response, check_inactivity_timer(State)}.


%% @private
-spec noreply(state()) -> {noreply, state()}.
noreply(State) ->
    {noreply, check_inactivity_timer(State)}.


%% @private
-spec check_inactivity_timer(state()) -> state().
check_inactivity_timer(State) ->
    case is_inactive(State) of
        true -> schedule_inactivity_shutdown(State);
        false -> cancel_inactivity_shutdown(State)
    end.


%% @private
-spec is_inactive(state()) -> boolean().
is_inactive(State) ->
    not space_files_monitor_common:has_observers(State#state.monitoring).


%% @private
-spec schedule_inactivity_shutdown(state()) -> state().
schedule_inactivity_shutdown(#state{inactivity_timer = undefined} = State) ->
    State#state{inactivity_timer = erlang:send_after(
        ?INACTIVITY_PERIOD_MS, self(), ?NOTIFY_INACTIVE_REQ
    )};
schedule_inactivity_shutdown(State) ->
    State.


%% @private
-spec cancel_inactivity_shutdown(state()) -> state().
cancel_inactivity_shutdown(#state{inactivity_timer = undefined} = State) ->
    State;
cancel_inactivity_shutdown(#state{inactivity_timer = TimerRef} = State) ->
    erlang:cancel_timer(TimerRef, [{async, true}, {info, false}]),
    State#state{inactivity_timer = undefined}.
