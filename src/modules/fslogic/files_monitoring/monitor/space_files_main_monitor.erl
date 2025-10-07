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
    spec/2,
    start_link/2,

    try_subscribe/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-record(state, {
    space_id :: od_space:id(),
    space_monitoring_sup_pid :: pid(),

    changes_stream_pid :: pid() | undefined,
    current_seq = 0 :: couchbase_changes:seq(),

    monitoring :: space_files_monitor_common:monitoring(),

    inactivity_timer = undefined :: undefined | reference()
}).
-type state() :: #state{}.


%% The process is supposed to die after ?INACTIVITY_PERIOD_MS time of idling (no subscribers)
-define(INACTIVITY_PERIOD_MS, op_worker:get_env(space_files_monitor_inactivity_period_ms, 10_000)).
-define(SHUTDOWN_INACTIVE_REQ, shutdown_inactive).


%%%===================================================================
%%% API
%%%===================================================================


-spec id() -> ?MODULE.
id() -> ?MODULE.


-spec spec(od_space:id(), pid()) -> supervisor:child_spec().
spec(SpaceId, SpaceMonitoringSupPid) ->
    #{
        id => id(),
        start => {?MODULE, start_link, [SpaceId, SpaceMonitoringSupPid]},
        restart => transient,
        shutdown => timer:seconds(10),
        type => worker
    }.


-spec start_link(od_space:id(), pid()) -> {ok, pid()} | {error, term()}.
start_link(SpaceId, SpaceMonitoringSupPid) ->
    gen_server2:start_link(?MODULE, [SpaceId, SpaceMonitoringSupPid], []).


-spec try_subscribe(pid(), space_files_monitor_common:subscribe_req()) ->
    ok | {error, {main_ahead, couchbase_changes:seq()}} | errors:error().
try_subscribe(MonitorPid, SubscribeReq) ->
    space_files_monitor_common:call_monitor(MonitorPid, SubscribeReq).


%%%===================================================================
%%% gen_server2 callbacks
%%%===================================================================


-spec init([od_space:id() | pid()]) -> {ok, state(), non_neg_integer()}.
init([SpaceId, SpaceMonitoringSupPid]) ->
    process_flag(trap_exit, true),

    ?info("[ space file events ]: Starting main monitor for space '~ts'", [SpaceId]),

    SinceSeq = dbsync_state:get_seq(SpaceId, oneprovider:get_id()),
    ChangesPid = space_files_monitor_common:start_link_changes_stream(SpaceId, SinceSeq),

    State = #state{
        space_id = SpaceId,
        space_monitoring_sup_pid = SpaceMonitoringSupPid,

        changes_stream_pid = ChangesPid,
        current_seq = SinceSeq,

        monitoring = #monitoring{}
    },
    {ok, State, ?INACTIVITY_PERIOD_MS}.


-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), state()} |
    {noreply, state()}.
handle_call(#subscribe_req{since_seq = SinceSeq}, _From, State = #state{current_seq = CurrentSeq}) when
    is_integer(SinceSeq) andalso CurrentSeq > SinceSeq
->
    %% Client is behind - reject and tell to start catching
    reply({error, {main_ahead, CurrentSeq}}, State);

handle_call(SubscribeReq = #subscribe_req{}, _From, State) ->
    case space_files_monitor_common:add_observer(State#state.monitoring, SubscribeReq) of
        {ok, NewMonitoring} ->
            reply(ok, State#state{monitoring = NewMonitoring});
        {error, _} = Error ->
            reply(Error, State)
    end;

handle_call(#docs_change_notification{docs = ChangedDocs}, From, State) ->
    gen_server2:reply(From, ok),

    {NewSeq, NewMonitoring} = space_files_monitor_common:process_docs(
        ChangedDocs, State#state.monitoring
    ),
    NewState = State#state{
        current_seq = NewSeq,
        monitoring = NewMonitoring
    },
    noreply(NewState);

handle_call(Request, _From, #state{} = State) ->
    ?log_bad_request(Request),
    noreply(State).


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

handle_info(?SHUTDOWN_INACTIVE_REQ, State = #state{}) ->
    case is_active(State) of
        true ->
            noreply(State);
        false ->
            ?info(
                "[ space file events ]: Stopping monitor for space '~ts' due to inactivity "
                "(no observers and no catching monitors)",
                [State#state.space_id]
            ),
            {stop, {shutdown, timeout}, State}
    end;

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
    {reply, Response, verify_activity(State)}.


%% @private
-spec noreply(state()) -> {noreply, state()}.
noreply(State) ->
    {noreply, verify_activity(State)}.


%% @private
-spec verify_activity(state()) -> state().
verify_activity(State) ->
    case is_active(State) of
        true -> cancel_inactivity_shutdown(State);
        false -> schedule_inactivity_shutdown(State)
    end.


%% @private
-spec is_active(state()) -> boolean().
is_active(State) ->
    case space_files_monitor_common:has_observers(State#state.monitoring) of
        true ->
            true;  %% Has direct observers - active
        false ->
            CatchingSupPid = space_files_monitoring_sup:get_catching_monitors_sup_pid(
                State#state.space_monitoring_sup_pid
            ),
            case space_files_catching_monitors_sup:get_active_children_count(CatchingSupPid) of
                0 -> false;  %% 0 catching monitors - inactive
                _ -> true  %% > 0 catching monitors - active
            end
    end.


%% @private
-spec schedule_inactivity_shutdown(state()) -> state().
schedule_inactivity_shutdown(#state{inactivity_timer = undefined} = State) ->
    State#state{inactivity_timer = erlang:send_after(
        ?INACTIVITY_PERIOD_MS, self(), ?SHUTDOWN_INACTIVE_REQ
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
