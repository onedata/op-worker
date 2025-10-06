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
%%% TODO: ensure couchbase docs changes do not prevent shutdown due to inactivity (such messages can disrupt inactivity timeout)
%%% @end
%%%-------------------------------------------------------------------
-module(space_files_main_monitor).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("http/space_file_events_stream.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    id/0,
    spec/2,
    start_link/2,

    try_subscribe/2,
    accept_takeover/5
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

    monitoring :: space_files_monitor_common:monitoring()
}).
-type state() :: #state{}.


%% The process is supposed to die after ?INACTIVITY_PERIOD_MS time of idling (no subscribers)
-define(INACTIVITY_PERIOD_MS, 10_000).


%%%===================================================================
%%% API
%%%===================================================================


-spec id() -> id().
id() ->
    ?MODULE.


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
    ok | {error, {behind, couchbase_changes:seq()}} | errors:error().
try_subscribe(MonitorPid, SubscribeReq) ->
    space_files_monitor_common:call_monitor(MonitorPid, SubscribeReq).


%%--------------------------------------------------------------------
%% TODO: finish takeover
%% @doc
%% Accepts a takeover from a catching monitor.
%% Called by catching monitor when it has caught up.
%% @end
%%--------------------------------------------------------------------
-spec accept_takeover(
    pid(),
    pid(),
    session:id(),
    space_files_monitoring_spec:t(),
    couchbase_changes:seq()
) ->
    takeover_accepted | {takeover_rejected, couchbase_changes:seq()}.
accept_takeover(MainPid, HandlerPid, SessionId, Spec, CatchingSeq) ->
    gen_server2:call(MainPid, {accept_takeover, HandlerPid, SessionId, Spec, CatchingSeq}, infinity).


%%%===================================================================
%%% gen_server2 callbacks
%%%===================================================================


-spec init([od_space:id() | pid()]) -> {ok, state(), non_neg_integer()}.
init([SpaceId, SpaceMonitoringSupPid]) ->
    process_flag(trap_exit, true),

    ?info("[ space file events ]: Starting monitor for space '~ts'", [SpaceId]),

    SinceSeq = dbsync_state:get_seq(SpaceId, oneprovider:get_id()),
    {ok, ChangesPid} = space_files_monitor_common:start_link_changes_stream(
        SpaceId, SinceSeq
    ),

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
    {reply, Reply :: term(), state(), non_neg_integer()} |
    {noreply, state()} |
    {noreply, state(), non_neg_integer()}.
handle_call(#subscribe_req{since_seq = SinceSeq}, _From, State = #state{current_seq = CurrentSeq}) when
    is_integer(SinceSeq) andalso CurrentSeq > SinceSeq
->
    %% Client is behind - reject and tell to start catching
    {reply, {error, {seq_behind, CurrentSeq}}, State, ?INACTIVITY_PERIOD_MS};

handle_call(SubscribeReq, _From, State) ->
    case space_files_monitor_common:add_observer(State#state.monitoring, SubscribeReq) of
        {ok, NewMonitoring} -> {reply, ok, State#state{monitoring = NewMonitoring}};
        ?ERR = Error -> {reply, Error, State, ?INACTIVITY_PERIOD_MS}
    end;

%% TODO: finish takeover
handle_call({accept_takeover, HandlerPid, SessionId, Spec, CatchingSeq}, _From, State) ->
    CurrentSeq = State#state.current_seq,
    
    case CurrentSeq of
        CatchingSeq ->
            %% Sequences match - accept takeover
            try
                erlang:link(HandlerPid),
                Observer = #observer{session_id = SessionId, files_monitoring_spec = Spec},
                NewState = space_files_monitor_common:add_observer(State, HandlerPid, Observer),
                {reply, takeover_accepted, NewState}
            catch
                error:noproc ->
                    %% Handler died before takeover completed
                    ?debug("Handler ~p died before takeover", [HandlerPid]),
                    {reply, takeover_accepted, State}
            end;
        CurrentSeq when CurrentSeq > CatchingSeq ->
            %% Main is ahead - reject
            {reply, {takeover_rejected, CurrentSeq}, State};
        _ ->
            %% Main is behind?! Shouldn't happen
            ?error("Main behind catching: current=~p, catching=~p", [CurrentSeq, CatchingSeq]),
            {reply, {takeover_rejected, CurrentSeq}, State}
    end;

handle_call(#docs_change_notification{docs = ChangedDocs}, From, State) ->
    gen_server2:reply(From, ok),

    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    lists:foreach(fun(ChangedDoc) ->
        try
            space_files_monitor_common:process_doc(RootUserCtx, ChangedDoc, State#state.monitoring)
        catch Class:Reason:Stacktrace ->
            ?error_exception("[ space file events ]: Failed to process doc ", Class, Reason, Stacktrace)
        end
    end, ChangedDocs),

    {noreply, State, ?INACTIVITY_PERIOD_MS};

handle_call(Request, _From, #state{} = State) ->
    ?log_bad_request(Request),
    {noreply, State, ?INACTIVITY_PERIOD_MS}.


-spec handle_cast(Request :: term(), state()) ->
    {noreply, state()} |
    {noreply, state(), non_neg_integer()}.
handle_cast(Request, #state{} = State) ->
    ?log_bad_request(Request),
    {noreply, State, ?INACTIVITY_PERIOD_MS}.


-spec handle_info(timeout() | term(), state()) ->
    {noreply, state()} |
    {noreply, state(), non_neg_integer()} |
    {stop, term(), state()}.
handle_info({'EXIT', ObserverPid, _Reason}, State = #state{}) ->
    NewState = State#state{
        monitoring = space_files_monitor_common:remove_observer(State#state.monitoring, ObserverPid)
    },
    {noreply, NewState, ?INACTIVITY_PERIOD_MS};

handle_info(stream_ended, State = #state{}) ->
    ?error(
        "[ space file events ]: Couchbase changes stream ended for main monitor space '~ts'",
        [State#state.space_id]
    ),
    {stop, {shutdown, stream_ended}, State};

handle_info(timeout, State = #state{}) ->
    case should_timeout(State) of
        true ->
            ?info(
                "[ space file events ]: Stopping monitor for space '~ts' due to inactivity "
                "(no observers and no catching monitors)",
                [State#state.space_id]
            ),
            {stop, {shutdown, timeout}, State};
        false ->
            {noreply, State}
    end;

handle_info(Info, #state{} = State) ->
    ?log_bad_request(Info),
    {noreply, State, ?INACTIVITY_PERIOD_MS}.


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


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if monitor should shutdown due to inactivity.
%% Main monitor exits only when:
%%   1. No direct observers (all clients disconnected)
%%   2. No catching monitors exist (no one is catching up)
%% @end
%%--------------------------------------------------------------------
-spec should_timeout(state()) -> boolean().
should_timeout(State) ->
    case space_files_monitor_common:has_observers(State#state.monitoring) of
        true ->
            false;
        false ->
            CatchingSupPid = space_files_monitoring_sup:get_catching_monitors_sup_pid(
                State#state.space_monitoring_sup_pid
            ),
            case space_files_catching_monitors_sup:get_active_children_count(CatchingSupPid) of
                0 -> true;
                _ -> false
            end
    end.
