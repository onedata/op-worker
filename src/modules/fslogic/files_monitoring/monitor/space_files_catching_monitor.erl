%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Catching monitor for space file events - temporary monitor that replays
%%% historical events for reconnecting clients.
%%% 
%%% Responsibilities:
%%%   - Replay events from SinceSeq to UntilSeq
%%%   - Propose takeover to main monitor when caught up
%%%   - Retry if main advanced during takeover
%%%   - Die gracefully after successful takeover
%%% 
%%% NOTE: Only one catching monitor per reconnecting client.
%%% NOTE: Catching monitor doesn't trap exit as it only manages single subscription -
%%% when it dies, the observer should get the 'EXIT' signal
%%% @end
%%%-------------------------------------------------------------------
-module(space_files_catching_monitor).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("http/space_file_events_stream.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([id/0, spec/1, start_link/3]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).


-record(state, {
    space_id :: od_space:id(),
    main_monitor_pid :: pid(),

    changes_stream_pid :: pid() | undefined,
    until_seq :: couchbase_changes:seq(),

    monitoring :: space_files_monitor_common:monitoring()
}).
-type state() :: #state{}.


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
        restart => temporary,  % Don't restart - one-time catch-up
        shutdown => timer:seconds(5),
        type => worker,
        modules => [?MODULE]
    }.


-spec start_link(od_space:id(), pid(), space_files_monitor_common:subscribe_req()) ->
    {ok, pid()} | {error, term()}.
start_link(SpaceId, MainMonitorPid, SubscribeReq) ->
    gen_server2:start_link(?MODULE, [SpaceId, MainMonitorPid, SubscribeReq], []).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


-spec init([od_space:id() | pid() | space_files_monitor_common:subscribe_req()]) ->
    {ok, state()}.
init([SpaceId, MainMonitorPid, SubscribeReq]) ->
    process_flag(trap_exit, true),

    SinceSeq = SubscribeReq#subscribe_req.since_seq,
    UntilSeq = SubscribeReq#subscribe_req.until_seq,
    ?info("[ space file events ]: Starting catching monitor for space '~ts' from seq ~B", [
        SpaceId, SinceSeq
    ]),

    ChangesPid = space_files_monitor_common:start_link_changes_stream(SpaceId, SinceSeq),

    {ok, Monitoring} = space_files_monitor_common:add_observer(
        #monitoring{current_seq = SinceSeq},
        SubscribeReq
    ),

    {ok, #state{
        space_id = SpaceId,
        main_monitor_pid = MainMonitorPid,

        changes_stream_pid = ChangesPid,
        until_seq = UntilSeq,

        monitoring = Monitoring
    }}.


-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {noreply, state()} |
    {stop, {shutdown, caught_up}, state()}.
handle_call(#docs_change_notification{docs = ChangedDocs}, From, State) ->
    gen_server2:reply(From, ok),

    NewState = State#state{
        monitoring = space_files_monitor_common:process_docs(
            ChangedDocs, State#state.monitoring
        )
    },
    case has_reached_target_seq(NewState) of
        true -> propose_takeover(NewState);
        false -> {noreply, NewState}
    end;

handle_call(Request, _From, #state{} = State) ->
    ?log_bad_request(Request),
    {reply, {error, unknown_request}, State}.


-spec handle_cast(Request :: term(), state()) ->
    {noreply, state()}.
handle_cast(Request, #state{} = State) ->
    ?log_bad_request(Request),
    {noreply, State}.


-spec handle_info(timeout() | term(), state()) ->
    {noreply, state()} |
    {stop, term(), state()}.
handle_info({'EXIT', _ObserverPid, _Reason}, State = #state{space_id = SpaceId}) ->
    ?error("[ space file events ]: Observer died for catching monitor space '~ts'", [SpaceId]),
    {stop, {shutdown, observer_died}, State};

handle_info(stream_ended, State = #state{}) ->
    ?error(
        "[ space file events ]: Couchbase changes stream ended for catching monitor space '~ts'",
        [State#state.space_id]
    ),
    {stop, {shutdown, stream_ended}, State};

handle_info(Info, #state{} = State) ->
    ?log_bad_request(Info),
    {noreply, State}.


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
-spec has_reached_target_seq(state()) -> boolean().
has_reached_target_seq(State) ->
    CurrentSeq = State#state.monitoring#monitoring.current_seq,
    UntilSeq = State#state.until_seq,

    CurrentSeq >= UntilSeq.


%% @private
-spec propose_takeover(state()) ->
    {noreply, state()} |
    {stop, {shutdown, caught_up}, state()}.
propose_takeover(State) ->
    ?debug("[ space file events ]: Catching monitor reached target sequence, proposing takeover"),

    MainPid = State#state.main_monitor_pid,
    ObserverSubscribeReq = build_subscribe_req(State),

    case space_files_main_monitor:try_subscribe(MainPid, ObserverSubscribeReq) of
        ok ->
            %% Takeover accepted - die gracefully
            {stop, {shutdown, caught_up}, State};

        {error, {main_ahead, MainCurrentSeq}} ->
            %% Main advanced - need to continue catching
            ?debug("[ space file events ]: Takeover rejected, retrying with new target: ~B", [MainCurrentSeq]),

            {noreply, State#state{until_seq = MainCurrentSeq}};

        {error, _} = Error ->
            ?error("[ space file events ]: Takeover failed due to: ~tp, retrying on next docs change", [Error]),
            {noreply, State}
    end.


%% @private
-spec build_subscribe_req(state()) -> space_files_monitor_common:subscribe_req().
build_subscribe_req(#state{monitoring = #monitoring{
    current_seq = SinceSeq,
    observers = Observers
}}) ->
    [{ObserverPid, Observer}] = maps:to_list(Observers),

    #subscribe_req{
        observer_pid = ObserverPid,
        session_id = Observer#observer.session_id,
        files_monitoring_spec = Observer#observer.files_monitoring_spec,
        since_seq = SinceSeq,
        until_seq = undefined
    }.
