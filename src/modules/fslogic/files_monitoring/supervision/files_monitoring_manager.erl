%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Manager for space files monitoring lifecycle.
%%% 
%%% Responsibilities:
%%%   - Ensure space monitoring trees exist (atomic operation)
%%%   - Handle inactivity notifications from main monitors
%%%   - Properly terminate space supervisor trees (preventing zombie processes)
%%%   - Handle races (e.g., subscription during teardown)
%%% 
%%% The manager is registered under its module name and is started as a child
%%% of files_monitoring_sup.
%%% @end
%%%-------------------------------------------------------------------
-module(files_monitoring_manager).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("http/space_file_events_stream.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    spec/0,
    start_link/0,

    try_subscribe/2,
    handle_monitor_exit/3,

    notify_inactive/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

-record(state, {}).
-type state() :: #state{}.

-record(subscription, {
    monitor_type :: main | catching,
    main_pid :: pid(),
    catching_pid :: pid() | undefined
}).
-opaque subscription() :: #subscription{}.

-export_type([subscription/0]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec spec() -> supervisor:child_spec().
spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => timer:seconds(5),
        type => worker
    }.


-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).


-spec try_subscribe(od_space:id(), space_files_monitor_common:subscribe_req()) ->
    {ok, subscription()} | errors:error().
try_subscribe(SpaceId, SubscribeReq) ->
    space_files_monitor_common:call_monitor(?MODULE, {try_subscribe, SpaceId, SubscribeReq}).


%%--------------------------------------------------------------------
%% @doc
%% Handles EXIT signal from a monitor process.
%%
%% Interprets the exit reason and updates subscription or signals
%% that the handler should terminate.
%% @end
%%--------------------------------------------------------------------
-spec handle_monitor_exit(pid(), term(), subscription()) ->
    {ok, subscription()} | stop.
handle_monitor_exit(ExitPid, _Reason, #subscription{main_pid = ExitPid}) ->
    %% EXIT from main monitor - always fatal
    stop;

handle_monitor_exit(
    ExitPid,
    {shutdown, caught_up},
    Subscription = #subscription{monitor_type = catching, catching_pid = ExitPid}
) ->
    %% EXIT from catching monitor after successful takeover
    {ok, Subscription#subscription{monitor_type = main, catching_pid = undefined}};

handle_monitor_exit(ExitPid, _Reason, #subscription{catching_pid = ExitPid}) ->
    %% EXIT from catching monitor before catching to main monitor
    stop.


-spec notify_inactive(od_space:id()) -> ok.
notify_inactive(SpaceId) ->
    gen_server2:cast(?MODULE, {inactive_space, SpaceId}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


-spec init([]) -> {ok, state()}.
init([]) ->
    ?info("[ space file events ]: Starting files monitoring manager"),
    {ok, #state{}}.


-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), state()}.
handle_call({try_subscribe, SpaceId, SubscribeReq}, _From, State) ->
    SpaceSupPid = files_monitoring_sup:ensure_monitoring_tree_for_space(SpaceId),
    Result = do_try_subscribe(SpaceSupPid, SubscribeReq),
    {reply, Result, State};

handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, {error, unknown_request}, State}.


-spec handle_cast(Request :: term(), state()) ->
    {noreply, state()}.
handle_cast({inactive_space, SpaceId}, State) ->
    %% Race handling: Verify inactivity before terminating
    %% Between the time main monitor sent notification and now, a new subscriber
    %% might have connected, making the monitor active again.
    SpaceMonitoringSup = files_monitoring_sup:find_sup_for_space(SpaceId),
    case should_terminate_monitoring_tree(SpaceMonitoringSup) of
        true -> terminate_space_monitoring_tree(SpaceId);
        false -> ok
    end,
    {noreply, State};

handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.


-spec handle_info(Info :: timeout() | term(), state()) ->
    {noreply, state()}.
handle_info(Info, State) ->
    ?log_bad_request(Info),
    {noreply, State}.


-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()), state()) ->
    term().
terminate(Reason, State) ->
    ?log_terminate(Reason, State).


-spec code_change(OldVsn :: term() | {down, term()}, state(), Extra :: term()) ->
    {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec do_try_subscribe(pid(), space_files_monitor_common:subscribe_req()) ->
    {ok, subscription()} | errors:error().
do_try_subscribe(SpaceSupPid, SubscribeReq) ->
    MainMonitorPid = space_files_monitoring_sup:get_main_monitor_pid(SpaceSupPid),
    case space_files_main_monitor:try_subscribe(MainMonitorPid, SubscribeReq) of
        ok ->
            %% Client is caught up - connected to main
            {ok, #subscription{
                monitor_type = main,
                main_pid = MainMonitorPid,
                catching_pid = undefined
            }};

        {error, {main_ahead, UntilSeq}} ->
            start_catching_monitor(SpaceSupPid, MainMonitorPid, SubscribeReq, UntilSeq);

        {error, _} = Error ->
            Error
    end.


%% @private
-spec start_catching_monitor(pid(), pid(), space_files_monitor_common:subscribe_req(), couchbase_changes:seq()) ->
    {ok, subscription()} | errors:error().
start_catching_monitor(SpaceSupPid, MainMonitorPid, SubscribeReq, UntilSeq) ->
    case space_files_catching_monitors_sup:start_catching_monitor(
        space_files_monitoring_sup:get_catching_monitors_sup_pid(SpaceSupPid),
        MainMonitorPid,
        SubscribeReq#subscribe_req{until_seq = UntilSeq}
    ) of
        {ok, CatchingPid} ->
            {ok, #subscription{
                monitor_type = catching,
                main_pid = MainMonitorPid,
                catching_pid = CatchingPid
            }};
        {error, _} = Error ->
            Error
    end.


%% @private
-spec should_terminate_monitoring_tree(undefined | pid()) -> boolean().
should_terminate_monitoring_tree(undefined) ->
    false;
should_terminate_monitoring_tree(SpaceMonitoringSup) ->
    CatchingSupPid = space_files_monitoring_sup:get_catching_monitors_sup_pid(SpaceMonitoringSup),
    case space_files_catching_monitors_sup:get_active_children_count(CatchingSupPid) of
        0 ->
            MainMonitorPid = space_files_monitoring_sup:get_main_monitor_pid(SpaceMonitoringSup),
            case space_files_main_monitor:verify_inactive(MainMonitorPid) of
                {ok, true} ->
                    true;
                _ ->
                    false
            end;
        _ ->
            false
    end.


%% @private
-spec terminate_space_monitoring_tree(od_space:id()) -> ok.
terminate_space_monitoring_tree(SpaceId) ->
    ?info(
        "[ space file events ]: Terminating monitoring tree for space '~ts' due to inactivity",
        [SpaceId]
    ),

    ChildId = space_files_monitoring_sup:id(SpaceId),

    %% Terminate the space supervisor (and all its children via rest_for_one)
    case supervisor:terminate_child(files_monitoring_sup, ChildId) of
        ok ->
            %% Delete the child spec so it can be recreated later
            supervisor:delete_child(files_monitoring_sup, ChildId);
        {error, not_found} ->
            %% Already terminated - this is fine
            ok;
        {error, Reason} ->
            ?error(
                "[ space file events ]: Failed to terminate space monitoring tree for '~ts': ~p",
                [SpaceId, Reason]
            ),
            ok
    end.
