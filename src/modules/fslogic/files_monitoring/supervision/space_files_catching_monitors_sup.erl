%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Supervisor for space files monitoring catching monitors (temporary monitors
%%% for reconnecting clients).
%%% Strategy: simple_one_for_one (all children are catching monitors).
%%% 
%%% Each catching monitor:
%%%   - Replays events from FromSeq to Until
%%%   - Proposes takeover to main monitor when caught up
%%%   - Dies after successful takeover
%%% @end
%%%-------------------------------------------------------------------
-module(space_files_catching_monitors_sup).
-author("Bartosz Walkowicz").

-behaviour(supervisor).

-include_lib("ctool/include/logging.hrl").

%% API
-export([
    id/0,
    spec/1,
    start_link/1,

    start_catching_monitor/3,
    get_active_children_count/1
]).

%% Supervisor callbacks
-export([init/1]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec id() -> ?MODULE.
id() -> ?MODULE.


-spec spec(od_space:id()) ->
    supervisor:child_spec().
spec(SpaceId) ->
    #{
        id => id(),
        start => {?MODULE, start_link, [SpaceId]},
        restart => permanent,
        shutdown => infinity,  % Wait for all catching monitors to terminate
        type => supervisor
    }.


-spec start_link(od_space:id()) -> {ok, pid()} | {error, term()}.
start_link(SpaceId) ->
    supervisor:start_link(?MODULE, [SpaceId]).


-spec start_catching_monitor(pid(), pid(), space_files_monitor_common:subscribe_req()) ->
    {ok, pid()} | errors:error().
start_catching_monitor(SupervisorPid, MainMonitorPid, SubscribeReq) ->
    case supervisor:start_child(SupervisorPid, [MainMonitorPid, SubscribeReq]) of
        {ok, _} = Result ->
            Result;
        {error, Reason} ->
            ?report_internal_server_error("Failed to start catching monitor due to: ~tp", [Reason])
    end.


-spec get_active_children_count(pid()) -> non_neg_integer().
get_active_children_count(SupervisorPid) ->
    {active, Count} = lists:keyfind(active, 1, supervisor:count_children(SupervisorPid)),
    Count.


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================


-spec init([od_space:id()]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([SpaceId]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 60
    },
    CatchingMonitorSpec = space_files_catching_monitor:spec(SpaceId),

    {ok, {SupFlags, [CatchingMonitorSpec]}}.
