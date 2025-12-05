%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Supervisor for space files monitoring replay monitors (temporary monitors
%%% for reconnecting clients).
%%% Strategy: simple_one_for_one (all children are replay monitors).
%%% 
%%% Each replay monitor:
%%%   - Replays events from FromSeq to Until
%%%   - Proposes takeover to main monitor when caught up
%%%   - Dies after successful takeover
%%% @end
%%%-------------------------------------------------------------------
-module(space_files_replay_monitors_sup).
-author("Bartosz Walkowicz").

-behaviour(supervisor).

-include_lib("ctool/include/logging.hrl").

%% API
-export([
    id/0,
    spec/1,
    start_link/1,

    start_replay_monitor/3,
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
        shutdown => infinity,  % Wait for all replay monitors to terminate
        type => supervisor
    }.


-spec start_link(od_space:id()) -> {ok, pid()} | {error, term()}.
start_link(SpaceId) ->
    supervisor:start_link(?MODULE, [SpaceId]).


-spec start_replay_monitor(pid(), pid(), space_files_monitor_common:subscribe_req()) ->
    {ok, pid()} | errors:error().
start_replay_monitor(SupervisorPid, MainMonitorPid, SubscribeReq) ->
    case supervisor:start_child(SupervisorPid, [MainMonitorPid, SubscribeReq]) of
        {ok, _} = Result ->
            Result;
        {error, Reason} ->
            ?report_internal_server_error("Failed to start replay monitor due to: ~tp", [Reason])
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
    ReplayMonitorSpec = space_files_replay_monitor:spec(SpaceId),

    {ok, {SupFlags, [ReplayMonitorSpec]}}.
