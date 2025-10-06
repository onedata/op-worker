%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Per-space supervisor managing main monitor and catching monitors supervisor.
%%% Strategy: rest_for_one (main → catching dependency).
%%% 
%%% Children (in order):
%%%   1. main monitor - space files primary event stream. When it dies,
%%%                     entire supervision for space is terminated.
%%%   2. catching monitors supervisor - manages temporary catch-up streams
%%% @end
%%%-------------------------------------------------------------------
-module(space_files_monitoring_sup).
-author("Bartosz Walkowicz").

-behaviour(supervisor).

%% API
-export([
    spec/1,
    start_link/1,

    get_main_monitor_pid/1,
    get_catching_monitors_sup_pid/1
]).

%% Supervisor callbacks
-export([init/1]).

-opaque id() :: {?MODULE, od_space:id()}.
-export_type([id/0]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec spec(od_space:id()) -> supervisor:child_spec().
spec(SpaceId) ->
    #{
        id => {?MODULE, SpaceId},
        start => {?MODULE, start_link, [SpaceId]},
        % Do not restart supervisor on death - reconnecting client will do this
        restart => temporary,
        shutdown => infinity,
        type => supervisor
    }.


-spec start_link(od_space:id()) -> {ok, pid()} | {error, term()}.
start_link(SpaceId) ->
    supervisor:start_link(?MODULE, [SpaceId]).


-spec get_main_monitor_pid(pid()) -> pid() | undefined.
get_main_monitor_pid(SpaceMonitoringSupPid) ->
    get_child_pid(SpaceMonitoringSupPid, space_files_main_monitor:id()).


-spec get_catching_monitors_sup_pid(pid()) -> pid() | undefined.
get_catching_monitors_sup_pid(SpaceMonitoringSupPid) ->
    get_child_pid(SpaceMonitoringSupPid, space_files_catching_monitors_sup:id()).


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Initializes the per-space supervisor.
%% 
%% CRITICAL: Order matters with rest_for_one strategy!
%% Main monitor MUST be first so its death terminates catching monitors sup.
%% @end
%%--------------------------------------------------------------------
-spec init([od_space:id()]) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([SpaceId]) ->
    SupFlags = #{
        strategy => rest_for_one,
        intensity => 5,
        period => 60
    },
    MainMonitorSpec = space_files_main_monitor:spec(SpaceId, self()),
    CatchingSupSpec = space_files_catching_monitors_sup:spec(SpaceId),

    {ok, {SupFlags, [MainMonitorSpec, CatchingSupSpec]}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_child_pid(pid(), child_id()) -> pid() | undefined.
get_child_pid(SupervisorPid, ChildId) ->
    Children = supervisor:which_children(SupervisorPid),

    case lists:keyfind(ChildId, 1, Children) of
        {ChildId, Pid, _Type, _Modules} when is_pid(Pid) ->
            Pid;
        _ ->
            undefined
    end.
