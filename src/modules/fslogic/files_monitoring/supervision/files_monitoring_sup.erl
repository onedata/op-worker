%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Top-level supervisor for files monitoring.
%%% Manages per-space monitor supervisors.
%%% @end
%%%-------------------------------------------------------------------
-module(files_monitoring_sup).
-author("Bartosz Walkowicz").

-behaviour(supervisor).

%% API
-export([
    spec/0,
    start_link/0,

    ensure_monitoring_tree_for_space/1,
    find_sup_for_space/1
]).

%% Supervisor callbacks
-export([init/1]).


-define(ID, ?MODULE).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec spec() -> supervisor:child_spec().
spec() ->
    #{
        id => ?ID,
        start => {?MODULE, start_link, []},
        restart => transient,
        shutdown => infinity,
        type => supervisor
    }.


-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?ID}, ?MODULE, []).


-spec ensure_monitoring_tree_for_space(od_space:id()) -> pid().
ensure_monitoring_tree_for_space(SpaceId) ->
    ChildSpec = space_files_monitoring_sup:spec(SpaceId),

    case supervisor:start_child(?ID, ChildSpec) of
        {ok, Pid} = _Result ->
            Pid;
        {error, {already_started, Pid}} ->
            % Space supervisor already running with healthy main monitor
            Pid
    end.


-spec find_sup_for_space(od_space:id()) -> pid() | undefined.
find_sup_for_space(SpaceId) ->
    ChildId = space_files_monitoring_sup:id(SpaceId),
    Children = supervisor:which_children(?ID),

    case lists:keyfind(ChildId, 1, Children) of
        {ChildId, Pid, _Type, _Modules} -> Pid;
        _ -> undefined
    end.


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================


-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    SupFlags = #{
        % if one space supervisor fails, others continue
        strategy => one_for_one,
        intensity => 10,
        period => 3600
    },
    ManagerSpec = files_monitoring_manager:spec(),
    {ok, {SupFlags, [ManagerSpec]}}.
