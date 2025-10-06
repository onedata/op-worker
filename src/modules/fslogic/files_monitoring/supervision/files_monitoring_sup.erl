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

-include_lib("ctool/include/logging.hrl").

%% API
-export([
    spec/0,
    start_link/0,

    ensure_monitoring_tree_for_space/1
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


-spec ensure_monitoring_tree_for_space(od_space:id()) -> {ok, pid()} | errors:error().
ensure_monitoring_tree_for_space(SpaceId) ->
    ChildSpec = space_files_monitoring_sup:spec(SpaceId),

    case supervisor:start_child(?ID, ChildSpec) of
        {ok, _Pid} = Result ->
            Result;
        {error, already_present} ->
            % When monitor dies naturally (due to inactivity) it is not restarted but its
            % spec is also not removed from supervisor (one_for_one supervisor behaviour)
            % - it needs to be done manually before starting it anew
            supervisor:delete_child(?ID, maps:get(id, ChildSpec)),
            ensure_monitoring_tree_for_space(SpaceId);
        {error, {already_started, Pid}} ->
            {ok, Pid}
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
    {ok, {SupFlags, []}}.
