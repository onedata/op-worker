%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% OTP supervisor for space_files_monitor gen_servers.
%%% @end
%%%-------------------------------------------------------------------
-module(space_files_monitor_sup).
-author("Bartosz Walkowicz").

-behaviour(supervisor).

-include("modules/harvesting/harvesting.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([spec/0, start_link/0, ensure_monitor_started/1]).

%% Exported for rpc
-export([do_ensure_monitor_started/1]).

%% Supervisor callbacks
-export([init/1]).


-define(SPACE_FILES_MONITOR_SUP, ?MODULE).
-define(SPACE_FILES_MONITOR(__SPACE_ID), {space_files_monitor, __SPACE_ID}).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec spec() -> supervisor:child_spec().
spec() ->
    #{
        id => ?SPACE_FILES_MONITOR_SUP,
        start => {?MODULE, start_link, []},
        restart => transient,
        shutdown => infinity,
        type => supervisor
    }.


-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    supervisor:start_link({local, ?SPACE_FILES_MONITOR_SUP}, ?MODULE, []).


-spec ensure_monitor_started(od_space:id()) -> pid().
ensure_monitor_started(SpaceId) ->
    Node = datastore_key:any_responsible_node(SpaceId),

    case node() of
        Node -> do_ensure_monitor_started(SpaceId);
        _ -> erpc:call(Node, ?MODULE, do_ensure_monitor_started, [SpaceId])
    end.


-spec do_ensure_monitor_started(od_space:id()) -> pid().
do_ensure_monitor_started(SpaceId) ->
    case supervisor:start_child(?SPACE_FILES_MONITOR_SUP, space_files_monitor_spec(SpaceId)) of
        {ok, Pid} ->
            Pid;
        {error, already_present} ->
            supervisor:delete_child(?SPACE_FILES_MONITOR_SUP, ?SPACE_FILES_MONITOR(SpaceId)),
            do_ensure_monitor_started(SpaceId);
        {error, {already_started, Pid}} ->
            Pid
    end.


%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}).
init([]) ->
    {ok, {#{strategy => one_for_one, intensity => 10, period => 3600}, []}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec space_files_monitor_spec(od_space:id()) -> supervisor:child_spec().
space_files_monitor_spec(SpaceId) ->
    #{
        id => ?SPACE_FILES_MONITOR(SpaceId),
        start => {space_files_monitor, start_link, [SpaceId]},
        restart => transient,
        shutdown => timer:seconds(5),
        type => worker
    }.
