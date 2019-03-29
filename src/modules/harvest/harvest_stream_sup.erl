%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% OTP supervisor for harvest_stream gen_servers.
%%% @end
%%%-------------------------------------------------------------------
-module(harvest_stream_sup).
-author("Jakub Kudzia").

-behaviour(supervisor).

-include("modules/harvest/harvest.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_link/0, start_child/3, terminate_child/3]).

%% Supervisor callbacks
-export([init/1]).

-type child_id() :: supervisor:child_id().

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    supervisor:start_link({local, ?HARVEST_STREAM_SUP}, ?MODULE, []).

-spec start_child(od_harvester:id(), od_space:id(), od_harvester:index()) -> ok.
start_child(HarvesterId, SpaceId, IndexId) ->
    {ok, _} = supervisor:start_child(?HARVEST_STREAM_SUP, child_spec(HarvesterId, SpaceId, IndexId)),
    ok.

-spec terminate_child(od_harvester:id(), od_space:id(), od_harvester:index()) ->
    ok | {error, term()}.
terminate_child(HarvesterId, SpaceId, IndexId) ->
    ChildId = child_id(HarvesterId, SpaceId, IndexId),
    ok = supervisor:terminate_child(?HARVEST_STREAM_SUP, ChildId),
    ok = supervisor:delete_child(?HARVEST_STREAM_SUP, ChildId).

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
    {ok, {#{strategy => one_for_one, intensity => 1000, period => 3600}, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec child_id(od_harvester:id(), od_space:id(), od_harvester:index()) ->
    child_id().
child_id(HarvesterId, SpaceId, IndexId) ->
    {HarvesterId, SpaceId, IndexId}.

-spec child_spec(od_harvester:id(), od_space:id(), od_harvester:index()) ->
    supervisor:child_spec().
child_spec(HarvesterId, SpaceId, IndexId) ->
    #{
        id => child_id(HarvesterId, SpaceId, IndexId),
        start => {harvest_stream, start_link, [HarvesterId, SpaceId, IndexId]},
        restart => transient,
        shutdown => timer:seconds(100),
        type => worker
    }.

