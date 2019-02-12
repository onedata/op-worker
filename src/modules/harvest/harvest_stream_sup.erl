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
-export([start_link/0, start_child/2, terminate_child/2]).

%% Supervisor callbacks
-export([init/1]).

-type child_id() :: supervisor:child_id().

-define(SERVER, ?HARVEST_STREAM_SUP).

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
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec start_child(od_harvester:id(), od_space:id()) -> ok.
start_child(HarvesterId, SpaceId) ->
    {ok, _} = supervisor:start_child(?SERVER, child_spec(HarvesterId, SpaceId)),
    ok.

-spec terminate_child(od_harvester:id(), od_space:id()) -> ok | {error, term()}.
terminate_child(HarvesterId, SpaceId) ->
    ChildId = child_id(HarvesterId, SpaceId),
    ok = supervisor:terminate_child(?SERVER, ChildId),
    ok = supervisor:delete_child(?SERVER, ChildId).

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

-spec child_id(od_harvester:id(), od_space:id()) -> child_id().
child_id(HarvesterId, SpaceId) ->
    {HarvesterId, SpaceId}.

-spec child_spec(od_harvester:id(), od_space:id()) -> supervisor:child_spec().
child_spec(HarvesterId, SpaceId) ->
    #{
        id => child_id(HarvesterId, SpaceId),
        start => {harvest_stream, start_link, [HarvesterId, SpaceId]},
        restart => transient,
        shutdown => timer:seconds(100),
        type => worker
    }.

