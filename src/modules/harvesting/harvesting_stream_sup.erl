%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% OTP supervisor for harvesting_stream gen_servers.
%%% @end
%%%-------------------------------------------------------------------
-module(harvesting_stream_sup).
-author("Jakub Kudzia").

-behaviour(supervisor).

-include("modules/harvesting/harvesting.hrl").

%% API
-export([start_link/0,
    start_main_stream/1, terminate_main_stream/1,
    start_aux_stream_async/4, terminate_aux_stream/3
]).

%% Supervisor callbacks
-export([init/1]).

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
    supervisor:start_link({local, ?HARVESTING_STREAM_SUP}, ?MODULE, []).

-spec start_main_stream(od_space:id()) -> ok.
start_main_stream(SpaceId) ->
    case supervisor:start_child(?HARVESTING_STREAM_SUP, main_stream_spec(SpaceId)) of
        {ok, _} -> ok;
        {error, already_present} ->
            supervisor:delete_child(?HARVESTING_STREAM_SUP, ?MAIN_HARVESTING_STREAM(SpaceId)),
            start_main_stream(SpaceId);
        {error, {already_started, _}} -> ok;
        {error, {{already_started, _}, _}} -> ok
    end.

start_aux_stream_async(SpaceId, HarvesterId, IndexId, Until) ->
    spawn_link(fun() -> start_aux_stream(SpaceId, HarvesterId, IndexId, Until) end).

-spec terminate_main_stream(od_space:id()) -> ok | {error, term()}.
terminate_main_stream(SpaceId) ->
    MainStream = ?MAIN_HARVESTING_STREAM(SpaceId),
    ok = supervisor:terminate_child(?HARVESTING_STREAM_SUP, MainStream),
    ok = supervisor:delete_child(?HARVESTING_STREAM_SUP, MainStream).

-spec terminate_aux_stream(od_space:id(), od_harvester:id(), od_harvester:index()) ->
    ok | {error, term()}.
terminate_aux_stream(SpaceId, HarvesterId, IndexId) ->
    AuxStream = ?AUX_HARVESTING_STREAM(SpaceId, HarvesterId, IndexId),
    ok = supervisor:terminate_child(?HARVESTING_STREAM_SUP, AuxStream),
    ok = supervisor:delete_child(?HARVESTING_STREAM_SUP, AuxStream).

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

-spec main_stream_spec(od_space:id()) -> supervisor:child_spec().
main_stream_spec(SpaceId) ->
    #{
        id => ?MAIN_HARVESTING_STREAM(SpaceId),
        start => {harvesting_stream, start_link, [main_harvesting_stream, [SpaceId]]},
        restart => transient,
        shutdown => timer:seconds(100),
        type => worker
    }.

-spec aux_stream_spec(od_space:id(), od_harvester:id(), od_harvester:index(),
    couchbase_changes:until()) -> supervisor:child_spec().
aux_stream_spec(SpaceId, HarvesterId, IndexId, Until) ->
    #{
        id => ?AUX_HARVESTING_STREAM(SpaceId, HarvesterId, IndexId),
        start => {harvesting_stream, start_link, [aux_harvesting_stream, [SpaceId, HarvesterId, IndexId, Until]]},
        restart => transient,
        shutdown => timer:seconds(5),
        type => worker
    }.


-spec start_aux_stream(od_space:id(), od_harvester:id(), od_harvester:index(),
    [couchbase_changes:until()]) -> ok.
start_aux_stream(SpaceId, HarvesterId, IndexId, Until) ->
    AuxStream = ?AUX_HARVESTING_STREAM(SpaceId, HarvesterId, IndexId),
    case supervisor:start_child(?HARVESTING_STREAM_SUP,
        aux_stream_spec(SpaceId, HarvesterId, IndexId, Until)) of
        {error, already_present} ->
            ok = supervisor:delete_child(?HARVESTING_STREAM_SUP, AuxStream),
            start_aux_stream(SpaceId, HarvesterId, IndexId, Until);
        {ok, _} ->
            ok
    end.