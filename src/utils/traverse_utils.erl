%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module containing overlay functions for traverse ensuring that traverses
%%% are not started before zone connection.
%%% @end
%%%-------------------------------------------------------------------
-module(traverse_utils).
-author("Michal Stanisz").

-include_lib("ctool/include/logging.hrl").

%% API
-export([
    init_pool_with_zone_connection/5,
    run/3, run/4
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec init_pool_with_zone_connection(traverse:pool(), non_neg_integer(), non_neg_integer(), non_neg_integer(),
    traverse:pool_options()) -> ok | no_return().
init_pool_with_zone_connection(PoolName, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, Options) ->
    spawn(fun() ->
        wait_for_zone_connection(),
        ok = traverse:init_pool(PoolName, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit, Options),
        ?info("Traverse pool '~ts' started.", [PoolName])
    end),
    ok.


-spec run(traverse:pool(), traverse:id(), traverse:job()) -> ok.
run(PoolName, TaskId, Job) ->
    run(PoolName, TaskId, Job, #{}).


-spec run(traverse:pool(), traverse:id(), traverse:job(), traverse:run_options()) -> ok.
run(PoolName, TaskId, Job, Options) ->
    wait_for_pool_start(PoolName),
    ok = traverse:run(PoolName, TaskId, Job, Options).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec wait_for_zone_connection() -> ok.
wait_for_zone_connection() ->
    utils:wait_until(fun gs_channel_service:is_connected/0, timer:seconds(10), infinity).


-spec wait_for_pool_start(traverse:pool()) -> ok.
wait_for_pool_start(PoolName) ->
    utils:wait_until(fun() -> traverse:is_pool_started(PoolName) end, timer:seconds(10), infinity).
