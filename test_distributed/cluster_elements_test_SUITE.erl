%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This test creates many Erlang virtual machines and uses them
%% to test basic elements of oneprovider Erlang cluster.
%%% @end
%%%--------------------------------------------------------------------
-module(cluster_elements_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("cluster_elements/node_manager/task_manager.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([ccm_and_worker_test/1, task_pool_test/1, task_manager_test/1]).

-performance({test_cases, []}).
all() -> [ccm_and_worker_test, task_pool_test, task_manager_test].

%%%===================================================================
%%% Test function
%% ====================================================================

ccm_and_worker_test(Config) ->
    % given
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    % then
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [http_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker1, worker_proxy, call, [dns_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker2, worker_proxy, call, [http_worker, ping])),
    ?assertEqual(pong, rpc:call(Worker2, worker_proxy, call, [dns_worker, ping])).

task_pool_test(Config) ->
    % given
    [W1, W2] = ?config(op_worker_nodes, Config),

    Tasks = [{#task_pool{task = t1}, ?NON_LEVEL, W1}, {#task_pool{task = t2}, ?NODE_LEVEL, W1},
        {#task_pool{task = t3}, ?CLUSTER_LEVEL, W1}, {#task_pool{task = t4}, ?CLUSTER_LEVEL, W2},
        {#task_pool{task = t5}, ?PERSISTENT_LEVEL, W1}, {#task_pool{task = t6}, ?PERSISTENT_LEVEL, W2},
        {#task_pool{task = t7}, ?PERSISTENT_LEVEL, W1}
    ],

    CreateAns = lists:foldl(fun({Task, Level, Worker}, Acc) ->
        {A1, A2} = rpc:call(Worker, task_pool, create, [Level, #document{value = Task#task_pool{owner = self()}}]),
        ?assertMatch({ok, _}, {A1, A2}),
        [{A2, Level} | Acc]
    end, [], Tasks),
    [_ | Keys] = lists:reverse(CreateAns),

    NewNames = [t2_2, t3_2, t4_2, t5_2, t6_2, t7_2],
    ToUpdate = lists:zip(Keys, NewNames),

    lists:foreach(fun({{Key, Level}, NewName}) ->
        ?assertMatch({ok, _}, rpc:call(W1, task_pool, update, [Level, Key, #{task => NewName}]))
    end, ToUpdate),

    ListTest = [{?NODE_LEVEL, [t2_2]}, {?CLUSTER_LEVEL, [t3_2, t4_2]}],
    %TODO - add PERSISTENT_LEVEL checking when list on db will be added
%%     ListTest = [{?NODE_LEVEL, [t2_2]}, {?CLUSTER_LEVEL, [t3_2, t4_2]}, {?PERSISTENT_LEVEL, [t5_2, t6_2, t7_2]}],
    lists:foreach(fun({Level, Names}) ->
        ?assertEqual({ok, []}, rpc:call(W1, task_pool, list_failed, [Level])),
        {A1, ListedTasks} = rpc:call(W1, task_pool, list, [Level]),
        ?assertMatch({ok, _}, {A1, ListedTasks}),
        ?assertEqual(length(Names), length(ListedTasks)),
        lists:foreach(fun(T) ->
            V = T#document.value,
            ?assert(lists:member(V#task_pool.task, Names))
        end, ListedTasks)
    end, ListTest).

task_manager_test(Config) ->
    % given
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).