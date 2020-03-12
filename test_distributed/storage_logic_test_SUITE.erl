%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests storage logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_logic_test_SUITE).
-author("Michal Stanisz").

-include("logic_tests_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    get_test/1,
    subscribe_test/1
]).

all() -> ?ALL([
    get_test,
    subscribe_test
]).


%%%===================================================================
%%% Test functions
%%%===================================================================

get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?STORAGE_PRIVATE_DATA_MATCHER(?STORAGE_1)},
        rpc:call(Node, storage_logic, get, [?STORAGE_1])
    ),

    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Storage private data should now be cached
    ?assertMatch(
        {ok, ?STORAGE_PRIVATE_DATA_MATCHER(?STORAGE_1)},
        rpc:call(Node, storage_logic, get, [?STORAGE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider can access non-cached data
    logic_tests_common:invalidate_cache(Config, od_storage, ?STORAGE_1),
    ?assertMatch(
        {ok, ?STORAGE_PRIVATE_DATA_MATCHER(?STORAGE_1)},
        rpc:call(Node, storage_logic, get, [?STORAGE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    logic_tests_common:invalidate_cache(Config, od_storage, ?STORAGE_1),
    % Simulate received updates
    Storage1PrivateGRI = #gri{type = od_storage, id = ?STORAGE_1, aspect = instance, scope = private},
    Storage1PrivateData = ?STORAGE_PRIVATE_DATA_VALUE(?STORAGE_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % private scope
    ?assertMatch(
        {ok, ?STORAGE_PRIVATE_DATA_MATCHER(?STORAGE_1)},
        rpc:call(Node, storage_logic, get, [?STORAGE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    NewQosParams = #{<<"key">> => <<"value">>},
    ChangedData1 = Storage1PrivateData#{
        <<"qos_parameters">> => NewQosParams,
        <<"revision">> => 6
    },
    PushMessage1 = #gs_push_graph{gri = Storage1PrivateGRI, data = ChangedData1, change_type = updated},
    logic_tests_common:simulate_push(Config, PushMessage1),

    ?assertMatch(
        {ok, #document{key = ?STORAGE_1, value = #od_storage{
            qos_parameters = NewQosParams,
            cache_state = #{revision := 6}
        }}},
        rpc:call(Node, storage_logic, get, [?STORAGE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),


    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage4 = #gs_push_graph{gri = Storage1PrivateGRI, change_type = deleted},
    logic_tests_common:simulate_push(Config, PushMessage4),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_storage, get_from_cache, [?STORAGE_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    logic_tests_common:invalidate_cache(Config, od_storage, ?STORAGE_1),
    ?assertMatch(
        {ok, ?STORAGE_PRIVATE_DATA_MATCHER(?STORAGE_1)},
        rpc:call(Node, storage_logic, get, [?STORAGE_1])
    ),

    PushMessage5 = #gs_push_nosub{gri = Storage1PrivateGRI, reason = forbidden},
    logic_tests_common:simulate_push(Config, PushMessage5),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_storage, get_from_cache, [?STORAGE_1])
    ),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        logic_tests_common:mock_gs_client(NewConfig),
        NewConfig
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [logic_tests_common, initializer]} | Config].

init_per_testcase(_, Config) ->
    logic_tests_common:init_per_testcase(Config).

end_per_testcase(_, _Config) ->
    ok.

end_per_suite(Config) ->
    logic_tests_common:unmock_gs_client(Config),
    ok.
