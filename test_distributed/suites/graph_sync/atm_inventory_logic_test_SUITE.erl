%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests automation inventory logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(atm_inventory_logic_test_SUITE).
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
    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    
    AtmInventoryGriMatcher = #gri{type = od_atm_inventory, id = ?ATM_INVENTORY_1, aspect = instance, _ = '_'},
    
    GraphCalls = logic_tests_common:count_reqs(Config, graph, AtmInventoryGriMatcher),

    ?assertMatch(
        {ok, ?ATM_INVENTORY_PRIVATE_DATA_MATCHER(?ATM_INVENTORY_1)},
        rpc:call(Node, atm_inventory_logic, get, [User1Sess, ?ATM_INVENTORY_1])
    ),

    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, AtmInventoryGriMatcher)),

    % AtmInventory private data should now be cached
    ?assertMatch(
        {ok, ?ATM_INVENTORY_PRIVATE_DATA_MATCHER(?ATM_INVENTORY_1)},
        rpc:call(Node, atm_inventory_logic, get, [User1Sess, ?ATM_INVENTORY_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, AtmInventoryGriMatcher)),

    % Make sure that provider can access non-cached data
    logic_tests_common:invalidate_cache(Config, od_atm_inventory, ?ATM_INVENTORY_1),
    ?assertMatch(
        {ok, ?ATM_INVENTORY_PRIVATE_DATA_MATCHER(?ATM_INVENTORY_1)},
        rpc:call(Node, atm_inventory_logic, get, [User1Sess, ?ATM_INVENTORY_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, AtmInventoryGriMatcher)),
    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    logic_tests_common:invalidate_cache(Config, od_atm_inventory, ?ATM_INVENTORY_1),
    % Simulate received updates
    AtmInventory1PrivateGRI = #gri{type = od_atm_inventory, id = ?ATM_INVENTORY_1, aspect = instance, scope = private},
    AtmInventory1PrivateData = ?ATM_INVENTORY_PRIVATE_DATA_VALUE(?ATM_INVENTORY_1),
    
    AtmInventoryGriMatcher = #gri{type = od_atm_inventory, id = ?ATM_INVENTORY_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, AtmInventoryGriMatcher),

    % private scope
    ?assertMatch(
        {ok, ?ATM_INVENTORY_PRIVATE_DATA_MATCHER(?ATM_INVENTORY_1)},
        rpc:call(Node, atm_inventory_logic, get, [User1Sess, ?ATM_INVENTORY_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, AtmInventoryGriMatcher)),

    NewName = <<"new_name">>,
    
    ChangedData1 = AtmInventory1PrivateData#{
        <<"name">> => NewName,
        <<"revision">> => 6
    },
    PushMessage1 = #gs_push_graph{gri = AtmInventory1PrivateGRI, data = ChangedData1, change_type = updated},
    logic_tests_common:simulate_push(Config, PushMessage1),

    ?assertMatch(
        {ok, #document{key = ?ATM_INVENTORY_1, value = #od_atm_inventory{
            name = NewName,
            cache_state = #{revision := 6}
        }}},
        rpc:call(Node, atm_inventory_logic, get, [User1Sess, ?ATM_INVENTORY_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, AtmInventoryGriMatcher)),


    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage4 = #gs_push_graph{gri = AtmInventory1PrivateGRI, change_type = deleted},
    logic_tests_common:simulate_push(Config, PushMessage4),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_atm_inventory, get_from_cache, [?ATM_INVENTORY_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    logic_tests_common:invalidate_cache(Config, od_atm_inventory, ?ATM_INVENTORY_1),
    ?assertMatch(
        {ok, ?ATM_INVENTORY_PRIVATE_DATA_MATCHER(?ATM_INVENTORY_1)},
        rpc:call(Node, atm_inventory_logic, get, [User1Sess, ?ATM_INVENTORY_1])
    ),

    PushMessage5 = #gs_push_nosub{gri = AtmInventory1PrivateGRI, reason = forbidden},
    logic_tests_common:simulate_push(Config, PushMessage5),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_atm_inventory, get_from_cache, [?ATM_INVENTORY_1])
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
