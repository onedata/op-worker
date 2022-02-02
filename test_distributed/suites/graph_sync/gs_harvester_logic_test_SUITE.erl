%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests harvester logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(gs_harvester_logic_test_SUITE).
-author("Jakub Kudzia").

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

-define(FILE_ID, <<"dummyFileId">>).
-define(PAYLOAD, #{<<"dummyKey">> => <<"dummyValue">>}).
-define(INDICES, #{<<"dummyKey">> => <<"dummyValue">>}).
-define(SEQ, 123).
-define(MAX_SEQ, 456).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    
    HarvesterGriMatcher = #gri{type = od_harvester, id = ?HARVESTER_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, HarvesterGriMatcher),

    ?assertMatch(
        {ok, ?HARVESTER_PRIVATE_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),

    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HarvesterGriMatcher)),

    % Harvester protected data should now be cached
    ?assertMatch(
        {ok, ?HARVESTER_PRIVATE_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HarvesterGriMatcher)),

    % Make sure that provider can access non-cached data
    logic_tests_common:invalidate_cache(Config, od_harvester, ?HARVESTER_1),
    ?assertMatch(
        {ok, ?HARVESTER_PRIVATE_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, HarvesterGriMatcher)),
    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    % Simulate received updates
    Harvester1PrivateGRI = #gri{type = od_harvester, id = ?HARVESTER_1, aspect = instance, scope = private},
    Harvester1PrivateData = ?HARVESTER_PRIVATE_DATA_VALUE(?HARVESTER_1),
    
    HarvesterGriMatcher = #gri{type = od_harvester, id = ?HARVESTER_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, HarvesterGriMatcher),

    % private scope
    ?assertMatch(
        {ok, ?HARVESTER_PRIVATE_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HarvesterGriMatcher)),

    ChangedData1 = Harvester1PrivateData#{
        <<"revision">> => 6,
        <<"indices">> => ?HARVESTER_INDICES2(?HARVESTER_1),
        <<"spaces">> => ?HARVESTER_SPACES2(?HARVESTER_1)
    },
    PushMessage1 = #gs_push_graph{gri = Harvester1PrivateGRI, data = ChangedData1, change_type = updated},
    logic_tests_common:simulate_push(Config, PushMessage1),

    ?assertMatch(
        {ok, #document{key = ?HARVESTER_1, value = #od_harvester{
            indices = ?HARVESTER_INDICES2(?HARVESTER_1),
            spaces = ?HARVESTER_SPACES2(?HARVESTER_1),
            cache_state = #{revision := 6}
        }}},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HarvesterGriMatcher)),


    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage4 = #gs_push_graph{gri = Harvester1PrivateGRI, change_type = deleted},
    logic_tests_common:simulate_push(Config, PushMessage4),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_harvester, get_from_cache, [?HARVESTER_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    logic_tests_common:invalidate_cache(Config, od_harvester, ?HARVESTER_1),
    ?assertMatch(
        {ok, ?HARVESTER_PRIVATE_DATA_MATCHER(?HARVESTER_1)},
        rpc:call(Node, harvester_logic, get, [?HARVESTER_1])
    ),

    PushMessage5 = #gs_push_nosub{gri = Harvester1PrivateGRI, reason = forbidden},
    logic_tests_common:simulate_push(Config, PushMessage5),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_harvester, get_from_cache, [?HARVESTER_1])
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
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Nodes, main_harvesting_stream),
    test_utils:mock_expect(Nodes, main_harvesting_stream, revise_harvester,
        fun(_, _, _) -> ok end),
    logic_tests_common:init_per_testcase(Config).

end_per_testcase(_, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Nodes, main_harvesting_stream),
    ok.

end_per_suite(Config) ->
    logic_tests_common:unmock_gs_client(Config),
    ok.