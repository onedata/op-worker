%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests token logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(token_logic_test_SUITE).
-author("Bartosz Walkowicz").

-include("logic_tests_common.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, init_per_testcase/2,
    end_per_testcase/2, end_per_suite/1
]).

-export([
    get_shared_data_test/1,
    subscribe_test/1,
    convenience_functions_test/1
]).

all() -> ?ALL([
    get_shared_data_test,
    subscribe_test,
    convenience_functions_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_shared_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Group shared data should now be cached
    ?assertMatch(
        {ok, ?TOKEN_SHARED_DATA_MATCHER(?TOKEN_1)},
        rpc:call(Node, token_logic, get_shared_data, [?TOKEN_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Provider should also be able to fetch non-cached data
    logic_tests_common:invalidate_cache(Config, od_token, ?TOKEN_1),
    ?assertMatch(
        {ok, ?TOKEN_SHARED_DATA_MATCHER(?TOKEN_1)},
        rpc:call(Node, token_logic, get_shared_data, [?TOKEN_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Simulate received updates on different scopes (in rising order)
    Token1SharedGRI = #gri{type = od_token, id = ?TOKEN_1, aspect = instance, scope = shared},
    Token1SharedData = ?TOKEN_SHARED_DATA_VALUE(?TOKEN_1),

    % shared scope
    ?assertMatch(
        {ok, ?TOKEN_SHARED_DATA_MATCHER(?TOKEN_1)},
        rpc:call(Node, token_logic, get_shared_data, [?TOKEN_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ChangedData1 = Token1SharedData#{
        <<"revision">> => 2,
        <<"revoked">> => true
    },
    PushMessage1 = #gs_push_graph{gri = Token1SharedGRI, data = ChangedData1, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage1]),

    ?assertMatch(
        {ok, #document{key = ?TOKEN_1, value = #od_token{
            revoked = true,
            cache_state = #{revision := 2}
        }}},
        rpc:call(Node, token_logic, get_shared_data, [?TOKEN_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage3 = #gs_push_graph{gri = Token1SharedGRI, change_type = deleted},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage3]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_token, get_from_cache, [?TOKEN_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    logic_tests_common:invalidate_cache(Config, od_token, ?TOKEN_1),
    ?assertMatch(
        {ok, ?TOKEN_SHARED_DATA_MATCHER(?TOKEN_1)},
        rpc:call(Node, token_logic, get_shared_data, [?TOKEN_1])
    ),

    PushMessage4 = #gs_push_nosub{gri = Token1SharedGRI, reason = forbidden},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage4]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_token, get_from_cache, [?TOKEN_1])
    ),

    ok.


convenience_functions_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Test convenience functions and if they fetch correct scopes

    ?assertMatch(
        {ok, false},
        rpc:call(Node, token_logic, is_revoked, [?TOKEN_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

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
