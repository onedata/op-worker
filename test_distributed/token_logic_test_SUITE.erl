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
    convenience_functions_test/1,
    get_temporary_tokens_generation_test/1,
    subscribe_for_temporary_tokens_generation_test/1
]).

all() -> ?ALL([
    get_shared_data_test,
    subscribe_test,
    convenience_functions_test,
    get_temporary_tokens_generation_test,
    subscribe_for_temporary_tokens_generation_test
]).


%%%===================================================================
%%% Test functions
%%%===================================================================


get_shared_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    GraphCalls = logic_tests_common:count_reqs(Config, graph, od_token),

    ?assertMatch(
        {ok, ?TOKEN_SHARED_DATA_MATCHER(?TOKEN_1)},
        rpc:call(Node, token_logic, get_shared_data, [?TOKEN_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, od_token)),

    % Token shared data should now be cached
    ?assertMatch(
        {ok, ?TOKEN_SHARED_DATA_MATCHER(?TOKEN_1)},
        rpc:call(Node, token_logic, get_shared_data, [?TOKEN_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, od_token)),

    % After cache invalidation token data should be fetched anew
    logic_tests_common:invalidate_cache(Config, od_token, ?TOKEN_1),
    ?assertMatch(
        {ok, ?TOKEN_SHARED_DATA_MATCHER(?TOKEN_1)},
        rpc:call(Node, token_logic, get_shared_data, [?TOKEN_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, od_token)),

    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    GraphCalls = logic_tests_common:count_reqs(Config, graph, od_token),

    Token1SharedGRI = #gri{type = od_token, id = ?TOKEN_1, aspect = instance, scope = shared},
    Token1SharedData = ?TOKEN_SHARED_DATA_VALUE(?TOKEN_1),

    % shared scope
    ?assertMatch(
        {ok, ?TOKEN_SHARED_DATA_MATCHER(?TOKEN_1)},
        rpc:call(Node, token_logic, get_shared_data, [?TOKEN_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, od_token)),

    ChangedData1 = Token1SharedData#{
        <<"revision">> => 2,
        <<"revoked">> => true
    },
    PushMessage1 = #gs_push_graph{gri = Token1SharedGRI, data = ChangedData1, change_type = updated},
    logic_tests_common:simulate_push(Config, PushMessage1),

    ?assertMatch(
        {ok, #document{key = ?TOKEN_1, value = #od_token{
            revoked = true,
            cache_state = #{revision := 2}
        }}},
        rpc:call(Node, token_logic, get_shared_data, [?TOKEN_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, od_token)),

    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage3 = #gs_push_graph{gri = Token1SharedGRI, change_type = deleted},
    logic_tests_common:simulate_push(Config, PushMessage3),
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
    logic_tests_common:simulate_push(Config, PushMessage4),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_token, get_from_cache, [?TOKEN_1])
    ),

    ok.


convenience_functions_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    GraphCalls = logic_tests_common:count_reqs(Config, graph, od_token),

    % Test convenience functions and if they fetch correct scopes

    ?assertMatch(
        {ok, false},
        rpc:call(Node, token_logic, is_token_revoked, [?TOKEN_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, od_token)),

    ok.


get_temporary_tokens_generation_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?TEMPORARY_TOKENS_SECRET_GENERATION(?USER_1)},
        rpc:call(Node, token_logic, get_temporary_tokens_generation, [?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Temporary token generation should now be cached
    ?assertMatch(
        {ok, ?TEMPORARY_TOKENS_SECRET_GENERATION(?USER_1)},
        rpc:call(Node, token_logic, get_temporary_tokens_generation, [?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % After cache invalidation temporary tokens generation should be fetched anew
    logic_tests_common:invalidate_cache(Config, temporary_token_secret, ?USER_1),
    ?assertMatch(
        {ok, ?TEMPORARY_TOKENS_SECRET_GENERATION(?USER_1)},
        rpc:call(Node, token_logic, get_temporary_tokens_generation, [?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)).


subscribe_for_temporary_tokens_generation_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    TemporaryTokenSecretSharedGRI = #gri{
        type = temporary_token_secret,
        id = ?USER_1,
        aspect = user,
        scope = shared
    },
    TemporaryTokenSecretSharedData = ?TEMPORARY_TOKENS_SECRET_SHARED_DATA_VALUE(?USER_1),

    ?assertMatch(
        {ok, ?TEMPORARY_TOKENS_SECRET_GENERATION(?USER_1)},
        rpc:call(Node, token_logic, get_temporary_tokens_generation, [?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ChangedData1 = TemporaryTokenSecretSharedData#{
        <<"revision">> => 2,
        <<"generation">> => 2
    },
    PushMessage1 = #gs_push_graph{
        gri = TemporaryTokenSecretSharedGRI,
        data = ChangedData1,
        change_type = updated
    },
    logic_tests_common:simulate_push(Config, PushMessage1),

    ?assertMatch(
        {ok, 2},
        rpc:call(Node, token_logic, get_temporary_tokens_generation, [?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage3 = #gs_push_graph{gri = TemporaryTokenSecretSharedGRI, change_type = deleted},
    logic_tests_common:simulate_push(Config, PushMessage3),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, temporary_token_secret, get_from_cache, [?USER_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    logic_tests_common:invalidate_cache(Config, temporary_token_secret, ?USER_1),
    ?assertMatch(
        {ok, ?TEMPORARY_TOKENS_SECRET_GENERATION(?USER_1)},
        rpc:call(Node, token_logic, get_temporary_tokens_generation, [?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    PushMessage4 = #gs_push_nosub{gri = TemporaryTokenSecretSharedGRI, reason = forbidden},
    logic_tests_common:simulate_push(Config, PushMessage4),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, temporary_token_secret, get_from_cache, [?USER_1])
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
