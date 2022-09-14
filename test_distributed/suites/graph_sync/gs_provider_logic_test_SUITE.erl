%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests provider logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(gs_provider_logic_test_SUITE).
-author("Lukasz Opiola").

-include("logic_tests_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    get_test/1,
    get_protected_data_test/1,
    mixed_get_test/1,
    subscribe_test/1,
    convenience_functions_test/1,
    confined_access_token_test/1
]).

all() -> ?ALL([
    get_test,
    get_protected_data_test,
    mixed_get_test,
    subscribe_test,
    convenience_functions_test,
    confined_access_token_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    
    ProviderGriMatcher = #gri{type = od_provider, id = ?PROVIDER_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher),
    Provider1Storages = ?PROVIDER_STORAGES(?PROVIDER_1),
    ?assertMatch(
        {ok, ?PROVIDER_PRIVATE_DATA_MATCHER(?PROVIDER_1, Provider1Storages)},
        rpc:call(Node, provider_logic, get, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    % Provider private data should now be cached
    ?assertMatch(
        {ok, ?PROVIDER_PRIVATE_DATA_MATCHER(?PROVIDER_1, Provider1Storages)},
        rpc:call(Node, provider_logic, get, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    % Make sure that users cannot access cached private data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, provider_logic, get, [User1Sess, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    % Make sure that users cannot access non-cached private data
    logic_tests_common:invalidate_cache(Config, od_provider, ?PROVIDER_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, provider_logic, get, [User1Sess, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),
    ok.


get_protected_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % User 3 does not belong to the provider
    User3Sess = logic_tests_common:get_user_session(Config, ?USER_3),
    
    ProviderGriMatcher = #gri{type = od_provider, id = ?PROVIDER_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher),

    ?assertMatch(
        {ok, ?PROVIDER_PROTECTED_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_protected_data, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    % Provider private data should now be cached
    ?assertMatch(
        {ok, ?PROVIDER_PROTECTED_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_protected_data, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    % User 1 should be able to get cached protected provider data, provider
    % is not able to verify user's right to view provider so another request
    % must be made.
    ?assertMatch(
        {ok, ?PROVIDER_PROTECTED_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_protected_data, [User1Sess, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    % User 3 should not be able to get cached protected provider data, provider
    % is not able to verify user's right to view provider so another request
    % must be made.
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, provider_logic, get_protected_data, [User3Sess, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    % User 1 should be able to get non-cached protected provider data
    logic_tests_common:invalidate_cache(Config, od_provider, ?PROVIDER_1),
    ?assertMatch(
        {ok, ?PROVIDER_PROTECTED_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_protected_data, [User1Sess, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    % User 3 should not be able to get non-cached protected provider data
    logic_tests_common:invalidate_cache(Config, od_provider, ?PROVIDER_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, provider_logic, get_protected_data, [User3Sess, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 5, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    ok.


mixed_get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    
    ProviderGriMatcher = #gri{type = od_provider, id = ?PROVIDER_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher),
    UnsubCalls = logic_tests_common:count_reqs(Config, unsub, ProviderGriMatcher),

    % Fetching rising scopes should cause an unsub and new fetch every time
    ?assertMatch(
        {ok, ?PROVIDER_PROTECTED_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_protected_data, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),
    ?assertEqual(UnsubCalls, logic_tests_common:count_reqs(Config, unsub, ProviderGriMatcher)),

    Provider1Storages = ?PROVIDER_STORAGES(?PROVIDER_1),
    ?assertMatch(
        {ok, ?PROVIDER_PRIVATE_DATA_MATCHER(?PROVIDER_1, Provider1Storages)},
        rpc:call(Node, provider_logic, get, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub, ProviderGriMatcher)),

    % When private data is cached, any scope should always be fetched from cache
    ?assertMatch(
        {ok, ?PROVIDER_PROTECTED_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_protected_data, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub, ProviderGriMatcher)),

    ?assertMatch(
        {ok, ?PROVIDER_PRIVATE_DATA_MATCHER(?PROVIDER_1, Provider1Storages)},
        rpc:call(Node, provider_logic, get, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub, ProviderGriMatcher)),

    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    
    ProviderGriMatcher = #gri{type = od_provider, id = ?PROVIDER_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher),

    % Simulate received updates on different scopes (in rising order)
    Provider1ProtectedGRI = #gri{type = od_provider, id = ?PROVIDER_1, aspect = instance, scope = protected},
    Provider1ProtectedData = ?PROVIDER_PROTECTED_DATA_VALUE(?PROVIDER_1),
    Provider1PrivateGRI = #gri{type = od_provider, id = ?PROVIDER_1, aspect = instance, scope = private},
    Provider1PrivateData = ?PROVIDER_PRIVATE_DATA_VALUE(?PROVIDER_1),

    % protected scope
    ?assertMatch(
        {ok, ?PROVIDER_PROTECTED_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_protected_data, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    ChangedData1 = Provider1ProtectedData#{
        <<"revision">> => 9,
        <<"name">> => <<"changedName">>
    },
    PushMessage1 = #gs_push_graph{gri = Provider1ProtectedGRI, data = ChangedData1, change_type = updated},
    logic_tests_common:simulate_push(Config, PushMessage1),

    ?assertMatch(
        {ok, #document{key = ?PROVIDER_1, value = #od_provider{
            name = <<"changedName">>,
            cache_state = #{revision := 9}
        }}},
        rpc:call(Node, provider_logic, get_protected_data, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    % private scope
    logic_tests_common:invalidate_cache(Config, od_provider, ?PROVIDER_1),
    Provider1Storages = ?PROVIDER_STORAGES(?PROVIDER_1),
    ?assertMatch(
        {ok, ?PROVIDER_PRIVATE_DATA_MATCHER(?PROVIDER_1, Provider1Storages)},
        rpc:call(Node, provider_logic, get, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    ChangedData2 = Provider1PrivateData#{
        <<"revision">> => 11,
        <<"name">> => <<"changedName2">>
    },
    PushMessage2 = #gs_push_graph{gri = Provider1PrivateGRI, data = ChangedData2, change_type = updated},
    logic_tests_common:simulate_push(Config, PushMessage2),

    ?assertMatch(
        {ok, #document{key = ?PROVIDER_1, value = #od_provider{
            name = <<"changedName2">>,
            cache_state = #{revision := 11}
        }}},
        rpc:call(Node, provider_logic, get, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),
    ?assertMatch(
        {ok, #document{key = ?PROVIDER_1, value = #od_provider{
            name = <<"changedName2">>,
            cache_state = #{revision := 11}
        }}},
        rpc:call(Node, provider_logic, get_protected_data, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage4 = #gs_push_graph{gri = Provider1PrivateGRI, change_type = deleted},
    logic_tests_common:simulate_push(Config, PushMessage4),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_provider, get_from_cache, [?PROVIDER_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    ?assertMatch(
        {ok, ?PROVIDER_PRIVATE_DATA_MATCHER(?PROVIDER_1, Provider1Storages)},
        rpc:call(Node, provider_logic, get, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),

    PushMessage5 = #gs_push_nosub{gri = Provider1PrivateGRI, reason = forbidden},
    logic_tests_common:simulate_push(Config, PushMessage5),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_provider, get_from_cache, [?PROVIDER_1])
    ),

    ok.


convenience_functions_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    
    ProviderGriMatcher = #gri{type = od_provider, id = ?PROVIDER_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher),

    % Test convenience functions and if they fetch correct scopes

    % Name is within protected scope
    ?assertMatch(
        {ok, ?PROVIDER_NAME(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_name, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    ?assertMatch(
        {ok, ?PROVIDER_DOMAIN(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_domain, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    % Name is within private scope
    ExpectedSpaces = maps:keys(?PROVIDER_SPACES_VALUE(?PROVIDER_1)),
    ?assertMatch(
        {ok, ExpectedSpaces},
        rpc:call(Node, provider_logic, get_spaces, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    % Eff users are within private scope
    ?assertMatch(
        true,
        rpc:call(Node, provider_logic, has_eff_user, [?ROOT_SESS_ID, ?PROVIDER_1, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),
    ?assertMatch(
        true,
        rpc:call(Node, provider_logic, has_eff_user, [?ROOT_SESS_ID, ?PROVIDER_1, ?USER_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),
    ?assertMatch(
        false,
        rpc:call(Node, provider_logic, has_eff_user, [?ROOT_SESS_ID, ?PROVIDER_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),


    % Eff groups are within private scope
    ?assertMatch(
        true,
        rpc:call(Node, provider_logic, supports_space, [?ROOT_SESS_ID, ?PROVIDER_1, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),
    ?assertMatch(
        true,
        rpc:call(Node, provider_logic, supports_space, [?ROOT_SESS_ID, ?PROVIDER_1, ?SPACE_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),
    ?assertMatch(
        false,
        rpc:call(Node, provider_logic, supports_space, [?ROOT_SESS_ID, ?PROVIDER_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ProviderGriMatcher)),

    ok.


confined_access_token_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    Caveat = #cv_api{whitelist = [{?OP_PANEL, all, ?GRI_PATTERN('*', <<"*">>, <<"*">>, '*')}]},
    AccessToken = initializer:create_access_token(?USER_1, [Caveat]),
    TokenCredentials = auth_manager:build_token_credentials(
        AccessToken, undefined,
        initializer:local_ip_v4(), graphsync, disallow_data_access_caveats
    ),
    UserGriMatcher = #gri{type = od_user, id = ?USER_1, aspect = instance, _ = '_'},
    OdTokenGriMatcher = #gri{type = od_token, aspect = verify_access_token, scope = public},
    TokenSecretGriMatcher = #gri{type = temporary_token_secret, id = ?USER_1, aspect = user, scope = shared},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, UserGriMatcher),
    OdTokenGraphCalls = logic_tests_common:count_reqs(Config, graph, OdTokenGriMatcher),
    TokenSecretGraphCalls = logic_tests_common:count_reqs(Config, graph, TokenSecretGriMatcher),

    % Request should be denied before contacting Onezone because of the
    % API caveat
    ?assertMatch(
        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED(Caveat)),
        rpc:call(Node, provider_logic, get_protected_data, [TokenCredentials, ?PROVIDER_1])
    ),
    % Nevertheless, following requests should be made:
    % - first to verify token credentials,
    % - second to subscribe for token revocation notifications in oz,
    % Normally third request to fetch user data to initialize userRootDir, etc should be
    % made. But in this case it was denied as api caveats forbids it.
    ?assertEqual(OdTokenGraphCalls + 1, logic_tests_common:count_reqs(Config, graph, OdTokenGriMatcher)),
    ?assertEqual(TokenSecretGraphCalls + 1, logic_tests_common:count_reqs(Config, graph, TokenSecretGriMatcher)),
    ?assertEqual(GraphCalls, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)).


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

%%%===================================================================
%%% Internal functions
%%%===================================================================