%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests handle service logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(gs_handle_service_logic_test_SUITE).
-author("Lukasz Opiola").

-include("logic_tests_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    get_test/1,
    get_public_data_test/1,
    mixed_get_test/1,
    subscribe_test/1,
    convenience_functions_test/1,
    confined_access_token_test/1
]).

all() -> ?ALL([
    get_test,
    get_public_data_test,
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
    % User 3 does not belong to the handle service
    User3Sess = logic_tests_common:get_user_session(Config, ?USER_3),
    
    HServiceGriMatcher = #gri{type = od_handle_service, id = ?HANDLE_SERVICE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher),

    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PRIVATE_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get, [User1Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),

    % Handle service private data should now be cached

    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PRIVATE_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get, [User1Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),

    % Make sure that provider and other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, handle_service_logic, get, [?ROOT_SESS_ID, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, handle_service_logic, get, [User3Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),

    % Make sure that provider and other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_handle_service, ?HANDLE_SERVICE_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, handle_service_logic, get, [?ROOT_SESS_ID, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, handle_service_logic, get, [User3Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),

    ok.


get_public_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % User 3 does not belong to the handle_service
    User3Sess = logic_tests_common:get_user_session(Config, ?USER_3),

    HServiceGriMatcher = #gri{type = od_handle_service, id = ?HANDLE_SERVICE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher),

    % All users and providers should be able to fetch public handle_service data
    % when it is cached
    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PUBLIC_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get_public_data, [User1Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),

    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PUBLIC_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get_public_data, [User1Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),

    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PUBLIC_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get_public_data, [User3Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),

    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PUBLIC_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get_public_data, [?ROOT_SESS_ID, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),

    % All users and providers should be able to fetch public handle_service data
    % when is is NOT cached
    logic_tests_common:invalidate_cache(Config, od_handle_service, ?HANDLE_SERVICE_1),
    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PUBLIC_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get_public_data, [User3Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),

    logic_tests_common:invalidate_cache(Config, od_handle_service, ?HANDLE_SERVICE_1),
    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PUBLIC_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get_public_data, [?ROOT_SESS_ID, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),

    ok.


mixed_get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    HServiceGriMatcher = #gri{type = od_handle_service, id = ?HANDLE_SERVICE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher),
    UnsubCalls = logic_tests_common:count_reqs(Config, unsub, HServiceGriMatcher),

    % Fetching rising scopes should cause an unsub and new fetch every time
    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PUBLIC_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get_public_data, [User1Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),
    ?assertEqual(UnsubCalls, logic_tests_common:count_reqs(Config, unsub, HServiceGriMatcher)),

    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PRIVATE_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get, [User1Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub, HServiceGriMatcher)),

    % When private data is cached, any scope should always be fetched from cache
    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PRIVATE_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get, [User1Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub, HServiceGriMatcher)),

    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PUBLIC_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get_public_data, [User1Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub, HServiceGriMatcher)),

    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    
    HServiceGriMatcher = #gri{type = od_handle_service, id = ?HANDLE_SERVICE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher),

    % Simulate received updates on private scope
    HService1PrivateGRI = #gri{type = od_handle_service, id = ?HANDLE_SERVICE_1, aspect = instance, scope = private},
    HService1PrivateData = ?HANDLE_SERVICE_PRIVATE_DATA_VALUE(?HANDLE_SERVICE_1),

    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PRIVATE_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get, [User1Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),

    ChangedData = HService1PrivateData#{
        <<"revision">> => 19,
        <<"name">> => <<"changedName">>
    },
    PushMessage = #gs_push_graph{gri = HService1PrivateGRI, data = ChangedData, change_type = updated},
    logic_tests_common:simulate_push(Config, PushMessage),
    ?assertMatch(
        {ok, #document{key = ?HANDLE_SERVICE_1, value = #od_handle_service{
            name = <<"changedName">>,
            cache_state = #{revision := 19}
        }}},
        rpc:call(Node, handle_service_logic, get, [User1Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),

    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage2 = #gs_push_graph{gri = HService1PrivateGRI, change_type = deleted},
    logic_tests_common:simulate_push(Config, PushMessage2),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_handle_service, get_from_cache, [?HANDLE_SERVICE_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    logic_tests_common:invalidate_cache(Config, od_handle_service, ?HANDLE_SERVICE_1),
    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PRIVATE_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get, [User1Sess, ?HANDLE_SERVICE_1])
    ),

    PushMessage3 = #gs_push_nosub{gri = HService1PrivateGRI, reason = forbidden},
    logic_tests_common:simulate_push(Config, PushMessage3),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_handle_service, get_from_cache, [?HANDLE_SERVICE_1])
    ),

    ok.


convenience_functions_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    
    HServiceGriMatcher = #gri{type = od_handle_service, id = ?HANDLE_SERVICE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher),

    % Test convenience functions and if they fetch correct scopes
    % Eff users are within private scope
    ?assertMatch(
        true,
        rpc:call(Node, handle_service_logic, has_eff_user, [User1Sess, ?HANDLE_SERVICE_1, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),
    ?assertMatch(
        true,
        rpc:call(Node, handle_service_logic, has_eff_user, [User1Sess, ?HANDLE_SERVICE_1, ?USER_2])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),
    ?assertMatch(
        false,
        rpc:call(Node, handle_service_logic, has_eff_user, [User1Sess, ?HANDLE_SERVICE_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HServiceGriMatcher)),

    ok.


confined_access_token_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    Caveat = #cv_data_path{whitelist = [<<"/spaceid/path">>]},
    AccessToken = initializer:create_access_token(?USER_1, [Caveat]),
    TokenCredentials = auth_manager:build_token_credentials(
        AccessToken, undefined,
        initializer:local_ip_v4(), rest, allow_data_access_caveats
    ),
    UserGriMatcher = #gri{type = od_user, id = ?USER_1, aspect = instance, _ = '_'},
    OdTokenGriMatcher = #gri{type = od_token, aspect = verify_access_token, scope = public},
    TokenSecretGriMatcher = #gri{type = temporary_token_secret, id = ?USER_1, aspect = user, scope = shared},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, UserGriMatcher),
    OdTokenGraphCalls = logic_tests_common:count_reqs(Config, graph, OdTokenGriMatcher),
    TokenSecretGraphCalls = logic_tests_common:count_reqs(Config, graph, TokenSecretGriMatcher),

    % Request should be denied before contacting Onezone because of the
    % data access caveat
    ?assertMatch(
        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED(Caveat)),
        rpc:call(Node, handle_service_logic, get, [TokenCredentials, ?HANDLE_SERVICE_1])
    ),
    % Nevertheless, following requests should be made:
    % - first to verify token credentials,
    % - second to subscribe for token revocation notifications in oz,
    % - third to fetch user data to initialize userRootDir, etc.
    ?assertEqual(OdTokenGraphCalls + 1, logic_tests_common:count_reqs(Config, graph, OdTokenGriMatcher)),
    ?assertEqual(TokenSecretGraphCalls + 1, logic_tests_common:count_reqs(Config, graph, TokenSecretGriMatcher)),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)).


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
