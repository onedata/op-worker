%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests user logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(user_logic_test_SUITE).
-author("Lukasz Opiola").

-include("logic_tests_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    get_test/1,
    get_protected_data_test/1,
    get_shared_data_test/1,
    mixed_get_test/1,
    subscribe_test/1,
    convenience_functions_test/1,
    fetch_idp_access_token_test/1,
    confined_access_token_test/1
]).

all() -> ?ALL([
    get_test,
    get_protected_data_test,
    get_shared_data_test,
    mixed_get_test,
    subscribe_test,
    convenience_functions_test,
    fetch_idp_access_token_test,
    confined_access_token_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================


get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    User1AccessToken = initializer:create_access_token(?USER_1),
    User1TokenCredentials = auth_manager:build_token_credentials(
        User1AccessToken, undefined,
        initializer:local_ip_v4(), graphsync, disallow_data_access_caveats
    ),
    User2Sess = logic_tests_common:get_user_session(Config, ?USER_2),
    % Creating session should fetch user (private aspect), invalidate
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_2),
    
    UserGriMatcher = #gri{type = od_user, id = ?USER_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, UserGriMatcher),

    ?assertMatch(
        {ok, ?USER_PRIVATE_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % User private data should now be cached
    ?assertMatch(
        {ok, ?USER_PRIVATE_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    ?assertMatch(
        {ok, ?USER_PRIVATE_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get, [User1TokenCredentials, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % Make sure that provider and other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, user_logic, get, [User2Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, user_logic, get, [?ROOT_SESS_ID, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % Make sure that provider and other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, user_logic, get, [User2Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, user_logic, get, [?ROOT_SESS_ID, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % Make sure that when using TokenCredentials to get non cached data get
    % and with auth cache purged 3 requests should be made:
    % - first to verify token credentials,
    % - second to subscribe for token revocation notification,
    % - third to fetch user data
    OdTokenGriMatcher = #gri{type = od_token, aspect = verify_access_token, scope = public},
    TokenSecretGriMatcher = #gri{type = temporary_token_secret, id = ?USER_1, aspect = user, scope = shared},
    OdTokenGraphCalls = logic_tests_common:count_reqs(Config, graph, OdTokenGriMatcher),
    TokenSecretGraphCalls = logic_tests_common:count_reqs(Config, graph, TokenSecretGriMatcher),
    
    true = rpc:call(Node, ets, delete_all_objects, [auth_cache]),
    ?assertMatch(
        {ok, ?USER_PRIVATE_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get, [User1TokenCredentials, ?USER_1])
    ),
    
    ?assertEqual(OdTokenGraphCalls + 1, logic_tests_common:count_reqs(Config, graph, OdTokenGriMatcher)),
    ?assertEqual(TokenSecretGraphCalls + 1, logic_tests_common:count_reqs(Config, graph, TokenSecretGriMatcher)),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),
    % And will be cached for later requests
    ?assertMatch(
        {ok, ?USER_PRIVATE_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get, [User1TokenCredentials, ?USER_1])
    ),
    ?assertEqual(OdTokenGraphCalls + 1, logic_tests_common:count_reqs(Config, graph, OdTokenGriMatcher)),
    ?assertEqual(TokenSecretGraphCalls + 1, logic_tests_common:count_reqs(Config, graph, TokenSecretGriMatcher)),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    ok.


get_protected_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    User2Sess = logic_tests_common:get_user_session(Config, ?USER_2),
    % Creating session should fetch user (private aspect), invalidate
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_2),
    
    UserGriMatcher = #gri{type = od_user, id = ?USER_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, UserGriMatcher),

    ?assertMatch(
        {ok, ?USER_PROTECTED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_protected_data, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % User protected data should now be cached
    ?assertMatch(
        {ok, ?USER_PROTECTED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_protected_data, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % Provider should also be able to fetch the data from cache
    ?assertMatch(
        {ok, ?USER_PROTECTED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_protected_data, [?ROOT_SESS_ID, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % Provider should also be able to fetch non-cached data
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        {ok, ?USER_PROTECTED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_protected_data, [?ROOT_SESS_ID, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % Make sure that other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, user_logic, get_protected_data, [User2Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % Make sure that other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, user_logic, get_protected_data, [User2Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),
    ok.


get_shared_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    User2Sess = logic_tests_common:get_user_session(Config, ?USER_2),
    % Creating session should fetch user (private aspect), invalidate
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_2),
    
    UserGriMatcher = #gri{type = od_user, id = ?USER_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, UserGriMatcher),

    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [User1Sess, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % User shared data should now be cached
    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [User1Sess, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % Provider should also be able to fetch the data from cache
    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [?ROOT_SESS_ID, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % Other user should be able to fetch the data through common membership
    % in space. Number of calls should rise because common space has to be
    % fetched to verify authorization.
    % fixme check that space calls are updated
    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [User2Sess, ?USER_1, ?THROUGH_SPACE(?SPACE_1)])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, user_logic, get_shared_data, [User2Sess, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % Provider should also be able to fetch non-cached data
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [?ROOT_SESS_ID, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % Other user should also be able to fetch non-cached data, the space is
    % already fetched so request count should rise by one
    % (user is invalidated and fetched every time).

    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [User2Sess, ?USER_1, ?THROUGH_SPACE(?SPACE_1)])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, user_logic, get_shared_data, [User2Sess, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    ok.


mixed_get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % Creating session should fetch user (private aspect), invalidate
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    
    UserGriMatcher = #gri{type = od_user, id = ?USER_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, UserGriMatcher),
    UnsubCalls = logic_tests_common:count_reqs(Config, unsub, UserGriMatcher),

    % Fetching rising scopes should cause an unsub and new fetch every time
    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [User1Sess, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),
    ?assertEqual(UnsubCalls, logic_tests_common:count_reqs(Config, unsub, UserGriMatcher)),

    ?assertMatch(
        {ok, ?USER_PROTECTED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_protected_data, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub, UserGriMatcher)),

    ?assertMatch(
        {ok, ?USER_PRIVATE_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),
    ?assertEqual(UnsubCalls + 2, logic_tests_common:count_reqs(Config, unsub, UserGriMatcher)),

    % When private data is cached, any scope should always be fetched from cache

    ?assertMatch(
        {ok, ?USER_PRIVATE_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),
    ?assertEqual(UnsubCalls + 2, logic_tests_common:count_reqs(Config, unsub, UserGriMatcher)),

    ?assertMatch(
        {ok, ?USER_PROTECTED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_protected_data, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),
    ?assertEqual(UnsubCalls + 2, logic_tests_common:count_reqs(Config, unsub, UserGriMatcher)),

    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [User1Sess, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),
    ?assertEqual(UnsubCalls + 2, logic_tests_common:count_reqs(Config, unsub, UserGriMatcher)),

    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % Creating session should fetch user (private aspect), invalidate
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    
    UserGriMatcher = #gri{type = od_user, id = ?USER_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, UserGriMatcher),

    % Simulate received updates on different scopes (in rising order)
    User1SharedGRI = #gri{type = od_user, id = ?USER_1, aspect = instance, scope = shared},
    User1SharedData = ?USER_SHARED_DATA_VALUE(?USER_1),
    User1ProtectedGRI = #gri{type = od_user, id = ?USER_1, aspect = instance, scope = protected},
    User1ProtectedData = ?USER_PROTECTED_DATA_VALUE(?USER_1),
    User1PrivateGRI = #gri{type = od_user, id = ?USER_1, aspect = instance, scope = private},
    User1PrivateData = ?USER_PRIVATE_DATA_VALUE(?USER_1),

    % shared scope
    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [User1Sess, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    ChangedData = User1SharedData#{
        <<"revision">> => 3,
        <<"fullName">> => <<"changedName">>
    },
    PushMessage1 = #gs_push_graph{gri = User1SharedGRI, data = ChangedData, change_type = updated},
    logic_tests_common:simulate_push(Config, PushMessage1),

    ?assertMatch(
        {ok, #document{key = ?USER_1, value = #od_user{
            full_name = <<"changedName">>,
            cache_state = #{revision := 3}
        }}},
        rpc:call(Node, user_logic, get_shared_data, [User1Sess, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % protected scope
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        {ok, ?USER_PROTECTED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_protected_data, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),
    ChangedData2 = User1ProtectedData#{
        <<"revision">> => 5,
        <<"fullName">> => <<"changedName2">>
    },
    PushMessage2 = #gs_push_graph{gri = User1ProtectedGRI, data = ChangedData2, change_type = updated},
    logic_tests_common:simulate_push(Config, PushMessage2),

    ?assertMatch(
        {ok, #document{key = ?USER_1, value = #od_user{
            full_name = <<"changedName2">>,
            cache_state = #{revision := 5}
        }}},
        rpc:call(Node, user_logic, get_protected_data, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % private scope
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        {ok, ?USER_PRIVATE_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),
    ChangedData4 = User1PrivateData#{
        <<"revision">> => 8,
        <<"fullName">> => <<"changedName4">>
    },
    PushMessage4 = #gs_push_graph{gri = User1PrivateGRI, data = ChangedData4, change_type = updated},
    logic_tests_common:simulate_push(Config, PushMessage4),

    ?assertMatch(
        {ok, #document{key = ?USER_1, value = #od_user{
            full_name = <<"changedName4">>,
            cache_state = #{revision := 8}
        }}},
        rpc:call(Node, user_logic, get, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage7 = #gs_push_graph{gri = User1PrivateGRI, change_type = deleted},
    logic_tests_common:simulate_push(Config, PushMessage7),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_user, get_from_cache, [?USER_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        {ok, ?USER_PRIVATE_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get, [User1Sess, ?USER_1])
    ),

    PushMessage8 = #gs_push_nosub{gri = User1PrivateGRI, reason = forbidden},
    logic_tests_common:simulate_push(Config, PushMessage8),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_user, get_from_cache, [?USER_1])
    ),

    ok.


convenience_functions_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % Creating session should fetch user (private aspect), invalidate
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    
    UserGriMatcher = #gri{type = od_user, id = ?USER_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, UserGriMatcher),

    % Test convenience functions and if they fetch correct scopes

    % Full name is within shared scope
    ?assertMatch(
        {ok, ?USER_FULL_NAME(?USER_1)},
        rpc:call(Node, user_logic, get_full_name, [?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),
    ?assertMatch(
        {ok, ?USER_FULL_NAME(?USER_1)},
        rpc:call(Node, user_logic, get_full_name, [User1Sess, ?USER_1, ?THROUGH_SPACE(?SPACE_1)])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % Eff groups are within private scope
    ?assertMatch(
        true,
        rpc:call(Node, user_logic, has_eff_group, [User1Sess, ?USER_1, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    ?assertMatch(
        true,
        rpc:call(Node, user_logic, has_eff_group, [User1Sess, ?USER_1, ?GROUP_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    ?assertMatch(
        false,
        rpc:call(Node, user_logic, has_eff_group, [User1Sess, ?USER_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    % Eff spaces are within private scope
    ?assertMatch(
        {ok, ?USER_EFF_SPACES(?USER_1)},
        rpc:call(Node, user_logic, get_eff_spaces, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    ?assertMatch(
        true,
        rpc:call(Node, user_logic, has_eff_space, [User1Sess, ?USER_1, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    ?assertMatch(
        true,
        rpc:call(Node, user_logic, has_eff_space, [User1Sess, ?USER_1, ?SPACE_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    ?assertMatch(
        false,
        rpc:call(Node, user_logic, has_eff_space, [User1Sess, ?USER_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    ?assertMatch(
        {true, ?SPACE_1},
        rpc:call(Node, user_logic, get_space_by_name, [User1Sess, ?USER_1, ?SPACE_NAME(?SPACE_1)])
    ),

    ?assertMatch(
        {true, ?SPACE_2},
        rpc:call(Node, user_logic, get_space_by_name, [User1Sess, ?USER_1, ?SPACE_NAME(?SPACE_2)])
    ),

    ?assertMatch(
        false,
        rpc:call(Node, user_logic, get_space_by_name, [User1Sess, ?USER_1, <<"wrongName">>])
    ),

    ok.


fetch_idp_access_token_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    
    UserGriMatcher = #gri{type = od_user, aspect = {idp_access_token, '_'}, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, UserGriMatcher),

    ?assertMatch(
        {ok, {?MOCK_IDP_ACCESS_TOKEN, _Ttl}},
        rpc:call(Node, user_logic, fetch_idp_access_token, [User1Sess, ?USER_1, ?MOCK_IDP])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, user_logic, fetch_idp_access_token, [User1Sess, <<"wrongId">>, ?MOCK_IDP])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, user_logic, fetch_idp_access_token, [User1Sess, ?USER_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, UserGriMatcher)),

    ok.


confined_access_token_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    Caveat = #cv_data_readonly{},
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

    % Request should be denied before contacting Onezone because of
    % data access caveat presence
    ?assertMatch(
        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED(Caveat)),
        rpc:call(Node, user_logic, fetch_idp_access_token, [TokenCredentials, ?USER_1, ?MOCK_IDP])
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
