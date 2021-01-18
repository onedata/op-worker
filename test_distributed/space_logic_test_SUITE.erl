%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests space logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(space_logic_test_SUITE).
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
    harvest_metadata_test/1,
    confined_access_token_test/1
]).

all() -> ?ALL([
    get_test,
    get_protected_data_test,
    mixed_get_test,
    subscribe_test,
    convenience_functions_test,
    harvest_metadata_test,
    confined_access_token_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % User 3 does not have access to the space
    User3Sess = logic_tests_common:get_user_session(Config, ?USER_3),

    SpaceGriMatcher = #gri{type = od_space, id = ?SPACE_1, aspect = instance, scope = private},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher),

    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % Space private data should now be cached
    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % Make sure that provider can access cached data
    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % Make sure that other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, space_logic, get, [User3Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % Make sure that provider can access non-cached data
    logic_tests_common:invalidate_cache(Config, od_space, ?SPACE_1),
    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % Make sure that other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_space, ?SPACE_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, space_logic, get, [User3Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    ok.


get_protected_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % User 3 does not have access to the space
    User3Sess = logic_tests_common:get_user_session(Config, ?USER_3),
    
    SpaceGriMatcher = #gri{type = od_space, id = ?SPACE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher),
    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % Space protected data should now be cached
    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % Make sure that provider can access cached data
    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % Provider should also be able to fetch non-cached data
    logic_tests_common:invalidate_cache(Config, od_space, ?SPACE_1),
    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % Make sure that other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, space_logic, get, [User3Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % Make sure that other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_space, ?SPACE_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, space_logic, get, [User3Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    ok.


mixed_get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    
    SpaceGriMatcher = #gri{type = od_space, id = ?SPACE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher),
    UnsubCalls = logic_tests_common:count_reqs(Config, unsub, SpaceGriMatcher),

    % Fetching rising scopes should cause an unsub and new fetch every time
    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),
    ?assertEqual(UnsubCalls, logic_tests_common:count_reqs(Config, unsub, SpaceGriMatcher)),

    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub, SpaceGriMatcher)),

    % When private data is cached, any scope should always be fetched from cache

    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub, SpaceGriMatcher)),

    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub, SpaceGriMatcher)),

    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    % Simulate received updates on different scopes (in rising order)
    Space1ProtectedGRI = #gri{type = od_space, id = ?SPACE_1, aspect = instance, scope = protected},
    Space1ProtectedData = ?SPACE_PROTECTED_DATA_VALUE(?SPACE_1),
    Space1PrivateGRI = #gri{type = od_space, id = ?SPACE_1, aspect = instance, scope = private},
    Space1PrivateData = ?SPACE_PRIVATE_DATA_VALUE(?SPACE_1),
    
    SpaceGriMatcher = #gri{type = od_space, id = ?SPACE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher),

    % protected scope
    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    ChangedData1 = Space1ProtectedData#{
        <<"revision">> => 12,
        <<"name">> => <<"changedName">>
    },
    PushMessage1 = #gs_push_graph{gri = Space1ProtectedGRI, data = ChangedData1, change_type = updated},
    logic_tests_common:simulate_push(Config, PushMessage1),

    ?assertMatch(
        {ok, #document{key = ?SPACE_1, value = #od_space{
            name = <<"changedName">>,
            cache_state = #{revision := 12}
        }}},
        rpc:call(Node, space_logic, get_protected_data, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % private scope
    logic_tests_common:invalidate_cache(Config, od_space, ?SPACE_1),
    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    ChangedData2 = Space1PrivateData#{
        <<"revision">> => 23,
        <<"name">> => <<"changedName2">>
    },
    PushMessage2 = #gs_push_graph{gri = Space1PrivateGRI, data = ChangedData2, change_type = updated},
    logic_tests_common:simulate_push(Config, PushMessage2),

    ?assertMatch(
        {ok, #document{key = ?SPACE_1, value = #od_space{
            name = <<"changedName2">>,
            cache_state = #{revision := 23}
        }}},
        rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),
    ?assertMatch(
        {ok, #document{key = ?SPACE_1, value = #od_space{
            name = <<"changedName2">>
        }}},
        rpc:call(Node, space_logic, get_protected_data, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage4 = #gs_push_graph{gri = Space1PrivateGRI, change_type = deleted},
    logic_tests_common:simulate_push(Config, PushMessage4),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_space, get_from_cache, [?SPACE_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    logic_tests_common:invalidate_cache(Config, od_space, ?SPACE_1),
    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, ?SPACE_1])
    ),

    PushMessage5 = #gs_push_nosub{gri = Space1PrivateGRI, reason = forbidden},
    logic_tests_common:simulate_push(Config, PushMessage5),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_space, get_from_cache, [?SPACE_1])
    ),

    ok.


convenience_functions_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    
    SpaceGriMatcher = #gri{type = od_space, id = ?SPACE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher),

    % Test convenience functions and if they fetch correct scopes

    % Name is within shared scope
    ?assertMatch(
        {ok, ?SPACE_NAME(?SPACE_1)},
        rpc:call(Node, space_logic, get_name, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % Eff users are within private scope
    ?assertMatch(
        {ok, ?SPACE_EFF_USERS_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_eff_users, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % Shares are within private scope
    ?assertMatch(
        {ok, ?SPACE_SHARES(?SPACE_1)},
        rpc:call(Node, space_logic, get_shares, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % Providers are within private scope
    ExpProviderIds = maps:keys(?SPACE_PROVIDERS_VALUE(?SPACE_1)),
    ?assertMatch(
        {ok, ExpProviderIds},
        rpc:call(Node, space_logic, get_provider_ids, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % Eff users are within private scope
    ?assertMatch(
        true,
        rpc:call(Node, space_logic, has_eff_user, [User1Sess, ?SPACE_1, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, has_eff_user, [User1Sess, ?SPACE_1, ?USER_3])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % User eff privileges are within private scope,
    % mocked users only have SPACE_VIEW privileges
    ?assertMatch(
        true,
        rpc:call(Node, space_logic, has_eff_privilege, [?SPACE_1, ?USER_1, ?SPACE_VIEW])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, has_eff_privilege, [?SPACE_1, ?USER_3, ?SPACE_VIEW])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),
    ?assertMatch(
        true,  % ?USER_1 is a space owner - effectively has all the privileges
        rpc:call(Node, space_logic, has_eff_privilege, [?SPACE_1, ?USER_1, ?SPACE_ADD_USER])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, has_eff_privilege, [?SPACE_1, ?USER_3, ?SPACE_ADD_USER])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    % can_view_user_through_space and can_view_group_through_space require private scope
    ?assertMatch(
        true,
        rpc:call(Node, space_logic, can_view_user_through_space, [User1Sess, ?SPACE_1, ?USER_1, ?USER_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, can_view_user_through_space, [User1Sess, ?SPACE_1, ?USER_1, ?USER_3])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, can_view_user_through_space, [User1Sess, ?SPACE_1, ?USER_3, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    ?assertMatch(
        true,
        rpc:call(Node, space_logic, can_view_group_through_space, [User1Sess, ?SPACE_1, ?USER_1, ?GROUP_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, can_view_group_through_space, [User1Sess, ?SPACE_1, ?USER_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, can_view_group_through_space, [User1Sess, ?SPACE_1, ?USER_3, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    ?assertMatch(
        {ok, ?SPACE_HARVESTERS(?SPACE_1)},
        rpc:call(Node, space_logic, get_harvesters, [?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    ?assertMatch(
        {ok, ?SPACE_SUPPORT_STAGE_REGISTRY_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_support_stage_registry, [?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    LatestEmittedSeqGriMatcher = #gri{type = space_stats, id = ?SPACE_1, aspect = {latest_emitted_seq, ?PROVIDER_1}, _ = '_'},
    LatestEmittedSeqCalls = logic_tests_common:count_reqs(Config, graph, LatestEmittedSeqGriMatcher),
    ?assertMatch(
        {ok, ?SPACE_MOCKED_LATEST_EMITTED_SEQ},
        rpc:call(Node, space_logic, get_latest_emitted_seq, [?SPACE_1, ?PROVIDER_1])
    ),
    ?assertEqual(LatestEmittedSeqCalls + 1, logic_tests_common:count_reqs(Config, graph, LatestEmittedSeqGriMatcher)),
    % the results of this query are not cached
    ?assertMatch(
        {ok, ?SPACE_MOCKED_LATEST_EMITTED_SEQ},
        rpc:call(Node, space_logic, get_latest_emitted_seq, [?SPACE_1, ?PROVIDER_1])
    ),
    ?assertEqual(LatestEmittedSeqCalls + 2, logic_tests_common:count_reqs(Config, graph, LatestEmittedSeqGriMatcher)).


harvest_metadata_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    
    SpaceGriMatcher = #gri{type = od_space, id = ?SPACE_1, aspect = harvest_metadata, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher),

    ?assertMatch(ok, rpc:call(Node, space_logic, harvest_metadata,
        [?SPACE_1, #{}, [], 100, 100])),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    ?assertMatch(ok, rpc:call(Node, space_logic, harvest_metadata,
        [?SPACE_1, #{}, [], 100, 100])),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, SpaceGriMatcher)),

    ok.


confined_access_token_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    {ok, Objectid} = file_id:guid_to_objectid(file_id:pack_guid(<<"123">>, ?SPACE_1)),
    Caveat = #cv_data_objectid{whitelist = [Objectid]},
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

    % Request should be denied before contacting Onezone because the space in
    % objectid is different than requested
    ?assertMatch(
        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED(Caveat)),
        rpc:call(Node, space_logic, get, [TokenCredentials, ?SPACE_2])
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
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Nodes, main_harvesting_stream),
    test_utils:mock_expect(Nodes, main_harvesting_stream, revise_space_harvesters,
        fun(_, _) -> ok end),
    logic_tests_common:init_per_testcase(Config).

end_per_testcase(_, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Nodes, main_harvesting_stream),
    ok.

end_per_suite(Config) ->
    logic_tests_common:unmock_gs_client(Config),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================