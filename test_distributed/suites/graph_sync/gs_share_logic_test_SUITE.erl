%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests share logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(gs_share_logic_test_SUITE).
-author("Lukasz Opiola").

-include("logic_tests_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    get_test/1,
    get_public_data_test/1,
    mixed_get_test/1,
    subscribe_test/1,
    create_update_delete_test/1,
    confined_access_token_test/1
]).

all() -> ?ALL([
    get_test,
    get_public_data_test,
    mixed_get_test,
    subscribe_test,
    create_update_delete_test,
    confined_access_token_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % User 3 does not belong to the share
    User3Sess = logic_tests_common:get_user_session(Config, ?USER_3),

    % Cache space 1 and provider 1 private data, as it is required to verify
    % access to share in cache
    rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1]),
    rpc:call(Node, provider_logic, get, [?ROOT_SESS_ID, ?PROVIDER_1]),
    
    ShareGriMatcher = #gri{type = od_share, id = ?SHARE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, ShareGriMatcher),

    ?assertMatch(
        {ok, ?SHARE_PRIVATE_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get, [User1Sess, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),

    % Share private data should now be cached

    ?assertMatch(
        {ok, ?SHARE_PRIVATE_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get, [User1Sess, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),

    % Make sure that provider can access cached share data
    % Provider must be aware of its ID to check access to cached share - this is
    % mocked in init_per_testcase.
    ?assertMatch(
        {ok, ?SHARE_PRIVATE_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get, [?ROOT_SESS_ID, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),

    % Make sure that provider can access non-cached share data
    logic_tests_common:invalidate_cache(Config, od_share, ?SHARE_1),
    ?assertMatch(
        {ok, ?SHARE_PRIVATE_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get, [?ROOT_SESS_ID, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),

    % Make sure that other users cannot access cached data

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, share_logic, get, [User3Sess, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),
    ok.


get_public_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % User 3 does not belong to the share
    User3Sess = logic_tests_common:get_user_session(Config, ?USER_3),

    % Cache space 1 private data, as it is required to verify access to
    % share in cache
    rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1]),
    
    ShareGriMatcher = #gri{type = od_share, id = ?SHARE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, ShareGriMatcher),

    % All users and providers should be able to fetch public share data
    % when it is cached
    ?assertMatch(
        {ok, ?SHARE_PUBLIC_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get_public_data, [User1Sess, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),

    ?assertMatch(
        {ok, ?SHARE_PUBLIC_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get_public_data, [User1Sess, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),

    ?assertMatch(
        {ok, ?SHARE_PUBLIC_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get_public_data, [User3Sess, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),

    ?assertMatch(
        {ok, ?SHARE_PUBLIC_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get_public_data, [?ROOT_SESS_ID, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),

    % All users and providers should be able to fetch public share data
    % when is is NOT cached
    logic_tests_common:invalidate_cache(Config, od_share, ?SHARE_1),
    ?assertMatch(
        {ok, ?SHARE_PUBLIC_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get_public_data, [User3Sess, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),

    logic_tests_common:invalidate_cache(Config, od_share, ?SHARE_1),
    ?assertMatch(
        {ok, ?SHARE_PUBLIC_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get_public_data, [?ROOT_SESS_ID, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),
    ok.


mixed_get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    % Cache space 1 private data, as it is required to verify access to
    % share in cache
    rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1]),
    
    ShareGriMatcher = #gri{type = od_share, id = ?SHARE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, ShareGriMatcher),
    UnsubCalls = logic_tests_common:count_reqs(Config, unsub, ShareGriMatcher),

    % Fetching rising scopes should cause an unsub and new fetch every time
    ?assertMatch(
        {ok, ?SHARE_PUBLIC_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get_public_data, [User1Sess, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),
    ?assertEqual(UnsubCalls, logic_tests_common:count_reqs(Config, unsub, ShareGriMatcher)),

    ?assertMatch(
        {ok, ?SHARE_PRIVATE_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get, [User1Sess, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub, ShareGriMatcher)),

    % When private data is cached, any scope should always be fetched from cache
    ?assertMatch(
        {ok, ?SHARE_PRIVATE_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get, [User1Sess, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub, ShareGriMatcher)),

    ?assertMatch(
        {ok, ?SHARE_PUBLIC_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get_public_data, [User1Sess, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub, ShareGriMatcher)),

    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    % Cache space 1 private data, as it is required to verify access to
    % share in cache
    rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1]),
    
    ShareGriMatcher = #gri{type = od_share, id = ?SHARE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, ShareGriMatcher),

    % Simulate received updates on different scopes (in rising order)
    Share1PublicGRI = #gri{type = od_share, id = ?SHARE_1, aspect = instance, scope = public},
    Share1PublicData = ?SHARE_PUBLIC_DATA_VALUE(?SHARE_1),
    Share1PrivateGRI = #gri{type = od_share, id = ?SHARE_1, aspect = instance, scope = private},
    Share1PrivateData = ?SHARE_PRIVATE_DATA_VALUE(?SHARE_1),

    % public scope
    ?assertMatch(
        {ok, ?SHARE_PUBLIC_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get_public_data, [User1Sess, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),

    ChangedData1 = Share1PublicData#{
        <<"revision">> => 2,
        <<"name">> => <<"changedName">>
    },
    PushMessage1 = #gs_push_graph{gri = Share1PublicGRI, data = ChangedData1, change_type = updated},
    logic_tests_common:simulate_push(Config, PushMessage1),

    ?assertMatch(
        {ok, #document{key = ?SHARE_1, value = #od_share{
            name = <<"changedName">>,
            cache_state = #{revision := 2}
        }}},
        rpc:call(Node, share_logic, get_public_data, [User1Sess, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),

    % private scope
    logic_tests_common:invalidate_cache(Config, od_share, ?SHARE_1),
    ?assertMatch(
        {ok, ?SHARE_PRIVATE_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get, [User1Sess, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),

    ChangedData2 = Share1PrivateData#{
        <<"revision">> => 3,
        <<"name">> => <<"changedName2">>
    },
    PushMessage2 = #gs_push_graph{gri = Share1PrivateGRI, data = ChangedData2, change_type = updated},
    logic_tests_common:simulate_push(Config, PushMessage2),
    ?assertMatch(
        {ok, #document{key = ?SHARE_1, value = #od_share{
            name = <<"changedName2">>,
            cache_state = #{revision := 3}
        }}},
        rpc:call(Node, share_logic, get, [User1Sess, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),

    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage4 = #gs_push_graph{gri = Share1PrivateGRI, change_type = deleted},
    logic_tests_common:simulate_push(Config, PushMessage4),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_share, get_from_cache, [?SHARE_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    ?assertMatch(
        {ok, ?SHARE_PUBLIC_DATA_MATCHER(?SHARE_1)},
        rpc:call(Node, share_logic, get_public_data, [User1Sess, ?SHARE_1])
    ),

    PushMessage5 = #gs_push_nosub{gri = Share1PrivateGRI, reason = forbidden},
    logic_tests_common:simulate_push(Config, PushMessage5),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_share, get_from_cache, [?SHARE_1])
    ),

    ok.


create_update_delete_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    
    ShareGriMatcher = #gri{type = od_share, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, ShareGriMatcher),

    % Create
    ?assertMatch(
        {ok, ?MOCK_CREATED_SHARE_ID},
        rpc:call(Node, share_logic, create, [
            User1Sess,
            ?MOCK_CREATED_SHARE_ID,
            ?SHARE_NAME(<<"newShare">>),
            ?SHARE_DESCRIPTION(<<"newShare">>),
            ?SHARE_SPACE(<<"newShare">>),
            ?SHARE_ROOT_FILE(<<"newShare">>),
            dir
        ])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),
    ?assertMatch(
        ?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"spaceId">>),
        rpc:call(Node, share_logic, create, [
            User1Sess,
            ?MOCK_CREATED_SHARE_ID,
            ?SHARE_NAME(<<"newShare">>),
            ?SHARE_DESCRIPTION(<<"newShare">>),
            <<"badSpaceId">>,
            ?SHARE_ROOT_FILE(<<"newShare">>),
            dir
        ])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),

    % Update
    ?assertMatch(
        ok,
        rpc:call(Node, share_logic, update, [User1Sess, ?SHARE_1, #{
            <<"name">> => <<"newName">>,
            <<"description">> => <<"New share description">>
        }])
    ),
    % two requests should be done - one for update and one for force fetch
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),
    ?assertMatch(
        ?ERROR_BAD_VALUE_BINARY(<<"name">>),
        rpc:call(Node, share_logic, update, [User1Sess, ?SHARE_1, #{<<"name">> => 1234}])
    ),
    ?assertEqual(GraphCalls + 5, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),
    ?assertMatch(
        ?ERROR_BAD_VALUE_BINARY(<<"description">>),
        rpc:call(Node, share_logic, update, [User1Sess, ?SHARE_1, #{<<"description">> => 87.9}])
    ),
    ?assertEqual(GraphCalls + 6, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),
    ?assertMatch(
        ?ERROR_MISSING_AT_LEAST_ONE_VALUE([<<"description">>, <<"name">>]),
        rpc:call(Node, share_logic, update, [User1Sess, ?SHARE_1, #{}])
    ),
    ?assertEqual(GraphCalls + 7, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),

    % Delete
    ?assertMatch(
        ok,
        rpc:call(Node, share_logic, delete, [User1Sess, ?SHARE_1])
    ),
    ?assertEqual(GraphCalls + 8, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),
    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, share_logic, delete, [User1Sess, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 9, logic_tests_common:count_reqs(Config, graph, ShareGriMatcher)),

    ok.


confined_access_token_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    Caveat = #cv_interface{interface = oneclient},
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
    % oneclient interface caveat
    ?assertMatch(
        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED(Caveat)),
        rpc:call(Node, share_logic, delete, [TokenCredentials, ?SHARE_1])
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

init_per_testcase(get_test, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    % Access to cached shares depends on checking if provider supports given space
    ok = test_utils:mock_expect(Nodes, provider_logic, supports_space,
        fun(?ROOT_SESS_ID, ?DUMMY_PROVIDER_ID, Space) ->
            Space == ?SPACE_1 orelse Space == ?SPACE_2
        end),
    init_per_testcase(default, Config);
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