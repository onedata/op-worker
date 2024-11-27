%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests handle logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(gs_handle_logic_test_SUITE).
-author("Lukasz Opiola").

-include("logic_tests_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    get_public_data_test/1,
    subscribe_test/1,
    create_test/1,
    confined_access_token_test/1
]).

all() -> ?ALL([
    get_public_data_test,
    subscribe_test,
    create_test,
    confined_access_token_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================


get_public_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % User 3 does not belong to the handle
    User3Sess = logic_tests_common:get_user_session(Config, ?USER_3),
    
    HandleGriMatcher = #gri{type = od_handle, id = ?HANDLE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, HandleGriMatcher),

    % All users and providers should be able to fetch public handle data
    % when it is cached
    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HandleGriMatcher)),

    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HandleGriMatcher)),

    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [User3Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HandleGriMatcher)),

    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [?ROOT_SESS_ID, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HandleGriMatcher)),

    % All users and providers should be able to fetch public handle data
    % when is is NOT cached
    logic_tests_common:invalidate_cache(Config, od_handle, ?HANDLE_1),
    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [User3Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, HandleGriMatcher)),

    logic_tests_common:invalidate_cache(Config, od_handle, ?HANDLE_1),
    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [?ROOT_SESS_ID, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph, HandleGriMatcher)),

    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    
    HandleGriMatcher = #gri{type = od_handle, id = ?HANDLE_1, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, HandleGriMatcher),

    % Simulate received updates on different scopes (in rising order)
    Handle1PublicGRI = #gri{type = od_handle, id = ?HANDLE_1, aspect = instance, scope = public},
    Handle1PublicData = ?HANDLE_PUBLIC_DATA_VALUE(?HANDLE_1),

    % public scope
    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HandleGriMatcher)),

    ChangedData1 = Handle1PublicData#{
        <<"revision">> => 4,
        <<"publicHandle">> => <<"changedPublicHandle">>
    },
    PushMessage1 = #gs_push_graph{gri = Handle1PublicGRI, data = ChangedData1, change_type = updated},
    logic_tests_common:simulate_push(Config, PushMessage1),

    ?assertMatch(
        {ok, #document{key = ?HANDLE_1, value = #od_handle{
            public_handle = <<"changedPublicHandle">>,
            cache_state = #{revision := 4}
        }}},
        rpc:call(Node, handle_logic, get_public_data, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HandleGriMatcher)),

    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage4 = #gs_push_graph{gri = Handle1PublicGRI, change_type = deleted},
    logic_tests_common:simulate_push(Config, PushMessage4),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_handle, get_from_cache, [?HANDLE_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    logic_tests_common:invalidate_cache(Config, od_handle, ?HANDLE_1),
    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [User1Sess, ?HANDLE_1])
    ),

    ok.


create_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    
    HandleGriMatcher = #gri{type = od_handle, aspect = instance, _ = '_'},
    GraphCalls = logic_tests_common:count_reqs(Config, graph, HandleGriMatcher),

    ?assertMatch(
        {ok, ?MOCK_CREATED_HANDLE_ID},
        rpc:call(Node, handle_logic, create, [
            User1Sess,
            ?HANDLE_H_SERVICE(<<"newHandle">>),
            ?HANDLE_RESOURCE_TYPE(<<"newHandle">>),
            ?HANDLE_RESOURCE_ID(<<"newHandle">>),
            ?HANDLE_METADATA_PREFIX(<<"newHandle">>),
            ?HANDLE_METADATA(<<"newHandle">>)
        ])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph, HandleGriMatcher)),

    ?assertMatch(
        ?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"handleServiceId">>),
        rpc:call(Node, handle_logic, create, [
            User1Sess,
            <<"badHService">>,
            ?HANDLE_RESOURCE_TYPE(<<"newHandle">>),
            ?HANDLE_RESOURCE_ID(<<"newHandle">>),
            ?HANDLE_METADATA_PREFIX(<<"newHandle">>),
            ?HANDLE_METADATA(<<"newHandle">>)
        ])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph, HandleGriMatcher)),

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

    % Request should be denied before contacting Onezone because of the
    % data access caveat
    ?assertMatch(
        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED(Caveat)),
        rpc:call(Node, handle_logic, get_public_data, [TokenCredentials, ?HANDLE_1])
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
