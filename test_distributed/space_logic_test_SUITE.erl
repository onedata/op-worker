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

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Space private data should now be cached
    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider can access cached data
    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, space_logic, get, [User3Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider can access non-cached data
    logic_tests_common:invalidate_cache(Config, od_space, ?SPACE_1),
    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_space, ?SPACE_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, space_logic, get, [User3Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    ok.


get_protected_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % User 3 does not have access to the space
    User3Sess = logic_tests_common:get_user_session(Config, ?USER_3),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Space protected data should now be cached
    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider can access cached data
    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Provider should also be able to fetch non-cached data
    logic_tests_common:invalidate_cache(Config, od_space, ?SPACE_1),
    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, space_logic, get, [User3Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_space, ?SPACE_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, space_logic, get, [User3Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph)),

    ok.


mixed_get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),
    UnsubCalls = logic_tests_common:count_reqs(Config, unsub),

    % Fetching rising scopes should cause an unsub and new fetch every time
    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls, logic_tests_common:count_reqs(Config, unsub)),

    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub)),

    % When private data is cached, any scope should always be fetched from cache

    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub)),

    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub)),

    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    % Simulate received updates on different scopes (in rising order)
    Space1ProtectedGRI = #gri{type = od_space, id = ?SPACE_1, aspect = instance, scope = protected},
    Space1ProtectedData = ?SPACE_PROTECTED_DATA_VALUE(?SPACE_1),
    Space1PrivateGRI = #gri{type = od_space, id = ?SPACE_1, aspect = instance, scope = private},
    Space1PrivateData = ?SPACE_PRIVATE_DATA_VALUE(?SPACE_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % protected scope
    ?assertMatch(
        {ok, ?SPACE_PROTECTED_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_protected_data, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ChangedData1 = Space1ProtectedData#{
        <<"revision">> => 12,
        <<"name">> => <<"changedName">>
    },
    PushMessage1 = #gs_push_graph{gri = Space1ProtectedGRI, data = ChangedData1, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage1]),

    ?assertMatch(
        {ok, #document{key = ?SPACE_1, value = #od_space{
            name = <<"changedName">>,
            cache_state = #{revision := 12}
        }}},
        rpc:call(Node, space_logic, get_protected_data, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % private scope
    logic_tests_common:invalidate_cache(Config, od_space, ?SPACE_1),
    ?assertMatch(
        {ok, ?SPACE_PRIVATE_DATA_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ChangedData2 = Space1PrivateData#{
        <<"revision">> => 23,
        <<"name">> => <<"changedName2">>
    },
    PushMessage2 = #gs_push_graph{gri = Space1PrivateGRI, data = ChangedData2, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage2]),

    ?assertMatch(
        {ok, #document{key = ?SPACE_1, value = #od_space{
            name = <<"changedName2">>,
            cache_state = #{revision := 23}
        }}},
        rpc:call(Node, space_logic, get, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        {ok, #document{key = ?SPACE_1, value = #od_space{
            name = <<"changedName2">>
        }}},
        rpc:call(Node, space_logic, get_protected_data, [?ROOT_SESS_ID, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage4 = #gs_push_graph{gri = Space1PrivateGRI, change_type = deleted},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage4]),
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
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage5]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_space, get_from_cache, [?SPACE_1])
    ),

    ok.


convenience_functions_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Test convenience functions and if they fetch correct scopes

    % Name is within shared scope
    ?assertMatch(
        {ok, ?SPACE_NAME(?SPACE_1)},
        rpc:call(Node, space_logic, get_name, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Eff users are within private scope
    ?assertMatch(
        {ok, ?SPACE_EFF_USERS_MATCHER(?SPACE_1)},
        rpc:call(Node, space_logic, get_eff_users, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Shares are within private scope
    ?assertMatch(
        {ok, ?SPACE_SHARES(?SPACE_1)},
        rpc:call(Node, space_logic, get_shares, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Providers are within private scope
    ExpProviderIds = maps:keys(?SPACE_PROVIDERS_VALUE(?SPACE_1)),
    ?assertMatch(
        {ok, ExpProviderIds},
        rpc:call(Node, space_logic, get_provider_ids, [User1Sess, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Eff users are within private scope
    ?assertMatch(
        true,
        rpc:call(Node, space_logic, has_eff_user, [User1Sess, ?SPACE_1, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, has_eff_user, [User1Sess, ?SPACE_1, ?USER_3])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % User eff privileges are within private scope,
    % mocked users only have SPACE_VIEW privileges
    ?assertMatch(
        true,
        rpc:call(Node, space_logic, has_eff_privilege, [?SPACE_1, ?USER_1, ?SPACE_VIEW])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, has_eff_privilege, [?SPACE_1, ?USER_3, ?SPACE_VIEW])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, has_eff_privilege, [?SPACE_1, ?USER_1, ?SPACE_ADD_USER])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % can_view_user_through_space and can_view_group_through_space require private scope
    ?assertMatch(
        true,
        rpc:call(Node, space_logic, can_view_user_through_space, [User1Sess, ?SPACE_1, ?USER_1, ?USER_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, can_view_user_through_space, [User1Sess, ?SPACE_1, ?USER_1, ?USER_3])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, can_view_user_through_space, [User1Sess, ?SPACE_1, ?USER_3, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        true,
        rpc:call(Node, space_logic, can_view_group_through_space, [User1Sess, ?SPACE_1, ?USER_1, ?GROUP_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, can_view_group_through_space, [User1Sess, ?SPACE_1, ?USER_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, space_logic, can_view_group_through_space, [User1Sess, ?SPACE_1, ?USER_3, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        {ok, ?SPACE_HARVESTERS(?SPACE_1)},
        rpc:call(Node, space_logic, get_harvesters, [?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ok.


harvest_metadata_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(ok, rpc:call(Node, space_logic, harvest_metadata,
        [?SPACE_1, #{}, [], 100, 100])),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(ok, rpc:call(Node, space_logic, harvest_metadata,
        [?SPACE_1, #{}, [], 100, 100])),

    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ok.


confined_access_token_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    {ok, Objectid} = file_id:guid_to_objectid(file_id:pack_guid(<<"123">>, ?SPACE_1)),
    Caveat = #cv_data_objectid{whitelist = [Objectid]},
    Auth = #token_auth{token = initializer:create_token(?USER_1, [Caveat])},
    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Request should be denied before contacting Onezone because the space in
    % objectid is different than requested
    ?assertMatch(
        ?ERROR_TOKEN_CAVEAT_UNVERIFIED(Caveat),
        rpc:call(Node, space_logic, get, [Auth, ?SPACE_2])
    ),
    ?assertEqual(GraphCalls, logic_tests_common:count_reqs(Config, graph)).

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