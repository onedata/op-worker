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
    fetch_idp_access_token_test/1
]).

all() -> ?ALL([
    get_test,
    get_protected_data_test,
    get_shared_data_test,
    mixed_get_test,
    subscribe_test,
    convenience_functions_test,
    fetch_idp_access_token_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================


get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    User2Sess = logic_tests_common:get_user_session(Config, ?USER_2),
    % Creating session should fetch user (private aspect), invalidate
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_2),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?USER_PRIVATE_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % User private data should now be cached
    ?assertMatch(
        {ok, ?USER_PRIVATE_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider and other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, user_logic, get, [User2Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, user_logic, get, [?ROOT_SESS_ID, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider and other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, user_logic, get, [User2Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, user_logic, get, [?ROOT_SESS_ID, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ok.


get_protected_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    User2Sess = logic_tests_common:get_user_session(Config, ?USER_2),
    % Creating session should fetch user (private aspect), invalidate
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_2),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?USER_PROTECTED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_protected_data, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % User protected data should now be cached
    ?assertMatch(
        {ok, ?USER_PROTECTED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_protected_data, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Provider should also be able to fetch the data from cache
    ?assertMatch(
        {ok, ?USER_PROTECTED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_protected_data, [?ROOT_SESS_ID, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Provider should also be able to fetch non-cached data
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        {ok, ?USER_PROTECTED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_protected_data, [?ROOT_SESS_ID, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, user_logic, get_protected_data, [User2Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, user_logic, get_protected_data, [User2Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ok.


get_shared_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    User2Sess = logic_tests_common:get_user_session(Config, ?USER_2),
    % Creating session should fetch user (private aspect), invalidate
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_2),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [User1Sess, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % User shared data should now be cached
    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [User1Sess, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Provider should also be able to fetch the data from cache
    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [?ROOT_SESS_ID, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Other user should be able to fetch the data through common membership
    % in space. Number of calls should rise because common space has to be
    % fetched to verify authorization.
    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [User2Sess, ?USER_1, ?THROUGH_SPACE(?SPACE_1)])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, user_logic, get_shared_data, [User2Sess, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Provider should also be able to fetch non-cached data
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [?ROOT_SESS_ID, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    % Other user should also be able to fetch non-cached data, the space is
    % already fetched so request count should rise by one
    % (user is invalidated and fetched every time).

    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [User2Sess, ?USER_1, ?THROUGH_SPACE(?SPACE_1)])
    ),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph)),

    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, user_logic, get_shared_data, [User2Sess, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 5, logic_tests_common:count_reqs(Config, graph)),

    ok.


mixed_get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % Creating session should fetch user (private aspect), invalidate
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),
    UnsubCalls = logic_tests_common:count_reqs(Config, unsub),

    % Fetching rising scopes should cause an unsub and new fetch every time
    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [User1Sess, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls, logic_tests_common:count_reqs(Config, unsub)),

    ?assertMatch(
        {ok, ?USER_PROTECTED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_protected_data, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub)),

    ?assertMatch(
        {ok, ?USER_PRIVATE_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 2, logic_tests_common:count_reqs(Config, unsub)),

    % When private data is cached, any scope should always be fetched from cache

    ?assertMatch(
        {ok, ?USER_PRIVATE_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 2, logic_tests_common:count_reqs(Config, unsub)),

    ?assertMatch(
        {ok, ?USER_PROTECTED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_protected_data, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 2, logic_tests_common:count_reqs(Config, unsub)),

    ?assertMatch(
        {ok, ?USER_SHARED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_shared_data, [User1Sess, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 2, logic_tests_common:count_reqs(Config, unsub)),

    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % Creating session should fetch user (private aspect), invalidate
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

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
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ChangedData = User1SharedData#{
        <<"revision">> => 3,
        <<"fullName">> => <<"changedName">>
    },
    PushMessage1 = #gs_push_graph{gri = User1SharedGRI, data = ChangedData, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage1]),

    ?assertMatch(
        {ok, #document{key = ?USER_1, value = #od_user{
            full_name = <<"changedName">>,
            cache_state = #{revision := 3}
        }}},
        rpc:call(Node, user_logic, get_shared_data, [User1Sess, ?USER_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % protected scope
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        {ok, ?USER_PROTECTED_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get_protected_data, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ChangedData2 = User1ProtectedData#{
        <<"revision">> => 5,
        <<"fullName">> => <<"changedName2">>
    },
    PushMessage2 = #gs_push_graph{gri = User1ProtectedGRI, data = ChangedData2, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage2]),

    ?assertMatch(
        {ok, #document{key = ?USER_1, value = #od_user{
            full_name = <<"changedName2">>,
            cache_state = #{revision := 5}
        }}},
        rpc:call(Node, user_logic, get_protected_data, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % private scope
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    ?assertMatch(
        {ok, ?USER_PRIVATE_DATA_MATCHER(?USER_1)},
        rpc:call(Node, user_logic, get, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),
    ChangedData4 = User1PrivateData#{
        <<"revision">> => 8,
        <<"fullName">> => <<"changedName4">>
    },
    PushMessage4 = #gs_push_graph{gri = User1PrivateGRI, data = ChangedData4, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage4]),

    ?assertMatch(
        {ok, #document{key = ?USER_1, value = #od_user{
            full_name = <<"changedName4">>,
            cache_state = #{revision := 8}
        }}},
        rpc:call(Node, user_logic, get, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage7 = #gs_push_graph{gri = User1PrivateGRI, change_type = deleted},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage7]),
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
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage8]),
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

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Test convenience functions and if they fetch correct scopes

    % Full name is within shared scope
    ?assertMatch(
        {ok, ?USER_FULL_NAME(?USER_1)},
        rpc:call(Node, user_logic, get_full_name, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Eff groups are within private scope
    ?assertMatch(
        true,
        rpc:call(Node, user_logic, has_eff_group, [User1Sess, ?USER_1, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        true,
        rpc:call(Node, user_logic, has_eff_group, [User1Sess, ?USER_1, ?GROUP_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        false,
        rpc:call(Node, user_logic, has_eff_group, [User1Sess, ?USER_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Eff spaces are within private scope
    ?assertMatch(
        {ok, ?USER_EFF_SPACES(?USER_1)},
        rpc:call(Node, user_logic, get_eff_spaces, [User1Sess, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        true,
        rpc:call(Node, user_logic, has_eff_space, [User1Sess, ?USER_1, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        true,
        rpc:call(Node, user_logic, has_eff_space, [User1Sess, ?USER_1, ?SPACE_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        false,
        rpc:call(Node, user_logic, has_eff_space, [User1Sess, ?USER_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

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

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, {?MOCK_IDP_ACCESS_TOKEN, _Ttl}},
        rpc:call(Node, user_logic, fetch_idp_access_token, [User1Sess, ?USER_1, ?MOCK_IDP])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, user_logic, fetch_idp_access_token, [User1Sess, <<"wrongId">>, ?MOCK_IDP])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        ?ERROR_NOT_FOUND,
        rpc:call(Node, user_logic, fetch_idp_access_token, [User1Sess, ?USER_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

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
