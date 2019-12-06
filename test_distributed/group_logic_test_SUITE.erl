%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests group logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(group_logic_test_SUITE).
-author("Lukasz Opiola").

-include("logic_tests_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    get_shared_data_test/1,
    subscribe_test/1,
    convenience_functions_test/1,
    confined_access_token_test/1
]).

all() -> ?ALL([
    get_shared_data_test,
    subscribe_test,
    convenience_functions_test,
    confined_access_token_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_shared_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % User 3 does not have access to the group
    User3Sess = logic_tests_common:get_user_session(Config, ?USER_3),

    % Spaces need to be fetched for provider to be able to assert
    % THROUGH_SPACE authorization without making additional requests.
    rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_1]),
    rpc:call(Node, space_logic, get, [User1Sess, ?SPACE_2]),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Group shared data should now be cached
    ?assertMatch(
        {ok, ?GROUP_SHARED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_shared_data, [User1Sess, ?GROUP_1, ?THROUGH_SPACE(?SPACE_1)])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Provider should also be able to fetch the data from cache without
    % additional requests
    ?assertMatch(
        {ok, ?GROUP_SHARED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_shared_data, [?ROOT_SESS_ID, ?GROUP_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Provider should also be able to fetch non-cached data
    logic_tests_common:invalidate_cache(Config, od_group, ?GROUP_1),
    ?assertMatch(
        {ok, ?GROUP_SHARED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_shared_data, [?ROOT_SESS_ID, ?GROUP_1, undefined])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, group_logic, get_shared_data, [User3Sess, ?GROUP_1, ?THROUGH_SPACE(?SPACE_2)])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Simulate received updates on different scopes (in rising order)
    Group1SharedGRI = #gri{type = od_group, id = ?GROUP_1, aspect = instance, scope = shared},
    Group1SharedData = ?GROUP_SHARED_DATA_VALUE(?GROUP_1),

    % shared scope
    ?assertMatch(
        {ok, ?GROUP_SHARED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_shared_data, [User1Sess, ?GROUP_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ChangedData1 = Group1SharedData#{
        <<"revision">> => 2,
        <<"name">> => <<"changedName">>
    },
    PushMessage1 = #gs_push_graph{gri = Group1SharedGRI, data = ChangedData1, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage1]),

    ?assertMatch(
        {ok, #document{key = ?GROUP_1, value = #od_group{
            name = <<"changedName">>,
            cache_state = #{revision := 2}
        }}},
        rpc:call(Node, group_logic, get_shared_data, [User1Sess, ?GROUP_1, undefined])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage3 = #gs_push_graph{gri = Group1SharedGRI, change_type = deleted},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage3]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_group, get_from_cache, [?GROUP_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    logic_tests_common:invalidate_cache(Config, od_group, ?GROUP_1),
    ?assertMatch(
        {ok, ?GROUP_SHARED_DATA_MATCHER(?GROUP_1)},
        rpc:call(Node, group_logic, get_shared_data, [User1Sess, ?GROUP_1, undefined])
    ),

    PushMessage4 = #gs_push_nosub{gri = Group1SharedGRI, reason = forbidden},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage4]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_group, get_from_cache, [?GROUP_1])
    ),

    ok.


convenience_functions_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Test convenience functions and if they fetch correct scopes

    % Name is within shared scope
    ?assertMatch(
        {ok, ?GROUP_NAME(?GROUP_1)},
        rpc:call(Node, group_logic, get_name, [User1Sess, ?GROUP_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ok.


confined_access_token_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    Caveat = #cv_data_path{whitelist = [<<"/spaceid/file/dir.txt">>]},
    Auth = #token_auth{subject_token = initializer:create_token(?USER_1, [Caveat])},
    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Request should be denied before contacting Onezone because of
    % data access caveat presence
    ?assertMatch(
        ?ERROR_TOKEN_CAVEAT_UNVERIFIED(Caveat),
        rpc:call(Node, group_logic, get_shared_data, [Auth, ?GROUP_1, undefined])
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
    logic_tests_common:init_per_testcase(Config).

end_per_testcase(_, _Config) ->
    ok.

end_per_suite(Config) ->
    logic_tests_common:unmock_gs_client(Config),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================