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
-module(handle_service_logic_test_SUITE).
-author("Lukasz Opiola").

-include("logic_tests_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    get_test/1,
    subscribe_test/1,
    convenience_functions_test/1
]).

all() -> ?ALL([
    get_test,
    subscribe_test,
    convenience_functions_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % User 3 does not belong to the handle service
    User3Sess = logic_tests_common:get_user_session(Config, ?USER_3),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PRIVATE_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get, [User1Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Handle service private data should now be cached

    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PRIVATE_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get, [User1Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider and other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, handle_service_logic, get, [?ROOT_SESS_ID, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, handle_service_logic, get, [User3Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider and other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_handle_service, ?HANDLE_SERVICE_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, handle_service_logic, get, [?ROOT_SESS_ID, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, handle_service_logic, get, [User3Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Simulate received updates on private scope
    HService1PrivateGRI = #gri{type = od_handle_service, id = ?HANDLE_SERVICE_1, aspect = instance, scope = private},
    HService1PrivateData = ?HANDLE_SERVICE_PRIVATE_DATA_VALUE(?HANDLE_SERVICE_1),

    ?assertMatch(
        {ok, ?HANDLE_SERVICE_PRIVATE_DATA_MATCHER(?HANDLE_SERVICE_1)},
        rpc:call(Node, handle_service_logic, get, [User1Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ChangedData = HService1PrivateData#{<<"name">> => <<"changedName">>},
    PushMessage = #gs_push_graph{gri = HService1PrivateGRI, data = ChangedData, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage]),
    ?assertMatch(
        {ok, #document{key = ?HANDLE_SERVICE_1, value = #od_handle_service{
            name = <<"changedName">>
        }}},
        rpc:call(Node, handle_service_logic, get, [User1Sess, ?HANDLE_SERVICE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ok.


convenience_functions_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Test convenience functions and if they fetch correct scopes
    % Eff users are within private scope
    ?assertMatch(
        true,
        rpc:call(Node, handle_service_logic, has_eff_user, [User1Sess, ?HANDLE_SERVICE_1, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        true,
        rpc:call(Node, handle_service_logic, has_eff_user, [User1Sess, ?HANDLE_SERVICE_1, ?USER_2])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, handle_service_logic, has_eff_user, [User1Sess, ?HANDLE_SERVICE_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

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
