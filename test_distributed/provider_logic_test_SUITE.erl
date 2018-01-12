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
-module(provider_logic_test_SUITE).
-author("Lukasz Opiola").

-include("logic_tests_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    get_test/1,
    get_protected_data_test/1,
    mixed_get_test/1,
    subscribe_test/1,
    convenience_functions_test/1
]).

all() -> ?ALL([
    get_test,
    get_protected_data_test,
    mixed_get_test,
    subscribe_test,
    convenience_functions_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:create_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?PROVIDER_PRIVATE_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Provider private data should now be cached
    ?assertMatch(
        {ok, ?PROVIDER_PRIVATE_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that users cannot access cached private data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, provider_logic, get, [User1Sess, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that users cannot access non-cached private data
    logic_tests_common:invalidate_cache(Config, od_provider, ?PROVIDER_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, provider_logic, get, [User1Sess, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ok.


get_protected_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:create_user_session(Config, ?USER_1),
    % User 3 does not belong to the provider
    User3Sess = logic_tests_common:create_user_session(Config, ?USER_3),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?PROVIDER_PROTECTED_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_protected_data, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Provider private data should now be cached
    ?assertMatch(
        {ok, ?PROVIDER_PROTECTED_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_protected_data, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % User 1 should be able to get cached protected provider data, provider
    % is not able to verify user's right to view provider so another request
    % must be made.
    ?assertMatch(
        {ok, ?PROVIDER_PROTECTED_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_protected_data, [User1Sess, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % User 3 should not be able to get cached protected provider data, provider
    % is not able to verify user's right to view provider so another request
    % must be made.
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, provider_logic, get_protected_data, [User3Sess, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    % User 1 should be able to get non-cached protected provider data
    logic_tests_common:invalidate_cache(Config, od_provider, ?PROVIDER_1),
    ?assertMatch(
        {ok, ?PROVIDER_PROTECTED_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_protected_data, [User1Sess, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 4, logic_tests_common:count_reqs(Config, graph)),

    % User 3 should not be able to get non-cached protected provider data
    logic_tests_common:invalidate_cache(Config, od_provider, ?PROVIDER_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, provider_logic, get_protected_data, [User3Sess, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 5, logic_tests_common:count_reqs(Config, graph)),

    ok.


mixed_get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),
    UnsubCalls = logic_tests_common:count_reqs(Config, unsub),

    % Fetching rising scopes should cause an unsub and new fetch every time
    ?assertMatch(
        {ok, ?PROVIDER_PROTECTED_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_protected_data, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls, logic_tests_common:count_reqs(Config, unsub)),

    ?assertMatch(
        {ok, ?PROVIDER_PRIVATE_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub)),

    % When private data is cached, any scope should always be fetched from cache
    ?assertMatch(
        {ok, ?PROVIDER_PROTECTED_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_protected_data, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub)),

    ?assertMatch(
        {ok, ?PROVIDER_PRIVATE_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub)),

    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

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
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ChangedData1 = Provider1ProtectedData#{<<"name">> => <<"changedName">>},
    PushMessage1 = #gs_push_graph{gri = Provider1ProtectedGRI, data = ChangedData1, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage1]),

    ?assertMatch(
        {ok, #document{key = ?PROVIDER_1, value = #od_provider{
            name = <<"changedName">>
        }}},
        rpc:call(Node, provider_logic, get_protected_data, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % private scope
    ?assertMatch(
        {ok, ?PROVIDER_PRIVATE_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ChangedData2 = Provider1PrivateData#{<<"name">> => <<"changedName2">>},
    PushMessage2 = #gs_push_graph{gri = Provider1PrivateGRI, data = ChangedData2, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage2]),

    ?assertMatch(
        {ok, #document{key = ?PROVIDER_1, value = #od_provider{
            name = <<"changedName2">>
        }}},
        rpc:call(Node, provider_logic, get, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        {ok, #document{key = ?PROVIDER_1, value = #od_provider{
            name = <<"changedName2">>
        }}},
        rpc:call(Node, provider_logic, get_protected_data, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Update of protected scope should not affect the cache
    ChangedData3 = Provider1ProtectedData#{<<"name">> => <<"changedName3">>},
    PushMessage3 = #gs_push_graph{gri = Provider1ProtectedGRI, data = ChangedData3, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage3]),

    ?assertMatch(
        {ok, #document{key = ?PROVIDER_1, value = #od_provider{
            name = <<"changedName2">>
        }}},
        rpc:call(Node, provider_logic, get, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        {ok, #document{key = ?PROVIDER_1, value = #od_provider{
            name = <<"changedName2">>
        }}},
        rpc:call(Node, provider_logic, get_protected_data, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage4 = #gs_push_graph{gri = Provider1PrivateGRI, change_type = deleted},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage4]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_provider, get, [?PROVIDER_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    ?assertMatch(
        {ok, ?PROVIDER_PRIVATE_DATA_MATCHER(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),

    PushMessage5 = #gs_push_nosub{gri = Provider1PrivateGRI, reason = forbidden},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage5]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_provider, get, [?PROVIDER_1])
    ),

    ok.


convenience_functions_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Test convenience functions and if they fetch correct scopes

    % Name is within protected scope
    ?assertMatch(
        {ok, ?PROVIDER_NAME(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_name, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        {ok, ?PROVIDER_DOMAIN(?PROVIDER_1)},
        rpc:call(Node, provider_logic, get_domain, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Name is within private scope
    ExpectedSpaces = maps:keys(?PROVIDER_SPACES_VALUE(?PROVIDER_1)),
    ?assertMatch(
        {ok, ExpectedSpaces},
        rpc:call(Node, provider_logic, get_spaces, [?ROOT_SESS_ID, ?PROVIDER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Eff users are within private scope
    ?assertMatch(
        true,
        rpc:call(Node, provider_logic, has_eff_user, [?ROOT_SESS_ID, ?PROVIDER_1, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        true,
        rpc:call(Node, provider_logic, has_eff_user, [?ROOT_SESS_ID, ?PROVIDER_1, ?USER_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, provider_logic, has_eff_user, [?ROOT_SESS_ID, ?PROVIDER_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),


    % Eff groups are within private scope
    ?assertMatch(
        true,
        rpc:call(Node, provider_logic, supports_space, [?ROOT_SESS_ID, ?PROVIDER_1, ?SPACE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        true,
        rpc:call(Node, provider_logic, supports_space, [?ROOT_SESS_ID, ?PROVIDER_1, ?SPACE_2])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, provider_logic, supports_space, [?ROOT_SESS_ID, ?PROVIDER_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

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
    logic_tests_common:wait_for_mocked_connection(Config),
    Config.

end_per_testcase(_, Config) ->
    logic_tests_common:invalidate_all_test_records(Config),
    ok.

end_per_suite(Config) ->
    logic_tests_common:unmock_gs_client(Config),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================