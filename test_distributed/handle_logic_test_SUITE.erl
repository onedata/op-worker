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
-module(handle_logic_test_SUITE).
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
    create_test/1
]).

all() -> ?ALL([
    get_test,
    get_public_data_test,
    mixed_get_test,
    subscribe_test,
    convenience_functions_test,
    create_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % User 3 does not belong to the handle
    User3Sess = logic_tests_common:get_user_session(Config, ?USER_3),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?HANDLE_PRIVATE_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Handle private data should now be cached

    ?assertMatch(
        {ok, ?HANDLE_PRIVATE_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider and other users cannot access cached data
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, handle_logic, get, [?ROOT_SESS_ID, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, handle_logic, get, [User3Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % Make sure that provider and other users cannot access non-cached data
    logic_tests_common:invalidate_cache(Config, od_handle, ?HANDLE_1),
    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, handle_logic, get, [?ROOT_SESS_ID, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        ?ERROR_FORBIDDEN,
        rpc:call(Node, handle_logic, get, [User3Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    ok.


get_public_data_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    % User 3 does not belong to the handle
    User3Sess = logic_tests_common:get_user_session(Config, ?USER_3),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % All users and providers should be able to fetch public handle data
    % when it is cached
    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [User3Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [?ROOT_SESS_ID, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % All users and providers should be able to fetch public handle data
    % when is is NOT cached
    logic_tests_common:invalidate_cache(Config, od_handle, ?HANDLE_1),
    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [User3Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    logic_tests_common:invalidate_cache(Config, od_handle, ?HANDLE_1),
    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [?ROOT_SESS_ID, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)),

    ok.


mixed_get_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),
    UnsubCalls = logic_tests_common:count_reqs(Config, unsub),

    % Fetching rising scopes should cause an unsub and new fetch every time
    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls, logic_tests_common:count_reqs(Config, unsub)),

    ?assertMatch(
        {ok, ?HANDLE_PRIVATE_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub)),

    % When private data is cached, any scope should always be fetched from cache
    ?assertMatch(
        {ok, ?HANDLE_PRIVATE_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub)),

    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),
    ?assertEqual(UnsubCalls + 1, logic_tests_common:count_reqs(Config, unsub)),

    ok.


subscribe_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Simulate received updates on different scopes (in rising order)
    Handle1PublicGRI = #gri{type = od_handle, id = ?HANDLE_1, aspect = instance, scope = public},
    Handle1PublicData = ?HANDLE_PUBLIC_DATA_VALUE(?HANDLE_1),
    Handle1PrivateGRI = #gri{type = od_handle, id = ?HANDLE_1, aspect = instance, scope = private},
    Handle1PrivateData = ?HANDLE_PRIVATE_DATA_VALUE(?HANDLE_1),

    % public scope
    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ChangedData1 = Handle1PublicData#{<<"publicHandle">> => <<"changedPublicHandle">>},
    PushMessage1 = #gs_push_graph{gri = Handle1PublicGRI, data = ChangedData1, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage1]),

    ?assertMatch(
        {ok, #document{key = ?HANDLE_1, value = #od_handle{
            public_handle = <<"changedPublicHandle">>
        }}},
        rpc:call(Node, handle_logic, get_public_data, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    % private scope
    ?assertMatch(
        {ok, ?HANDLE_PRIVATE_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ChangedData2 = Handle1PrivateData#{<<"publicHandle">> => <<"changedPublicHandle2">>},
    PushMessage2 = #gs_push_graph{gri = Handle1PrivateGRI, data = ChangedData2, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage2]),
    ?assertMatch(
        {ok, #document{key = ?HANDLE_1, value = #od_handle{
            public_handle = <<"changedPublicHandle2">>
        }}},
        rpc:call(Node, handle_logic, get, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Update of public scope should not affect the cache
    ChangedData3 = Handle1PublicData#{<<"publicHandle">> => <<"changedPublicHandle3">>},
    PushMessage3 = #gs_push_graph{gri = Handle1PublicGRI, data = ChangedData3, change_type = updated},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage3]),

    ?assertMatch(
        {ok, #document{key = ?HANDLE_1, value = #od_handle{
            public_handle = <<"changedPublicHandle2">>
        }}},
        rpc:call(Node, handle_logic, get, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        {ok, #document{key = ?HANDLE_1, value = #od_handle{
            public_handle = <<"changedPublicHandle2">>
        }}},
        rpc:call(Node, handle_logic, get_public_data, [User1Sess, ?HANDLE_1])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    % Simulate a 'deleted' push and see if cache was invalidated
    PushMessage4 = #gs_push_graph{gri = Handle1PrivateGRI, change_type = deleted},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage4]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_handle, get, [?HANDLE_1])
    ),

    % Simulate a 'nosub' push and see if cache was invalidated, fetch the
    % record first.
    ?assertMatch(
        {ok, ?HANDLE_PUBLIC_DATA_MATCHER(?HANDLE_1)},
        rpc:call(Node, handle_logic, get_public_data, [User1Sess, ?HANDLE_1])
    ),

    PushMessage5 = #gs_push_nosub{gri = Handle1PrivateGRI, reason = forbidden},
    rpc:call(Node, gs_client_worker, process_push_message, [PushMessage5]),
    ?assertMatch(
        {error, not_found},
        rpc:call(Node, od_handle, get, [?HANDLE_1])
    ),

    ok.


convenience_functions_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    % Test convenience functions and if they fetch correct scopes
    % Eff users are within private scope
    ?assertMatch(
        true,
        rpc:call(Node, handle_logic, has_eff_user, [User1Sess, ?HANDLE_1, ?USER_1])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        true,
        rpc:call(Node, handle_logic, has_eff_user, [User1Sess, ?HANDLE_1, ?USER_2])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),
    ?assertMatch(
        false,
        rpc:call(Node, handle_logic, has_eff_user, [User1Sess, ?HANDLE_1, <<"wrongId">>])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ok.


create_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        {ok, ?MOCK_CREATED_HANDLE_ID},
        rpc:call(Node, handle_logic, create, [
            User1Sess,
            ?HANDLE_H_SERVICE(<<"newHandle">>),
            ?HANDLE_RESOURCE_TYPE(<<"newHandle">>),
            ?HANDLE_RESOURCE_ID(<<"newHandle">>),
            ?HANDLE_METADATA(<<"newHandle">>)
        ])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        ?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"handleServiceId">>),
        rpc:call(Node, handle_logic, create, [
            User1Sess,
            <<"badHService">>,
            ?HANDLE_RESOURCE_TYPE(<<"newHandle">>),
            ?HANDLE_RESOURCE_ID(<<"newHandle">>),
            ?HANDLE_METADATA(<<"newHandle">>)
        ])
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
    logic_tests_common:init_per_testcase(Config).

end_per_testcase(_, _Config) ->
    ok.

end_per_suite(Config) ->
    logic_tests_common:unmock_gs_client(Config),
    ok.
