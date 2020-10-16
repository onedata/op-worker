%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests user authentication
%%% @end
%%%--------------------------------------------------------------------
-module(gs_channel_test_SUITE).
-author("Lukasz Opiola").

-include("logic_tests_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    oz_connection_test/1,
    cache_consistency_test/1,
    async_request_handling_test/1
]).

all() -> ?ALL([
    oz_connection_test,
    cache_consistency_test,
    async_request_handling_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

oz_connection_test(Config) ->
    [Node | _] = Nodes = ?config(op_worker_nodes, Config),

    % If provider can't connect to onezone, it should return
    % ?ERROR_NO_CONNECTION_TO_ONEZONE for all requests.
    test_utils:set_env(Nodes, ?APP_NAME, graph_sync_path, ?PATH_CAUSING_CONN_ERROR),
    ?assertMatch(
        ?ERROR_NO_CONNECTION_TO_ONEZONE,
        rpc:call(Node, provider_logic, get, []),
        10
    ),

    % If provider can connect to onezone, but was authenticated as nobody,
    % it should return ?ERROR_NO_CONNECTION_TO_ONEZONE for all requests.
    test_utils:set_env(Nodes, ?APP_NAME, graph_sync_path, ?PATH_CAUSING_NOBODY_IDENTITY),
    ?assertMatch(
        ?ERROR_NO_CONNECTION_TO_ONEZONE,
        rpc:call(Node, provider_logic, get, []),
        10
    ),

    % Requests should work when the provider connects to onezone.
    test_utils:set_env(Nodes, ?APP_NAME, graph_sync_path, ?PATH_CAUSING_CORRECT_CONNECTION),
    ?assertMatch(
        {ok, _},
        rpc:call(Node, provider_logic, get, []),
        10
    ),

    ok.


cache_consistency_test(Config) ->
    % This test operates on ?USER_1 and simulates a series of push messages
    % regarding the user, checking if caching logic works as expected

    simulate_user_1_push(Config, <<"alpha">>, private, 1),
    ?assertEqual({<<"alpha">>, private, 1}, get_cached_user_1(Config)),
    % Pushing a non-greater scope with the same rev should not overwrite the cache
    simulate_user_1_push(Config, <<"beta">>, private, 1),
    ?assertEqual({<<"alpha">>, private, 1}, get_cached_user_1(Config)),
    simulate_user_1_push(Config, <<"beta">>, protected, 1),
    ?assertEqual({<<"alpha">>, private, 1}, get_cached_user_1(Config)),
    simulate_user_1_push(Config, <<"beta">>, shared, 1),
    ?assertEqual({<<"alpha">>, private, 1}, get_cached_user_1(Config)),

    invalidate_user_1_cache(Config),
    simulate_user_1_push(Config, <<"alpha">>, protected, 1),
    ?assertEqual({<<"alpha">>, protected, 1}, get_cached_user_1(Config)),

    simulate_user_1_push(Config, <<"beta">>, protected, 1),
    ?assertEqual({<<"alpha">>, protected, 1}, get_cached_user_1(Config)),
    simulate_user_1_push(Config, <<"beta">>, shared, 1),
    ?assertEqual({<<"alpha">>, protected, 1}, get_cached_user_1(Config)),

    invalidate_user_1_cache(Config),
    simulate_user_1_push(Config, <<"alpha">>, shared, 1),
    ?assertEqual({<<"alpha">>, shared, 1}, get_cached_user_1(Config)),
    simulate_user_1_push(Config, <<"beta">>, shared, 1),
    ?assertEqual({<<"alpha">>, shared, 1}, get_cached_user_1(Config)),


    % Pushing a greater scope with the same rev should overwrite the cache
    invalidate_user_1_cache(Config),
    simulate_user_1_push(Config, <<"alpha">>, shared, 1),
    ?assertEqual({<<"alpha">>, shared, 1}, get_cached_user_1(Config)),
    simulate_user_1_push(Config, <<"beta">>, protected, 1),
    ?assertEqual({<<"beta">>, protected, 1}, get_cached_user_1(Config)),
    simulate_user_1_push(Config, <<"gamma">>, private, 1),
    ?assertEqual({<<"gamma">>, private, 1}, get_cached_user_1(Config)),


    % Pushing a lower rev should never overwrite the cache
    invalidate_user_1_cache(Config),
    simulate_user_1_push(Config, <<"alpha">>, shared, 2),

    simulate_user_1_push(Config, <<"beta">>, private, 1),
    ?assertEqual({<<"alpha">>, shared, 2}, get_cached_user_1(Config)),
    simulate_user_1_push(Config, <<"beta">>, protected, 1),
    ?assertEqual({<<"alpha">>, shared, 2}, get_cached_user_1(Config)),
    simulate_user_1_push(Config, <<"beta">>, shared, 1),
    ?assertEqual({<<"alpha">>, shared, 2}, get_cached_user_1(Config)),

    invalidate_user_1_cache(Config),
    simulate_user_1_push(Config, <<"alpha">>, protected, 12),

    simulate_user_1_push(Config, <<"beta">>, private, 1),
    ?assertEqual({<<"alpha">>, protected, 12}, get_cached_user_1(Config)),
    simulate_user_1_push(Config, <<"beta">>, protected, 1),
    ?assertEqual({<<"alpha">>, protected, 12}, get_cached_user_1(Config)),
    simulate_user_1_push(Config, <<"beta">>, shared, 1),
    ?assertEqual({<<"alpha">>, protected, 12}, get_cached_user_1(Config)),

    invalidate_user_1_cache(Config),
    simulate_user_1_push(Config, <<"alpha">>, private, 7),

    simulate_user_1_push(Config, <<"beta">>, private, 1),
    ?assertEqual({<<"alpha">>, private, 7}, get_cached_user_1(Config)),
    simulate_user_1_push(Config, <<"beta">>, protected, 1),
    ?assertEqual({<<"alpha">>, private, 7}, get_cached_user_1(Config)),
    simulate_user_1_push(Config, <<"beta">>, shared, 1),
    ?assertEqual({<<"alpha">>, private, 7}, get_cached_user_1(Config)),


    % Pushing a greater rev with any scope should always overwrite the cache
    invalidate_user_1_cache(Config),
    simulate_user_1_push(Config, <<"alpha">>, private, 3),

    simulate_user_1_push(Config, <<"beta">>, private, 5),
    ?assertEqual({<<"beta">>, private, 5}, get_cached_user_1(Config)),
    simulate_user_1_push(Config, <<"gamma">>, protected, 9),
    ?assertEqual({<<"gamma">>, protected, 9}, get_cached_user_1(Config)),
    simulate_user_1_push(Config, <<"delta">>, shared, 10),
    ?assertEqual({<<"delta">>, shared, 10}, get_cached_user_1(Config)),
    simulate_user_1_push(Config, <<"theta">>, protected, 67),
    ?assertEqual({<<"theta">>, protected, 67}, get_cached_user_1(Config)),
    simulate_user_1_push(Config, <<"omega">>, private, 99),
    ?assertEqual({<<"omega">>, private, 99}, get_cached_user_1(Config)),


    % Pushing an older revision should not cause cache overwrite
    invalidate_user_1_cache(Config),
    simulate_user_1_push(Config, <<"alpha">>, shared, 10),

    simulate_user_1_push(Config, <<"beta">>, shared, 9),
    simulate_user_1_push(Config, <<"beta">>, private, 3),
    simulate_user_1_push(Config, <<"beta">>, protected, 7),

    ?assertEqual({<<"alpha">>, shared, 10}, get_cached_user_1(Config)).


async_request_handling_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    User1Sess = logic_tests_common:get_user_session(Config, ?USER_1),
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),

    % Check that the GS client works asynchronously and long-running
    % requests do not block the GS channel - post some long-running harvest
    % requests, then some shorter requests and check that they finish before
    % the harvest requests
    logic_tests_common:mock_request_processing_time(Config, 20, 50),
    logic_tests_common:mock_harvest_request_processing_time(Config, 15000, 20000),
    logic_tests_common:set_harvest_request_timeout(Config, 30000),

    % Harvest requests are made within async processes (does not block the test process)
    lists:foreach(fun(_) ->
        rpc:cast(Node, space_logic, harvest_metadata, [?SPACE_1, #{}, [], 100, 100])
    end, lists:seq(1, 5)),

    % Short requests are made in parallel and the results are collected (blocks the test process)
    Start = time_utils:timestamp_millis(),
    lists_utils:pforeach(fun(_) ->
        rpc:call(Node, user_logic, get, [User1Sess, ?USER_1])
    end, lists:seq(1, 20)),
    % Processing time should be shorter than for harvest requests
    End = time_utils:timestamp_millis(),
    ?assert(End - Start < 15000),


    % Check that in case of a race condition between many async requests
    % fetching the same entity, the cache will still hold the newest version.
    % ?USER_INCREASING_REV is mocked to return a higher rev with every fetch.
    {_, _, StartingRev} = get_cached_user(Config, ?USER_INCREASING_REV),
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_INCREASING_REV),
    logic_tests_common:mock_request_processing_time(Config, 2000, 2500),
    UserIncRevSess = logic_tests_common:get_user_session(Config, ?USER_INCREASING_REV),
    lists_utils:pforeach(fun(_) ->
        rpc:call(Node, user_logic, get, [UserIncRevSess, ?USER_INCREASING_REV])
    end, lists:seq(1, 20)),

    EndRev = StartingRev + 20,
    ?assertMatch({_, _, EndRev}, get_cached_user(Config, ?USER_INCREASING_REV)),


    % Check that timeouts are properly handled
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1),
    logic_tests_common:set_request_timeout(Config, 1000),
    logic_tests_common:mock_request_processing_time(Config, 1500, 2000),
    ?assertEqual(
        ?ERROR_TIMEOUT,
        rpc:call(Node, user_logic, get, [User1Sess, ?USER_1])
    ),

    logic_tests_common:set_harvest_request_timeout(Config, 10000),
    logic_tests_common:mock_harvest_request_processing_time(Config, 15000, 20000),
    ?assertEqual(
        ?ERROR_TIMEOUT,
        rpc:call(Node, space_logic, harvest_metadata, [?SPACE_1, #{}, [], 100, 100])
    ),
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

init_per_testcase(oz_connection_test, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    % Modify env variables to ensure frequent reconnect attempts
    test_utils:set_env(Nodes, ?APP_NAME, graph_sync_reconnect_base_interval, 1000),
    test_utils:set_env(Nodes, ?APP_NAME, graph_sync_reconnect_backoff_rate, 1),
    test_utils:set_env(Nodes, ?APP_NAME, graph_sync_reconnect_max_backoff, 1000),
    % In oz_connection_test, start with a bad connection path to test connection
    % errors.
    test_utils:set_env(Nodes, ?APP_NAME, graph_sync_path, ?PATH_CAUSING_CONN_ERROR),
    Config;
init_per_testcase(async_request_handling_test, Config) ->
    logic_tests_common:invalidate_all_test_records(Config),
    NewConfig = logic_tests_common:init_per_testcase(Config),
    [Node | _] = ?config(op_worker_nodes, NewConfig),
    UserIncRevSess = logic_tests_common:get_user_session(NewConfig, ?USER_INCREASING_REV),
    rpc:call(Node, user_logic, get, [UserIncRevSess, ?USER_INCREASING_REV]),
    init_per_testcase(default, NewConfig);
init_per_testcase(_, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:set_env(Nodes, ?APP_NAME, graph_sync_path, ?PATH_CAUSING_CORRECT_CONNECTION),
    Config.


end_per_testcase(_, _) ->
    ok.

end_per_suite(Config) ->
    logic_tests_common:unmock_gs_client(Config),
    ok.


% Simulates a push message from Onezone with new user data (for ?USER_1)
simulate_user_1_push(Config, Username, Scope, Revision) ->
    Data = case Scope of
        private -> ?USER_PRIVATE_DATA_VALUE(?USER_1);
        protected -> ?USER_PROTECTED_DATA_VALUE(?USER_1);
        shared -> ?USER_SHARED_DATA_VALUE(?USER_1)
    end,
    logic_tests_common:simulate_push(Config, #gs_push_graph{
        gri = #gri{type = od_user, id = ?USER_1, aspect = instance, scope = Scope},
        change_type = updated,
        data = Data#{
            <<"username">> => Username,
            <<"revision">> => Revision
        }
    }).


get_cached_user_1(Config) ->
    get_cached_user(Config, ?USER_1).


get_cached_user(Config, UserId) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    {ok, #document{value = #od_user{
        username = Username,
        cache_state = #{
            scope := Scope,
            revision := Revision
        }
    }}} = rpc:call(Node, od_user, get_from_cache, [UserId]),
    {Username, Scope, Revision}.


invalidate_user_1_cache(Config) ->
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1).
