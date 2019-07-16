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
    cache_consistency_test/1
]).

all() -> ?ALL([
    oz_connection_test,
    cache_consistency_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

oz_connection_test(Config) ->
    [Node | _] = Nodes = ?config(op_worker_nodes, Config),

    % If provider can't connect to onezone, it should return
    % ?ERROR_NO_CONNECTION_TO_OZ for all requests.
    [test_utils:set_env(N, ?APP_NAME, graph_sync_path, ?PATH_CAUSING_CONN_ERROR) || N <- Nodes],
    ?assertMatch(
        ?ERROR_NO_CONNECTION_TO_OZ,
        rpc:call(Node, user_logic, authorize, [?MOCK_CAVEAT_ID]),
        10
    ),

    % If provider can connect to onezone, but was authenticated as nobody,
    % it should return ?ERROR_NO_CONNECTION_TO_OZ for all requests.
    [test_utils:set_env(N, ?APP_NAME, graph_sync_path, ?PATH_CAUSING_NOBODY_IDENTITY) || N <- Nodes],
    ?assertMatch(
        ?ERROR_NO_CONNECTION_TO_OZ,
        rpc:call(Node, user_logic, authorize, [?MOCK_CAVEAT_ID]),
        10
    ),

    % Requests should work when the provider connects to onezone.
    [test_utils:set_env(N, ?APP_NAME, graph_sync_path, ?PATH_CAUSING_CORRECT_CONNECTION) || N <- Nodes],
    ?assertMatch(
        {ok, ?MOCK_DISCH_MACAROON},
        rpc:call(Node, user_logic, authorize, [?MOCK_CAVEAT_ID]),
        10
    ),

    ok.


cache_consistency_test(Config) ->
    % This test operates on ?USER_1 and simulates a series of push messages
    % regarding the user, checking if caching logic works as expected

    simulate_user_push(Config, <<"alpha">>, private, 1),
    ?assertEqual({<<"alpha">>, private, 1}, get_cached_user(Config)),
    % Pushing a non-greater scope with the same rev should not overwrite the cache
    simulate_user_push(Config, <<"beta">>, private, 1),
    ?assertEqual({<<"alpha">>, private, 1}, get_cached_user(Config)),
    simulate_user_push(Config, <<"beta">>, protected, 1),
    ?assertEqual({<<"alpha">>, private, 1}, get_cached_user(Config)),
    simulate_user_push(Config, <<"beta">>, shared, 1),
    ?assertEqual({<<"alpha">>, private, 1}, get_cached_user(Config)),

    invalidate_cache(Config),
    simulate_user_push(Config, <<"alpha">>, protected, 1),
    ?assertEqual({<<"alpha">>, protected, 1}, get_cached_user(Config)),

    simulate_user_push(Config, <<"beta">>, protected, 1),
    ?assertEqual({<<"alpha">>, protected, 1}, get_cached_user(Config)),
    simulate_user_push(Config, <<"beta">>, shared, 1),
    ?assertEqual({<<"alpha">>, protected, 1}, get_cached_user(Config)),

    invalidate_cache(Config),
    simulate_user_push(Config, <<"alpha">>, shared, 1),
    ?assertEqual({<<"alpha">>, shared, 1}, get_cached_user(Config)),
    simulate_user_push(Config, <<"beta">>, shared, 1),
    ?assertEqual({<<"alpha">>, shared, 1}, get_cached_user(Config)),


    % Pushing a greater scope with the same rev should overwrite the cache
    invalidate_cache(Config),
    simulate_user_push(Config, <<"alpha">>, shared, 1),
    ?assertEqual({<<"alpha">>, shared, 1}, get_cached_user(Config)),
    simulate_user_push(Config, <<"beta">>, protected, 1),
    ?assertEqual({<<"beta">>, protected, 1}, get_cached_user(Config)),
    simulate_user_push(Config, <<"gamma">>, private, 1),
    ?assertEqual({<<"gamma">>, private, 1}, get_cached_user(Config)),


    % Pushing a lower rev should never overwrite the cache
    invalidate_cache(Config),
    simulate_user_push(Config, <<"alpha">>, shared, 2),

    simulate_user_push(Config, <<"beta">>, private, 1),
    ?assertEqual({<<"alpha">>, shared, 2}, get_cached_user(Config)),
    simulate_user_push(Config, <<"beta">>, protected, 1),
    ?assertEqual({<<"alpha">>, shared, 2}, get_cached_user(Config)),
    simulate_user_push(Config, <<"beta">>, shared, 1),
    ?assertEqual({<<"alpha">>, shared, 2}, get_cached_user(Config)),

    invalidate_cache(Config),
    simulate_user_push(Config, <<"alpha">>, protected, 12),

    simulate_user_push(Config, <<"beta">>, private, 1),
    ?assertEqual({<<"alpha">>, protected, 12}, get_cached_user(Config)),
    simulate_user_push(Config, <<"beta">>, protected, 1),
    ?assertEqual({<<"alpha">>, protected, 12}, get_cached_user(Config)),
    simulate_user_push(Config, <<"beta">>, shared, 1),
    ?assertEqual({<<"alpha">>, protected, 12}, get_cached_user(Config)),

    invalidate_cache(Config),
    simulate_user_push(Config, <<"alpha">>, private, 7),

    simulate_user_push(Config, <<"beta">>, private, 1),
    ?assertEqual({<<"alpha">>, private, 7}, get_cached_user(Config)),
    simulate_user_push(Config, <<"beta">>, protected, 1),
    ?assertEqual({<<"alpha">>, private, 7}, get_cached_user(Config)),
    simulate_user_push(Config, <<"beta">>, shared, 1),
    ?assertEqual({<<"alpha">>, private, 7}, get_cached_user(Config)),


    % Pushing a greater rev with any scope should always overwrite the cache
    invalidate_cache(Config),
    simulate_user_push(Config, <<"alpha">>, private, 3),

    simulate_user_push(Config, <<"beta">>, private, 5),
    ?assertEqual({<<"beta">>, private, 5}, get_cached_user(Config)),
    simulate_user_push(Config, <<"gamma">>, protected, 9),
    ?assertEqual({<<"gamma">>, protected, 9}, get_cached_user(Config)),
    simulate_user_push(Config, <<"delta">>, shared, 10),
    ?assertEqual({<<"delta">>, shared, 10}, get_cached_user(Config)),
    simulate_user_push(Config, <<"theta">>, protected, 67),
    ?assertEqual({<<"theta">>, protected, 67}, get_cached_user(Config)),
    simulate_user_push(Config, <<"omega">>, private, 99),
    ?assertEqual({<<"omega">>, private, 99}, get_cached_user(Config)),

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
    [test_utils:set_env(N, ?APP_NAME, graph_sync_healthcheck_interval, 1000) || N <- Nodes],
    [test_utils:set_env(N, ?APP_NAME, graph_sync_reconnect_backoff_rate, 1) || N <- Nodes],
    [test_utils:set_env(N, ?APP_NAME, graph_sync_reconnect_max_backoff, 1000) || N <- Nodes],
    % In oz_connection_test, start with a bad connection path to test connection
    % errors.
    [test_utils:set_env(N, ?APP_NAME, graph_sync_path, ?PATH_CAUSING_CONN_ERROR) || N <- Nodes],
    Config;
init_per_testcase(_, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    [test_utils:set_env(N, ?APP_NAME, graph_sync_path, ?PATH_CAUSING_CORRECT_CONNECTION) || N <- Nodes],
    Config.


end_per_testcase(_, _) ->
    ok.

end_per_suite(Config) ->
    logic_tests_common:unmock_gs_client(Config),
    ok.


% Simulates a push message from Onezone with new user data (for ?USER_1)
simulate_user_push(Config, Username, Scope, Revision) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    Data = case Scope of
        private -> ?USER_PRIVATE_DATA_VALUE(?USER_1);
        protected -> ?USER_PROTECTED_DATA_VALUE(?USER_1);
        shared -> ?USER_SHARED_DATA_VALUE(?USER_1)
    end,
    rpc:call(Node, gs_client_worker, process_push_message, [#gs_push_graph{
        gri = #gri{type = od_user, id = ?USER_1, aspect = instance, scope = Scope},
        change_type = updated,
        data = Data#{
            <<"username">> => Username,
            <<"revision">> => Revision
        }
    }]).


% Returns the scope and revision cached for ?USER_1
get_cached_user(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    {ok, #document{value = #od_user{
        username = Username,
        cache_state = #{
            scope := Scope,
            revision := Revision
        }
    }}} = rpc:call(Node, od_user, get_from_cache, [?USER_1]),
    {Username, Scope, Revision}.


invalidate_cache(Config) ->
    logic_tests_common:invalidate_cache(Config, od_user, ?USER_1).
