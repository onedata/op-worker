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
    oz_connection_test/1
]).

all() -> ?ALL([
    oz_connection_test
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
    Config.

end_per_testcase(_, _) ->
    ok.

end_per_suite(Config) ->
    logic_tests_common:unmock_gs_client(Config),
    ok.
