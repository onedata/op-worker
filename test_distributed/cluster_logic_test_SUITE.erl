%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests provider logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(cluster_logic_test_SUITE).
-author("Lukasz Opiola").

-include("logic_tests_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    update_version_info_test/1
]).

all() -> ?ALL([
    update_version_info_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

update_version_info_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(
        ok,
        rpc:call(Node, cluster_logic, update_version_info, [<<"1">>, <<"2">>, <<"3">>])
    ),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        ?ERROR_BAD_VALUE_ID_NOT_FOUND(<<"gui">>),
        rpc:call(Node, cluster_logic, update_version_info, [1, 2, 3])
    ),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(
        ok,
        rpc:call(Node, cluster_logic, update_version_info, [<<"a">>, <<"b">>, <<"c">>])
    ),
    ?assertEqual(GraphCalls + 3, logic_tests_common:count_reqs(Config, graph)).


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