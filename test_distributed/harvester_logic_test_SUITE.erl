%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests harvester logic API using mocked gs_client module.
%%% @end
%%%--------------------------------------------------------------------
-module(harvester_logic_test_SUITE).
-author("Jakub Kudzia").

-include("logic_tests_common.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

-export([
    submit_entry_test/1,
    delete_entry_test/1
]).

all() -> ?ALL([
    submit_entry_test,
    delete_entry_test
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

submit_entry_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),
    
    FileId = <<"dummyFileId">>,
    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(ok, rpc:call(Node, harvester_logic, submit_entry, [?HARVESTER_1, FileId, #{}])),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(ok, rpc:call(Node, harvester_logic, submit_entry, [?HARVESTER_1, FileId, #{}])),
    ?assertEqual(GraphCalls + 2, logic_tests_common:count_reqs(Config, graph)),

    ok.

delete_entry_test(Config) ->
    [Node | _] = ?config(op_worker_nodes, Config),

    FileId = <<"dummyFileId">>,
    GraphCalls = logic_tests_common:count_reqs(Config, graph),

    ?assertMatch(ok, rpc:call(Node, harvester_logic, delete_entry, [?HARVESTER_1, FileId])),
    ?assertEqual(GraphCalls + 1, logic_tests_common:count_reqs(Config, graph)),

    ?assertMatch(ok, rpc:call(Node, harvester_logic, delete_entry, [?HARVESTER_1, FileId])),
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

%%%===================================================================
%%% Internal functions
%%%===================================================================