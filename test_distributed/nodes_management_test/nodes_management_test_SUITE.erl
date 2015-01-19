%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This test creates many Erlang virtual machines and uses them
%% to test how ccm manages workers and monitors nodes.
%%% @end
%%%--------------------------------------------------------------------
-module(nodes_management_test_SUITE).
-author("Michal Wrzeszcz").

-include("test_utils.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([one_node_test/1, ccm_and_worker_test/1]).

all() -> [one_node_test, ccm_and_worker_test].

%%%===================================================================
%%% Test function
%% ====================================================================
one_node_test(Config) ->
    [Node] = ?config(nodes, Config),
    ?assertMatch(ccm, gen_server:call({?NODE_MANAGER_NAME, Node}, getNodeType)).

ccm_and_worker_test(Config) ->
    [Ccm, Worker] = ?config(nodes, Config),
    gen_server:call({?NODE_MANAGER_NAME, Ccm}, getNodeType),
    ?assertMatch(ccm, gen_server:call({?NODE_MANAGER_NAME, Ccm}, getNodeType)),
    ?assertMatch(worker, gen_server:call({?NODE_MANAGER_NAME, Worker}, getNodeType)),

    timer:sleep(15000), %todo reorganize cluster startup, so we don't have to wait

    ?assertEqual(ok, gen_server:call({?DISPATCHER_NAME, Ccm}, {http_worker, 1, self(), ping})),
    ?assertEqual(pong, receive Msg -> Msg end),
    ?assertEqual(ok, gen_server:call({?DISPATCHER_NAME, Worker}, {http_worker, 1, self(), ping})),
    ?assertEqual(pong, receive Msg -> Msg end).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_testcase(one_node_test, Config) ->
    ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
    test_node_starter:start_deps_for_tester_node(),

    [Node] = test_node_starter:start_test_nodes(1, true),
    DBNode = ?DB_NODE,

    test_node_starter:start_app_on_nodes(?APP_NAME, ?ONEPROVIDER_DEPS, [Node], [
        [{node_type, ccm}, {dispatcher_port, 8888}, {ccm_nodes, [Node]}, {db_nodes, [DBNode]}, {heart_beat_success_interval, 1}]]),

    lists:append([{nodes, [Node]}, {dbnode, DBNode}], Config);

init_per_testcase(ccm_and_worker_test, Config) ->
    ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
    test_node_starter:start_deps_for_tester_node(),

    Nodes = [Ccm, _] = test_node_starter:start_test_nodes(2, true),
    DBNode = ?DB_NODE,

    test_node_starter:start_app_on_nodes(?APP_NAME, ?ONEPROVIDER_DEPS, Nodes, [
        [{node_type, ccm}, {ccm_nodes, [Ccm]}, {db_nodes, [DBNode]}, {heart_beat_success_interval, 1}],
        [{node_type, worker}, {dns_port, 1300}, {ccm_nodes, [Ccm]}, {db_nodes, [DBNode]}, {heart_beat_success_interval, 1}]
    ]),

    lists:append([{nodes, Nodes}, {dbnode, DBNode}], Config).
end_per_testcase(_, Config) ->
  Nodes = ?config(nodes, Config),
  test_node_starter:stop_app_on_nodes(?APP_NAME, ?ONEPROVIDER_DEPS, Nodes),
  test_node_starter:stop_test_nodes(Nodes),
  test_node_starter:stop_deps_for_tester_node().