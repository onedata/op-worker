%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This test is an example that shows how distributed test
%% should look like.
%% @end
%% ===================================================================

-module(example_SUITE).
-include("test_utils.hrl").
-include("modules_and_args.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_node_starter.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([distributed_test/1, local_test/1]).

%% export nodes' codes
-export([node1_code1/0, node1_code2/0, node2_code/0]).

all() -> []. %todo refill

%% ====================================================================
%% Test functions
%% ====================================================================

%% ====================================================================
%% Sample distributed test
%% ====================================================================

%% Test function (it runs on tester node)
distributed_test(Config) ->
  Nodes = ?config(nodes, Config),

  [Node1 | Nodes2] = Nodes,
  [Node2 | _] = Nodes2,

  DuplicatedPermanentNodes = (length(Nodes) - 1) * length(?PERMANENT_MODULES),

  ?assertEqual(ok, rpc:call(Node1, ?MODULE, node1_code1, [])),
  test_utils:wait_for_cluster_cast(),
  ?assertEqual(ok, rpc:call(Node2, ?MODULE, node2_code, [])),
  test_utils:wait_for_cluster_cast({?NODE_MANAGER_NAME, Node2}),
  ?assertEqual(ok, rpc:call(Node1, ?MODULE, node1_code2, [])),
  test_utils:wait_for_cluster_init(DuplicatedPermanentNodes),

  NodesListFromCCM = gen_server:call({global, ?CCM}, get_nodes, 500),
  ?assertEqual(length(Nodes), length(NodesListFromCCM)),
  lists:foreach(fun(Node) ->
    ?assert(lists:member(Node, NodesListFromCCM))
  end, Nodes).

%% Code of nodes used during the test
node1_code1() ->
  gen_server:cast(?NODE_MANAGER_NAME, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  ok.

node1_code2() ->
  gen_server:cast({global, ?CCM}, init_cluster),
  ok.

node2_code() ->
  gen_server:cast(?NODE_MANAGER_NAME, do_heart_beat),
  ok.

%% ====================================================================
%% Sample local test
%% It is very useful during the test development but finally test
%% should use slaves (even when only one node is needed).
%% ====================================================================

local_test(_Config) ->
  gen_server:cast(?NODE_MANAGER_NAME, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  test_utils:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  test_utils:wait_for_cluster_init(),

  NodesListFromCCM = gen_server:call({global, ?CCM}, get_nodes, 500),
  ?assertEqual(1, length(NodesListFromCCM)).


%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(distributed_test, Config) ->
  ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,

  %% To see slaves output use test_node_starter:start_test_nodes with 2 arguments (second argument should be true)
  %% e.g. test_node_starter:start_test_nodes(2, true)
  Nodes = test_node_starter:start_test_nodes(2),
  [Node1 | _] = Nodes,

  test_node_starter:start_app_on_nodes(?APP_NAME, ?ONEPROVIDER_DEPS, Nodes, [[{node_type, ccm_test}, {dispatcher_port, 5055}, {gateway_listener_port, 8877}, {gateway_proxy_port, 8876}, {ccm_nodes, [Node1]}, {dns_port, 1308}, {http_worker_https_port, 2308}, {http_worker_redirect_port, 1354}, {http_worker_rest_port, 3308}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, [Node1]}, {dns_port, 1309}, {http_worker_https_port, 2309}, {gateway_listener_port, 8878}, {gateway_proxy_port, 8879}, {http_worker_redirect_port, 1355}, {http_worker_rest_port, 3309}, {heart_beat, 1}]]),

  lists:append([{nodes, Nodes}], Config);

init_per_testcase(local_test, Config) ->
  ?INIT_CODE_PATH,?CLEAN_TEST_DIRS,
  test_node_starter:start_deps(?ONEPROVIDER_DEPS),
  test_node_starter:start_app_on_nodes(?APP_NAME, ?ONEPROVIDER_DEPS,[node()],[[{node_type, ccm_test}, {dispatcher_port, 7777}, {ccm_nodes, [node()]}, {dns_port, 1312}]]),
  Config.


end_per_testcase(distributed_test, Config) ->
  Nodes = ?config(nodes, Config),
  test_node_starter:stop_app_on_nodes(?APP_NAME, ?ONEPROVIDER_DEPS, Nodes),
  test_node_starter:stop_test_nodes(Nodes);

end_per_testcase(local_test, _Config) ->
  test_node_starter:stop_app_on_nodes(?APP_NAME, ?ONEPROVIDER_DEPS, [node()]).
