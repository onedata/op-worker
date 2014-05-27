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
-include("nodes_manager.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/assertions.hrl").
-include_lib("ctool/include/test_node_starter.hrl").

%% export for ct
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([distributed_test/1, local_test/1]).

%% export nodes' codes
-export([node1_code1/0, node1_code2/0, node2_code/0]).

all() -> [distributed_test].

%% ====================================================================
%% Test functions
%% ====================================================================

%% ====================================================================
%% Sample distributed test
%% ====================================================================

%% Test function (it runs on tester node)
distributed_test(Config) ->
  nodes_manager:check_start_assertions(Config),
  Nodes = ?config(nodes, Config),

  [Node1 | Nodes2] = Nodes,
  [Node2 | _] = Nodes2,

  ?assertEqual(ok, rpc:call(Node1, ?MODULE, node1_code1, [])),
  nodes_manager:wait_for_cluster_cast(),
  ?assertEqual(ok, rpc:call(Node2, ?MODULE, node2_code, [])),
  nodes_manager:wait_for_cluster_cast({?Node_Manager_Name, Node2}),
  ?assertEqual(ok, rpc:call(Node1, ?MODULE, node1_code2, [])),
  nodes_manager:wait_for_cluster_init(),

  NodesListFromCCM = gen_server:call({global, ?CCM}, get_nodes, 500),
  ?assertEqual(length(Nodes), length(NodesListFromCCM)),
  lists:foreach(fun(Node) ->
    ?assert(lists:member(Node, NodesListFromCCM))
  end, Nodes).

%% Code of nodes used during the test
node1_code1() ->
  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  ok.

node1_code2() ->
  gen_server:cast({global, ?CCM}, init_cluster),
  ok.

node2_code() ->
  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  ok.

%% ====================================================================
%% Sample local test
%% It is very useful during the test development but finally test
%% should use slaves (even when only one node is needed).
%% ====================================================================

local_test(Config) ->
  nodes_manager:check_start_assertions(Config),

  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  nodes_manager:wait_for_cluster_cast(),
  gen_server:cast({global, ?CCM}, init_cluster),
  nodes_manager:wait_for_cluster_init(),

  NodesListFromCCM = gen_server:call({global, ?CCM}, get_nodes, 500),
  ?assertEqual(1, length(NodesListFromCCM)).


%% ====================================================================
%% SetUp and TearDown functions
%% ====================================================================

init_per_testcase(distributed_test, Config) ->
  ?INIT_DIST_TEST,

  %% To see slaves output use nodes_manager:start_test_on_nodes with 2 arguments (second argument should be true)
  %% e.g. nodes_manager:start_test_on_nodes(2, true)
  Nodes = nodes_manager:start_test_on_nodes(2),
  [Node1 | _] = Nodes,

  StartLog = nodes_manager:start_app_on_nodes(Nodes, [[{node_type, ccm_test}, {dispatcher_port, 5055}, {ccm_nodes, [Node1]}, {dns_port, 1308}, {control_panel_port, 2308}, {control_panel_redirect_port, 1354}, {rest_port, 3308}, {heart_beat, 1}],
    [{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, [Node1]}, {dns_port, 1309}, {control_panel_port, 2309}, {control_panel_redirect_port, 1355}, {rest_port, 3309}, {heart_beat, 1}]]),

  Assertions = [{false, lists:member(error, Nodes)}, {false, lists:member(error, StartLog)}],
  lists:append([{nodes, Nodes}, {assertions, Assertions}], Config);

init_per_testcase(local_test, Config) ->
  ?INIT_DIST_TEST,
  StartTestAns = nodes_manager:start_test_on_local_node(),
  StartAppAns = nodes_manager:start_app_local([{node_type, ccm_test}, {dispatcher_port, 7777}, {ccm_nodes, [node()]}, {dns_port, 1312}]),
  Assertions = [{ok, StartTestAns}, {ok, StartAppAns}],
  lists:append([{assertions, Assertions}], Config).


end_per_testcase(distributed_test, Config) ->
  Nodes = ?config(nodes, Config),
  StopLog = nodes_manager:stop_app_on_nodes(Nodes),
  StopAns = nodes_manager:stop_nodes(Nodes),
  %% use assertions AFTER all code that should be executed (they will show info for user but do not disturb cleaning up)
  ?assertEqual(false, lists:member(error, StopLog)),
  ?assertEqual(ok, StopAns);

end_per_testcase(local_test, _Config) ->
  StopAns = nodes_manager:stop_test_on_local_nod(),
  ?assertEqual(ok, StopAns).
