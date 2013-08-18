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

%% export for ct
-export([all/0]).
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
distributed_test(_Config) ->
  ?INIT_DIST_TEST,

  %% To see slaves output use nodes_manager:start_test_on_nodes with 2 arguments (second argument should be true)
  %% e.g. nodes_manager:start_test_on_nodes(2, true)
  Nodes = nodes_manager:start_test_on_nodes(2),
  ?assertEqual(false, lists:member(error, Nodes)),

  [Node1 | Nodes2] = Nodes,
  [Node2 | _] = Nodes2,

  StartLog = nodes_manager:start_app_on_nodes(Nodes, [[{node_type, ccm_test}, {dispatcher_port, 5055}, {ccm_nodes, [Node1]}, {dns_port, 1308}],
    [{node_type, worker}, {dispatcher_port, 6666}, {ccm_nodes, [Node1]}, {dns_port, 1309}]]),
  ?assertEqual(false, lists:member(error, StartLog)),

  ?assertEqual(ok, rpc:call(Node1, ?MODULE, node1_code1, [])),
  timer:sleep(100),
  ?assertEqual(ok, rpc:call(Node2, ?MODULE, node2_code, [])),
  timer:sleep(100),
  ?assertEqual(ok, rpc:call(Node1, ?MODULE, node1_code2, [])),

  NodesListFromCCM = gen_server:call({global, ?CCM}, get_nodes),
  ?assertEqual(length(Nodes), length(NodesListFromCCM)),
  lists:foreach(fun(Node) ->
    ?assert(lists:member(Node, NodesListFromCCM))
  end, Nodes),

  StopLog = nodes_manager:stop_app_on_nodes(Nodes),
  ?assertEqual(false, lists:member(error, StopLog)),
  ?assertEqual(ok, nodes_manager:stop_nodes(Nodes)).

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

local_test(_Config) ->
  ?INIT_DIST_TEST,
  ?assertEqual(ok, nodes_manager:start_local_test()),

  ?assertEqual(ok, nodes_manager:start_app([{node_type, ccm_test}, {dispatcher_port, 7777}, {ccm_nodes, [node()]}, {dns_port, 1312}])),

  gen_server:cast(?Node_Manager_Name, do_heart_beat),
  gen_server:cast({global, ?CCM}, {set_monitoring, on}),
  gen_server:cast({global, ?CCM}, init_cluster),

  NodesListFromCCM = gen_server:call({global, ?CCM}, get_nodes),
  ?assertEqual(1, length(NodesListFromCCM)),

  ?assertEqual(ok, nodes_manager:stop_local_test()).


