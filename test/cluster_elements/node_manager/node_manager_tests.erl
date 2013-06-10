%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of node_manager.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(node_manager_tests).
-include("registered_names.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

%% ====================================================================
%% Test functions
%% ====================================================================

%% This test checks if node_manager is resistant to incorrect requests.
wrong_request_test() ->
  application:set_env(?APP_Name, ccm_nodes, [not_existing_node]),
  application:set_env(?APP_Name, heart_beat, 60),
	node_manager:start_link(worker),
	gen_server:cast(?Node_Manager_Name, abc),
	Reply = gen_server:call(?Node_Manager_Name, abc),
	?assert(Reply =:= wrong_request),
  node_manager:stop().

%% This test checks if node_manager is able to properly identify type of node which it coordinates.
node_type_test() ->
  application:set_env(?APP_Name, ccm_nodes, [not_existing_node]),
  application:set_env(?APP_Name, heart_beat, 60),
  node_manager:start_link(worker),
	NodeType = gen_server:call(?Node_Manager_Name, getNodeType),
	?assert(NodeType =:= worker),
  node_manager:stop().

%% This test checks if node manager is able to register in ccm.
heart_beat_test() ->
	net_kernel:start([node1, shortnames]),

	application:set_env(?APP_Name, heart_beat, 60),
	application:set_env(?APP_Name, ccm_nodes, [not_existing_node, node()]),
  application:set_env(?APP_Name, worker_load_memory_size, 1000),
  application:set_env(?APP_Name, hot_swapping_time, 10000),
  application:set_env(?APP_Name, initialization_time, 10),
  application:set_env(?APP_Name, cluster_clontrol_period, 300),

  node_manager:start_link(worker),
  cluster_manager:start_link(test),

	timer:sleep(500),

	Ccm_status = gen_server:call(?Node_Manager_Name, get_ccm_connection_status),
	?assert(Ccm_status =:= connected),

	application:set_env(?APP_Name, ccm_nodes, [not_existing_node]),
	ok = gen_server:cast(?Node_Manager_Name, reset_ccm_connection),
  timer:sleep(50),
	Ccm_status2 = gen_server:call(?Node_Manager_Name, get_ccm_connection_status),
	?assert(Ccm_status2 =:= not_connected),

  node_manager:stop(),
  cluster_manager:stop(),
	net_kernel:stop().

-endif.