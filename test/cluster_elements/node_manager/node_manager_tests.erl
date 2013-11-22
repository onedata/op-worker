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

%% TODO sprawdzić funkcję get_node_stats (raczej testy ct niż eunit)

-module(node_manager_tests).
-include("registered_names.hrl").
-include("records.hrl").

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
  application:set_env(?APP_Name, node_monitoring_period, 15),
	node_manager:start_link(test_worker),
	gen_server:cast(?Node_Manager_Name, abc),
	Reply = gen_server:call(?Node_Manager_Name, abc),
	?assert(Reply =:= wrong_request),
  node_manager:stop().

%% This test checks if node_manager is able to properly identify type of node which it coordinates.
node_type_test() ->
  application:set_env(?APP_Name, ccm_nodes, [not_existing_node]),
  application:set_env(?APP_Name, heart_beat, 60),
  application:set_env(?APP_Name, node_monitoring_period, 15),
  node_manager:start_link(test_worker),
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
  application:set_env(?APP_Name, node_monitoring_period, 15),

  node_manager:start_link(test_worker),
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

callbacks_test() ->
  State = #node_state{},
  SampleCallbacks = [{fuse1, pid1}, {fuse1, pid2}, {fuse2, pid1}, {fuse2, pid2}, {fuse1, pid3}, {fuse3, pid1}],
  StateAfterAdd = lists:foldl(fun({Fuse, Pid}, TmpState) ->
    node_manager:addCallback(TmpState, Fuse, Pid)
  end, State, SampleCallbacks),
  ?assertEqual([{fuse3,{[pid1],[]}}, {fuse2,{[pid2,pid1],[]}}, {fuse1,{[pid3,pid2,pid1],[]}}], StateAfterAdd#node_state.callbacks),

  {Get1, State1} = node_manager:get_callback(StateAfterAdd, fuse1),
  ?assertEqual(pid3, Get1),
  ?assertEqual([{fuse1,{[pid2,pid1],[pid3]}}, {fuse2,{[pid2,pid1],[]}}, {fuse3,{[pid1],[]}}], State1#node_state.callbacks),
  {Get2, State2} = node_manager:get_callback(State1, fuse1),
  ?assertEqual(pid2, Get2),
  ?assertEqual([{fuse1,{[pid1],[pid2, pid3]}}, {fuse2,{[pid2,pid1],[]}}, {fuse3,{[pid1],[]}}], State2#node_state.callbacks),
  {Get3, State3} = node_manager:get_callback(State2, fuse1),
  ?assertEqual(pid1, Get3),
  ?assertEqual([{fuse1,{[],[pid1, pid2, pid3]}}, {fuse2,{[pid2,pid1],[]}}, {fuse3,{[pid1],[]}}], State3#node_state.callbacks),
  {Get4, State4} = node_manager:get_callback(State3, fuse1),
  ?assertEqual(pid1, Get4),
  ?assertEqual([{fuse1,{[pid2, pid3],[pid1]}}, {fuse2,{[pid2,pid1],[]}}, {fuse3,{[pid1],[]}}], State4#node_state.callbacks),

  {Get5, State5} = node_manager:get_callback(State4, fuse2),
  ?assertEqual(pid2, Get5),
  ?assertEqual([{fuse2,{[pid1],[pid2]}}, {fuse3,{[pid1],[]}}, {fuse1,{[pid2, pid3],[pid1]}}], State5#node_state.callbacks),
  {Get6, State6} = node_manager:get_callback(State5, fuse2),
  ?assertEqual(pid1, Get6),
  ?assertEqual([{fuse2,{[],[pid1, pid2]}}, {fuse3,{[pid1],[]}}, {fuse1,{[pid2, pid3],[pid1]}}], State6#node_state.callbacks),
  {Get7, State7} = node_manager:get_callback(State6, fuse2),
  ?assertEqual(pid1, Get7),
  ?assertEqual([{fuse2,{[pid2],[pid1]}}, {fuse3,{[pid1],[]}}, {fuse1,{[pid2, pid3],[pid1]}}], State7#node_state.callbacks),

  {Get8, State8} = node_manager:get_callback(State7, fuse3),
  ?assertEqual(pid1, Get8),
  ?assertEqual([{fuse3,{[],[pid1]}}, {fuse1,{[pid2, pid3],[pid1]}}, {fuse2,{[pid2],[pid1]}}], State8#node_state.callbacks),
  {Get9, State9} = node_manager:get_callback(State8, fuse3),
  ?assertEqual(pid1, Get9),
  ?assertEqual([{fuse3,{[],[pid1]}}, {fuse1,{[pid2, pid3],[pid1]}}, {fuse2,{[pid2],[pid1]}}], State9#node_state.callbacks),

  {State10, DeleteAns1} = node_manager:delete_callback(State9, fuse1, pid2),
  ?assertEqual([{fuse1,{[pid3],[pid1]}}, {fuse3,{[],[pid1]}}, {fuse2,{[pid2],[pid1]}}], State10#node_state.callbacks),
  ?assertEqual(pid_deleted, DeleteAns1),
  {State11, DeleteAns2} = node_manager:delete_callback(State10, fuse1, pid2),
  ?assertEqual([{fuse1,{[pid3],[pid1]}}, {fuse3,{[],[pid1]}}, {fuse2,{[pid2],[pid1]}}], State11#node_state.callbacks),
  ?assertEqual(pid_not_found, DeleteAns2),
  {State12, DeleteAns3} = node_manager:delete_callback(State11, fuse4, pid2),
  ?assertEqual([{fuse2,{[pid2],[pid1]}}, {fuse3,{[],[pid1]}}, {fuse1,{[pid3],[pid1]}}], State12#node_state.callbacks),
  ?assertEqual(fuse_not_found, DeleteAns3),
  {State13, DeleteAns4} = node_manager:delete_callback(State12, fuse3, pid1),
  ?assertEqual([{fuse2,{[pid2],[pid1]}}, {fuse1,{[pid3],[pid1]}}], State13#node_state.callbacks),
  ?assertEqual(fuse_deleted, DeleteAns4).

-endif.