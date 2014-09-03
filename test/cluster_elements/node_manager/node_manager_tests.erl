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
  application:set_env(?APP_Name, node_monitoring_initialization, 10),
  application:set_env(?APP_Name, cluster_monitoring_initialization, 30),
  application:set_env(?APP_Name, short_monitoring_time_window, 60),
  application:set_env(?APP_Name, medium_monitoring_time_window, 300),
  application:set_env(?APP_Name, long_monitoring_time_window, 900),
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
  application:set_env(?APP_Name, node_monitoring_initialization, 10),
  application:set_env(?APP_Name, cluster_monitoring_initialization, 30),
  application:set_env(?APP_Name, short_monitoring_time_window, 60),
  application:set_env(?APP_Name, medium_monitoring_time_window, 300),
  application:set_env(?APP_Name, long_monitoring_time_window, 900),
  meck:new(veil_cluster_node_app),
  meck:expect(veil_cluster_node_app, ports_ok, fun() -> true end),
  node_manager:start_link(test_worker),
  NodeType = gen_server:call(?Node_Manager_Name, getNodeType),
  ?assert(NodeType =:= worker),
  node_manager:stop(),
  meck:unload(veil_cluster_node_app).

%% This test checks if node manager is able to register in ccm.
heart_beat_test() ->
  net_kernel:start([node1, shortnames]),

  application:set_env(?APP_Name, heart_beat, 60),
  application:set_env(?APP_Name, ccm_nodes, [not_existing_node, node()]),
  application:set_env(?APP_Name, worker_load_memory_size, 1000),
  application:set_env(?APP_Name, initialization_time, 10),
  application:set_env(?APP_Name, cluster_monitoring_period, 300),
  application:set_env(?APP_Name, node_monitoring_period, 15),
  application:set_env(?APP_Name, node_monitoring_initialization, 10),
  application:set_env(?APP_Name, cluster_monitoring_initialization, 30),
  application:set_env(?APP_Name, short_monitoring_time_window, 60),
  application:set_env(?APP_Name, medium_monitoring_time_window, 300),
  application:set_env(?APP_Name, long_monitoring_time_window, 900),

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
  ?assertEqual([{fuse3, {[pid1], []}}, {fuse2, {[pid2, pid1], []}}, {fuse1, {[pid3, pid2, pid1], []}}], StateAfterAdd#node_state.callbacks),

  {Get1, State1} = node_manager:get_callback(StateAfterAdd, fuse1),
  ?assertEqual(pid3, Get1),
  ?assertEqual([{fuse1, {[pid2, pid1], [pid3]}}, {fuse2, {[pid2, pid1], []}}, {fuse3, {[pid1], []}}], State1#node_state.callbacks),
  {Get2, State2} = node_manager:get_callback(State1, fuse1),
  ?assertEqual(pid2, Get2),
  ?assertEqual([{fuse1, {[pid1], [pid2, pid3]}}, {fuse2, {[pid2, pid1], []}}, {fuse3, {[pid1], []}}], State2#node_state.callbacks),
  {Get3, State3} = node_manager:get_callback(State2, fuse1),
  ?assertEqual(pid1, Get3),
  ?assertEqual([{fuse1, {[], [pid1, pid2, pid3]}}, {fuse2, {[pid2, pid1], []}}, {fuse3, {[pid1], []}}], State3#node_state.callbacks),
  {Get4, State4} = node_manager:get_callback(State3, fuse1),
  ?assertEqual(pid1, Get4),
  ?assertEqual([{fuse1, {[pid2, pid3], [pid1]}}, {fuse2, {[pid2, pid1], []}}, {fuse3, {[pid1], []}}], State4#node_state.callbacks),

  {Get5, State5} = node_manager:get_callback(State4, fuse2),
  ?assertEqual(pid2, Get5),
  ?assertEqual([{fuse2, {[pid1], [pid2]}}, {fuse3, {[pid1], []}}, {fuse1, {[pid2, pid3], [pid1]}}], State5#node_state.callbacks),
  {Get6, State6} = node_manager:get_callback(State5, fuse2),
  ?assertEqual(pid1, Get6),
  ?assertEqual([{fuse2, {[], [pid1, pid2]}}, {fuse3, {[pid1], []}}, {fuse1, {[pid2, pid3], [pid1]}}], State6#node_state.callbacks),
  {Get7, State7} = node_manager:get_callback(State6, fuse2),
  ?assertEqual(pid1, Get7),
  ?assertEqual([{fuse2, {[pid2], [pid1]}}, {fuse3, {[pid1], []}}, {fuse1, {[pid2, pid3], [pid1]}}], State7#node_state.callbacks),

  {Get8, State8} = node_manager:get_callback(State7, fuse3),
  ?assertEqual(pid1, Get8),
  ?assertEqual([{fuse3, {[], [pid1]}}, {fuse1, {[pid2, pid3], [pid1]}}, {fuse2, {[pid2], [pid1]}}], State8#node_state.callbacks),
  {Get9, State9} = node_manager:get_callback(State8, fuse3),
  ?assertEqual(pid1, Get9),
  ?assertEqual([{fuse3, {[], [pid1]}}, {fuse1, {[pid2, pid3], [pid1]}}, {fuse2, {[pid2], [pid1]}}], State9#node_state.callbacks),

  {State10, DeleteAns1} = node_manager:delete_callback(State9, fuse1, pid2),
  ?assertEqual([{fuse1, {[pid3], [pid1]}}, {fuse3, {[], [pid1]}}, {fuse2, {[pid2], [pid1]}}], State10#node_state.callbacks),
  ?assertEqual(pid_deleted, DeleteAns1),
  {State11, DeleteAns2} = node_manager:delete_callback(State10, fuse1, pid2),
  ?assertEqual([{fuse1, {[pid3], [pid1]}}, {fuse3, {[], [pid1]}}, {fuse2, {[pid2], [pid1]}}], State11#node_state.callbacks),
  ?assertEqual(pid_not_found, DeleteAns2),
  {State12, DeleteAns3} = node_manager:delete_callback(State11, fuse4, pid2),
  ?assertEqual([{fuse2, {[pid2], [pid1]}}, {fuse3, {[], [pid1]}}, {fuse1, {[pid3], [pid1]}}], State12#node_state.callbacks),
  ?assertEqual(fuse_not_found, DeleteAns3),
  {State13, DeleteAns4} = node_manager:delete_callback(State12, fuse3, pid1),
  ?assertEqual([{fuse2, {[pid2], [pid1]}}, {fuse1, {[pid3], [pid1]}}], State13#node_state.callbacks),
  ?assertEqual(fuse_deleted, DeleteAns4).

calculate_network_stats_test() ->
  RxBytes = 1000, TxBytes = 2000, RxPackets = 3000, TxPackets = 4000,
  NetworkStats = [{<<"net_rx_b_eth0">>, 0}, {<<"net_tx_b_eth0">>, 0},
    {<<"net_rx_pps_eth0">>, 0}, {<<"net_tx_pps_eth0">>, 0}],
  CurrentNetworkStats = [{<<"net_rx_b_eth0">>, RxBytes}, {<<"net_tx_b_eth0">>, TxBytes},
    {<<"net_rx_pps_eth0">>, RxPackets}, {<<"net_rx_pps_eth0">>, TxPackets}],
  ExpectedNetworkStats = [{<<"net_rx_b_eth0">>, RxBytes}, {<<"net_tx_b_eth0">>, TxBytes},
    {<<"net_rx_pps_eth0">>, RxPackets}, {<<"net_rx_pps_eth0">>, TxPackets}],
  ActualNetworkStats = node_manager:calculate_network_stats(CurrentNetworkStats, NetworkStats, []),
  ?assertEqual(ExpectedNetworkStats, ActualNetworkStats).

get_interface_stats_test() ->
  ExpectedInterfaceStats = 1000,
  meck:new(file, [unstick, passthrough]),
  meck:expect(file, open, fun(_, [raw]) -> {ok, fd} end),
  meck:expect(file, read_line, fun(fd) -> {ok, integer_to_list(ExpectedInterfaceStats) ++ "\n"} end),
  meck:expect(file, close, fun(fd) -> ok end),
  ActualInterfaceStats = node_manager:get_interface_stats("eth0", "rx_bytes"),
  ?assert(meck:validate(file)),
  ?assertEqual(ok, meck:unload(file)),
  ?assertEqual(ExpectedInterfaceStats, ActualInterfaceStats).

get_single_core_cpu_stats_test() ->
  SampleFile = create_sample_cpu_stats_file(1),
  ?assertNotEqual(error, SampleFile),
  ExpectedCpuStats = [{<<"cpu">>, 100 * 60 / 450}],
  {ok, Fd} = SampleFile,
  meck:new(file, [unstick, passthrough]),
  meck:expect(file, open, fun("/proc/stat", [read]) -> {ok, Fd} end),
  ActualCpuStats = node_manager:get_cpu_stats([{<<"core0">>, 0, 0}, {<<"cpu">>, 0, 0}]),
  ?assert(meck:validate(file)),
  ?assertEqual(ok, meck:unload(file)),
  ?assertEqual(ExpectedCpuStats, ActualCpuStats).

get_multi_core_cpu_stats_test() ->
  SampleFile = create_sample_cpu_stats_file(4),
  ?assertNotEqual(error, SampleFile),
  ExpectedCpuStats = [{<<"cpu">>, 100 * 60 / 450}, {<<"core0">>, 100 * 60 / 450}, {<<"core1">>, 100 * 60 / 450},
    {<<"core2">>, 100 * 60 / 450}, {<<"core3">>, 100 * 60 / 450}],
  {ok, Fd} = SampleFile,
  meck:new(file, [unstick, passthrough]),
  meck:expect(file, open, fun("/proc/stat", [read]) -> {ok, Fd} end),
  ActualCpuStats = node_manager:get_cpu_stats([{<<"core3">>, 0, 0}, {<<"core2">>, 0, 0}, {<<"core1">>, 0, 0},
    {<<"core0">>, 0, 0}, {<<"cpu">>, 0, 0}]),
  ?assert(meck:validate(file)),
  ?assertEqual(ok, meck:unload(file)),
  ?assertEqual(ExpectedCpuStats, ActualCpuStats).

calculate_ports_transfer_test() ->
  PortsStats = [{p1, 100, 40}, {p2, 300, 100}, {p3, 100, 200}],
  ExistingPortsStats = [{p1, 120, 200}, {p2, 310, 500}, {p4, 50, 10}],
  ExpectedPortsTransfer = [{<<"ports_rx_b">>, 20 + 10 + 50}, {<<"ports_tx_b">>, 160 + 400 + 10}],
  ActualPortsTransfer = node_manager:calculate_ports_transfer(PortsStats, ExistingPortsStats, 0, 0),
  ?assertEqual(ExpectedPortsTransfer, ActualPortsTransfer).

get_memory_stats_test() ->
  SampleFile = create_sample_memory_stats_file(),
  ?assertNotEqual(error, SampleFile),
  {ok, Fd, ExpectedMemoryStats} = SampleFile,
  meck:new(file, [unstick, passthrough]),
  meck:expect(file, open, fun("/proc/meminfo", [read]) -> {ok, Fd} end),
  ActualMemoryStats = node_manager:get_memory_stats(),
  ?assert(meck:validate(file)),
  ?assertEqual(ok, meck:unload(file)),
  ?assertEqual(ExpectedMemoryStats, ActualMemoryStats).

is_valid_name_test() ->
  ?assert(node_manager:is_valid_name("azAZ09_", 12)),
  ?assertNot(node_manager:is_valid_name("azAZ09_", 13)),
  ?assertNot(node_manager:is_valid_name("", 20)),
  ?assertNot(node_manager:is_valid_name("", 10)),
  ?assertNot(node_manager:is_valid_name("az.AZ", 1)).

is_valid_character_test() ->
  ?assert(node_manager:is_valid_character($a)),
  ?assert(node_manager:is_valid_character($z)),
  ?assert(node_manager:is_valid_character($A)),
  ?assert(node_manager:is_valid_character($Z)),
  ?assert(node_manager:is_valid_character($0)),
  ?assert(node_manager:is_valid_character($9)),
  ?assert(node_manager:is_valid_character($_)),
  ?assertNot(node_manager:is_valid_character($.)),
  ?assertNot(node_manager:is_valid_character($/)).


%% ====================================================================
%% Helper functions
%% ====================================================================

create_sample_cpu_stats_file(Cores) ->
  File = "/tmp/sample_cpu_stats",
  case file:open(File, [write]) of
    {ok, WriteFd} ->
      file:write(WriteFd, "cpu 10 20 30 40 50 60 70 80 90\n"),
      write_core_stats(WriteFd, 1, Cores),
      case file:close(WriteFd) of
        ok -> case file:open(File, [read]) of
                {ok, ReadFd} -> {ok, ReadFd};
                _ -> error
              end;
        _ -> error
      end;
    _ -> error
  end.

write_core_stats(WriteFd, Cores, Cores) ->
  file:write(WriteFd, "cpu" ++ integer_to_list(Cores - 1) ++ " 10 20 30 40 50 60 70 80 90\n");
write_core_stats(WriteFd, Core, Cores) ->
  file:write(WriteFd, "cpu" ++ integer_to_list(Core - 1) ++ " 10 20 30 40 50 60 70 80 90\n"),
  write_core_stats(WriteFd, Core + 1, Cores).

create_sample_memory_stats_file() ->
  File = "/tmp/sample_memory_stats",
  MemTotal = 1000,
  MemFree = 300,
  case file:open(File, [write]) of
    {ok, WriteFd} ->
      file:write(WriteFd, "MemTotal: " ++ integer_to_list(MemTotal) ++ " kB\n"),
      file:write(WriteFd, "Buffers: 20208 kB\n"),
      file:write(WriteFd, "MemFree: " ++ integer_to_list(MemFree) ++ " kB\n"),
      file:write(WriteFd, "Cached: 218904 kB\n"),
      case file:close(WriteFd) of
        ok -> case file:open(File, [read]) of
                {ok, ReadFd} -> {ok, ReadFd, [{<<"mem">>, 100 * (MemTotal - MemFree) / MemTotal}]};
                _ -> error
              end;
        _ -> error
      end;
    _ -> error
  end.

-endif.
