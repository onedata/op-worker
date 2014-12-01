%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of cluster_manager.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(cluster_manager_tests).
-include("registered_names.hrl").
-include("records.hrl").

%% TODO dodać testy (raczej ct) sytuacji, w których pada węzeł oraz
%% kiedy jakiś węzeł jest mocno obciążony (równoważenia obciążenia)

%% TODO dodać test mergowania stanu aktualnego ze stanem z bazy

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).


%% ====================================================================
%% Test generation
%% ====================================================================

update_dns_state_test_() ->
  [
    ?_test(update_dns_state__current_worker_host_implementation()),
    ?_test(update_dns_state__unresolveable_ip_address()),
    ?_test(update_dns_state__empty_list()),
    ?_test(calculate_node_load_error()),
    ?_test(calculate_worker_load_error()),
    ?_test(loads_merging_on_error()),
    ?_test(update_dns_state__one_non_dns_worker()),
    ?_test(update_dns_state__one_dns_worker()),
    ?_test(calculate_worker_load__one_worker()),
    ?_test(update_dns_state__many_non_dns_workers()),
    ?_test(update_dns_state__many_non_dns_workers_one_dns_worker()),
    ?_test(update_dns_state__many_non_dns_workers_one_dns_worker_errors()),
    ?_test(calculate_worker_load__many_non_dns_workers()),
    ?_test(update_dns_state__many_non_dns_workers_many_dns_workers())
  ].

%% ====================================================================
%% Test functions
%% ====================================================================

%% This test checks if ccm is resistant to incorrect requests.
wrong_request_test() ->
  application:set_env(?APP_Name, worker_load_memory_size, 1000),
  application:set_env(?APP_Name, initialization_time, 10),
  application:set_env(?APP_Name, cluster_monitoring_period, 300),
  application:set_env(?APP_Name, node_monitoring_initialization, 10),
  application:set_env(?APP_Name, cluster_monitoring_initialization, 30),
  application:set_env(?APP_Name, short_monitoring_time_window, 60),
  application:set_env(?APP_Name, medium_monitoring_time_window, 300),
  application:set_env(?APP_Name, long_monitoring_time_window, 900),

  cluster_manager:start_link(test),

  gen_server:cast({global, ?CCM}, abc),
  Reply = gen_server:call({global, ?CCM}, abc),
  ?assert(Reply =:= wrong_request),
  cluster_manager:stop().

%% ====================================================================
%% Functions used by tests
%% ====================================================================

%% Checks if update_dns_state can work with empty list.
update_dns_state__empty_list() ->
  {NodesLoad, AvgLoad} = cluster_manager:calculate_node_load([], medium),
  WorkersLoad = cluster_manager:calculate_worker_load([]),
  Load = cluster_manager:calculate_load(NodesLoad, WorkersLoad),
  cluster_manager:update_dns_state([], Load, AvgLoad).

calculate_node_load_error() ->
  {NodesLoad, AvgLoad} = cluster_manager:calculate_node_load([n1, n2], medium),
  ?assertEqual(NodesLoad, [{n1, error}, {n2, error}]),
  ?assert(AvgLoad == 0).

calculate_worker_load_error() ->
  Load = cluster_manager:calculate_worker_load([{n1, module, self()}]),
  ?assertEqual([{n1, [{module, error}]}], Load).

loads_merging_on_error() ->
  Load = cluster_manager:calculate_load([{n2, error}, {n1, error}], [{n1, [{module, error}]}]),
  ?assertEqual([{n2, error, []}, {n1, error, [{module, error}]}], Load).

%% Checks if update_dns_state can work with one dns worker.
update_dns_state__one_dns_worker() ->
  {ok, DNS_Gen_Server} = gen_server_mock:new(),

  OneDnsWorker = [{'node@127.0.0.1', dns_worker, DNS_Gen_Server}],
  Load = [{'node@127.0.0.1', 3, [{dns_worker, 1}]}],
  Avg = 3,

  expect_update_state(DNS_Gen_Server, [{dns_worker, [{{127, 0, 0, 1}, 1}]}], [{{127, 0, 0, 1}, 3}]),

  try
    cluster_manager:update_dns_state(OneDnsWorker, Load, Avg)
  after
    assert_expectations_and_stop(DNS_Gen_Server)
  end.

%% Checks if update_dns_state can work with one non dns worker.
update_dns_state__one_non_dns_worker() ->
  {ok, Gen_Server} = gen_server_mock:new(),

  OneNonDnsWorker = [{'node@127.0.0.1', module, Gen_Server}],
  Load = [{'node@127.0.0.1', 3, [{module, 1}]}],
  Avg = 3,

  try
    cluster_manager:update_dns_state(OneNonDnsWorker, Load, Avg)
  after
    assert_expectations_and_stop(Gen_Server)
  end.

%% Checks workers load calculation
calculate_worker_load__one_worker() ->
  {ok, Gen_Server} = gen_server_mock:new(),
  expect_load_info(Gen_Server),

  try
    Load = cluster_manager:calculate_worker_load([{'node@127.0.0.1', module, Gen_Server}]),
    ?assertEqual([{'node@127.0.0.1', [{module, 1.0}]}], Load)
  after
    assert_expectations_and_stop(Gen_Server)
  end.

%% Checks if update_dns_state can work with many non dns workers.
update_dns_state__many_non_dns_workers() ->
  Gen_Servers = gen_server_mock:new(5),

  Times = lists:seq(10, 50, 10),
  Gen_ServersAndTimes = lists:zip(Gen_Servers, Times),

  ManyNonDnsWorkers = lists:map(fun({Gen_Server, Load}) ->
    {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), module, Gen_Server}
  end, Gen_ServersAndTimes),

  Load = lists:map(fun({_Gen_Server, Load}) ->
    {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), 3, [{module, 1}]}
  end, Gen_ServersAndTimes),
  Avg = 3,

  try
    cluster_manager:update_dns_state(ManyNonDnsWorkers, Load, Avg)
  after
    assert_expectations_and_stop(Gen_Servers)
  end.

%% Checks if update_dns_state can work with many non dns workers on same node and one dns worker.
update_dns_state__many_non_dns_workers_one_dns_worker() ->
  Non_DNS_Workers = gen_server_mock:new(5),
  {ok, DNS_Worker} = gen_server_mock:new(),

  Times = lists:seq(10, 50, 10),
  Non_DNS_WorkersAndTimes = lists:zip(Non_DNS_Workers, Times),

  ManyNonDnsWorkers = lists:map(fun({Gen_Server, Load}) ->
    {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), module, Gen_Server}
  end, Non_DNS_WorkersAndTimes),
  Workers = [{'node@192.168.0.60', dns_worker, DNS_Worker} | ManyNonDnsWorkers],

  Load = lists:map(fun({_Gen_Server, Load}) ->
    {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), 3, [{module, 1}]}
  end, Non_DNS_WorkersAndTimes),
  Load2 = [{'node@192.168.0.60', 3, [{dns_worker, 1}]} | Load],
  Avg = 3,

  Expected_DNS_State = [
    {dns_worker, [{{192, 168, 0, 60}, 1}]},
    {module, [{{192, 168, 0, 50}, 1}, {{192, 168, 0, 40}, 1}, {{192, 168, 0, 30}, 1}, {{192, 168, 0, 20}, 1}, {{192, 168, 0, 10}, 1}]}
  ],

  expect_update_state(DNS_Worker, Expected_DNS_State, [{{192, 168, 0, 60}, 3}, {{192, 168, 0, 10}, 3}, {{192, 168, 0, 20}, 3}, {{192, 168, 0, 30}, 3}, {{192, 168, 0, 40}, 3}, {{192, 168, 0, 50}, 3}]),

  try
    cluster_manager:update_dns_state(Workers, Load2, Avg)
  after
    assert_expectations_and_stop([DNS_Worker | Non_DNS_Workers])
  end.

%% Checks if update_dns_state can work with many non dns workers on same node and one dns worker when errors appear
update_dns_state__many_non_dns_workers_one_dns_worker_errors() ->
  Non_DNS_Workers = gen_server_mock:new(5),
  {ok, DNS_Worker} = gen_server_mock:new(),

  Times = lists:seq(10, 50, 10),
  Non_DNS_WorkersAndTimes = lists:zip(Non_DNS_Workers, Times),

  ManyNonDnsWorkers = lists:map(fun({Gen_Server, Load}) ->
    {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), module, Gen_Server}
  end, Non_DNS_WorkersAndTimes),
  WorkersTmp1 = [{'node@192.168.1.1', module, self()} | ManyNonDnsWorkers],
  WorkersTmp2 = [{'node@192.168.0.60', dns_worker, DNS_Worker} | WorkersTmp1],
  WorkersTmp3 = [{'node@192.168.1.1', module3, self()} | WorkersTmp2],
  Workers = [{'node@192.168.1.1', module2, self()} | WorkersTmp3],

  Load = lists:map(fun({_Gen_Server, Load}) ->
    {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), 3, [{module, 1}]}
  end, Non_DNS_WorkersAndTimes),

  ErrorLoad = cluster_manager:calculate_load([{n1, error}, {'node@192.168.1.1', error}], [{n1, [{module, error}, {module2, error}]}, {'node@192.168.1.1', [{module, error}, {module2, error}]}]),
  TmpLoad = [{'node@192.168.0.60', 3, [{dns_worker, 1}]} | Load],
  Load2 = lists:flatten([ErrorLoad, TmpLoad]),
  Avg = 3,

  Expected_DNS_State = [
    {dns_worker, [{{192, 168, 0, 60}, 1}]},
    {module, [{{192, 168, 0, 50}, 1}, {{192, 168, 0, 40}, 1}, {{192, 168, 0, 30}, 1}, {{192, 168, 0, 20}, 1}, {{192, 168, 0, 10}, 1}, {{192, 168, 1, 1}, 1}]},
    {module2, [{{192, 168, 1, 1}, 1}]},
    {module3, [{{192, 168, 1, 1}, 1}]}
  ],

  ExpectedNodes = [{{192, 168, 1, 1}, error}, {{192, 168, 0, 60}, 3}, {{192, 168, 0, 10}, 3}, {{192, 168, 0, 20}, 3}, {{192, 168, 0, 30}, 3}, {{192, 168, 0, 40}, 3}, {{192, 168, 0, 50}, 3}],

  expect_update_state(DNS_Worker, Expected_DNS_State, ExpectedNodes),

  try
    cluster_manager:update_dns_state(Workers, Load2, Avg)
  after
    assert_expectations_and_stop([DNS_Worker | Non_DNS_Workers])
  end.

% Checks workers load calculation
calculate_worker_load__many_non_dns_workers() ->
  Non_DNS_Workers = gen_server_mock:new(5),
  {ok, DNS_Worker} = gen_server_mock:new(),

  Times = lists:seq(10, 50, 10),
  Gen_ServersAndTimes = lists:zip(Non_DNS_Workers, Times),

  ManyNonDnsWorkers = lists:map(fun({Gen_Server, Load}) ->
    {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), module, Gen_Server}
  end, Gen_ServersAndTimes),
  Workers = [{'node@192.168.0.50', dns_worker, DNS_Worker} | ManyNonDnsWorkers],

  expect_load_info([{DNS_Worker, 100} | Gen_ServersAndTimes]),

  ExpectedAns = lists:map(fun({_Gen_Server, Load}) ->
    N = list_to_atom("node@192.168.0." ++ integer_to_list(Load)),
    case Load == 50 of
      true -> {N, [{module, 1.0}, {dns_worker, 1.0}]};
      false -> {N, [{module, 1.0}]}
    end
  end, Gen_ServersAndTimes),

  try
    Load = cluster_manager:calculate_worker_load(Workers),
    ?assertEqual(length(ExpectedAns), length(Load)),
    lists:foreach(fun({{ExpectedN, ExpectedM}, {AnsN, AnsM}}) ->
      ?assertEqual(ExpectedN, AnsN),
      case ExpectedN == 'node@192.168.0.50' of
        true ->
          ?assertEqual(length(ExpectedM), length(AnsM)),
          lists:foreach(fun({{ExpectedM2, _}, {AnsM2, _}}) ->
            ?assertEqual(ExpectedM2, AnsM2)
          end, lists:zip(ExpectedM, AnsM));
        false -> ?assertEqual(ExpectedM, AnsM)
      end
    end, lists:zip(ExpectedAns, Load))
  after
    assert_expectations_and_stop([DNS_Worker | Non_DNS_Workers])
  end.

%% Checks if update_dns_state can work with many non dns workers and many dns workers.
update_dns_state__many_non_dns_workers_many_dns_workers() ->
  Non_DNS_Workers = gen_server_mock:new(5),
  DNS_Workers = gen_server_mock:new(5),

  Times = lists:seq(10, 100, 10),
  WorkersAndTimes = lists:zip(Non_DNS_Workers ++ DNS_Workers, Times),

  Workers = lists:map(fun({Gen_Server, Load}) ->
    case Load > 50 of
      true -> {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), dns_worker, Gen_Server};
      false -> {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), module, Gen_Server}
    end
  end, WorkersAndTimes),

  Load = lists:map(fun({_Gen_Server, Load}) ->
    case Load > 50 of
      true -> {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), 3, [{dns_worker, 1}]};
      false -> {list_to_atom("node@192.168.0." ++ integer_to_list(Load)), 3, [{module, 1}]}
    end
  end, WorkersAndTimes),
  Avg = 3,

  Expected_DNS_State = [
    {dns_worker, [{{192, 168, 0, 100}, 1}, {{192, 168, 0, 90}, 1}, {{192, 168, 0, 80}, 1}, {{192, 168, 0, 70}, 1}, {{192, 168, 0, 60}, 1}]},
    {module, [{{192, 168, 0, 50}, 1}, {{192, 168, 0, 40}, 1}, {{192, 168, 0, 30}, 1}, {{192, 168, 0, 20}, 1}, {{192, 168, 0, 10}, 1}]}
  ],

  ExpectedNodes = [{{192, 168, 0, 10}, 3}, {{192, 168, 0, 20}, 3}, {{192, 168, 0, 30}, 3}, {{192, 168, 0, 40}, 3}, {{192, 168, 0, 50}, 3},
    {{192, 168, 0, 60}, 3}, {{192, 168, 0, 70}, 3}, {{192, 168, 0, 80}, 3}, {{192, 168, 0, 90}, 3}, {{192, 168, 0, 100}, 3}],

  expect_update_state(DNS_Workers, Expected_DNS_State, ExpectedNodes),

  try
    cluster_manager:update_dns_state(Workers, Load, Avg)
  after
    assert_expectations_and_stop(Non_DNS_Workers ++ DNS_Workers)
  end.


%% Checks if update_dns_state can handle unresolveable ip address.
update_dns_state__unresolveable_ip_address() ->
  UnresolveableAddresses = ['node@unresolveable_address', 'node@', '@', '', 'node@256.256.256.1'],

  lists:foreach(fun(NodeName) ->
    update_dns_state_with_wrong_ip_address(NodeName)
  end, UnresolveableAddresses).


%% Checks if update_dns_state can work with current worker host implementation.
update_dns_state__current_worker_host_implementation() ->
  {ok, State} = worker_host:init([dns_worker, test, 100]),
  Ref = erlang:monitor(process, self()),

  {reply, {{MegaS, S, MicroS}, Load}, _} = worker_host:handle_call(getLoadInfo, {self(), Ref}, State),

  ?assert(is_integer(MegaS)),
  ?assert(is_integer(S)),
  ?assert(is_integer(MicroS)),
  ?assert(is_integer(Load)),
  erlang:demonitor(Ref).

%% Checks if dispatcher is updated properly
update_dispatcher_test() ->
  {ok, _} = request_dispatcher:start_link(),

  N1 = node(),
  Loads = [{'node@127.0.0.1', 1}, {N1, 1}],
  Nodes = ['node@127.0.0.1', N1],
  WorkersList = [{N1, fslogic}, {N1, dao_worker}, {n2, fslogic}, {n3, fslogic}, {n3, dao_worker}, {n4, gateway}, {N1, dns_worker}],
  cluster_manager:update_dispatcher_state(WorkersList, [], Nodes, 1, Loads, 1),

  gen_server:cast(?Dispatcher_Name, {update_workers, WorkersList, [], 1, 1, 1}),
  Requests = [fslogic, fslogic, fslogic, fslogic, fslogic, fslogic, fslogic, dao_worker, rtransfer, dao_worker, dns_worker, dns_worker, gateway, gateway],
  ExpectedAns = [n3, n2, N1, N1, n2, n3, n3, n3, non, N1, N1, N1, n4, n4],

  FullAns = lists:foldl(fun(R, TmpAns) ->
    Ans = gen_server:call(?Dispatcher_Name, {get_worker_node, R}),
    [Ans | TmpAns]
  end, [], Requests),
  ?assertEqual(ExpectedAns, lists:reverse(FullAns)),

  request_dispatcher:stop().

map_nodes_stats_to_load_test() ->
  NodesStats = [
    {node1, [{"cpu", 10}, {"mem", 80}, {"net_rx_b_eth0", 1}, {"net_tx_b_eth0", 2}, {"net_rx_pps_eth0", 3},
      {"net_tx_pps_eth0", 4}, {"ports_rx_b", 5}, {"ports_tx_b", 6}]},
    {node2, [{"cpu", 20}, {"core0", 10}, {"core1", 10}, {"mem", 85}, {"net_rx_b_eth1", 2}, {"net_tx_b_eth1", 3},
      {"net_rx_pps_eth1", 4}, {"net_tx_pps_eth1", 5}, {"ports_rx_b", 6}, {"ports_tx_b", 7}]},
    {node3, [{"cpu", 30}, {"mem", 90}, {"net_rx_b_wlan0", 3}, {"net_tx_b_wlan0", 4}, {"net_rx_pps_wlan0", 5},
      {"net_tx_pps_wlan0", 6}, {"ports_rx_b", 7}, {"ports_tx_b", 8}]},
    {node4, undefined},
    {node5, [{"cpu", 40}, {"mem", 95}, {"net_rx_b_wlan1", 4}, {"net_tx_b_wlan1", 5}, {"net_rx_pps_wlan1", 6},
      {"net_tx_pps_wlan1", 7}, {"ports_rx_b", 8}, {"ports_tx_b", 9}]}
  ],
  Node1 = 100 * 10 / 40 + 100 * 80 / 95 + 1 / 4 + 2 / 5 + 3 / 6 + 4 / 7 + 5 / 8 + 6 / 9,
  Node2 = 100 * 20 / 40 + 100 * 85 / 95 + 2 / 4 + 3 / 5 + 4 / 6 + 5 / 7 + 6 / 8 + 7 / 9,
  Node3 = 100 * 30 / 40 + 100 * 90 / 95 + 3 / 4 + 4 / 5 + 5 / 6 + 6 / 7 + 7 / 8 + 8 / 9,
  Node4 = error,
  Node5 = 100 * 40 / 40 + 100 * 95 / 95 + 4 / 4 + 5 / 5 + 6 / 6 + 7 / 7 + 8 / 8 + 9 / 9,
  ExpectedLoad = {[
    {node1, Node1},
    {node2, Node2},
    {node3, Node3},
    {node4, Node4},
    {node5, Node5}
  ], (Node1 + Node2 + Node3 + Node5) / 4},
  ActualLoad = cluster_manager:map_node_stats_to_load(NodesStats),
  ?assertEqual(ExpectedLoad, ActualLoad).

merge_nodes_stats_test() ->
  NodesStats = [
    {node1, [{"cpu", 10}, {"mem", 80}, {"net_rx_b_eth0", 1}, {"net_tx_b_eth0", 2}, {"net_rx_pps_eth0", 3},
      {"net_tx_pps_eth0", 4}, {"ports_rx_b", 5}, {"ports_tx_b", 6}]},
    {node2, [{"cpu", 20}, {"core0", 10}, {"core1", 10}, {"mem", 85}, {"net_rx_b_eth1", 2}, {"net_tx_b_eth1", 3},
      {"net_rx_pps_eth1", 4}, {"net_tx_pps_eth1", 5}, {"ports_rx_b", 6}, {"ports_tx_b", 7}]},
    {node3, [{"cpu", 30}, {"mem", 90}, {"net_rx_b_wlan0", 3}, {"net_tx_b_wlan0", 4}, {"net_rx_pps_wlan0", 5},
      {"net_tx_pps_wlan0", 6}, {"ports_rx_b", 7}, {"ports_tx_b", 8}]},
    {node4, undefined},
    {node5, [{"cpu", 40}, {"mem", 95}, {"net_rx_b_wlan1", 4}, {"net_tx_b_wlan1", 5}, {"net_rx_pps_wlan1", 6},
      {"net_tx_pps_wlan1", 7}, {"ports_rx_b", 8}, {"ports_tx_b", 9}]}
  ],
  ExpectedMergedStats = [
    (10 + 20 + 30 + 40) / 4, (80 + 85 + 90 + 95) / 4, (1 + 2 + 3 + 4) / 4, (2 + 3 + 4 + 5) / 4,
    (3 + 4 + 5 + 6) / 4, (4 + 5 + 6 + 7) / 4, (5 + 6 + 7 + 8) / 4, (6 + 7 + 8 + 9) / 4
  ],
  ActualMergedStats = cluster_manager:merge_nodes_stats(NodesStats),
  ?assertEqual(ExpectedMergedStats, ActualMergedStats).

count_required_permanent_workers_test() ->
  PermanentModules1 = [],
  ModulesWithArgs = [{central_logger, []}, {cluster_rengine, []}, {control_panel, []}, {dao_worker, []}, {fslogic, []}, {gateway, []}, {rtransfer, []}, {rule_manager, []}, {dns_worker, []}, {remote_files_manager, []}],
  State1 = #cm_state{workers = [{"ccm_worker1@veildev_45.local", dao_worker, list_to_pid("<0.39.0>")},
                                {"ccm_worker1@veildev_45.local", cluster_rengine, list_to_pid("<0.39.0>")},
                                {"ccm_worker1@veildev_45.local", gateway, list_to_pid("<0.39.0>")},
                                {"ccm_worker2@veildev_45.local", dao_worker, list_to_pid("<0.39.0>")},
                                {"ccm_worker2@veildev_45.local", control_panel, list_to_pid("<0.39.0>")},
                                {"ccm_worker2@veildev_45.local", rtransfer, list_to_pid("<0.39.0>")}
                               ]},
  Nodes = ["ccm_worker1@veildev_45.local", "ccm_worker2@veildev_45.local", "ccm_worker3@veildev_45.local"],

  CompareWorkerLists = fun(WorkersExpected, Workers) ->
                         ?assertEqual(length(WorkersExpected),length(Workers)),
                         FilteredWorkers = [Worker || Worker <- Workers, lists:member(Worker, WorkersExpected)],
                         ?assertEqual(length(WorkersExpected),length(FilteredWorkers))
                       end,
  {PermamentWorkers1, RequiredPermamentWorkers1} = cluster_manager:required_permanent_workers(PermanentModules1, ModulesWithArgs, State1, Nodes),
  CompareWorkerLists([], PermamentWorkers1),
  CompareWorkerLists([], RequiredPermamentWorkers1),

  PermanentModules2 = [gateway],
  {PermamentWorkers2, RequiredPermamentWorkers2} = cluster_manager:required_permanent_workers(PermanentModules2, ModulesWithArgs, State1, Nodes),
  CompareWorkerLists([{"ccm_worker1@veildev_45.local", gateway}], PermamentWorkers2),
  CompareWorkerLists([{"ccm_worker2@veildev_45.local", gateway, []},
                {"ccm_worker3@veildev_45.local", gateway, []}
               ], RequiredPermamentWorkers2),


  PermanentModules3 = [gateway, dao_worker],
  {PermamentWorkers3, RequiredPermamentWorkers3} = cluster_manager:required_permanent_workers(PermanentModules3, ModulesWithArgs, State1, Nodes),
  CompareWorkerLists([{"ccm_worker2@veildev_45.local", dao_worker},
                {"ccm_worker1@veildev_45.local", gateway},
                {"ccm_worker1@veildev_45.local", dao_worker}
               ], PermamentWorkers3),
  CompareWorkerLists([{"ccm_worker2@veildev_45.local", gateway, []},
                {"ccm_worker3@veildev_45.local", gateway, []},
                {"ccm_worker3@veildev_45.local", dao_worker, []}
               ], RequiredPermamentWorkers3),

  PermanentModules4 = [gateway, dao_worker, fslogic],
  {PermamentWorkers4, RequiredPermamentWorkers4} = cluster_manager:required_permanent_workers(PermanentModules4, ModulesWithArgs, State1, Nodes),
  CompareWorkerLists([{"ccm_worker2@veildev_45.local", dao_worker},
                {"ccm_worker1@veildev_45.local", gateway},
                {"ccm_worker1@veildev_45.local", dao_worker}
               ], PermamentWorkers4),
  CompareWorkerLists([{"ccm_worker1@veildev_45.local", fslogic, []},
                {"ccm_worker2@veildev_45.local", gateway, []},
                {"ccm_worker2@veildev_45.local", fslogic, []},
                {"ccm_worker3@veildev_45.local", gateway, []},
                {"ccm_worker3@veildev_45.local", dao_worker, []},
                {"ccm_worker3@veildev_45.local", fslogic, []}
               ], RequiredPermamentWorkers4).

%% ====================================================================
%% Helping functions
%% ====================================================================

%% Helper function for testing update_dns_state with wrong ip address.
update_dns_state_with_wrong_ip_address(NodeName) ->
  {ok, DNS_Gen_Server} = gen_server_mock:new(),

  OneDnsWorker = [{NodeName, dns_worker, DNS_Gen_Server}],
  Load = [{NodeName, 3, [{dns_worker, 1}]}],
  Avg = 3,

  expect_update_state(DNS_Gen_Server, [], []),

  cluster_manager:update_dns_state(OneDnsWorker, Load, Avg),

  assert_expectations_and_stop(DNS_Gen_Server).

%% Helper function for setting getLoadInfo mock expectation.
expect_load_info(Gen_Server, Load) ->
  gen_server_mock:expect_call(Gen_Server, fun(getLoadInfo, _From, State) ->
    {ok, {{0, 0, 1000}, Load}, State}
  end).

%% Helper function for setting getLoadInfo mock expectation.
expect_load_info(Gen_ServersAndTimes) when is_list(Gen_ServersAndTimes) ->
  lists:foreach(fun({Gen_Server, Load}) ->
    expect_load_info(Gen_Server, Load)
  end, Gen_ServersAndTimes);

expect_load_info(Gen_Server) ->
  expect_load_info(Gen_Server, 1000).

%% Helper function for setting update_state mock expectation with specified state.
expect_update_state(Gen_Servers, Expected_State, ExpectedNodes) when is_list(Gen_Servers) ->
  lists:foreach(fun(Gen_Server) ->
    expect_update_state(Gen_Server, Expected_State, ExpectedNodes)
  end, Gen_Servers);

expect_update_state(Gen_Server, Expected_State, ExpectedNodes) ->
  gen_server_mock:expect_cast(Gen_Server, fun({asynch, 1, {update_state, Actual_State, Nodes, _}}, _State) ->
    ?assertEqual(Expected_State, Actual_State),
    ?assertEqual(ExpectedNodes, Nodes)
  end).

%% Helper function - shortcut for gen_server_mock:assert_expectations and gen_server_mock:stop.
assert_expectations_and_stop([]) ->
  ok;

assert_expectations_and_stop([Gen_Server | Gen_Servers]) ->
  try
    assert_expectations_and_stop(Gen_Server)
  after
    assert_expectations_and_stop(Gen_Servers)
  end;

assert_expectations_and_stop(Gen_Server) ->
  try
    gen_server_mock:assert_expectations(Gen_Server)
  after
    gen_server_mock:stop(Gen_Server)
  end.

-endif.
