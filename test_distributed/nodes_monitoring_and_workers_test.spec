%% slave nodes
{node, ccm1, 'ccm1@localhost'}.
{node, worker1, 'worker1@localhost'}.
{node, worker2, 'worker2@localhost'}.
{node, worker3, 'worker3@localhost'}.
{node, tester, 'tester@localhost'}.

%% start nodes
{init, [ccm1, worker1, worker2, worker3, tester], [{node_start, [{monitor_master, true}]}]}.

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./nodes_monitoring_and_workers_test"}.

%% test castes to be run
{alias, nodes_monitoring_and_workers_test, "./nodes_monitoring_and_workers_test"}.
{cases, [ccm1], nodes_monitoring_and_workers_test, nodes_monitoring_and_workers_test_SUITE, [ccm1_test]}.
{cases, [worker1], nodes_monitoring_and_workers_test, nodes_monitoring_and_workers_test_SUITE, [worker1_test]}.
{cases, [worker2], nodes_monitoring_and_workers_test, nodes_monitoring_and_workers_test_SUITE, [worker2_test]}.
{cases, [worker3], nodes_monitoring_and_workers_test, nodes_monitoring_and_workers_test_SUITE, [worker3_test]}.
{cases, [tester], nodes_monitoring_and_workers_test, nodes_monitoring_and_workers_test_SUITE, [tester_test]}.