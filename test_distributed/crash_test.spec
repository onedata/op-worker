%% slave nodes
{node, ccm1, 'ccm1@localhost'}.
{node, ccm2, 'ccm2@localhost'}.
{node, worker, 'worker@localhost'}.
{node, tester, 'tester@localhost'}.

%% start nodes
{init, [ccm1, ccm2, worker, tester], [{node_start, [{monitor_master, true}]}]}.

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./crash_test"}.

%% test castes to be run
{alias, crash_test, "./crash_test"}.
{cases, [ccm1], crash_test, crash_test_SUITE, [ccm1_test]}.
{cases, [ccm2], crash_test, crash_test_SUITE, [ccm2_test]}.
{cases, [worker], crash_test, crash_test_SUITE, [worker_test]}.
{cases, [tester], crash_test, crash_test_SUITE, [tester_test]}.