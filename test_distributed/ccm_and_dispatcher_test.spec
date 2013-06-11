%% slave nodes
{node, ccm, 'ccm@localhost'}.

%% start nodes
{init, [ccm], [{node_start, [{monitor_master, true}]}]}.

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./example"}.

%% test castes to be run
{alias, ccm_and_dispatcher_test, "./ccm_and_dispatcher_test"}.
{cases, [ccm], ccm_and_dispatcher_test, ccm_and_dispatcher_test_SUITE, [modules_start_and_ping_test, dispatcher_connection_test, workers_list_actualization_test, ping_test, application_start_test]}.