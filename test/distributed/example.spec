%% slave nodes
{node, ccm1, 'ccm1@localhost'}.
{node, ccm2, 'ccm2@localhost'}.

%% start nodes
{init, [ccm1, ccm2], [{node_start, [{monitor_master, true}]}]}.

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./example"}.

%% test castes to be run
{alias, example, "./example"}.
{cases, [ccm1], example, example_SUITE, [ccm1_test]}.
{cases, [ccm2], example, example_SUITE, [ccm2_test]}.