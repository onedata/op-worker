{node, ccm1, 'ccm1@localhost'}.
{node, ccm2, 'ccm2@localhost'}.

{init, [ccm1, ccm2], [{node_start, [{monitor_master, true}]}]}.

{logdir, "../../logs/"}.

{alias, example, "./example"}.
{cases, [ccm1], example, example_SUITE, [ccm1_test]}.
{cases, [ccm2], example, example_SUITE, [ccm2_test]}.