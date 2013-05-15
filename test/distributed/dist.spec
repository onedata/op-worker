{node, ccm1, 'ccm1@plgsl63.local'}.
{node, ccm2, 'ccm2@plgsl63.local'}.

{init, [ccm1, ccm2], [{node_start, [{monitor_master, true}]}]}.
%%{init, [ccm1, ccm2], [{node_start, [{monitor_master, true}, {callback_module, local_slave}]}]}.

{logdir, "../../logs/"}.

{alias, example, "./example"}.
{cases, [ccm1], example, example_SUITE, [ccm1_test]}.
{cases, [ccm2], example, example_SUITE, [ccm2_test]}.