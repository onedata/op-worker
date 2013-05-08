{alias, basic2, "./basic2"}.
{suites, basic2, all}.

{node, ccm1, 'ccm1@plgsl63.local'}.
{node, ccm2, 'ccm2@plgsl63.local'}.

{init, [ccm1, ccm2], [{node_start, [{monitor_master, true}]}]}.%%[{node_start, [{basic2_SUITE, init_vm}]}]}.

{logdir, "./logs/"}.