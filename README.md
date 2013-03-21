-------------------------------------------------------------------------------
Using Makefile to generate releases and test environments of veil cluster nodes
-------------------------------------------------------------------------------


1. GENERATING AND MANAGING A SINGLE NODE RELEASE
   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    The script 'gen_dev' produces a vars.config file used in rebar. It is used in the process of release generation.

    Every node (worker or CCM) requires information about all CCMs running in the cluster. Hence to generate release of
    a node it is required to specify the following set of arguments:

    -name node_name@host -main_ccm main_ccm_node@host [-opt_ccm opt_ccm_node1@host opt_ccm_node2@host ...]

    The expression after -name specifies the node for which the release will be generated. It can be one of CCMs listed later on.
    The expression after -main_ccm specifies the node name of the main CCM.
    The expression after -opt_ccm specifies the list of optional CCMs. These arguments are not mandatory.

    The above argument string can be either placed in file 'gen_dev.args' located in the root directory or passed to Makefile.
    Therefore the two ways of generating a release are:

~$  make release_config args="-name node_name@host -main_ccm main_ccm_node@host -opt_ccm opt_ccm_node1@host opt_ccm_node2@host"

~$  make release_config_from_file

    After either of these operations, the release will be placed in releases/node_name, with all the environment variables set up.
    Then, to start and manage generated packages there are several commands:

~$  make start_config node="node_name"               -> starts the node called 'node_name' as a daemon

~$  make node_attach node="node_name"                -> attaches to the running node called 'node_name' with an erlang shell

~$  make start_config_console node="node_name"       -> combines the two above



2. GENERATING A LOCAL TEST ENVIRONMENT
   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    The script 'gen_test' simplifies setting up a bunch of cluster nodes for testing. It uses the functionalities listed above.

    To generate a testing environment proper arguments must be passed to the script:

    -worker worker1@host worker2@host ... -main_ccm main_ccm_node@host [-opt_ccm opt_ccm_node1@host opt_ccm_node2@host ...]

    The expression after -worker specifies the list of workers in the cluster.
    The expression after -main_ccm specifies the node name of the main CCM.
    The expression after -opt_ccm specifies the list of optional CCMs in the cluster (not mandatory).

    Again, these arguments can be obtained from 'gen_test.args' or passed via args to Makefile. Possible usages:

~$  make gen_test_env args="-worker worker1@host -main_ccm main_ccm_node@host -opt_ccm opt_ccm_node1@host opt_ccm_node2@host"

~$  make gen_test_env_from_file

    Both of these commands produce a release for each node in corresponding directories. Commands to start the whole cluster:

~$  make start_test_env args="-worker worker1@host -main_ccm main_ccm_node@host -opt_ccm opt_ccm_node1@host opt_ccm_node2@host"       -

~$  make start_test_env_from_file

    It is important that the same args are passed to Makefile or remain it the .args file. This is the way for the script
    to know which release packages need to be started.

    Every node can be started independently with use of 'start_config', 'node_attach' and 'start_config_console' make targets.