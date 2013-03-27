VeilCluster
===========

VeilCluster is a part of VeilFS system that unifies access to files stored at heterogeneous data storage systems that belong to geographically distributed organizations.

Goals
-----

The goal of VeilCluster is provision of self-scalable cluster that will be a central point of each data centre that uses VeilFS. This central point will decide where users' files should be put. It will also execute rules (defined by administrators and users) and migrate data.


Getting Started
---------------
VeilCluster is built with Rebar. It contains application that starts node. The environment variable 'nodeType' decides what type of node should be started (worker or ccm (Central Cluster Manager)).

#### Src
Sources are put in 'src'. Directly in the 'src' directory only files needed to start application can be put. The 'src' includes subdirectories: 'cluster_elements' and 'veil_modules'.

The 'cluster_elements' includes directories that contain code of Erlang modules that enable cluster management and host 'proper' modules of VeilFS. These Erlang modules are responsible for load balancing, spawning processes for requests etc. This allows implementation of 'proper' modules using sequential code.

The 'veil_modules' includes directories that contain code of 'proper' modules of VeilFS. Each 'proper' module will work inside of 'worker_host' (one of 'cluster_elements') so it must implement 'worker_plugin_behaviour' defined in 'worker_plugin_behaviour.erl' file in this directory.

#### Tests
Tests should be put in 'test' directory. It should contain the same subdirectories as 'src'. Each test name should be constructed as follows: 'name of tested file'_tests.erl, e.g., 'node_manager_tests.erl' should contain functions that test code from 'node_manager.erl' file.

Eunit is used during tests so each test file should:

* include "eunit/include/eunit.hrl",
* use proper functions names (name of each test function must end with '_test'),
* use compilation control macros (code between '-ifdef(TEST).' and '-endif.').

#### Releases
Release handling is done using 'releases' directory and 'reltool.config' file. To create new release, version must be changed in both 'src/veil_cluster_node.app.src' and 'releases/reltool.conf'.

#### Documentation
Documentation is generated automatically using edoc so it should use tags defined by it.

#### Useful commands:

standard compilation:
    ~$  rebar compile
    ~$  make compile

compilation & execution of unit tests:

    ~$  rebar compile eunit
    ~$  make test

compilation & creation of release:
    ~$  rebar compile generate
    ~$  make generate

generates documentation:
    ~$  rebar doc
    ~$  make docs

generation of package for hot code swapping
    ~$  make PREV="name of directory with previous release" upgrade

Note:

* in Linux you should use './rebar' instead of 'rebar'
* rebar is built with Erlang/OTP R16B - if you have other Erlang version installed, it may not work properly (in this case download rebar sources and rebuilt rebar)


-------------------------------------------------------------------------------


Using Makefile to generate releases and test environments of veil cluster nodes
===============================================================================


1. Generating and managing a single node release
------------------------------------------------

The script 'gen_dev' produces a vars.config file used in rebar. It is used in the process of release generation.

Every node (worker or CCM) requires information about all CCMs running in the cluster. Hence to generate release of
a node it is required to specify the following set of arguments:

    -name node_name@host -main_ccm main_ccm_node@host [-opt_ccm opt_ccm_node1@host opt_ccm_node2@host ...]

 - The expression after -name specifies the node for which the release will be generated. It can be one of CCMs listed later on.
 - The expression after -main_ccm specifies the node name of the main CCM.
 - The expression after -opt_ccm specifies the list of optional CCMs. These arguments are not mandatory.

The above argument string can be either placed in file 'gen_dev.args' located in the root directory or passed to Makefile.

#### Generating a release:

    ~$  make release_config args="-name node_name@host -main_ccm main_ccm_node@host -opt_ccm opt_ccm_node1@host opt_ccm_node2@host"

    ~$  make release_config_from_file

After either of these operations, the release will be placed in releases/node_name, with all the environment variables set up.

#### Starting and managing generated packages:

    ~$  make start_config node="node_name"               -> starts the node called 'node_name' as a daemon

    ~$  make node_attach node="node_name"                -> attaches to the running node called 'node_name' with an erlang shell

    ~$  make start_config_console node="node_name"       -> combines the two above



#### To produce a vars.config file without generation one can use:

    ~$  make gen_config args="-name node_name@host -main_ccm main_ccm_node@host -opt_ccm opt_ccm_node1@host opt_ccm_node2@host"

    ~$  make gen_config_from_file

NOTE - this will change reltool.config accordingly to arguments and save the old version in old_reltool.config.
After generation the old file should be restored, either manually or by using:

    ~$  make gen_config_cleanup



#### Another files that take part in vars.config creation are:
     - vars/ccm_vars.config
     - vars/worker_vars.config

Those are strongly connected with the script itself. The script swaps strings starting with "$" with ones calculated
from script arguments and thus creates the vars.config file. For a broader description consult these files.



2. Generating a local test environment
--------------------------------------

The script 'gen_test' simplifies setting up a bunch of cluster nodes for testing. It uses the functionalities listed above.
To generate a testing environment proper arguments must be passed to the script:

    -worker worker1@host worker2@host ... -main_ccm main_ccm_node@host [-opt_ccm opt_ccm_node1@host opt_ccm_node2@host ...]

 - The expression after -worker specifies the list of workers in the cluster.
 - The expression after -main_ccm specifies the node name of the main CCM.
 - The expression after -opt_ccm specifies the list of optional CCMs in the cluster (not mandatory).

Again, these arguments can be obtained from 'gen_test.args' or passed via args to Makefile.

####Possible usages:

    ~$  make gen_test_env args="-worker worker1@host -main_ccm main_ccm_node@host -opt_ccm opt_ccm_node1@host opt_ccm_node2@host"

    ~$  make gen_test_env_from_file

Both of these commands produce a release for each node in corresponding directories.

#### Starting the whole cluster:

    ~$  make start_test_env args="-worker worker1@host -main_ccm main_ccm_node@host -opt_ccm opt_ccm_node1@host opt_ccm_node2@host"       -

    ~$  make start_test_env_from_file

It is important that the same args are passed to Makefile or remain it the .args file. This is the way for the script
to know which release packages need to be started.

Every node can be started independently with use of 'start_config', 'node_attach' and 'start_config_console' make targets.





Support
-------
For more information visit project Confluence or write to 'wrzeszcz@agh.edu.pl'.