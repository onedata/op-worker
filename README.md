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
Sources are put in 'src'. Directly in the 'src' directory only files needed to start application can be put. The 'src' includes subdirectories: 'cluster_elements', 'veil_modules' and 'proto'.

The 'cluster_elements' includes directories that contain code of Erlang modules that enable cluster management and host 'proper' modules of VeilFS. These Erlang modules are responsible for load balancing, spawning processes for requests etc. This allows implementation of 'proper' modules using sequential code.

The 'veil_modules' includes directories that contain code of 'proper' modules of VeilFS. Each 'proper' module will work inside of 'worker_host' (one of 'cluster_elements') so it must implement 'worker_plugin_behaviour' defined in 'worker_plugin_behaviour.erl' file in this directory.

The 'proto' includes definitions of protocol buffer messages used by clients during the communication with VeilCluster.

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


Deploying a release
===================

After generation of a release package, configuration files contain default parameters. The script 'veil_cluster', that
comes with the package (in directory 'bin') is used to set up and start a release.

Prerequisites
-------------

Firstly, the user must have execution rights on both '/bin/veil_cluster' and '/bin/veil_cluster_node' scripts.

Secondly, 'config.args' file must be present in 'config' directory (along with 'veil_cluster' script).

Setting parameters
------------------

List of parameters that can be set:

    name       -> name of the node (erlang long name)
    main_ccm   -> main CCM name (of the cluster this node operates in)
    opt_ccms   -> list of optional CCMs (this parameter is not mandatory)
    db_nodes   -> list of DBMS nodes

Primarily, these parameters are retrieved from 'config.args' file. It should contain these parameters in following manner:

    <parameter_name>: <parameter_value>

Another way is passing these parameters via command line arguments. In this case the syntax is:

    ./veil_cluster -<parameter1_name> <parameter1_value> -<parameter2_name> <parameter2_value> ...

NOTE:

* parameters passed via command line OVERRIDE those in 'config.args' file
* "command line way" can specify any subset of parameters, for instance:
    * './veil_cluster' will cause the script to use all parameters from 'config.args'
    * './veil_cluster -name somename@host.net' will cause the script to use parameters from 'config.args' except node name
    * './veil_cluster -opt_ccms' (no opt_ccms value specified) will override opt_ccms from 'config.args' with null value
* (both) parameter order can be arbitrary
* (both) multiple values (eg. DBMS nodes) are passed as a space-delimited list (eg. -db_nodes dbnode1@host.net dbnode2@host.net)
* (both) parameter values can't contain spaces or hyphens
* (command line) parameter names are preceded by a hyphen; '-'

Starting a release
------------------

There are three options which are used to start a release. They are passed along with other arguments to the 'veil_cluster' script.

    -start    -> the script will perform the configuration and then start the node as a daemon
    -attach   -> the script will skip configuration and try to attach to a running node with an erlang shell (used after -start)
    -console  -> the script will perform the configuration and then start the node with an erlang shell

If none of these arguments occur, the script will terminate after setting up the configuration.

Full example of usage
---------------------

    ~$  ./veil_cluster -name mynode@host.net -main_ccm ccmnode@host.net -console

Above command will configure the release according to 'config.args', except for name and main_ccm which will be modified
corresponding to command line arguments. Then, the node will be started presenting to the user with an erlang shell.

-------------------------------------------------------------------------------


Development - using Makefile to generate single releases and test environments of veil cluster nodes
====================================================================================================


1. Generating and managing a single node release for development purposes
-------------------------------------------------------------------------

The script 'gen_dev' generates a single node release for testing purposes. It uses 'veil_cluster' script to set the configuration.

Every node (worker or CCM) requires information about all CCMs running in the cluster. Hence to generate release of
a node it is required to specify the following set of arguments:

    -name node_name@host -main_ccm main_ccm_node@host [-opt_ccms opt_ccm_node1@host opt_ccm_node2@host ...] -db_nodes db1@host db2@host

 - The expression after -name specifies the node for which the release will be generated. It can be one of CCMs listed later on.
 - The expression after -main_ccm specifies the node name of the main CCM.
 - The expression after -opt_ccms specifies the list of optional CCMs. These arguments are not mandatory.
 - The expression after -db_nodes specifies the list of all DBMS nodes.

The above argument string can be either placed in file 'gen_dev.args' located in the root directory or passed to Makefile.

Optionally, one of following can be added to arguments:

    -no_compile     -> skips code compilation straight to release generation
    -no_generate    -> skips code compilation and release generation (creates a copy of existing release).

#### Generating a release:

    ~$  make gen_test_node args="-name node_name@host -main_ccm main_ccm_node@host -opt_ccms opt_ccm_node1@host opt_ccm_node2@host -db_nodes db1@host db2@host"

    ~$  make gen_test_node_from_file

After either of these operations, the release will be placed in releases/test_cluster/node_name, with all the environment variables set up.

#### Starting and managing generated packages:

    ~$  make start_node node="node_name"               -> starts the node called 'node_name' as a daemon

    ~$  make attach_to_node node="node_name"           -> attaches to the running node called 'node_name' with an erlang shell

    ~$  make start_node_console node="node_name"       -> combines the two above

Note, that it only works for packages in releases/test_cluster/ - those created with 'gen_dev' or 'gen_test'


2. Generating a local test environment
--------------------------------------

The script 'gen_test' simplifies setting up a bunch of cluster nodes for testing. It uses both 'gen_dev' and 'veil_cluster' scripts.
To generate a testing environment proper arguments must be passed to the script:

    -workers worker1@host worker2@host ... -main_ccm main_ccm_node@host [-opt_ccms opt_ccm_node1@host opt_ccm_node2@host ...] -db_nodes db1@host db2@host

 - The expression after -workers specifies the list of workers in the cluster.
 - The expression after -main_ccm specifies the node name of the main CCM.
 - The expression after -opt_ccms specifies the list of optional CCMs in the cluster (not mandatory).
 - The expression after -db_nodes specifies the list of all DBMS nodes.

Again, these arguments can be obtained from 'gen_test.args' or passed via args to Makefile.

#### Generating releases for the whole cluster:

    ~$  make gen_test_env args="-workers worker1@host -main_ccm main_ccm_node@host -opt_ccms opt_ccm_node1@host opt_ccm_node2@host -db_nodes db1@host db2@host"

    ~$  make gen_test_env_from_file

Both of these commands produce a release for each node in corresponding directories (/releases/test_cluster/).

#### Starting previously generated cluster:

    ~$  make start_test_env args="-workers worker1@host -main_ccm main_ccm_node@host -opt_ccms opt_ccm_node1@host opt_ccm_node2@host -db_nodes db1@host db2@host"

    ~$  make start_test_env_from_file

It is important that the same args are passed to Makefile or remain in the .args file. This is the way for the script
to know which release packages need to be started.

#### Generating and immediately starting the whole cluster:

    ~$  make gen_start_test_env args="-workers worker1@host -main_ccm main_ccm_node@host -opt_ccms opt_ccm_node1@host opt_ccm_node2@host -db_nodes db1@host db2@host"

    ~$  make gen_start_test_env_from_file

Every node can be started independently with use of 'start_node', 'attach_to_node' and 'start_node_console' make targets.




Support
-------
For more information visit project Confluence or write to 'wrzeszcz@agh.edu.pl'.