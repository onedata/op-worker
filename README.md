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

#### Useful commands:
* 'rebar compile' - standard compilation
* 'rebar compile eunit' - compilation & execution of unit tests
* 'rebar compile generate' - compilation & creation of release

Support
-------
For more information visit project Confluence or write to 'wrzeszcz@agh.edu.pl'.
