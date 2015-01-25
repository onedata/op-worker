%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file is a specification of a test. The test can be found
%% in nodes_management_test directory. The test description
%% can be found in nodes_management_test.erl file.
%% @end
%% ===================================================================

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./nodes_management_test"}.
{include, ["../include", "."]}.

%% test cases to be run
{alias, nodes_management_test, "./nodes_management_test"}.
{suites, nodes_management_test, all}.