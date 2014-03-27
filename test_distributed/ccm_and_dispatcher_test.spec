%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file is a specification of a test. The test can be found
%% in nodes_monitoring_and_workers_test directory. The test description
%% can be found in nodes_monitoring_and_workers_test.erl file.
%% @end
%% ===================================================================

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./ccm_and_dispatcher_test"}.
{include, ["../include", "."]}.

%% test castes to be run
{alias, ccm_and_dispatcher_test, "./ccm_and_dispatcher_test"}.
{suites, ccm_and_dispatcher_test, all}.