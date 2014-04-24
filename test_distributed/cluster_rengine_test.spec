%% ===================================================================
%% @author Michał Sitko
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file is a specification of a test. The test can be found
%% in cluster_rengine directory. The test description can be found in
%% cluster_rengine_test_SUITE.erl file.
%% @end
%% ===================================================================

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./cluster_rengine_test"}.
{include, ["../include", "."]}.

%% test castes to be run
{alias, cluster_rengine_test, "./cluster_rengine_test"}.

{suites, cluster_rengine_test, all}.