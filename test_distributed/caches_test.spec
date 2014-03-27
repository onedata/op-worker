%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file is a specification of a test. The test can be found
%% in caches_test directory. The test description
%% can be found in caches_test.erl file.
%% @end
%% ===================================================================

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./caches_test"}.
{include, ["../include", "."]}.

%% test castes to be run
{alias, caches_test, "./caches_test"}.
{suites, caches_test, all}.