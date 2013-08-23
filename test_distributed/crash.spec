%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file is specification crash test. The test can be found
%% in crash_test directory. The test description can be found in crash_test_SUITE.erl file.
%% @end
%% ===================================================================

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./crash_test"}.
{include, ["../include", "."]}.

%% test suits to be run
{alias, crash_test, "./crash_test"}.
{suites, crash_test, all}.