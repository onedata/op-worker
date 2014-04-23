%% ===================================================================
%% @author Michał Wrzeszcz
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file is a specification of a test. The test can be found
%% in high_load_test directory. The test description can be found in
%% high_load_test_SUITE.erl file.
%% @end
%% ===================================================================

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./high_load_test"}.
{include, ["../include", "."]}.

%% test castes to be run
{alias, high_load_test, "./high_load_test"}.

{suites, high_load_test, all}.

