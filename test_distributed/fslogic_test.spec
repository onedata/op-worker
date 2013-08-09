%% ===================================================================
%% @author Michał Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file is a specification of a test. The test can be found
%% in fslogic_test directory. The test description can be found in
%% fslogic_test_SUITE.erl file.
%% @end
%% ===================================================================

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./fslogic_test"}.
{include, ["../include", "."]}.

%% test castes to be run
{alias, fslogic_test, "./fslogic_test"}.

{suites, fslogic_test, all}.

