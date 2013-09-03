%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file is a specification of a test. The test can be found
%% in veilhelpers_test directory. The test description can be found in
%% veilhelpers_test_SUITE.erl file.
%% @end
%% ===================================================================

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./fslogic_test"}.
{include, ["../include", "."]}.

%% test castes to be run
{alias, veilhelpers_test, "./veilhelpers_test"}.

{suites, veilhelpers_test, all}.

