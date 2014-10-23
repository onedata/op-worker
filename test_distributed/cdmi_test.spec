%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This file is specification of cdmi test. The test can be found
%% in cdmi_test directory. The test description can be found in cdmi_test_SUITE.erl file.
%% @end
%% ===================================================================

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./cdmi_test"}.
{include, ["../include", "."]}.

%% test suits to be run
{alias, cdmi_test, "./cdmi_test"}.
{suites, cdmi_test, all}.