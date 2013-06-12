%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file is an example of test's specification.. The test can be found
%% in example directory. The test description can be found in example.erl file.
%% @end
%% ===================================================================

%% slave nodes
{node, ccm1, 'ccm1@localhost'}.
{node, ccm2, 'ccm2@localhost'}.

%% start nodes
{init, [ccm1, ccm2], [{node_start, [{monitor_master, true}]}]}.

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./example"}.

%% test castes to be run
{alias, example, "./example"}.
{cases, [ccm1], example, example_SUITE, [ccm1_test]}.
{cases, [ccm2], example, example_SUITE, [ccm2_test]}.