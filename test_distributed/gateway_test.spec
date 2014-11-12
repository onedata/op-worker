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

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./gateway_test"}.
{include, ["../include", "."]}.

%% test suits to be run
{alias, example, "./gateway_test"}.
{suites, example, all}.
