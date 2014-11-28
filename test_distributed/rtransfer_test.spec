%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This file is a specification for rtransfer tests. The test can be found
%% in rtransfer_test directory.
%% @end
%% ===================================================================

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./rtransfer_test"}.
{include, ["../include", "."]}.

%% test suits to be run
{alias, rtransfer_test, "./rtransfer_test"}.
{suites, rtransfer_test, all}.
