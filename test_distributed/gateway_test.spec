%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This file is a specification for gateway tests. The test can be found
%% in gateway_test directory.
%% @end
%% ===================================================================

%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./gateway_test"}.
{include, ["../include", "."]}.

%% test suits to be run
{alias, gateway_test, "./gateway_test"}.
{suites, gateway_test, all}.
