%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains REST module suite specification. 
%% ===================================================================



%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./rest_test"}.
{include, ["../include", "."]}.

%% test suites to be run
{alias, rest_test, "./rest_test"}.
{suites, rest_test, all}.