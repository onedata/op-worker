%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains gui suite specification. 
%% ===================================================================



%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./gui_test"}.
{include, ["../include", "."]}.

%% test suites to be run
{alias, gui_test, "./gui_test"}.
{suites, gui_test, all}.