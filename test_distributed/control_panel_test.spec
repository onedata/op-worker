%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file is control_panel suite specification. 
%% ===================================================================



%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./control_panel_test"}.
{include, ["../include", "."]}.

%% test suites to be run
{alias, control_panel_test, "./control_panel_test"}.
{suites, control_panel_test, all}.