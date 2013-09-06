%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file is central_logger suite specification. 
%% ===================================================================



%% log directories (all_nodes does not include master)
%% {logdir, master, "."}.
{logdir, all_nodes, "./central_logger_test"}.
{include, ["../include", "."]}.

%% test suites to be run
{alias, central_logger_test, "./central_logger_test"}.
{suites, central_logger_test, all}.