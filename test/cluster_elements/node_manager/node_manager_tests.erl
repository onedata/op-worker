%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of node_manager.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(node_manager_tests).

-define(APP_Name, veil_cluster_node).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

type_test() -> 
	ok = application:start(?APP_Name),
	NodeType = gen_server:call(node_manager, getNodeType),
	{ok, NodeType2} = application:get_env(?APP_Name, nodeType),
	?assert(NodeType =:= NodeType2),
	ok = application:stop(?APP_Name).

-endif.