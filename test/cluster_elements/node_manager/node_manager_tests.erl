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
-include("registered_names.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

env_test() -> 
	ok = application:start(?APP_Name),
	{ok, _Time} = application:get_env(?APP_Name, worker_sleep_time),
	{ok, _Nodes} = application:get_env(?APP_Name, ccm_nodes),
	ok = application:stop(?APP_Name).

node_type_test() ->
	{ok, NodeType} = application:get_env(?APP_Name, node_type),
	ok = application:start(?APP_Name),
	NodeType2 = gen_server:call(node_manager, getNodeType),
	?assert(NodeType =:= NodeType2),
	ok = application:stop(?APP_Name).

registration_test() ->
	net_kernel:start([node1, shortnames]),

	application:set_env(?APP_Name, node_type, worker),
	application:set_env(?APP_Name, ccm_nodes, [not_existing_node, node()]), 
	ok = application:start(?APP_Name),

	Ccm_status = gen_server:call(node_manager, get_ccm_connection_status),
	?assert(Ccm_status =:= connected),
	
	application:set_env(?APP_Name, ccm_nodes, [not_existing_node]), 
	ok = gen_server:cast(node_manager, reset_ccm_connection),
	Ccm_status2 = gen_server:call(node_manager, get_ccm_connection_status),
	?assert(Ccm_status2 =:= not_connected),

	ok = application:stop(?APP_Name),
	net_kernel:stop().

-endif.