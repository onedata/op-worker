%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of modules_manager.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(modules_manager_tests).
-include("registered_names.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

env_test() ->
	{ok, _Time} = application:get_env(?APP_Name, worker_sleep_time),
	{ok, _Nodes} = application:get_env(?APP_Name, ccm_nodes).

registration_test() ->
	net_kernel:start([node1, shortnames]),

	application:set_env(?APP_Name, node_type, worker),
	application:set_env(?APP_Name, ccm_nodes, [not_existing_node, node()]), 
	ok = application:start(?APP_Name),

	Ccm_status = gen_server:call(node_manager, get_ccm_connection_status),
	?assert(Ccm_status =:= connected),
	
	application:set_env(?APP_Name, ccm_nodes, [not_existing_node]), 
	ok = gen_server:call(node_manager, reset_ccm_connection),
	Ccm_status2 = gen_server:call(node_manager, get_ccm_connection_status),
	?assert(Ccm_status2 =:= not_connected),

	ok = application:stop(?APP_Name),
	net_kernel:stop().

-endif.