%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of cluster_manager.
%% It contains unit tests that base on eunit.
%% @end
%% ===================================================================

-module(cluster_manager_tests).
-include("registered_names.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(TEST).

env_test() ->
	ok = application:start(?APP_Name),
	{ok, _InitTime} = application:get_env(?APP_Name, initialization_time),
	ok = application:stop(?APP_Name).

nodes_counting_test() ->
	net_kernel:start([node1, shortnames]),

	application:set_env(?APP_Name, node_type, ccm), 
	application:set_env(?APP_Name, ccm_nodes, [node()]), 

	ok = application:start(?APP_Name),

	Nodes = [n1, n2, n3],
	lists:foreach(fun(Node) -> gen_server:call({global, ?CCM}, {node_is_up, Node}) end, Nodes),
	Nodes2 = gen_server:call({global, ?CCM}, getNodes),
	?assert(length(Nodes) + 1 == length(Nodes2)),
	lists:foreach(fun(Node) -> ?assert(lists:member(Node, Nodes2)) end, Nodes),
	?assert(lists:member(node(), Nodes2)),
	
	gen_server:call({global, ?CCM}, {node_is_up, n2}),
	gen_server:call({global, ?CCM}, {node_is_up, n1}),
	Nodes3 = gen_server:call({global, ?CCM}, getNodes),
	?assert(length(Nodes) + 1 == length(Nodes3)),
	
	ok = application:stop(?APP_Name),
	net_kernel:stop().

-endif.