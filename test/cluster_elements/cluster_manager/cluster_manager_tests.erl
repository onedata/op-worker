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
	{ok, _InitTime} = application:get_env(?APP_Name, initialization_time).

nodes_counting_test() ->
	application:set_env(?APP_Name, node_type, ccm), 
	ok = application:start(?APP_Name),

	Nodes = [n1, n2, n3],
	lists:foreach(fun(Node) -> gen_server:call({global, ?CCM}, {node_is_up, Node}) end, Nodes),
	Nodes2 = gen_server:call({global, ?CCM}, getNodes),
	?assert(length(Nodes) == length(Nodes2)),
	lists:foreach(fun(Node) -> ?assert(lists:member(Node, Nodes2)) end, Nodes),

	ok = application:stop(?APP_Name).

-endif.