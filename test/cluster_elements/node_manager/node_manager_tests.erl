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

type1_test() -> 
	{ok, _NodeType} = application:get_env(?APP_Name, node_type).

type2_test() ->
	application:set_env(?APP_Name, node_type, worker), 
	ok = application:start(?APP_Name),
	NodeType = gen_server:call(node_manager, getNodeType),
	?assert(NodeType =:= worker),
	ok = application:stop(?APP_Name).

type3_test() -> 
	application:set_env(?APP_Name, node_type, ccm), 
	ok = application:start(?APP_Name),
	NodeType = gen_server:call({global, ?CCM}, getNodeType),
	?assert(NodeType =:= ccm),
	ok = application:stop(?APP_Name).

-endif.