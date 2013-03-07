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

type_test() -> 
	ok = application:start(?APP_Name),
	{ok, NodeType} = application:get_env(?APP_Name, nodeType),
	
	NodeType2 = case NodeType of
		ccm -> gen_server:call({global, ?CCM}, getNodeType);
		_Other -> gen_server:call(node_manager, getNodeType)
	end,
	
	?assert(NodeType =:= NodeType2),
	ok = application:stop(?APP_Name).

-endif.