%% @author Michal Wrzeszcz

-module(node_manager_tests).

-define(APP_Name, veilFS_cluster_node).

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