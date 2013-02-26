-module(veilFS_cluster_node_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
	{ok, NodeType} = application:get_env(veilFS_cluster_node, nodeType),
    veilFS_cluster_node_sup:start_link(NodeType).

stop(_State) ->
    ok.
