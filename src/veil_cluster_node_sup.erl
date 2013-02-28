%% ===================================================================
%% @author Michal Wrzeszcz
%% Copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%%
%% ===================================================================
%% Description: It is the main supervisor. It starts node manager
%% which initializes node.
%% ===================================================================

-module(veil_cluster_node_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args), {I, {I, start_link, [Args]}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(NodeType) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [NodeType]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([NodeType]) ->
    {ok, { {one_for_one, 5, 10}, [?CHILD(node_manager, worker, NodeType)]} }.
