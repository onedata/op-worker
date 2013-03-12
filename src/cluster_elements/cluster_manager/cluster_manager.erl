%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module coordinates central cluster. It works as a plug-in
%% of node_manager.
%% @end
%% ===================================================================

-module(cluster_manager).
-include("registered_names.hrl").

-record(cm_state, {nodes = [], monitoring_interval = [], dispatchers = [], workers = []}).

%% ====================================================================
%% API
%% ====================================================================
-export([init/0, handle/2]).

%% ====================================================================
%% API functions
%% ====================================================================

%% init/0
%% ====================================================================
%% @doc Cluster manager starts listen for nodes requests so nodes can
%% register. After initialization_time (environment variable)
%% cluster is initialized (modules are divided between nodes).
-spec init() -> State when
	State :: tuple().
%% ====================================================================
init() ->
	{ok, Interval} = application:get_env(veil_cluster_node, initialization_time),
	timer:apply_after(Interval * 1000, gen_server, call, [{global, ?CCM}, initCluster]),
	#cm_state{}.

%% handle/2
%% ====================================================================
%% @doc Handles requests from nodes, control_panel etc.
-spec handle(Request :: term(), State :: term()) -> Result when
	Result :: {Reply, NewState},
	Reply :: term(),
	NewState :: term().
%% ====================================================================
handle({node_is_up, Node}, State) ->
	{ok, State#cm_state{nodes = [Node | State#cm_state.nodes]}};

handle(getNodes, State) ->
	{State#cm_state.nodes, State};

handle(initCluster, State) ->
	NewState = initCluster(State),
	{ok, NewState};

handle(_Request, State) ->
	{wrong_request, State}.
 
%% ====================================================================
%% Internal functions
%% ====================================================================

initCluster(State) ->
	State.