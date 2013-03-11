%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module coordinates each node. It works as a plug-in
%% of node_manager.
%% @end
%% ===================================================================

-module(modules_manager).

-record(node_state, {ccm_con_status = [], dispatchers = [], workers = []}).

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/0, handle/2]).

init() ->
	[init_connection()].

handle(init_ccm_connection, State) ->
	{ok, State#node_state{ccm_con_status = init_connection()}};

handle(_Request, State) ->
	{wrong_request, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

init_connection() ->
	{ok, CCM_Nodes} = application:get_env(veil_cluster_node, ccm_nodes),
	Ans = init_connection(CCM_Nodes),
	case Ans of
		ok -> ok;
		error -> {ok, Interval} = application:get_env(veil_cluster_node, worker_sleep_time),
			timer:apply_after(Interval * 1000, gen_server, call, [node_manager, init_ccm_connection]),
			error
	end.

init_connection([]) ->
	error;

init_connection([Node | Nodes]) ->
	Ans = net_adm:ping(Node),
	case Ans of
		pong -> ok;
		pang -> init_connection(Nodes)
	end.