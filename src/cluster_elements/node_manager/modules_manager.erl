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
-include("registered_names.hrl").

-record(node_state, {ccm_con_status = not_connected, dispatchers = [], workers = []}).

%% ====================================================================
%% API
%% ====================================================================
-export([init/0, handle/2]).

%% ====================================================================
%% API functions
%% ====================================================================

%% init/0
%% ====================================================================
%% @doc Tries to register node in ccm (if registration fails, it will try
%% again after worker_sleep_time (environment variable)). Afterwards,
%% it is waiting for orders from ccm.
-spec init() -> State when
	State :: tuple().
%% ====================================================================
init() ->
	#node_state{ccm_con_status = init_connection(not_connected)}.

%% handle/2
%% ====================================================================
%% @doc Handles requests from ccm
-spec handle(Request :: term(), State :: term()) -> Result when
	Result :: {Reply, NewState},
	Reply :: term(),
	NewState :: term().
%% ====================================================================
handle(init_ccm_connection, State) ->
	{ok, State#node_state{ccm_con_status = init_connection(State#node_state.ccm_con_status)}};

handle(reset_ccm_connection, State) ->
	{ok, State#node_state{ccm_con_status = init_connection(not_connected)}};

handle(get_ccm_connection_status, State) ->
	{State#node_state.ccm_con_status, State};

handle(_Request, State) ->
	{wrong_request, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

init_connection(Conn_status) ->
	New_conn_status = case Conn_status of
		not_connected ->
			{ok, CCM_Nodes} = application:get_env(veil_cluster_node, ccm_nodes),
			Ans = init_net_connection(CCM_Nodes),
			case Ans of
				ok -> connected;
				error -> not_connected
			end;
		Other -> Other
	end,
	New_conn_status2 = case New_conn_status of
		connected -> register();
		_Other2 -> New_conn_status
	end,
	case New_conn_status2 of
		registered -> ok;
		_Other3 -> {ok, Interval} = application:get_env(veil_cluster_node, worker_sleep_time),
			timer:apply_after(Interval * 1000, gen_server, call, [node_manager, init_ccm_connection])
	end,
	New_conn_status2.

init_net_connection([]) ->
	error;

init_net_connection([Node | Nodes]) ->
	Ans = net_adm:ping(Node),
	case Ans of
		pong -> ok;
		pang -> init_net_connection(Nodes)
	end.

register() ->
	case send_to_ccm({node_is_up, node()}) of
		ok -> registered;
		_Other -> connected
	end.

send_to_ccm(Message) ->
	try
		gen_server:call({global, ?CCM}, Message)
	catch
		_:_ -> connection_error
	end.