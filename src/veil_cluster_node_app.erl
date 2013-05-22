%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: It is the main module of application. It lunches 
%% supervisor which then initializes appropriate components of node.
%% @end
%% ===================================================================

-module(veil_cluster_node_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

%% start/1
%% ====================================================================
%% @doc Starts application by supervisor initialization.
-spec start(_StartType :: any(), _StartArgs :: any()) -> Result when
	Result ::  {ok, pid()}
                | ignore
                | {error, Error},
	Error :: {already_started, pid()}
                | {shutdown, term()}
                | term().
%% ====================================================================
start(_StartType, _StartArgs) ->
	{ok, NodeType} = application:get_env(veil_cluster_node, node_type),
  {ok, Port} = application:get_env(veil_cluster_node, dispatcher_port),
  {ok, PoolSize} = application:get_env(veil_cluster_node, dispatcher_pool_size),
  {ok, CertFile} = application:get_env(veil_cluster_node, ssl_cert_path),
  {ok, _} = ranch:start_listener(dispatcher_listener, PoolSize, ranch_ssl, [{port, Port}, {certfile, atom_to_list(CertFile)}], ranch_handler, []),
  veil_cluster_node_sup:start_link(NodeType).

%% stop/1
%% ====================================================================
%% @doc Stops application.
-spec stop(_State :: any()) -> Result when
	Result ::  ok.
%% ====================================================================
stop(_State) ->
  ranch:stop_listener(dispatcher_listener),
  ok.
