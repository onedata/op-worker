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
-include("registered_names.hrl").

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
  {ok, NodeType} = application:get_env(?APP_Name, node_type),
  case NodeType =/= ccm of
    true ->
      {ok, Port} = application:get_env(?APP_Name, dispatcher_port),
      {ok, DispatcherPoolSize} = application:get_env(?APP_Name, dispatcher_pool_size),
      {ok, CertFile} = application:get_env(?APP_Name, ssl_cert_path),
      {ok, _} = ranch:start_listener(dispatcher_listener, DispatcherPoolSize, ranch_ssl,
          [{port, Port}, {certfile, atom_to_list(CertFile)},
              {verify, verify_peer}, {verify_fun, {fun gsi_handler:verify_callback/3, []}}],
          ranch_handler, []);
    false -> ok
  end,
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
