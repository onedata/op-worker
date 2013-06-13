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
  {ok, Port} = application:get_env(?APP_Name, dispatcher_port),
  {ok, DispatcherPoolSize} = application:get_env(?APP_Name, dispatcher_pool_size),
  {ok, CertFile} = application:get_env(?APP_Name, ssl_cert_path),
  {ok, DispatcherTimeout} = application:get_env(?APP_Name, dispatcher_timeout),
  {ok, TcpAcceptorPool} = application:get_env(?APP_Name, dns_tcp_acceptor_pool_size),
  {ok, TcpTimeout} = application:get_env(?APP_Name, dns_tcp_timeout),
  {ok, DNSPort} = application:get_env(?APP_Name, dns_port),
  {ok, DNSResponseTTL} = application:get_env(?APP_Name, dns_response_ttl),
  {ok, _} = ranch:start_listener(dispatcher_listener, DispatcherPoolSize, ranch_tcp, [{port, Port}, {certfile, atom_to_list(CertFile)}], ranch_handler, []),
  DNS_Transport_Options =  [{packet, 2}, {dispatcher_timeout, DispatcherTimeout}, {dns_response_ttl, DNSResponseTTL}, {dns_tcp_timeout, TcpTimeout}, {keepalive, true}],
  {ok, _} = ranch:start_listener(dns_tcp_listener, TcpAcceptorPool, ranch_tcp, [{port, DNSPort}], dns_ranch_tcp_handler, DNS_Transport_Options),
  veil_cluster_node_sup:start_link(NodeType).


%% stop/1
%% ====================================================================
%% @doc Stops application.
-spec stop(_State :: any()) -> Result when
	Result ::  ok.
%% ====================================================================
stop(_State) ->
  ranch:stop_listener(dispatcher_listener),
  ranch:stop_listener(dns_tcp_listener),
  ok.
