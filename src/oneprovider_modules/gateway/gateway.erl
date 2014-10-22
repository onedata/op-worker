%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour to provide
%% gateway functionality (transfer of files between data centers).
%% @end
%% ===================================================================

-module(gateway).
-behaviour(worker_plugin_behaviour).

-include("oneprovider_modules/gateway/gateway.hrl").
-include("oneprovider_modules/gateway/registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% @TODO: config
-define(acceptor_number, 100).
-define(max_concurrent_connections, 5).

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

init(_Args) ->
    {ok, _} = ranch:start_listener(?GATEWAY_LISTENER, ?acceptor_number,
    	ranch_tcp, [{port, ?gw_port}], gateway_protocol, []),
	{ok, _} = gateway_dispatcher_supervisor:start_link(?max_concurrent_connections).

handle(_ProtocolVersion, ping) ->
    pong;

handle(_ProtocolVersion, healthcheck) ->
    ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();

handle(_ProtocolVersion, {send, _Data, _Pid, _Remote} = Request) ->
    gen_server:cast(?GATEWAY_DISPATCHER, Request),
    ok;

handle(_ProtocolVersion, _Msg) ->
    ?warning("Wrong request: ~p", [_Msg]),
    ok.

cleanup() ->
    ranch:stop_listener(?GATEWAY_LISTENER),
    ok.
