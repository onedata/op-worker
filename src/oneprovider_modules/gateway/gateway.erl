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
-author("Konrad Zemek").
-behaviour(worker_plugin_behaviour).

-include("oneprovider_modules/gateway/gateway.hrl").
-include("oneprovider_modules/gateway/registered_names.hrl").

%% @TODO: config
-define(acceptor_number, 100).
-define(network_interfaces, [{192,168,122,169}, {192,168,122,169}, {192,168,122,169}]).

-export([init/1, handle/2, cleanup/0]).
-export([compute_request_hash/1]).

-export([test/0]).
test() ->
    FileId = "05073bf6703dee28bdb5016b3b53bf70",

    lists:foreach(
        fun(Offset) ->
            Req = #fetchrequest{file_id = FileId, offset = Offset, size = 1000},
            gen_server:cast(?MODULE, {asynch, 1, #fetch{remote = {192,168,122,236}, notify = self(), request = Req}}) end,
        lists:seq(0, 3)).


%% ====================================================================
%% API functions
%% ====================================================================

init(_Args) ->
    LocalServerPort = oneproxy:get_local_port(?gw_port),

    {ok, _} = ranch:start_listener(?GATEWAY_LISTENER, ?acceptor_number,
    	ranch_tcp, [{port, LocalServerPort}], gateway_protocol, []),

    {ok, Cert} = application:get_env(oneprovider_node, web_ssl_cert_path), %% @TODO: global_registry_provider_cert_path),
    CertString = atom_to_list(Cert),

    LocalPort = oneproxy:get_local_port(gw_proxy),
    Pid = spawn_link(fun() -> oneproxy:start(LocalPort, CertString, verify_none) end), %% @TODO: verify_peer
    register(oneproxy_gateway, Pid),

    Pid2 = spawn_link(fun() -> oneproxy:start(?gw_port, LocalServerPort, CertString, verify_none, no_http) end), %% @TODO: verify_peer
    register(oneproxy_gw, Pid2),

	{ok, _} = gateway_dispatcher_supervisor:start_link(?network_interfaces).

handle(_ProtocolVersion, ping) ->
    pong;

handle(_ProtocolVersion, healthcheck) ->
    ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();

handle(_ProtocolVersion, #fetch{} = Request) ->
    gen_server:cast(?GATEWAY_DISPATCHER, Request),
    ok;

handle(_ProtocolVersion, _Msg) ->
    ?log_call(_Msg),
    %% @TODO: handle exit from gateway_dispatcher_supervisor
    ok.

cleanup() ->
    ranch:stop_listener(?GATEWAY_LISTENER),
    %% @TODO: stop supervisors, oneproxy
    ok.


-spec compute_request_hash(RequestBytes :: iodata()) -> Hash :: binary().
compute_request_hash(RequestBytes) ->
    crypto:hash(sha256, RequestBytes).
