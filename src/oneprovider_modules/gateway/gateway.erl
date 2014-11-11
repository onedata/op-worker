%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module implements worker_plugin_behaviour to provide
%% gateway functionality (transfer of files between data centers).
%% @end
%% ===================================================================

-module(gateway).
-author("Konrad Zemek").
-behaviour(worker_plugin_behaviour).

-include("oneprovider_modules/gateway/gateway.hrl").
-include("oneprovider_modules/gateway/registered_names.hrl").
-include("registered_names.hrl").

-export([init/1, handle/2, cleanup/0]).
-export([compute_request_hash/1]).

-export([test/0]).
test() ->
    FileId = "05073bf6703dee28bdb5016b3b53bf70",

    lists:foreach(
        fun(Offset) ->
            Req = #fetchrequest{file_id = FileId, offset = Offset * 1000, size = 1000},
            gen_server:cast(?MODULE, {asynch, 1, #fetch{remote = {192,168,122,236}, notify = self(), request = Req}}) end,
        lists:seq(0, 3)).


%% ====================================================================
%% API functions
%% ====================================================================


%% init/1
%% ====================================================================
%% @doc Initialize the module, starting all necessary services.
%% @see worker_plugin_behaviour
-spec init(Args :: term()) -> ok | {error, Error :: any()}.
%% ====================================================================
init(_Args) ->
    {ok, GwPort} = application:get_env(?APP_Name, gateway_listener_port),
    {ok, Cert} = application:get_env(?APP_Name, global_registry_provider_cert_path),
    {ok, Acceptors} = application:get_env(?APP_Name, gateway_acceptor_number),
    {ok, NICs} = application:get_env(?APP_Name, gateway_network_interfaces),

    LocalServerPort = oneproxy:get_local_port(gateway_listener_port),

    {ok, _} = ranch:start_listener(?GATEWAY_LISTENER, Acceptors, ranch_tcp,
        [{port, LocalServerPort}], gateway_protocol, []),

    LocalPort = oneproxy:get_local_port(gateway_proxy_port),
    OpPid = spawn_link(fun() -> oneproxy:start_proxy(LocalPort, Cert, verify_none) end),
    register(gw_oneproxy_outgoing, OpPid),

    OpPid2 = spawn_link(fun() -> oneproxy:start_rproxy(GwPort, LocalServerPort, Cert, verify_none, no_http) end),
    register(gw_oneproxy_incoming, OpPid2),

    %% @TODO: On supervisor's exit we should be able to reinitialize the module.
	{ok, _} = gateway_dispatcher_supervisor:start_link(NICs),
    ok.


%% handle/2
%% ====================================================================
%% @doc Handle a message.
%% @see worker_plugin_behaviour
-spec handle(ProtocolVersion :: term(), Request :: term()) ->
    {ok, Ans :: term()} | {error, Error :: any()}.
%% ====================================================================
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
    ok.


%% cleanup/0
%% ====================================================================
%% @doc Cleanup any state associated with the module.
%% @see worker_plugin_behaviour
-spec cleanup() -> ok | {error, Error :: any()}.
%% ====================================================================
cleanup() ->
    ranch:stop_listener(?GATEWAY_LISTENER),
    exit(gw_oneproxy_outgoing, shutdown),
    exit(gw_oneproxy_incoming, shutdown),
    exit(?GATEWAY_DISPATCHER_SUPERVISOR, shutdown).


%% compute_request_hash/1
%% ====================================================================
%% @doc Computes a sha256 hash of an encoded protobuf #filerequest
-spec compute_request_hash(RequestBytes :: iodata()) -> Hash :: binary().
%% ====================================================================
compute_request_hash(RequestBytes) ->
    crypto:hash(sha256, RequestBytes).


%% ====================================================================
%% Internal functions
%% ====================================================================
