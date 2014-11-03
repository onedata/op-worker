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
-include_lib("ctool/include/logging.hrl").

%% @TODO: config
-define(acceptor_number, 100).
-define(max_concurrent_connections, 5).

-export([init/1, handle/2, cleanup/0]).
-export([compute_request_hash/1]).

-export([test/1]).
test(FileId) ->
    Req = #fetchrequest{file_id = FileId, offset = 0, size = 128},
    gen_server:cast(?MODULE, #fetch{remote = {192,168,122,236}, notify = self(), request = Req}),
    receive
        A -> A
    after
        timer:seconds(3) -> timeout
    end.

%% ====================================================================
%% API functions
%% ====================================================================

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

handle(_ProtocolVersion, #fetch{} = Request) ->
    gen_server:cast(?GATEWAY_DISPATCHER, Request),
    ok;

handle(_ProtocolVersion, _Msg) ->
    ?log_call(_Msg),
    %% @TODO: handle exit from gateway_dispatcher_supervisor
    ok.

cleanup() ->
    ranch:stop_listener(?GATEWAY_LISTENER),
    ok.


-spec compute_request_hash(RequestBytes :: binary()) -> Hash :: binary().
compute_request_hash(RequestBytes) ->
    crypto:hash(sha256, RequestBytes).
