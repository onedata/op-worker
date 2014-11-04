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
-define(network_interfaces, [{127,0,0,1}, {127,0,0,1}, {127,0,0,1}]).

-export([init/1, handle/2, cleanup/0]).
-export([compute_request_hash/1]).

-export([test/1]).
test(FileId) ->
    Offset = crypto:rand_uniform(0, 2048),
    Size = crypto:rand_uniform(0, 2048),
    Req = #fetchrequest{file_id = FileId, offset = Offset, size = Size},
    gen_server:cast(?MODULE, {asynch, 1, #fetch{remote = {192,168,122,236}, notify = self(), request = Req}}),
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
    ok.


-spec compute_request_hash(RequestBytes :: iodata()) -> Hash :: binary().
compute_request_hash(RequestBytes) ->
    crypto:hash(sha256, RequestBytes).
