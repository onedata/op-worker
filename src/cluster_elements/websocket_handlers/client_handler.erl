%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module forwards requests from socket to dispatcher.
%% @end
%% ===================================================================

%% TODO
%% Zrobić tak, żeby odpowiadając atomem nie trzeba było wysyłać do handlera
%% rekordu atom tylko, żeby handler sam przepakował ten atom do rekordu

-module(client_handler).
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("public_key/include/public_key.hrl").

%% Holds state of websocket connection. peer_dn field contains DN of certificate of connected peer.
-record(handler_state, {peer_serial, dispatcher_timeout, fuse_id = "", connection_id = "",
    peer_type = user, %% user | provider
    provider_id = <<>>, %% only valid if peer_type = provider
    peer_dn, access_token, user_global_id %% only valid if peer_type = user
}).

%% ====================================================================
%% API
%% ====================================================================
-export([init/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

%% ====================================================================
%% API functions
%% ====================================================================

%% init/3
%% ====================================================================
%% @doc Switches protocol to WebSocket
-spec init(Proto :: term(), Req :: term(), Opts :: term()) -> {upgrade, protocol, cowboy_websocket}.
%% ====================================================================
init(_Proto, _Req, _Opts) ->
    {upgrade, protocol, cowboy_websocket}.


%% websocket_init/3
%% ====================================================================
%% @doc Cowboy's webscoket_init callback. Initialize connection, proceed with TLS-GSI authentication. <br/>
%%      If GSI validation fails, connection will be closed. <br/>
%%      Currently validation is handled by Globus NIF library loaded on erlang slave nodes.
-spec websocket_init(TransportName :: atom(), Req :: term(), Opts :: list()) -> {ok, Req :: term(), State :: term()} | {shutdown, Req :: term()}.
%% ====================================================================
websocket_init(TransportName, Req, _Opts) ->
    ?info("Provider's WebSocket connection received. Transport: ~p", [TransportName]),
    {ok, Req, #handler_state{}}.

%% websocket_handle/3
%% ====================================================================
%% @doc Cowboy's websocket_handle callback. Binary data was received on socket. <br/>
%%      For more information please refer Cowboy's user manual.
-spec websocket_handle({Type :: atom(), Data :: term()}, Req, State) ->
    {reply, {Type :: atom(), Data :: term()}, Req, State} | {ok, Req, State} | {shutdown, Req, State}
    when
    Req :: term(),
    State :: #handler_state{}.
%% ====================================================================
websocket_handle(Data, Req, #handler_state{} = State) ->
    ?info("Received data ~p", [Data]),
    {ok, Req, State}.

%% websocket_info/3
%% ====================================================================
%% @doc Cowboy's webscoket_info callback. Erlang message received. <br/>
%%      For more information please refer Cowboy's user manual.
-spec websocket_info(Msg :: term(), Req, State) ->
    {reply, {Type :: atom(), Data :: term()}, Req, State} | {ok, Req, State} | {shutdown, Req, State}
    when
    Req :: term(),
    State :: #handler_state{}.
%% ====================================================================
websocket_info({Pid, shutdown}, Req, State) -> %% Handler internal shutdown request - close the connection
    Pid ! ok,
    {shutdown, Req, State};
websocket_info(_Msg, Req, State) ->
    ?info("WebSocket PUSH request. Message: ~p", [_Msg]),
    {ok, Req, State}.

%% websocket_terminate/3
%% ====================================================================
%% @doc Cowboy's webscoket_info callback. Connection was closed. <br/>
%%      For more information please refer Cowboy's user manual.
-spec websocket_terminate(Reason :: term(), Req, State) -> ok
    when
    Req :: term(),
    State :: #handler_state{}.
%% ====================================================================
websocket_terminate(_Reason, _Req, State) ->
    ?info("WebSocket connection  terminate"),
    ok.

%% ====================================================================
%% Internal functions
%% ====================================================================