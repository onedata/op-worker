%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Handler for inter-provider requests.
%%% @end
%%%-------------------------------------------------------------------
-module(provider_handler).
-author("Rafal Slota").

-include_lib("ctool/include/logging.hrl").

-record(handler_state, {}).

%% API
-export([init/3, websocket_init/3, websocket_handle/3, websocket_info/3, websocket_terminate/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Switches protocol to WebSocket
%% @end
%%--------------------------------------------------------------------
-spec init(Proto :: term(), Req :: term(), Opts :: term()) -> {upgrade, protocol, cowboy_websocket}.
init(_Proto, _Req, _Opts) ->
    {upgrade, protocol, cowboy_websocket}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy's webscoket_init callback. Initialize connection, proceed with TLS-GSI authentication. <br/>
%% If GSI validation fails, connection will be closed. <br/>
%% Currently validation is handled by Globus NIF library loaded on erlang slave nodes.
%% @end
%%--------------------------------------------------------------------
-spec websocket_init(TransportName :: atom(), Req :: term(), Opts :: list()) -> {ok, Req :: term(), State :: term()} | {shutdown, Req :: term()}.
websocket_init(TransportName, Req, _Opts) ->
    ?info("Provider's WebSocket connection received. Transport: ~p", [TransportName]),
    {ok, Req, #handler_state{}}.

%%--------------------------------------------------------------------
%% @doc
%% Cowboy's websocket_handle callback. Binary data was received on socket. <br/>
%% For more information please refer Cowboy's user manual.
%% @end
%%--------------------------------------------------------------------
-spec websocket_handle({Type :: atom(), Data :: term()}, Req, State) ->
    {reply, {Type :: atom(), Data :: term()}, Req, State} | {ok, Req, State} | {shutdown, Req, State}
    when
    Req :: term(),
    State :: #handler_state{}.
websocket_handle(Data, Req, #handler_state{} = State) ->
    ?info("Received data ~p", [Data]),
    {ok, Req, State}.

%%--------------------------------------------------------------------
%% @doc
%% Cowboy's webscoket_info callback. Erlang message received. <br/>
%% For more information please refer Cowboy's user manual.
%% @end
%%--------------------------------------------------------------------
-spec websocket_info(Msg :: term(), Req, State) ->
    {reply, {Type :: atom(), Data :: term()}, Req, State} | {ok, Req, State} | {shutdown, Req, State}
    when
    Req :: term(),
    State :: #handler_state{}.
websocket_info({Pid, shutdown}, Req, State) -> %% Handler internal shutdown request - close the connection
    Pid ! ok,
    {shutdown, Req, State};
websocket_info(_Msg, Req, State) ->
    ?info("WebSocket PUSH request. Message: ~p", [_Msg]),
    {ok, Req, State}.

%%--------------------------------------------------------------------
%% @doc
%% Cowboy's webscoket_info callback. Connection was closed. <br/>
%% For more information please refer Cowboy's user manual.
%% @end
%%--------------------------------------------------------------------
-spec websocket_terminate(Reason :: term(), Req, State) -> ok
    when
    Req :: term(),
    State :: #handler_state{}.
websocket_terminate(_Reason, _Req, _State) ->
    ?info("WebSocket connection  terminate"),
    ok.