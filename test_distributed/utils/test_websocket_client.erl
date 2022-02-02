%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% A rudimentary WebSocket client that allows connecting and sending
%%% messages to a WebSocket server.
%%% @end
%%%-------------------------------------------------------------------
-module(test_websocket_client).
-author("Lukasz Opiola").

-export([start/3]).
-export([send/2]).

%%% websocket client API
-export([init/2, websocket_handle/3, websocket_info/3, websocket_terminate/3]).

-record(client, {
    ws_controller_pid :: pid()
}).
-type client() :: #client{}.
-export_type([client/0]).

-type state() :: no_state.

%%%===================================================================
%%% API
%%%===================================================================

-spec start(oct_background:node_selector(), binary(), http_client:headers()) -> {ok, client()} | {error, term()}.
start(NodeSelector, Path, Headers) ->
    Url = binary_to_list(opw_test_rpc:call(NodeSelector, oneprovider, build_url, [wss, Path])),
    Opts = [{cacerts, opw_test_rpc:get_cert_chain_ders(NodeSelector)}],
    case websocket_client:start_link(Url, Headers, ?MODULE, [], Opts) of
        {ok, WsControllerPid} ->
            {ok, #client{ws_controller_pid = WsControllerPid}};
        {error, Reason} ->
            {error, Reason}
    end.


-spec send(client(), json_utils:json_term()) -> ok.
send(#client{ws_controller_pid = WsControllerPid}, JsonMessage) ->
    WsControllerPid ! {send, json_utils:encode(JsonMessage)},
    ok.

%%%===================================================================
%%% websocket client API
%%%===================================================================

-spec init([term()], websocket_req:req()) -> {ok, state()}.
init([], _) ->
    {ok, no_state}.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when data is received via WebSocket protocol.
%% @end
%%--------------------------------------------------------------------
-spec websocket_handle({text | binary | ping | pong, binary()},
    websocket_req:req(), state()) ->
    {ok, state()} |
    {reply, websocket_req:frame(), state()} |
    {close, Reply :: binary(), state()}.
websocket_handle({text, Data}, _, State) ->
    % currently, the client is used only to send data to the server
    ct:print("Unexpected text frame in ~w: ~s", [?MODULE, Data]),
    {ok, State};

websocket_handle({ping, <<"">>}, _, State) ->
    {ok, State};

websocket_handle({pong, <<"">>}, _, State) ->
    {ok, State};

websocket_handle(Msg, _, State) ->
    ct:print("Unexpected frame in ~p: ~p", [?MODULE, Msg]),
    {ok, State}.


%%--------------------------------------------------------------------
%% @doc
%% Callback called when a message is sent to the process handling the connection.
%% @end
%%--------------------------------------------------------------------
-spec websocket_info(term(), websocket_req:req(), state()) ->
    {ok, state()} |
    {reply, websocket_req:frame(), state()} |
    {close, Reply :: binary(), state()}.
websocket_info({send, Data}, _, State) ->
    {reply, {text, Data}, State};

websocket_info(terminate, _, State) ->
    {close, <<"">>, State};

websocket_info(Msg, _, State) ->
    ct:print("Unexpected message in ~p: ~p", [?MODULE, Msg]),
    {ok, State}.


-spec websocket_terminate({Reason, term()} | {Reason, integer(), binary()},
    websocket_req:req(), state()) -> ok when
    Reason :: normal | error | remote.
websocket_terminate(_Reason, _ConnState, _State) ->
    ok.
