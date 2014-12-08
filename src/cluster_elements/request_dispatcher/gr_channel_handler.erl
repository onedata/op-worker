%% ===================================================================
%% @author Krzysztof Trzepla
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module provides raw communication layer between provider
%% and Global Registry based on WebSocket Secure protocol
%% @end
%% ===================================================================
-module(gr_channel_handler).
-behaviour(websocket_client_handler).
-author("Krzysztof Trzepla").

-include("registered_names.hrl").
-include("oneprovider_modules/gr_channel/gr_channel.hrl").
-include("gr_communication_protocol_pb.hrl").
-include_lib("ctool/include/logging.hrl").

%% WebSocket client handler callbacks
-export([init/2,
    websocket_handle/3,
    websocket_info/3,
    websocket_terminate/3]).

%% ====================================================================
%% Test API
%% ====================================================================
-ifdef(TEST).
-export([send_to_gr_channel/1]).
-endif.

%% ====================================================================
%% API functions
%% ====================================================================

%% init/2
%% ====================================================================
%% @doc Initializes the state for a session.
%% @end
-spec init(Args, Req) -> {ok, State} | {ok, State, KeepAlive} when
    Args :: term(),
    Req :: websocket_req:req(),
    State :: any(),
    KeepAlive :: integer().
%% ====================================================================
init([Pid], _Req) ->
    Pid ! {connected, self()},
    {ok, state}.


%% websocket_handle/3
%% ====================================================================
%% @doc Handles the data received from the Websocket connection.
%% @end
-spec websocket_handle(InFrame, Req, State) ->
    {ok, State} | {reply, OutFrame, State} | {close, Payload, State} when
    InFrame :: {text | binary | ping | pong, binary()},
    Req :: websocket_req:req(),
    State :: any(),
    Payload :: binary(),
    OutFrame :: cowboy_websocket:frame().
%% ====================================================================
websocket_handle({binary, Data}, _Req, State) ->
    %% Warning - update is asynch! Handler ends before update is finished.
    send_to_gr_channel(Data),
    {ok, State};

websocket_handle(_InFrame, _Req, State) ->
    {ok, State}.


%% websocket_info/3
%% ====================================================================
%% @doc Handles the Erlang message received.
%% @end
-spec websocket_info(Info, Req, State) ->
    {ok, State} | {reply, OutFrame, State} | {close, Payload, State} when
    Info :: any(),
    Req :: websocket_req:req(),
    State :: any(),
    Payload :: binary(),
    OutFrame :: cowboy_websocket:frame().
%% ====================================================================
websocket_info({push, Msg}, _Req, State) ->
    {reply, {binary, Msg}, State};

websocket_info(disconnect, _Req, State) ->
    {close, <<>>, State};

websocket_info(_Info, _Req, State) ->
    {ok, State}.


%% websocket_terminate/3
%% ====================================================================
%% @doc Performs any necessary cleanup of the state.
%% @end
-spec websocket_terminate(Reason, Req, State) -> ok when
    Reason :: {CloseType, Payload} | {CloseType, Code, Payload},
    CloseType :: normal | error | remote,
    Payload :: binary(),
    Code :: integer(),
    Req :: websocket_req:req(),
    State :: any().
%% ====================================================================
websocket_terminate(_Reason, _Req, _State) ->
    ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% send_to_gr_channel/1
%% ====================================================================
%% @doc Decode message and send to gr_channel module.
%% @end
-spec send_to_gr_channel(Data :: binary()) -> ok.
%% ====================================================================
send_to_gr_channel(Data) ->
    try
        {ok, GRMessage} = pb:decode("gr_communication_protocol", "message", Data),
        ProtocolVersion = GRMessage#message.protocol_version,
        Type = GRMessage#message.message_type,
        Decoder = GRMessage#message.message_decoder_name,
        Input = GRMessage#message.input,
        {ok, Request} = pb:decode(Decoder, Type, Input),

        Ans = gen_server:call(?Dispatcher_Name, {node_chosen, {gr_channel, ProtocolVersion, {gr_message, Request}}}),
        case Ans of
            ok ->
                ok;
            Other ->
                ?error("Dispatcher connection error: ~p for request ~p", [Other, Request])
        end
    catch
        E1:E2 ->
            ?error("Dispatcher connection error: ~p:~p for request ~p", [E1, E2, Data])
    end,
    ok.