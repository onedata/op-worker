%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides communication API with remote client/provider.
%%% @end
%%%-------------------------------------------------------------------
-module(communicator).
-author("Bartosz Walkowicz").

-include("timeouts.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").

%% API
-export([
    send_to_client/2, send_to_client/3,
    send_to_provider/2, send_to_provider/3,
    cast_to_provider/2, cast_to_provider/3,
    stream_to_provider/3,
    communicate_with_provider/2, communicate_with_provider/3,
    send_async_to_provider/3
]).

-type retries() :: non_neg_integer() | infinity.
-type generic_msg() :: tuple().
-type client_msg() :: #client_message{}.
-type server_msg() :: #server_message{}.
-type msg() :: client_msg() | server_msg().


%%%===================================================================
%%% API - convenience functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% @equiv send_to_client(SessionId, Msg, 1).
%% @end
%%--------------------------------------------------------------------
-spec send_to_client(session:id(), generic_msg()) ->
    ok | {error, Reason :: term()}.
send_to_client(SessionId, Msg) ->
    communicator:send_to_client(SessionId, Msg, 1).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to client. In case of errors keeps retrying
%% until either message is sent or no more retries are left.
%% Exception to this is lack of valid connections, that is
%% no_connections error which fails call immediately.
%% @end
%%--------------------------------------------------------------------
-spec send_to_client(session:id(), generic_msg(), retries()) ->
    ok | {error, Reason :: term()}.
send_to_client(SessionId, #server_message{} = Msg, Retries) ->
    MsgWithProxyInfo = protocol_utils:fill_proxy_info(Msg, SessionId),
    send_to_client_internal(SessionId, MsgWithProxyInfo, Retries);
send_to_client(SessionId, Msg, RetriesLeft) ->
    ServerMsg = #server_message{message_body = Msg},
    send_to_client(SessionId, ServerMsg, RetriesLeft).


%%--------------------------------------------------------------------
%% @doc
%% @equiv send_to_provider(SessionId, Msg, 1).
%% @end
%%--------------------------------------------------------------------
-spec send_to_provider(session:id(), generic_msg()) ->
    ok | {error, Reason :: term()}.
send_to_provider(SessionId, Msg) ->
    communicator:send_to_provider(SessionId, Msg, 1).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to peer provider. In case of errors keeps retrying
%% until either message is sent or no more retries are left.
%% @end
%%--------------------------------------------------------------------
-spec send_to_provider(session:id(), generic_msg(), retries()) ->
    ok | {error, Reason :: term()}.
send_to_provider(SessionId, #client_message{} = Msg, Retries) ->
    MsgWithProxyInfo = protocol_utils:fill_proxy_info(Msg, SessionId),
    send_to_provider_internal(SessionId, MsgWithProxyInfo, Retries);
send_to_provider(SessionId, Msg, Retries) ->
    ClientMsg = #client_message{message_body = Msg},
    send_to_provider(SessionId, ClientMsg, Retries).


%%--------------------------------------------------------------------
%% @doc
%% @equiv cast_to_provider(SessionId, Msg, 1).
%% @end
%%--------------------------------------------------------------------
-spec cast_to_provider(session:id(), generic_msg()) ->
    ok | {error, Reason :: term()}.
cast_to_provider(SessionId, Msg) ->
    cast_to_provider(SessionId, Msg, 1).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to peer provider and ignores send errors beside
%% no_connections error in case which retries sending.
%% @end
%%--------------------------------------------------------------------
-spec cast_to_provider(session:id(), generic_msg(), retries()) ->
    ok | {error, term()}.
cast_to_provider(SessionId, #client_message{} = Msg, 0) ->
    session_connections:ensure_connected(SessionId),
    connection_manager:send_async(SessionId, Msg);
cast_to_provider(SessionId, #client_message{} = Msg, Retries) ->
    session_connections:ensure_connected(SessionId),
    case connection_manager:send_async(SessionId, Msg) of
        ok ->
            ok;
        {error, no_connections} ->
            timer:sleep(?SEND_RETRY_DELAY),
            cast_to_provider(SessionId, Msg, retries_left(Retries))
    end;
cast_to_provider(SessionId, Msg, Retries) ->
    ClientMsg = #client_message{message_body = Msg},
    send_to_provider(SessionId, ClientMsg, Retries).


%%--------------------------------------------------------------------
%% @doc
%% Sends stream message to peer provider.
%% @end
%%--------------------------------------------------------------------
-spec stream_to_provider(session:id(), generic_msg(), sequencer:stream_id()) ->
    ok | {error, Reason :: term()}.
stream_to_provider(SessionId, #client_message{} = Msg, StmId) ->
    session_connections:ensure_connected(SessionId),
    sequencer:send_message(Msg, StmId, SessionId);
stream_to_provider(SessionId, Msg, StmId) ->
    ClientMsg = #client_message{message_body = Msg},
    stream_to_provider(SessionId, ClientMsg, StmId).


%%--------------------------------------------------------------------
%% @doc
%% @equiv communicate_with_provider(SessionId, Msg, 1).
%% @end
%%--------------------------------------------------------------------
-spec communicate_with_provider(session:id(), generic_msg()) ->
    {ok | msg()} | {error, Reason :: term()}.
communicate_with_provider(SessionId, Msg) ->
    communicate_with_provider(SessionId, Msg, 1).


%%--------------------------------------------------------------------
%% @doc
%% Sends message to peer provider and awaits answer. In case of errors keeps
%% retrying until either message is sent or no more retries are left.
%% @end
%%--------------------------------------------------------------------
-spec communicate_with_provider(session:id(), generic_msg(), retries()) ->
    {ok, msg()} | {error, Reason :: term()}.
communicate_with_provider(SessionId, #client_message{} = Msg, Retries) ->
    MsgWithProxyInfo = protocol_utils:fill_proxy_info(Msg, SessionId),
    communicate_with_provider_internal(SessionId, MsgWithProxyInfo, Retries);
communicate_with_provider(SessionId, Msg, Retries) ->
    ClientMsg = #client_message{message_body = Msg},
    communicate_with_provider(SessionId, ClientMsg, Retries).


%%--------------------------------------------------------------------
%% @doc
%% Creates message_id for message, if it is not already assigned,
%% and sends prepared message to peer.
%% @end
%%--------------------------------------------------------------------
-spec send_async_to_provider(session:id(), generic_msg(), pid()) ->
    {ok | message_id:id()} | {error, Reason :: term()}.
send_async_to_provider(SessionId, #client_message{} = Msg0, Recipient) ->
    {MsgId, Msg} = maybe_set_msg_id(Msg0, Recipient),
    Res = case Msg of
        #client_message{message_stream = #message_stream{
            stream_id = StmId
        }} when is_integer(StmId) ->
            stream_to_provider(SessionId, Msg, StmId);
        _ ->
            send_to_provider(SessionId, Msg)
    end,
    case Res of
        ok ->
            {ok, MsgId};
        Error ->
            Error
    end;
send_async_to_provider(SessionId, Msg, Recipient) ->
    ClientMsg = #client_message{message_body = Msg},
    send_async_to_provider(SessionId, ClientMsg, Recipient).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec send_to_client_internal(session:id(), server_msg(), retries()) ->
    ok | {error, Reason :: term()}.
send_to_client_internal(SessionId, Msg, 0) ->
    connection_manager:send_sync(SessionId, Msg);
send_to_client_internal(SessionId, Msg, Retries) ->
    case connection_manager:send_sync(SessionId, Msg) of
        ok ->
            ok;
        {error, no_connections} = NoConnectionsError ->
            NoConnectionsError;
        {error, _Reason} ->
            timer:sleep(?SEND_RETRY_DELAY),
            send_to_client_internal(SessionId, Msg, retries_left(Retries))
    end.


%% @private
-spec send_to_provider_internal(session:id(), client_msg(), retries()) ->
    ok | {error, Reason :: term()}.
send_to_provider_internal(SessionId, Msg, 0) ->
    session_connections:ensure_connected(SessionId),
    connection_manager:send_sync(SessionId, Msg);
send_to_provider_internal(SessionId, Msg, Retries) ->
    session_connections:ensure_connected(SessionId),
    case connection_manager:send_sync(SessionId, Msg) of
        ok ->
            ok;
        {error, _Reason} ->
            timer:sleep(?SEND_RETRY_DELAY),
            send_to_provider_internal(SessionId, Msg, retries_left(Retries))
    end.


%% @private
-spec communicate_with_provider_internal(session:id(), client_msg(),
    retries()) -> {ok, msg()} | {error, Reason :: term()}.
communicate_with_provider_internal(SessionId, Msg, 0) ->
    session_connections:ensure_connected(SessionId),
    connection_manager:communicate(SessionId, Msg);
communicate_with_provider_internal(SessionId, Msg, Retries) ->
    session_connections:ensure_connected(SessionId),
    case connection_manager:communicate(SessionId, Msg) of
        {ok, _Response} = Ans ->
            Ans;
        {error, _Reason} ->
            timer:sleep(?SEND_RETRY_DELAY),
            RetriesLeft = retries_left(Retries),
            communicate_with_provider_internal(SessionId, Msg, RetriesLeft)
    end.


%% @private
-spec retries_left(retries()) -> retries().
retries_left(infinity) -> infinity;
retries_left(Num) -> Num - 1.


%% @private
-spec maybe_set_msg_id(msg(), pid()) -> {message_id:id(), msg()}.
maybe_set_msg_id(#client_message{message_id = undefined} = Msg, Recipient) ->
    {ok, MsgId} = message_id:generate(Recipient),
    {MsgId, Msg#client_message{message_id = MsgId}};
maybe_set_msg_id(#client_message{message_id = MsgId} = Msg, _Recipient) ->
    {MsgId, Msg}.
