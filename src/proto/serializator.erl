%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Serializes and deserializes protobuf messages
%%% @end
%%%-------------------------------------------------------------------
-module(serializator).
-author("Tomasz Lichon").

-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([deserialize_client_message/2, serialize_server_message/1]).
-export([deserialize_server_message/2, serialize_client_message/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% deserialize protobuf binary data to client message
%% @end
%%--------------------------------------------------------------------
-spec deserialize_client_message(Message :: binary(), SessionId :: undefined | session:id()) ->
    {ok, ClientMsg :: #client_message{}} | no_return().
deserialize_client_message(Message, SessionId) ->
    #'ClientMessage'{
        message_id = MsgId,
        message_stream = MsgStm,
        message_body = {_, MsgBody},
        proxy_session_id = PSessID,
        proxy_session_token = PToken
    } = messages:decode_msg(Message, 'ClientMessage'),
    {ok, DecodedId} = message_id:decode(MsgId),
    {ok, #client_message{
        message_id = DecodedId,
        message_stream = translator:translate_from_protobuf(MsgStm),
        session_id = SessionId,
        proxy_session_id = PSessID,
        proxy_session_auth = translator:translate_from_protobuf(PToken),
        message_body = translator:translate_from_protobuf(MsgBody)
    }}.


%%--------------------------------------------------------------------
%% @doc
%% deserialize protobuf binary data to client message
%% @end
%%--------------------------------------------------------------------
-spec deserialize_server_message(Message :: binary(), SessionId :: undefined | session:id()) ->
    {ok, ClientMsg :: #server_message{}} | no_return().
deserialize_server_message(Message, _SessionId) ->
    #'ServerMessage'{
        message_id = MsgId,
        message_stream = MsgStm,
        message_body = {_, MsgBody},
        proxy_session_id = SessionId
    } = messages:decode_msg(Message, 'ServerMessage'),
    {ok, DecodedId} = message_id:decode(MsgId),
    {ok, #server_message{
        message_id = DecodedId,
        message_stream = translator:translate_from_protobuf(MsgStm),
        message_body = translator:translate_from_protobuf(MsgBody),
        proxy_session_id = SessionId
    }}.

%%--------------------------------------------------------------------
%% @doc
%% serialize server message to protobuf binary data
%% @end
%%--------------------------------------------------------------------
-spec serialize_server_message(#server_message{}) -> {ok, binary()} | no_return().
serialize_server_message(#server_message{message_id = MsgId, message_stream = MsgStm,
    message_body = MsgBody, proxy_session_id = SessionId}) ->
    {ok, EncodedId} = message_id:encode(MsgId),
    ServerMessage = #'ServerMessage'{
        message_id = EncodedId,
        message_stream = translator:translate_to_protobuf(MsgStm),
        message_body = translator:translate_to_protobuf(MsgBody),
        proxy_session_id = SessionId
    },
    {ok, messages:encode_msg(ServerMessage, [verify])}.


%%--------------------------------------------------------------------
%% @doc
%% serialize client message to protobuf binary data
%% @end
%%--------------------------------------------------------------------
-spec serialize_client_message(#client_message{}) -> {ok, binary()} | no_return().
serialize_client_message(#client_message{message_id = MsgId, message_stream = MsgStm, proxy_session_id = PSessID,
    proxy_session_auth = Auth, message_body = MsgBody}) ->
    {ok, EncodedId} = message_id:encode(MsgId),
    ClientMessage = #'ClientMessage'{
        message_id = EncodedId,
        message_stream = translator:translate_to_protobuf(MsgStm),
        message_body = translator:translate_to_protobuf(MsgBody),
        proxy_session_id = PSessID,
        proxy_session_token = translator:translate_to_protobuf(Auth)
    },
    {ok, messages:encode_msg(ClientMessage, [verify])}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
