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

-include("proto/oneproxy/oneproxy_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneproxy/oneproxy_messages.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("clproto/include/oneproxy_messages.hrl").

%% API
-export([deserialize_client_message/2, serialize_server_message/1,
    deserialize_oneproxy_certificate_info_message/1]).

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
        message_body = {_, MsgBody}
    } = messages:decode_msg(Message, 'ClientMessage'),
    {ok, DecodedId} = message_id:decode(MsgId),
    {ok, #client_message{
        message_id = DecodedId,
        message_stream = translator:translate_from_protobuf(MsgStm),
        session_id = SessionId,
        message_body = translator:translate_from_protobuf(MsgBody)
    }}.

%%--------------------------------------------------------------------
%% @doc
%% serialize server message to protobuf binary data
%% @end
%%--------------------------------------------------------------------
-spec serialize_server_message(#server_message{}) -> {ok, binary()} | no_return().
serialize_server_message(#server_message{message_id = MsgId, message_stream = MsgStm,
    message_body = MsgBody}) ->
    {ok, EncodedId} = message_id:encode(MsgId),
    ServerMessage = #'ServerMessage'{
        message_id = EncodedId,
        message_stream = translator:translate_to_protobuf(MsgStm),
        message_body = translator:translate_to_protobuf(MsgBody)
    },
    {ok, messages:encode_msg(ServerMessage)}.

%%--------------------------------------------------------------------
%% @doc
%% deserialize oneproxy protobuf binary data
%% @end
%%--------------------------------------------------------------------
-spec deserialize_oneproxy_certificate_info_message(Message :: binary()) ->
    {ok, #certificate_info{}} | no_return().
deserialize_oneproxy_certificate_info_message(Message) ->
    #'CertificateInfo'{client_session_id = Id, client_subject_dn = Dn} =
        oneproxy_messages:decode_msg(Message, 'CertificateInfo'),
    {ok, #certificate_info{client_session_id = Id, client_subject_dn = Dn}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================
