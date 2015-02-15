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

-include("proto/oneclient/messages.hrl").
-include("proto/oneproxy/oneproxy_messages.hrl").
-include("proto_internal/oneclient/client_messages.hrl").
-include("proto_internal/oneclient/server_messages.hrl").
-include("proto_internal/oneproxy/oneproxy_messages.hrl").

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
-spec deserialize_client_message(Message :: binary(), SessionId :: undefined | session:session_id()) ->
    {ok, ClientMsg :: #client_message{}} | no_return().
deserialize_client_message(Message, SessionId) ->
    #'ClientMessage'{message_id = Id, seq_num = SeqNum, last_message = Last,
        client_message = {_, Msg}} =
        client_messages:decode_msg(Message, 'ClientMessage'),
    {ok, #client_message{message_id = message_id:decode(Id), seq_num = SeqNum, last_message = Last,
        session_id = SessionId, client_message = translator:translate_from_protobuf(Msg)}}.

%%--------------------------------------------------------------------
%% @doc
%% serialize server message to protobuf binary data
%% @end
%%--------------------------------------------------------------------
-spec serialize_server_message(#server_message{}) -> {ok, binary()} | no_return().
serialize_server_message(#server_message{message_id = Id, seq_num = Seq,
    last_message = Last, server_message = Msg}) ->
    ProtobufMessage = translator:translate_to_protobuf(Msg),
    ServerMessage = #'ServerMessage'{message_id = message_id:encode(Id), seq_num = Seq,
        last_message = Last, server_message = {element(1, Msg), ProtobufMessage}},
    {ok, server_messages:encode_msg(ServerMessage)}.

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
