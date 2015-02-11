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
-spec deserialize_client_message(Message :: binary(), Cred :: undefined | #credentials{}) ->
    {ok, Record :: term()} | {error, term()}.
deserialize_client_message(Message, Cred) ->
    try client_messages:decode_msg(Message, 'ClientMessage') of
        #'ClientMessage'{message_id = Id, seq_num = SeqNum,
            last_message = Last, client_message = {_, Msg}} ->
            {ok, #client_message{message_id = Id, seq_num = SeqNum, last_message = Last,
                credentials = Cred, client_message = translator:translate_from_protobuf(Msg)}}
    catch
        _:Error -> {error, Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%% serialize server message to protobuf binary data
%% @end
%%--------------------------------------------------------------------
-spec serialize_server_message(#server_message{}) ->
    binary() | {error, term()}.
serialize_server_message(#server_message{message_id = Id, seq_num = Seq,
    last_message = Last, server_message = Msg}) ->
    try
        ProtobufMessage = translator:translate_to_protobuf(Msg),
        ServerMessage = #'ServerMessage'{message_id = Id, seq_num = Seq,
            last_message = Last, server_message = {element(1, Msg), ProtobufMessage}},
        {ok, server_messages:encode_msg(ServerMessage)}
    catch
        _:Error  -> {error, Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%% deserialize oneproxy protobuf binary data
%% @end
%%--------------------------------------------------------------------
-spec deserialize_oneproxy_certificate_info_message(Message :: binary()) ->
    {ok, #certificate_info{}} | {error, term()}.
deserialize_oneproxy_certificate_info_message(Message) ->
    try oneproxy_messages:decode_msg(Message, 'CertificateInfo') of
        #'CertificateInfo'{client_session_id = Id, client_subject_dn = Dn} ->
            {ok, #certificate_info{client_session_id = Id, client_subject_dn = Dn}}
    catch
        _:Error -> {error, Error}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
