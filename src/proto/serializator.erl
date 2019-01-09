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
-export([load_msg_defs/0]).
-export([deserialize_client_message/2, serialize_server_message/2]).
-export([deserialize_server_message/2, serialize_client_message/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Loads clproto message definitions for later use by enif_protobuf
%% when encoding and decoding messages.
%% @end
%%--------------------------------------------------------------------
-spec load_msg_defs() -> ok.
load_msg_defs() ->
    enif_protobuf:load_cache(messages:get_msg_defs()).


%%--------------------------------------------------------------------
%% @doc
%% deserialize protobuf binary data to client message
%% @end
%%--------------------------------------------------------------------
-spec deserialize_client_message(Message :: binary(), SessionId :: undefined | session:id()) ->
    {ok, ClientMsg :: #client_message{}} | no_return().
deserialize_client_message(Message, SessionId) ->
    DecodedMsg = messages:decode_msg(Message, 'ClientMessage'),
%%    DecodedMsg = enif_protobuf:decode(Message, 'ClientMessage'),

    #'ClientMessage'{
        message_id = MsgId,
        message_stream = MsgStm,
        message_body = {_, MsgBody},
        proxy_session_id = PSessID,
        proxy_session_macaroon = PToken
    } = DecodedMsg,

    {ok, DecodedId} = message_id:decode(MsgId),
    Rec = #client_message{
        message_id = DecodedId,
        message_stream = translator:translate_from_protobuf(MsgStm),
        session_id = SessionId,
        proxy_session_id = PSessID,
        proxy_session_auth = translator:translate_from_protobuf(PToken),
        message_body = translator:translate_from_protobuf(MsgBody)
    },

    {ok, Rec}.


%%--------------------------------------------------------------------
%% @doc
%% deserialize protobuf binary data to client message
%% @end
%%--------------------------------------------------------------------
-spec deserialize_server_message(Message :: binary(), SessionId :: undefined | session:id()) ->
    {ok, ClientMsg :: #server_message{}} | no_return().
deserialize_server_message(Message, _SessionId) ->
    DecodedMsg = messages:decode_msg(Message, 'ServerMessage'),
%%    DecodedMsg = enif_protobuf:decode(Message, 'ServerMessage'),

    #'ServerMessage'{
        message_id = MsgId,
        message_stream = MsgStm,
        message_body = {_, MsgBody},
        proxy_session_id = SessionId
    } = DecodedMsg,

    {ok, DecodedId} = message_id:decode(MsgId),
    Rec = #server_message{
        message_id = DecodedId,
        message_stream = translator:translate_from_protobuf(MsgStm),
        message_body = translator:translate_from_protobuf(MsgBody),
        proxy_session_id = SessionId
    },

    {ok, Rec}.

%%--------------------------------------------------------------------
%% @doc
%% serialize server message to protobuf binary data
%% @end
%%--------------------------------------------------------------------
-spec serialize_server_message(#server_message{}, boolean()) ->
    {ok, binary()} | no_return().
serialize_server_message(#server_message{
    message_id = MsgId,
    message_stream = MsgStm,
    message_body = MsgBody,
    proxy_session_id = SessionId
}, VerifyMsg) ->

    {ok, EncodedId} = message_id:encode(MsgId),
    ServerMessage = #'ServerMessage'{
        message_id = EncodedId,
        message_stream = translator:translate_to_protobuf(MsgStm),
        message_body = translator:translate_to_protobuf(MsgBody),
        proxy_session_id = SessionId
    },

    case VerifyMsg of
        true ->
            ok = messages:verify_msg(ServerMessage);
        false ->
            ok
    end,

    {ok, enif_protobuf:encode(ServerMessage)}.


%%--------------------------------------------------------------------
%% @doc
%% serialize client message to protobuf binary data
%% @end
%%--------------------------------------------------------------------
-spec serialize_client_message(#client_message{}, boolean()) ->
    {ok, binary()} | no_return().
serialize_client_message(#client_message{
    message_id = MsgId,
    message_stream = MsgStm,
    proxy_session_id = PSessID,
    proxy_session_auth = Auth,
    message_body = MsgBody
}, VerifyMsg) ->

    {ok, EncodedId} = message_id:encode(MsgId),
    ClientMessage = #'ClientMessage'{
        message_id = EncodedId,
        message_stream = translator:translate_to_protobuf(MsgStm),
        message_body = translator:translate_to_protobuf(MsgBody),
        proxy_session_id = PSessID,
        proxy_session_macaroon = translator:translate_to_protobuf(Auth)
    },

    case VerifyMsg of
        true ->
            ok = messages:verify_msg(ClientMessage);
        false ->
            ok
    end,

    {ok, enif_protobuf:encode(ClientMessage)}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
