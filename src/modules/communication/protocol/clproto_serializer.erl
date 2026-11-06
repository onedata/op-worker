%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module handling serialization to and deserialization from protobuf format
%%% of client/server messages.
%%% @end
%%%-------------------------------------------------------------------
-module(clproto_serializer).
-author("Tomasz Lichon").

-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("clproto/include/messages.hrl").


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


-spec deserialize_client_message(binary(), undefined | session:id()) ->
    {ok, #client_message{}} | no_return().
deserialize_client_message(Message, SessionId) ->
    #'ClientMessage'{
        message_id = MsgId,
        message_stream = MsgStm,
        message_body = {_, MsgBody},
        proxy_session_id = EffSessionId,
        proxy_session_macaroon = PToken,
        proxy_session_mode = PSessionMode
    } = enif_protobuf:decode(Message, 'ClientMessage'),

    {ok, DecodedId} = clproto_message_id:decode(MsgId),

    try
        Stream = clproto_translator:from_protobuf(MsgStm),
        EffSessionAuth = clproto_translator:from_protobuf(PToken),
        EffSessionMode = clproto_translator:session_mode_from_protobuf(PSessionMode),
        Body = clproto_translator:from_protobuf(MsgBody),

        {ok, #client_message{
            message_id = DecodedId,
            message_stream = Stream,
            session_id = SessionId,
            effective_session_id = EffSessionId,
            effective_client_tokens = EffSessionAuth,
            effective_session_mode = EffSessionMode,
            message_body = Body
        }}
    catch
        _:Reason ->
            throw({translation_failed, Reason, DecodedId})
    end.


-spec deserialize_server_message(binary(), undefined | session:id()) ->
    {ok, #server_message{}} | no_return().
deserialize_server_message(Message, SessionId) ->
    #'ServerMessage'{
        message_id = MsgId,
        message_stream = MsgStm,
        message_body = {_, MsgBody},
        proxy_session_id = EffSessionId
    } = enif_protobuf:decode(Message, 'ServerMessage'),

    {ok, DecodedId} = clproto_message_id:decode(MsgId),
    {ok, #server_message{
        message_id = DecodedId,
        message_stream = clproto_translator:from_protobuf(MsgStm),
        message_body = clproto_translator:from_protobuf(MsgBody),
        effective_session_id = utils:ensure_defined(
            EffSessionId, undefined, SessionId
        )
    }}.


-spec serialize_server_message(#server_message{}, VerifyMsg :: boolean()) ->
    {ok, binary()} | no_return().
serialize_server_message(#server_message{
    message_id = MsgId,
    message_stream = MsgStm,
    message_body = MsgBody,
    effective_session_id = EffSessionId
}, VerifyMsg) ->

    {ok, EncodedId} = clproto_message_id:encode(MsgId),
    ServerMessage = #'ServerMessage'{
        message_id = EncodedId,
        message_stream = clproto_translator:to_protobuf(MsgStm),
        message_body = clproto_translator:to_protobuf(MsgBody),
        proxy_session_id = EffSessionId
    },
    serialize_message(ServerMessage, VerifyMsg).


-spec serialize_client_message(#client_message{}, VerifyMsg :: boolean()) ->
    {ok, binary()} | no_return().
serialize_client_message(#client_message{
    message_id = MsgId,
    message_stream = MsgStm,
    effective_session_id = EffSessionId,
    effective_client_tokens = Auth,
    effective_session_mode = EffSessionMode,
    message_body = MsgBody
}, VerifyMsg) ->

    {ok, EncodedId} = clproto_message_id:encode(MsgId),
    ClientMessage = #'ClientMessage'{
        message_id = EncodedId,
        message_stream = clproto_translator:to_protobuf(MsgStm),
        message_body = clproto_translator:to_protobuf(MsgBody),
        proxy_session_id = EffSessionId,
        proxy_session_macaroon = clproto_translator:to_protobuf(Auth),
        proxy_session_mode = clproto_translator:session_mode_to_protobuf(EffSessionMode)
    },
    serialize_message(ClientMessage, VerifyMsg).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec serialize_message(#'ClientMessage'{} | #'ServerMessage'{}, boolean()) ->
    {ok, binary()} | no_return().
serialize_message(Message, VerifyMsg) ->
    case VerifyMsg of
        true ->
            ok = messages:verify_msg(Message);
        false ->
            ok
    end,
    
    case enif_protobuf:encode(Message) of
        {error, Reason} ->
            throw({serialization_failed, Reason});
        EncodedMessage ->
            {ok, EncodedMessage}
    end.
