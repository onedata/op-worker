%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module handling translations between protobuf and internal protocol.
%%% @end
%%%-------------------------------------------------------------------
-module(clproto_connection_translator).
-author("Tomasz Lichon").
-author("Rafal Slota").

-include("proto/common/handshake_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([
    from_protobuf/1, session_mode_from_protobuf/1,
    to_protobuf/1, handshake_error_to_protobuf/1, session_mode_to_protobuf/1
]).

%%%===================================================================
%%% FROM PROTOBUF
%%%===================================================================

-spec from_protobuf(tuple()) -> tuple(); (undefined) -> undefined.

% HANDSHAKE
from_protobuf(#'ClientHandshakeRequest'{
    macaroon = Macaroon,
    session_id = Nonce,
    version = Version,
    compatible_oneprovider_versions = CompOpVersions,
    session_mode = SessionMode
}) ->
    #client_handshake_request{
        client_tokens = from_protobuf(Macaroon),
        nonce = Nonce,
        version = Version,
        compatible_oneprovider_versions = CompOpVersions,
        session_mode = session_mode_from_protobuf(SessionMode)
    };
from_protobuf(#'ProviderHandshakeRequest'{
    provider_id = ProviderId,
    token = Token
}) ->
    #provider_handshake_request{
        provider_id = ProviderId,
        token = Token
    };
from_protobuf(#'Macaroon'{
    macaroon = AccessToken
}) ->
    #client_tokens{access_token = AccessToken};
from_protobuf(#'HandshakeResponse'{status = Status}) ->
    #handshake_response{status = Status};


% PROCESSING STATUS
from_protobuf(#'ProcessingStatus'{code = Code}) ->
    #processing_status{code = Code};


%% DIAGNOSTIC
from_protobuf(#'Ping'{data = Data}) ->
    #ping{data = Data};
from_protobuf(#'GetProtocolVersion'{}) ->
    #get_protocol_version{};
from_protobuf(#'ProtocolVersion'{
    major = Major,
    minor = Minor
}) ->
    #protocol_version{
        major = Major,
        minor = Minor
    };
from_protobuf(#'GetConfiguration'{}) ->
    #get_configuration{};


%% STREAM
from_protobuf(#'MessageStream'{
    stream_id = StmId,
    sequence_number = SeqNum
}) ->
    #message_stream{
        stream_id = StmId,
        sequence_number = SeqNum
    };
from_protobuf(#'MessageRequest'{
    stream_id = StmId,
    lower_sequence_number = LowerSeqNum,
    upper_sequence_number = UpperSeqNum
}) ->
    #message_request{
        stream_id = StmId,
        lower_sequence_number = LowerSeqNum,
        upper_sequence_number = UpperSeqNum
    };
from_protobuf(#'MessageAcknowledgement'{
    stream_id = StmId,
    sequence_number = SeqNum
}) ->
    #message_acknowledgement{
        stream_id = StmId,
        sequence_number = SeqNum
    };
from_protobuf(#'MessageStreamReset'{stream_id = StmId}) ->
    #message_stream_reset{stream_id = StmId};
from_protobuf(#'EndOfMessageStream'{}) ->
    #end_of_message_stream{};


%% SESSION
from_protobuf(#'CloseSession'{}) ->
    #close_session{};

%% OTHER
from_protobuf(undefined) -> undefined.


-spec session_mode_from_protobuf(undefined | 'NORMAL' | 'OPEN_HANDLE') ->
    session:mode().
session_mode_from_protobuf('OPEN_HANDLE') -> open_handle;
session_mode_from_protobuf(_)             -> normal.



%%%===================================================================
%%% TO PROTOBUF
%%%===================================================================

-spec to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.

%% HANDSHAKE
to_protobuf(#provider_handshake_request{
    provider_id = ProviderId,
    token = Token
}) ->
    {provider_handshake_request, #'ProviderHandshakeRequest'{
        provider_id = ProviderId,
        token = Token
    }};
to_protobuf(#handshake_response{
    status = Status
}) ->
    {handshake_response, #'HandshakeResponse'{
        status = Status
    }};
to_protobuf(#client_tokens{
    access_token = SerializedToken
}) ->
    #'Macaroon'{
        macaroon = SerializedToken
    };


% PROCESSING STATUS
to_protobuf(#processing_status{code = Code}) ->
    {processing_status, #'ProcessingStatus'{code = Code}};


%% DIAGNOSTIC
to_protobuf(#pong{data = Data}) ->
    {pong, #'Pong'{data = Data}};
to_protobuf(#protocol_version{
    major = Major,
    minor = Minor
}) ->
    {protocol_version, #'ProtocolVersion'{
        major = Major,
        minor = Minor
    }};
to_protobuf(#configuration{
    root_guid = RootGuid,
    subscriptions = Subs,
    disabled_spaces = Spaces
}) ->
    {configuration, #'Configuration'{
        root_uuid = RootGuid,
        subscriptions = lists:map(
            fun(Sub) ->
                {_, Record} = clproto_subscription_translator:to_protobuf(Sub),
                Record
            end, Subs),
        disabled_spaces = Spaces
    }};


%% STREAM
to_protobuf(#message_stream{
    stream_id = StmId,
    sequence_number = SeqNum
}) ->
    #'MessageStream'{
        stream_id = StmId,
        sequence_number = SeqNum
    };
to_protobuf(#message_request{
    stream_id = StmId,
    lower_sequence_number = LowerSeqNum,
    upper_sequence_number = UpperSeqNum
}) ->
    {message_request, #'MessageRequest'{
        stream_id = StmId,
        lower_sequence_number = LowerSeqNum,
        upper_sequence_number = UpperSeqNum
    }};
to_protobuf(#message_acknowledgement{
    stream_id = StmId,
    sequence_number = SeqNum
}) ->
    {message_acknowledgement, #'MessageAcknowledgement'{
        stream_id = StmId,
        sequence_number = SeqNum
    }};
to_protobuf(#message_stream_reset{stream_id = StmId}) ->
    {message_stream_reset, #'MessageStreamReset'{stream_id = StmId}};
to_protobuf(#end_of_message_stream{}) ->
    {end_of_stream, #'EndOfMessageStream'{}};

%% SESSION
to_protobuf(#close_session{}) ->
    {close_session, #'CloseSession'{}};


%% OTHER
to_protobuf(undefined) -> undefined.


-spec handshake_error_to_protobuf(Type :: binary()) -> Type :: atom().
handshake_error_to_protobuf(<<"token_expired">>) ->
    'MACAROON_EXPIRED';
handshake_error_to_protobuf(<<"token_not_found">>) ->
    'MACAROON_NOT_FOUND';
handshake_error_to_protobuf(<<"invalid_token">>) ->
    'INVALID_MACAROON';
handshake_error_to_protobuf(<<"invalid_method">>) ->
    'INVALID_METHOD';
handshake_error_to_protobuf(<<"root_resource_not_found">>) ->
    'ROOT_RESOURCE_NOT_FOUND';
handshake_error_to_protobuf(<<"invalid_provider">>) ->
    'INVALID_PROVIDER';
handshake_error_to_protobuf(<<"bad_signature_for_macaroon">>) ->
    'BAD_SIGNATURE_FOR_MACAROON';
handshake_error_to_protobuf(<<"failed_to_decrypt_caveat">>) ->
    'FAILED_TO_DESCRYPT_CAVEAT';
handshake_error_to_protobuf(<<"no_discharge_macaroon_for_caveat">>) ->
    'NO_DISCHARGE_MACAROON_FOR_CAVEAT';
handshake_error_to_protobuf(_) ->
    'INTERNAL_SERVER_ERROR'.


-spec session_mode_to_protobuf(undefined | session:mode()) ->
    'NORMAL' | 'OPEN_HANDLE'.
session_mode_to_protobuf(open_handle) -> 'OPEN_HANDLE';
session_mode_to_protobuf(_)           -> 'NORMAL'.
