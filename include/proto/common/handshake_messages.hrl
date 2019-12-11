%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal version of protocol handshake messages.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(HANDSHAKE_MESSAGES_HRL).
-define(HANDSHAKE_MESSAGES_HRL, 1).

-include("proto/common/credentials.hrl").

-record(client_handshake_request, {
    credentials :: auth_manager:credentials(),
    nonce :: binary(),
    version :: binary(),
    compatible_oneprovider_versions :: [binary()]
}).

-record(provider_handshake_request, {
    provider_id :: od_provider:id(),
    token :: tokens:serialized()
}).

-record(handshake_response, {
    status = 'OK' :: 'OK' | 'MACAROON_EXPIRED' | 'MACAROON_NOT_FOUND' |
    'INVALID_METHOD' | 'ROOT_RESOURCE_NOT_FOUND' | 'INVALID_PROVIDER' |
    'BAD_SIGNATURE_FOR_MACAROON' | 'FAILED_TO_DESCRYPT_CAVEAT' |
    'NO_DISCHARGE_MACAROON_FOR_CAVEAT' | 'INVALID_MACAROON' | 'INVALID_NONCE' |
    'INCOMPATIBLE_VERSION' | 'INTERNAL_SERVER_ERROR'
}).

-endif.
