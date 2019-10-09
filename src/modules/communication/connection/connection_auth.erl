%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Client and provider authentication library.
%%% @end
%%%-------------------------------------------------------------------
-module(connection_auth).
-author("Tomasz Lichon").
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/onedata.hrl").

%% API
-export([handle_handshake/2, get_handshake_error_msg/1]).


%%%===================================================================
%%% API
%%%===================================================================

handle_handshake(#client_handshake_request{} = Request, IpAddress) ->
    handle_client_handshake(Request, IpAddress);
handle_handshake(#provider_handshake_request{} = Request, IpAddress) ->
    handle_provider_handshake(Request, IpAddress).


%%--------------------------------------------------------------------
%% @doc
%% Returns a server message with the handshake error details.
%% @end
%%--------------------------------------------------------------------
-spec get_handshake_error_msg(Error :: term()) -> #server_message{}.
get_handshake_error_msg(incompatible_client_version) ->
    #server_message{
        message_body = #handshake_response{
            status = 'INCOMPATIBLE_VERSION'
        }
    };
get_handshake_error_msg(invalid_token) ->
    #server_message{
        message_body = #handshake_response{
            status = 'INVALID_MACAROON'
        }
    };
get_handshake_error_msg(invalid_provider) ->
    #server_message{
        message_body = #handshake_response{
            status = 'INVALID_PROVIDER'
        }
    };
get_handshake_error_msg(invalid_nonce) ->
    #server_message{
        message_body = #handshake_response{
            status = 'INVALID_NONCE'
        }
    };
get_handshake_error_msg({badmatch, {error, Error}}) ->
    get_handshake_error_msg(Error);
get_handshake_error_msg({Code, Error, _Description}) when is_integer(Code) ->
    #server_message{
        message_body = #handshake_response{
            status = clproto_translator:translate_handshake_error(Error)
        }
    };
get_handshake_error_msg(_) ->
    #server_message{
        message_body = #handshake_response{
            status = 'INTERNAL_SERVER_ERROR'
        }
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec handle_client_handshake(#client_handshake_request{}, inet:ip_address()) ->
    {od_user:id(), session:id()} | no_return().
handle_client_handshake(#client_handshake_request{
    nonce = Nonce,
    auth = #token_auth{token = Token} = Auth0
} = Req, IpAddress) when is_binary(Nonce) ->

    assert_client_compatibility(Req, IpAddress),

    Auth1 = Auth0#token_auth{peer_ip = IpAddress},
    case user_identity:get_or_fetch(Auth1) of
        ?ERROR_FORBIDDEN ->
            throw(invalid_provider);
        {error, _} = Error ->
            ?debug("Cannot authorize user based on token ~s due to ~w", [Token, Error]),
            throw(invalid_token);
        {ok, #document{value = Iden = #user_identity{user_id = UserId}}} ->
            {ok, SessionId} = session_manager:reuse_or_create_fuse_session(
                Nonce, Iden, Auth1
            ),
            {UserId, SessionId}
    end.


%% @private
-spec handle_provider_handshake(#provider_handshake_request{}, inet:ip_address()) ->
    {od_provider:id(), session:id()} | no_return().
handle_provider_handshake(#provider_handshake_request{
    provider_id = ProviderId,
    nonce = Nonce
}, IpAddress) when is_binary(ProviderId) andalso is_binary(Nonce) ->

    case provider_logic:verify_provider_identity(ProviderId) of
        ok ->
            ok;
        Error ->
            ?debug("Discarding provider connection from ~ts @ ~s as its "
                   "identity cannot be verified: ~p", [
                provider_logic:to_string(ProviderId),
                inet_parse:ntoa(IpAddress), Error
            ]),
            throw(invalid_provider)
    end,

    case provider_logic:verify_provider_nonce(ProviderId, Nonce) of
        ok ->
            ok;
        Error1 ->
            ?debug("Discarding provider connection from ~ts @ ~s as its "
                   "nonce cannot be verified: ~p", [
                provider_logic:to_string(ProviderId),
                inet_parse:ntoa(IpAddress), Error1
            ]),
            throw(invalid_nonce)
    end,

    Identity = #user_identity{provider_id = ProviderId},
    SessionId = session_utils:get_provider_session_id(incoming, ProviderId),
    {ok, _} = session_manager:reuse_or_create_incoming_provider_session(
        SessionId, Identity
    ),
    {ProviderId, SessionId}.


%% @private
-spec assert_client_compatibility(#client_handshake_request{}, inet:ip_address()) ->
    ok | no_return().
assert_client_compatibility(#client_handshake_request{
    version = OcVersion
}, IpAddress) ->
    OpVersion = oneprovider:get_version(),

    case compatibility:check_products_compatibility(
        ?ONEPROVIDER, OpVersion, ?ONECLIENT, OcVersion
    ) of
        true ->
            ok;
        {false, CompatibleOcVersions} ->
            ?debug("Discarding connection from oneclient @ ~s because of "
            "incompatible version.~n"
            "Oneclient version: ~s ~n"
            "Oneprovider version: ~s, supports clients: ~p~n", [
                inet_parse:ntoa(IpAddress),
                OcVersion,
                OpVersion, CompatibleOcVersions
            ]),
            throw(incompatible_client_version)
    end.
