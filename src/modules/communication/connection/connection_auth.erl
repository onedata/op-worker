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
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([handle_handshake/2, get_handshake_error/1]).

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
-spec get_handshake_error(Error :: term()) -> #server_message{}.
get_handshake_error(incompatible_client_version) ->
    #server_message{
        message_body = #handshake_response{
            status = 'INCOMPATIBLE_VERSION'
        }
    };
get_handshake_error(invalid_token) ->
    #server_message{
        message_body = #handshake_response{
            status = 'INVALID_MACAROON'
        }
    };
get_handshake_error(bad_macaroon) ->
    #server_message{
        message_body = #handshake_response{
            status = 'INVALID_MACAROON'
        }
    };
get_handshake_error(invalid_provider) ->
    #server_message{
        message_body = #handshake_response{
            status = 'INVALID_PROVIDER'
        }
    };
get_handshake_error(invalid_nonce) ->
    #server_message{
        message_body = #handshake_response{
            status = 'INVALID_NONCE'
        }
    };
get_handshake_error({badmatch, {error, Error}}) ->
    get_handshake_error(Error);
get_handshake_error({Code, Error, _Description}) when is_integer(Code) ->
    #server_message{
        message_body = #handshake_response{
            status = clproto_translator:translate_handshake_error(Error)
        }
    };
get_handshake_error(_) ->
    #server_message{
        message_body = #handshake_response{
            status = 'INTERNAL_SERVER_ERROR'
        }
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles client handshake request
%% @end
%%--------------------------------------------------------------------
-spec handle_client_handshake(#client_handshake_request{}, inet:ip_address()) ->
    {od_user:id(), session:id()} | no_return().
handle_client_handshake(#client_handshake_request{
    session_id = SessId,
    auth = Auth
} = Req, IpAddress) when is_binary(SessId) andalso is_record(Auth, macaroon_auth) ->

    assert_client_compatibility(Req, IpAddress),

    case user_identity:get_or_fetch(Auth) of
        ?ERROR_FORBIDDEN -> throw(invalid_provider);
        ?ERROR_UNAUTHORIZED -> throw(bad_macaroon);
        ?ERROR_BAD_MACAROON -> throw(bad_macaroon);
        ?ERROR_MACAROON_INVALID -> throw(bad_macaroon);
        ?ERROR_MACAROON_EXPIRED -> throw(bad_macaroon);
        ?ERROR_MACAROON_TTL_TO_LONG(_) -> throw(bad_macaroon);
        {ok, #document{value = Iden = #user_identity{user_id = UserId}}} ->
            {ok, _} = session_manager:reuse_or_create_fuse_session(SessId, Iden, Auth, self()),
            {UserId, SessId}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Handles provider handshake request
%% @end
%%--------------------------------------------------------------------
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
    {ok, _} = session_manager:reuse_or_create_provider_session(
        SessionId, provider_incoming, Identity, self()
    ),
    {ProviderId, SessionId}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if client is of compatible version.
%% @end
%%--------------------------------------------------------------------
-spec assert_client_compatibility(#client_handshake_request{}, inet:ip_address()) ->
    ok | no_return().
assert_client_compatibility(HandshakeRequest, IpAddress) ->
    #client_handshake_request{
        version = OcVersionBin,
        compatible_oneprovider_versions = CompatibleOpVersions
    } = HandshakeRequest,
    OcVersion = binary_to_list(OcVersionBin),
    {ok, CompatibleOcVersions} = application:get_env(?APP_NAME, compatible_oc_versions),
    OpVersion = oneprovider:get_version(),
    % Client sends full build version (e.g. 17.06.0-rc9-aiosufshx) so instead
    % of matching whole build version we check only the prefix
    IsOcVersionPrefix = fun(Ver) -> lists:prefix(Ver, OcVersion) end,
    case lists:any(IsOcVersionPrefix, CompatibleOcVersions) orelse
        lists:member(OpVersion, CompatibleOpVersions) of
        true ->
            ok;
        false ->
            ?debug("Discarding connection from oneclient @ ~s because of "
            "incompatible version.~n"
            "Oneclient version: ~s, supports providers: ~p~n"
            "Oneprovider version: ~s, supports clients: ~p~n", [
                inet_parse:ntoa(IpAddress),
                OcVersion, CompatibleOpVersions,
                OpVersion, CompatibleOcVersions
            ]),
            throw(incompatible_client_version)
    end.