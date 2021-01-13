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

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

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
    client_tokens = #client_tokens{
        access_token = AccessToken,
        consumer_token = ConsumerToken
    }
} = Req, IpAddress) when is_binary(Nonce) ->

    assert_client_compatibility(Req, IpAddress),

    TokenCredentials = auth_manager:build_token_credentials(
        AccessToken, ConsumerToken,
        IpAddress, oneclient, allow_data_access_caveats
    ),
    case auth_manager:verify_credentials(TokenCredentials) of
        {ok, #auth{subject = ?SUB(user, UserId) = Subject}, _} ->
            {ok, SessionId} = session_manager:reuse_or_create_fuse_session(
                Nonce, Subject, TokenCredentials
            ),
            {UserId, SessionId};
        ?ERROR_FORBIDDEN ->
            throw(invalid_provider);
        {error, _} = Error ->
            case tokens:deserialize(AccessToken) of
                {ok, #token{subject = Subject, id = TokenId} = Token} ->
                    ?debug("Cannot authorize user (id: ~p) based on token (id: ~p) "
                           "with caveats: ~p due to ~w", [
                        Subject,
                        TokenId,
                        tokens:get_caveats(Token),
                        Error
                    ]);
                _ ->
                    ?debug("Cannot authorize user based on token due to ~w", [
                        Error
                    ])
            end,
            throw(invalid_token)
    end.


%% @private
-spec handle_provider_handshake(#provider_handshake_request{}, inet:ip_address()) ->
    {od_provider:id(), session:id()} | no_return().
handle_provider_handshake(#provider_handshake_request{
    provider_id = ProviderId,
    token = Token
}, IpAddress) when is_binary(ProviderId) andalso is_binary(Token) ->

    case token_logic:verify_provider_identity_token(Token) of
        {ok, ?SUB(?ONEPROVIDER, ProviderId) = Subject} ->
            {ok, SessId} = session_manager:reuse_or_create_incoming_provider_session(
                Subject
            ),
            {ProviderId, SessId};
        {ok, _} ->
            throw(invalid_provider);
        {error, _} = Error ->
            ?debug("Discarding provider connection from ~ts @ ~s as its "
                   "identity cannot be verified: ~p", [
                provider_logic:to_printable(ProviderId),
                inet_parse:ntoa(IpAddress), Error
            ]),
            throw(invalid_token)
    end.


%% @private
-spec assert_client_compatibility(#client_handshake_request{}, inet:ip_address()) ->
    ok | no_return().
assert_client_compatibility(#client_handshake_request{
    version = OcVersion
}, IpAddress) ->
    OpVersion = op_worker:get_release_version(),

    case compatibility:check_products_compatibility(
        ?ONEPROVIDER, OpVersion, ?ONECLIENT, OcVersion
    ) of
        true ->
            ok;
        {false, CompatibleOcVersions} ->
            ?debug("Discarding connection from oneclient @ ~s because of "
            "incompatible version.~n"
            "Oneclient version: ~s~n"
            "Oneprovider version: ~s, supports clients: ~s", [
                inet_parse:ntoa(IpAddress),
                OcVersion,
                OpVersion, str_utils:join_binary(CompatibleOcVersions, <<", ">>)
            ]),
            throw(incompatible_client_version)
    end.
