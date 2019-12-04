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
    session_id = SessionId,
    auth = #token_auth{token = SerializedToken} = TokenAuth0
} = Req, IpAddress) when is_binary(SessionId) ->

    assert_client_compatibility(Req, IpAddress),

    TokenAuth1 = TokenAuth0#token_auth{
        peer_ip = IpAddress,
        interface = oneclient,
        data_access_caveats_policy = allow_data_access_caveats
    },
    case auth_manager:verify(TokenAuth1) of
        {ok, ?USER(UserId), _TTL} ->
            {ok, SessionId} = session_manager:reuse_or_create_fuse_session(
                SessionId, #user_identity{user_id = UserId}, TokenAuth1
            ),
            {UserId, SessionId};
        ?ERROR_FORBIDDEN ->
            throw(invalid_provider);
        {error, _} = Error ->
            case tokens:deserialize(SerializedToken) of
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
        {ok, ?SUB(?ONEPROVIDER, ProviderId)} ->
            Identity = #user_identity{provider_id = ProviderId},
            SessId = session_utils:get_provider_session_id(incoming, ProviderId),
            {ok, _} = session_manager:reuse_or_create_incoming_provider_session(
                SessId, Identity
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
