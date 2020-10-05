%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Provides utility functions to operate on auth() objects. The main one
%%% being subject identity verification in Onezone service.
%%%
%%% NOTE !!!
%%% Tokens can be revoked and deleted, which means that they may become
%%% invalid before their actual expiration.
%%% To assert that tokens are valid (and not revoked/deleted) verification
%%% checks should be performed periodically.
%%% @end
%%%-------------------------------------------------------------------
-module(auth_manager).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    build_token_credentials/5,

    get_access_token/1,
    get_consumer_token/1,
    get_peer_ip/1,
    get_interface/1,
    get_data_access_caveats_policy/1,
    get_client_tokens/1, update_client_tokens/3
]).
-export([
    credentials_to_gs_auth_override/1,
    get_caveats/1,
    verify_credentials/1
]).

-type access_token() :: tokens:serialized().
-type consumer_token() :: undefined | tokens:serialized().
-type client_tokens() :: #client_tokens{}.

% Record containing access token for user authorization in OZ.
-record(token_credentials, {
    access_token :: access_token(),
    consumer_token = undefined :: consumer_token(),
    peer_ip = undefined :: undefined | ip_utils:ip(),
    interface = undefined :: undefined | cv_interface:interface(),
    data_access_caveats_policy = disallow_data_access_caveats :: data_access_caveats:policy()
}).

-opaque token_credentials() :: #token_credentials{}.
-type credentials() :: ?ROOT_CREDENTIALS | ?GUEST_CREDENTIALS | token_credentials().

-type verification_result() ::
    {ok, aai:auth(), TokenValidUntil :: undefined | time_utils:seconds()} |
    errors:error().

-export_type([
    access_token/0, consumer_token/0, client_tokens/0,
    token_credentials/0, credentials/0,
    verification_result/0
]).

-define(NOW(), time_utils:timestamp_seconds()).


%%%===================================================================
%%% API
%%%===================================================================


-spec build_token_credentials(
    access_token(), consumer_token(),
    PeerIp :: undefined | ip_utils:ip(),
    Interface :: undefined | cv_interface:interface(),
    data_access_caveats:policy()
) ->
    token_credentials().
build_token_credentials(AccessToken, ConsumerToken, PeerIp, Interface, DataAccessCaveatsPolicy) ->
    #token_credentials{
        access_token = AccessToken,
        consumer_token = ConsumerToken,
        peer_ip = PeerIp,
        interface = Interface,
        data_access_caveats_policy = DataAccessCaveatsPolicy
    }.


-spec get_access_token(token_credentials()) -> access_token().
get_access_token(#token_credentials{access_token = AccessToken}) ->
    AccessToken.


-spec get_consumer_token(token_credentials()) -> consumer_token().
get_consumer_token(#token_credentials{consumer_token = ConsumerToken}) ->
    ConsumerToken.


-spec get_peer_ip(token_credentials()) -> undefined | ip_utils:ip().
get_peer_ip(#token_credentials{peer_ip = PeerIp}) ->
    PeerIp.


-spec get_interface(token_credentials()) -> undefined | cv_interface:interface().
get_interface(#token_credentials{interface = Interface}) ->
    Interface.


-spec get_data_access_caveats_policy(token_credentials()) ->
    data_access_caveats:policy().
get_data_access_caveats_policy(#token_credentials{data_access_caveats_policy = Policy}) ->
    Policy.


-spec get_client_tokens(token_credentials()) -> client_tokens().
get_client_tokens(#token_credentials{
    access_token = AccessToken,
    consumer_token = ConsumerToken
}) ->
    #client_tokens{
        access_token = AccessToken,
        consumer_token = ConsumerToken
    }.


-spec update_client_tokens(token_credentials(), access_token(), consumer_token()) ->
    token_credentials().
update_client_tokens(TokenCredentials, AccessToken, ConsumerToken) ->
    TokenCredentials#token_credentials{
        access_token = AccessToken,
        consumer_token = ConsumerToken
    }.


-spec credentials_to_gs_auth_override(credentials()) -> gs_protocol:auth_override().
credentials_to_gs_auth_override(?ROOT_CREDENTIALS) ->
    undefined;
credentials_to_gs_auth_override(?GUEST_CREDENTIALS) ->
    #auth_override{client_auth = nobody};
credentials_to_gs_auth_override(#token_credentials{
    access_token = AccessToken,
    peer_ip = PeerIp,
    interface = Interface,
    consumer_token = ConsumerToken,
    data_access_caveats_policy = DataAccessCaveatsPolicy
}) ->
    #auth_override{
        client_auth = {token, AccessToken},
        peer_ip = PeerIp,
        interface = Interface,
        consumer_token = ConsumerToken,
        data_access_caveats_policy = DataAccessCaveatsPolicy
    }.


-spec get_caveats(credentials()) -> {ok, [caveats:caveat()]} | errors:error().
get_caveats(?ROOT_CREDENTIALS) ->
    {ok, []};
get_caveats(?GUEST_CREDENTIALS) ->
    {ok, []};
get_caveats(TokenCredentials) ->
    case verify_credentials(TokenCredentials) of
        {ok, #auth{caveats = Caveats}, _} ->
            {ok, Caveats};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Verifies identity of subject identified by specified credentials() and
%% returns time this auth will be valid until. Nevertheless this check
%% should be performed periodically for token_credentials() as tokens
%% can be revoked/deleted.
%% @end
%%--------------------------------------------------------------------
-spec verify_credentials(credentials()) -> verification_result().
verify_credentials(?ROOT_CREDENTIALS) ->
    {ok, #auth{subject = ?ROOT_IDENTITY}, undefined};

verify_credentials(?GUEST_CREDENTIALS) ->
    {ok, #auth{subject = ?GUEST_IDENTITY}, undefined};

verify_credentials(TokenCredentials) ->
    case auth_cache:get_token_credentials_verification_result(TokenCredentials) of
        {ok, CachedVerificationResult} ->
            CachedVerificationResult;
        ?ERROR_NOT_FOUND ->
            try
                {TokenRef, VerificationResult} = verify_token_credentials(TokenCredentials),
                auth_cache:save_token_credentials_verification_result(
                    TokenCredentials, TokenRef, VerificationResult
                ),
                VerificationResult
            catch Type:Reason ->
                ?error_stacktrace("Cannot verify user credentials due to ~p:~p", [
                    Type, Reason
                ]),
                ?ERROR_INTERNAL_SERVER_ERROR
            end
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec verify_token_credentials(token_credentials()) ->
    {undefined | auth_cache:token_ref(), verification_result()}.
verify_token_credentials(#token_credentials{
    access_token = AccessToken,
    consumer_token = ConsumerToken,
    peer_ip = PeerIp,
    interface = Interface,
    data_access_caveats_policy = DataAccessCaveatsPolicy
}) ->
    case deserialize_and_validate_token(AccessToken) of
        {ok, #token{subject = Subject} = Token} ->
            case token_logic:verify_access_token(
                AccessToken, ConsumerToken,
                PeerIp, Interface, DataAccessCaveatsPolicy
            ) of
                {ok, Subject, TokenTTL} ->
                    AaiAuth = #auth{
                        subject = Subject,
                        caveats = tokens:get_caveats(Token)
                    },
                    TokenExpiration = case TokenTTL of
                        undefined -> undefined;
                        _ -> ?NOW() + TokenTTL
                    end,
                    TokenRef = auth_cache:get_token_ref(Token),
                    {TokenRef, {ok, AaiAuth, TokenExpiration}};
                {error, _} = VerificationError ->
                    {undefined, VerificationError}
            end;
        {error, _} = Error ->
            {undefined, Error}
    end.


%% @private
-spec deserialize_and_validate_token(tokens:serialized()) ->
    {ok, tokens:token()} | errors:error().
deserialize_and_validate_token(SerializedToken) ->
    case tokens:deserialize(SerializedToken) of
        {ok, #token{subject = ?SUB(user, UserId)} = Token} ->
            case provider_logic:has_eff_user(UserId) of
                true ->
                    {ok, Token};
                false ->
                    ?ERROR_USER_NOT_SUPPORTED
            end;
        {ok, _} ->
            ?ERROR_TOKEN_SUBJECT_INVALID;
        {error, _} = DeserializationError ->
            DeserializationError
    end.
