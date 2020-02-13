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
-export([root_auth/0, guest_auth/0]).
-export([
    build_token_auth/5,

    get_access_token/1,
    get_audience_token/1,
    get_peer_ip/1,
    get_interface/1,
    get_data_access_caveats_policy/1,
    get_credentials/1, update_credentials/3
]).
-export([
    auth_to_gs_auth_override/1,
    get_caveats/1,
    verify_auth/1
]).

-type credentials() :: #credentials{}.
-type access_token() :: tokens:serialized().
-type audience_token() :: undefined | tokens:serialized().

% Record containing access token for user authorization in OZ.
-record(token_auth, {
    access_token :: access_token(),
    audience_token = undefined :: audience_token(),
    peer_ip = undefined :: undefined | ip_utils:ip(),
    interface = undefined :: undefined | cv_interface:interface(),
    data_access_caveats_policy = disallow_data_access_caveats :: data_access_caveats:policy()
}).

-opaque token_auth() :: #token_auth{}.
-type guest_auth() :: ?GUEST_AUTH.
-type root_auth() :: ?ROOT_AUTH.
-type auth() :: token_auth() | guest_auth() | root_auth().

-type verification_result() ::
    {ok, aai:auth(), TokenValidUntil :: undefined | time_utils:seconds()} |
    errors:error().

-export_type([
    credentials/0, access_token/0, audience_token/0,
    token_auth/0, guest_auth/0, root_auth/0, auth/0,
    verification_result/0
]).

-define(NOW(), time_utils:system_time_seconds()).


%%%===================================================================
%%% API
%%%===================================================================


-spec root_auth() -> root_auth().
root_auth() ->
    ?ROOT_AUTH.


-spec guest_auth() -> guest_auth().
guest_auth() ->
    ?GUEST_AUTH.


-spec build_token_auth(
    access_token(), audience_token(),
    PeerIp :: undefined | ip_utils:ip(),
    Interface :: undefined | cv_interface:interface(),
    data_access_caveats:policy()
) ->
    token_auth().
build_token_auth(AccessToken, AudienceToken, PeerIp, Interface, DataAccessCaveatsPolicy) ->
    #token_auth{
        access_token = AccessToken,
        audience_token = AudienceToken,
        peer_ip = PeerIp,
        interface = Interface,
        data_access_caveats_policy = DataAccessCaveatsPolicy
    }.


-spec get_access_token(token_auth()) -> access_token().
get_access_token(#token_auth{access_token = AccessToken}) ->
    AccessToken.


-spec get_audience_token(token_auth()) -> audience_token().
get_audience_token(#token_auth{audience_token = AudienceToken}) ->
    AudienceToken.


-spec get_peer_ip(token_auth()) -> undefined | ip_utils:ip().
get_peer_ip(#token_auth{peer_ip = PeerIp}) ->
    PeerIp.


-spec get_interface(token_auth()) -> undefined | cv_interface:interface().
get_interface(#token_auth{interface = Interface}) ->
    Interface.


-spec get_data_access_caveats_policy(token_auth()) ->
    data_access_caveats:policy().
get_data_access_caveats_policy(#token_auth{data_access_caveats_policy = Policy}) ->
    Policy.


-spec get_credentials(token_auth()) -> credentials().
get_credentials(#token_auth{
    access_token = AccessToken,
    audience_token = AudienceToken
}) ->
    #credentials{
        access_token = AccessToken,
        audience_token = AudienceToken
    }.


-spec update_credentials(token_auth(), access_token(), audience_token()) ->
    token_auth().
update_credentials(TokenAuth, AccessToken, AudienceToken) ->
    TokenAuth#token_auth{
        access_token = AccessToken,
        audience_token = AudienceToken
    }.


-spec auth_to_gs_auth_override(auth()) -> gs_protocol:auth_override().
auth_to_gs_auth_override(?ROOT_AUTH) ->
    undefined;
auth_to_gs_auth_override(?GUEST_AUTH) ->
    #auth_override{client_auth = nobody};
auth_to_gs_auth_override(#token_auth{
    access_token = AccessToken,
    peer_ip = PeerIp,
    interface = Interface,
    audience_token = AudienceToken,
    data_access_caveats_policy = DataAccessCaveatsPolicy
}) ->
    #auth_override{
        client_auth = {token, AccessToken},
        peer_ip = PeerIp,
        interface = Interface,
        audience_token = AudienceToken,
        data_access_caveats_policy = DataAccessCaveatsPolicy
    }.


-spec get_caveats(auth()) -> {ok, [caveats:caveat()]} | errors:error().
get_caveats(?ROOT_AUTH) ->
    {ok, []};
get_caveats(?GUEST_AUTH) ->
    {ok, []};
get_caveats(TokenAuth) ->
    case verify_auth(TokenAuth) of
        {ok, #auth{caveats = Caveats}, _} ->
            {ok, Caveats};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Verifies identity of subject identified by specified auth() and returns
%% time this auth will be valid until. Nevertheless this check should be
%% performed periodically for token_auth() as tokens can be revoked.
%% @end
%%--------------------------------------------------------------------
-spec verify_auth(auth()) -> verification_result().
verify_auth(?ROOT_AUTH) ->
    {ok, #auth{subject = ?SUB(root, ?ROOT_USER_ID)}, undefined};
verify_auth(?GUEST_AUTH) ->
    {ok, #auth{subject = ?SUB(nobody, ?GUEST_USER_ID)}, undefined};
verify_auth(TokenAuth) ->
    verify_token_auth(TokenAuth).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec verify_token_auth(token_auth()) -> verification_result().
verify_token_auth(TokenAuth) ->
    case auth_cache:get_token_auth_verification_result(TokenAuth) of
        {ok, CachedVerificationResult} ->
            CachedVerificationResult;
        ?ERROR_NOT_FOUND ->
            try
                {TokenRef, VerificationResult} = verify_token_auth_insecure(TokenAuth),
                auth_cache:save_token_auth_verification_result(
                    TokenAuth, TokenRef, VerificationResult
                ),
                VerificationResult
            catch Type:Reason ->
                ?error_stacktrace("Cannot verify user auth due to ~p:~p", [
                    Type, Reason
                ]),
                ?ERROR_UNAUTHORIZED
            end
    end.


%% @private
-spec verify_token_auth_insecure(token_auth()) ->
    {undefined | auth_cache:token_ref(), verification_result()}.
verify_token_auth_insecure(#token_auth{
    access_token = AccessToken,
    peer_ip = PeerIp,
    interface = Interface,
    data_access_caveats_policy = DataAccessCaveatsPolicy
}) ->
    case deserialize_and_validate_token(AccessToken) of
        {ok, #token{subject = Subject} = Token} ->
            case token_logic:verify_access_token(AccessToken,
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
