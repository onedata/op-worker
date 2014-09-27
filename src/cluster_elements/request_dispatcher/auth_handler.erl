%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides helper functions for client's
%%       authorization using GlobalRegistry
%% @end
%% ===================================================================
-module(auth_handler).
-author("Rafal Slota").

-include("registered_names.hrl").
-include("veil_modules/dao/dao.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("ctool/include/logging.hrl").

%% @TODO: Expiration could be given by Global Registry
-define(AUTH_CACHE_EXPIRATION_MINUTES, 10).

%% API
-export([get_access_token/1]).
-export([is_provider/1, get_provider_id/1]).
-export([authenticate_user_by_secret/2]).

%% get_access_token/1
%% ====================================================================
%% @doc Returns user's AccessToken that is currently used by local Provider.
%% @end
-spec get_access_token(UserGlobalId :: binary()) -> {UserGlobalId :: binary(), AccessToken :: binary() | undefined} | {undefined, undefined}.
%% ====================================================================
get_access_token(undefined) ->
    {undefined, undefined};
get_access_token(GlobalId) ->
    case user_logic:get_user({global_id, GlobalId}) of
        {ok, #veil_document{record = #user{access_token = AccessToken}}} ->
            {GlobalId, AccessToken};
        {error, Reason} ->
            ?error("Cannot find user ~p due to: ~p", [GlobalId, Reason]),
            {GlobalId, undefined}
    end.

%% get_provider_id/1
%% ====================================================================
%% @doc Returns ProviderId based on provider's certificate (issued by GlobalRegistry).
%% @end
-spec get_provider_id(Cert :: #'OTPCertificate'{}) -> ProviderId :: binary() | no_return().
%% ====================================================================
get_provider_id(#'OTPCertificate'{} = Cert) ->
    #'OTPCertificate'{tbsCertificate =
    #'OTPTBSCertificate'{subject = {rdnSequence, Attrs}}} = Cert,

    [ProviderId] = lists:filtermap(fun([Attribute]) ->
        case Attribute#'AttributeTypeAndValue'.type of
            ?'id-at-commonName' ->
                {_, Id} = Attribute#'AttributeTypeAndValue'.value,
                {true, vcn_utils:ensure_binary(Id)};
            _ -> false
        end
    end, Attrs),

    vcn_utils:ensure_binary(ProviderId).


%% is_provider/1
%% ====================================================================
%% @doc Checks whether given certificate belongs to provider or not.
%% @end
%% @todo: improve the implementation by adding internal CA validation
-spec is_provider(Cert :: #'OTPCertificate'{}) -> boolean().
%% ====================================================================
is_provider(#'OTPCertificate'{} = Cert) ->
    try
        #'OTPCertificate'{tbsCertificate =
        #'OTPTBSCertificate'{subject = {rdnSequence, Attrs}}} = Cert,

        OU = lists:filtermap(fun([Attribute]) ->
            case Attribute#'AttributeTypeAndValue'.type of
                ?'id-at-organizationalUnitName' ->
                    {_, Id} = Attribute#'AttributeTypeAndValue'.value,
                    {true, vcn_utils:ensure_binary(Id)};
                _ -> false
            end
        end, Attrs),
        [<<"Providers">>] =:= OU
    catch
        _:_ -> false
    end.


%% authenticate_user_by_secret/2
%% ====================================================================
%% @doc Attempts to verify user credentials with Global Registry.
-spec authenticate_user_by_secret(GRUID :: binary(), Secret :: binary()) ->
    {true, AccessToken :: binary()} | false.
%% ====================================================================
authenticate_user_by_secret(GRUID, Secret) ->
    Now = vcn_utils:time(),

    CachedData = case ets:lookup(?TOKEN_AUTHENTICATION_CACHE, Secret) of
        [{Secret, {Expiration, CachedGRUID, CachedAccessToken}}] when Expiration > Now ->
            {CachedGRUID, CachedAccessToken};
        _ ->
            undefined
    end,

    %% Asynchronously clear expired tokens
    spawn(fun() ->
        ets:select_delete(?TOKEN_AUTHENTICATION_CACHE,
            [{{'_', {'$1', '_', '_'}}, [{'<', '$1', Now}], [true]}])
    end),

    case CachedData of
        {GRUID, AccessToken} -> {true, AccessToken};
        undefined ->
            case gr_adapter:verify_client(GRUID, Secret) of
                false -> false;
                true ->
                    {GRUID, AccessToken} = auth_handler:get_access_token(GRUID),
                    ExpirationTime = vcn_utils:time() + timer:minutes(?AUTH_CACHE_EXPIRATION_MINUTES),
                    ets:insert(?TOKEN_AUTHENTICATION_CACHE, {Secret, {ExpirationTime, GRUID, AccessToken}}),
                    {true, AccessToken}
            end
    end.
