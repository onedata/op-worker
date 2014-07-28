%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Jul 2014 17:47
%%%-------------------------------------------------------------------
-module(auth_handler).
-author("Rafal Slota").

-include_lib("public_key/include/public_key.hrl").
-include("veil_modules/dao/dao.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_access_token/1]).
-export([is_provider/1, get_provider_id/1]).

-spec get_access_token(UserGlobalId) ->
    {UserGlobalId, AccessToken} | {undefined, undefined}
    when UserGlobalId, AccessToken :: binary() | undefined.
get_access_token(undefined) ->
    {undefined, undefined};
get_access_token(GlobalId) ->
    case user_logic:get_user({global_id, GlobalId}) of
        {ok, #veil_document{record = #user{access_token = AccessToken}}} ->
            {GlobalId, AccessToken};
        {error, Reason} ->
            ?error("Cannot find user ~p due to: ~p", [GlobalId, Reason]),
            {undefined, undefined}
    end.

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

    ProviderId.


%% HACK PARTY !!
is_provider(#'OTPCertificate'{} = Cert) ->
    #'OTPCertificate'{tbsCertificate =
    #'OTPTBSCertificate'{subject = {rdnSequence, Attrs}}} = Cert,

    [OU] = lists:filtermap(fun([Attribute]) ->
        case Attribute#'AttributeTypeAndValue'.type of
            ?'id-at-organizationalUnitName' ->
                {_, Id} = Attribute#'AttributeTypeAndValue'.value,
                {true, vcn_utils:ensure_binary(Id)};
            _ -> false
        end
    end, Attrs),
    <<"Providers">> =:= OU.