%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Provider's authentication helper functions.
%%% @end
%%%-------------------------------------------------------------------
-module(provider_auth_manager).
-author("Rafal Slota").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([is_provider/1, handshake/2, get_provider_id/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Checks whether given certificate belongs to provider or not.
%% @end
%% @todo: improve the implementation by adding internal CA validation
%%--------------------------------------------------------------------
-spec is_provider(Cert :: #'OTPCertificate'{}) -> boolean().
is_provider(#'OTPCertificate'{} = Cert) ->
    try
        #'OTPCertificate'{tbsCertificate =
        #'OTPTBSCertificate'{subject = {rdnSequence, Attrs}}} = Cert,

        OU = lists:filtermap(
            fun([Attribute]) ->
                case Attribute#'AttributeTypeAndValue'.type of
                    ?'id-at-organizationalUnitName' ->
                        {_, Id} = Attribute#'AttributeTypeAndValue'.value,
                        {true, str_utils:to_binary(Id)};
                    _ -> false
                end
            end, Attrs),
        [<<"Providers">>] =:= OU
    catch
        _:_ -> false
    end;
is_provider(_) ->
    false.


%%--------------------------------------------------------------------
%% @doc
%% Initializes provider's session based on its certificate.
%% @end
%%--------------------------------------------------------------------
-spec handshake(Cert :: #'OTPCertificate'{}, Conn :: pid()) ->
    session:id() | no_return().
handshake(Cert, Conn) ->
    ProviderId = provider_auth_manager:get_provider_id(Cert),
    Identity = #user_identity{provider_id = ProviderId},
    SessionId = session_manager:get_provider_session_id(incoming, ProviderId),
    {ok, _} = session_manager:reuse_or_create_provider_session(SessionId, provider_incoming, Identity, Conn),
    SessionId.


%%--------------------------------------------------------------------
%% @doc Returns ProviderId based on provider's certificate (issued by OZ).
%% @end
%%--------------------------------------------------------------------
-spec get_provider_id(Cert :: #'OTPCertificate'{}) -> ProviderId :: oneprovider:id() | no_return().
get_provider_id(#'OTPCertificate'{} = Cert) ->
    #'OTPCertificate'{tbsCertificate =
    #'OTPTBSCertificate'{subject = {rdnSequence, Attrs}}} = Cert,

    [ProviderId] = lists:filtermap(
        fun([Attribute]) ->
            case Attribute#'AttributeTypeAndValue'.type of
                ?'id-at-commonName' ->
                    {_, Id} = Attribute#'AttributeTypeAndValue'.value,
                    {true, str_utils:to_binary(Id)};
                _ -> false
            end
        end, Attrs),

    str_utils:to_binary(ProviderId).

%%%===================================================================
%%% Internal functions
%%%===================================================================