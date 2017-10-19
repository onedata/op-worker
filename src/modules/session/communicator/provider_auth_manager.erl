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

-include("modules/datastore/datastore_models.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([is_provider/1, handshake/2, get_provider_id/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks whether given certificate belongs to provider or not.
%% @end
%%--------------------------------------------------------------------
-spec is_provider(public_key:der_encoded()) -> boolean().
is_provider(DerCert) ->
    try
        OTPCert = public_key:pkix_decode_cert(DerCert, otp),
        #'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{
            subject = {rdnSequence, Attrs}
        }} = OTPCert,

        OU = lists:filtermap(
            fun([Attribute]) ->
                case Attribute#'AttributeTypeAndValue'.type of
                    ?'id-at-organizationalUnitName' ->
                        {_, Id} = Attribute#'AttributeTypeAndValue'.value,
                        {true, str_utils:to_binary(Id)};
                    _ -> false
                end
            end, Attrs),
        [<<"Providers">>] =:= OU andalso verify_provider_cert(DerCert)
    catch
        _:_ -> false
    end.


%%--------------------------------------------------------------------
%% @doc
%% Initializes provider's session based on its certificate.
%% @end
%%--------------------------------------------------------------------
-spec handshake(public_key:der_encoded(), Conn :: pid()) ->
    session:id() | no_return().
handshake(DerCert, Conn) ->
    ProviderId = provider_auth_manager:get_provider_id(DerCert),
    Identity = #user_identity{provider_id = ProviderId},
    SessionId = session_manager:get_provider_session_id(incoming, ProviderId),
    {ok, _} = session_manager:reuse_or_create_provider_session(SessionId, provider_incoming, Identity, Conn),
    SessionId.


%%--------------------------------------------------------------------
%% @doc Returns ProviderId based on provider's certificate (issued by OZ).
%% @end
%%--------------------------------------------------------------------
-spec get_provider_id(public_key:der_encoded()) -> oneprovider:id() | no_return().
get_provider_id(DerCert) ->
    OTPCert = public_key:pkix_decode_cert(DerCert, otp),
    #'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{
        subject = {rdnSequence, Attrs}
    }} = OTPCert,

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

%%--------------------------------------------------------------------
%% @doc
%% Checks if given provider cert was issued by OZ CA.
%% @end
%%--------------------------------------------------------------------
-spec verify_provider_cert(public_key:der_encoded()) -> boolean().
verify_provider_cert(DerCert) ->
    OzCaCertDer = cert_utils:load_der(oz_plugin:get_oz_cacert_path()),
    OzCaCert = #'OTPCertificate'{} = public_key:pkix_decode_cert(OzCaCertDer, otp),
    case public_key:pkix_path_validation(OzCaCert, [DerCert], [{max_path_length, 0}]) of
        {ok, _} -> true;
        _ -> false
    end.