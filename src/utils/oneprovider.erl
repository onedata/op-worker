%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Library module for oneprovider-wide operations.
%%% @end
%%%-------------------------------------------------------------------
-module(oneprovider).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_provider_id/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns Provider ID for current oneprovider instance.
%% Fails with undefined exception if the oneprovider is not registered as a provider.
%% @end
%%--------------------------------------------------------------------
-spec get_provider_id() -> ProviderId :: binary() | no_return().
get_provider_id() ->
    % Cache the provider ID so that we don't decode the cert every time
    case application:get_env(?APP_NAME, provider_id) of
        {ok, ProviderId} ->
            ProviderId;
        _ ->
            {ok, Bin} = file:read_file(gr_plugin:get_cert_path()),
            [{_, PeerCertDer, _} | _] = public_key:pem_decode(Bin),
            PeerCert = public_key:pkix_decode_cert(PeerCertDer, otp),
            ProviderId = get_provider_id(PeerCert),
            application:set_env(?APP_NAME, provider_id, ProviderId),
            ProviderId
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns ProviderId based on provider's certificate (issued by globalregistry).
%% @end
%%--------------------------------------------------------------------
-spec get_provider_id(Cert :: #'OTPCertificate'{}) -> ProviderId :: binary() | no_return().
get_provider_id(#'OTPCertificate'{} = Cert) ->
    #'OTPCertificate'{tbsCertificate =
    #'OTPTBSCertificate'{subject = {rdnSequence, Attrs}}} = Cert,

    [ProviderId] = lists:filtermap(fun([Attribute]) ->
        case Attribute#'AttributeTypeAndValue'.type of
            ?'id-at-commonName' ->
                {_, Id} = Attribute#'AttributeTypeAndValue'.value,
                {true, utils:ensure_binary(Id)};
            _ -> false
        end
    end, Attrs),

    utils:ensure_binary(ProviderId).

