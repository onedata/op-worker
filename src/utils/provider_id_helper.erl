%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for provision of provider ID. This module duplicates
%%% some functionality of oneprovider.erl because to many calls to single
%%% function in one module resulted in performance problems.
%%% TODO: VFS-3806
%%% @end
%%%-------------------------------------------------------------------
-module(provider_id_helper).
-author("Michał Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("ctool/include/logging.hrl").

%% ID of provider that is not currently registered in Global Registry
-define(NON_GLOBAL_PROVIDER_ID, <<"non_global_provider">>).

-export([get_provider_id/0]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% @equiv oneprovider:get_provider_id/0
%% @end
%%--------------------------------------------------------------------
-spec get_provider_id() -> ProviderId :: binary() | no_return().
get_provider_id() ->
    case application:get_env(?APP_NAME, provider_id) of
        {ok, ProviderId} ->
            ProviderId;
        _ ->
            try file:read_file(oz_plugin:get_cert_file()) of
                {ok, Bin} ->
                    [{_, PeerCertDer, _} | _] = public_key:pem_decode(Bin),
                    PeerCert = public_key:pkix_decode_cert(PeerCertDer, otp),
                    ProviderId = get_provider_id(PeerCert),
                    catch application:set_env(?APP_NAME, provider_id, ProviderId),
                    ProviderId;
                % TODO - better caching when cert is not found
%%                {error,enoent} ->
%%                    application:set_env(?APP_NAME, provider_id, ?NON_GLOBAL_PROVIDER_ID),
%%                    ?NON_GLOBAL_PROVIDER_ID;
                {error, _} ->
                    ?NON_GLOBAL_PROVIDER_ID
            catch
                _:Reason ->
                    ?error_stacktrace("Unable to read certificate file due to ~p", [Reason]),
                    ?NON_GLOBAL_PROVIDER_ID
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv oneprovider:get_provider_id/1
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
                {true, str_utils:to_binary(Id)};
            _ -> false
        end
    end, Attrs),

    str_utils:to_binary(ProviderId).