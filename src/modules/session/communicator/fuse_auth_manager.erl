%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Client authentication library.
%%% @end
%%%-------------------------------------------------------------------
-module(fuse_auth_manager).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/events/definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([handle_handshake/2, authenticate_using_token/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handle first client message, which describes authentication method
%% (cert/token)
%% @end
%%--------------------------------------------------------------------
-spec handle_handshake(#client_message{}, #'OTPCertificate'{}) ->
    {ok, SessId :: session:id()} | no_return().
handle_handshake(#client_message{message_body = #handshake_request{
    session_id = SessId, auth = Auth, version = Version}}, _)
    when is_binary(SessId) andalso (is_record(Auth, macaroon_auth) orelse is_record(Auth, token_auth)) ->
    assert_client_compatibility(Version),
    {ok, Iden} = authenticate_using_token(Auth),
    {ok, _} = session_manager:reuse_or_create_fuse_session(SessId, Iden, Auth, self()),
    {ok, SessId};

handle_handshake(#client_message{message_body = #handshake_request{
    session_id = SessId, version = Version}}, OtpCert) when is_binary(SessId) ->
    assert_client_compatibility(Version),
    {ok, Iden} = authenticate_using_certificate(OtpCert),
    {ok, _} = session_manager:reuse_or_create_fuse_session(SessId, Iden, self()),
    {ok, SessId}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Authenticate client using given token, returns client identity.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_token(#macaroon_auth{}) -> {ok, #user_identity{}} | {error, term()}.
authenticate_using_token(Auth) ->
    {ok, #document{value = Iden}} = user_identity:get_or_fetch(Auth),
    {ok, Iden}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Authenticate client using given certificate. Returns client identity.
%% @end
%%--------------------------------------------------------------------
-spec authenticate_using_certificate(#'OTPCertificate'{}) -> {ok, #user_identity{}}.
authenticate_using_certificate(_OtpCert) ->
    %identity:get_or_fetch(_CertInfo). todo integrate with identity model
    {ok, #user_identity{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Check if client is of compatible version.
%% @end
%%--------------------------------------------------------------------
-spec assert_client_compatibility(string()) -> ok | no_return().
assert_client_compatibility(ClientVersion) ->
    {ok, SupportedClientVersions} = application:get_env(
        ?APP_NAME, supported_oc_versions
    ),
    Pred = fun(Ver) -> string:find(ClientVersion, Ver) =:= ClientVersion end,
    case lists:any(Pred, SupportedClientVersions) of
        true ->
            ok;
        false ->
            ?error("Refusing client connection because of incompatible "
                   "versions ~p. Supported ones: ~p.",
                [ClientVersion, SupportedClientVersions]
            ),
            throw(incompatible_client_version)
    end.
