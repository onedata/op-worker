%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements oz_plugin_behaviour in order
%%% to customize connection settings to OneZone.
%%% @end
%%%-------------------------------------------------------------------
-module(oz_plugin).
-author("Krzysztof Trzepla").

-behaviour(oz_plugin_behaviour).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/handshake_messages.hrl").

%% oz_plugin_behaviour API
-export([get_oz_url/0, get_oz_rest_port/0, get_oz_rest_api_prefix/0]).
-export([get_key_path/0, get_csr_path/0, get_cert_path/0, get_cacert_path/0]).
-export([auth_to_rest_client/1]).

%%%===================================================================
%%% oz_plugin_behaviour API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Should return a Global Registry URL.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_url() -> string().
get_oz_url() ->
    {ok, Hname} = application:get_env(?APP_NAME, oz_domain),
    Hostname = str_utils:to_list(Hname),
    "https://" ++ Hostname.

%%--------------------------------------------------------------------
%% @doc
%% Should return OZ REST port.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_rest_port() -> integer().
get_oz_rest_port() ->
    {ok, Port} = application:get_env(?APP_NAME, oz_rest_port),
    Port.

%%--------------------------------------------------------------------
%% @doc
%% @doc Should return OZ REST API prefix - for example /api/v3/onezone.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_rest_api_prefix() -> string().
get_oz_rest_api_prefix() ->
    {ok, Prefix} = application:get_env(?APP_NAME, oz_rest_api_prefix),
    Prefix.

%%--------------------------------------------------------------------
%% @doc
%% Should return a path to file containing provider's private key.
%% @end
%%--------------------------------------------------------------------
-spec get_key_path() -> file:name_all().
get_key_path() ->
    {ok, KeyFile} = application:get_env(?APP_NAME, oz_provider_key_path),
    KeyFile.

%%--------------------------------------------------------------------
%% @doc
%% Should return a path to file containing provider's private key.
%% @end
%%--------------------------------------------------------------------
-spec get_csr_path() -> file:name_all().
get_csr_path() ->
    {ok, CSRFile} = application:get_env(?APP_NAME, oz_provider_csr_path),
    CSRFile.

%%--------------------------------------------------------------------
%% @doc
%% Should return a path to file containing provider's
%% public certificate signed by Global Registry.
%% @end
%%--------------------------------------------------------------------
-spec get_cert_path() -> file:name_all().
get_cert_path() ->
    {ok, CertFile} = application:get_env(?APP_NAME, oz_provider_cert_path),
    CertFile.

%%--------------------------------------------------------------------
%% @doc
%% Should return a path to file containing Global Registry
%% CA certificate.
%% @end
%%--------------------------------------------------------------------
-spec get_cacert_path() -> file:name_all().
get_cacert_path() ->
    {ok, CACertFile} = application:get_env(?APP_NAME, oz_ca_cert_path),
    CACertFile.

%%--------------------------------------------------------------------
%% @doc
%% This callback is used to convert Auth term, which is transparent to ctool,
%% into one of possible authorization methods. Thanks to this, the code
%% using OZ API can always use its specific Auth terms and they are converted
%% when request is done.
%% @end
%%--------------------------------------------------------------------
-spec auth_to_rest_client(Auth :: term()) -> file:name_all().
auth_to_rest_client(#token_auth{macaroon = Mac, disch_macaroons = DMacs}) ->
    {user, token, {Mac, DMacs}};

auth_to_rest_client(#basic_auth{credentials = Credentials}) ->
    {user, basic, Credentials};

auth_to_rest_client(?ROOT_SESS_ID) ->
    provider;

auth_to_rest_client(SessId) when is_binary(SessId) ->
    {ok, #document{
        value = #session{
            auth = Auth,
            type = Type
        }}} = session:get(SessId),
    case Type of
        provider_outgoing ->
            provider;
        provider_incoming ->
            provider;
        _ ->
            % This will evaluate either to user_token, user_basic or provider.
            auth_to_rest_client(Auth)
    end;

auth_to_rest_client(Other) ->
    % Other can be provider|client.
    Other.
