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
-export([get_oz_url/0, get_oz_rest_port/0, get_oz_rest_api_prefix/0, get_oz_rest_endpoint/1]).
-export([get_key_file/0, get_csr_file/0, get_cert_file/0]).
-export([get_cacerts_dir/0, get_oz_cacert_path/0]).
-export([auth_to_rest_client/1]).

%%%===================================================================
%%% oz_plugin_behaviour API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Should return a OneZone URL.
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
%% @doc Should return OZ REST endpoint, ended with given Path.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_rest_endpoint(string() | binary()) -> binary().
get_oz_rest_endpoint(Path) ->
    str_utils:format_bin("~s:~B~s~s", [
        get_oz_url(),
        get_oz_rest_port(),
        get_oz_rest_api_prefix(),
        Path
    ]).

%%--------------------------------------------------------------------
%% @doc
%% Should return a path to file containing provider's private key.
%% @end
%%--------------------------------------------------------------------
-spec get_key_file() -> file:name_all().
get_key_file() ->
    {ok, KeyFile} = application:get_env(?APP_NAME, oz_provider_key_file),
    KeyFile.

%%--------------------------------------------------------------------
%% @doc
%% Should return a path to file containing provider's private key.
%% @end
%%--------------------------------------------------------------------
-spec get_csr_file() -> file:name_all().
get_csr_file() ->
    {ok, CSRFile} = application:get_env(?APP_NAME, oz_provider_csr_file),
    CSRFile.

%%--------------------------------------------------------------------
%% @doc
%% Should return a path to file containing provider's
%% public certificate signed by OneZone.
%% @end
%%--------------------------------------------------------------------
-spec get_cert_file() -> file:name_all().
get_cert_file() ->
    {ok, CertFile} = application:get_env(?APP_NAME, oz_provider_cert_file),
    CertFile.

%%--------------------------------------------------------------------
%% @doc
%% Should return the path to CA certs directory.
%% @end
%%--------------------------------------------------------------------
-spec get_cacerts_dir() -> file:name_all().
get_cacerts_dir() ->
    {ok, CACertsDir} = application:get_env(?APP_NAME, cacerts_dir),
    CACertsDir.

%%--------------------------------------------------------------------
%% @doc
%% Should return the path to file containing OneZone CA certificate.
%% @end
%%--------------------------------------------------------------------
-spec get_oz_cacert_path() -> file:name_all().
get_oz_cacert_path() ->
    {ok, CaFile} = application:get_env(?APP_NAME, oz_ca_file),
    filename:join(get_cacerts_dir(), CaFile).

%%--------------------------------------------------------------------
%% @doc
%% This callback is used to convert Auth term, which is transparent to ctool,
%% into one of possible authorization methods. Thanks to this, the code
%% using OZ API can always use its specific Auth terms and they are converted
%% when request is done.
%% @end
%%--------------------------------------------------------------------
-spec auth_to_rest_client(Auth :: term()) -> {user, token, binary()} |
{user, macaroon, {Macaroon :: binary(), DischargeMacaroons :: [binary()]}} |
{user, basic, binary()} | provider.
auth_to_rest_client(#macaroon_auth{macaroon = Mac, disch_macaroons = DMacs}) ->
    {user, macaroon, {Mac, DMacs}};

auth_to_rest_client(#token_auth{token = Token}) ->
    {user, token, Token};

auth_to_rest_client(#basic_auth{credentials = Credentials}) ->
    {user, basic, Credentials};

auth_to_rest_client(?ROOT_SESS_ID) ->
    provider;

auth_to_rest_client(?GUEST_SESS_ID) ->
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
