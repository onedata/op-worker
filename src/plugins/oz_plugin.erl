%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements oz_plugin_behaviour in order
%%% to customize connection settings to Global Registry.
%%% @end
%%%-------------------------------------------------------------------
-module(oz_plugin).
-author("Krzysztof Trzepla").

-behaviour(oz_plugin_behaviour).

-include("global_definitions.hrl").

%% oz_plugin_behaviour API
-export([get_oz_url/0, get_oz_rest_port/0, get_oz_rest_api_prefix/0]).
-export([get_key_path/0, get_csr_path/0, get_cert_path/0, get_cacert_path/0]).

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

%%%===================================================================
%%% Internal functions
%%%===================================================================
