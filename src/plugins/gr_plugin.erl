%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gr_plugin_behaviour in order
%%% to customize connection settings to Global Registry.
%%% @end
%%%-------------------------------------------------------------------
-module(gr_plugin).
-author("Krzysztof Trzepla").

-behaviour(gr_plugin_behaviour).

-include("global_definitions.hrl").

%% gr_plugin_behaviour API
-export([get_gr_url/0, get_key_path/0, get_cert_path/0, get_cacert_path/0]).

%%%===================================================================
%%% gr_plugin_behaviour API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Should return a Global Registry URL.
%% @end
%%--------------------------------------------------------------------
-spec get_gr_url() -> string().
get_gr_url() ->
    {ok, Node} = application:get_env(?APP_NAME, global_registry_node),
    {ok, Port} = application:get_env(?APP_NAME, global_registry_rest_port),
    string:join(["https://", utils:get_host(Node), ":", integer_to_list(Port)], "").

%%--------------------------------------------------------------------
%% @doc
%% Should return a path to file containing provider's private key.
%% @end
%%--------------------------------------------------------------------
-spec get_key_path() -> string().
get_key_path() ->
    {ok, KeyFile} = application:get_env(?APP_NAME, global_registry_provider_key_path),
    KeyFile.

%%--------------------------------------------------------------------
%% @doc
%% Should return a path to file containing provider's
%% public certificate signed by Global Registry.
%% @end
%%--------------------------------------------------------------------
-spec get_cert_path() -> string().
get_cert_path() ->
    {ok, CertFile} = application:get_env(?APP_NAME, global_registry_provider_cert_path),
    CertFile.

%%--------------------------------------------------------------------
%% @doc
%% Should return a path to file containing Global Registry
%% CA certificate.
%% @end
%%--------------------------------------------------------------------
-spec get_cacert_path() -> string().
get_cacert_path() ->
    {ok, CACertFile} = application:get_env(?APP_NAME, global_registry_ca_cert_path),
    CACertFile.

%%%===================================================================
%%% Internal functions
%%%===================================================================
