%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements gui_route_plugin_behaviour. It decides on:
%%%   - mapping of URLs to pages (routes)
%%%   - logic and requirements on different routes
%%%   - what pages are used for login, logout, displaying errors
%%%   - what modules handles server logic of WebSocket connection with
%%%         the client (data and callback backends)
%%% @end
%%%-------------------------------------------------------------------
-module(gui_route_plugin).
-author("Lukasz Opiola").
-behaviour(gui_route_plugin_behaviour).


-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").
-include_lib("gui/include/gui.hrl").

-export([route/1, data_backend/2, private_rpc_backend/0, public_rpc_backend/0]).
-export([session_details/0]).
-export([login_page_path/0, default_page_path/0]).
-export([error_404_html_file/0, error_500_html_file/0]).

%% Convenience macros for defining routes.
-define(LOGIN, #gui_route{
    requires_session = ?SESSION_NOT_LOGGED_IN,
    html_file = undefined,
    page_backend = login_backend
}).

-define(LOGOUT, #gui_route{
    requires_session = ?SESSION_LOGGED_IN,
    html_file = undefined,
    page_backend = logout_backend
}).

-define(VALIDATE_LOGIN, #gui_route{
    requires_session = ?SESSION_NOT_LOGGED_IN,
    html_file = undefined,
    page_backend = validate_login_backend
}).

-define(INDEX, #gui_route{
    requires_session = ?SESSION_LOGGED_IN,
    websocket = ?SESSION_LOGGED_IN,
    html_file = <<"index.html">>,
    page_backend = undefined
}).

%% ====================================================================
%% API
%% ====================================================================

%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% {@link gui_route_plugin_behaviour} callback route/1.
%% @end
%%--------------------------------------------------------------------
-spec route(Path :: binary()) -> #gui_route{}.
route(<<"/login.html">>) -> ?LOGIN;
route(<<"/logout.html">>) -> ?LOGOUT;
route(<<"/validate_login.html">>) -> ?VALIDATE_LOGIN;
route(<<"/">>) -> ?INDEX;
route(<<"/index.html">>) -> ?INDEX.


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_route_plugin_behaviour} callback data_backend/2
%% @end
%%--------------------------------------------------------------------
-spec data_backend(HasSession :: boolean(), Identifier :: binary()) ->
    HandlerModule :: module().
data_backend(true, <<"file">>) -> file_data_backend;
data_backend(true, <<"file-distribution">>) -> file_data_backend;
data_backend(true, <<"data-space">>) -> data_space_data_backend;
data_backend(true, <<"space">>) -> space_data_backend;
data_backend(true, <<"space-user-permission">>) -> space_data_backend;
data_backend(true, <<"space-group-permission">>) -> space_data_backend;
data_backend(true, <<"group">>) -> group_data_backend;
data_backend(true, <<"group-user-permission">>) -> group_data_backend;
data_backend(true, <<"group-group-permission">>) -> group_data_backend;
data_backend(true, <<"system-provider">>) -> system_data_backend;
data_backend(true, <<"system-user">>) -> system_data_backend;
data_backend(true, <<"system-group">>) -> system_data_backend.


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_route_plugin_behaviour} callback private_rpc_backend/0
%% @end
%%--------------------------------------------------------------------
-spec private_rpc_backend() -> HandlerModule :: module().
private_rpc_backend() -> private_rpc_backend.


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_route_plugin_behaviour} callback public_rpc_backend/0
%% @end
%%--------------------------------------------------------------------
-spec public_rpc_backend() -> HandlerModule :: module().
public_rpc_backend() -> public_rpc_backend.


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_route_plugin_behaviour} callback get_session_details/0
%% @end
%%--------------------------------------------------------------------
-spec session_details() ->
    {ok, proplists:proplist()} | gui_error:error_result().
session_details() ->
    {ok, #document{
        value = #onedata_user{
            name = Name
        }}} = onedata_user:get(g_session:get_user_id()),
    Res = [
        {<<"userName">>, Name},
        {<<"manageProvidersURL">>,
            str_utils:to_binary(oneprovider:get_oz_providers_page())}
    ],
    {ok, Res}.


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_route_plugin_behaviour} callback login_page_path/0
%% @end
%%--------------------------------------------------------------------
-spec login_page_path() -> Path :: binary().
login_page_path() ->
    <<"/login.html">>.


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_route_plugin_behaviour} callback default_page_path/0
%% @end
%%--------------------------------------------------------------------
-spec default_page_path() -> Path :: binary().
default_page_path() ->
    <<"/">>.


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_route_plugin_behaviour} callback error_404_html_file/0
%% @end
%%--------------------------------------------------------------------
-spec error_404_html_file() -> FileName :: binary().
error_404_html_file() ->
    <<"page404.html">>.


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_route_plugin_behaviour} callback error_500_html_file/0
%% @end
%%--------------------------------------------------------------------
-spec error_500_html_file() -> FileName :: binary().
error_500_html_file() ->
    <<"page500.html">>.
