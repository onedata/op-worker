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


-include("global_definitions.hrl").
-include_lib("gui/include/gui.hrl").

-export([route/1, data_backend/2, private_rpc_backend/0, public_rpc_backend/0]).
-export([session_details/0]).
-export([login_page_path/0, default_page_path/0]).
-export([error_404_html_file/0, error_500_html_file/0]).
-export([response_headers/0]).
-export([check_ws_origin/1]).

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
    requires_session = ?SESSION_ANY,
    html_file = undefined,
    page_backend = validate_login_backend
}).

-define(INDEX, #gui_route{
    requires_session = ?SESSION_ANY,
    websocket = ?SESSION_ANY,
    html_file = <<"index.html">>,
    page_backend = undefined
}).

%% ====================================================================
%% API
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link gui_route_plugin_behaviour} callback route/1.
%% @end
%%--------------------------------------------------------------------
-spec route(Path :: binary()) -> #gui_route{} | undefined.
route(<<"/login.html">>) -> ?LOGIN;
route(<<"/logout.html">>) -> ?LOGOUT;
route(<<"/validate_login.html">>) -> ?VALIDATE_LOGIN;
route(<<"/">>) -> ?INDEX;
route(<<"/index.html">>) -> ?INDEX;
% Ember-style URLs also point to index file
route(<<"/#/", _/binary>>) -> ?INDEX;
route(_) -> undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_route_plugin_behaviour} callback data_backend/2
%% @end
%%--------------------------------------------------------------------
-spec data_backend(HasSession :: boolean(), Identifier :: binary()) ->
    HandlerModule :: module().
data_backend(true, <<"user">>) -> user_data_backend;

data_backend(true, <<"group">>) -> group_data_backend;
data_backend(true, <<"group-user-list">>) -> group_data_backend;
data_backend(true, <<"group-group-list">>) -> group_data_backend;
data_backend(true, <<"group-user-permission">>) -> group_data_backend;
data_backend(true, <<"group-group-permission">>) -> group_data_backend;

data_backend(true, <<"space">>) -> space_data_backend;
data_backend(true, <<"space-user-list">>) -> space_data_backend;
data_backend(true, <<"space-group-list">>) -> space_data_backend;
data_backend(true, <<"space-provider-list">>) -> space_data_backend;
data_backend(true, <<"space-transfer-list">>) -> space_data_backend;
data_backend(true, <<"space-user-permission">>) -> space_data_backend;
data_backend(true, <<"space-group-permission">>) -> space_data_backend;
data_backend(true, <<"space-transfer-stat">>) -> space_data_backend;
data_backend(true, <<"space-transfer-time-stat">>) -> space_data_backend;
data_backend(true, <<"space-transfer-link-state">>) -> space_data_backend;
data_backend(true, <<"space-on-the-fly-transfer-list">>) -> space_data_backend;

data_backend(true, <<"share">>) -> share_data_backend;
data_backend(_, <<"share-public">>) -> share_data_backend;

data_backend(true, <<"handle-service">>) -> handle_service_data_backend;

data_backend(true, <<"handle">>) -> handle_data_backend;
data_backend(_, <<"handle-public">>) -> handle_data_backend;

data_backend(true, <<"system-provider">>) -> system_data_backend;
data_backend(true, <<"system-user">>) -> system_data_backend;
data_backend(true, <<"system-group">>) -> system_data_backend;

data_backend(true, <<"file">>) -> file_data_backend;
data_backend(true, <<"file-shared">>) -> file_data_backend;
data_backend(_, <<"file-public">>) -> file_data_backend;

data_backend(true, <<"file-permission">>) -> file_permissions_data_backend;

data_backend(true, <<"file-distribution">>) -> file_distribution_data_backend;

data_backend(true, <<"file-property">>) -> metadata_data_backend;
data_backend(_, <<"file-property-public">>) -> metadata_data_backend;
data_backend(true, <<"file-property-shared">>) -> metadata_data_backend;

data_backend(true, <<"transfer">>) -> transfer_data_backend;
data_backend(true, <<"on-the-fly-transfer">>) -> transfer_data_backend;
data_backend(true, <<"transfer-time-stat">>) -> transfer_data_backend;
data_backend(true, <<"transfer-current-stat">>) -> transfer_data_backend;

data_backend(true, <<"db-index">>) -> db_index_data_backend.


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
    ProviderId = oneprovider:get_id(),
    {ok, ProviderName} = provider_logic:get_name(ProviderId),
    TransfersHistoryLimitPerFile = application:get_env(
        ?APP_NAME, transfers_history_limit_per_file, 100
    ),
    Res = [
        {<<"userId">>, gui_session:get_user_id()},
        {<<"providerId">>, ProviderId},
        {<<"providerName">>, ProviderName},
        {<<"onezoneURL">>,
            str_utils:to_binary(oneprovider:get_oz_url())
        },
        {<<"manageProvidersURL">>,
            str_utils:to_binary(oneprovider:get_oz_providers_page())
        },
        {<<"serviceVersion">>, oneprovider:get_version()},
        {<<"config">>, [
            {<<"transfersHistoryLimitPerFile">>, TransfersHistoryLimitPerFile}
        ]}
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


%%--------------------------------------------------------------------
%% @doc
%% {@link gui_route_plugin_behaviour} callback response_headers/0
%% @end
%%--------------------------------------------------------------------
-spec response_headers() -> [{Key :: binary(), Value :: binary()}].
response_headers() ->
    {ok, Headers} = application:get_env(?APP_NAME, gui_response_headers),
    Headers.

%%--------------------------------------------------------------------
%% @doc
%% {@link gui_route_plugin_behaviour} callback check_ws_origin/1
%% @end
%%--------------------------------------------------------------------
-spec check_ws_origin(cowboy_req:req()) -> boolean().
check_ws_origin(Req) ->
    case application:get_env(?APP_NAME, check_ws_origin, true) of
        false ->
            true;
        _ ->
            OriginHeader = cowboy_req:header(<<"origin">>, Req),
            Host = maps:get(host, url_utils:parse(OriginHeader)),
            Domain = case provider_logic:get_domain() of
                {ok, D} -> D;
                _ -> undefined
            end,
            {_, IP} = inet:parse_ipv4strict_address(binary_to_list(Host)),
            (Domain == Host) or (IP == node_manager:get_ip_address())
    end.

