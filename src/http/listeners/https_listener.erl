%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module is responsible for GUI listener starting and stopping.
%%% @end
%%%--------------------------------------------------------------------
-module(https_listener).
-author("Lukasz Opiola").

-behaviour(listener_behaviour).

-include("http/gui_paths.hrl").
-include("global_definitions.hrl").
-include_lib("gui/include/gui.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("gui/include/new_gui.hrl").

% Listener config
-define(PORT, application:get_env(?APP_NAME, https_server_port, 443)).
-define(ACCEPTORS_NUM, application:get_env(?APP_NAME, https_acceptors, 10)).
-define(REQUEST_TIMEOUT, application:get_env(?APP_NAME, https_request_timeout, timer:seconds(30))).
-define(MAX_KEEPALIVE, application:get_env(?APP_NAME, https_max_keepalive, 30)).

%% listener_behaviour callbacks
-export([port/0, start/0, stop/0, healthcheck/0]).
-export([get_cert_chain_pems/0]).

%%%===================================================================
%%% listener_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback port/0.
%% @end
%%--------------------------------------------------------------------
-spec port() -> integer().
port() ->
    ?PORT.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback start/0.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, Reason :: term()}.
start() ->
    % Get certs
    {ok, KeyFile} = application:get_env(?APP_NAME, web_key_file),
    {ok, CertFile} = application:get_env(?APP_NAME, web_cert_file),
    ChainFile = application:get_env(?APP_NAME, web_cert_chain_file, undefined),

    {ok, CustomRoot} = application:get_env(?APP_NAME, gui_custom_static_root),
    {ok, DefaultRoot} = application:get_env(?APP_NAME, gui_default_static_root),

    CustomCowboyRoutes = lists:flatten([
        {?NAGIOS_PATH, nagios_handler, []},
        {?CLIENT_PROTOCOL_PATH, incoming_connection, []},
        {?WEBSOCKET_PREFIX_PATH ++ "[...]", gui_ws_handler, []},
        rest_router:top_level_routing()
    ]),

    DynamicPageRoutes = [
        {?LOGIN_PATH, [<<"GET">>], page_login},
        {?LOGOUT_PATH, [<<"GET">>], page_logout},
        {?VALIDATE_LOGIN_PATH_DEPRECATED, [<<"GET">>], page_validate_login},
        {?VALIDATE_LOGIN_PATH, [<<"GET">>], page_validate_login},
        {?NAGIOS_OZ_CONNECTIVITY_PATH, [<<"GET">>], page_oz_connectivity},
        {?IDENTITY_MACAROON_PATH, [<<"GET">>], page_identity_macaroon},
        {?NONCE_VERIFY_PATH, [<<"GET">>], page_nonce_verify},
        {?PROVIDER_VERSION_PATH, [<<"GET">>], page_provider_version},
        {?PROVIDER_CONFIGURATION_PATH, [<<"GET">>], page_provider_configuration},
        {?FILE_UPLOAD_PATH, [<<"POST">>], page_file_upload},
        {?FILE_DOWNLOAD_PATH ++ "/:id", [<<"GET">>], page_file_download}
    ],

    % Call gui init, which will call init on all modules that might need state.
    gui:init(),

    new_gui:start(#gui_config{
        port = port(),
        key_file = KeyFile,
        cert_file = CertFile,
        chain_file = ChainFile,
        number_of_acceptors = ?ACCEPTORS_NUM,
        max_keepalive = ?MAX_KEEPALIVE,
        request_timeout = ?REQUEST_TIMEOUT,
        dynamic_pages = DynamicPageRoutes,
        custom_cowboy_routes = CustomCowboyRoutes,
        default_static_root = DefaultRoot,
        custom_static_root = CustomRoot
    }).


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback stop/0.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok | {error, Reason :: term()}.
stop() ->
    % Call gui cleanup, which will call cleanup on all modules that
    % were previously set up with gui:init/0.
    gui:cleanup(),
    new_gui:stop().


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback healthcheck/0.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | {error, server_not_responding}.
healthcheck() ->
    new_gui:healthcheck().


%%--------------------------------------------------------------------
%% @doc
%% Returns intermediate CA chain in PEM format for gui web cert.
%% @end
%%--------------------------------------------------------------------
-spec get_cert_chain_pems() -> [public_key:der_encoded()].
get_cert_chain_pems() ->
    new_gui:get_cert_chain_pems().
