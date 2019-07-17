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

-include("global_definitions.hrl").
-include("http/gui_paths.hrl").
-include("http/op_gui.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("gui/include/gui.hrl").

% Listener config
-define(PORT, application:get_env(?APP_NAME, https_server_port, 443)).
-define(ACCEPTORS_NUM, application:get_env(?APP_NAME, https_acceptors, 100)).
-define(REQUEST_TIMEOUT, application:get_env(?APP_NAME, https_request_timeout, timer:minutes(5))).
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

    CustomCowboyRoutes = lists:flatten([
        {?NAGIOS_PATH, nagios_handler, []},
        {?CLIENT_PROTOCOL_PATH, connection, []},
        {?WEBSOCKET_PREFIX_PATH ++ "[...]", op_gui_ws_handler, []},
        {"/cdmi/cdmi_objectid/:id/[...]", cdmi_handler, by_id},
        {"/cdmi/[...]", cdmi_handler, by_path},
        rest_handler:rest_routes(),
        rest_router:top_level_routing()
    ]),

    DynamicPageRoutes = [
        {?NAGIOS_OZ_CONNECTIVITY_PATH, [<<"GET">>], page_oz_connectivity},
        {?IDENTITY_MACAROON_PATH, [<<"GET">>], page_identity_macaroon},
        {?NONCE_VERIFY_PATH, [<<"GET">>], page_nonce_verify},
        {?DEPRECATED_PROVIDER_CONFIGURATION_PATH, [<<"GET">>], page_provider_configuration},
        {?FILE_UPLOAD_PATH, [<<"OPTIONS">>, <<"POST">>], page_file_upload},
        {?FILE_DOWNLOAD_PATH ++ "/:code", [<<"GET">>], page_file_download},
        {?PUBLIC_SHARE_COWBOY_ROUTE, [<<"GET">>], page_public_share},
        {?FAVICON_PATH, [<<"GET">>], page_favicon},
        {"/", [<<"GET">>], page_redirect_to_onezone}
    ],

    % Call gui init, which will call init on all modules that might need state.
    op_gui:init(),

    gui:start(#gui_config{
        port = port(),
        key_file = KeyFile,
        cert_file = CertFile,
        chain_file = ChainFile,
        number_of_acceptors = ?ACCEPTORS_NUM,
        max_keepalive = ?MAX_KEEPALIVE,
        request_timeout = ?REQUEST_TIMEOUT,
        dynamic_pages = DynamicPageRoutes,
        custom_cowboy_routes = CustomCowboyRoutes
    }).


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback stop/0.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok | {error, Reason :: term()}.
stop() ->
    % Call gui cleanup, which will call cleanup on all modules that
    % were previously set up with op_gui:init/0.
    op_gui:cleanup(),
    gui:stop().


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback healthcheck/0.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | {error, server_not_responding}.
healthcheck() ->
    gui:healthcheck().


%%--------------------------------------------------------------------
%% @doc
%% Returns intermediate CA chain in PEM format for gui web cert.
%% @end
%%--------------------------------------------------------------------
-spec get_cert_chain_pems() -> [public_key:der_encoded()].
get_cert_chain_pems() ->
    gui:get_cert_chain_pems().
