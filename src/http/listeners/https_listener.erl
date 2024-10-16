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
-include("http/cdmi.hrl").
-include("http/gui_paths.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("gui/include/gui.hrl").

% Listener config
-define(PORT, op_worker:get_env(https_server_port, 443)).
-define(ACCEPTORS_NUM, op_worker:get_env(https_acceptors, 100)).
-define(REQUEST_TIMEOUT, op_worker:get_env(https_request_timeout, timer:minutes(5))).
-define(MAX_KEEPALIVE, op_worker:get_env(https_max_keepalive, 30)).

-define(ONEPANEL_CONNECT_OPTS, fun() -> [
    {recv_timeout, timer:seconds(op_worker:get_env(onepanel_proxy_recv_timeout_sec, 60))},
    {ssl_options, [
        {secure, only_verify_peercert},
        {cacerts, get_cert_chain_ders()}
    ]}
] end).

%% listener_behaviour callbacks
-export([port/0, start/0, stop/0, reload_web_certs/0, healthcheck/0]).
-export([get_cert_chain_ders/0]).


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
    gui:start(gui_config()).


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback stop/0.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok | {error, Reason :: term()}.
stop() ->
    gui:stop().


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback reload_web_certs/0.
%% @end
%%--------------------------------------------------------------------
-spec reload_web_certs() -> ok | {error, term()}.
reload_web_certs() ->
    gui:reload_web_certs(gui_config()).


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
%% Returns intermediate CA chain in DER format for gui web cert.
%% @end
%%--------------------------------------------------------------------
-spec get_cert_chain_ders() -> [public_key:der_encoded()].
get_cert_chain_ders() ->
    gui:get_cert_chain_ders().


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec gui_config() -> gui:gui_config().
gui_config() ->
    % Get certs
    KeyFile = op_worker:get_env(web_key_file),
    CertFile = op_worker:get_env(web_cert_file),
    ChainFile = op_worker:get_env(web_cert_chain_file, undefined),

    CustomCowboyRoutes = lists:flatten([
        {?NAGIOS_PATH, nagios_handler, []},
        {?CLIENT_PROTOCOL_PATH, connection, []},
        {?PANEL_REST_PROXY_PATH ++ "[...]", http_port_forwarder, [9443, ?ONEPANEL_CONNECT_OPTS]},
        {?GUI_GRAPH_SYNC_WS_PATH, gs_ws_handler, [gui_gs_translator]}, % blocked when no DB space
        {?OPENFAAS_ACTIVITY_FEED_WS_COWBOY_ROUTE, atm_openfaas_activity_feed_ws_handler, []},
        {?ATM_JOB_OUTPUT_CALLBACK_PATH, atm_openfaas_task_callback_handler, #{type => output}},
        {?ATM_JOB_HEARTBEAT_CALLBACK_PATH, atm_openfaas_task_callback_handler, #{type => heartbeat}},
        {?CDMI_ID_PATH, cdmi_handler, by_id}, % blocked when no DB space
        {?CDMI_PATH, cdmi_handler, by_path}, % blocked when no DB space
        rest_routes:routes() % blocked when no DB space
    ]),

    DynamicPageRoutes = [
        {?NAGIOS_OZ_CONNECTIVITY_PATH, [<<"GET">>], page_oz_connectivity},
        {?IDENTITY_TOKEN_PATH, [<<"GET">>], page_identity_token},
        {?DEPRECATED_PROVIDER_CONFIGURATION_PATH, [<<"GET">>], page_provider_configuration},
        {?FILE_UPLOAD_PATH, [<<"OPTIONS">>, <<"POST">>], page_file_upload},
        {?GUI_FILE_CONTENT_DOWNLOAD_PATH ++ "/:code", [<<"GET">>], page_file_content_download},
        {?GUI_ATM_STORE_DUMP_DOWNLOAD_PATH ++ "/:code", [<<"GET">>], page_atm_store_dump_download},
        {?PUBLIC_SHARE_COWBOY_ROUTE, [<<"GET">>], page_public_share},
        {"/", [<<"GET">>], page_redirect_to_onezone}
    ],

    #gui_config{
        port = port(),
        key_file = KeyFile,
        cert_file = CertFile,
        chain_file = ChainFile,
        number_of_acceptors = ?ACCEPTORS_NUM,
        max_keepalive = ?MAX_KEEPALIVE,
        request_timeout = ?REQUEST_TIMEOUT,
        dynamic_pages = DynamicPageRoutes,
        custom_cowboy_routes = CustomCowboyRoutes
    }.