%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module is responsible for GUI listener starting and stopping.
%%% @end
%%%--------------------------------------------------------------------
-module(gui_listener).
-behaviour(listener_behaviour).
-author("Michal Zmuda").
-author("Lukasz Opiola").

-include("http/http_common.hrl").
-include("global_definitions.hrl").
-include_lib("gui/include/gui.hrl").
-include_lib("ctool/include/logging.hrl").

% Cowboy listener references
-define(HTTPS_LISTENER, https).

%% listener_behaviour callbacks
-export([port/0, start/0, stop/0, healthcheck/0]).

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
    {ok, GuiPort} = application:get_env(?APP_NAME, gui_https_port),
    GuiPort.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback start/0.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, Reason :: term()}.
start() ->
    % Get params from env for gui
    {ok, GuiPort} = application:get_env(?APP_NAME, gui_https_port),
    {ok, GuiNbAcceptors} =
        application:get_env(?APP_NAME, gui_number_of_acceptors),
    {ok, MaxKeepAlive} =
        application:get_env(?APP_NAME, gui_max_keepalive),
    {ok, Timeout} =
        application:get_env(?APP_NAME, gui_socket_timeout_seconds),
    {ok, KeyFile} = application:get_env(?APP_NAME, web_ssl_key_file),
    {ok, CertFile} = application:get_env(?APP_NAME, web_ssl_cert_file),
    {ok, CaCertsDir} = application:get_env(?APP_NAME, cacerts_dir),
    {ok, CaCertPems} = file_utils:read_files({dir, CaCertsDir}),
    CaCerts = lists:map(fun cert_decoder:pem_to_der/1, CaCertPems),

    % Resolve static files root. First, check if there is a non-empty dir
    % located in gui_custom_static_root. If not, use default.
    {ok, CstmRoot} = application:get_env(?APP_NAME, gui_custom_static_root),
    {ok, DefRoot} = application:get_env(?APP_NAME, gui_default_static_root),
    DocRoot = case file:list_dir_all(CstmRoot) of
        {error, enoent} -> DefRoot;
        {ok, []} -> DefRoot;
        {ok, _} -> CstmRoot
    end,

    % Setup GUI dispatch opts for cowboy
    Dispatch = cowboy_router:compile([
        % Matching requests will be redirected
        % to the same address without leading 'www.'
        % Cowboy does not have a mechanism to match
        % every hostname starting with 'www.'
        % This will match hostnames with up to 6 segments
        % e. g. www.seg2.seg3.seg4.seg5.com
        {"www.:_[.:_[.:_[.:_[.:_]]]]", [
            % redirector_handler is defined in cluster_worker
            {'_', redirector_handler, []}
        ]},
        % Proper requests are routed to handler modules
        {'_', lists:flatten([
            {?provider_id_path, get_provider_id_handler, []},
            {?provider_version_path, get_provider_version_handler, []},
            {"/nagios/[...]", nagios_handler, []},
            {"/upload", upload_handler, []},
            {"/download/:id", download_handler, []},
            {?WEBSOCKET_PREFIX_PATH ++ "[...]", gui_ws_handler, []},
            rest_router:top_level_routing(),
            {"/[...]", gui_static_handler, {dir, DocRoot}}
        ])}
    ]),

    % Call gui init, which will call init on all modules that might need state.
    gui:init(),
    % Start the listener for web gui and nagios handler
    Result = ranch:start_listener(?HTTPS_LISTENER, GuiNbAcceptors,
        ranch_ssl, [
            {port, GuiPort},
            {keyfile, KeyFile},
            {certfile, CertFile},
            {cacerts, CaCerts},
            {ciphers, ssl:cipher_suites() -- ssl_utils:weak_ciphers()}
        ], cowboy_protocol, [
            {env, [{dispatch, Dispatch}]},
            {max_keepalive, MaxKeepAlive},
            {timeout, timer:seconds(Timeout)},
            % On every request, add headers that improve
            % security to the response
            {onrequest, fun gui:response_headers/1}
        ]),
    case Result of
        {ok, _} -> ok;
        _ -> Result
    end.


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
    case catch cowboy:stop_listener(?HTTPS_LISTENER) of
        (ok) ->
            ok;
        (Error) ->
            ?error("Error on stopping listener ~p: ~p",
                [?HTTPS_LISTENER, Error]),
            {error, https_listener_stop_error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback healthcheck/0.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | {error, server_not_responding}.
healthcheck() ->
    Endpoint = "https://127.0.0.1:" ++ integer_to_list(port()),
    case http_client:get(Endpoint, #{}, <<>>, [insecure]) of
        {ok, _, _, _} -> ok;
        _ -> {error, server_not_responding}
    end.
