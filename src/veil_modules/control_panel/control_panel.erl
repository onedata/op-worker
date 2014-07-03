%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour callbacks.
%% It is responsible for setting up cowboy listener and registering
%% handlers for n2o (GUI) and REST.
%% @end
%% ===================================================================

-module(control_panel).
-behaviour(worker_plugin_behaviour).

-include("veil_modules/control_panel/common.hrl").
-include_lib("ctool/include/logging.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

% Paths in gui static directory
-define(static_paths, ["/css/", "/fonts/", "/images/", "/js/", "/n2o/"]).

% Session logic module
-define(session_logic_module, session_logic).

% GUI routing module
-define(gui_routing_module, gui_routes).

% Cowboy listener reference
-define(https_listener, https).
-define(http_redirector_listener, http).
-define(rest_listener, rest).

%% ===================================================================
%% Behaviour callback functions
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1 <br />
%% Sets up cowboy handlers for GUI and REST.
%% @end
-spec init(Args :: term()) -> Result when
    Result :: ok | {error, Error},
    Error :: term().
%% ====================================================================
init(_Args) ->
    % Get params from env for gui
    {ok, DocRoot} = application:get_env(veil_cluster_node, control_panel_static_files_root),

    {ok, Cert} = application:get_env(veil_cluster_node, ssl_cert_path),
    CertString = atom_to_list(Cert),

    {ok, GuiPort} = application:get_env(veil_cluster_node, control_panel_port),
    {ok, GuiNbAcceptors} = application:get_env(veil_cluster_node, control_panel_number_of_acceptors),
    {ok, MaxKeepAlive} = application:get_env(veil_cluster_node, control_panel_max_keepalive),
    {ok, Timeout} = application:get_env(veil_cluster_node, control_panel_socket_timeout),

    % Setup GUI dispatch opts for cowboy
    GUIDispatch = [
        % Matching requests will be redirected to the same address without leading 'www.'
        % Cowboy does not have a mechanism to match every hostname starting with 'www.'
        % This will match hostnames with up to 6 segments
        % e. g. www.seg2.seg3.seg4.seg5.com
        {"www.:_[.:_[.:_[.:_[.:_]]]]", [{'_', redirect_handler, []}]},
        % Proper requests are routed to handler modules
        {'_', static_dispatches(atom_to_list(DocRoot), ?static_paths) ++ [
            {"/nagios/[...]", nagios_handler, []},
            {?user_content_download_path ++ "/:path", file_download_handler, [{type, ?user_content_request_type}]},
            {?shared_files_download_path ++ "/:path", file_download_handler, [{type, ?shared_files_request_type}]},
            {?file_upload_path, file_upload_handler, []},
            {"/ws/[...]", bullet_handler, [{handler, n2o_bullet}]},
            {'_', n2o_cowboy, []}
        ]}
    ],

    % Create ets tables and set envs needed by n2o
    gui_utils:init_n2o_ets_and_envs(GuiPort, ?gui_routing_module, ?session_logic_module),

    % Start the listener for web gui and nagios handler
    {ok, _} = cowboy:start_https(?https_listener, GuiNbAcceptors,
        [
            {port, GuiPort},
            {certfile, CertString},
            {keyfile, CertString},
            {cacerts, gsi_handler:strip_self_signed_ca(gsi_handler:get_ca_certs())},
            {password, ""},
            {ciphers, gsi_handler:get_ciphers()}
        ],
        [
            {env, [{dispatch, cowboy_router:compile(GUIDispatch)}]},
            {max_keepalive, MaxKeepAlive},
            {timeout, Timeout},
            % On every request, add headers that improve security to the response
            {onrequest, fun gui_utils:onrequest_adjust_headers/1}
        ]),


    {ok, RedirectPort} = application:get_env(veil_cluster_node, control_panel_redirect_port),
    {ok, RedirectNbAcceptors} = application:get_env(veil_cluster_node, control_panel_number_of_http_acceptors),
    % Start the listener that will redirect all requests of http to https
    RedirectDispatch = [
        {'_', [
            {'_', redirect_handler, []}
        ]}
    ],

    {ok, _} = cowboy:start_http(?http_redirector_listener, RedirectNbAcceptors,
        [
            {port, RedirectPort}
        ],
        [
            {env, [{dispatch, cowboy_router:compile(RedirectDispatch)}]},
            {max_keepalive, 1},
            {timeout, Timeout}
        ]),


    % Get REST port from env and setup dispatch opts for cowboy
    {ok, RestPort} = application:get_env(veil_cluster_node, rest_port),
    RestDispatch = [
        {'_', [
            {"/rest/:version/[...]", rest_handler, []}
        ]}
    ],
    % Start the listener for REST handler
    {ok, _} = cowboy:start_https(?rest_listener, GuiNbAcceptors,
        [
            {port, RestPort},
            {certfile, CertString},
            {keyfile, CertString},
            {cacerts, gsi_handler:strip_self_signed_ca(gsi_handler:get_ca_certs())},
            {password, ""},
            {verify, verify_peer}, {verify_fun, {fun gsi_handler:verify_callback/3, []}},
            {ciphers, gsi_handler:get_ciphers()}
        ],
        [
            {env, [{dispatch, cowboy_router:compile(RestDispatch)}]},
            {max_keepalive, 1},
            {timeout, Timeout}
        ]),

    % Schedule the clearing of expired sessions - a periodical job
    Pid = self(),
    {ok, ClearingInterval} = application:get_env(veil_cluster_node, control_panel_sessions_clearing_period),
    erlang:send_after(ClearingInterval * 1000, Pid, {timer, {asynch, 1, {clear_expired_sessions, Pid}}}),

    ok.


%% handle/2
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1
-spec handle(ProtocolVersion :: term(), Request) -> Result when
    Request :: ping | healthcheck | get_version,
    Result :: ok | {ok, Response} | {error, Error} | pong | Version,
    Response :: term(),
    Version :: term(),
    Error :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
    pong;

handle(_ProtocolVersion, healthcheck) ->
    ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();

handle(ProtocolVersion, {clear_expired_sessions, Pid}) ->
    SessionLogicModule = gui_session_handler:get_session_logic_module(),
    NumSessionsCleared = SessionLogicModule:clear_expired_sessions(),
    ?info("Expired GUI sessions cleared (~p tokens removed)", [NumSessionsCleared]),
    {ok, ClearingInterval} = application:get_env(veil_cluster_node, control_panel_sessions_clearing_period),
    erlang:send_after(ClearingInterval * 1000, Pid, {timer, {asynch, ProtocolVersion, {clear_expired_sessions, Pid}}}),
    ok;

handle(_ProtocolVersion, _Msg) ->
    ok.


%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0 <br />
%% Stops cowboy listener and terminates
%% @end
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
%% ====================================================================
cleanup() ->
    % Stop all listeners
    cowboy:stop_listener(?http_redirector_listener),
    cowboy:stop_listener(?rest_listener),
    cowboy:stop_listener(?https_listener),

    % Clean up after n2o.
    gui_utils:cleanup_n2o(?session_logic_module),

    ok.


%% ====================================================================
%% Auxiliary functions
%% ====================================================================

%% Generates static file routing for cowboy.
static_dispatches(DocRoot, StaticPaths) ->
    _StaticDispatches = lists:map(fun(Dir) ->
        {Dir ++ "[...]", cowboy_static, {dir, DocRoot ++ Dir}}
    end, StaticPaths).
