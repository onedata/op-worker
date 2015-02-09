%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions allowing to start listeners
%%% @end
%%%-------------------------------------------------------------------
-module(listener_starter).
-author("Tomasz Lichon").

-include("registered_names.hrl").
-include("cluster_elements/oneproxy/oneproxy.hrl").
-include_lib("ctool/include/logging.hrl").

%% Path (relative to domain) on which cowboy expects incomming websocket connections with client and provider
-define(ONECLIENT_URI_PATH, "/oneclient").
-define(ONEPROVIDER_URI_PATH, "/oneprovider").

% Custom cowboy bridge module
-define(COWBOY_BRIDGE_MODULE, n2o_handler).

% Session logic module
-define(SESSION_LOGIC_MODULE, session_logic).

% GUI routing module
-define(GUI_ROUTING_MODULE, gui_routes).

% Paths in gui static directory
-define(STATIC_PATHS, ["/common/", "/css/", "/flatui/", "/fonts/", "/images/", "/js/", "/n2o/"]).
-define(ONEPROXY_PROTOCOL_LISTENER, oneproxy_protocol_listener).

% Cowboy listener references
-define(HTTPS_LISTENER, https).
-define(REST_LISTENER, rest).
-define(HTTP_REDIRECTOR_LISTENER, http).
-define(TCP_PROTO_LISTENER, tcp_proto).

%% API
-export([start_protocol_listener/0, start_gui_listener/0, start_redirector_listener/0, start_rest_listener/0,
    start_dns_listeners/0, stop_listeners/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts a cowboy listener for request_dispatcher.
%% @end
%%--------------------------------------------------------------------
-spec start_protocol_listener() -> {ok, pid()} | no_return().
start_protocol_listener() ->
    {ok, Port} = application:get_env(?APP_NAME, dispatcher_port),
    {ok, DispatcherPoolSize} = application:get_env(?APP_NAME, dispatcher_pool_size),
    {ok, CertFile} = application:get_env(?APP_NAME, fuse_ssl_cert_path),

    LocalPort = oneproxy:get_local_port(Port),
    Pid = spawn_link(fun() -> oneproxy:start_rproxy(Port, LocalPort, CertFile, verify_none, no_http) end),
    register(?ONEPROXY_PROTOCOL_LISTENER, Pid),

    {ok, _} = ranch:start_listener(?TCP_PROTO_LISTENER, DispatcherPoolSize,
        ranch_tcp, [{ip, {127, 0, 0, 1}}, {port, LocalPort}],
        protocol_handler, []
    ).


%%--------------------------------------------------------------------
%% @doc
%% Starts a cowboy listener for n2o GUI.
%% @end
%%--------------------------------------------------------------------
-spec start_gui_listener() -> {ok, pid()} | no_return().
start_gui_listener() ->
    % Get params from env for gui
    {ok, DocRoot} = application:get_env(?APP_NAME, http_worker_static_files_root),
    {ok, Cert} = application:get_env(?APP_NAME, web_ssl_cert_path),
    {ok, GuiPort} = application:get_env(?APP_NAME, http_worker_https_port),
    {ok, GuiNbAcceptors} = application:get_env(?APP_NAME, http_worker_number_of_acceptors),
    {ok, MaxKeepAlive} = application:get_env(?APP_NAME, http_worker_max_keepalive),
    {ok, Timeout} = application:get_env(?APP_NAME, http_worker_socket_timeout),
    LocalPort = oneproxy:get_local_port(GuiPort),
    spawn_link(fun() -> oneproxy:start_rproxy(GuiPort, LocalPort, Cert, verify_none) end),

    % Setup GUI dispatch opts for cowboy
    GUIDispatch = [
        % Matching requests will be redirected to the same address without leading 'www.'
        % Cowboy does not have a mechanism to match every hostname starting with 'www.'
        % This will match hostnames with up to 6 segments
        % e. g. www.seg2.seg3.seg4.seg5.com
        {"www.:_[.:_[.:_[.:_[.:_]]]]", [{'_', opn_cowboy_bridge,
            [
                {delegation, true},
                {handler_module, redirect_handler},
                {handler_opts, []}
            ]}
        ]},
        % Proper requests are routed to handler modules
        {'_', static_dispatches(DocRoot, ?STATIC_PATHS) ++ [
            {'_', opn_cowboy_bridge,
                [
                    {delegation, true},
                    {handler_module, n2o_handler},
                    {handler_opts, []}
                ]}
        ]}
    ],

    % Create ets tables and set envs needed by n2o
    gui_utils:init_n2o_ets_and_envs(GuiPort, ?GUI_ROUTING_MODULE, ?SESSION_LOGIC_MODULE, ?COWBOY_BRIDGE_MODULE),

    % Start the listener for web gui and nagios handler
    {ok, _} = cowboy:start_http(?HTTPS_LISTENER, GuiNbAcceptors,
        [
            {ip, {127, 0, 0, 1}},
            {port, LocalPort}
        ],
        [
            {env, [{dispatch, cowboy_router:compile(GUIDispatch)}]},
            {max_keepalive, MaxKeepAlive},
            {timeout, Timeout},
            % On every request, add headers that improve security to the response
            {onrequest, fun gui_utils:onrequest_adjust_headers/1}
        ]).


%%--------------------------------------------------------------------
%% @doc
%% Starts a cowboy listener that will redirect all requests of http to https.
%% @end
%%--------------------------------------------------------------------
-spec start_redirector_listener() -> {ok, pid()} | no_return().
start_redirector_listener() ->
    {ok, RedirectPort} = application:get_env(?APP_NAME, http_worker_redirect_port),
    {ok, RedirectNbAcceptors} = application:get_env(?APP_NAME, http_worker_number_of_http_acceptors),
    {ok, Timeout} = application:get_env(?APP_NAME, http_worker_socket_timeout),
    RedirectDispatch = [
        {'_', [
            {'_', opn_cowboy_bridge,
                [
                    {delegation, true},
                    {handler_module, opn_redirect_handler},
                    {handler_opts, []}
                ]}
        ]}
    ],
    {ok, _} = cowboy:start_http(?HTTP_REDIRECTOR_LISTENER, RedirectNbAcceptors,
        [
            {port, RedirectPort}
        ],
        [
            {env, [{dispatch, cowboy_router:compile(RedirectDispatch)}]},
            {max_keepalive, 1},
            {timeout, Timeout}
        ]).

%%--------------------------------------------------------------------
%% @doc
%% Starts a cowboy listener for REST requests.
%% @end
%%--------------------------------------------------------------------
-spec start_rest_listener() -> {ok, pid()} | no_return().
start_rest_listener() ->
    {ok, NbAcceptors} = application:get_env(?APP_NAME, http_worker_number_of_acceptors),
    {ok, Timeout} = application:get_env(?APP_NAME, http_worker_socket_timeout),
    {ok, Cert} = application:get_env(?APP_NAME, web_ssl_cert_path),
    {ok, RestPort} = application:get_env(?APP_NAME, http_worker_rest_port),
    LocalPort = oneproxy:get_local_port(RestPort),
    Pid = spawn_link(fun() -> oneproxy:start_rproxy(RestPort, LocalPort, Cert, verify_peer) end),
    register(?ONEPROXY_REST, Pid),

    RestDispatch = [
        {'_', [
            {"/rest/:version/[...]", opn_cowboy_bridge,
                [
                    {delegation, true},
                    {handler_module, rest_handler},
                    {handler_opts, []}
                ]},
            {"/cdmi/[...]", opn_cowboy_bridge,
                [
                    {delegation, true},
                    {handler_module, cdmi_handler},
                    {handler_opts, []}
                ]}
        ]}
    ],

    % Start the listener for REST handler
    {ok, _} = cowboy:start_http(?REST_LISTENER, NbAcceptors,
        [
            {ip, {127, 0, 0, 1}},
            {port, LocalPort}
        ],
        [
            {env, [{dispatch, cowboy_router:compile(RestDispatch)}]},
            {max_keepalive, 1},
            {timeout, Timeout}
        ]).

%%--------------------------------------------------------------------
%% @doc
%% Starts DNS UDP and TCP listeners.
%% @end
%%--------------------------------------------------------------------
-spec start_dns_listeners() -> ok | no_return().
start_dns_listeners() ->
    {ok, DNSPort} = application:get_env(?APP_NAME, dns_port),
    {ok, EdnsMaxUdpSize} = application:get_env(?APP_NAME, edns_max_udp_size),
    {ok, TCPNumAcceptors} = application:get_env(?APP_NAME, dns_tcp_acceptor_pool_size),
    {ok, TCPTImeout} = application:get_env(?APP_NAME, dns_tcp_timeout),
    OnFailureFun = fun() ->
        ?error("Could not start DNS server on node ~p.", [node()])
    end,
    ok = dns_server:start(?APPLICATION_SUPERVISOR_NAME, DNSPort, dns_worker, EdnsMaxUdpSize, TCPNumAcceptors, TCPTImeout, OnFailureFun).

%%--------------------------------------------------------------------
%% @doc
%% Stops all listeners defined in this module
%% @end
%%--------------------------------------------------------------------
-spec stop_listeners() -> ok.
stop_listeners() ->
    Listeners = [?HTTP_REDIRECTOR_LISTENER, ?REST_LISTENER, ?HTTPS_LISTENER, ?SESSION_LOGIC_MODULE],
    Results = lists:map(
        fun (?SESSION_LOGIC_MODULE) -> catch gui_utils:cleanup_n2o(?SESSION_LOGIC_MODULE);
            (X) -> {X, catch cowboy:stop_listener(X)}
        end, Listeners),
    lists:foreach(
        fun ({_, ok}) -> ok;
            (ok) -> ok;
            ({X, Error}) -> ?error("Error on stopping listener ~p: ~p", [X, Error])
        end, Results).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generates static file routing rules for cowboy.
%% @end
%%--------------------------------------------------------------------
-spec static_dispatches(DocRoot :: string(), StaticPaths :: [string()]) -> [term()].
static_dispatches(DocRoot, StaticPaths) ->
    _StaticDispatches = lists:map(fun(Dir) ->
        {Dir ++ "[...]", opn_cowboy_bridge,
            [
                {delegation, true},
                {handler_module, cowboy_static},
                {handler_opts, {dir, DocRoot ++ Dir}}
            ]}
    end, StaticPaths).