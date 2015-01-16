%%%-------------------------------------------------------------------
%%% @author lichon
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Jan 2015 16:59
%%%-------------------------------------------------------------------
-module(node_manager_listener_starter).
-include("registered_names.hrl").
-include("cluster_elements/node_manager/node_manager_listeners.hrl").
-include("cluster_elements/oneproxy/oneproxy.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_dispatcher_listener/0, start_gui_listener/0, start_redirector_listener/0, start_rest_listener/0, start_dns_listeners/0]).

%todo remove those mocks
%% start_dispatcher_listener() -> ok.
start_gui_listener() -> ok.
start_redirector_listener() -> ok.
%% start_rest_listener() -> ok.
%% start_dns_listeners() -> ok.

%% ====================================================================
%% Cowboy listeners starting
%% ====================================================================

%% start_dispatcher_listener/0
%% ====================================================================
%% @doc Starts a cowboy listener for request_dispatcher.
%% @end
-spec start_dispatcher_listener() -> ok | no_return().
%% ====================================================================
start_dispatcher_listener() ->
    catch cowboy:stop_listener(?dispatcher_listener),
    {ok, Port} = application:get_env(?APP_Name, dispatcher_port),
    {ok, DispatcherPoolSize} = application:get_env(?APP_Name, dispatcher_pool_size),
    {ok, CertFile} = application:get_env(?APP_Name, fuse_ssl_cert_path),

    LocalPort = oneproxy:get_local_port(Port),
    Pid = spawn_link(fun() -> oneproxy:start_rproxy(Port, LocalPort, CertFile, verify_none) end),
    register(?ONEPROXY_DISPATCHER, Pid),

    Dispatch = cowboy_router:compile([{'_', [
        {?ONECLIENT_URI_PATH, client_handler, []},
        {?ONEPROVIDER_URI_PATH, provider_handler, []}
    ]}]),

    {ok, _} = cowboy:start_http(?dispatcher_listener, DispatcherPoolSize,
        [
            {ip, {127, 0, 0, 1}},
            {port, LocalPort}
        ],
        [
            {env, [{dispatch, Dispatch}]}
        ]),
    ok.


%% %% start_gui_listener/0
%% %% ====================================================================
%% %% @doc Starts a cowboy listener for n2o GUI.
%% %% @end
%% -spec start_gui_listener() -> ok | no_return().
%% %% ====================================================================
%% start_gui_listener() ->
%%     % Get params from env for gui
%%     {ok, DocRoot} = application:get_env(?APP_Name, http_worker_static_files_root),
%%
%%     {ok, Cert} = application:get_env(?APP_Name, web_ssl_cert_path),
%%
%%     {ok, GuiPort} = application:get_env(?APP_Name, http_worker_port),
%%     {ok, GuiNbAcceptors} = application:get_env(?APP_Name, http_worker_number_of_acceptors),
%%     {ok, MaxKeepAlive} = application:get_env(?APP_Name, http_worker_max_keepalive),
%%     {ok, Timeout} = application:get_env(?APP_Name, http_worker_socket_timeout),
%%
%%     LocalPort = oneproxy:get_local_port(GuiPort),
%%     spawn_link(fun() -> oneproxy:start_rproxy(GuiPort, LocalPort, Cert, verify_none) end),
%%
%%     % Setup GUI dispatch opts for cowboy
%%     GUIDispatch = [
%%         % Matching requests will be redirected to the same address without leading 'www.'
%%         % Cowboy does not have a mechanism to match every hostname starting with 'www.'
%%         % This will match hostnames with up to 6 segments
%%         % e. g. www.seg2.seg3.seg4.seg5.com
%%         {"www.:_[.:_[.:_[.:_[.:_]]]]", [{'_', opn_cowboy_bridge,
%%             [
%%                 {delegation, true},
%%                 {handler_module, redirect_handler},
%%                 {handler_opts, []}
%%             ]}
%%         ]},
%%         % Proper requests are routed to handler modules
%%         {'_', static_dispatches(DocRoot, ?static_paths) ++ [
%%             {'_', opn_cowboy_bridge,
%%                 [
%%                     {delegation, true},
%%                     {handler_module, n2o_handler},
%%                     {handler_opts, []}
%%                 ]}
%%         ]}
%%     ],
%%
%%     % Create ets tables and set envs needed by n2o
%%     gui_utils:init_n2o_ets_and_envs(GuiPort, ?gui_routing_module, ?session_logic_module, ?cowboy_bridge_module),
%%
%%     % Start the listener for web gui and nagios handler
%%     {ok, _} = cowboy:start_http(?https_listener, GuiNbAcceptors,
%%         [
%%             {ip, {127, 0, 0, 1}},
%%             {port, LocalPort}
%%         ],
%%         [
%%             {env, [{dispatch, cowboy_router:compile(GUIDispatch)}]},
%%             {max_keepalive, MaxKeepAlive},
%%             {timeout, Timeout},
%%             % On every request, add headers that improve security to the response
%%             {onrequest, fun gui_utils:onrequest_adjust_headers/1}
%%         ]).
%%
%%
%% %% start_redirector_listener/0
%% %% ====================================================================
%% %% @doc Starts a cowboy listener that will redirect all requests of http to https.
%% %% @end
%% -spec start_redirector_listener() -> ok | no_return().
%% %% ====================================================================
%% start_redirector_listener() ->
%%     {ok, RedirectPort} = application:get_env(?APP_Name, http_worker_redirect_port),
%%     {ok, RedirectNbAcceptors} = application:get_env(?APP_Name, http_worker_number_of_http_acceptors),
%%     {ok, Timeout} = application:get_env(?APP_Name, http_worker_socket_timeout),
%%
%%     RedirectDispatch = [
%%         {'_', [
%%             {'_', opn_cowboy_bridge,
%%                 [
%%                     {delegation, true},
%%                     {handler_module, opn_redirect_handler},
%%                     {handler_opts, []}
%%                 ]}
%%         ]}
%%     ],
%%
%%     {ok, _} = cowboy:start_http(?http_redirector_listener, RedirectNbAcceptors,
%%         [
%%             {port, RedirectPort}
%%         ],
%%         [
%%             {env, [{dispatch, cowboy_router:compile(RedirectDispatch)}]},
%%             {max_keepalive, 1},
%%             {timeout, Timeout}
%%         ]).


%% start_rest_listener/0
%% ====================================================================
%% @doc Starts a cowboy listener for REST requests.
%% @end
-spec start_rest_listener() -> ok | no_return().
%% ====================================================================
start_rest_listener() ->
    {ok, NbAcceptors} = application:get_env(?APP_Name, http_worker_number_of_acceptors),
    {ok, Timeout} = application:get_env(?APP_Name, http_worker_socket_timeout),

    {ok, Cert} = application:get_env(?APP_Name, web_ssl_cert_path),

    % Get REST port from env and setup dispatch opts for cowboy
    {ok, RestPort} = application:get_env(?APP_Name, rest_port),

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
    {ok, _} = cowboy:start_http(?rest_listener, NbAcceptors,
        [
            {ip, {127, 0, 0, 1}},
            {port, LocalPort}
        ],
        [
            {env, [{dispatch, cowboy_router:compile(RestDispatch)}]},
            {max_keepalive, 1},
            {timeout, Timeout}
        ]),

    ok.

%% start_dns_listeners/0
%% ====================================================================
%% @doc Starts DNS UDP and TCP listeners.
%% @end
-spec start_dns_listeners() -> ok | no_return().
%% ====================================================================
start_dns_listeners() ->
    {ok, DNSPort} = application:get_env(?APP_Name, dns_port),
    {ok, EdnsMaxUdpSize} = application:get_env(?APP_Name, edns_max_udp_size),
    {ok, TCPNumAcceptors} = application:get_env(?APP_Name, dns_tcp_acceptor_pool_size),
    {ok, TCPTImeout} = application:get_env(?APP_Name, dns_tcp_timeout),
    OnFailureFun = fun() ->
        ?error("Could not start DNS server on node ~p.", [node()])
    end,
    ok = dns_server:start(?Supervisor_Name, DNSPort, dns_worker, EdnsMaxUdpSize, TCPNumAcceptors, TCPTImeout, OnFailureFun).

%% %% ====================================================================
%% %% Internal functions
%% %% ====================================================================
%%
%% static_dispatches/2
%% ====================================================================
%% @doc Generates static file routing rules for cowboy.
%% @end
-spec static_dispatches(DocRoot :: string(), StaticPaths :: [string()]) -> [term()].
%% ====================================================================
static_dispatches(DocRoot, StaticPaths) ->
    _StaticDispatches = lists:map(fun(Dir) ->
        {Dir ++ "[...]", opn_cowboy_bridge,
            [
                {delegation, true},
                {handler_module, cowboy_static},
                {handler_opts, {dir, DocRoot ++ Dir}}
            ]}
    end, StaticPaths).