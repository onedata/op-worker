%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc gui listener starting & stopping
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
%%% listener_starter_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link listener_starter_behaviour} callback port/0.
%% @end
%%--------------------------------------------------------------------
-spec port() -> integer().
port() ->
    {ok, GuiPort} = application:get_env(?APP_NAME, gui_https_port),
    GuiPort.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_starter_behaviour} callback start/1.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, Reason :: term()}.
start() ->
    % Get params from env for gui
    {ok, DocRoot} =
        application:get_env(?APP_NAME, gui_static_files_root),
    {ok, GuiPort} = application:get_env(?APP_NAME, gui_https_port),
    {ok, GuiNbAcceptors} =
        application:get_env(?APP_NAME, gui_number_of_acceptors),
    {ok, MaxKeepAlive} =
        application:get_env(?APP_NAME, gui_max_keepalive),
    {ok, Timeout} =
        application:get_env(?APP_NAME, gui_socket_timeout_seconds),
    {ok, Cert} = application:get_env(?APP_NAME, web_ssl_cert_path),

    % Setup GUI dispatch opts for cowboy
    GUIDispatch = [
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
        {'_', [
            {?provider_id_path, get_provider_id_handler, []},
            {"/nagios/[...]", nagios_handler, []},
            {?WEBSOCKET_PREFIX_PATH ++ "[...]", gui_ws_handler, []},
            {"/[...]", gui_static_handler, {dir, DocRoot}}
        ]}
    ],

    % Call gui init, which will call init on all modules that might need state.
    gui:init(),
    % Start the listener for web gui and nagios handler
    Result = ranch:start_listener(?HTTPS_LISTENER, GuiNbAcceptors,
        ranch_ssl2, [
            {ip, {127, 0, 0, 1}},
            {port, GuiPort},
            {certfile, Cert}
        ], cowboy_protocol, [
            {env, [{dispatch, cowboy_router:compile(GUIDispatch)}]},
            {max_keepalive, MaxKeepAlive},
            {timeout, timer:seconds(Timeout)},
            % On every request add headers that improve security of the response
            {onrequest, fun gui_utils:onrequest_adjust_headers/1}
        ]),
    case Result of
        {ok, _} -> ok;
        _ -> Result
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_starter_behaviour} callback stop/1.
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
%% Returns the status of a listener.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | {error, server_not_responding}.
healthcheck() ->
    {ok, GuiPort} = application:get_env(?APP_NAME, gui_https_port),
    case http_client:get("https://127.0.0.1:" ++ integer_to_list(GuiPort),
        [], <<>>, [insecure]) of
        {ok, _, _, _} ->
            ok;
        _ ->
            {error, server_not_responding}
    end.
