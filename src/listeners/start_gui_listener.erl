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
-module(start_gui_listener).
-author("Tomasz Lichon").
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

% Custom cowboy bridge module
-define(COWBOY_BRIDGE_MODULE, n2o_handler).

% Session logic module
-define(SESSION_LOGIC_MODULE, session_logic).

% GUI routing module
-define(GUI_ROUTING_MODULE, gui_routes).

% Paths in gui static directory
-define(STATIC_PATHS, ["/common/", "/css/", "/flatui/", "/fonts/", "/images/", "/js/", "/n2o/"]).

% Cowboy listener references
-define(HTTPS_LISTENER, https).

-behaviour(listener_starter_behaviour).

%% listener_starter_behaviour callbacks
-export([start_listener/0, stop_listener/0]).

%%%===================================================================
%%% listener_starter_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link listener_starter_behaviour} callback start_listener/1.
%% @end
%%--------------------------------------------------------------------
-spec start_listener() -> {ok, pid()} | no_return().
start_listener() ->
  % Get params from env for gui
  {ok, DocRoot} =
    application:get_env(?APP_NAME, http_worker_static_files_root),
  {ok, Cert} = application:get_env(?APP_NAME, web_ssl_cert_path),
  {ok, GuiPort} = application:get_env(?APP_NAME, http_worker_https_port),
  {ok, GuiNbAcceptors} =
    application:get_env(?APP_NAME, http_worker_number_of_acceptors),
  {ok, MaxKeepAlive} =
    application:get_env(?APP_NAME, http_worker_max_keepalive),
  {ok, Timeout} =
    application:get_env(?APP_NAME, http_worker_socket_timeout_seconds),

  % Setup GUI dispatch opts for cowboy
  GUIDispatch = [
    % Matching requests will be redirected to the same address without leading 'www.'
    % Cowboy does not have a mechanism to match every hostname starting with 'www.'
    % This will match hostnames with up to 6 segments
    % e. g. www.seg2.seg3.seg4.seg5.com
    {"www.:_[.:_[.:_[.:_[.:_]]]]", [{'_', opn_cowboy_bridge,
      [
        {delegation, true},
        {handler_module, https_redirect_handler},
        {handler_opts, []}
      ]}
    ]},
    % Proper requests are routed to handler modules
    {'_', static_dispatches(DocRoot, ?STATIC_PATHS) ++ [
      {"/nagios/[...]", opn_cowboy_bridge,
        [
          {delegation, true},
          {handler_module, nagios_handler},
          {handler_opts, []}
        ]},
      {'_', opn_cowboy_bridge,
        [
          {delegation, true},
          {handler_module, n2o_handler},
          {handler_opts, []}
        ]}
    ]}
  ],

  % Create ets tables and set envs needed by n2o
  gui_utils:init_n2o_ets_and_envs(GuiPort, ?GUI_ROUTING_MODULE,
    ?SESSION_LOGIC_MODULE, ?COWBOY_BRIDGE_MODULE),

  % Start the listener for web gui and nagios handler
  {ok, _} = ranch:start_listener(?HTTPS_LISTENER, GuiNbAcceptors,
    ranch_ssl2, [
      {ip, {127, 0, 0, 1}},
      {port, GuiPort},
      {certfile, Cert}
    ], cowboy_protocol, [
      {env, [{dispatch, cowboy_router:compile(GUIDispatch)}]},
      {max_keepalive, MaxKeepAlive},
      {timeout, timer:seconds(Timeout)},
      % On every request, add headers that improve security to the response
      {onrequest, fun gui_utils:onrequest_adjust_headers/1}
    ]).

%%--------------------------------------------------------------------
%% @doc
%% {@link listener_starter_behaviour} callback stop_listener/1.
%% @end
%%--------------------------------------------------------------------
-spec stop_listener() -> ok | {error, Reason :: term()}.
stop_listener() ->
  case {catch cowboy:stop_listener(?HTTPS_LISTENER), catch gui_utils:cleanup_n2o(?SESSION_LOGIC_MODULE)} of
    ({ok, ok}) -> ok;
    ({Error, ok}) -> ?error("Error on stopping listener ~p: ~p", [?HTTPS_LISTENER, Error]);
    ({_, Error}) -> ?error("Error on cleaning n2o ~p: ~p", [?SESSION_LOGIC_MODULE, Error])
  end.

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

