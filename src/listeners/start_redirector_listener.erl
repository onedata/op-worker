%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc redirector listener starting & stopping
%%% @end
%%%--------------------------------------------------------------------
-module(start_redirector_listener).
-author("Tomasz Lichon").
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

% Session logic module
-define(SESSION_LOGIC_MODULE, session_logic).

% Cowboy listener references
-define(HTTP_REDIRECTOR_LISTENER, http).

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
  {ok, RedirectPort} =
    application:get_env(?APP_NAME, http_worker_redirect_port),
  {ok, RedirectNbAcceptors} =
    application:get_env(?APP_NAME, http_worker_number_of_http_acceptors),
  {ok, Timeout} =
    application:get_env(?APP_NAME, http_worker_socket_timeout_seconds),
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
      {timeout, timer:seconds(Timeout)}
    ]).

%%--------------------------------------------------------------------
%% @doc
%% {@link listener_starter_behaviour} callback stop_listener/1.
%% @end
%%--------------------------------------------------------------------
-spec stop_listener() -> ok | {error, Reason :: term()}.
stop_listener() ->
  case catch cowboy:stop_listener(?HTTP_REDIRECTOR_LISTENER) of
    (ok) -> ok;
    (Error) -> ?error("Error on stopping listener ~p: ~p", [?HTTP_REDIRECTOR_LISTENER, Error])
  end.