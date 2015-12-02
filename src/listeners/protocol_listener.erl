%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc protocol listener starting & stopping
%%% @end
%%%--------------------------------------------------------------------
-module(protocol_listener).
-author("Tomasz Lichon").
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

% Session logic module
-define(SESSION_LOGIC_MODULE, session_logic).

% Cowboy listener references
-define(TCP_PROTO_LISTENER, tcp_proto).

-behaviour(listener_behaviour).

%% listener_starter_behaviour callbacks
-export([start/0, stop/0]).

%%%===================================================================
%%% listener_starter_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link listener_starter_behaviour} callback start/1.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, Reason :: term()}.
start() ->
  {ok, Port} = application:get_env(?APP_NAME, protocol_handler_port),
  {ok, DispatcherPoolSize} =
    application:get_env(?APP_NAME, protocol_handler_pool_size),
  {ok, CertFile} =
    application:get_env(?APP_NAME, protocol_handler_ssl_cert_path),
  Ip = case application:get_env(?APP_NAME, protocol_handler_bind_addr) of
         {ok, loopback} -> {127, 0, 0, 1};
         {ok, all} -> {0, 0, 0, 0}
       end,

  Result = ranch:start_listener(?TCP_PROTO_LISTENER, DispatcherPoolSize,
    ranch_ssl2, [
      {ip, Ip},
      {port, Port},
      {certfile, CertFile}
    ],
    connection, []
  ),
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
  case catch cowboy:stop_listener(?TCP_PROTO_LISTENER) of
    (ok) -> ok;
    (Error) -> ?error("Error on stopping listener ~p: ~p", [?TCP_PROTO_LISTENER, Error])
  end.