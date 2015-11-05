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
-module(start_protocol_listener).
-author("Tomasz Lichon").
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

% Session logic module
-define(SESSION_LOGIC_MODULE, session_logic).

% Cowboy listener references
-define(TCP_PROTO_LISTENER, tcp_proto).

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
  {ok, Port} = application:get_env(?APP_NAME, protocol_handler_port),
  {ok, DispatcherPoolSize} =
    application:get_env(?APP_NAME, protocol_handler_pool_size),
  {ok, CertFile} =
    application:get_env(?APP_NAME, protocol_handler_ssl_cert_path),
  Ip = case application:get_env(?APP_NAME, protocol_handler_bind_addr) of
         {ok, loopback} -> {127, 0, 0, 1};
         {ok, all} -> {0, 0, 0, 0}
       end,

  {ok, _} = ranch:start_listener(?TCP_PROTO_LISTENER, DispatcherPoolSize,
    ranch_ssl2, [
      {ip, Ip},
      {port, Port},
      {certfile, CertFile}
    ],
    connection, []
  ).

%%--------------------------------------------------------------------
%% @doc
%% {@link listener_starter_behaviour} callback stop_listener/1.
%% @end
%%--------------------------------------------------------------------
-spec stop_listener() -> ok | {error, Reason :: term()}.
stop_listener() ->
  case catch cowboy:stop_listener(?TCP_PROTO_LISTENER) of
    (ok) -> ok;
    (Error) -> ?error("Error on stopping listener ~p: ~p", [?TCP_PROTO_LISTENER, Error])
  end.