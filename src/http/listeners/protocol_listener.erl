%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module is responsible for starting and stopping protocol listener
%%% for clients and providers.
%%% @end
%%%--------------------------------------------------------------------
-module(protocol_listener).
-behaviour(listener_behaviour).
-author("Tomasz Lichon").
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

% Cowboy listener references
-define(PROTO_LISTENER, tcp_proto).


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
    {ok, Port} = application:get_env(?APP_NAME, protocol_handler_port),
    Port.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback start/0.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, Reason :: term()}.
start() ->
    {ok, Port} = application:get_env(?APP_NAME, protocol_handler_port),
    {ok, AcceptorPoolSize} =
        application:get_env(?APP_NAME, protocol_handler_pool_size),
    {ok, KeyFile} =
        application:get_env(?APP_NAME, web_key_file),
    {ok, CertFile} =
        application:get_env(?APP_NAME, web_cert_file),
    CaCerts = oneprovider:get_ca_certs(),

    Result = ranch:start_listener(?PROTO_LISTENER, AcceptorPoolSize,
        ranch_ssl, [
            {port, Port},
            {keyfile, KeyFile},
            {certfile, CertFile},
            {cacerts, CaCerts},
            {ciphers, ssl:cipher_suites() -- ssl_utils:weak_ciphers()}
        ],
        connection, []
    ),
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
    case catch cowboy:stop_listener(?PROTO_LISTENER) of
        (ok) ->
            ok;
        (Error) ->
            ?error("Error on stopping listener ~p: ~p",
                [?PROTO_LISTENER, Error]),
            {error, protocol_listener_stop_error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback healthcheck/0.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | {error, server_not_responding}.
healthcheck() ->
    {ok, Timeout} = application:get_env(?CLUSTER_WORKER_APP_NAME,
        nagios_healthcheck_timeout),
    case ssl:connect("127.0.0.1", port(), [{packet, 4}, {active, false}], Timeout) of
        {ok, Sock} ->
            ssl:close(Sock),
            ok;
        _ ->
            {error, server_not_responding}
    end.