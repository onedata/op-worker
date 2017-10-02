%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module is responsible for providers' protocol listener starting
%%% and stopping.
%%% @end
%%%--------------------------------------------------------------------
-module(provider_listener).
-behaviour(listener_behaviour).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

% Cowboy listener references
-define(TCP_PROTO_LISTENER, tcp_proto_provider).


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
    {ok, Port} = application:get_env(?APP_NAME, provider_protocol_handler_port),
    Port.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback start/0.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, Reason :: term()}.
start() ->
    {ok, Port} = application:get_env(?APP_NAME, provider_protocol_handler_port),
    {ok, DispatcherPoolSize} =
        application:get_env(?APP_NAME, protocol_handler_pool_size),
    {ok, KeyFile} =
        application:get_env(?APP_NAME, protocol_handler_ssl_key_file),
    {ok, CertFile} =
        application:get_env(?APP_NAME, protocol_handler_ssl_cert_file),
    {ok, CaCertsDir} = application:get_env(?APP_NAME, cacerts_dir),
    {ok, CaCertPems} = file_utils:read_files({dir, CaCertsDir}),
    CaCerts = lists:map(fun cert_decoder:pem_to_der/1, CaCertPems),

    Result = ranch:start_listener(?TCP_PROTO_LISTENER, DispatcherPoolSize,
        ranch_ssl, [
            {port, Port},
            {keyfile, KeyFile},
            {certfile, CertFile},
            {cacerts, CaCerts},
            {verify, verify_peer},
            {fail_if_no_peer_cert, true},
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
    case catch cowboy:stop_listener(?TCP_PROTO_LISTENER) of
        (ok) ->
            ok;
        (Error) ->
            ?error("Error on stopping listener ~p: ~p",
                [?TCP_PROTO_LISTENER, Error]),
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
    KeyFile = oz_plugin:get_key_file(),

    case file:read_file(KeyFile) of
        {ok, _} ->
            case ssl:connect("127.0.0.1", port(), [{certfile, oz_plugin:get_cert_file()},
                {keyfile, KeyFile}], Timeout) of
                {ok, Sock} ->
                    ssl:close(Sock),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, enoent} ->
            ok; % single provider test environment
        Error ->
            Error
    end.