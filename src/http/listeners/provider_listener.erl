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
-export([ensure_started/0]).

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
    case oneprovider:is_registered() of
        false ->
            % Do not start the listener if the provider is not registered.
            ok;
        true ->
            {ok, Port} = application:get_env(?APP_NAME, provider_protocol_handler_port),
            {ok, DispatcherPoolSize} =
                application:get_env(?APP_NAME, protocol_handler_pool_size),

            KeyFile = oz_plugin:get_key_file(),
            CertFile = oz_plugin:get_cert_file(),
            OzCaCertDer = cert_utils:load_der(oz_plugin:get_oz_cacert_path()),

            Result = ranch:start_listener(?TCP_PROTO_LISTENER, DispatcherPoolSize,
                ranch_ssl, [
                    {port, Port},
                    {keyfile, KeyFile},
                    {certfile, CertFile},
                    {cacerts, [OzCaCertDer]},
                    {verify, verify_peer},
                    {fail_if_no_peer_cert, true},
                    {ciphers, ssl:cipher_suites() -- ssl_utils:weak_ciphers()}
                ],
                connection, []
            ),
            case Result of
                {ok, _} -> ok;
                _ -> Result
            end
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
    case oneprovider:is_registered() of
        false ->
            % The listener is not started if the provider is not registered.
            ok;
        true ->
            {ok, Timeout} = application:get_env(?CLUSTER_WORKER_APP_NAME,
                nagios_healthcheck_timeout),
            Opts = [
                {certfile, oz_plugin:get_cert_file()},
                {keyfile, oz_plugin:get_key_file()}
            ],
            case ssl:connect("127.0.0.1", port(), Opts, Timeout) of
                {ok, Sock} ->
                    ssl:close(Sock),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Checks if this listener is running and starts it if not.
%% @end
%%--------------------------------------------------------------------
-spec ensure_started() -> ok | {error, Reason :: term()}.
ensure_started() ->
    case healthcheck() of
        ok -> ok;
        _ -> start()
    end.
