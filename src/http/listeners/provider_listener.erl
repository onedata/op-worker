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
    {ok, CertFile} =
        application:get_env(?APP_NAME, protocol_handler_ssl_cert_path),

    Ip = {0, 0, 0, 0},
    CACerts = lists:map(
        fun(Path) ->
            {ok, Data} = file:read_file(Path),
            Data
        end, [oz_plugin:get_cacert_path()]),

    Result = ranch:start_listener(?TCP_PROTO_LISTENER, DispatcherPoolSize,
        ranch_etls, [
            {ip, Ip},
            {port, Port},
            {certfile, CertFile},
            {cacerts, CACerts},
            {verify_type, verify_peer},
            {fail_if_no_peer_cert, true},
            {ciphers, ssl:cipher_suites() -- weak_ciphers()},
            {versions, ['tlsv1.2', 'tlsv1.1']}
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
    case gen_tcp:connect("127.0.0.1", port(), [{packet, 4}, {active, false}], Timeout) of
        {ok, Sock} ->
            gen_tcp:close(Sock),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns list of weak ciphers.
%% @end
-spec weak_ciphers() -> list().
%%--------------------------------------------------------------------
weak_ciphers() ->
    [{dhe_rsa, des_cbc, sha}, {rsa, des_cbc, sha}].
