%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc provider's protocol listener starting & stopping
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
%%% listener_starter_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link listener_starter_behaviour} callback port/0.
%% @end
%%--------------------------------------------------------------------
-spec port() -> integer().
port() ->
    {ok, Port} = application:get_env(?CLUSTER_WORKER_APP_NAME, provider_protocol_handler_port),
    Port.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_starter_behaviour} callback start/1.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, Reason :: term()}.
start() ->
    {ok, Port} = application:get_env(?APP_NAME, provider_protocol_handler_port),
    {ok, DispatcherPoolSize} =
        application:get_env(?APP_NAME, protocol_handler_pool_size),
    {ok, CertFile} =
        application:get_env(?APP_NAME, protocol_handler_ssl_cert_path),
    Ip = case application:get_env(?APP_NAME, protocol_handler_bind_addr) of
             {ok, loopback} -> {127, 0, 0, 1};
             {ok, all} -> {0, 0, 0, 0}
         end,

    CACerts = lists:map(
        fun(Path) ->
            {ok, Data} = file:read_file(Path),
            Data
        end, [gr_plugin:get_cacert_path()]),

    Result = ranch:start_listener(?TCP_PROTO_LISTENER, DispatcherPoolSize,
        ranch_ssl2, [
            {ip, Ip},
            {port, Port},
            {certfile, CertFile},
            {cacerts, CACerts},
            {verify_type, verify_peer},
            {fail_if_no_peer_cert, true}
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
        (ok) ->
            ok;
        (Error) ->
            ?error("Error on stopping listener ~p: ~p",
                [?TCP_PROTO_LISTENER, Error]),
            {error, protocol_listener_stop_error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns the status of a listener.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | {error, server_not_responding}.
healthcheck() ->
    {ok, ProtoPort} = application:get_env(?APP_NAME, provider_protocol_handler_port),
    case gen_tcp:connect("127.0.0.1", ProtoPort, [{packet, 4}, {active, false}]) of
        {ok, Sock} ->
            gen_tcp:close(Sock),
            ok;
        _ ->
            {error, server_not_responding}
    end.
