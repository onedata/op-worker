%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module is responsible for REST listener starting and stopping.
%%% @end
%%%--------------------------------------------------------------------
-module(rest_listener).
-behaviour(listener_behaviour).
-author("Tomasz Lichon").
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

% Cowboy listener references
-define(REST_LISTENER, rest).


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
    {ok, RestPort} = application:get_env(?APP_NAME, rest_port),
    RestPort.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback start/1.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, Reason :: term()}.
start() ->
    {ok, NbAcceptors} =
        application:get_env(?APP_NAME, rest_number_of_acceptors),
    {ok, Timeout} =
        application:get_env(?APP_NAME, rest_socket_timeout_seconds),
    {ok, RestPort} = application:get_env(?APP_NAME, rest_port),
    {ok, Cert} = application:get_env(?APP_NAME, web_ssl_cert_path),

    RestDispatch = [
        {'_', rest_router:top_level_routing()}
    ],

    % Start the listener for REST handler
    Result = ranch:start_listener(?REST_LISTENER, NbAcceptors,
        ranch_etls, [
            {ip, {127, 0, 0, 1}},
            {port, RestPort},
            {certfile, Cert},
            {ciphers, ssl:cipher_suites() -- weak_ciphers()},
            {versions, ['tlsv1.2', 'tlsv1.1']}
        ], cowboy_protocol, [
            {env, [{dispatch, cowboy_router:compile(RestDispatch)}]},
            {max_keepalive, 1},
            {timeout, timer:seconds(Timeout)}
        ]),
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
    case catch cowboy:stop_listener(?REST_LISTENER) of
        (ok) ->
            ok;
        (Error) ->
            ?error("Error on stopping listener ~p: ~p",
                [?REST_LISTENER, Error]),
            {error, rest_stop_error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback healthcheck/0.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | {error, server_not_responding}.
healthcheck() ->
    Endpoint = "https://127.0.0.1:" ++ integer_to_list(port()),
    case http_client:get(Endpoint, [], <<>>, [insecure]) of
        {ok, _, _, _} -> ok;
        _ -> {error, server_not_responding}
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
