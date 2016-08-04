%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module is responsible for REST listener starting and stopping.
%%% @end
%%%--------------------------------------------------------------------
-module(identity_test_listener).
-behaviour(listener_behaviour).
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

% Cowboy listener references
-define(REST_LISTENER, identity_test).


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
    {ok, _RestPort} = application:get_env(?APP_NAME, rest_port),
    6443.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback start/1.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, Reason :: term()}.
start() ->
    {ok, NbAcceptors} = application:get_env(?APP_NAME, rest_number_of_acceptors),

    % Get cert paths
    {ok, KeyFile} = application:get_env(?APP_NAME, identity_key_file),
    {ok, CertFile} = application:get_env(?APP_NAME, identity_cert_file),

    % Setup certs
    Domain = oneprovider:get_oz_domain(),
    identity:ensure_identity_cert_created(KeyFile, CertFile, Domain),
    Cert = identity:read_cert(CertFile),
    ok = identity:publish_to_dht(Cert),

    RestDispatch = [
%%        {'_', rest_router:top_level_routing()}
    ],

    % Start the listener for REST handler
    Result = ranch:start_listener(?REST_LISTENER, NbAcceptors,
        ranch_etls, [
            {ip, {127, 0, 0, 1}},
            {port, port()},
            {certfile, CertFile},
            {keyfile, KeyFile},
            {verify, verify_peer},
            {verify_fun, {fun identity:ssl_verify_fun_impl/3, []}}
        ], cowboy_protocol, [
            {env, [{dispatch, cowboy_router:compile(RestDispatch)}]}
        ]),

%%    Result = cowboy:start_https(?REST_LISTENER, NbAcceptors,
%%        [
%%            {ip, {127, 0, 0, 1}},
%%            {port, port()},
%%            {certfile, CertFile},
%%            {keyfile, KeyFile},
%%            {verify, verify_peer},
%%            {verify_fun, {fun identity:ssl_verify_fun_impl/3, []}}
%%        ],
%%        [
%%            {env, [{dispatch, cowboy_router:compile(RestDispatch)}]}
%%        ]),

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
