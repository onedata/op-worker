%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module is responsible for starting and stopping
%%% redirector listener that redirects client from HTTP (port 80) to HTTPS.
%%% @end
%%%--------------------------------------------------------------------
-module(redirector_listener).
-author("Lukasz Opiola").

-behaviour(listener_behaviour).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

% Cowboy listener reference
-define(HTTP_REDIRECTOR_LISTENER, http_listener).

% Listener config
-define(PORT, application:get_env(?APP_NAME, http_server_port, 80)).
-define(ACCEPTORS_NUM, application:get_env(?APP_NAME, http_acceptors, 10)).
-define(REQUEST_TIMEOUT, application:get_env(?APP_NAME, http_request_timeout, timer:seconds(30))).

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
    ?PORT.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback start/0.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, Reason :: term()}.
start() ->
    RedirectDispatch = cowboy_router:compile([
        {'_', [
            {'_', redirector_handler, https_listener:port()}
        ]}
    ]),
    Result = cowboy:start_clear(?HTTP_REDIRECTOR_LISTENER,
        [
            {port, port()},
            {num_acceptors, ?ACCEPTORS_NUM}
        ], #{
            env => #{dispatch => RedirectDispatch},
            max_keepalive => 1,
            request_timeout => ?REQUEST_TIMEOUT
        }),
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
    case cowboy:stop_listener(?HTTP_REDIRECTOR_LISTENER) of
        ok ->
            ok;
        {error, Error} ->
            ?error("Error on stopping listener ~p: ~p",
                [?HTTP_REDIRECTOR_LISTENER, Error]),
            {error, redirector_stop_error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback healthcheck/0.
%% @end
%%--------------------------------------------------------------------
-spec healthcheck() -> ok | {error, server_not_responding}.
healthcheck() ->
    Endpoint = str_utils:format_bin("http://127.0.0.1:~B", [port()]),
    case http_client:get(Endpoint) of
        {ok, _, _, _} -> ok;
        _ -> {error, server_not_responding}
    end.
