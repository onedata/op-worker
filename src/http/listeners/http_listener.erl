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
-module(http_listener).
-author("Lukasz Opiola").

-behaviour(listener_behaviour).

-include("global_definitions.hrl").
-include("http/gui_paths.hrl").
-include_lib("ctool/include/logging.hrl").

% Cowboy listener reference
-define(HTTP_LISTENER, http_listener).

% Listener config
-define(PORT, op_worker:get_env(http_server_port, 80)).
-define(ACCEPTORS_NUM, op_worker:get_env(http_acceptors, 10)).
-define(REQUEST_TIMEOUT, op_worker:get_env(http_request_timeout, timer:seconds(30))).

-define(LE_CHALLENGE_PATH, op_worker:get_env(letsencrypt_challenge_api_prefix,
    "/.well-known/acme-challenge")).
-define(LE_CHALLENGE_ROOT, op_worker:get_env(letsencrypt_challenge_static_root,
    "/tmp/op_worker/http/.well-known/acme-challenge/")).

%% listener_behaviour callbacks
-export([port/0, start/0, stop/0, reload_web_certs/0, healthcheck/0]).
-export([set_response_to_letsencrypt_challenge/2]).

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
    ?info("Starting '~p' server...", [?HTTP_LISTENER]),

    Dispatch = cowboy_router:compile([
        {'_', [
            {?LE_CHALLENGE_PATH ++ "/[...]", cowboy_static, {dir, ?LE_CHALLENGE_ROOT}},
            % TODO VFS-7628 make openfaas respond to https
            {?ATM_TASK_FINISHED_CALLBACK_PATH ++ "[...]", workflow_engine_callback_handler, []},
            {'_', redirector_handler, https_listener:port()}
        ]}
    ]),
    Result = cowboy:start_clear(?HTTP_LISTENER,
        #{
            num_acceptors => ?ACCEPTORS_NUM,
            socket_opts => [
                {ip, any},
                {port, port()}
            ]
        },
        #{
            env => #{dispatch => Dispatch},
            max_keepalive => 1,
            request_timeout => ?REQUEST_TIMEOUT
        }
    ),
    case Result of
        {ok, _} ->
            ?info("Server '~p' started successfully", [?HTTP_LISTENER]);
        _ ->
            ?error("Could not start server '~p' - ~p", [?HTTP_LISTENER, Result]),
            Result
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback stop/0.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok | {error, Reason :: term()}.
stop() ->
    ?info("Stopping '~p' server...", [?HTTP_LISTENER]),

    case cowboy:stop_listener(?HTTP_LISTENER) of
        ok ->
            ?info("Server '~p' stopped", [?HTTP_LISTENER]);
        {error, Error} ->
            ?error("Error on stopping server ~p: ~p", [?HTTP_LISTENER, Error]),
            {error, redirector_stop_error}
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link listener_behaviour} callback reload_web_certs/0.
%% @end
%%--------------------------------------------------------------------
-spec reload_web_certs() -> ok.
reload_web_certs() ->
    ok.


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


%%--------------------------------------------------------------------
%% @doc
%% Writes a file served via HTTP in a directory expected by
%% Let's Encrypt HTTP authorization challenge.
%% @end
%%--------------------------------------------------------------------
-spec set_response_to_letsencrypt_challenge(Name :: file:name_all(), Content :: binary()) ->
    ok | {error, Reason}
    when Reason :: file:posix() | badarg | terminated | system_limit.
set_response_to_letsencrypt_challenge(Name, Content) ->
    Path = filename:join(?LE_CHALLENGE_ROOT, Name),
    case filelib:ensure_dir(Path) of
        ok -> file:write_file(Path, Content);
        {error, Reason} -> {error, Reason}
    end.
