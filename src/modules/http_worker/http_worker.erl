%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module implements worker_plugin_behaviour callbacks.
%%% It is responsible for spawning processes which then process HTTP requests.
%%% @end
%%%--------------------------------------------------------------------
-module(http_worker).
-author("Lukasz Opiola").

-behaviour(worker_plugin_behaviour).

-include("proto/common/credentials.hrl").
-include("cluster/worker/modules/datastore/datastore.hrl").
-include_lib("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State :: worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    GrCert = oneprovider:get_globalregistry_cert(),
    identity:save(#document{key = GrCert, value = ?GLOBALREGISTRY_IDENTITY}),
    {ok, #{}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck | {spawn_handler, SocketPid :: pid()},
    Result :: nagios_handler:healthcheck_response() | ok | {ok, Response} |
    {error, Reason} | pong,
    Response :: term(),
    Reason :: term().
handle(ping) ->
    pong;

handle(healthcheck) ->
    Endpoints = [protocol_handler, gui, redirector, rest],
    lists:foldl(
        fun
            (Endpoint, ok) -> healthcheck(Endpoint);
            (_, Error) -> Error
        end, ok, Endpoints);

handle({spawn_handler, SocketPid}) ->
    Pid = spawn(
        fun() ->
            erlang:monitor(process, SocketPid),
            opn_cowboy_bridge:set_socket_pid(SocketPid),
            opn_cowboy_bridge:request_processing_loop()
        end),
    {ok, Pid};

handle(_Request) ->
    ?log_bad_request(_Request).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
cleanup() ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% healthcheck given endpoint
%% @end
%%--------------------------------------------------------------------
-spec healthcheck(Endpoint :: atom()) -> ok | {error, Reason :: atom()}.
healthcheck(protocol_handler) ->
    {ok, ProtocolHandlerPort} = application:get_env(?APP_NAME, protocol_handler_port),
    case ssl:connect("127.0.0.1", ProtocolHandlerPort, [{packet, 4}, {active, false}]) of
        {ok, Sock} ->
            ssl:close(Sock),
            ok;
        _ -> {error, no_protocol_handler}
    end;
healthcheck(gui) ->
    {ok, GuiPort} = application:get_env(?APP_NAME, http_worker_https_port),
    case ibrowse:send_req("https://127.0.0.1:" ++ integer_to_list(GuiPort), [], get) of
        {ok, _, _, _} ->
            ok;
        _ -> {error, no_gui}
    end;
healthcheck(redirector) ->
    {ok, RedirectPort} = application:get_env(?APP_NAME, http_worker_redirect_port),
    case ibrowse:send_req("http://127.0.0.1:" ++ integer_to_list(RedirectPort), [], get) of
        {ok, _, _, _} -> ok;
        _ -> {error, no_http_redirector}
    end;
healthcheck(rest) ->
    {ok, RestPort} = application:get_env(?APP_NAME, http_worker_rest_port),
    case ibrowse:send_req("https://127.0.0.1:" ++ integer_to_list(RestPort), [], get) of
        {ok, _, _, _} -> ok;
        _ -> {error, no_rest}
    end.

