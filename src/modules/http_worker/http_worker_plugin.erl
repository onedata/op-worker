%%%--------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc Enhances http_worker with op-worker specifics.
%%%      Provides (partial) definition of endpoints healthcheck.
%%% @end
%%%--------------------------------------------------------------------
-module(http_worker_plugin).
-author("Michal Zmuda").

-behaviour(http_worker_plugin_behaviour).
-behaviour(endpoint_healthcheck_behaviour).

-include("proto/common/credentials.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% http_worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0, healthcheck_endpoints/0]).

%% endpoint_healthcheck_behaviour callbacks
-export([ healthcheck/1]).

-define(HTTP_WORKER_MODULE, http_worker).

%%%===================================================================
%%% http_worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link http_worker_plugin_behaviour} callback init/1.
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
%% {@link http_worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck | {spawn_handler, SocketPid :: pid()},
    Result :: nagios_handler:healthcheck_response() | ok | {ok, Response} |
    {error, Reason} | pong,
    Response :: term(),
    Reason :: term().

handle(_Request) ->
    ?log_bad_request(_Request).

%%--------------------------------------------------------------------
%% @doc
%% {@link http_worker_plugin_behaviour} callback healthcheck_endpoints/0.
%% @end
%%--------------------------------------------------------------------

-spec healthcheck_endpoints() -> list({Module :: atom(), Endpoint :: atom()}).
healthcheck_endpoints() ->
    [
        {?MODULE, protocol_handler}, {?MODULE, rest}, {?MODULE, gui},
        {?HTTP_WORKER_MODULE, nagios}, {?HTTP_WORKER_MODULE, redirector}
    ].

%%--------------------------------------------------------------------
%% @doc
%% {@link http_worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
cleanup() ->
    ok.

%%%===================================================================
%%% endpoint_healthcheck_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link endpoint_healthcheck_behaviour} callback healthcheck/0
%% @end
%%--------------------------------------------------------------------
-spec healthcheck(Endpoint :: atom()) -> ok | {error, Reason :: atom()}.
healthcheck(protocol_handler) ->
    {ok, ProtoPort} = application:get_env(?APP_NAME, protocol_handler_port),
    case ssl2:connect("127.0.0.1", ProtoPort, [{packet, 4}, {active, false}]) of
        {ok, Sock} ->
            ssl2:close(Sock),
            ok;
        _ ->
            {error, no_protocol_handler}
    end;

healthcheck(gui) ->
    {ok, GuiPort} = application:get_env(?APP_NAME, http_worker_https_port),
    case http_client:get("https://127.0.0.1:" ++ integer_to_list(GuiPort),
        [], <<>>, [insecure]) of
        {ok, _, _, _} ->
            ok;
        _ ->
            {error, no_gui}
    end;

healthcheck(rest) ->
    {ok, RestPort} = application:get_env(?APP_NAME, http_worker_rest_port),
    case http_client:get("https://127.0.0.1:" ++ integer_to_list(RestPort),
        [], <<>>, [insecure]) of
        {ok, _, _, _} ->
            ok;
        _ ->
            {error, no_rest}
    end.

