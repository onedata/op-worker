%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for starting and monitoring a global
%%% gs_client_worker instance that maintains Graph Sync channel with Onezone.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_worker).
-author("Lukasz Opiola").

-behaviour(worker_plugin_behaviour).

-include("graph_sync/provider_graph_sync.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%% API
-export([is_connected/0, force_connection_start/0]).
-export([terminate_connection/0, restart_connection/0]).
-export([on_db_and_workers_ready/0]).
-export([supervisor_flags/0]).
%% Internal services API
-export([start_gs_client_worker/0, stop_gs_client_worker/0, takeover_gs_client_worker/0, connection_healthcheck/1]).

-define(GS_WORKER_SUP, gs_worker_sup).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns supervisor flags for gs_worker.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_flags() -> supervisor:sup_flags().
supervisor_flags() ->
    #{strategy => one_for_one, intensity => 0, period => 1}.


%%--------------------------------------------------------------------
%% @doc
%% Predicate saying if the provider is actively connected to Onezone via
%% GraphSync channel.
%% @end
%%--------------------------------------------------------------------
is_connected() ->
    worker_proxy:call(?MODULE, is_connected).


%%--------------------------------------------------------------------
%% @doc
%% Immediately triggers onezone connection attempt.
%% Returns boolean indicating success.
%% @end
%%--------------------------------------------------------------------
-spec force_connection_start() -> boolean() | no_return().
force_connection_start() ->
    ?info("Attempting to start connection to Onezone (forced)"),
    Node = get_gs_client_node(),
    case rpc:call(Node, worker_proxy, call, [?MODULE, ensure_connected]) of
        % throw on rpc error
        Result when is_boolean(Result) -> Result
    end.


%%--------------------------------------------------------------------
%% @doc
%% Terminates existing Onezone connection (if any).
%% @end
%%--------------------------------------------------------------------
-spec terminate_connection() -> ok.
terminate_connection() ->
    ?info("Terminating connection to Onezone (forced)"),
    gs_client_worker:force_terminate().


%%--------------------------------------------------------------------
%% @doc
%% Kills existing Onezone connection (if any) and starts a new one.
%% @end
%%--------------------------------------------------------------------
-spec restart_connection() -> ok.
restart_connection() ->
    terminate_connection(),
    force_connection_start(),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Callback for when the cluster's DB and workers are initialized and it is
%% possible to run the deferred on-connect procedures. They are not run during
%% the first connection to Onezone as is is established in the upgrade_cluster
%% step, before initialization of all workers (which are required in the process).
%% @end
%%--------------------------------------------------------------------
-spec on_db_and_workers_ready() -> ok.
on_db_and_workers_ready() ->
    case {node() == get_gs_client_node(), is_connected()} of
        {true, true} ->
            try
                run_on_connect_to_oz_procedures()
            catch
                % Connection was lost in meantime. Ignore it as `on_connect_to_oz`
                % will be called when connection is established again.
                _:{_, ?ERROR_NO_CONNECTION_TO_ONEZONE} -> ok
            end;
        _ ->
            ok
    end.

%%%===================================================================
%%% Internal services API
%%%===================================================================

-spec start_gs_client_worker() -> ok | no_return().
start_gs_client_worker() ->
    case supervisor:start_child(?GS_WORKER_SUP, gs_client_worker_spec()) of
        {ok, _} ->
            try
                case node_manager:get_cluster_status() of
                    {error, cluster_not_ready} ->
                        ?info("Deferring on-connect-to-oz procedures as the cluster is not ready yet");
                    {ok, _} ->
                        run_on_connect_to_oz_procedures()
                end,
                ok
            catch Type:Reason ->
                ?error_stacktrace(
                    "Failed to execute on-connect procedures, disconnecting - ~p:~p",
                    [Type, Reason]
                ),
                % Kill the connection to Onezone, which will cause a
                % connection retry during the next healthcheck
                gs_client_worker:force_terminate()
            end;
        {error, Error} ->
            ?error("Failed to start gs client worker supervisor child: ~p", [Error]),
            ok % Ignore error - will be tried again during next healthcheck
    end.

-spec stop_gs_client_worker() -> ok | no_return().
stop_gs_client_worker() ->
    supervisor:terminate_child(?GS_WORKER_SUP, ?GS_CLIENT_WORKER_GLOBAL_NAME),
    supervisor:delete_child(?GS_WORKER_SUP, ?GS_CLIENT_WORKER_GLOBAL_NAME),
    ok.

-spec takeover_gs_client_worker() -> ok | no_return().
takeover_gs_client_worker() ->
    oneprovider:on_disconnect_from_oz(),
    start_gs_client_worker().

%%--------------------------------------------------------------------
%% @doc
%% Checks if gs_client_worker instance is up and running and in case it's not, starts the worker.
%% @end
%%--------------------------------------------------------------------
-spec connection_healthcheck(non_neg_integer()) -> {ok | restart, non_neg_integer()}.
connection_healthcheck(LastInterval) ->
    case get_connection_status() of
        unregistered ->
            ?debug("Checking connection to Onezone - the provider is not registered."),
            {restart, ?GS_HEALTHCHECK_INTERVAL};
        alive ->
            {ok, ?GS_HEALTHCHECK_INTERVAL};
        not_started ->
            Interval2 = min(
                ?GS_RECONNECT_MAX_BACKOFF,
                round(LastInterval * ?GS_RECONNECT_BACKOFF_RATE)
            ),
            ?info("Checking connection to Onezone - next Onezone connection attempt in ~B seconds.", [
                Interval2 div 1000
            ]),
            {restart, Interval2}
    end.

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    ServiceOptions = #{
        start_function => start_gs_client_worker,
        stop_function => stop_gs_client_worker,
        takeover_function => takeover_gs_client_worker,
        healthcheck_fun => connection_healthcheck,
        async_start => true
    },
    ok = internal_services_manager:start_service(?MODULE, ?GS_CLIENT_WORKER_GLOBAL_NAME_BIN,
        ?GS_CLIENT_WORKER_GLOBAL_NAME_BIN, ServiceOptions),
    {ok, #{}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(ping | healthcheck) -> pong | ok.
handle(ping) ->
    pong;
handle(healthcheck) ->
    ok;
handle(ensure_connected) ->
    try
        case get_connection_status() of
            not_started -> ok =:= start_gs_client_worker();
            Status -> alive =:= Status
        end
    catch
        Type:Reason ->
            ?error_stacktrace(
                "Failed to start connection to Onezone due to ~p:~p",
                [Type, Reason]
            ),
            false
    end;
handle(is_connected) ->
    case get_connection_status() of
        alive -> true;
        _ -> false
    end;
handle(Request) ->
    ?log_bad_request(Request).


%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> ok | {error, timeout | term()}.
cleanup() ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks connection status on the current node.
%% Status skipped indicates that another node is responsible for
%% the graph sync connection.
%% @end
%%--------------------------------------------------------------------
-spec get_connection_status() -> unregistered | not_started | alive.
get_connection_status() ->
    IsRegistered = provider_auth:is_registered(),
    Pid = global:whereis_name(?GS_CLIENT_WORKER_GLOBAL_NAME),
    case {IsRegistered, Pid} of
        {false, _} -> unregistered;
        {true, undefined} -> not_started;
        {true, Pid} when is_pid(Pid) -> alive
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns node on which gs_client_worker should be running.
%% @end
%%--------------------------------------------------------------------
get_gs_client_node() ->
    internal_services_manager:get_processing_node(?GS_CLIENT_WORKER_GLOBAL_NAME_BIN).

%% @private
-spec run_on_connect_to_oz_procedures() -> ok.
run_on_connect_to_oz_procedures() ->
    ?info("Executing on-connect-to-oz procedures..."),
    oneprovider:on_connect_to_oz(),
    ?info("Successfully executed on-connect-to-oz procedures").


%% @private
-spec gs_client_worker_spec() -> supervisor:child_spec().
gs_client_worker_spec() -> #{
    id => ?GS_CLIENT_WORKER_GLOBAL_NAME,
    start => {gs_client_worker, start_link, []},
    restart => temporary,
    shutdown => timer:seconds(10),
    type => worker,
    modules => [gs_client_worker]
}.
