%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal (persistent) service that maintains an open GraphSync channel
%%% to Onezone. The internal service interface is used in a non-standard way -
%%% this service does not depend on the start_function and manages GS channel
%%% restarts by itself. Healthchecks are essentially used to periodically run
%%% the logic that checks the connection and restarts it as needed.
%%% In case of the channel crash, the connection will be down until the next
%%% healthcheck. The healthchecks are done in short intervals, unless there are
%%% persistent problems with connection - in such case, backoff is applied.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_channel_service).
-author("Lukasz Opiola").

-include("graph_sync/provider_graph_sync.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([setup_internal_service/0]).
-export([is_connected/0]).
-export([force_start_connection/0, force_terminate_connection/0, force_restart_connection/0]).
-export([on_db_and_workers_ready/0]).

%% Internal Service callbacks
-export([start_service/0, stop_service/0, takeover_service/0, healthcheck/1]).

%% By default, the channel waits for the node's clock to be synchronized, which
%% is done by Onepanel. During that time, no connection is attempted. If
%% needed, this mechanism can be turned off using this env variable.
-define(REQUIRE_CLOCK_SYNC_FOR_CONNECTION, application:get_env(
    ?APP_NAME, graph_sync_require_clock_sync_for_connection, true
)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Registers the GS channel service in the internal services manager.
%% @end
%%--------------------------------------------------------------------
-spec setup_internal_service() -> ok.
setup_internal_service() ->
    ok = internal_services_manager:start_service(?MODULE, ?GS_CHANNEL_SERVICE_NAME, ?GS_CHANNEL_SERVICE_NAME, #{
        start_function => start_service,
        stop_function => stop_service,
        takeover_function => takeover_service,
        healthcheck_fun => healthcheck,
        healthcheck_interval => ?GS_RECONNECT_BASE_INTERVAL,
        async_start => true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Returns if the provider is actively connected to Onezone via GraphSync channel.
%% @end
%%--------------------------------------------------------------------
-spec is_connected() -> boolean().
is_connected() ->
    gs_client_worker:is_connected().


%%--------------------------------------------------------------------
%% @doc
%% Immediately triggers Onezone connection attempt (if applicable).
%% Returns boolean indicating if the connection is active after this operation.
%% @end
%%--------------------------------------------------------------------
-spec force_start_connection() -> boolean().
force_start_connection() ->
    case {oneprovider:is_registered(), is_clock_sync_satisfied(), is_connected()} of
        {false, _, _} ->
            ?warning("Ignoring attempt to force start Onezone connection - Oneprovider is not registered."),
            false;
        {true, false, _} ->
            ?warning(
                "Ignoring attempt to force start Onezone connection - the clock has not been yet "
                "synchronized by Onepanel (see op_panel logs for details)."
            ),
            false;
        {true, true, true} ->
            ?info("Ignoring attempt to force start Onezone connection - already started."),
            true;
        {true, true, false} ->
            ResponsibleNode = responsible_node(),
            case node() of
                ResponsibleNode ->
                    ?info("Attempting to start Onezone connection (forced)..."),
                    case start_gs_client_worker() of
                        ok ->
                            internal_services_manager:reschedule_healthcheck(
                                ?MODULE, ?GS_CHANNEL_SERVICE_NAME, ?GS_CHANNEL_SERVICE_NAME,
                                ?GS_RECONNECT_BASE_INTERVAL
                            ),
                            true;
                        error ->
                            false
                    end;
                _OtherNode ->
                    ?info("Attempting to start Onezone connection at node ~p (forced)...", [
                        ResponsibleNode
                    ]),
                    rpc:call(ResponsibleNode, ?MODULE, force_start_connection, [])
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Terminates existing Onezone connection (if any).
%% @end
%%--------------------------------------------------------------------
-spec force_terminate_connection() -> ok | not_started.
force_terminate_connection() ->
    gs_client_worker:force_terminate().


%%--------------------------------------------------------------------
%% @doc
%% Terminates existing Onezone connection (if any).
%% @end
%%--------------------------------------------------------------------
-spec force_restart_connection() -> boolean().
force_restart_connection() ->
    force_terminate_connection(),
    force_start_connection().


%%--------------------------------------------------------------------
%% @doc
%% Callback for when the cluster's DB and workers are initialized and it is
%% possible to run the deferred on-connect procedures. They are not run during
%% the first Onezone connection as it is established before the upgrade_cluster
%% step and initialization of all workers (which are required in the process).
%% @end
%%--------------------------------------------------------------------
-spec on_db_and_workers_ready() -> ok.
on_db_and_workers_ready() ->
    node() =:= responsible_node() andalso is_connected() andalso run_on_connect_to_oz_procedures(),
    ok.

%%%===================================================================
%%% Internal services API
%%%===================================================================

-spec start_service() -> ok.
start_service() ->
    ok. % the GS channel will be started upon the first healthcheck


-spec stop_service() -> ok | no_return().
stop_service() ->
    force_terminate_connection().


-spec takeover_service() -> ok.
takeover_service() ->
    gs_hooks:handle_disconnected_from_oz(),
    start_service().


-spec healthcheck(time:millis()) -> {ok, time:millis()}.
healthcheck(LastInterval) ->
    case {oneprovider:is_registered(), is_clock_sync_satisfied(), is_connected()} of
        {false, _, _} ->
            ?debug("Skipping Onezone connection as the provider is not registered."),
            {ok, calculate_backoff(LastInterval)};
        {true, false, _} ->
            ?debug("Deferring Onezone connection as the clock has not been yet synchronized with Onepanel"),
            utils:throttle(?OZ_CONNECTION_AWAIT_LOG_INTERVAL, fun() ->
                ?info(
                    "Deferring Onezone connection as the clock has not been yet "
                    "synchronized with Onepanel (see op_panel logs for details)."
                )
            end),
            {ok, calculate_backoff(LastInterval)};
        {true, true, true} ->
            gs_hooks:handle_successful_healthcheck(),
            {ok, ?GS_RECONNECT_BASE_INTERVAL};
        {true, true, false} ->
            case start_gs_client_worker() of
                ok ->
                    {ok, ?GS_RECONNECT_BASE_INTERVAL};
                error ->
                    % specific errors are logged in gs_client_worker
                    {ok, calculate_backoff(LastInterval)}
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec calculate_backoff(time:millis()) -> time:millis().
calculate_backoff(LastInterval) ->
    min(?GS_RECONNECT_MAX_BACKOFF, round(LastInterval * ?GS_RECONNECT_BACKOFF_RATE)).


%% @private
-spec is_clock_sync_satisfied() -> boolean().
is_clock_sync_satisfied() ->
    case global_clock:is_synchronized() of
        true ->
            true;
        false ->
            case ?REQUIRE_CLOCK_SYNC_FOR_CONNECTION of
                false ->
                    ?notice("Ignoring unsynchronized clock for Onezone GS connection (forced in config)"),
                    true;
                _ ->
                    false
            end
    end.


%% @private
-spec responsible_node() -> node().
responsible_node() ->
    internal_services_manager:get_processing_node(?GS_CHANNEL_SERVICE_NAME).


%% @private
-spec start_gs_client_worker() -> ok | error.
start_gs_client_worker() ->
    case gs_client_worker:start() of
        ok ->
            case node_manager:get_cluster_status() of
                {error, cluster_not_ready} ->
                    ?info("Deferring on-connect-to-oz procedures as the cluster is not ready yet");
                {ok, _} ->
                    run_on_connect_to_oz_procedures()
            end;
        already_started ->
            ok;
        error ->
            error
    end.


%% @private
-spec run_on_connect_to_oz_procedures() -> ok | error.
run_on_connect_to_oz_procedures() ->
    case gs_hooks:handle_connected_to_oz() of
        ok ->
            ok;
        error ->
            % kill the connection, which will cause a retry during the next healthcheck
            force_terminate_connection(),
            error
    end.
