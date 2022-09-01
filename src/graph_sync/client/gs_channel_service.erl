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
-include("http/gui_paths.hrl").
-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([setup_internal_service/0]).
-export([terminate_internal_service/0]).
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

-define(THROTTLE_LOG(Log), utils:throttle(?OZ_CONNECTION_AWAIT_LOG_INTERVAL, fun() -> Log end)).

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


-spec terminate_internal_service() -> ok.
terminate_internal_service() ->
    internal_services_manager:stop_service(?MODULE, ?GS_CHANNEL_SERVICE_NAME, ?GS_CHANNEL_SERVICE_NAME).


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
    case {oneprovider:is_registered(), is_connected()} of
        {false, _} ->
            ?warning("Ignoring attempt to force start Onezone connection - Oneprovider is not registered"),
            false;
        {true, true} ->
            ?info("Ignoring attempt to force start Onezone connection - already started"),
            true;
        {true, false} ->
            ResponsibleNode = responsible_node(),
            case node() of
                ResponsibleNode ->
                    ?info("Attempting to start Onezone connection (forced)..."),
                    case try_to_start_connection() of
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
    case {oneprovider:is_registered(), is_connected()} of
        {false, _} ->
            ?debug("Skipping Onezone connection as the provider is not registered"),
            {ok, calculate_backoff(LastInterval)};
        {true, true} ->
            {ok, ?GS_RECONNECT_BASE_INTERVAL};
        {true, false} ->
            case try_to_start_connection() of
                ok ->
                    {ok, ?GS_RECONNECT_BASE_INTERVAL};
                error ->
                    % specific errors are already logged
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
-spec responsible_node() -> node().
responsible_node() ->
    internal_services_manager:get_processing_node(?GS_CHANNEL_SERVICE_NAME).


%% @private
-spec try_to_start_connection() -> ok | error.
try_to_start_connection() ->
    case check_compatibility_with_onezone() of
        false ->
            error;
        true ->
            case is_clock_sync_satisfied() of
                true ->
                    start_gs_client_worker();
                false ->
                    ?debug("Deferring Onezone connection as the clock has not been yet synchronized with Onepanel"),
                    ?THROTTLE_LOG(?info(
                        "Deferring Onezone connection as the clock has not been yet "
                        "synchronized with Onepanel (see op_panel logs for details)"
                    )),
                    error
            end
    end.


%% @private
-spec start_gs_client_worker() -> ok | error.
start_gs_client_worker() ->
    case gs_client_worker:start() of
        ok ->
            % The on connection procedures require operational db and workers, but
            % the connection may be established before in order to perform an upgrade.
            % In such case, the procedures are deferred and will be called
            % when the 'on_db_and_workers_ready' callback fires.
            case node_manager:are_db_and_workers_ready() of
                false ->
                    ?info("Deferring on-connect-to-oz procedures as DB and workers are not ready yet");
                true ->
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


%% @private
-spec is_clock_sync_satisfied() -> boolean().
is_clock_sync_satisfied() ->
    case global_clock:is_synchronized() of
        true ->
            true;
        false ->
            case ?REQUIRE_CLOCK_SYNC_FOR_CONNECTION of
                false ->
                    ?THROTTLE_LOG(?notice(
                        "Ignoring unsynchronized clock for Onezone GS connection (forced in config)"
                    )),
                    true;
                _ ->
                    false
            end
    end.


%% @private
-spec check_compatibility_with_onezone() -> boolean().
check_compatibility_with_onezone() ->
    case provider_logic:get_service_configuration(onezone) of
        {ok, OzConfiguration} ->
            Resolver = compatibility:build_resolver(consistent_hashing:get_all_nodes(), oneprovider:trusted_ca_certs()),
            check_for_compatibility_registry_updates(Resolver, OzConfiguration),
            OzVersion = maps:get(<<"version">>, OzConfiguration, <<"unknown">>),
            OpVersion = op_worker:get_release_version(),
            case compatibility:check_products_compatibility(Resolver, ?ONEPROVIDER, OpVersion, ?ONEZONE, OzVersion) of
                true ->
                    true;
                {false, CompOzVersions} ->
                    ?THROTTLE_LOG(?critical(
                        "This Oneprovider is not compatible with its Onezone service.~n"
                        "Oneprovider version: ~s, supports zones: ~s~n"
                        "Onezone version: ~s~n"
                        "The service will not be operational until the problem is resolved "
                        "(may require Oneprovider / Onezone upgrade or compatibility registry refresh).", [
                            OpVersion, str_utils:join_binary(CompOzVersions, <<", ">>), OzVersion
                        ])),
                    false;
                {error, Error} ->
                    ?THROTTLE_LOG(?critical(
                        "Cannot check Oneprovider's compatibility due to ~p~n."
                        "The service will not be operational until the problem is resolved.", [
                            {error, Error}
                        ])),
                    false
            end;
        {error, {bad_response, Code, ResponseBody}} ->
            ?THROTTLE_LOG(?critical(
                "Failure while checking Onezone version. "
                "The service will not be operational until the problem is resolved.~n"
                "HTTP response: ~B ~s", [
                    Code, ResponseBody
                ])),
            false;
        {error, _} = Error ->
            ?THROTTLE_LOG(?warning(
                "Onezone is not reachable, is the service online (~ts)? "
                "Last error was: ~w. Retrying as long as it takes...", [oneprovider:get_oz_domain(), Error]
            )),
            false
    end.


%% @private
-spec check_for_compatibility_registry_updates(compatibility:resolver(), json_utils:json_term()) -> ok.
check_for_compatibility_registry_updates(Resolver, OzConfiguration) ->
    case maps:get(<<"compatibilityRegistryRevision">>, OzConfiguration, <<"unknown">>) of
        RemoteRevision when is_integer(RemoteRevision) ->
            LocalRevision = case compatibility:peek_current_registry_revision(Resolver) of
                {ok, R} -> R;
                _ -> 0
            end,
            case RemoteRevision > LocalRevision of
                true ->
                    compatibility:check_for_updates(
                        Resolver, [oneprovider:get_oz_url(?ZONE_COMPATIBILITY_REGISTRY_PATH)]
                    );
                false ->
                    ?debug(
                        "Local compatibility registry (v. ~s) is not older than Onezone's (v. ~s)",
                        [LocalRevision, RemoteRevision]
                    )
            end;
        Other ->
            ?THROTTLE_LOG(?warning(
                "Cannot check Onezone's compatibility registry revision - got '~w'", [Other]
            ))
    end.
