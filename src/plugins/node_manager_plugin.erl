%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Plugin which extends node manager for op_worker
%%% @end
%%%-------------------------------------------------------------------
-module(node_manager_plugin).
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include_lib("cluster_worker/include/elements/node_manager/node_manager.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("ctool/include/onedata.hrl").

%% node_manager_plugin_behaviour callbacks
-export([cluster_generations/0]).
-export([oldest_upgradable_cluster_generation/0]).
-export([app_name/0, cm_nodes/0, db_nodes/0]).
-export([before_init/0]).
-export([before_cluster_upgrade/0]).
-export([upgrade_cluster/1]).
-export([custom_workers/0]).
-export([on_db_and_workers_ready/0]).
-export([listeners/0]).
-export([renamed_models/0]).
-export([modules_with_exometer/0, exometer_reporters/0]).
-export([master_node_down/1, master_node_up/1, master_node_ready/1]).

-type model() :: datastore_model:model().
-type record_version() :: datastore_model:record_version().

% List of all known cluster generations.
% When cluster is not in newest generation it will be upgraded during initialization.
% This can be used to e.g. move models between services.
% Oldest upgradable generation is the lowest one that can be directly upgraded to newest.
% Human readable version is included to for logging purposes.
-define(CLUSTER_GENERATIONS, [
    {1, ?LINE_19_02},
    {2, ?LINE_20_02(<<"0-beta3">>)},
    {3, ?LINE_20_02(<<"1">>)},
    {4, oneprovider:get_version()}
]).
-define(OLDEST_UPGRADABLE_CLUSTER_GENERATION, 1).


%%%===================================================================
%%% node_manager_plugin_default callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:cluster_generations/0}.
%% @end
%%--------------------------------------------------------------------
-spec cluster_generations() -> node_manager:cluster_generation().
cluster_generations() ->
    ?CLUSTER_GENERATIONS.

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:oldest_upgradable_cluster_generation/0}.
%% @end
%%--------------------------------------------------------------------
-spec oldest_upgradable_cluster_generation() ->
    {node_manager:cluster_generation(), HumanReadableVersion :: binary()}.
oldest_upgradable_cluster_generation() ->
    ?OLDEST_UPGRADABLE_CLUSTER_GENERATION.

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:app_name/0}.
%% @end
%%--------------------------------------------------------------------
-spec app_name() -> {ok, Name :: atom()}.
app_name() ->
    {ok, op_worker}.

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:cm_nodes/0}.
%% @end
%%--------------------------------------------------------------------
-spec cm_nodes() -> {ok, Nodes :: [atom()]} | undefined.
cm_nodes() ->
    application:get_env(?APP_NAME, cm_nodes).

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:db_nodes/0}.
%% @end
%%--------------------------------------------------------------------
-spec db_nodes() -> {ok, Nodes :: [atom()]} | undefined.
db_nodes() ->
    application:get_env(?APP_NAME, db_nodes).

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:renamed_models/0}.
%% @end
%%--------------------------------------------------------------------
-spec renamed_models() -> #{{record_version(), model()} => model()}.
renamed_models() ->
    #{
        {1, open_file} => file_handles
    }.

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:before_init/0}.
%% This callback is executed on all cluster nodes.
%% @end
%%--------------------------------------------------------------------
-spec before_init() -> ok | {error, Reason :: term()}.
before_init() ->
    try
        op_worker_sup:start_link(),
        ok = helpers_nif:init()
    catch
        _:Error ->
            ?error_stacktrace("Error in node_manager_plugin:before_init: ~p",
                [Error]),
            {error, cannot_start_node_manager_plugin}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Callback executed before cluster upgrade so that any required preparation
%% can be done.
%% @end
%%--------------------------------------------------------------------
-spec before_cluster_upgrade() -> ok.
before_cluster_upgrade() ->
    gs_channel_service:setup_internal_service().

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:upgrade_cluster/1}.
%% This callback is executed only on one cluster node.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_cluster(node_manager:cluster_generation()) ->
    {ok, node_manager:cluster_generation()}.
upgrade_cluster(1) ->
    await_zone_connection_and_run(fun storage:migrate_to_zone/0),
    {ok, 2};
upgrade_cluster(2) ->
    await_zone_connection_and_run(fun storage:migrate_imported_storages_to_zone/0),
    {ok, 3}.

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:custom_workers/0}.
%% @end
%%--------------------------------------------------------------------
-spec custom_workers() -> [{module(), [any()]}].
custom_workers() -> filter_disabled_workers([
    {session_manager_worker, [
        {supervisor_flags, session_manager_worker:supervisor_flags()},
        {supervisor_children_spec, session_manager_worker:supervisor_children_spec()}
    ], [worker_first]},
    {fslogic_worker, []},
    {dbsync_worker, [
        {supervisor_flags, dbsync_worker:supervisor_flags()}
    ]},
    {monitoring_worker, [
        {supervisor_flags, monitoring_worker:supervisor_flags()},
        {supervisor_children_spec, monitoring_worker:supervisor_children_spec()}
    ]},
    {rtransfer_worker, [
        {supervisor_flags, rtransfer_worker:supervisor_flags()},
        {supervisor_children_spec, rtransfer_worker:supervisor_children_spec()}
    ]},
    {storage_sync_worker, []},
    {harvesting_worker, [
        {supervisor_flags, harvesting_worker:supervisor_flags()},
        {supervisor_children_spec, harvesting_worker:supervisor_children_spec()}
    ]},
    {qos_worker, []}
]).

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:on_db_and_workers_ready/1}.
%% This callback is executed on all cluster nodes.
%% @end
%%--------------------------------------------------------------------
on_db_and_workers_ready() ->
    fslogic_delete:cleanup_opened_files(),
    space_unsupport:init_pools(),
    gs_channel_service:on_db_and_workers_ready().

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:listeners/0}.
%% @end
%%--------------------------------------------------------------------
-spec listeners() -> Listeners :: [atom()].
listeners() -> [
    http_listener,
    https_listener
].

%%--------------------------------------------------------------------
%% @doc
%% Returns list of modules that register exometer reporters.
%% @end
%%--------------------------------------------------------------------
-spec modules_with_exometer() -> list().
modules_with_exometer() ->
    [fslogic_worker, helpers, session, event_stream, event].

%%--------------------------------------------------------------------
%% @doc
%% Returns list of exometer reporters.
%% @end
%%--------------------------------------------------------------------
-spec exometer_reporters() -> list().
exometer_reporters() -> [].

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Filters node_manager_plugins that were turned off in app.config
%% @end
%%-------------------------------------------------------------------
-spec filter_disabled_workers(
    [{atom(), [any()]} |{singleton, atom(), [any()]}] | {atom(), [any()], list()}) ->
    [{atom(), [any()]} |{singleton, atom(), [any()]}] | {atom(), [any()], list()}.
filter_disabled_workers(WorkersSpecs) ->
    DisabledWorkers = application:get_env(?APP_NAME, disabled_workers, []),
    DisabledWorkersSet = sets:from_list(DisabledWorkers),
    lists:filter(fun
        ({Worker, _WorkerArgs}) ->
            not sets:is_element(Worker, DisabledWorkersSet);
        ({singleton, Worker, _WorkerArgs}) ->
            not sets:is_element(Worker, DisabledWorkersSet);
        ({Worker, _WorkerArgs, _Options}) ->
            not sets:is_element(Worker, DisabledWorkersSet)
    end, WorkersSpecs).

%%--------------------------------------------------------------------
%% @doc
%% Callback used to customize behavior in case of master node failure.
%% @end
%%--------------------------------------------------------------------
-spec master_node_down(FailedNode :: node()) -> ok.
master_node_down(_FailedNode) ->
    session_manager:restart_dead_sessions().

%%--------------------------------------------------------------------
%% @doc
%% Callback used to customize behavior when master node recovers after failure.
%% It is called after basic workers (especially datastore) have been restarted.
%% @end
%%--------------------------------------------------------------------
-spec master_node_up(node()) -> ok.
master_node_up(RecoveredNode) ->
    oneprovider:replicate_oz_domain_to_node(RecoveredNode),
    provider_auth:backup_to_file(RecoveredNode),
    replica_synchronizer:cancel_and_terminate_slaves().

%%--------------------------------------------------------------------
%% @doc
%% Callback used to customize behavior when master node recovers after failure.
%% It is called after all workers and listeners have been restarted.
%% @end
%%--------------------------------------------------------------------
-spec master_node_ready(node()) -> ok.
master_node_ready(_RecoveredNode) ->
    qos_bounded_cache:ensure_exists_for_all_spaces().


-define(ZONE_CONNECTION_RETRIES, 180).

%% @private
-spec await_zone_connection_and_run(Fun :: fun(() -> ok)) -> ok.
await_zone_connection_and_run(Fun) ->
    ?info("Awaiting Onezone connection..."),
    await_zone_connection_and_run(gs_channel_service:is_connected(), ?ZONE_CONNECTION_RETRIES, Fun).

-spec await_zone_connection_and_run(IsConnectedToZone :: boolean(), Retries :: integer(),
    Fun :: fun(() -> ok)) -> ok.
await_zone_connection_and_run(false, 0, _) ->
    ?critical("Could not establish Onezone connection. Aborting upgrade procedure."),
    throw(?ERROR_NO_CONNECTION_TO_ONEZONE);
await_zone_connection_and_run(false, Retries, Fun) ->
    ?warning("The Onezone connection is down. Next retry in 10 seconds..."),
    timer:sleep(timer:seconds(10)),
    await_zone_connection_and_run(gs_channel_service:is_connected(), Retries - 1, Fun);
await_zone_connection_and_run(true, _, Fun) ->
    Fun().
