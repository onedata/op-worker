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
-include("modules/dataset/dataset.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_definitions.hrl").
-include_lib("ctool/include/onedata.hrl").

%% node_manager_plugin_behaviour callbacks
-export([cluster_generations/0]).
-export([oldest_upgradable_cluster_generation/0]).
-export([app_name/0, release_version/0, build_version/0]).
-export([cm_nodes/0, db_nodes/0]).
-export([before_init/0]).
-export([before_custom_workers_start/0]).
-export([custom_workers/0]).
-export([before_cluster_upgrade/0]).
-export([upgrade_cluster/1]).
-export([before_listeners_start/0, after_listeners_stop/0]).
-export([listeners/0]).
-export([renamed_models/0]).
-export([modules_with_exometer/0, exometer_reporters/0]).
-export([master_node_down/1, master_node_up/1, master_node_ready/1]).
-export([init_etses_for_space_on_all_nodes/1]).

% For rpc
-export([init_etses_for_space_on_current_node/1, init_etses_for_space_internal/1]).

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
    {4, ?LINE_21_02(<<"2">>)},
    {5, ?LINE_21_02(<<"3">>)},
    {6, ?LINE_21_02(<<"5">>)},
    {7, op_worker:get_release_version()}
]).
-define(OLDEST_UPGRADABLE_CLUSTER_GENERATION, 3).


%%%===================================================================
%%% node_manager_plugin_default callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:cluster_generations/0}.
%% @end
%%--------------------------------------------------------------------
-spec cluster_generations() ->
    [{node_manager:cluster_generation(), onedata:release_version()}].
cluster_generations() ->
    ?CLUSTER_GENERATIONS.

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:oldest_upgradable_cluster_generation/0}.
%% @end
%%--------------------------------------------------------------------
-spec oldest_upgradable_cluster_generation() ->
    node_manager:cluster_generation().
oldest_upgradable_cluster_generation() ->
    ?OLDEST_UPGRADABLE_CLUSTER_GENERATION.

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:app_name/0}.
%% @end
%%--------------------------------------------------------------------
-spec app_name() -> atom().
app_name() ->
    op_worker.

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:release_version/0}.
%% @end
%%--------------------------------------------------------------------
-spec release_version() -> string() | binary().
release_version() ->
    op_worker:get_release_version().

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:app_name/0}.
%% @end
%%--------------------------------------------------------------------
-spec build_version() -> string() | binary().
build_version() ->
    op_worker:get_build_version().

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
        _:Error:Stacktrace ->
            ?error_stacktrace("Error in node_manager_plugin:before_init: ~p", [Error], Stacktrace),
            {error, cannot_start_node_manager_plugin}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Callback executed before custom workers start so that any required preparation
%% can be done.
%%
%% This callback is executed on all cluster nodes.
%% @end
%%--------------------------------------------------------------------
-spec before_custom_workers_start() -> ok.
before_custom_workers_start() ->
    init_etses_on_current_node().


%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:custom_workers/0}.
%% @end
%%--------------------------------------------------------------------
-spec custom_workers() ->
    [{atom(), [any()]} | {singleton, atom(), [any()]} | {atom(), [any()], list()}].
custom_workers() -> filter_disabled_workers([
    {dir_stats_service_worker, [
        {supervisor_flags, pes:get_root_supervisor_flags(dir_stats_collector)},
        {supervisor_children_spec, pes:get_root_supervisor_child_specs(dir_stats_collector)}
    ]},
    {session_manager_worker, [
        {supervisor_flags, session_manager_worker:supervisor_flags()},
        {supervisor_children_spec, session_manager_worker:supervisor_children_spec()}
    ], [worker_first]},
    {fslogic_worker, [
        {supervisor_flags, fslogic_worker:supervisor_flags()},
        {supervisor_children_spec, fslogic_worker:supervisor_children_spec()}
    ]},
    {qos_worker, []},
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
    {auto_storage_import_worker, []},
    {harvesting_worker, [
        {supervisor_flags, harvesting_worker:supervisor_flags()},
        {supervisor_children_spec, harvesting_worker:supervisor_children_spec()}
    ]},
    {middleware_worker, []},
    {provider_rpc_worker, []},
    {atm_supervision_worker, [
        {supervisor_flags, atm_supervision_worker:supervisor_flags()},
        {supervisor_children_spec, atm_supervision_worker:supervisor_children_spec()}
    ], [{terminate_timeout, infinity}]}
]).


%%--------------------------------------------------------------------
%% @doc
%% Callback executed before cluster upgrade so that any required preparation
%% can be done.
%%
%% This callback is executed on all cluster nodes.
%% @end
%%--------------------------------------------------------------------
-spec before_cluster_upgrade() -> ok.
before_cluster_upgrade() ->
    safe_mode:whitelist_pid(self()),
    gs_channel_service:setup_internal_service().


%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:upgrade_cluster/1}.
%% This callback is executed only on one cluster node.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_cluster(node_manager:cluster_generation()) ->
    {ok, node_manager:cluster_generation()}.
upgrade_cluster(3) ->
    % Upgrade is performed by spawned process, so it also needs to be whitelisted by safe mode.
    safe_mode:whitelist_pid(self()),
    await_zone_connection_and_run(fun storage_import:migrate_space_strategies/0),
    await_zone_connection_and_run(fun storage_import:migrate_storage_sync_monitoring/0),
    {ok, 4};
upgrade_cluster(4) ->
    % Upgrade is performed by spawned process, so it also needs to be whitelisted by safe mode.
    safe_mode:whitelist_pid(self()),
    await_zone_connection_and_run(fun() ->
        {ok, SpaceIds} = provider_logic:get_spaces(),

        lists:foreach(fun(SpaceId) ->
            case file_meta:ensure_tmp_dir_exists(SpaceId) of
                created -> ?info("Created tmp dir for space '~s'.", [SpaceId]);
                already_exists -> ok
            end
        end, SpaceIds)
    end),
    {ok, 5};
upgrade_cluster(5) ->
    % Upgrade is performed by spawned process, so it also needs to be whitelisted by safe mode.
    safe_mode:whitelist_pid(self()),
    await_zone_connection_and_run(fun() ->
        {ok, SpaceIds} = provider_logic:get_spaces(),
        lists:foreach(fun(SpaceId) -> init_etses_for_space_on_all_nodes(SpaceId) end, SpaceIds),
        ?info("Upgrading tmp directory links..."),
        % NOTE: existence of tmp directory was ensured in previous version upgrade, but we still need to ensure,
        % that link exists (it was not ensured then).
        lists:foreach(fun file_meta:ensure_tmp_dir_link_exists/1, SpaceIds),
        % NOTE: there is no link for trash dir, so there is no need to ensure it existence.
        ?info("Upgrading trash directories..."),
        lists:foreach(fun trash:ensure_exists/1, SpaceIds),
        ?info("Upgrading archive root directories..."),
        % NOTE: below function also ensures existence of archives root link.
        lists:foreach(fun archivisation_tree:ensure_archives_root_dir_exists/1, SpaceIds),
        % NOTE: there is no need to ensure dataset directory existence, as any operation requiring
        % it will create it if it is not yet synced.
        lists:foreach(fun(SpaceId) ->
            ?info("Upgrading dataset directory links for space '~s'...", [SpaceId]),
            ok = datasets_structure:apply_to_all_datasets(SpaceId, ?ATTACHED_DATASETS_STRUCTURE, fun(DatasetId) ->
                archivisation_tree:ensure_dataset_root_link_exists(DatasetId, SpaceId) end),
            ok = datasets_structure:apply_to_all_datasets(SpaceId, ?DETACHED_DATASETS_STRUCTURE, fun(DatasetId) ->
                archivisation_tree:ensure_dataset_root_link_exists(DatasetId, SpaceId) end)
        end, SpaceIds),
        lists:foreach(fun(SpaceId) ->
            % NOTE: this dir is local in tmp dir, so there is no need to ensure its link existence.
            ?info("Creating directory for opened deleted files for space '~s'...", [SpaceId]),
            file_meta:ensure_opened_deleted_files_dir_exists(SpaceId)
        end, SpaceIds),
        lists:foreach(fun dir_stats_service_state:reinitialize_stats_for_space/1, SpaceIds)
    end),
    {ok, 6};
upgrade_cluster(6) ->
    % Upgrade is performed by spawned process, so it also needs to be whitelisted by safe mode.
    safe_mode:whitelist_pid(self()),
    await_zone_connection_and_run(fun() ->
        {ok, SpaceIds} = provider_logic:get_spaces(),
        % Allow for file links reconciliation traverses to be run again - due to a bug in previous versions it
        % could have been accidentally cancelled.
        lists:foreach(fun file_links_reconciliation_traverse:mark_traverse_needed_for_space/1, SpaceIds)
    end),
    {ok, 7}.


%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:before_listeners_start/0}.
%%
%% NOTE: this callback blocks the application supervisor and must not be used to
%% interact with the main supervision tree.
%%
%% This callback is executed on all cluster nodes.
%% @end
%%--------------------------------------------------------------------
before_listeners_start() ->
    middleware:load_known_atoms(),
    fslogic_delete:cleanup_opened_files(),
    space_unsupport:init_pools(),
    file_upload_manager_watcher_service:setup_internal_service(),
    atm_warden_service:setup_internal_service(),
    atm_workflow_execution_api:init_engine(),
    gs_channel_service:trigger_pending_on_connect_to_oz_procedures().

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:after_listeners_stop/0}.
%%
%% NOTE: this callback blocks the application supervisor and must not be used to
%% interact with the main supervision tree.
%%
%% This callback is executed on all cluster nodes.
%% @end
%%--------------------------------------------------------------------
after_listeners_stop() ->
    atm_supervision_worker:try_to_gracefully_stop_atm_workflow_executions(),
    atm_warden_service:terminate_internal_service(),
    file_upload_manager_watcher_service:terminate_internal_service(),
    % GS connection should be closed at the end as other services
    % may still require access to synced documents
    % (though a working connection cannot be guaranteed here).
    gs_channel_service:terminate_internal_service().

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
    [{atom(), [any()]} | {singleton, atom(), [any()]} | {atom(), [any()], list()}]) ->
    [{atom(), [any()]} | {singleton, atom(), [any()]} | {atom(), [any()], list()}].
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
    session_manager:restart_dead_sessions(),
    process_handles:release_all_dead_processes_handles().

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
    init_etses_for_space_on_current_node(all).


-spec init_etses_for_space_on_all_nodes(od_space:id() | all) -> ok.
init_etses_for_space_on_all_nodes(SpaceId) ->
    Nodes = consistent_hashing:get_all_nodes(),
    {Res, BadNodes} = utils:rpc_multicall(Nodes, ?MODULE, init_etses_for_space_on_current_node, [SpaceId]),
    case BadNodes of
        [] ->
            ok;
        _ ->
            ?error("Could not initialize etses for space ~p on nodes: ~w (RPC error)", [SpaceId, BadNodes]),
            error({etses_not_ready, BadNodes})
    end,
    lists:foreach(fun
        (ok) ->
            ok;
        ({badrpc, _} = Error) ->
            ?error("Could not initialize etses for space: ~p.~nReason: ~p", [SpaceId, Error]),
            error({etses_not_ready, Error})
    end, Res).


%% @private
-spec init_etses_on_current_node() -> ok.
init_etses_on_current_node() ->
    % TODO VFS-7412 refactor effective_value cache
    auto_storage_import_worker:init_ets(),
    paths_cache:init_group(),
    dataset_eff_cache:init_group(),
    file_meta_sync_status_cache:init_group(),
    archive_recall_cache:init_group(),
    permissions_cache:init_group(),
    qos_eff_cache:init_group().


%% @private
-spec init_etses_for_space_on_current_node(od_space:id() | all) -> ok.
init_etses_for_space_on_current_node(SpaceId) ->
    gen_server2:call(?NODE_MANAGER_NAME, {apply, ?MODULE, init_etses_for_space_internal, [SpaceId]}).


%% @private
-spec init_etses_for_space_internal(od_space:id() | all) -> ok.
init_etses_for_space_internal(Space) ->
    paths_cache:init(Space),
    dataset_eff_cache:init(Space),
    archive_recall_cache:init(Space),
    qos_eff_cache:init(Space),
    file_meta_sync_status_cache:init(Space).


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