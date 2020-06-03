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
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_definitions.hrl").

%% node_manager_plugin_behaviour callbacks
-export([installed_cluster_generation/0]).
-export([oldest_known_cluster_generation/0]).
-export([app_name/0, cm_nodes/0, db_nodes/0]).
-export([listeners/0]).
-export([upgrade_essential_workers/0, custom_workers/0]).
-export([before_init/1]).
-export([upgrade_cluster/1]).
-export([on_cluster_ready/0]).
-export([renamed_models/0]).
-export([modules_with_exometer/0, exometer_reporters/0]).
-export([node_down/2, node_up/2, node_ready/2]).

-type model() :: datastore_model:model().
-type record_version() :: datastore_model:record_version().

% When cluster is not in newest generation it will be upgraded during initialization.
% This can be used to e.g. move models between services.
% Oldest known generation is the lowest one that can be directly upgraded to newest.
% Human readable version is included to for logging purposes.
-define(INSTALLED_CLUSTER_GENERATION, 2).
-define(OLDEST_KNOWN_CLUSTER_GENERATION, {1, <<"19.02.*">>}).

%%%===================================================================
%%% node_manager_plugin_default callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:installed_cluster_generation/0}.
%% @end
%%--------------------------------------------------------------------
-spec installed_cluster_generation() -> node_manager:cluster_generation().
installed_cluster_generation() ->
    ?INSTALLED_CLUSTER_GENERATION.

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:oldest_known_cluster_generation/0}.
%% @end
%%--------------------------------------------------------------------
-spec oldest_known_cluster_generation() ->
    {node_manager:cluster_generation(), HumanReadableVersion :: binary()}.
oldest_known_cluster_generation() ->
    ?OLDEST_KNOWN_CLUSTER_GENERATION.

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
%% List of workers modules with configs that should be started before upgrade.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_essential_workers() -> [{module(), [any()]}].
upgrade_essential_workers() -> filter_disabled_workers([
    {gs_worker, [
        {supervisor_flags, gs_worker:supervisor_flags()}
    ]}
]).

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
    ]},
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

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Filters node_manager_plugins that were turned off in app.config
%% @end
%%-------------------------------------------------------------------
-spec filter_disabled_workers(
    [{atom(), [any()]} |{singleton | early_init, atom(), [any()]}]) ->
    [{atom(), [any()]} |{singleton | early_init, atom(), [any()]}].
filter_disabled_workers(WorkersSpecs) ->
    DisabledWorkers = application:get_env(?APP_NAME, disabled_workers, []),
    DisabledWorkersSet = sets:from_list(DisabledWorkers),
    lists:filter(fun
        ({Worker, _WorkerArgs}) ->
            not sets:is_element(Worker, DisabledWorkersSet);
        ({early_init, Worker, _WorkerArgs}) ->
            not sets:is_element(Worker, DisabledWorkersSet);
        ({singleton, Worker, _WorkerArgs}) ->
            not sets:is_element(Worker, DisabledWorkersSet)
    end, WorkersSpecs).

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
%% Overrides {@link node_manager_plugin_default:before_init/1}.
%% This callback is executed on all cluster nodes.
%% @end
%%--------------------------------------------------------------------
-spec before_init(Args :: term()) -> Result :: ok | {error, Reason :: term()}.
before_init([]) ->
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
%% Overrides {@link node_manager_plugin_default:upgrade_cluster/1}.
%% This callback is executed only on one cluster node.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_cluster(node_manager:cluster_generation()) ->
    {ok, node_manager:cluster_generation()}.
upgrade_cluster(1) ->
    storage:migrate_to_zone(),
    {ok, 2}.

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:on_cluster_ready/1}.
%% This callback is executed on all cluster nodes.
%% @end
%%--------------------------------------------------------------------
on_cluster_ready() ->
    fslogic_delete:cleanup_opened_files(),
    space_unsupport:init_pools(),
    gs_worker:on_cluster_ready().

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

%%--------------------------------------------------------------------
%% @doc
%% Callback used to customize behavior in case of other node failure. Second argument
%% informs if failed node is master (see ha_datastore.hrl in cluster_worker) for current node.
%% @end
%%--------------------------------------------------------------------
-spec node_down(FailedNode :: node(), IsFailedNodeMaster :: boolean()) -> ok.
node_down(_FailedNode, true) ->
%%    session_manager:restart_dead_sessions(),
    ok;
node_down(_FailedNode, false) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Callback used to customize behavior when other node recovers after failure.
%% It is called after basic workers (especially datastore) have been restarted.
%% Second argument informs if recovered node is master (see ha_datastore.hrl) for current node.
%% @end
%%--------------------------------------------------------------------
-spec node_up(node(), boolean()) -> ok.
node_up(RecoveredNode, IsRecoveredNodeMaster) ->
    case IsRecoveredNodeMaster of
        true ->
            oneprovider:set_oz_domain(RecoveredNode),
            provider_auth:backup_to_file(RecoveredNode),
            replica_synchronizer:cancel_and_terminate_slaves();
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Callback used to customize behavior when other node recovers after failure.
%% It is called after all workers and listeners have been restarted.
%% Second argument informs if recovered node is master (see ha_datastore.hrl) for current node.
%% @end
%%--------------------------------------------------------------------
-spec node_ready(node(), boolean()) -> ok.
node_ready(_RecoveredNode, IsRecoveredNodeMaster) ->
    case IsRecoveredNodeMaster of
        true ->
            qos_bounded_cache:ensure_exists_for_all_spaces();
        false ->
            ok
    end.