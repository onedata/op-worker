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
-export([app_name/0, cm_nodes/0, db_nodes/0]).
-export([listeners/0, modules_with_args/0]).
-export([before_init/1, on_cluster_initialized/1]).
-export([handle_cast/2]).
-export([renamed_models/0]).
-export([modules_with_exometer/0, exometer_reporters/0]).

-type model() :: datastore_model:model().
-type record_version() :: datastore_model:record_version().

-type state() :: #state{}.

%%%===================================================================
%%% node_manager_plugin_behaviour callbacks
%%%===================================================================

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
listeners() -> node_manager:cluster_worker_listeners() ++ [
    gui_listener
].

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:modules_with_args/0}.
%% @end
%%--------------------------------------------------------------------
-spec modules_with_args() -> Models :: [{atom(), [any()]}].
modules_with_args() -> filter_disabled_workers([
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
    {gs_worker, [
        {supervisor_flags, gs_worker:supervisor_flags()}
    ]},
    {fslogic_deletion_worker, []},
    {singleton, rtransfer_worker, [
        {supervisor_flags, rtransfer_worker:supervisor_flags()},
        {supervisor_children_spec, rtransfer_worker:supervisor_children_spec()}
    ]},
    {space_sync_worker, []}
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
    #{{1, open_file} => file_handles}.

%%--------------------------------------------------------------------
%% @doc
%% Overrides {@link node_manager_plugin_default:before_init/1}.
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
%% This callback is executed when the cluster has been initialized, i.e. all
%% nodes have connected to cluster manager.
%% @end
%%--------------------------------------------------------------------
-spec on_cluster_initialized(Nodes :: [node()]) -> Result :: ok | {error, Reason :: term()}.
on_cluster_initialized(_Nodes) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Overrides {@link node_manager_plugin_default:handle_cast/2}.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast(update_subdomain_delegation_ips, State) ->
    % This cast will be usually used only after acquiring connection
    % to onezone in order to send current cluster IPs
    case provider_logic:update_subdomain_delegation_ips() of
        ok ->
            ok;
        error ->
            % Kill the connection to Onezone in case provider IPs cannot be
            % updated, which will cause a reconnection and update retry.
            gen_server2:call({global, ?GS_CLIENT_WORKER_GLOBAL_NAME},
                {terminate, normal})
    end,
    {noreply, State};

handle_cast(restart_listeners, State) ->
    Modules = [
        gui_listener,
        nagios_listener,
        hackney,
        ssl
    ],
    lists:foreach(fun(Module) ->
        Module:stop()
    end, Modules),
    lists:foreach(fun(Module) ->
        ok = Module:start()
    end, lists:reverse(Modules)),
    % As ssl was restarted, connection to Onezone must be restarted too.
    gs_worker:restart_connection(),
    {noreply, State};

handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of modules that register exometer reporters.
%% @end
%%--------------------------------------------------------------------
-spec modules_with_exometer() -> list().
modules_with_exometer() ->
    [storage_sync_monitoring, fslogic_worker, helpers, session, router,
        event_stream, event].

%%--------------------------------------------------------------------
%% @doc
%% Returns list of exometer reporters.
%% @end
%%--------------------------------------------------------------------
-spec exometer_reporters() -> list().
exometer_reporters() ->
    [{exometer_report_rrd_ets, storage_sync_monitoring}].
