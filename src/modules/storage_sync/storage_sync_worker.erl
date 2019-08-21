%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Worker responsible for starting scans of storage_sync mechanism.
%%% Every ?STORAGE_SYNC_CHECK_INTERVAL seconds it lists all locally
%%% supported spaces, checks whether storage_sync mechanism is configured
%%% for this spaces and decides whether scans should be started.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_worker).
-author("Jakub Kudzia").
-behavior(worker_plugin_behaviour).

%%-include("modules/datastore/datastore_models.hrl").
%%-include_lib("cluster_worker/include/elements/worker_host/worker_protocol.hrl").
-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% Interval between successive checks of spaces' storage_sync configuration.
-define(STORAGE_SYNC_CHECK_INTERVAL, application:get_env(?APP_NAME, storage_sync_check_interval, 10)).
-define(PROVIDER_CONNECTED_TO_OZ, provider_connected_to_oz).
-define(SPACES_CHECK, spaces_check).

-define(INIT_ERROR_INTERVAL, timer:seconds(60)).


%% Callbacks
-export([init/1, handle/1, cleanup/0]).

%% API
-export([notify_connection_to_oz/0, is_syncable_s3/1]).

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
    ok = storage_sync_traverse:init_pool(),
    ok = storage_sync_deletion:init_pool(),
    {ok, #{}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck | term(),
    Result :: cluster_status:status() | ok | {ok, Response} |
    {error, Reason} | pong,
    Response :: term(),
    Reason :: term().
handle(ping) ->
    pong;
handle(healthcheck) ->
    ok;
handle(?PROVIDER_CONNECTED_TO_OZ) ->
    init_on_connection_to_oz();
handle(?SPACES_CHECK) ->
    ?debug("Check spaces"),
    check_spaces(),
    schedule_spaces_check();
handle(_Request) ->
    ?log_bad_request(_Request),
    {error, wrong_request}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
cleanup() ->
    storage_sync_traverse:stop_pool(),
%%    storage_sync_deletion:stop_pool(),
    ok.

%%%===================================================================
%%% API functions
%%%===================================================================

-spec notify_connection_to_oz() -> ok.
notify_connection_to_oz() ->
    schedule_init_on_connection_to_oz(0).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec init_on_connection_to_oz() -> ok.
init_on_connection_to_oz() ->
    try
        case reset_stalled_scans() of
            ok ->
                schedule_spaces_check();
            Error = {error, Reason}
                when Reason =:= ?ERROR_NO_CONNECTION_TO_OZ
                orelse Reason =:= ?ERROR_UNREGISTERED_PROVIDER
            ->
                ?error("storage_sync_worker could not initialise due to error ~w", [Error]);
            Error = {error, _} ->
                ?error("storage_sync_worker could not initialise due to unexpected error ~w", [Error]),
                schedule_init_on_connection_to_oz(?INIT_ERROR_INTERVAL)
        end
    catch
        Error2:Reason2 ->
            ?error_stacktrace("storage_sync_worker could not initialise due to: ~w", [{Error2, Reason2}]),
            schedule_init_on_connection_to_oz(?INIT_ERROR_INTERVAL)
    end.

-spec reset_stalled_scans() -> ok | {error, term()}.
reset_stalled_scans() ->
    case provider_logic:get_spaces() of
        {ok, Spaces} ->
            lists:foreach(fun(SpaceId) ->
                {ok, SyncConfigs} = space_strategies:get_sync_configs(SpaceId),
                maps:fold(fun(StorageId, SyncConfig, undefined) ->
                    reset_stalled_scans(SpaceId, StorageId, SyncConfig)
                end, undefined, SyncConfigs)
            end, Spaces);
        Error = {error, _} ->
            Error
    end.

-spec reset_stalled_scans(od_space:id(), storage:id(), space_strategies:sync_config()) -> ok.
reset_stalled_scans(SpaceId, StorageId, SyncConfig) ->
    {ImportEnabled, _ImportConfig} = space_strategies:get_import_details(SyncConfig),
    case ImportEnabled of
        false ->
            ok;
        _ ->
            case storage_sync_monitoring:is_scan_in_progress(SpaceId, StorageId) of
                false -> ok;
                true -> storage_sync_monitoring:reset(SpaceId, StorageId)
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is responsible for checking and optionally starting
%% storage_sync scans in synced spaces supported by this provider.
%% @end
%%--------------------------------------------------------------------
-spec check_spaces() -> ok.
check_spaces() ->
    try
        case provider_logic:get_spaces() of
            {ok, Spaces} ->
                lists:foreach(fun(SpaceId) -> check_space(SpaceId) end, Spaces);
            Error = {error, _} ->
                ?debug("Unable to check space strategies due to: ~p", [Error]),
                Error
        end
    catch
        throw:?ERROR_UNREGISTERED_PROVIDER ->
            ?debug("Unable to check space strategies - unregistered provider");
        _:TReason ->
            ?error_stacktrace("Unable to check space strategies due to: ~p", [TReason])
    end.

-spec check_space(od_space:id()) -> ok.
check_space(SpaceId) ->
    case space_strategies:get_sync_configs(SpaceId) of
        {ok, SyncConfigs} ->
            maps:fold(fun(StorageId, SyncConfig, undefined) ->
                check_storage(SpaceId, StorageId, SyncConfig)
            end, undefined, SyncConfigs);
        _ -> ok
    end.

-spec check_storage(od_space:id(), storage:id(), space_strategies:sync_config()) -> ok.
check_storage(SpaceId, StorageId, SyncConfig) ->
    case is_syncable(StorageId) of
        true -> maybe_start_scan(SpaceId, StorageId, SyncConfig);
        false -> ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is responsible for starting suitable storage_sync scans.
%% @end
%%--------------------------------------------------------------------
-spec maybe_start_scan(od_space:id(), storage:id(), space_strategies:sync_config()) -> ok.
maybe_start_scan(SpaceId, StorageId, SyncConfig) ->
    {ImportEnabled, ImportConfig} = space_strategies:get_import_details(SyncConfig),
    case ImportEnabled of
        false ->
            ok;
        true ->
            storage_sync_monitoring:ensure_created(SpaceId, StorageId),
            case storage_sync_monitoring:get_import_status(SpaceId, StorageId) of
                not_started -> storage_sync_traverse:run_import(SpaceId, StorageId, ImportConfig);
                in_progress -> ok;
                finished -> maybe_start_update_scan(SpaceId, StorageId, SyncConfig)
            end
    end.

-spec maybe_start_update_scan(od_space:id(), storage:id(), space_strategies:sync_config()) -> ok.
maybe_start_update_scan(SpaceId, StorageId, SyncConfig) ->
    {UpdateEnabled, UpdateConfig} = space_strategies:get_update_details(SyncConfig),
    case UpdateEnabled of
        false ->
            ok;
        true ->
            {ScanInterval, UpdateConfig2} = maps:take(scan_interval, UpdateConfig),
            {ok, SSMDoc} = storage_sync_monitoring:get(SpaceId, StorageId),
            case storage_sync_monitoring:get_update_status(SSMDoc) of
                in_progress ->
                    ok;
                not_started ->
                    ImportFinishTime = storage_sync_monitoring:get_import_finish_time(SSMDoc),
                    case ImportFinishTime + ScanInterval < time_utils:cluster_time_seconds() of
                        true -> storage_sync_traverse:run_update(SpaceId, StorageId, UpdateConfig2);
                        false -> ok
                    end;
                finished ->
                    LastUpdateFinishTime = storage_sync_monitoring:get_last_update_finish_time(SSMDoc),
                    case LastUpdateFinishTime + ScanInterval < time_utils:cluster_time_seconds() of
                        true -> storage_sync_traverse:run_update(SpaceId, StorageId, UpdateConfig2);
                        false -> ok
                    end
            end
    end.


-spec is_syncable(storage:doc() | storage:id()) -> boolean().
is_syncable(StorageDoc = #document{value = #storage{}}) ->
    Helper = storage:get_helper(StorageDoc),
    HelperName = helper:get_name(Helper),
    case lists:member(HelperName,
        [?POSIX_HELPER_NAME, ?GLUSTERFS_HELPER_NAME, ?NULL_DEVICE_HELPER_NAME, ?WEBDAV_HELPER_NAME])
    of
        true ->
            true;
        false ->
            is_syncable_s3(StorageDoc)
    end;
is_syncable(StorageId) when is_binary(StorageId) ->
    case storage:get(StorageId) of
        {ok, StorageDoc} ->
            is_syncable(StorageDoc);
        _ ->
            false
    end.

is_syncable_s3(StorageDoc = #document{value = #storage{}}) ->
    Helper = storage:get_helper(StorageDoc),
    HelperName = helper:get_name(Helper),
    StoragePathType = helper:get_storage_path_type(Helper),
    Args = helper:get_args(Helper),
    (HelperName =:= ?S3_HELPER_NAME)
        andalso (StoragePathType =:= ?CANONICAL_STORAGE_PATH)
        andalso (<<"0">> =:= maps:get(<<"blockSize">>, Args, undefined)).

-spec schedule_init_on_connection_to_oz(non_neg_integer()) -> ok.
schedule_init_on_connection_to_oz(Interval) ->
    schedule(Interval, ?PROVIDER_CONNECTED_TO_OZ).

-spec schedule_spaces_check() -> ok.
schedule_spaces_check() ->
    schedule(?STORAGE_SYNC_CHECK_INTERVAL, ?SPACES_CHECK).

-spec schedule(non_neg_integer(), term()) -> ok.
schedule(IntervalSeconds, Request) ->
    {ok, _} = timer:apply_after(timer:seconds(IntervalSeconds), worker_proxy, cast, [?MODULE, Request]),
    ok.