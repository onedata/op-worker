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
%%% For more info on storage_sync please see storage_sync_traverse.erl.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_worker).
-author("Jakub Kudzia").
-behavior(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% Interval between successive checks of spaces' storage_sync configuration.
-define(STORAGE_SYNC_CHECK_INTERVAL, application:get_env(?APP_NAME, storage_sync_check_interval, 10)).
-define(PROVIDER_CONNECTED_TO_OZ, provider_connected_to_oz).
-define(SPACES_CHECK, spaces_check).

-define(INIT_ERROR_INTERVAL, timer:seconds(60)).


%% Callbacks
-export([init/1, handle/1, cleanup/0]).

%% API
-export([notify_connection_to_oz/0]).

%% Exported for tests
-export([schedule_spaces_check/1]).

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
    schedule_spaces_check();
handle(?SPACES_CHECK) ->
    ?debug("Check spaces"),
    check_spaces();
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
    ok.

%%%===================================================================
%%% API functions
%%%===================================================================

-spec notify_connection_to_oz() -> ok.
notify_connection_to_oz() ->
    schedule_spaces_check().

%%%===================================================================
%%% Internal functions
%%%===================================================================

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
                lists:foreach(fun(SpaceId) -> check_space(SpaceId) end, Spaces),
                schedule_spaces_check();
            ?ERROR_NO_CONNECTION_TO_ONEZONE ->
                ?warning("storage_sync_worker was unable to check spaces due to no connection to oz");
            ?ERROR_UNREGISTERED_ONEPROVIDER ->
                ?warning("storage_sync_worker was unable to check spaces due to unregistered provider");
            {error, _} = Error ->
                ?error("storage_sync_worker was unable to check spaces due to unexpected ~p", [Error]),
                schedule_spaces_check()
        end
    catch
        Error2:Reason ->
            ?error_stacktrace("storage_sync_worker was unable to check spaces due to unexpected ~p:~p", [Error2, Reason]),
            schedule_spaces_check()
    end.

-spec check_space(od_space:id()) -> ok.
check_space(SpaceId) ->
    {ok, SyncConfigs} = space_strategies:get_sync_configs(SpaceId),
    maps:fold(fun(StorageId, SyncConfig, undefined) ->
        check_storage(SpaceId, StorageId, SyncConfig)
    end, undefined, SyncConfigs).

-spec check_storage(od_space:id(), storage:id(), space_strategies:sync_config()) -> ok.
check_storage(SpaceId, StorageId, SyncConfig) ->
    case is_sync_supported_on(StorageId) of
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
                    case ImportFinishTime + ScanInterval =< time_utils:cluster_time_seconds() of
                        true -> storage_sync_traverse:run_update(SpaceId, StorageId, UpdateConfig2);
                        false -> ok
                    end;
                finished ->
                    LastUpdateFinishTime = storage_sync_monitoring:get_last_update_finish_time(SSMDoc),
                    case LastUpdateFinishTime + ScanInterval =< time_utils:cluster_time_seconds() of
                        true -> storage_sync_traverse:run_update(SpaceId, StorageId, UpdateConfig2);
                        false -> ok
                    end
            end
    end.


-spec is_sync_supported_on(storage:data() | storage:id()) -> boolean().
is_sync_supported_on(StorageId) when is_binary(StorageId) ->
    case storage:get(StorageId) of
        {ok, Storage} -> is_sync_supported_on(Storage);
        _ -> false
    end;
is_sync_supported_on(Storage) ->
    Helper = storage:get_helper(Storage),
    helper:is_sync_supported_on(Helper).


-spec schedule_spaces_check() -> ok.
schedule_spaces_check() ->
    schedule(?STORAGE_SYNC_CHECK_INTERVAL, ?SPACES_CHECK).

-spec schedule_spaces_check(non_neg_integer()) -> ok.
schedule_spaces_check(IntervalSeconds) ->
    schedule(IntervalSeconds, ?SPACES_CHECK).

-spec schedule(non_neg_integer(), term()) -> ok.
schedule(IntervalSeconds, Request) ->
    erlang:send_after(timer:seconds(IntervalSeconds), ?MODULE, {sync_timer, Request}),
    ok.