%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Worker responsible for starting scans of storage_import mechanism.
%%% Every ?storage_import_check_interval seconds it lists all locally
%%% supported spaces, checks whether storage_import mechanism is configured
%%% for this spaces and decides whether scans should be started.
%%% For more info on storage_import please see storage_sync_traverse.erl.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_worker).
-author("Jakub Kudzia").
-behavior(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/import/storage_import.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% Interval between successive checks of spaces' storage_import configuration.
-define(STORAGE_IMPORT_CHECK_INTERVAL, application:get_env(?APP_NAME, storage_import_check_interval, 10)).
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

% TODO dodać budowanie wiedzy o spejsach imported
% TODO przy starcie i on supported
% TODO czy ogarniac tez cykliczne/nie cykliczne?
% TODO próba wystartowania co 1 sek


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
%% storage_import scans in synced spaces supported by this provider.
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
    case storage_import_config:get(SpaceId) of
        {error, not_found} ->
            ok;
        {ok, ImportConfig} ->
            {ok, StorageId} = space_logic:get_local_storage_id(SpaceId),
            check_storage(SpaceId, StorageId, ImportConfig)
    end.

-spec check_storage(od_space:id(), storage:id(), storage_import:config()) -> ok.
check_storage(SpaceId, StorageId, ImportConfig) ->
    case is_import_supported(StorageId) of
        true -> maybe_start_scan(SpaceId, StorageId, ImportConfig);
        false -> ok
    end.


-spec maybe_start_scan(od_space:id(), storage:id(), storage_import:config()) -> ok.
maybe_start_scan(SpaceId, StorageId, ImportConfig) ->
    {ok, Mode} = storage_import_config:get_mode(ImportConfig),
    case Mode of
        ?AUTO_IMPORT ->
            {ok, ScanConfig} = storage_import_config:get_scan_config(ImportConfig),
            {ok, SIMDoc} = storage_import_monitoring:get_or_create(SpaceId),
            case storage_import_monitoring:is_initial_scan_not_started_yet(SIMDoc) of
                true ->
                    storage_sync_traverse:run_scan(SpaceId, StorageId, ScanConfig);
                false ->
                    case storage_import_monitoring:is_initial_scan_finished(SIMDoc) of
                        false -> ok;
                        true -> maybe_start_consecutive_scan(SpaceId, StorageId, SIMDoc, ScanConfig)
                    end
            end;
        ?MANUAL_IMPORT ->
            ok
    end.

-spec maybe_start_consecutive_scan(od_space:id(), storage:id(), storage_import_monitoring:doc(),
    storage_import:scan_config()) -> ok.
maybe_start_consecutive_scan(SpaceId, StorageId, SIMDoc, ScanConfig) ->
    case auto_scan_config:is_continuous_enabled(ScanConfig) of
        false ->
            ok;
        true ->
            ScanInterval = auto_scan_config:get_scan_interval(ScanConfig),
            case storage_import_monitoring:is_scan_in_progress(SIMDoc) of
                true ->
                    ok;
                false ->
                    {ok, ScanStopTime} = storage_import_monitoring:get_scan_stop_time(SIMDoc),
                    case ScanStopTime + (ScanInterval * 1000) =< time_utils:cluster_time_millis() of
                        true -> storage_sync_traverse:run_scan(SpaceId, StorageId, ScanConfig);
                        false -> ok
                    end
            end
    end.


-spec is_import_supported(storage:data() | storage:id()) -> boolean().
is_import_supported(StorageId) when is_binary(StorageId) ->
    case storage:get(StorageId) of
        {ok, Storage} -> is_import_supported(Storage);
        _ -> false
    end;
is_import_supported(Storage) ->
    Helper = storage:get_helper(Storage),
    helper:is_import_supported(Helper).


-spec schedule_spaces_check() -> ok.
schedule_spaces_check() ->
    schedule(?STORAGE_IMPORT_CHECK_INTERVAL, ?SPACES_CHECK).

-spec schedule_spaces_check(non_neg_integer()) -> ok.
schedule_spaces_check(IntervalSeconds) ->
    schedule(IntervalSeconds, ?SPACES_CHECK).

-spec schedule(non_neg_integer(), term()) -> ok.
schedule(IntervalSeconds, Request) ->
    erlang:send_after(timer:seconds(IntervalSeconds), ?MODULE, {sync_timer, Request}),
    ok.