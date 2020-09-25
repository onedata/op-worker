%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Worker responsible for starting auto scans of storage import.
%%% The scans are started in spaces with enabled ?AUTO mode of storage import.
%%%
%%% Every ?STORAGE_IMPORT_CHECK_INTERVAL seconds it lists all locally
%%% supported spaces, checks whether storage import mechanism is configured
%%% for these spaces and decides whether scans should be started.
%%% For more info on auto storage import please see storage_sync_traverse.erl.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_worker).
-author("Jakub Kudzia").
-behavior(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("modules/storage/import/utils/auto_imported_spaces_registry.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/import/storage_import.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% Interval between successive checks of spaces' storage import configuration.
-define(STORAGE_IMPORT_CHECK_INTERVAL, application:get_env(?APP_NAME, storage_import_check_interval, 1)).
-define(STORAGE_IMPORT_REVISION_INTERVAL, application:get_env(?APP_NAME, storage_import_revision_interval, 60)).

-define(SPACES_CHECK, spaces_check).
-define(REVISE_REGISTRY, revise_registry).
-define(SPACE_WITH_AUTO_IMPORT_CONFIGURED(SpaceId), {space_with_auto_import_configured, SpaceId}).
-define(SPACE_UNSUPPORTED(SpaceId), {space_unsupported, SpaceId}).
-define(SPACE_DELETED(SpaceId), {space_deleted, SpaceId}).
-define(SCAN_STARTED(SpaceId), {scan_started, SpaceId}).
-define(SCAN_FINISHED(SpaceId), {scan_finished, SpaceId}).

%% Callbacks
-export([init/1, handle/1, cleanup/0]).

%% API
-export([
    notify_connection_to_oz/0,
    notify_space_with_auto_import_configured/1,
    notify_space_unsupported/1,
    notify_space_deleted/1,
    notify_started_scan/1,
    notify_finished_scan/1
]).

%% Exported for tests
-export([schedule_spaces_check/1]).

-define(STORAGE_IMPORT_WORKER, ?MODULE).

-define(SCHEDULED_SCAN, scheduled_scan).
-define(NOT_SCHEDULED_SCAN, not_scheduled_scan).

-type schedule_status() :: ?SCHEDULED_SCAN | ?NOT_SCHEDULED_SCAN.

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
handle(?SPACES_CHECK) ->
    check_spaces(),
    schedule_spaces_check();
handle(?REVISE_REGISTRY) ->
    auto_imported_spaces_registry:revise(),
    schedule_registry_revision();
handle(?SPACE_WITH_AUTO_IMPORT_CONFIGURED(SpaceId)) ->
    auto_imported_spaces_registry:register(SpaceId);
handle(?SPACE_UNSUPPORTED(SpaceId)) ->
    auto_imported_spaces_registry:deregister(SpaceId);
handle(?SPACE_DELETED(SpaceId)) ->
    auto_imported_spaces_registry:deregister(SpaceId);
handle(?SCAN_STARTED(SpaceId)) ->
    auto_imported_spaces_registry:mark_scanning(SpaceId);
handle(?SCAN_FINISHED(SpaceId)) ->
    auto_imported_spaces_registry:mark_inactive(SpaceId);
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
    schedule_spaces_check(),
    schedule_registry_revision().

-spec notify_space_with_auto_import_configured(od_space:id()) -> ok.
notify_space_with_auto_import_configured(SpaceId) ->
    worker_proxy:cast(?STORAGE_IMPORT_WORKER, ?SPACE_WITH_AUTO_IMPORT_CONFIGURED(SpaceId)),
    ok.

-spec notify_space_unsupported(od_space:id()) -> ok.
notify_space_unsupported(SpaceId) ->
    worker_proxy:cast(?STORAGE_IMPORT_WORKER, ?SPACE_UNSUPPORTED(SpaceId)),
    ok.

-spec notify_space_deleted(od_space:id()) -> ok.
notify_space_deleted(SpaceId) ->
    worker_proxy:cast(?STORAGE_IMPORT_WORKER, ?SPACE_DELETED(SpaceId)),
    ok.

-spec notify_started_scan(od_space:id()) -> ok.
notify_started_scan(SpaceId) ->
    worker_proxy:cast(?STORAGE_IMPORT_WORKER, ?SCAN_STARTED(SpaceId)),
    ok.

-spec notify_finished_scan(od_space:id()) -> ok.
notify_finished_scan(SpaceId) ->
    worker_proxy:cast(?STORAGE_IMPORT_WORKER, ?SCAN_FINISHED(SpaceId)),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is responsible for checking and optionally starting
%% storage import scans in synced spaces supported by this provider.
%% @end
%%--------------------------------------------------------------------
-spec check_spaces() -> ok.
check_spaces() ->
    try
        case auto_imported_spaces_registry:ensure_initialized() of
            ?INITIALIZED ->
                iterate_spaces_and_maybe_schedule_scans();
            ?NOT_INITIALIZED ->
                ok
        end
    catch
        Error2:Reason ->
            ?error_stacktrace("storage_import_worker was unable to check spaces due to unexpected ~p:~p", [Error2, Reason])
    end.


-spec iterate_spaces_and_maybe_schedule_scans() -> ok.
iterate_spaces_and_maybe_schedule_scans() ->
    auto_imported_spaces_registry:fold(fun
        (SpaceId, ?INACTIVE, _) ->
            case maybe_start_scan(SpaceId) of
                ?SCHEDULED_SCAN ->
                    auto_imported_spaces_registry:mark_scanning(SpaceId);
                ?NOT_SCHEDULED_SCAN ->
                    ok
            end;
        (_SpaceId, ?SCANNING, _) ->
            ok
    end, undefined),
    ok.


-spec maybe_start_scan(od_space:id()) -> schedule_status().
maybe_start_scan(SpaceId) ->
    case storage_import_config:get_auto_config(SpaceId) of
        {ok, AutoConfig} ->
            {ok, SIMDoc} = storage_import_monitoring:get_or_create(SpaceId),
            case storage_import_monitoring:is_initial_scan_not_started_yet(SIMDoc) of
                true ->
                    schedule_scan(SpaceId, AutoConfig);
                false ->
                    case storage_import_monitoring:is_initial_scan_finished(SIMDoc) of
                        false -> ?NOT_SCHEDULED_SCAN;
                        true -> maybe_start_consecutive_scan(SpaceId, SIMDoc, AutoConfig)
                    end
            end;
        _ ->
            ?NOT_SCHEDULED_SCAN
    end.


-spec maybe_start_consecutive_scan(od_space:id(), storage_import_monitoring:doc(),
    storage_import:scan_config()) -> schedule_status().
maybe_start_consecutive_scan(SpaceId, SIMDoc, ScanConfig) ->
    case auto_storage_import_config:is_continuous_scan_enabled(ScanConfig) of
        false ->
            ?NOT_SCHEDULED_SCAN;
        true ->
            ScanInterval = auto_storage_import_config:get_scan_interval(ScanConfig),
            case storage_import_monitoring:is_scan_in_progress(SIMDoc) of
                true ->
                    ?NOT_SCHEDULED_SCAN;
                false ->
                    {ok, ScanStopTime} = storage_import_monitoring:get_scan_stop_time(SIMDoc),
                    case ScanStopTime + (ScanInterval * 1000) =< time_utils:cluster_time_millis() of
                        true -> schedule_scan(SpaceId, ScanConfig);
                        false -> ?NOT_SCHEDULED_SCAN
                    end
            end
    end.


-spec schedule_scan(od_space:id(), storage_import:scan_config()) -> schedule_status().
schedule_scan(SpaceId, ScanConfig) ->
    case storage_sync_traverse:run_scan(SpaceId, ScanConfig) of
        ok -> ?SCHEDULED_SCAN;
        _ -> ?NOT_SCHEDULED_SCAN
    end.


-spec schedule_spaces_check() -> ok.
schedule_spaces_check() ->
    schedule(?STORAGE_IMPORT_CHECK_INTERVAL, ?SPACES_CHECK).


-spec schedule_spaces_check(non_neg_integer()) -> ok.
schedule_spaces_check(IntervalSeconds) ->
    schedule(IntervalSeconds, ?SPACES_CHECK).

schedule_registry_revision() ->
    schedule(?STORAGE_IMPORT_REVISION_INTERVAL, ?REVISE_REGISTRY).

-spec schedule(non_neg_integer(), term()) -> ok.
schedule(IntervalSeconds, Request) ->
    erlang:send_after(timer:seconds(IntervalSeconds), ?STORAGE_IMPORT_WORKER, {sync_timer, Request}),
    ok.