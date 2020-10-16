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
-module(auto_storage_import_worker).
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
-export([schedule_spaces_check/1, is_ready/0]).

-define(AUTO_STORAGE_IMPORT_WORKER, ?MODULE).

% messages
-define(SCHEDULED_SCAN, scheduled_scan).
-define(NOT_SCHEDULED_SCAN, not_scheduled_scan).
-define(IS_POOL_INITIALIZED, is_pool_initialized).

% worker ETS associated keys
-define(ETS_STATE, auto_storage_import_worker_state).
-define(STALLED_SCAN_FIXED, stalled_scan_fixed).
-define(POOL_INITIALIZED, pool_initialized).

% errors
-define(STALLED_SCANS_NOT_FIXED_ERROR, {error, stalled_scan_not_fixed_error}).

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
    case gs_channel_service:is_connected() of
        true -> notify_connection_to_oz();
        false -> ok
    end,
    state_init(),
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
handle(?IS_POOL_INITIALIZED) ->
    is_pool_initialized();
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
    state_cleanup(),
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
    worker_proxy:cast(?AUTO_STORAGE_IMPORT_WORKER, ?SPACE_WITH_AUTO_IMPORT_CONFIGURED(SpaceId)),
    ok.

-spec notify_space_unsupported(od_space:id()) -> ok.
notify_space_unsupported(SpaceId) ->
    worker_proxy:cast(?AUTO_STORAGE_IMPORT_WORKER, ?SPACE_UNSUPPORTED(SpaceId)),
    ok.

-spec notify_space_deleted(od_space:id()) -> ok.
notify_space_deleted(SpaceId) ->
    worker_proxy:cast(?AUTO_STORAGE_IMPORT_WORKER, ?SPACE_DELETED(SpaceId)),
    ok.

-spec notify_started_scan(od_space:id()) -> ok.
notify_started_scan(SpaceId) ->
    worker_proxy:cast(?AUTO_STORAGE_IMPORT_WORKER, ?SCAN_STARTED(SpaceId)),
    ok.

-spec notify_finished_scan(od_space:id()) -> ok.
notify_finished_scan(SpaceId) ->
    worker_proxy:cast(?AUTO_STORAGE_IMPORT_WORKER, ?SCAN_FINISHED(SpaceId)),
    ok.

%%%===================================================================
%%% Functions used in tests
%%%===================================================================

-spec is_ready() -> boolean().
is_ready() ->
    worker_proxy:call(?AUTO_STORAGE_IMPORT_WORKER, ?IS_POOL_INITIALIZED).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec ensure_pool_initialized() -> ok.
ensure_pool_initialized() ->
    case is_pool_initialized() of
        true ->
            ok;
        false ->
            ok = storage_sync_traverse:init_pool(),
            mark_pool_initialized(),
            ok
    end.

-spec ensure_stalled_scans_are_fixed() -> ok.
ensure_stalled_scans_are_fixed() ->
    case are_stalled_scans_fixed() of
        true ->
            ok;
        false ->
            case provider_logic:get_spaces() of
                {ok, SpaceIds} ->
                    fix_stalled_scans(SpaceIds);
                ?ERROR_NO_CONNECTION_TO_ONEZONE ->
                    ?debug("auto_storage_import_worker was unable to fix stalled scans due to no connection to oz."),
                    throw(?STALLED_SCANS_NOT_FIXED_ERROR);
                ?ERROR_UNREGISTERED_ONEPROVIDER ->
                    ?debug("auto_storage_import_worker was unable to fix stalled scans due to unregistered provider."),
                    throw(?STALLED_SCANS_NOT_FIXED_ERROR);
                {error, _} = Error ->
                    ?error("auto_storage_import_worker was unable to fix stalled scans due to unexpected error: ~p", [Error]),
                    throw(?STALLED_SCANS_NOT_FIXED_ERROR)
            end
    end.


-spec fix_stalled_scans([od_space:id()]) -> ok.
fix_stalled_scans(SpaceIds) ->
    case storage_sync_traverse:fix_stalled_scans(SpaceIds) of
        ok ->
            mark_stalled_scans_fixed(),
            ok;
        {error, _} = Error ->
            ?error("auto_storage_import_worker was unable to fix stalled scans due to unexpected error: ~p", [Error]),
            throw(?STALLED_SCANS_NOT_FIXED_ERROR)
    end.


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
        ensure_pool_initialized(),
        ensure_stalled_scans_are_fixed(),
        case auto_imported_spaces_registry:ensure_initialized() of
            ?INITIALIZED ->
                iterate_spaces_and_maybe_schedule_scans();
            ?NOT_INITIALIZED ->
                ok
        end
    catch
        throw:?STALLED_SCANS_NOT_FIXED_ERROR ->
            ok;
        Error2:Reason ->
            ?error_stacktrace("~s was unable to check spaces due to unexpected ~p:~p",
                [?AUTO_STORAGE_IMPORT_WORKER, Error2, Reason])
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
            case storage_import_monitoring:is_scan_in_progress(SIMDoc) of
                true ->
                    ?NOT_SCHEDULED_SCAN;
                false ->
                    case storage_import_monitoring:is_initial_scan_finished(SIMDoc) of
                        false -> schedule_scan(SpaceId, AutoConfig);
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
                    case ScanStopTime + (ScanInterval * 1000) =< time_utils:timestamp_millis() of
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
    erlang:send_after(timer:seconds(IntervalSeconds), ?AUTO_STORAGE_IMPORT_WORKER, {sync_timer, Request}),
    ok.

%%%===================================================================
%%% Internal functions for operations on worker's state stored in ETS
%%%===================================================================

-spec is_pool_initialized() -> boolean().
is_pool_initialized() ->
    state_get(?POOL_INITIALIZED) =:= true.

-spec mark_pool_initialized() -> ok.
mark_pool_initialized() ->
    state_put(?POOL_INITIALIZED, true).

-spec are_stalled_scans_fixed() -> boolean().
are_stalled_scans_fixed() ->
    state_get(?STALLED_SCAN_FIXED) =:= true.

-spec mark_stalled_scans_fixed() -> ok.
mark_stalled_scans_fixed() ->
    state_put(?STALLED_SCAN_FIXED, true).

    -spec state_init() -> ok.
state_init() ->
    ?ETS_STATE = ets:new(?ETS_STATE, [public, named_table]),
    ok.

-spec state_cleanup() -> ok.
state_cleanup() ->
    ets:delete(?ETS_STATE),
    ok.

-spec state_get(term()) -> term().
state_get(Key) ->
    case ets:lookup(?ETS_STATE, Key) of
        [{Key, Value}] -> Value;
        [] -> undefined
    end.

-spec state_put(term(), term()) -> ok.
state_put(Key, Value) ->
    true = ets:insert(?ETS_STATE, {Key, Value}),
    ok.