%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% Storage import API module.
%%%
%%% Storage import allows to register (import) files, located on the
%%% storage, in the space supported by the storage.
%%% Registering files does not copy the data. It only creates necessary
%%% metadata so that the files are visible in the space.
%%%
%%% Storage import can be enabled if and only if the space is supported
%%% with an `imported` storage.
%%%
%%% There are 2 possible modes of storage import:
%%% * ?MANUAL - in case of `manual` mode, the files must be registered manually
%%%             by the space users with REST API. Registration of directories
%%%             is not supported.
%%%             For more info go to file_registration module.
%%% * ?AUTO - in case of `auto` mode, the storage will be automatically
%%%           scanned and data will be imported from storage into the
%%%           assigned space without need for copying the data.
%%%           For more info go to storage_sync_traverse and
%%%           storage_import_engine modules.
%%%
%%% Configuration of the storage import is stored in the
%%% storage_import_config model.
%%% Mode of the storage import, once set, cannot be modified.
%%% In case of ?AUTO mode, configuration of scans can be modified.
%%% For more info on configuration of storage_import go to
%%% storage_import_config module.
%%%
%%%
%%% storage_import_worker is the process which is responsible for
%%% scheduling auto scans of storages that support spaces with ?AUTO mode.
%%% For more info go to storage_import_worker module.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import).
-author("Jakub Kudzia").

-include("modules/storage/import/storage_import.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([set_manual_mode/1, configure_auto_mode/2]).
-export([start_auto_scan/1, stop_auto_scan/1]).
-export([get_mode/1, get_configuration/1]).
-export([get_info/1, get_stats/3]).
-export([get_manual_example/1]).
-export([clean_up/1]).

%% migration API
-export([migrate_space_strategies/0, migrate_storage_sync_monitoring/0]).

-type mode() :: storage_import_config:mode().
-type config() :: storage_import_config:config().
-type scan_config() :: storage_import_config:auto_config().
-type scan_config_map() :: storage_import_config:auto_config_map().
-type status() :: storage_import_monitoring:status().
-type stats() :: storage_import_monitoring:import_stats().

-export_type([status/0, mode/0, config/0, scan_config/0, scan_config_map/0, stats/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

% TODO * rename suit testowych sync, sync_s3, sync_deletion

-spec set_manual_mode(od_space:id()) -> ok | {error, term()}.
set_manual_mode(SpaceId) ->
    assert_space_supported_with_imported_storage(SpaceId),
    storage_import_config:set_manual_mode(SpaceId).


-spec configure_auto_mode(od_space:id(), scan_config_map()) -> ok | {error, term()}.
configure_auto_mode(SpaceId, ScanConfigMap) ->
    assert_space_supported_with_imported_storage(SpaceId),
    assert_auto_storage_import_supported(SpaceId),

    file_meta:make_space_exist(SpaceId),
    case storage_import_config:configure_auto_mode(SpaceId, ScanConfigMap) of
        ok ->
            storage_import_worker:notify_space_with_auto_import_configured(SpaceId),
            ok;
        {error, _} = Error -> Error
    end.


-spec start_auto_scan(od_space:id()) -> ok | {error, term()}.
start_auto_scan(SpaceId) ->
    assert_space_supported_with_imported_storage(SpaceId),
    assert_auto_import_mode(SpaceId),

    storage_sync_traverse:run_scan(SpaceId).


-spec stop_auto_scan(od_space:id()) -> ok.
stop_auto_scan(SpaceId) ->
    assert_space_supported_with_imported_storage(SpaceId),
    assert_auto_import_mode(SpaceId),

    storage_sync_traverse:cancel(SpaceId).


-spec clean_up(od_space:id()) -> ok | {error, term()}.
clean_up(SpaceId) ->
    storage_import_monitoring:delete(SpaceId),
    storage_import_config:delete(SpaceId).


-spec get_configuration(od_space:id()) ->
    {ok, storage_import:scan_config_map()} | {error, term()}.
get_configuration(SpaceId) ->
    case storage_import_config:get(SpaceId) of
        {ok, Doc} ->
            {ok, storage_import_config:to_map(Doc)};
        Error ->
            Error
    end.


-spec get_mode(od_space:id()) -> {ok, mode()} | {error, term()}.
get_mode(SpaceId) ->
    storage_import_config:get_mode(SpaceId).


-spec get_info(od_space:id()) -> {ok, json_utils:json_term()} | {error, term()}.
get_info(SpaceId) ->
    case storage_import_monitoring:get(SpaceId) of
        {ok, SIMDoc} ->
            case storage_import_monitoring:is_scan_in_progress(SIMDoc) of
                true ->
                    storage_import_monitoring:get_info(SIMDoc);
                false ->
                    {ok, AutoConfig} = storage_import_config:get_auto_config(SpaceId),
                    case auto_storage_import_config:is_continuous_scan_enabled(AutoConfig) of
                        true ->
                            ScanInterval = auto_storage_import_config:get_scan_interval(AutoConfig),
                            {ok, Info} = storage_import_monitoring:get_info(SIMDoc),
                            {ok, ScanStopTime} = storage_import_monitoring:get_scan_stop_time(SIMDoc),
                            {ok, Info#{nextScan => (ScanStopTime div 1000) + ScanInterval}};
                        false ->
                            storage_import_monitoring:get_info(SIMDoc)
                    end
            end;
        Error ->
            Error
    end.


-spec get_stats(od_space:id(), [storage_import_monitoring:plot_counter_type()],
    storage_import_monitoring:window()) -> {ok, stats()}.
get_stats(SpaceId, Type, Window) ->
    storage_import_monitoring:get_stats(SpaceId, Type, Window).


-spec get_manual_example(od_space:id()) -> {ok, binary()}.
get_manual_example(SpaceId) ->
    assert_space_supported_with_imported_storage(SpaceId),

    {ok, StorageId} = space_logic:get_local_storage_id(SpaceId),
    Domain = oneprovider:get_domain(),

    {ok, str_utils:format_bin(
        "curl -X POST -H \"X-Auth-Token:$TOKEN\" -H \"content-type:application/json\" \ "
        "-d '{\"storageId\":\"~s\", \"spaceId\":\"~s\", \"storageFileId\":\"$STORAGE_FILE_ID\", \"destinationPath\":\"'$DESTINATION_PATH'\"}' \ "
        "https://~s/api/v3/oneprovider/data/register",
        [StorageId, SpaceId, Domain]
    )}.

%%%===================================================================
%%% Migrate from space_strategies
%%%===================================================================

-spec migrate_space_strategies() -> ok.
migrate_space_strategies() ->
    ?info("Starting space_strategies migration procedure..."),
    {ok, SpaceIds} = provider_logic:get_spaces(),
    lists:foreach(fun(SpaceId) ->
        case space_strategies:get(SpaceId) of
            {ok, #document{value = SpaceStrategies}} ->
                case storage_import_config:migrate_to_v1(SpaceStrategies) of
                    undefined ->
                        % storage was not imported so we do not have to create storage_import_config document
                        ok;
                    StorageImportConfigV1 ->
                        case storage_import_config:create(SpaceId, StorageImportConfigV1) of
                            ok ->
                                ?info("space_strategies migration procedure for space ~s finished succesfully.", [SpaceId]),
                                ok;
                            {error, already_exists} ->
                                ?warning(
                                    "space_strategies migration procedure failed for space ~s because storage_import_config documenr already exists.",
                                    [SpaceId]),
                                ok;
                            Error ->
                                ?error(
                                    "space_strategies migration procedure unexpectedly failed for space ~s due to ~p.",
                                    [SpaceId, Error]),
                                throw(Error)
                        end
                end,
                % delete deprecated space_strategies doc
                ok = space_strategies:delete(SpaceId);
            {error, not_found} ->
                ok
        end
    end, SpaceIds),
    ?info("space_strategies migration procedure finished succesfully.").


-spec migrate_storage_sync_monitoring() -> ok.
migrate_storage_sync_monitoring() ->
    ?info("Starting storage_sync_monitoring migration procedure..."),
    {ok, SpaceIds} = provider_logic:get_spaces(),
    lists:foreach(fun(SpaceId) ->
        case space_logic:get_local_storage_id(SpaceId) of
            {ok, StorageId} ->
                case storage_sync_monitoring:get(SpaceId, StorageId) of
                    {ok, #document{value = SSM}} ->
                        SIMV1 = storage_import_monitoring:migrate_to_v1(SSM),
                        case storage_import_monitoring:create(SpaceId, SIMV1) of
                            {ok, _} ->
                                ?info("storage_sync_monitoring migration procedure for space ~s finished succesfully.", [SpaceId]),
                                ok;
                            {error, already_exists} ->
                                ?warning(
                                    "storage_sync_monitoring migration procedure failed for space ~s because storage_import_monitoring document already exists.",
                                    [SpaceId]),
                                ok;
                            Error ->
                                ?error(
                                    "storage_sync_monitoring migration procedure unexpectedly failed for space ~s due to ~p.",
                                    [SpaceId, Error]),
                                throw(Error)
                        end,
                        % delete deprecated storage_sync_monitoring doc
                        ok = storage_sync_monitoring:delete(SpaceId, StorageId);
                    {error, not_found} ->
                        ok
                end;
            _ ->
                ok
        end
    end, SpaceIds),
    ?info("storage_sync_monitoring migration procedure finished succesfully.").

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec assert_space_supported_with_imported_storage(od_space:id()) -> ok.
assert_space_supported_with_imported_storage(SpaceId) ->
    case space_logic:get_local_storage_id(SpaceId) of
        {ok, StorageId} ->
            case storage:is_imported(StorageId) of
                true -> ok;
                false -> throw(?ERROR_REQUIRES_IMPORTED_STORAGE(StorageId))
            end;
        Error ->
            throw(Error)
    end.

-spec assert_auto_storage_import_supported(od_space:id()) -> ok.
assert_auto_storage_import_supported(SpaceId) ->
    {ok, StorageId} = space_logic:get_local_storage_id(SpaceId),
    Helper = storage:get_helper(StorageId),
    case helper:is_auto_import_supported(Helper) of
        true ->
            ok;
        false ->
            throw(?ERROR_AUTO_STORAGE_IMPORT_NOT_SUPPORTED(StorageId, ?AUTO_IMPORT_HELPERS, ?AUTO_IMPORT_OBJECT_HELPERS))
    end.


-spec assert_auto_import_mode(od_space:id()) -> ok.
assert_auto_import_mode(SpaceId) ->
    case storage_import:get_mode(SpaceId) of
        {ok, ?AUTO_IMPORT} ->
            ok;
        {ok, ?MANUAL_IMPORT} ->
            throw(?ERROR_REQUIRES_AUTO_STORAGE_IMPORT_MODE)
    end.