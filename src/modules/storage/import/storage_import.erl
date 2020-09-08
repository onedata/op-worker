%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% Main API for storage_import.
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
-export([clean_up/1]).

%% migration API
-export([migrate_space_strategies/0, migrate_storage_sync_monitoring/0]).

-type mode() :: storage_import_config:mode().
-type config() :: storage_import_config:config().
-type scan_config() :: storage_import_config:scan_config().
-type scan_config_map() :: storage_import_config:scan_config_map().
-type status() :: storage_import_monitoring:status().
-type stats() :: storage_import_monitoring:import_stats().

-export_type([status/0, mode/0, config/0, scan_config/0, scan_config_map/0, stats/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

% TODO * przerobis startowanie przez sync_worker
% TODO * get rid of all storage_import, storage sync etc occurrences
% TODO * usuniecie suit readonly?
% TODO * rename suit testowych
% TODO * czy gui ogarnie pusty wykres jak jeszcze nie bedzie dokumentu monitoringu?

-spec set_manual_mode(od_space:id()) -> ok | {error, term()}.
set_manual_mode(SpaceId) ->
    assert_space_supported_with_imported_storage(SpaceId),
    storage_import_config:set_manual_mode(SpaceId).


-spec configure_auto_mode(od_space:id(), scan_config_map()) -> ok | {error, term()}.
configure_auto_mode(SpaceId, ScanConfigMap) ->
    assert_space_supported_with_imported_storage(SpaceId),
    file_meta:make_space_exist(SpaceId),
    case storage_import_config:configure_auto_mode(SpaceId, ScanConfigMap) of
        ok -> ok;
        {error, _} = Error -> Error
    end.


-spec start_auto_scan(od_space:id()) -> ok.
start_auto_scan(SpaceId) ->
    assert_space_supported_with_imported_storage(SpaceId),
    storage_sync_traverse:run_scan(SpaceId).


-spec stop_auto_scan(od_space:id()) -> ok.
stop_auto_scan(SpaceId) ->
    assert_space_supported_with_imported_storage(SpaceId),
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
                    {ok, ScanConfig} = storage_import_config:get_scan_config(SpaceId),
                    ScanInterval = auto_scan_config:get_scan_interval(ScanConfig),
                    {ok, Info} = storage_import_monitoring:get_info(SIMDoc),
                    {ok, ScanStopTime} = storage_import_monitoring:get_scan_stop_time(SIMDoc),
                    {ok, Info#{nextScan => ScanStopTime + (ScanInterval div 1000)}}
            end;
        Error ->
            Error
    end.


-spec get_stats(od_space:id(), [storage_import_monitoring:plot_counter_type()],
    storage_import_monitoring:window()) -> {ok, stats()}.
get_stats(SpaceId, Type, Window) ->
    storage_import_monitoring:get_stats(SpaceId, Type, Window).


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
                            ok -> ok;
                            {error, already_exists} -> ok;
                            Error -> throw(Error)
                        end
                end,
                % delete deprecated space_strategies doc
                ok = space_strategies:delete(SpaceId);
            {error, not_found} ->
                ok
        end
    end, SpaceIds),
    ?notice("space_strategies migration procedure finished succesfully").


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
                            {ok, _} -> ok;
                            {error, already_exists} -> ok;
                            Error -> throw(Error)
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
    ?notice("storage_sync_monitoring migration procedure finished succesfully").

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