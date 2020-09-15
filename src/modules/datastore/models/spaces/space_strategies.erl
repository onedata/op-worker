%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for storing storage import configurations.
%%% Each document stores map of configurations of storage import on each
%%% storage supporting given space.
%%%
%%% @TODO VFS-6767 deprecated, included for upgrade procedure. Remove in next major release after 20.02.*.
%%% @end
%%%-------------------------------------------------------------------
-module(space_strategies).
-author("Rafal Slota").
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([get/1, delete/1]).

%% datastore_model callbacks
-export([get_record_version/0, get_record_struct/1, upgrade_record/2, get_ctx/0]).
-export([decode/1, encode/1]).

-type key() :: datastore:key().
-type record() :: #space_strategies{}.
-type doc() :: datastore_doc:doc(record()).
-type sync_config() :: #storage_sync_config{}.
-type sync_configs() :: #{storage:id() => sync_config()}.

%% @formatter:off
-type import_config() :: #{
    max_depth => non_neg_integer(),
    sync_acl => boolean()
}.

-type update_config() :: #{
    max_depth => non_neg_integer(),
    sync_acl => boolean(),
    scan_interval => non_neg_integer(),
    write_once => boolean(),
    delete_enable => boolean()
}.
%% @formatter:on

-type config() :: import_config() | update_config().

-export_type([record/0, import_config/0, update_config/0, config/0, sync_config/0, sync_configs/0]).

-define(CTX, #{model => ?MODULE, memory_copies => all}).

%%%===================================================================
%%% API
%%%===================================================================

-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes space strategies.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    6.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {storage_strategies, #{string => {record, [
            {filename_mapping, {atom, #{atom => term}}},
            {storage_import, {atom, #{atom => term}}},
            {storage_update, [{atom, #{atom => term}}]}, %% List of strategies
            {last_import_time, integer}
        ]}}},
        {file_conflict_resolution, {atom, #{atom => term}}},
        {file_caching, {atom, #{atom => term}}},
        {enoent_handling, {atom, #{atom => term}}}
    ]};
get_record_struct(2) ->
    {record, [
        {storage_strategies, #{string => {record, [
            {filename_mapping, {atom, #{atom => term}}},
            {storage_import, {atom, #{atom => term}}},
            {storage_update, {atom, #{atom => term}}},
            {last_import_time, integer}
        ]}}},
        {file_conflict_resolution, {atom, #{atom => term}}},
        {file_caching, {atom, #{atom => term}}},
        {enoent_handling, {atom, #{atom => term}}}
    ]};
get_record_struct(3) ->
    {record, [
        {storage_strategies, #{string => {record, [
            {filename_mapping, {atom, #{atom => term}}},
            {storage_import, {atom, #{atom => term}}},
            {storage_update, {atom, #{atom => term}}},
            {import_start_time, integer},
            {import_finish_time, integer},
            {last_update_start_time, integer},
            {last_update_finish_time, integer}
        ]}}},
        {file_conflict_resolution, {atom, #{atom => term}}},
        {file_caching, {atom, #{atom => term}}},
        {enoent_handling, {atom, #{atom => term}}}
    ]};
get_record_struct(4) ->
    {record, [
        {storage_strategies, #{string => {record, [
            {storage_import, {atom, #{atom => term}}},
            {storage_update, {atom, #{atom => term}}},
            {import_start_time, integer},
            {import_finish_time, integer},
            {last_update_start_time, integer},
            {last_update_finish_time, integer}
        ]}}},
        {file_conflict_resolution, {atom, #{atom => term}}},
        {file_caching, {atom, #{atom => term}}},
        {enoent_handling, {atom, #{atom => term}}}
    ]};
get_record_struct(5) ->
    {record, [
        {storage_strategies, #{string => {record, [
            {storage_import, {atom, #{atom => term}}},
            {storage_update, {atom, #{atom => term}}}
        ]}}},
        {file_conflict_resolution, {atom, #{atom => term}}},
        {file_caching, {atom, #{atom => term}}},
        {enoent_handling, {atom, #{atom => term}}}
    ]};
get_record_struct(6) ->
    {record, [
        {sync_configs, #{string => {record, [
            {import_enabled, boolean},
            {update_enabled, boolean},
            {import_config, {custom, json, {?MODULE, encode, decode}}},
            {update_config, {custom, json, {?MODULE, encode, decode}}}
        ]}}}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, SyncConfigs, FileConflictResolution, FileCaching, EnoentHandling}) ->
    NewSyncConfigs = maps:map(fun(_, {storage_strategies,
        {filename_mapping, FilenameMappingStrategy},
        {storage_import, StorageImportStrategy},
        {storage_update, StorageUpdateStrategies},
        {last_import_time, LastImportTime}
    }) ->
        {storage_strategies,
            {filename_mapping, FilenameMappingStrategy},
            {storage_import, StorageImportStrategy},
            {storage_update, hd(StorageUpdateStrategies)},
            {last_import_time, LastImportTime}
        }
    end, SyncConfigs),
    {2, {?MODULE, NewSyncConfigs, FileConflictResolution, FileCaching, EnoentHandling}};
upgrade_record(2, {?MODULE, SyncConfigs, FileConflictResolution, FileCaching, EnoentHandling}) ->
    NewSyncConfigs = maps:map(fun(_, {storage_strategies,
        {filename_mapping, FilenameMappingStrategy},
        {storage_import, StorageImportStrategy},
        {storage_update, StorageUpdateStrategy},
        {last_import_time, LastImportTime}
    }) ->
        {storage_strategies,
            FilenameMappingStrategy,
            StorageImportStrategy,
            StorageUpdateStrategy,
            undefined,
            LastImportTime,
            undefined,
            undefined
        }
    end, SyncConfigs),
    {3, {?MODULE, NewSyncConfigs, FileConflictResolution, FileCaching, EnoentHandling}};
upgrade_record(3, {?MODULE, SyncConfigs, FileConflictResolution, FileCaching, EnoentHandling}) ->
    NewSyncConfigs = maps:map(fun(_, {storage_strategies,
        {filename_mapping, _FilenameMappingStrategy},
        {storage_import, StorageImportStrategy},
        {storage_update, StorageUpdateStrategy},
        {import_start_time, ImportStartTime},
        {import_finish_time, ImportFinishTime},
        {last_update_start_time, LastUpdateStartTime},
        {last_update_finish_time, LastImportFinishTime}
    }) ->
        {storage_strategies,
            StorageImportStrategy,
            StorageUpdateStrategy,
            ImportStartTime,
            ImportFinishTime,
            LastUpdateStartTime,
            LastImportFinishTime
        }
    end, SyncConfigs),
    {4, {?MODULE, NewSyncConfigs, FileConflictResolution, FileCaching, EnoentHandling}};
upgrade_record(4, {?MODULE, SyncConfigs, FileConflictResolution, FileCaching, EnoentHandling}) ->
    NewSyncConfigs = maps:map(fun(_, {storage_strategies,
        {storage_import, StorageImportStrategy},
        {storage_update, StorageUpdateStrategy},
        {import_start_time, _ImportStartTime},
        {import_finish_time, _ImportFinishTime},
        {last_update_start_time, _LastUpdateStartTime},
        {last_update_finish_time, _LastImportFinishTime}
    }) ->
        {storage_strategies,
            StorageImportStrategy,
            StorageUpdateStrategy
        }
    end, SyncConfigs),
    {5, {?MODULE, NewSyncConfigs, FileConflictResolution, FileCaching, EnoentHandling}};
upgrade_record(5, {?MODULE, SyncConfigs, _, _, _}) ->
    NewSyncConfigs = maps:map(fun(_StorageId, StorageStrategy) ->
        {storage_strategies,
            {ImportStrategyName, ImportConfig},
            {UpdateStrategyName, UpdateConfig}
        } = StorageStrategy,
        ImportEnabled = ImportStrategyName =:= simple_scan,
        UpdateEnabled = UpdateStrategyName =:= simple_scan,
        {storage_sync_config,
            ImportEnabled,
            ImportConfig,
            UpdateEnabled,
            UpdateConfig
        }
    end, SyncConfigs),
    {6, {space_strategies, NewSyncConfigs}}.

-spec encode(config()) -> binary().
encode(Config) ->
    json_utils:encode(Config).

-spec decode(binary()) -> config().
decode(Config) ->
    DecodedConfig = json_utils:decode(Config),
    maps:fold(fun(K, V, AccIn) ->
        AccIn#{binary_to_atom(K, utf8) => V}
    end, #{}, DecodedConfig).