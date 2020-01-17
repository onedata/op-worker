%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for storing storage_sync configurations.
%%% Each document stores map of configurations of storage_sync on each
%%% storage supporting given space.
%%% TODO VFS-5717 rename to space_sync_config
%%% @end
%%%-------------------------------------------------------------------
-module(space_strategies).
-author("Rafal Slota").
-author("Jakub Kudzia").

-include("modules/storage/sync/storage_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get/1, delete/1]).
-export([
    get_import_details/1, get_import_details/2,
    get_update_details/1, get_update_details/2,
    get_sync_configs/1, is_any_storage_imported/1,
    configure_import/4, configure_update/4
]).

%% datastore_model callbacks
-export([get_record_version/0, get_record_struct/1, upgrade_record/2, get_ctx/0]).
-export([decode/1, encode/1]).

-type key() :: datastore:key().
-type record() :: #space_strategies{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type sync_config() :: #storage_sync_config{}.
-type sync_configs() :: #{storage:id() => sync_config()}.
-type sync_details() :: {boolean(), import_config() | update_config()}.

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

-export_type([import_config/0, update_config/0, config/0, sync_config/0, sync_configs/0, sync_details/0]).

-define(CTX, #{model => ?MODULE, memory_copies => all}).
-define(DEFAULT_IMPORT_SYNC_CONFIG(Enabled, Config),
    #storage_sync_config{
        import_enabled = Enabled,
        import_config = Config
    }).
-define(DEFAULT_UPDATE_SYNC_CONFIG(Enabled, Config),
    #storage_sync_config{
        update_enabled = Enabled,
        update_config = Config
    }).
-define(DEFAULT_RECORD(StorageId, SyncConfig),
    #space_strategies{sync_configs = #{StorageId => SyncConfig}}).

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

-spec is_any_storage_imported(od_space:id()) -> boolean().
is_any_storage_imported(SpaceId) ->
    {ok, StorageIds} = space_logic:get_local_storage_ids(SpaceId),
    lists:any(fun(StorageId) ->
        case get_import_details(SpaceId, StorageId) of
            {false, _} -> false;
            _ -> true
        end
    end, StorageIds).

-spec configure_import(od_space:id(), storage:id(), boolean(), import_config()) -> ok.
configure_import(SpaceId, StorageId, Enabled, NewConfig) ->
    FilledConfig = fill_import_config(NewConfig),
    DefaultSyncConfig = ?DEFAULT_IMPORT_SYNC_CONFIG(Enabled, FilledConfig),
    ok = ?extract_ok(update(SpaceId, fun(#space_strategies{sync_configs = SyncConfigs} = SS) ->
        NewSS = maps:update_with(StorageId, fun(SSC = #storage_sync_config{import_config = OldConfig}) ->
            UpdatedConfig = maps:merge(OldConfig, FilledConfig),
            SSC#storage_sync_config{import_enabled = Enabled, import_config = UpdatedConfig}
        end,
            DefaultSyncConfig, SyncConfigs
        ),
        {ok, SS#space_strategies{sync_configs = NewSS}}
    end, ?DEFAULT_RECORD(StorageId, DefaultSyncConfig))).

-spec configure_update(od_space:id(), storage:id(), boolean(), update_config()) -> ok.
configure_update(SpaceId, StorageId, Enabled, NewConfig) ->
    FilledConfig = fill_update_config(NewConfig),
    DefaultSyncConfig = ?DEFAULT_UPDATE_SYNC_CONFIG(Enabled, FilledConfig),
    ok = ?extract_ok(update(SpaceId, fun(#space_strategies{sync_configs = SyncConfigs} = SS) ->
        NewSS = maps:update_with(StorageId, fun(SSC = #storage_sync_config{update_config = OldConfig}) ->
            UpdatedConfig = maps:merge(OldConfig, FilledConfig),
            SSC#storage_sync_config{update_enabled = Enabled, update_config = UpdatedConfig}
        end,
            DefaultSyncConfig, SyncConfigs
        ),
        {ok, SS#space_strategies{sync_configs = NewSS}}
    end, ?DEFAULT_RECORD(StorageId, DefaultSyncConfig))).

-spec get_import_details(od_space:id(), storage:id()) -> sync_details().
get_import_details(SpaceId, StorageId) ->
    case space_strategies:get(SpaceId) of
        {ok, #document{value = #space_strategies{sync_configs = Configs}}} ->
            case maps:get(StorageId, Configs, undefined) of
                undefined -> {false, #{}};
                SyncConfig -> get_import_details(SyncConfig)
            end;
        {error, not_found} ->
            {false, #{}}
    end.

-spec get_import_details(sync_config()) -> sync_details().
get_import_details(#storage_sync_config{
    import_enabled = ImportEnabled,
    import_config = ImportConfig
}) ->
    {ImportEnabled, ImportConfig}.

-spec get_update_details(od_space:id(), storage:id()) -> sync_details().
get_update_details(SpaceId, StorageId) ->
    case space_strategies:get(SpaceId) of
        {ok, #document{value = #space_strategies{sync_configs = Configs}}} ->
            case maps:get(StorageId, Configs, undefined) of
                undefined -> {false, #{}};
                SyncConfig -> get_update_details(SyncConfig)
            end;
        {error, not_found} ->
            {false, #{}}
    end.

-spec get_update_details(sync_config()) -> sync_details().
get_update_details(#storage_sync_config{
    update_enabled = UpdateEnabled,
    update_config = UpdateConfig
}) ->
    {UpdateEnabled, UpdateConfig}.

-spec get_sync_configs(od_space:id()) -> {ok, sync_configs()}.
get_sync_configs(SpaceId) ->
    case space_strategies:get(SpaceId) of
        {error, not_found} ->
            {ok, #{}};
        {ok, #document{value = #space_strategies{sync_configs = SyncConfigs}}} ->
            {ok, SyncConfigs}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec update(key(), diff(), record()) -> {ok, key()} | {error, term()}.
update(Key, Diff, Default) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff, Default)).

-spec fill_import_config(map()) -> import_config().
fill_import_config(Config) ->
    #{
        max_depth => maps:get(max_depth, Config, ?DEFAULT_SYNC_MAX_DEPTH),
        sync_acl => maps:get(sync_acl, Config, ?DEFAULT_SYNC_ACL)
    }.

-spec fill_update_config(map()) -> update_config().
fill_update_config(Config) ->
    #{
        delete_enable => maps:get(delete_enable, Config, ?DEFAULT_DELETE_ENABLE),
        write_once => maps:get(write_once, Config, ?DEFAULT_WRITE_ONCE),
        scan_interval => maps:get(scan_interval, Config, ?DEFAULT_SCAN_INTERVAL),
        max_depth => maps:get(max_depth, Config, ?DEFAULT_SYNC_MAX_DEPTH),
        sync_acl => maps:get(sync_acl, Config, ?DEFAULT_SYNC_ACL)
    }.

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
        #storage_sync_config{
            import_enabled = ImportEnabled,
            import_config = ImportConfig,
            update_enabled = UpdateEnabled,
            update_config = UpdateConfig
        }
    end, SyncConfigs),
    {6, #space_strategies{sync_configs = NewSyncConfigs}}.

-spec encode(config()) -> binary().
encode(Config) ->
    json_utils:encode(Config).

-spec decode(binary()) -> config().
decode(Config) ->
    DecodedConfig = json_utils:decode(Config),
    maps:fold(fun(K, V, AccIn) ->
        AccIn#{binary_to_atom(K, utf8) => V}
    end, DecodedConfig, DecodedConfig).