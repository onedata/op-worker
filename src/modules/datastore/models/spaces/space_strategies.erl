%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing strategies for operations related to storage <-> space sync
%%% process.
% TODO THIS MODULE SHOULD BE RENAMED (problems with compatibility !!!)
%%% @end
%%%-------------------------------------------------------------------
-module(space_strategies).
-author("Rafal Slota").

-include("modules/storage_sync/storage_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([new/1, add_storage/2]).
-export([
    get_import_details/2, get_update_details/2, is_import_on/1, enable_import/3, configure_update/4,
    get_sync_configs/1, get_update_details/1, get_import_details/1, disable_import/2, disable_update/2]).
-export([save/1, get/1, delete/1, update/2, create/1]).

%% datastore_model callbacks
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type key() :: datastore:key().
-type record() :: #space_strategies{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type sync_config() :: #storage_sync_config{}.
-type sync_configs() :: #{storage:id() => sync_config()}.

-export_type([sync_config/0, sync_configs/0]).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

-spec save(doc()) -> {ok, key()} | {error, term()}.
save(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).

-spec update(key(), diff()) -> {ok, key()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

-spec create(doc()) -> {ok, key()} | {error, term()}.
create(Doc) ->
    ?extract_key(datastore_model:create(?CTX, Doc)).

-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    {ok, StorageIds} = space_storage:get_storage_ids(Key),
    lists:foreach(fun(StorageId) ->
        storage_sync_monitoring:delete(Key, StorageId)
    end, StorageIds),
    datastore_model:delete(?CTX, Key).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks if any storage is imported for a space.
%% @end
%%--------------------------------------------------------------------
-spec is_import_on(od_space:id()) -> boolean().
is_import_on(SpaceId) ->
    {ok, Doc} = space_storage:get(SpaceId),
    {ok, StorageIds} = space_storage:get_storage_ids(Doc),
    lists:foldl(fun
        (_StorageId, true) ->
            true;
        (StorageId, _) ->
            case get_import_details(SpaceId, StorageId) of
                {false, _} -> false;
                _ -> true
            end
    end, false, StorageIds).

%%--------------------------------------------------------------------
%% @doc
%% Returns datastore document for space-strategies mapping.
%% @end
%%--------------------------------------------------------------------
-spec new(od_space:id()) -> Doc :: #document{}.
new(SpaceId) ->
    #document{key = SpaceId, value = #space_strategies{}}.

%%--------------------------------------------------------------------
%% @doc
%% Adds default strategies for new storage in this space.
%% @end
%%--------------------------------------------------------------------
-spec add_storage(od_space:id(), storage:id()) -> ok | no_return().
add_storage(SpaceId, StorageId) ->
    #document{value = Value = #space_strategies{
        sync_configs = SyncConfigs
    }} = Doc = case space_strategies:get(SpaceId) of
        {error, not_found} ->
            new(SpaceId);
        {ok, Doc0} ->
            Doc0
    end,
    {ok, _} = save(Doc#document{
        value = Value#space_strategies{
            sync_configs = SyncConfigs#{StorageId => #storage_sync_config{}}
    }}),
    ok.


enable_import(SpaceId, StorageId, Config) ->
    ConfigWithDefaults = fill_import_config(Config),
    ?extract_ok(update(SpaceId, fun(#space_strategies{sync_configs = SyncConfigs} = SS) ->
        NewSS = maps:update_with(StorageId, fun
            (SSC = #storage_sync_config{import_enabled = false}) ->
                SSC#storage_sync_config{import_enabled = true, import_config = ConfigWithDefaults};
            (#storage_sync_config{import_enabled = true}) ->
                {error, import_already_enabled}
            end,
            #storage_sync_config{
                import_enabled = true,
                import_config = ConfigWithDefaults
            },
            SyncConfigs
        ),
        {ok, SS#space_strategies{sync_configs = NewSS}}
    end)).

disable_import(SpaceId, StorageId) ->
    UpdateFun = fun(#space_strategies{sync_configs = SyncConfigs} = SS) ->
        NewSS = case maps:is_key(StorageId, SyncConfigs) of
            true ->
                maps:update_with(StorageId, fun(SSC) ->
                    SSC#storage_sync_config{import_enabled = false}
                end, SyncConfigs);
            false ->
                SS
        end,
        {ok, SS#space_strategies{sync_configs = NewSS}}
    end,
    case ?extract_ok(update(SpaceId, UpdateFun)) of
        ok -> ok;
        {error, not_found} -> ok
    end.

configure_update(SpaceId, StorageId, Enabled, NewConfig) ->
    ok = ?extract_ok(update(SpaceId, fun(#space_strategies{sync_configs = SyncConfigs} = SS) ->
        NewSS = maps:update_with(StorageId, fun(SSC = #storage_sync_config{update_config = OldConfig}) ->
            UpdatedConfig = maps:merge(OldConfig, NewConfig),
            SSC#storage_sync_config{update_enabled = Enabled, update_config = UpdatedConfig}
        end,
            #storage_sync_config{
                update_enabled = Enabled,
                update_config = fill_update_config(NewConfig)
            },
            SyncConfigs
        ),
        {ok, SS#space_strategies{sync_configs = NewSS}}
    end)).

disable_update(SpaceId, StorageId) ->
    UpdateFun = fun
        (#space_strategies{sync_configs = SyncConfigs} = SS) ->
            NewSS = case maps:is_key(StorageId, SyncConfigs) of
                true ->
                    maps:update_with(StorageId, fun(SSC) ->
                        SSC#storage_sync_config{update_enabled = false}
                    end, SyncConfigs);
                false ->
                    SS
            end,
        {ok, SS#space_strategies{sync_configs = NewSS}}
    end,
    case ?extract_ok(update(SpaceId, UpdateFun)) of
        ok -> ok;
        {error, not_found} -> ok
    end.

-spec get_import_details(od_space:id(), storage:id()) -> storage_sync_traverse:details() | {error, term()}.
get_import_details(SpaceId, StorageId) ->
    {ok, #document{value = #space_strategies{sync_configs = Configs}}} = space_strategies:get(SpaceId),
    case maps:get(StorageId, Configs, undefined) of
        undefined -> {error, not_found};
        SyncConfig -> get_import_details(SyncConfig)
    end.

get_import_details(#storage_sync_config{
    import_enabled = ImportEnabled,
    import_config = ImportConfig
}) ->
    {ImportEnabled, ImportConfig}.

-spec get_update_details(od_space:id(), storage:id()) -> storage_sync_traverse:details() | {error, term()}.
get_update_details(SpaceId, StorageId) ->
    {ok, #document{value = #space_strategies{sync_configs = Configs}}} = space_strategies:get(SpaceId),
    case maps:get(StorageId, Configs, undefined) of
        undefined -> {error, not_found};
        SyncConfig -> get_update_details(SyncConfig)
    end.

get_update_details(#storage_sync_config{
    update_enabled = UpdateEnabled,
    update_config = UpdateConfig
}) ->
    {UpdateEnabled, UpdateConfig}.

-spec get_sync_configs(od_space:id()) -> {ok, sync_configs()}.
get_sync_configs(SpaceId)  ->
    case space_strategies:get(SpaceId) of
        {error, not_found} ->
            #{};
        {ok, #document{value = #space_strategies{sync_configs = SyncConfigs}}} ->
            {ok, SyncConfigs}
    end.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

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
            {import_config, {custom, {json_utils, encode, decode}}},
            {update_config, {custom, {json_utils, encode, decode}}}
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
        ImportEnabled = case ImportStrategyName of
            simple_scan -> true;
            _ -> false
        end,
        UpdateEnabled = case UpdateStrategyName of
            simple_scan -> true;
            _ -> false
        end,
        #storage_sync_config{
            import_enabled = ImportEnabled,
            import_config = ImportConfig,
            update_enabled = UpdateEnabled,
            update_config = UpdateConfig
        }
    end, SyncConfigs),
    {6, #space_strategies{sync_configs = NewSyncConfigs}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

fill_import_config(Config) ->
    #{
        max_depth => maps:get(max_depth, Config, ?DEFAULT_SYNC_MAX_DEPTH),
        sync_acl => maps:get(sync_acl, Config, ?DEFAULT_SYNC_ACL)
    }.

fill_update_config(Config) ->
    #{
        delete_enable => maps:get(delete_enable, Config, ?DEFAULT_DELETE_ENABLE),
        write_once => maps:get(write_once, Config, ?DEFAULT_WRITE_ONCE),
        scan_interval => maps:get(scan_interval, Config, ?DEFAULT_SCAN_INTERVAL),
        max_depth => maps:get(max_depth, Config, ?DEFAULT_SYNC_MAX_DEPTH),
        sync_acl => maps:get(sync_acl, Config, ?DEFAULT_SYNC_ACL)
    }.