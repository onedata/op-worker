%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model stores configuration of storage import for the space.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_config).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/storage/import/storage_import.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([set_manual_mode/1, configure_auto_mode/2]).
-export([to_map/1]).
-export([get_mode/1, get_scan_config/1]).

%% datastore API
-export([create/2, delete/1, get/1]).

%% migration procedures
-export([migrate_to_v1/1]).

%% datastore_model callbacks
-export([get_record_version/0, get_record_struct/1, get_ctx/0]).

-define(CTX, #{model => ?MODULE, memory_copies => all}).

-type key() :: od_space:id().
-type record() :: #storage_import_config{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type config() :: doc() | record().
-type scan_config() :: auto_scan_config:config().
-type scan_config_map() :: auto_scan_config:config_map().
-type mode() :: ?AUTO_IMPORT | ?MANUAL_IMPORT.

-export_type([record/0, doc/0, config/0, mode/0, scan_config/0, scan_config_map/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec set_manual_mode(key()) -> ok | {error, term()}.
set_manual_mode(SpaceId) ->
    create(SpaceId, #storage_import_config{mode = ?MANUAL_IMPORT}).


-spec configure_auto_mode(key(), scan_config_map()) -> ok | {error, term()}.
configure_auto_mode(SpaceId, ScanConfigMap) ->
    UpdateFun = fun(SIC = #storage_import_config{
        mode = ?AUTO_IMPORT, % only auto mode can be updated
        scan_config = ScanConfig
    }) ->
        {ok, SIC#storage_import_config{
            scan_config = auto_scan_config:configure(ScanConfig, ScanConfigMap)
        }}
    end,
    {ok, Default} = UpdateFun(default_auto_import_record(ScanConfigMap)),
    update(SpaceId, UpdateFun, Default).


-spec to_map(doc() | record()) -> json_utils:json_term().
to_map(#document{value = StorageImportConfig}) ->
    to_map(StorageImportConfig);
to_map(#storage_import_config{mode = ?MANUAL_IMPORT}) ->
    #{mode => ?MANUAL_IMPORT};
to_map(#storage_import_config{
    mode = ?AUTO_IMPORT,
    scan_config = ScanConfig
}) ->
    #{
        mode => ?AUTO_IMPORT,
        scan_config => auto_scan_config:to_map(ScanConfig)
    }.


-spec get_mode(doc() | record() | key()) -> {ok, mode()} | {error, term()}.
get_mode(#document{value = StorageImportConfig}) ->
    get_mode(StorageImportConfig);
get_mode(#storage_import_config{mode = Mode}) ->
    {ok, Mode};
get_mode(SpaceId) ->
    case storage_import_config:get(SpaceId) of
        {ok, Doc} ->
            get_mode(Doc);
        Error ->
            Error
    end.


-spec get_scan_config(doc() | record() | key()) -> {ok, scan_config()} | {error, term()}.
get_scan_config(#document{value = StorageImportConfig}) ->
    get_scan_config(StorageImportConfig);
get_scan_config(#storage_import_config{scan_config = ScanConfig}) ->
    {ok, ScanConfig};
get_scan_config(SpaceId) ->
    case storage_import_config:get(SpaceId) of
        {ok, Doc} ->
            get_scan_config(Doc);
        Error ->
            Error
    end.


%%%===================================================================
%%% datastore API functions
%%%===================================================================

-spec get(key()) -> {ok, doc()} | {error, term()}.
get(SpaceId) ->
    datastore_model:get(?CTX, SpaceId).


-spec create(key(), record()) -> ok | {error, term()} .
create(SpaceId, StorageImportConfig) ->
    ?extract_ok(datastore_model:create(?CTX, #document{
        key = SpaceId,
        value = StorageImportConfig
    })).


-spec delete(key()) -> ok | {error, term()}.
delete(SpaceId) ->
    datastore_model:delete(?CTX, SpaceId).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec update(key(), diff(), record()) -> ok | {error, term()}.
update(Key, Diff, Default) ->
    ?extract_ok(datastore_model:update(?CTX, Key, Diff, Default)).


-spec default_auto_import_record(scan_config_map()) -> record().
default_auto_import_record(ScanConfigMap) ->
    #storage_import_config{
        mode = ?AUTO_IMPORT,
        scan_config = auto_scan_config:configure(ScanConfigMap)
    }.

%%%===================================================================
%%% Migration functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% This function migrates old `space_strategies` record to
%% `storage_import_config` in version 1.
%% @end
%%--------------------------------------------------------------------
-spec migrate_to_v1(space_strategies:record()) -> undefined | record().
migrate_to_v1(#space_strategies{sync_configs = SyncConfigs}) ->
    % multi-support hasn't been implemented so the map SyncConfigs
    % has just one key
    [StorageId | _] = maps:keys(SyncConfigs),
    [StorageSyncConfig | _] = maps:values(SyncConfigs),
    {storage_sync_config,
        ImportEnabled,
        UpdateEnabled,
        ImportConfig,
        UpdateConfig
    } = StorageSyncConfig,
    ImportedStorage = storage:is_imported(StorageId),
    case {ImportEnabled, ImportedStorage} of
        {true, true} ->
            MergedConfig = maps:merge(ImportConfig, UpdateConfig),
            MaxDepth = maps:get(max_depth, MergedConfig, undefined),
            SyncAcl = maps:get(sync_acl, MergedConfig, undefined),
            ScanInterval = maps:get(scan_interval, MergedConfig, undefined),
            DetectModifications = case maps:get(write_once, MergedConfig, undefined) of
                undefined -> undefined;
                Boolean when is_boolean(Boolean) -> not Boolean
            end,
            DetectDeletions = maps:get(delete_enable, MergedConfig, undefined),

            Diff = #{
                max_depth => MaxDepth,
                sync_acl => SyncAcl,
                continuous_scan => UpdateEnabled,
                scan_interval => ScanInterval,
                detect_modifications => DetectModifications,
                detect_deletions => DetectDeletions
            },
            % filter undefined values
            Diff2 = maps:filter(fun
                (_, undefined) -> false;
                (_, _) -> true
            end, Diff),
            {?MODULE, ?AUTO_IMPORT, auto_scan_config:configure(Diff2)};
        {false, true} ->
            {?MODULE, ?MANUAL_IMPORT, undefined};
        {_, false} ->
            % if storage was not imported, do not save storage_import_config
            undefined
    end.

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
    1.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {mode, atom},
        {scan_config, {record, [
            {max_depth, integer},
            {sync_acl, boolean},
            {continuous_scan, boolean},
            {scan_interval, integer},
            {detect_modifications, boolean},
            {detect_deletions, boolean}
        ]}}
    ]}.