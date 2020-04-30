%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% Main API for storage_sync.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([
    enable_import/3, configure_import/3, configure_import/4, disable_import/2,
    configure_update/3, configure_update/4, disable_update/2,
    get_import_details/2, get_update_details/2, is_import_enabled/2, 
    cancel/2, clean_up/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec enable_import(od_space:id(), storage:id(), space_strategies:import_config()) ->
    ok | {error, term()}.
enable_import(SpaceId, StorageId, Config) ->
    file_meta:make_space_exist(SpaceId),
    configure_import(SpaceId, StorageId, true, Config).

-spec configure_import(od_space:id(), boolean(), space_strategies:import_config()) -> ok.
configure_import(SpaceId, Enabled, Config) ->
    {ok, StorageId} = space_logic:get_local_storage_id(SpaceId),
    configure_import(SpaceId, StorageId, Enabled, Config).

-spec configure_import(od_space:id(), storage:id(), boolean(), space_strategies:import_config()) -> ok.
configure_import(SpaceId, StorageId, Enabled, Config) ->
    space_strategies:configure_import(SpaceId, StorageId, Enabled, Config).

-spec disable_import(od_space:id(), storage:id()) -> ok.
disable_import(SpaceId, StorageId) ->
    configure_import(SpaceId, StorageId, false, #{}).

-spec configure_update(od_space:id(), boolean(), space_strategies:update_config()) ->
    ok | {error, term()}.
configure_update(SpaceId, Enabled, Config) ->
    {ok, StorageId} = space_logic:get_local_storage_id(SpaceId),
    configure_update(SpaceId, StorageId, Enabled, Config).

-spec configure_update(od_space:id(), storage:id(), boolean(),
    space_strategies:update_config()) -> ok | {error, term()}.
configure_update(SpaceId, StorageId, Enabled, Config) ->
    file_meta:make_space_exist(SpaceId),
    {ImportEnabled, _} = get_import_details(SpaceId, StorageId),
    case {Enabled, ImportEnabled} of
        {true, false} ->
            {error, import_disabled};
        _ ->
            space_strategies:configure_update(SpaceId, StorageId, Enabled, Config)
    end.

-spec disable_update(od_space:id(), storage:id()) -> ok.
disable_update(SpaceId, StorageId) ->
    configure_update(SpaceId, StorageId, false, #{}).

-spec get_import_details(od_space:id(), storage:id()) ->
    space_strategies:sync_details().
get_import_details(SpaceId, StorageId) ->
    space_strategies:get_import_details(SpaceId, StorageId).

-spec get_update_details(od_space:id(), storage:id()) ->
    space_strategies:sync_details().
get_update_details(SpaceId, StorageId) ->
    space_strategies:get_update_details(SpaceId, StorageId).

-spec is_import_enabled(od_space:id(), storage:id()) -> boolean().
is_import_enabled(SpaceId, StorageId) ->
    {ok, SyncConfigs} = space_strategies:get_sync_configs(SpaceId),
    SyncConfig = maps:get(StorageId, SyncConfigs, undefined),
    case SyncConfig of
        undefined -> false;
        _ ->
            {ImportEnabled, _ImportConfig} = space_strategies:get_import_details(SyncConfig),
            ImportEnabled
    end.

-spec cancel(od_space:id(), storage:id()) -> ok.
cancel(SpaceId, StorageId) ->
    storage_sync_traverse:cancel(SpaceId, StorageId).

-spec clean_up(od_space:id(), storage:id()) -> ok | {error, term()}.
clean_up(SpaceId, StorageId) ->
    storage_sync_monitoring:delete(SpaceId, StorageId),
    space_strategies:delete(SpaceId).