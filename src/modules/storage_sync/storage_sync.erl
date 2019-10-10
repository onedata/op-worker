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
    enable_import/2, enable_import/3,
    configure_storage_update/3, configure_storage_update/4,
    disable_import/2, disable_update/1,
    get_import_details/2, get_update_details/2, cancel/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec enable_import(od_space:id(), space_strategies:import_config()) -> ok | {error, term()}.
enable_import(SpaceId, Args) ->
    StorageId = space_storage:get_storage_id(SpaceId),
    enable_import(SpaceId, StorageId, Args).

-spec enable_import(od_space:id(), storage:id(), space_strategies:import_config()) ->
    ok | {error, term()}.
enable_import(SpaceId, StorageId, Config) ->
    file_meta:make_space_exist(SpaceId),
    space_strategies:enable_import(SpaceId, StorageId, Config).

-spec configure_storage_update(od_space:id(), boolean(), space_strategies:update_config()) ->
    ok | {error, term()}.
configure_storage_update(SpaceId, Enabled, Config) ->
    StorageId = space_storage:get_storage_id(SpaceId),
    configure_storage_update(SpaceId, StorageId, Enabled, Config).

-spec configure_storage_update(od_space:id(), storage:id(), boolean(),
    space_strategies:update_config()) -> ok | {error, term()}.
configure_storage_update(SpaceId, StorageId, Enabled, Config) ->
    file_meta:make_space_exist(SpaceId),
    {ImportEnabled, _} = get_import_details(SpaceId, StorageId),
    case {Enabled, ImportEnabled} of
        {true, false} ->
            {error, import_disabled};
        _ ->
            space_strategies:configure_update(SpaceId, StorageId, Enabled, Config)
    end.

-spec disable_import(od_space:id(), storage:id()) -> ok.
disable_import(SpaceId, StorageId) ->
    space_strategies:disable_import(SpaceId, StorageId).

-spec disable_update(od_space:id()) -> ok.
disable_update(SpaceId) ->
    StorageId = space_storage:get_storage_id(SpaceId),
    disable_storage_update(SpaceId, StorageId).

-spec disable_storage_update(od_space:id(), storage:id()) -> ok.
disable_storage_update(SpaceId, StorageId) ->
    space_strategies:disable_update(SpaceId, StorageId).

-spec get_import_details(od_space:id(), storage:id()) ->
    space_strategies:sync_details() | {error, term()}.
get_import_details(SpaceId, StorageId) ->
    space_strategies:get_import_details(SpaceId, StorageId).

-spec get_update_details(od_space:id(), storage:id()) ->
    space_strategies:sync_details() | {error, term()}.
get_update_details(SpaceId, StorageId) ->
    space_strategies:get_update_details(SpaceId, StorageId).

-spec cancel(od_space:id(), storage:id()) -> ok.
cancel(SpaceId, StorageId) ->
    storage_sync_traverse:cancel(SpaceId, StorageId).