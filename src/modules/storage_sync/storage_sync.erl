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


%% API
-export([start_storage_import/2, start_storage_import/3, start_storage_import/4,
    stop_storage_import/2, stop_storage_import/1,
    start_storage_update/2, start_storage_update/3, start_storage_update/4,
    stop_storage_update/2, stop_storage_update/1
    , start_storage_import_and_update/2, start_storage_import_and_update/3, start_storage_import_and_update/4, stop_storage_import_and_update/1, stop_storage_import_and_update/2]).

-define(DEFAULT_STRATEGY_NAME, bfs_scan).

%%--------------------------------------------------------------------
%% @doc
%% @equiv start_storage_import(SpaceId, ScanInterval, ?DEFAULT_STRATEGY_NAME).
%% @end
%%--------------------------------------------------------------------
-spec start_storage_import(od_space:id(), non_neg_integer()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
start_storage_import(SpaceId, ScanInterval) ->
    start_storage_import(SpaceId, ScanInterval, ?DEFAULT_STRATEGY_NAME).

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for starting storage import. By default uses head from list
%% of storage_ids associated with given SpaceId.
%% @end
%%--------------------------------------------------------------------
-spec start_storage_import(od_space:id(), non_neg_integer(), space_strategy:name()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
start_storage_import(SpaceId, ScanInterval, StrategyName) ->
    StorageId = get_supporting_storage(SpaceId),
    start_storage_import(SpaceId, ScanInterval, StrategyName, StorageId).

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for starting storage import.
%% @end
%%--------------------------------------------------------------------
-spec start_storage_import(od_space:id(), non_neg_integer(), space_strategy:name(),
    storage:id()) -> {ok, datastore:ext_key()} | datastore:update_error().
start_storage_import(SpaceId, ScanInterval, StrategyName, StorageId) ->
    file_meta:make_space_exist(SpaceId),
    space_strategies:set_strategy(SpaceId, StorageId, storage_import,
        StrategyName, #{scan_interval => ScanInterval}).

%%--------------------------------------------------------------------
%% @doc
%% @equiv start_storage_update(SpaceId, ScanInterval, ?DEFAULT_STRATEGY_NAME).
%% @end
%%--------------------------------------------------------------------
-spec start_storage_update(od_space:id(), non_neg_integer()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
start_storage_update(SpaceId, ScanInterval) ->
    start_storage_update(SpaceId, ScanInterval, ?DEFAULT_STRATEGY_NAME).

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for starting storage update. By default uses head from list
%% of storage_ids associated with given SpaceId.
%% @end
%%--------------------------------------------------------------------
-spec start_storage_update(od_space:id(), non_neg_integer(), space_strategy:name()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
start_storage_update(SpaceId, ScanInterval, StrategyName) ->
    StorageId = get_supporting_storage(SpaceId),
    start_storage_update(SpaceId, ScanInterval, StrategyName, StorageId).

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for starting storage update.
%% @end
%%--------------------------------------------------------------------
-spec start_storage_update(od_space:id(), non_neg_integer(), space_strategy:name(),
    storage:id()) -> {ok, datastore:ext_key()} | datastore:update_error().
start_storage_update(SpaceId, ScanInterval, StrategyName, StorageId) ->
    file_meta:make_space_exist(SpaceId),
    space_strategies:set_strategy(SpaceId, StorageId, storage_update,
        StrategyName, #{scan_interval => ScanInterval}).


%%--------------------------------------------------------------------
%% @doc
%% @equiv start_storage_import_and_update(SpaceId, ScanInterval, ?DEFAULT_STRATEGY_NAME).
%% @end
%%--------------------------------------------------------------------
-spec start_storage_import_and_update(od_space:id(), non_neg_integer()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
start_storage_import_and_update(SpaceId, ScanInterval) ->
    start_storage_import_and_update(SpaceId, ScanInterval, ?DEFAULT_STRATEGY_NAME).


%%--------------------------------------------------------------------
%% @doc
%% Wrapper for starting storage import and update. By default uses head from list
%% of storage_ids associated with given SpaceId.
%% @end
%%--------------------------------------------------------------------
-spec start_storage_import_and_update(od_space:id(), non_neg_integer(), space_strategy:name()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
start_storage_import_and_update(SpaceId, ScanInterval, StrategyName) ->
    StorageId = get_supporting_storage(SpaceId),
    start_storage_import_and_update(SpaceId, ScanInterval, StrategyName, StorageId).

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for starting storage import and update
%% @end
%%--------------------------------------------------------------------
-spec start_storage_import_and_update(od_space:id(), non_neg_integer(), space_strategy:name(),
    storage:id()) -> {ok, datastore:ext_key()} | datastore:update_error().
start_storage_import_and_update(SpaceId, ScanInterval, StrategyName, StorageId) ->
    start_storage_import(SpaceId, ScanInterval, StrategyName, StorageId),
    start_storage_update(SpaceId, ScanInterval, StrategyName, StorageId).


%%--------------------------------------------------------------------
%% @doc
%% Wrapper for stopping storage import.
%% @end
%%--------------------------------------------------------------------
-spec stop_storage_import(od_space:id()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
stop_storage_import(SpaceId) ->
    StorageId = get_supporting_storage(SpaceId),
    stop_storage_import(SpaceId, StorageId).

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for stopping storage import.
%% @end
%%--------------------------------------------------------------------
-spec stop_storage_import(od_space:id(), storage:id()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
stop_storage_import(SpaceId, StorageId) ->
    space_strategies:set_strategy(SpaceId, StorageId, storage_import, no_import, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for stopping storage update.
%% @end
%%--------------------------------------------------------------------
-spec stop_storage_update(od_space:id()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
stop_storage_update(SpaceId) ->
    StorageId = get_supporting_storage(SpaceId),
    stop_storage_update(SpaceId, StorageId).

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for stopping storage update.
%% @end
%%--------------------------------------------------------------------
-spec stop_storage_update(od_space:id(), storage:id()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
stop_storage_update(SpaceId, StorageId) ->
    space_strategies:set_strategy(SpaceId, StorageId, storage_update, no_import, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for stopping storage import and update.
%% @end
%%--------------------------------------------------------------------
-spec stop_storage_import_and_update(od_space:id()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
stop_storage_import_and_update(SpaceId) ->
    StorageId = get_supporting_storage(SpaceId),
    stop_storage_import_and_update(SpaceId, StorageId).

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for stopping storage import and update.
%% @end
%%--------------------------------------------------------------------
-spec stop_storage_import_and_update(od_space:id(), storage:id()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
stop_storage_import_and_update(SpaceId, StorageId) ->
    stop_storage_import(SpaceId, StorageId),
    stop_storage_update(SpaceId, StorageId).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns head of list of storages supporting given space.
%% @end
%%--------------------------------------------------------------------
-spec get_supporting_storage(od_space:id()) -> storage:id().
get_supporting_storage(SpaceId) ->
    {ok, #document{value=#space_storage{storage_ids=StorageIds}}} = space_storage:get(SpaceId),
    hd(StorageIds).
