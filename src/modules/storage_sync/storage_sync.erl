%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Main API for storage_sync.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").


%% API
-export([start_storage_import/2, start_storage_import/3, start_storage_import/4]).

-define(DEFAULT_STRATEGY_NAME, bfs_scan).

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for starting storage import. By default uses ?DEFAULT_STRATEGY_NAME.
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
    {ok, #document{value=#space_storage{storage_ids=StorageIds}}} = space_storage:get(SpaceId),
    start_storage_import(SpaceId, ScanInterval, StrategyName, hd(StorageIds)).

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for starting storage import.
%% @end
%%--------------------------------------------------------------------
-spec start_storage_import(od_space:id(), non_neg_integer(), space_strategy:name(),
    storage:id()) -> {ok, datastore:ext_key()} | datastore:update_error().
start_storage_import(SpaceId, ScanInterval, StrategyName, StorageId) ->
    fslogic_spaces:make_space_exist(SpaceId),
    space_strategies:set_strategy(SpaceId, StorageId, storage_import,
        StrategyName, #{scan_interval => ScanInterval}).
