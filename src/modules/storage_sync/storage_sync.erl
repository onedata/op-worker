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
-include_lib("ctool/include/logging.hrl").

%% API
-export([start_simple_scan_import/4, modify_storage_import/3,
    modify_storage_import/4, stop_storage_import/1, stop_storage_update/1,
    modify_storage_update/4, modify_storage_update/3,
    start_simple_scan_update/7
]).

%%--------------------------------------------------------------------
%% @doc
%% Modifies storage_import strategy for given space
%% @end
%%--------------------------------------------------------------------
-spec modify_storage_import(od_space:id(), space_strategy:name(),
    space_strategy:arguments()) -> {ok, datastore:key()} | {error, term()}.
modify_storage_import(SpaceId, StrategyName, Args) ->
    StorageId = get_supporting_storage(SpaceId),
    modify_storage_import(SpaceId, StrategyName, StorageId, Args).

%%--------------------------------------------------------------------
%% @doc
%% Modifies storage_import strategy for given space
%% @end
%%--------------------------------------------------------------------
-spec modify_storage_import(od_space:id(), space_strategy:name(),
    storage:id(), space_strategy:arguments()) ->
    {ok, datastore:key()} | {error, term()}.
modify_storage_import(SpaceId, StrategyName, StorageId, Args) ->
    file_meta:make_space_exist(SpaceId),
    {CurrentStrategyName, _} = space_strategies:get_storage_import_details(SpaceId, StorageId),
    switch_monitoring_status(SpaceId, storage_import, CurrentStrategyName, StrategyName),
    space_strategies:set_strategy(SpaceId, StorageId, storage_import,
        StrategyName, Args).

%%--------------------------------------------------------------------
%% @doc
%% @equiv modify_storage_import(SpaceId, simple_scan, StorageId, #{max_depth =>MaxDepth}).
%% @end
%%--------------------------------------------------------------------
-spec start_simple_scan_import(od_space:id(), storage:id(), non_neg_integer(), boolean()) ->
    {ok, datastore:key()} | {error, term()}.
start_simple_scan_import(SpaceId, StorageId, MaxDepth, SyncAcl) ->
    modify_storage_import(SpaceId, simple_scan, StorageId, #{
        max_depth => MaxDepth,
        sync_acl => SyncAcl
    }).

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for stopping storage import.
%% @end
%%--------------------------------------------------------------------
-spec stop_storage_import(od_space:id()) ->
    {ok, datastore:key()} | {error, term()}.
stop_storage_import(SpaceId) ->
    StorageId = get_supporting_storage(SpaceId),
    modify_storage_import(SpaceId, no_import, StorageId, #{}).

%%--------------------------------------------------------------------
%% @doc
%% @equiv modify_storage_update(SpaceId, StrategyName, StorageId, Args).
%% @end
%%--------------------------------------------------------------------
-spec modify_storage_update(od_space:id(), space_strategy:name(),
    space_strategy:arguments()) -> {ok, datastore:key()} | {error, term()}.
modify_storage_update(SpaceId, StrategyName, Args) ->
    StorageId = get_supporting_storage(SpaceId),
    modify_storage_update(SpaceId, StrategyName, StorageId, Args).

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for starting storage update.
%% @end
%%--------------------------------------------------------------------
-spec modify_storage_update(od_space:id(),space_strategy:name(), storage:id(),
    space_strategy:arguments()) -> {ok, datastore:key()} | {error, term()}.
modify_storage_update(SpaceId, StrategyName, StorageId, Args) ->
    file_meta:make_space_exist(SpaceId),
    {CurrentImportStrategyName, _} = space_strategies:get_storage_import_details(SpaceId, StorageId),
    case {StrategyName, CurrentImportStrategyName} of
        {StrategyName, no_import} when StrategyName =/= no_update ->
            {error, import_disabled};
        _ ->
            {CurrentStrategyName, _} = space_strategies:get_storage_update_details(SpaceId, StorageId),
            switch_monitoring_status(SpaceId, storage_update, CurrentStrategyName,
                StrategyName),
            space_strategies:set_strategy(SpaceId, StorageId, storage_update,
                StrategyName, Args)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv modify_storage_update(SpaceId, simple_scan, Args).
%% @end
%%--------------------------------------------------------------------
-spec start_simple_scan_update(od_space:id(), storage:id(),
    non_neg_integer(), non_neg_integer(), boolean(), boolean(), boolean()) ->
    {ok, datastore:key()} | {error, term()}.
start_simple_scan_update(SpaceId, StorageId, MaxDepth, ScanInterval, WriteOnce,
    DeleteEnable, SyncAcl
) ->
    modify_storage_update(SpaceId, simple_scan, StorageId, #{
        max_depth => MaxDepth,
        scan_interval => ScanInterval,
        write_once => WriteOnce,
        delete_enable => DeleteEnable,
        sync_acl => SyncAcl
    }).

%%--------------------------------------------------------------------
%% @doc
%% Wrapper for stopping storage update.
%% @end
%%--------------------------------------------------------------------
-spec stop_storage_update(od_space:id()) ->
    {ok, datastore:key()} | {error, term()}.
stop_storage_update(SpaceId) ->
    modify_storage_update(SpaceId, no_update, #{}).

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
    {ok, #document{value=#space_storage{storage_ids=StorageIds}}} =
        space_storage:get(SpaceId),
    hd(StorageIds).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Turns import or update metrics on or off.
%% @end
%%-------------------------------------------------------------------
-spec switch_monitoring_status(od_space:id(), space_strategy:type(),
    space_strategy:name(), space_strategy:name()) -> ok.
switch_monitoring_status(SpaceId, storage_import, CurrentStrategyName,
    NewStrategyName) ->
    switch_import_monitoring_status(SpaceId, CurrentStrategyName,
        NewStrategyName);
switch_monitoring_status(SpaceId, storage_update, CurrentStrategyName,
    NewStrategyName) ->
    switch_update_monitoring_status(SpaceId, CurrentStrategyName,
        NewStrategyName).    

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Turns import metrics on or off, according to current and new strategy.
%% @end
%%-------------------------------------------------------------------
-spec switch_import_monitoring_status(od_space:id(), space_strategy:name(),
    space_strategy:name()) -> ok.
switch_import_monitoring_status(_SpaceId, no_import, no_import) ->
    ok;
switch_import_monitoring_status(SpaceId, _, no_import) ->
    turn_sync_monitoring_off(SpaceId);
switch_import_monitoring_status(_SpaceId, _, _) ->
    ok.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Turns update metrics on or off, according to current and new strategy.
%% @end./
%%-------------------------------------------------------------------
-spec switch_update_monitoring_status(od_space:id(), space_strategy:name(),
    space_strategy:name()) -> ok.
switch_update_monitoring_status(_SpaceId, no_update, no_update) ->
    ok;
switch_update_monitoring_status(SpaceId, _, no_update) ->
    turn_sync_monitoring_off(SpaceId);
switch_update_monitoring_status(_SpaceId, _, _) ->
    ok.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Turns import monitoring off.
%% @end
%%-------------------------------------------------------------------
-spec turn_sync_monitoring_off(od_space:id()) -> ok.
turn_sync_monitoring_off(SpaceId) ->
    storage_sync_monitoring:stop_counters(SpaceId).