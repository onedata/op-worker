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
-include("modules/storage_sync/storage_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    start_simple_scan_import/4, stop_storage_import/1,
    modify_storage_import/3, modify_storage_import/4,
    start_simple_scan_update/7, stop_storage_update/1,
    modify_storage_update/4, modify_storage_update/3,
    is_syncable/1]).

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
    storage_sync_monitoring:ensure_created(SpaceId, StorageId),
    file_meta:make_space_exist(SpaceId),
    space_strategies:set_strategy(SpaceId, StorageId, storage_import,
        StrategyName, Args).

%%--------------------------------------------------------------------
%% @doc
%% @equiv modify_storage_import(SpaceId, simple_scan, StorageId, #{max_depth =>MaxDepth}).
%% @end
%%--------------------------------------------------------------------
-spec start_simple_scan_import(od_space:id(), storage:id(), non_neg_integer(),
    boolean()) -> {ok, datastore:key()} | {error, term()}.
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
    storage_sync_monitoring:ensure_created(SpaceId, StorageId),
    file_meta:make_space_exist(SpaceId),
    {CurrentImportStrategyName, _} =
        space_strategies:get_storage_import_details(SpaceId, StorageId),
    case {StrategyName, CurrentImportStrategyName} of
        {StrategyName, no_import} when StrategyName =/= no_update ->
            {error, import_disabled};
        _ ->
            case maps:get(delete_enable, Args, undefined) of
                true ->
                    Helper = storage:get_helper(StorageId),
                    HelperName = helper:get_name(Helper),
                    case HelperName =:= ?S3_HELPER_NAME of
                        true ->
                            {error, 'Detecting deletions not implemented on s3'};
                        false ->
                            space_strategies:set_strategy(SpaceId, StorageId, storage_update,
                                StrategyName, Args)
                    end;
                _ ->
                    space_strategies:set_strategy(SpaceId, StorageId, storage_update,
                        StrategyName, Args)
            end
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

-spec is_syncable(storage:doc() | storage:id()) -> {true, space_strategy:sync_mode()} | false.
is_syncable(StorageDoc = #document{value = #storage{}}) ->
    Helper = storage:get_helper(StorageDoc),
    HelperName = helper:get_name(Helper),
    StoragePathType = helper:get_storage_path_type(Helper),
    case lists:member(HelperName, [?POSIX_HELPER_NAME, ?GLUSTERFS_HELPER_NAME, ?NULL_DEVICE_HELPER_NAME, ?WEBDAV_HELPER_NAME]) of
        true ->
            {true, ?STORAGE_SYNC_POSIX_MODE};
        false when HelperName =:= ?S3_HELPER_NAME andalso StoragePathType =:= ?CANONICAL_STORAGE_PATH ->
            Args = helper:get_args(Helper),
            case maps:get(<<"blockSize">>, Args, undefined) of
                <<"0">> ->
                    {true, ?STORAGE_SYNC_OBJECT_MODE};
                _ ->
                    false
            end;
        false ->
            false
    end;
is_syncable(StorageId) when is_binary(StorageId) ->
    case storage:get(StorageId) of
        {ok, StorageDoc} ->
            is_syncable(StorageDoc);
        _ ->
            false
    end.

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
