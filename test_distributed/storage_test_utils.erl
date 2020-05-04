%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(storage_test_utils).
-author("Jakub Kudzia").

%% API
-export([read_file/2, read_file_info/2, list_dir/2,
    space_path/2, file_path/3,
    get_space_mount_point/2, get_supporting_storage_id/2, storage_mount_point/2, get_helper/2]).


%%%===================================================================
%%% API functions
%%%===================================================================

read_file(Worker, FilePath) ->
    rpc:call(Worker, file, read_file, [FilePath]).

read_file_info(Worker, FilePath) ->
    rpc:call(Worker, file, read_file_info, [FilePath]).

list_dir(Worker, DirPath) ->
    rpc:call(Worker, file, list_dir, [DirPath]).

space_path(Worker, SpaceId) ->
    file_path(Worker, SpaceId, <<"">>).

file_path(Worker, SpaceId, FilePath) ->
    SpaceMnt = get_space_mount_point(Worker, SpaceId),
    filename:join([SpaceMnt, FilePath]).

get_space_mount_point(Worker, SpaceId) ->
    {ok, StorageId} = get_supporting_storage_id(Worker, SpaceId),
    IsImportedStorage = rpc:call(Worker, storage, is_imported_storage, [StorageId]),
    StorageMountPoint = storage_mount_point(Worker, StorageId),
    case IsImportedStorage of
        true -> StorageMountPoint;
        false -> filename:join([StorageMountPoint, SpaceId])
    end.

get_supporting_storage_id(Worker, SpaceId) ->
    rpc:call(Worker, space_logic, get_local_storage_id, [SpaceId]).

get_helper(Worker, StorageId) ->
    rpc:call(Worker, storage, get_helper, [StorageId]).

storage_mount_point(Worker, StorageId) ->
    Helper = get_helper(Worker, StorageId),
    HelperArgs = helper:get_args(Helper),
    maps:get(<<"mountPoint">>, HelperArgs).
