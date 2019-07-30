%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(sfm_test_utils).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/posix/errors.hrl").


%% API
-export([get_storage_id/2, get_storage_doc/2, new_handle/3, new_child_handle/2]).
-export([mkdir/3, create_file/3, write_file/4, read_file/4, unlink/3, chown/4,
    chmod/3, stat/2, ls/4, rmdir/2, truncate/4, recursive_rm/2, open/3]).

-define(DEFAULT_TIMEOUT, timer:minutes(1)).

%%%===================================================================
%%% API functions
%%%===================================================================

get_storage_id(Worker, SpaceId) ->
    [StorageId | _] = rpc:call(Worker, space_storage, get_storage_ids, [SpaceId]),
    StorageId.

get_storage_doc(Worker, StorageId) ->
    rpc:call(Worker, storage, get, [StorageId]).

new_handle(Worker, SpaceId, StorageFileId) ->
    StorageId = get_storage_id(Worker, SpaceId),
    {ok, StorageDoc} = get_storage_doc(Worker, StorageId),
    rpc:call(Worker, storage_file_manager, new_handle,
        [?ROOT_SESS_ID, SpaceId, undefined, StorageDoc, StorageFileId, undefined]).

new_child_handle(ParentHandle, ChildName) ->
    storage_file_manager:get_child_handle(ParentHandle, ChildName).

mkdir(Worker, SFMHandle, Mode) ->
    ok = rpc:call(Worker, storage_file_manager, mkdir, [SFMHandle, Mode, true]).

create_file(Worker, SFMHandle, Mode) ->
    ok = rpc:call(Worker, storage_file_manager, create, [SFMHandle, Mode, true]).

open(Worker, SFMHandle, Flag) ->
    rpc:call(Worker, storage_file_manager, open, [SFMHandle, Flag]).

write_file(Node, SFMHandle, Offset, Data) ->
    % this function opens and writes to file to ensure that file_handle is not deleted
    % after RPC process dies
    Self = self(),
    rpc:call(Node, erlang, spawn, [fun() ->
        Result = try
            case storage_file_manager:open_insecure(SFMHandle, write) of
                {ok, SFMHandle2} ->
                    storage_file_manager:write(SFMHandle2, Offset, Data);
                Error = {error, _} ->
                    Error
            end
        catch
            E:R ->
                {error, {E, R}}
        end,
        Self ! {result, Result}
    end]),
    receive
        {result, Result} -> Result
    after
        ?DEFAULT_TIMEOUT ->
            ct:fail("write file timeout")
    end.

read_file(Node, SFMHandle, Offset, Size) ->
    % this function opens and writes to file to ensure that file_handle is not deleted
    % after RPC process dies
    Self = self(),
    rpc:call(Node, erlang, spawn, [fun() ->
        Result = try
            case storage_file_manager:open_insecure(SFMHandle, read) of
                {ok, SFMHandle2} ->
                    storage_file_manager:read(SFMHandle2, Offset, Size);
                Error = {error, _} ->
                    Error
            end
        catch
            E:R ->
                {E, R}
        end,
        Self ! {result, Result}
    end]),
    receive
        {result, Result} -> Result
    after
        ?DEFAULT_TIMEOUT ->
            ct:fail("read file timeout")
    end.

unlink(Worker, SFMHandle, Size) ->
    ok = rpc:call(Worker, storage_file_manager, unlink, [SFMHandle, Size]).

chown(Worker, SFMHandle, Uid, Gid) ->
    ok = rpc:call(Worker, storage_file_manager, chown, [SFMHandle, Uid, Gid]).

chmod(Worker, SFMHandle, Mode) ->
    ok = rpc:call(Worker, storage_file_manager, chmod, [SFMHandle, Mode]).

stat(Worker, SFMHandle) ->
    rpc:call(Worker, storage_file_manager, stat, [SFMHandle]).

ls(Worker, SFMHandle, Offset, Count) ->
    rpc:call(Worker, storage_file_manager, readdir, [SFMHandle, Offset, Count]).

rmdir(Worker, SFMHandle) ->
    ok = rpc:call(Worker, storage_file_manager, rmdir, [SFMHandle]).

truncate(Worker, SFMHandle, NewSize, CurrentSize) ->
    ok = rpc:call(Worker, storage_file_manager, truncate, [SFMHandle, NewSize, CurrentSize]).

type(Worker, SFMHandle) ->
    case stat(Worker, SFMHandle) of
        {ok, #statbuf{st_mode = Mode}} ->
            file_meta:type(Mode);
        Error ->
            Error
    end.

size(Worker, SFMHandle) ->
    case stat(Worker, SFMHandle) of
        {ok, #statbuf{st_size = Size}} ->
            Size;
        Error ->
            Error
    end.

recursive_rm(Worker, SFMHandle) ->
    case type(Worker, SFMHandle) of
        ?REGULAR_FILE_TYPE ->
            case size(Worker, SFMHandle) of
                {error, ?ENOENT} ->
                    ok;
                Size ->
                    unlink(Worker, SFMHandle, Size)
            end;
        ?DIRECTORY_TYPE ->
            recursive_rm(Worker, SFMHandle, 0, 1000);
        {error, ?ENOENT} ->
            ok
    end.

recursive_rm(Worker, SFMHandle = #sfm_handle{storage = StorageDoc}, Offset, Count) ->
    Helper = storage:get_helper(StorageDoc),
    HelperName = helper:get_name(Helper),
    case HelperName of
        ?POSIX_HELPER_NAME ->
            case ls(Worker, SFMHandle, Offset, Count) of
                {ok, Children} when length(Children) < Count ->
                    rm_children(Worker, SFMHandle, Children),
                    rmdir(Worker, SFMHandle);
                {ok, Children} ->
                    rm_children(Worker, SFMHandle, Children),
                    recursive_rm(Worker, SFMHandle, Offset + Count, Count)
            end;
        ?S3_HELPER_NAME ->
            todo
    end.


rm_children(Worker, ParentHandle, ChildrenNames) ->
    lists:foreach(fun(ChildName) ->
        ChildHandle = new_child_handle(ParentHandle, ChildName),
        recursive_rm(Worker, ChildHandle)
    end, ChildrenNames).