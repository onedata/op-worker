%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Util functions for performing operations on storage using storage_driver
%%% in tests.
%%% @end
%%%-------------------------------------------------------------------
-module(sd_test_utils).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([get_storage_id/2, get_storage_doc/2, new_handle/4, new_child_handle/2,
    setup_test_files_structure/3, setup_test_files_structure/4, recursive_rm/3]).
-export([mkdir/3, create_file/3, write_file/4, read_file/4, unlink/3, chown/4,
    chmod/3, stat/2, ls/4, rmdir/2, truncate/4, recursive_rm/2, open/3, listobjects/5]).

-define(DEFAULT_TIMEOUT, timer:minutes(1)).

%%%===================================================================
%%% API functions
%%%===================================================================

get_storage_id(Worker, SpaceId) ->
    {ok, [StorageId | _]} = rpc:call(Worker, space_storage, get_storage_ids, [SpaceId]),
    StorageId.

get_storage_doc(Worker, StorageId) ->
    rpc:call(Worker, storage, get, [StorageId]).

new_handle(Worker, SpaceId, StorageFileId, #document{key = StorageId}) ->
    new_handle(Worker, SpaceId, StorageFileId, StorageId);
new_handle(Worker, SpaceId, StorageFileId, StorageId) when is_binary(StorageId) ->
    rpc:call(Worker, storage_driver, new_handle,
        [?ROOT_SESS_ID, SpaceId, undefined, StorageId, StorageFileId, undefined]).

new_child_handle(ParentHandle, ChildName) ->
    storage_driver:get_child_handle(ParentHandle, ChildName).

mkdir(Worker, SDHandle, Mode) ->
    rpc:call(Worker, storage_driver, mkdir, [SDHandle, Mode, true]).

create_file(Worker, SDHandle, Mode) ->
    ok = rpc:call(Worker, storage_driver, create, [SDHandle, Mode, true]).

open(Worker, SDHandle, Flag) ->
    rpc:call(Worker, storage_driver, open_insecure, [SDHandle, Flag]).

write_file(Node, SDHandle, Offset, Data) ->
    % this function opens and writes to file to ensure that file_handle is not deleted
    % after RPC process dies
    Self = self(),
    rpc:call(Node, erlang, spawn, [fun() ->
        Result = try
            case storage_driver:open_insecure(SDHandle, write) of
                {ok, SDHandle2} ->
                    storage_driver:write(SDHandle2, Offset, Data);
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

read_file(Node, SDHandle, Offset, Size) ->
    % this function opens and writes to file to ensure that file_handle is not deleted
    % after RPC process dies
    Self = self(),
    rpc:call(Node, erlang, spawn, [fun() ->
        Result = try
            case storage_driver:open_insecure(SDHandle, read) of
                {ok, SDHandle2} ->
                    storage_driver:read(SDHandle2, Offset, Size);
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

unlink(Worker, SDHandle, Size) ->
    ok = rpc:call(Worker, storage_driver, unlink, [SDHandle, Size]).

chown(Worker, SDHandle, Uid, Gid) ->
    ok = rpc:call(Worker, storage_driver, chown, [SDHandle, Uid, Gid]).

chmod(Worker, SDHandle, Mode) ->
    ok = rpc:call(Worker, storage_driver, chmod, [SDHandle, Mode]).

stat(Worker, SDHandle) ->
    rpc:call(Worker, storage_driver, stat, [SDHandle]).

ls(Worker, SDHandle, Offset, Count) ->
    rpc:call(Worker, storage_driver, readdir, [SDHandle, Offset, Count]).

listobjects(Worker, SDHandle, Marker, Offset, Count) ->
    rpc:call(Worker, storage_driver, listobjects, [SDHandle, Marker, Offset, Count]).

rmdir(Worker, SDHandle) ->
    rpc:call(Worker, storage_driver, rmdir, [SDHandle]).

truncate(Worker, SDHandle, NewSize, CurrentSize) ->
    ok = rpc:call(Worker, storage_driver, truncate, [SDHandle, NewSize, CurrentSize]).

type(Worker, SDHandle) ->
    case stat(Worker, SDHandle) of
        {ok, #statbuf{st_mode = Mode}} ->
            file_meta:type(Mode);
        Error ->
            Error
    end.

size(Worker, SDHandle) ->
    case stat(Worker, SDHandle) of
        {ok, #statbuf{st_size = Size}} ->
            Size;
        Error ->
            Error
    end.

recursive_rm(Worker, SDHandle) ->
    recursive_rm(Worker, SDHandle, false).

recursive_rm(Worker, SDHandle = #sd_handle{storage_id = StorageId}, DoNotDeleteRoot) ->
    case type(Worker, SDHandle) of
        ?REGULAR_FILE_TYPE ->
            case size(Worker, SDHandle) of
                {error, ?ENOENT} ->
                    ok;
                Size ->
                    unlink(Worker, SDHandle, Size)
            end;
        ?DIRECTORY_TYPE ->
            {ok, Storage} = rpc:call(Worker, storage, get, [StorageId]),
            Helper = storage_config:get_helper(Storage),
            HelperName = helper:get_name(Helper),
            case HelperName of
                ?POSIX_HELPER_NAME ->
                    recursive_rm_posix(Worker, SDHandle, 0, 1000, DoNotDeleteRoot);
                ?S3_HELPER_NAME ->
                    recursive_rm_s3(Worker, SDHandle, <<"/">>, 0, 1000)
            end;
        {error, ?ENOENT} ->
            ok
    end.

recursive_rm_posix(Worker, SDHandle, Offset, Count, DoNotDeleteRoot) ->
    case ls(Worker, SDHandle, Offset, Count) of
        {ok, Children} when length(Children) < Count ->
            rm_children(Worker, SDHandle, Children),
            case DoNotDeleteRoot of
                true ->
                    ok;
                false ->
                    case rmdir(Worker, SDHandle) of
                        ok -> ok;
                        {error, ?EBUSY} -> ok;
                        {error, ?ENOENT} -> ok
                    end
            end;
        {ok, Children} ->
            rm_children(Worker, SDHandle, Children),
            recursive_rm_posix(Worker, SDHandle, Offset + Count, Count, false)
    end.

recursive_rm_s3(Worker, SDHandle, Marker, Offset, Count) ->
    case listobjects(Worker, SDHandle, Marker, Offset, Count) of
        {ok, Children} when length(Children) < Count ->
            rm_children(Worker, SDHandle, Children),
            ok;
        {ok, Children} ->
            rm_children(Worker, SDHandle, Children),
            NewMarker = lists:last(Children),
            recursive_rm_s3(Worker, SDHandle, NewMarker, Offset, Count)
    end.


rm_children(Worker, ParentHandle, ChildrenNames) ->
    lists:foreach(fun(ChildName) ->
        ChildHandle = new_child_handle(ParentHandle, ChildName),
        recursive_rm(Worker, ChildHandle)
    end, ChildrenNames).


setup_test_files_structure(W, RootHandle, Structure) ->
    setup_test_files_structure(W, RootHandle, Structure, [], [], false).

setup_test_files_structure(W, RootHandle, Structure, OnlyGenerateNames) ->
    setup_test_files_structure(W, RootHandle, Structure, [], [], OnlyGenerateNames).

setup_test_files_structure(_W, _RootHandle, [], _CreatedDirs, _CreatedFiles, _OnlyGenerateNames) ->
    {[], []};
setup_test_files_structure(W, RootHandle, [{Dirs, Files}], CreatedDirs, CreatedFiles, OnlyGenerateNames) ->
    CreatedDirs2 = lists:foldl(fun(I, CreatedDirsIn) ->
        SubDirHandle = new_child_handle(RootHandle, <<"dir", (integer_to_binary(I))/binary>>),
        case OnlyGenerateNames of
            true ->
                ok;
            false ->
                ok = sd_test_utils:mkdir(W, SubDirHandle, 8#755)
        end,
        NewDirId = storage_driver:get_storage_file_id(SubDirHandle),
        [NewDirId | CreatedDirsIn]
    end, CreatedDirs, lists:seq(1, Dirs)),
    {CreatedDirs2, create_files(W, RootHandle, Files, CreatedFiles, OnlyGenerateNames)};
setup_test_files_structure(W, RootHandle, [{Dirs, Files} | Rest], CreatedDirs, CreatedFiles, OnlyGenerateNames) ->
    {CreatedDirs2, CreatedFiles2} = lists:foldl(fun(I, {CreatedDirsIn, CreatedFilesIn}) ->
        SubDirHandle = new_child_handle(RootHandle, <<"dir", (integer_to_binary(I))/binary>>),
        case OnlyGenerateNames of
            true ->
                ok;
            false ->
                ok = sd_test_utils:mkdir(W, SubDirHandle, 8#755)
        end,
        {CreatedDirsIn2, CreatedFilesIn2} =
            setup_test_files_structure(W, SubDirHandle, Rest, CreatedDirsIn, CreatedFilesIn, OnlyGenerateNames),
        NewDirId = storage_driver:get_storage_file_id(SubDirHandle),
        {[NewDirId | CreatedDirsIn2], CreatedFilesIn2}
    end, {CreatedDirs, CreatedFiles}, lists:seq(1, Dirs)),
    {CreatedDirs2, create_files(W, RootHandle, Files, CreatedFiles2, OnlyGenerateNames)}.

create_files(W, RootHandle, FilesNum, CreatedFiles, OnlyGenerateNames) ->
    lists:foldl(fun(I, CreatedFilesIn) ->
        SubDirHandle = new_child_handle(RootHandle, <<"file", (integer_to_binary(I))/binary>>),
        case OnlyGenerateNames of
            true ->
                ok;
            false ->
                ok = sd_test_utils:create_file(W, SubDirHandle, 8#664)
        end,
        NewFileId = storage_driver:get_storage_file_id(SubDirHandle),
        [NewFileId | CreatedFilesIn]
    end, CreatedFiles, lists:seq(1, FilesNum)).