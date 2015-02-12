%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module provides high level file system operations that
%%% operates directly on storage.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_file_manager).

-include("types.hrl").
-include("errors.hrl").

-export([mkdir/1, mkdir/2, mv/2, chmod/2, chown/3, link/2]).
-export([stat/1, read/3, write/3, create/1, create/2, truncate/2, rm/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a directory on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec mkdir(Path :: file_path()) -> {ok, file_id()} | error_reply().
mkdir(Path) ->
    DefaultMode = 777, % TODO retrieve default mode
    mkdir(Path, DefaultMode).

%%--------------------------------------------------------------------
%% @doc
%% Creates a directory on storage with given perms.
%%
%% @end
%%--------------------------------------------------------------------
-spec mkdir(Path :: file_path(), Mode :: perms_octal()) -> {ok, file_id()} | error_reply().
mkdir(Path, Mode) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec mv(FileHandleFrom :: file_handle(), PathOnStorageTo :: file_path()) -> ok | error_reply().
mv(FileHandleFrom, PathOnStorageTo) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Changes the permissions of a file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec chmod(FileHandle :: file_handle(), NewPerms :: perms_octal()) -> ok | error_reply().
chmod(FileHandle, NewPerms) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Changes owner of a file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec chown(FileHandle :: file_handle(), User :: user_id(), Group :: group_id()) -> ok | error_reply().
chown(FileHandle, User, Group) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Creates a symbolic link on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec link(Path :: binary(), TargetFileHandle :: file_handle()) -> {ok, file_id()} | error_reply().
link(Path, TargetFileHandle) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes, reading them from storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec stat(FileHandle :: file_handle()) -> {ok, file_attributes()} | error_reply().
stat(FileHandle) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Writes data to a file on storage. Returns number of written bytes.
%%
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: file_handle(), Offset :: integer(), Buffer :: binary()) -> {ok, integer()} | error_reply().
write(FileHandle, Offset, Buffer) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file from storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: file_handle(), Offset :: integer(), MaxSize :: integer()) -> {ok, binary()} | error_reply().
read(FileHandle, Offset, MaxSize) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec create(Path :: file_path()) -> {ok, file_id()} | error_reply().
create(Path) ->
    DefaultMode = 777, % TODO retrieve default mode
    create(Path, DefaultMode).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file on storage with given permissions.
%%
%% @end
%%--------------------------------------------------------------------
-spec create(Path :: file_path(), Mode :: perms_octal()) -> {ok, file_id()} | error_reply().
create(Path, Mode) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Truncates a file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec truncate(FileHandle :: file_handle(), Size :: integer()) -> ok | error_reply().
truncate(FileHandle, Size) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec rm(Path :: file_path()) -> ok | error_reply().
rm(Path) ->
    error(not_implemented).