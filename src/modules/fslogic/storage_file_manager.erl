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
mkdir(_Path, _Mode) ->
    {ok, <<"">>}.


%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec mv(FileHandleFrom :: file_handle(), PathOnStorageTo :: file_path()) -> ok | error_reply().
mv(_FileHandleFrom, _PathOnStorageTo) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Changes the permissions of a file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec chmod(FileHandle :: file_handle(), NewPerms :: perms_octal()) -> ok | error_reply().
chmod(_FileHandle, _NewPerms) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Changes owner of a file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec chown(FileHandle :: file_handle(), User :: user_id(), Group :: group_id()) -> ok | error_reply().
chown(_FileHandle, _User, _Group) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Creates a symbolic link on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec link(Path :: binary(), TargetFileHandle :: file_handle()) -> {ok, file_id()} | error_reply().
link(_Path, _TargetFileHandle) ->
    {ok, <<"">>}.


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes, reading them from storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec stat(FileHandle :: file_handle()) -> {ok, file_attributes()} | error_reply().
stat(_FileHandle) ->
    {ok, undefined}.


%%--------------------------------------------------------------------
%% @doc
%% Writes data to a file on storage. Returns number of written bytes.
%%
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: file_handle(), Offset :: integer(), Buffer :: binary()) -> {ok, integer()} | error_reply().
write(_FileHandle, _Offset, _Buffer) ->
    {ok, 0}.


%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file from storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: file_handle(), Offset :: integer(), MaxSize :: integer()) -> {ok, binary()} | error_reply().
read(_FileHandle, _Offset, _MaxSize) ->
    {ok, <<"">>}.


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
create(_Path, _Mode) ->
    {ok, <<"">>}.


%%--------------------------------------------------------------------
%% @doc
%% Truncates a file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec truncate(FileHandle :: file_handle(), Size :: integer()) -> ok | error_reply().
truncate(_FileHandle, _Size) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec rm(Path :: file_path()) -> ok | error_reply().
rm(_Path) ->
    ok.