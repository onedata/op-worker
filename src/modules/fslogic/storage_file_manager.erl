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
-include("modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

-record(sfm_handle, {
    helper_handle :: helpers:handle(),
    file :: helpers:file()
}).

-export([mkdir/3, mkdir/4, mv/2, chmod/2, chown/3, link/2]).
-export([stat/1, read/3, write/3, create/4, open/3, truncate/3, unlink/2]).

-type handle() :: #sfm_handle{}.

-export_type([handle/0]).

%%%===================================================================
%%% API
%%%===================================================================

open(Storage, Path, OpenMode) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    case helpers:open(HelperHandle, Path, OpenMode) of
        {ok, _} ->
            {ok, #sfm_handle{helper_handle = HelperHandle, file = Path}};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates a directory on storage.
%%
%% @end
%%--------------------------------------------------------------------
mkdir(Storage, Path, Mode) ->
    create(Storage, Path, Mode, false).

mkdir(Storage, Path, Mode, Recursive) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    ?error("helper:mkdir ~p ~p ~p", [HelperHandle, Path, Mode]),
    case helpers:mkdir(HelperHandle, Path, Mode) of
        ok ->
            ok;
        {error, enoent} when Recursive ->
            Tokens = fslogic_path:split(Path),
            LeafLess = fslogic_path:join(lists:sublist(Tokens, 1, length(Tokens) - 1)),
            ok = mkdir(Storage, LeafLess, 8#755, true),
            mkdir(Storage, Path, Mode, false);
        {error, Reason} ->
            {error, Reason}
    end.


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
write(#sfm_handle{helper_handle = HelperHandle, file = File}, Offset, Buffer) ->
    helpers:write(HelperHandle, File, Offset, Buffer).


%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file from storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: file_handle(), Offset :: integer(), MaxSize :: integer()) -> {ok, binary()} | error_reply().
read(#sfm_handle{helper_handle = HelperHandle, file = File}, Offset, MaxSize) ->
    helpers:read(HelperHandle, File, Offset, MaxSize).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file on storage.
%%
%% @end
%%--------------------------------------------------------------------
create(Storage, Path, Mode) ->
    create(Storage, Path, Mode, false).

create(Storage, Path, Mode, Recursive) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    ?error("helper:create ~p ~p ~p", [HelperHandle, Path, Mode]),
    case helpers:mknod(HelperHandle, Path, Mode, reg) of
        ok ->
            ok;
        {error, enoent} when Recursive ->
            Tokens = fslogic_path:split(Path),
            LeafLess = fslogic_path:join(lists:sublist(Tokens, 1, length(Tokens) - 1)),
            ok = mkdir(Storage, LeafLess, 8#333, true),
            create(Storage, Path, Mode, false);
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Truncates a file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec truncate(Storage :: #document{}, Path :: file_handle(), Size :: integer()) -> ok | error_reply().
truncate(Storage, Path, Size) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:truncate(HelperHandle, Path, Size).


%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec unlink(Path :: file_path()) -> ok | error_reply().
unlink(Storage, Path) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:unlink(HelperHandle, Path).