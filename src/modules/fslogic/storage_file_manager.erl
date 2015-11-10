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
-include("cluster/worker/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% File handle used by the module
-record(sfm_handle, {
    helper_handle :: helpers:handle(),
    file :: helpers:file()
}).

-export([mkdir/3, mkdir/4, mv/2, chmod/3, chown/3, link/2]).
-export([stat/1, read/3, write/3, create/3, create/4, open/3, truncate/3, unlink/2]).

-type handle() :: #sfm_handle{}.

-export_type([handle/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Opens the file. To used opened descriptor, pass returned handle to other functions.
%% File may and should be closed with release/1, but file will be closed automatically
%% when handle goes out of scope (term will be released by Erlang's GC).
%% @end
%%--------------------------------------------------------------------
-spec open(Storage :: datastore:document(), FileId :: helpers:file(), OpenMode :: helpers:open_mode()) ->
    {ok, handle()} | error_reply().
open(Storage, FileId, OpenMode) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    case helpers:open(HelperHandle, FileId, OpenMode) of
        {ok, _} ->
            {ok, #sfm_handle{helper_handle = HelperHandle, file = FileId}};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates a directory on storage.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(Storage :: datastore:document(), FileId :: helpers:file(), Mode :: non_neg_integer()) ->
    ok | error_reply().
mkdir(Storage, FileId, Mode) ->
    mkdir(Storage, FileId, Mode, false).

%%--------------------------------------------------------------------
%% @doc
%% Creates a directory on storage. Recursive states whether parent directories shall be also created.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(Storage :: datastore:document(), FileId :: helpers:file(), Mode :: non_neg_integer(), Recursive :: boolean()) ->
    ok | error_reply().
mkdir(Storage, FileId, Mode, Recursive) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    case helpers:mkdir(HelperHandle, FileId, Mode) of
        ok ->
            ok;
        {error, enoent} when Recursive ->
            Tokens = fslogic_path:split(FileId),
            case Tokens of
                [_] -> ok;
                [_ | _]  ->
                    LeafLess = fslogic_path:dirname(Tokens),
                    ok = mkdir(Storage, LeafLess, ?AUTO_CREATED_PARENT_DIR_MODE, true)
            end,
            mkdir(Storage, FileId, Mode, false);
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec mv(FileHandleFrom :: handle(), PathOnStorageTo :: file_path()) -> ok | error_reply().
mv(_FileHandleFrom, _PathOnStorageTo) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Changes the permissions of a file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec chmod(Storage :: datastore:document(), File :: helpers:file(), NewMode :: perms_octal()) -> ok | error_reply().
chmod(Storage, File, Mode) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:chmod(HelperHandle, File, Mode).


%%--------------------------------------------------------------------
%% @doc
%% Changes owner of a file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec chown(FileHandle :: handle(), User :: user_id(), Group :: group_id()) -> ok | error_reply().
chown(_FileHandle, _User, _Group) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Creates a symbolic link on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec link(Path :: binary(), TargetFileHandle :: handle()) -> {ok, file_uuid()} | error_reply().
link(_Path, _TargetFileHandle) ->
    {ok, <<"">>}.


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes, reading them from storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec stat(FileHandle :: handle()) -> {ok, undefined} | error_reply().
stat(_FileHandle) ->
    {ok, undefined}.


%%--------------------------------------------------------------------
%% @doc
%% Writes data to a file on storage. Returns number of written bytes.
%%
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: handle(), Offset :: non_neg_integer(), Buffer :: binary()) -> {ok, non_neg_integer()} | error_reply().
write(#sfm_handle{helper_handle = HelperHandle, file = File}, Offset, Buffer) ->
    helpers:write(HelperHandle, File, Offset, Buffer).


%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file from storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: handle(), Offset :: non_neg_integer(), MaxSize :: non_neg_integer()) -> {ok, binary()} | error_reply().
read(#sfm_handle{helper_handle = HelperHandle, file = File}, Offset, MaxSize) ->
    helpers:read(HelperHandle, File, Offset, MaxSize).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec create(Storage :: datastore:document(), Path :: helpers:file(), Mode :: non_neg_integer()) ->
    ok | error_reply().
create(Storage, Path, Mode) ->
    create(Storage, Path, Mode, false).

-spec create(Storage :: datastore:document(), Path :: helpers:file(), Mode :: non_neg_integer(), Recursive :: boolean()) ->
    ok | error_reply().
create(Storage, Path, Mode, Recursive) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    case helpers:mknod(HelperHandle, Path, Mode, reg) of
        ok ->
            ok;
        {error, enoent} when Recursive ->
            Tokens = fslogic_path:split(Path),
            LeafLess = fslogic_path:join(lists:sublist(Tokens, 1, length(Tokens) - 1)),
            ok =
                case mkdir(Storage, LeafLess, 8#333, true) of
                    ok -> ok;
                    {error, eexist} -> ok;
                    E0 -> E0
                end,
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
-spec truncate(Storage :: #document{}, Path :: helpers:file(), Size :: integer()) -> ok | error_reply().
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
-spec unlink(Storage :: #document{}, Path :: file_path()) -> ok | error_reply().
unlink(Storage, Path) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:unlink(HelperHandle, Path).