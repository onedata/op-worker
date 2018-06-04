%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides high level file system operations that
%%% operates directly on storage.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_file_manager).

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("storage_file_manager_errors.hrl").
-include_lib("ctool/include/logging.hrl").

-export([new_handle/2, new_handle/6, set_size/1, increase_size/2]).
-export([mkdir/2, mkdir/3, mv/2, chmod/2, chown/4, link/2, readdir/3,
    get_child_handle/2]).
-export([stat/1, read/3, write/3, create/2, create/3, open/2, release/1,
    truncate/2, unlink/1, fsync/2, rmdir/1]).
-export([setxattr/5, getxattr/2, removexattr/2, listxattr/1]).
-export([open_at_creation/1]).

-type handle() :: #sfm_handle{}.
-type handle_id() :: binary().
-type error_reply() :: {error, term()}.

-export_type([handle/0, handle_id/0, error_reply/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv new_handle(SessionId, SpaceUuid, FileUuid, Storage, FileId, undefined).
%% @end
%%--------------------------------------------------------------------
-spec new_handle(session:id(), file_ctx:ctx()) -> {handle(), file_ctx:ctx()}.
new_handle(SessionId, FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {Storage, FileCtx2} = file_ctx:get_storage_doc(FileCtx),
    {FileId, FileCtx3} = file_ctx:get_storage_file_id(FileCtx2),
    ShareId = file_ctx:get_share_id_const(FileCtx3),
    {new_handle(SessionId, SpaceId, FileUuid, Storage, FileId, ShareId), FileCtx3}.

%%--------------------------------------------------------------------
%% @doc
%% Opens the file. To use opened descriptor, pass returned handle to other functions.
%% File may and should be closed with release/1, but file will be closed automatically
%% when handle goes out of scope (term will be released by Erlang's GC).
%% Handle created by this function may not be used for remote files.
%% @end
%%--------------------------------------------------------------------
-spec new_handle(session:id(), od_space:id(), file_meta:uuid(),
    Storage :: datastore:doc(), FileId :: helpers:file_id(),
    ShareId :: od_share:id() | undefined) -> handle().
new_handle(SessionId, SpaceId, FileUuid, #document{key = StorageId} = Storage,
    FileId, ShareId) ->
    StorageFileId = case space_strategies:is_import_on(SpaceId) of
        true ->
            filename_mapping:to_storage_path(SpaceId, StorageId, FileId);
        _ ->
            FileId
    end,
    #sfm_handle{
        session_id = SessionId,
        space_id = SpaceId,
        file_uuid = FileUuid,
        file = StorageFileId,
        storage = Storage,
        share_id = ShareId
    }.

%%--------------------------------------------------------------------
%% @doc
%% Sets size in handle.
%% @end
%%--------------------------------------------------------------------
-spec set_size(handle()) -> handle().
set_size(#sfm_handle{
    space_id = SpaceId,
    file_uuid = FileUuid
} = Handle) ->
    FSize = get_size(FileUuid, SpaceId),
    Handle#sfm_handle{
        file_size = FSize
    }.

%%--------------------------------------------------------------------
%% @doc
%% Sets size in handle.
%% @end
%%--------------------------------------------------------------------
-spec increase_size(handle(), non_neg_integer()) -> handle().
increase_size(SFMHandle = #sfm_handle{file_size = CurrentSize}, Increase) ->
    SFMHandle#sfm_handle{
        file_size = CurrentSize + Increase
    }.

%%--------------------------------------------------------------------
%% @doc
%% Opens the file. To used opened descriptor, pass returned handle to other functions.
%% File may and should be closed with release/1, but file will be closed automatically
%% when handle goes out of scope (term will be released by Erlang's GC).
%% @end
%%--------------------------------------------------------------------
-spec open(handle(), OpenFlag :: helpers:open_flag()) ->
    {ok, handle()} | error_reply().
open(SFMHandle, read) ->
    open_for_read(SFMHandle);
open(SFMHandle, write) ->
    open_for_write(SFMHandle);
open(SFMHandle, rdwr) ->
    open_for_rdwr(SFMHandle).

%%--------------------------------------------------------------------
%% @doc
%% Opens the file. To used opened descriptor, pass returned handle to other functions.
%% File may and should be closed with release/1, but file will be closed automatically
%% when handle goes out of scope (term will be released by Erlang's GC).
%% Bypasses permissions check to allow to open file at creation.
%% @end
%%--------------------------------------------------------------------
-spec open_at_creation(handle()) -> {ok, handle()} | error_reply().
open_at_creation(SFMHandle) ->
    open_insecure(SFMHandle#sfm_handle{session_id = ?ROOT_SESS_ID}, rdwr).

%%--------------------------------------------------------------------
%% @doc
%% Closes the file.
%% @end
%%--------------------------------------------------------------------
-spec release(handle()) -> ok | error_reply().
release(#sfm_handle{file_handle = FileHandle}) ->
    helpers:release(FileHandle).

%%--------------------------------------------------------------------
%% @doc
%% Creates a directory on storage.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(handle(), Mode :: non_neg_integer()) -> ok | error_reply().
mkdir(Handle, Mode) ->
    mkdir(Handle, Mode, false).

%%--------------------------------------------------------------------
%% @doc
%% Creates a directory on storage. Recursive states whether parent directories
%% shall be also created.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(handle(), Mode :: non_neg_integer(), Recursive :: boolean()) ->
    ok | error_reply().
mkdir(#sfm_handle{
    storage = Storage,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
} = SFMHandle, Mode, Recursive) ->
    Noop = fun(_) -> ok end,

    {ok, HelperHandle} = session:get_helper(SessionId, SpaceId, Storage),

    case helpers:mkdir(HelperHandle, FileId, Mode) of
        ok ->
            ok;
        {error, enoent} when Recursive ->
            Tokens = fslogic_path:split(FileId),
            case Tokens of
                [_] -> ok;
                [_ | _] ->
                    LeafLess = filename:dirname(FileId),
                    case mkdir(SFMHandle#sfm_handle{file = LeafLess},
                        ?AUTO_CREATED_PARENT_DIR_MODE, true)
                    of
                        ok -> ok;
                        {error, eexist} -> ok;
                        ParentError ->
                            ?error("Cannot create parent for file ~p, error ~p",
                                [FileId, ParentError]),
                            throw(ParentError)
                    end
            end,
            R = case mkdir(SFMHandle, Mode, false) of
                ok ->
                    chmod(SFMHandle, Mode); %% @todo: find out why umask(0) in helpers_nif.cc doesn't work
                E -> E
            end,
            Noop(HelperHandle), %% @todo: check why NIF crashes when this term is destroyed before recursive call
            R;
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location on storage.
%% @end
%%--------------------------------------------------------------------
-spec mv(FileHandleFrom :: handle(), FileTo :: helpers:file_id()) ->
    ok | error_reply().
mv(#sfm_handle{
    storage = Storage,
    file = FileFrom,
    space_id = SpaceId,
    session_id = SessionId
}, FileTo) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceId, Storage),
    helpers:rename(HelperHandle, FileFrom, FileTo).


%%--------------------------------------------------------------------
%% @doc
%% Changes the permissions of a file on storage.
%% @end
%%--------------------------------------------------------------------
-spec chmod(handle(), NewMode :: file_meta:posix_permissions()) ->
    ok | error_reply().
chmod(#sfm_handle{
    storage = Storage,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}, Mode) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceId, Storage),
    helpers:chmod(HelperHandle, FileId, Mode).


%%--------------------------------------------------------------------
%% @doc
%% Changes owner of a file on storage.
%% @end
%%--------------------------------------------------------------------
-spec chown(FileHandle :: handle(), user_id(), group_id()| undefined, od_space:id()) ->
    ok | error_reply().
chown(#sfm_handle{
    storage = Storage,
    file = FileId,
    session_id = ?ROOT_SESS_ID,
    space_id = SpaceId
}, UserId, GroupId, SpaceId) ->
    {ok, HelperHandle} = session:get_helper(?ROOT_SESS_ID, SpaceId, Storage),
    {Uid, Gid} = luma:get_posix_user_ctx(?ROOT_SESS_ID, UserId, GroupId, SpaceId),
    helpers:chown(HelperHandle, FileId, Uid, Gid);
chown(_, _, _, _) ->
    throw(?EPERM).

%%--------------------------------------------------------------------
%% @doc
%% Creates a link on storage.
%% @end
%%--------------------------------------------------------------------
-spec link(FileHandleFrom :: handle(), FileTo :: helpers:file_id()) ->
    ok | error_reply().
link(#sfm_handle{
    storage = Storage,
    file = FileFrom,
    space_id = SpaceId,
    session_id = SessionId
}, FileTo) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceId, Storage),
    helpers:link(HelperHandle, FileFrom, FileTo).

%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes, reading them from storage.
%% @end
%%--------------------------------------------------------------------
-spec stat(FileHandle :: handle()) -> {ok, #statbuf{}} | error_reply().
stat(#sfm_handle{
    storage = Storage,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceId, Storage),
    helpers:getattr(HelperHandle, FileId).


%%--------------------------------------------------------------------
%% @doc
%% Returns list of file's children ids.
%% @end
%%--------------------------------------------------------------------
-spec readdir(FileHandle :: handle(), Offset :: non_neg_integer(),
    Count :: non_neg_integer()) ->
    {ok, [helpers:file_id()]} | error_reply().
readdir(#sfm_handle{
    storage = Storage,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}, Offset, Count) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceId, Storage),
    helpers:readdir(HelperHandle, FileId, Offset, Count).


%%--------------------------------------------------------------------
%% @doc
%% Returns file handle for child of given file.
%% @end
%%--------------------------------------------------------------------
-spec get_child_handle(handle(), helpers:file_id()) -> handle().
get_child_handle(ParentSFMHandle = #sfm_handle{file = ParentFileId}, ChildName) ->
    ParentSFMHandle#sfm_handle{
        file = filename:join([ParentFileId, ChildName])
    }.


%%--------------------------------------------------------------------
%% @doc
%% Writes data to a file on storage. Returns number of written bytes.
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: handle(), Offset :: non_neg_integer(),
    Buffer :: binary()) ->
    {ok, non_neg_integer()} | error_reply().
write(#sfm_handle{open_flag = undefined}, _, _) ->
    throw(?EPERM);
write(#sfm_handle{open_flag = read}, _, _) ->
    throw(?EPERM);
write(#sfm_handle{
    space_id = SpaceId,
    file_handle = FileHandle,
    file_size = CSize
}, Offset, Buffer) ->
    %% @todo: VFS-2086 handle sparse files
    space_quota:soft_assert_write(SpaceId, max(0, Offset + size(Buffer) - CSize)),
    helpers:write(FileHandle, Offset, Buffer).

%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file from storage.
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: handle(), Offset :: non_neg_integer(),
    MaxSize :: non_neg_integer()) ->
    {ok, binary()} | error_reply().
read(#sfm_handle{open_flag = undefined}, _, _) ->
    throw(?EPERM);
read(#sfm_handle{open_flag = write}, _, _) ->
    throw(?EPERM);
read(#sfm_handle{file_handle = FileHandle}, Offset, MaxSize) ->
    helpers:read(FileHandle, Offset, MaxSize).

%%--------------------------------------------------------------------
%% @doc
%% Creates a new file on storage.
%% @end
%%--------------------------------------------------------------------
-spec create(handle(), Mode :: non_neg_integer()) -> ok | error_reply().
create(Handle, Mode) ->
    create(Handle, Mode, false).
-spec create(handle(), Mode :: non_neg_integer(), Recursive :: boolean()) ->
    ok | error_reply().
create(#sfm_handle{
    storage = Storage,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
} = SFMHandle, Mode, Recursive) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceId, Storage),
    case helpers:mknod(HelperHandle, FileId, Mode, reg) of
        ok ->
            ok;
        {error, enoent} when Recursive ->
            Tokens = fslogic_path:split(FileId),
            LeafLess = fslogic_path:join(lists:sublist(Tokens, 1, length(Tokens) - 1)),
            ok =
                case mkdir(SFMHandle#sfm_handle{file = LeafLess},
                    ?AUTO_CREATED_PARENT_DIR_MODE, true)
                of
                    ok -> ok;
                    {error, eexist} -> ok;
                    E0 -> E0
                end,
            create(SFMHandle, Mode, false);
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Truncates a file on storage.
%% @end
%%--------------------------------------------------------------------
-spec truncate(handle(), Size :: integer()) -> ok | error_reply().
truncate(#sfm_handle{open_flag = undefined}, _) ->
    throw(?EPERM);
truncate(#sfm_handle{open_flag = read}, _) -> throw(?EPERM);
truncate(#sfm_handle{
    storage = Storage,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}, Size) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceId, Storage),
    helpers:truncate(HelperHandle, FileId, Size).

%%--------------------------------------------------------------------
%% @doc
%% Sets an extended attribute on a file.
%% @end
%%--------------------------------------------------------------------
-spec setxattr(handle(), Name :: binary(), Value :: binary(),
    Create :: boolean(), Replace :: boolean()) -> ok | error_reply().
setxattr(#sfm_handle{
    storage = Storage,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}, Name, Value, Create, Replace) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceId, Storage),
    helpers:setxattr(HelperHandle, FileId, Name, Value, Create, Replace).

%%--------------------------------------------------------------------
%% @doc
%% Returns a value of an extended attribute for a specific file.
%% @end
%%--------------------------------------------------------------------
-spec getxattr(handle(), Name :: binary()) -> {ok, binary()} | error_reply().
getxattr(#sfm_handle{
    storage = Storage,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}, Name) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceId, Storage),
    helpers:getxattr(HelperHandle, FileId, Name).

%%--------------------------------------------------------------------
%% @doc
%% Removes an extended attribute from a file.
%% @end
%%--------------------------------------------------------------------
-spec removexattr(handle(), Name :: binary()) -> ok | error_reply().
removexattr(#sfm_handle{
    storage = Storage,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}, Name) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceId, Storage),
    helpers:removexattr(HelperHandle, FileId, Name).

%%--------------------------------------------------------------------
%% @doc
%% List extended attribute names for a specific file.
%% @end
%%--------------------------------------------------------------------
-spec listxattr(handle()) -> {ok, [binary()]} | error_reply().
listxattr(#sfm_handle{
    storage = Storage,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceId, Storage),
    helpers:listxattr(HelperHandle, FileId).

%%--------------------------------------------------------------------
%% @doc
%% Removes a file.
%% @end
%%--------------------------------------------------------------------
-spec unlink(handle()) -> ok | error_reply().
unlink(#sfm_handle{
    storage = Storage,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceId, Storage),
    helpers:unlink(HelperHandle, FileId).

%%--------------------------------------------------------------------
%% @doc
%% Removes an empty directory.
%% @end
%%--------------------------------------------------------------------
-spec rmdir(handle()) -> ok | error_reply().
rmdir(#sfm_handle{
    storage = Storage,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceId, Storage),
    helpers:rmdir(HelperHandle, FileId).

%%--------------------------------------------------------------------
%% @doc
%% Assures that changes made on file are persistent.
%% @end
%%--------------------------------------------------------------------
-spec fsync(handle(), boolean()) -> ok | error_reply().
fsync(#sfm_handle{file_handle = FileHandle}, DataOnly) ->
    helpers:fsync(FileHandle, DataOnly).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Opens file in read mode and checks necessary permissions.
%% @end
%%--------------------------------------------------------------------
-spec open_for_read(handle()) -> {ok, handle()} | error_reply().
open_for_read(SFMHandle) ->
    check_permissions:execute(
        [?read_object],
        [SFMHandle#sfm_handle{session_id = ?ROOT_SESS_ID}, read],
        fun open_insecure/2
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Opens file in write mode and checks necessary permissions.
%% @end
%%--------------------------------------------------------------------
-spec open_for_write(handle()) -> {ok, handle()} | error_reply().
open_for_write(SFMHandle) ->
    check_permissions:execute(
        [?write_object],
        [SFMHandle#sfm_handle{session_id = ?ROOT_SESS_ID}, write],
        fun open_insecure/2
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Opens file in rdwr mode and checks necessary permissions.
%% @end
%%--------------------------------------------------------------------
-spec open_for_rdwr(handle()) -> {ok, handle()} | error_reply().
open_for_rdwr(SFMHandle) ->
    check_permissions:execute(
        [?read_object, ?write_object],
        [SFMHandle#sfm_handle{session_id = ?ROOT_SESS_ID}, rdwr],
        fun open_insecure/2
    ).

%%--------------------------------------------------------------------
%% @private
%% @equiv open/2, but without permission control
%% @end
%%--------------------------------------------------------------------
-spec open_insecure(handle(), OpenFlag :: helpers:open_flag()) ->
    {ok, handle()} | error_reply().
open_insecure(#sfm_handle{
    storage = Storage,
    file = FileId,
    session_id = SessionId,
    space_id = SpaceId
} = SFMHandle, OpenFlag
) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceId, Storage),
    case helpers:open(HelperHandle, FileId, OpenFlag) of
        {ok, FileHandle} ->
            {ok, SFMHandle#sfm_handle{
                file_handle = FileHandle,
                open_flag = OpenFlag
            }};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns size of file, or 0 in case of error.
%% @end
%%--------------------------------------------------------------------
-spec get_size(file_meta:uuid(), od_space:id()) -> non_neg_integer().
get_size(FileUuid, _SpaceId) ->
    fslogic_blocks:get_size(file_location:local_id(FileUuid), FileUuid).