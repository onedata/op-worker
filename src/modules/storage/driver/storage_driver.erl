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
-module(storage_driver).

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").

-export([new_handle/2, new_handle/3, new_handle/6, set_size/1, increase_size/2, get_storage_file_id/1]).
-export([mkdir/2, mkdir/3, mv/2, chmod/2, chown/3, link/2, readdir/3,
    get_child_handle/2, listobjects/4]).
-export([stat/1, read/3, write/3, create/2, create/3, open/2, release/1,
    truncate/3, unlink/2, fsync/2, rmdir/1, exists/1]).
-export([setxattr/5, getxattr/2, removexattr/2, listxattr/1]).
-export([open_at_creation/1]).

% Export for tests
-export([open_insecure/2]).

-type handle() :: #sd_handle{}.
-type handle_id() :: binary().
-type error_reply() :: {error, term()}.

-export_type([handle/0, handle_id/0, error_reply/0]).

-define(RUN(SDHandle, Fun, HelperOrFileHandle),
    helpers_fallback:apply_and_maybe_handle_ekeyexpired(SDHandle, Fun, HelperOrFileHandle)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv new_handle(SessionId, FileCtx, true).
%% @end
%%--------------------------------------------------------------------
-spec new_handle(session:id(), file_ctx:ctx()) -> {handle(), file_ctx:ctx()}.
new_handle(SessionId, FileCtx) ->
    new_handle(SessionId, FileCtx, true).

%%--------------------------------------------------------------------
%% @equiv new_handle(SessionId, SpaceUuid, FileUuid, Storage, FileId, undefined).
%% @end
%%--------------------------------------------------------------------
-spec new_handle(session:id(), file_ctx:ctx(), boolean()) ->
    {handle() | undefined, file_ctx:ctx()}.
new_handle(SessionId, FileCtx, Generate) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {StorageId, FileCtx2} = file_ctx:get_storage_id(FileCtx),
    case file_ctx:get_storage_file_id(FileCtx2, Generate) of
        {undefined, FileCtx3} ->
            {undefined, FileCtx3};
        {FileId, FileCtx3} ->
            ShareId = file_ctx:get_share_id_const(FileCtx3),
            {new_handle(SessionId, SpaceId, FileUuid, StorageId, FileId, ShareId), FileCtx3}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Opens the file. To use opened descriptor, pass returned handle to other functions.
%% File may and should be closed with release/1, but file will be closed automatically
%% when handle goes out of scope (term will be released by Erlang's GC).
%% Handle created by this function may not be used for remote files.
%% @end
%%--------------------------------------------------------------------
-spec new_handle(session:id(), od_space:id(), file_meta:uuid() | undefined, storage:id(),
    helpers:file_id(), od_share:id() | undefined) -> handle().
new_handle(SessionId, SpaceId, FileUuid, StorageId, StorageFileId, ShareId) ->
    #sd_handle{
        file = StorageFileId,
        file_uuid = FileUuid,
        session_id = SessionId,
        space_id = SpaceId,
        storage_id = StorageId,
        share_id = ShareId
    }.

%%--------------------------------------------------------------------
%% @doc
%% Sets size in handle.
%% @end
%%--------------------------------------------------------------------
-spec set_size(handle()) -> handle().
set_size(#sd_handle{
    space_id = SpaceId,
    file_uuid = FileUuid
} = Handle) ->
    FSize = get_size(FileUuid, SpaceId),
    Handle#sd_handle{
        file_size = FSize
    }.

%%--------------------------------------------------------------------
%% @doc
%% Sets size in handle.
%% @end
%%--------------------------------------------------------------------
-spec increase_size(handle(), non_neg_integer()) -> handle().
increase_size(SDHandle = #sd_handle{file_size = CurrentSize}, Increase) ->
    SDHandle#sd_handle{
        file_size = CurrentSize + Increase
    }.

-spec get_storage_file_id(handle()) -> helpers:file_id().
get_storage_file_id(#sd_handle{file = StorageFileId}) ->
    StorageFileId.

%%--------------------------------------------------------------------
%% @doc
%% Opens the file. To used opened descriptor, pass returned handle to other functions.
%% File may and should be closed with release/1, but file will be closed automatically
%% when handle goes out of scope (term will be released by Erlang's GC).
%% @end
%%--------------------------------------------------------------------
-spec open(handle(), OpenFlag :: helpers:open_flag()) ->
    {ok, handle()} | error_reply().
open(SDHandle, read) ->
    open_for_read(SDHandle);
open(SDHandle, write) ->
    open_for_write(SDHandle);
open(SDHandle, rdwr) ->
    open_for_rdwr(SDHandle).

%%--------------------------------------------------------------------
%% @doc
%% Opens the file. To used opened descriptor, pass returned handle to other functions.
%% File may and should be closed with release/1, but file will be closed automatically
%% when handle goes out of scope (term will be released by Erlang's GC).
%% Bypasses permissions check to allow to open file at creation.
%% @end
%%--------------------------------------------------------------------
-spec open_at_creation(handle()) -> {ok, handle()} | error_reply().
open_at_creation(SDHandle) ->
    open_insecure(SDHandle#sd_handle{session_id = ?ROOT_SESS_ID}, rdwr).

%%--------------------------------------------------------------------
%% @doc
%% Closes the file.
%% @end
%%--------------------------------------------------------------------
-spec release(handle()) -> ok | error_reply().
release(SDHandle = #sd_handle{file_handle = FileHandle}) ->
    ?RUN(SDHandle, fun() ->
        helpers:release(FileHandle)
    end, FileHandle).

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
mkdir(#sd_handle{
    storage_id = StorageId,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
} = SDHandle, Mode, Recursive) ->
    {ok, HelperHandle} = session_helpers:get_helper(SessionId, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        Noop = fun(_) -> ok end,
        case helpers:mkdir(HelperHandle, FileId, Mode) of
            ok ->
                ok;
            {error, ?ENOENT} when Recursive ->
                Tokens = fslogic_path:split(FileId),
                case Tokens of
                    [_] -> ok;
                    [_ | _] ->
                        LeafLess = filename:dirname(FileId),
                        case mkdir(SDHandle#sd_handle{file = LeafLess},
                            ?AUTO_CREATED_PARENT_DIR_MODE, true)
                        of
                            ok -> ok;
                            {error, ?EEXIST} -> ok;
                            ParentError ->
                                ?error("Cannot create parent for file ~p, error ~p",
                                    [FileId, ParentError]),
                                throw(ParentError)
                        end
                end,
                R = case mkdir(SDHandle, Mode, false) of
                    ok ->
                        chmod(SDHandle, Mode); %% @todo: find out why umask(0) in helpers_nif.cc doesn't work
                    E -> E
                end,
                Noop(HelperHandle), %% @todo: check why NIF crashes when this term is destroyed before recursive call
                R;
            {error, Reason} ->
                {error, Reason}
        end
    end, HelperHandle).


%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location on storage.
%% @end
%%--------------------------------------------------------------------
-spec mv(FileHandleFrom :: handle(), FileTo :: helpers:file_id()) ->
    ok | error_reply().
mv(SDHandle = #sd_handle{
    storage_id = StorageId,
    file = FileFrom,
    space_id = SpaceId,
    session_id = SessionId
}, FileTo) ->
    {ok, HelperHandle} = session_helpers:get_helper(SessionId, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        helpers:rename(HelperHandle, FileFrom, FileTo)
    end, HelperHandle).


%%--------------------------------------------------------------------
%% @doc
%% Changes the permissions of a file on storage.
%% @end
%%--------------------------------------------------------------------
-spec chmod(handle(), NewMode :: file_meta:posix_permissions()) ->
    ok | error_reply().
chmod(SDHandle = #sd_handle{
    storage_id = StorageId,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}, Mode) ->
    {ok, HelperHandle} = session_helpers:get_helper(SessionId, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        helpers:chmod(HelperHandle, FileId, Mode)
    end, HelperHandle).


%%--------------------------------------------------------------------
%% @doc
%% Changes owner of a file on storage.
%% @end
%%--------------------------------------------------------------------
-spec chown(FileHandle :: handle(), non_neg_integer(), non_neg_integer()) -> ok | error_reply().
chown(SDHandle = #sd_handle{
    storage_id = StorageId,
    file = FileId,
    session_id = ?ROOT_SESS_ID,
    space_id = SpaceId
}, Uid, Gid) ->
    {ok, HelperHandle} = session_helpers:get_helper(?ROOT_SESS_ID, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        helpers:chown(HelperHandle, FileId, Uid, Gid)
    end, HelperHandle);
chown(_, _, _) ->
    throw(?EPERM).

%%--------------------------------------------------------------------
%% @doc
%% Creates a link on storage.
%% @end
%%--------------------------------------------------------------------
-spec link(FileHandleFrom :: handle(), FileTo :: helpers:file_id()) ->
    ok | error_reply().
link(SDHandle = #sd_handle{
    storage_id = StorageId,
    file = FileFrom,
    space_id = SpaceId,
    session_id = SessionId
}, FileTo) ->
    {ok, HelperHandle} = session_helpers:get_helper(SessionId, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        helpers:link(HelperHandle, FileFrom, FileTo)
    end, HelperHandle).

%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes, reading them from storage.
%% @end
%%--------------------------------------------------------------------
-spec stat(FileHandle :: handle()) -> {ok, helpers:stat()} | error_reply().
stat(SDHandle = #sd_handle{
    storage_id = StorageId,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}) ->
    {ok, HelperHandle} = session_helpers:get_helper(SessionId, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        helpers:getattr(HelperHandle, FileId)
    end, HelperHandle).

-spec exists(handle()) -> boolean().
exists(SDHandle) ->
    case stat(SDHandle) of
        {ok, _} -> true;
        {error, ?ENOENT} -> false
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of file's children ids.
%% @end
%%--------------------------------------------------------------------
-spec readdir(FileHandle :: handle(), Offset :: non_neg_integer(),
    Count :: non_neg_integer()) ->
    {ok, [helpers:file_id()]} | error_reply().
readdir(SDHandle = #sd_handle{
    storage_id = StorageId,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}, Offset, Count) ->
    {ok, HelperHandle} = session_helpers:get_helper(SessionId, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        helpers:readdir(HelperHandle, FileId, Offset, Count)
    end, HelperHandle).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of file's children ids.
%% @end
%%--------------------------------------------------------------------
-spec listobjects(FileHandle :: handle(), Marker :: binary(), Offset :: non_neg_integer(),
    Count :: non_neg_integer()) -> {ok, [helpers:file_id()]} | error_reply().
listobjects(SDHandle = #sd_handle{
    storage_id = StorageId,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}, Marker, Offset, Count) ->
    {ok, HelperHandle} = session_helpers:get_helper(SessionId, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        helpers:listobjects(HelperHandle, FileId, Marker, Offset, Count)
    end, HelperHandle).

%%--------------------------------------------------------------------
%% @doc
%% Returns file handle for child of given file.
%% @end
%%--------------------------------------------------------------------
-spec get_child_handle(handle(), helpers:file_id()) -> handle().
get_child_handle(#sd_handle{
    session_id = SessionId,
    space_id = SpaceId,
    file = StorageFileId,
    storage_id = StorageId,
    share_id = ShareId
}, ChildName) ->
    #sd_handle{
        session_id = SessionId,
        space_id = SpaceId,
        file = filename:join(StorageFileId, ChildName),
        storage_id = StorageId,
        share_id = ShareId
    }.


%%--------------------------------------------------------------------
%% @doc
%% Writes data to a file on storage. Returns number of written bytes.
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: handle(), Offset :: non_neg_integer(),
    Buffer :: binary()) ->
    {ok, non_neg_integer()} | error_reply().
write(#sd_handle{open_flag = undefined}, _, _) ->
    throw(?EPERM);
write(#sd_handle{open_flag = read}, _, _) ->
    throw(?EPERM);
write(SDHandle = #sd_handle{
    space_id = SpaceId,
    file_handle = FileHandle,
    file_size = CSize
}, Offset, Buffer) ->
    %% @todo: VFS-2086 handle sparse files
    ?RUN(SDHandle, fun() ->
        space_quota:assert_write(SpaceId, max(0, Offset + size(Buffer) - CSize)),
        helpers:write(FileHandle, Offset, Buffer)
    end, FileHandle).

%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file from storage.
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: handle(), Offset :: non_neg_integer(),
    MaxSize :: non_neg_integer()) ->
    {ok, binary()} | error_reply().
read(#sd_handle{open_flag = undefined}, _, _) ->
    throw(?EPERM);
read(#sd_handle{open_flag = write}, _, _) ->
    throw(?EPERM);
read(SDHandle = #sd_handle{file_handle = FileHandle}, Offset, MaxSize) ->
    ?RUN(SDHandle, fun() ->
        case read_internal(SDHandle, Offset, MaxSize) of
            {ok, Bytes} ->
                case byte_size(Bytes) of
                    0 ->
                        {ok, Bytes};
                    MaxSize ->
                        {ok, Bytes};
                    Size ->
                        case read(SDHandle, Offset + Size, MaxSize - Size) of
                            {ok, Bytes2} ->
                                {ok, <<Bytes/binary, Bytes2/binary>>};
                            Error = {error, _} ->
                                Error
                        end
                end;
            {error, ?ENOENT} when Offset > 0 ->
                % some object storages return enoent when trying to read bytes out of file's range
                % we must ensure that file exists
                case stat(SDHandle) of
                    {ok, _} ->
                        {ok, <<"">>};
                    {error, ?ENOENT} ->
                        {error, ?ENOENT}
                end;
            Error = {error, _} ->
                Error
        end
    end, FileHandle).

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
create(#sd_handle{
    storage_id = StorageId,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
} = SDHandle, Mode, Recursive) ->
    {ok, HelperHandle} = session_helpers:get_helper(SessionId, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        case helpers:mknod(HelperHandle, FileId, Mode, reg) of
            ok ->
                ok;
            {error, ?ENOENT} when Recursive ->
                Tokens = fslogic_path:split(FileId),
                LeafLess = fslogic_path:join(lists:sublist(Tokens, 1, length(Tokens) - 1)),
                ok =
                    case mkdir(SDHandle#sd_handle{file = LeafLess},
                        ?AUTO_CREATED_PARENT_DIR_MODE, true)
                    of
                        ok -> ok;
                        {error, ?EEXIST} -> ok;
                        E0 -> E0
                    end,
                create(SDHandle, Mode, false);
            {error, Reason} ->
                {error, Reason}
        end
    end, HelperHandle).


%%--------------------------------------------------------------------
%% @doc
%% Truncates a file on storage to Size. CurrentSize specifies the
%% current size of the file known by the op-worker.
%% @end
%%--------------------------------------------------------------------
-spec truncate(handle(), Size :: integer(), CurrentSize :: non_neg_integer())
    -> ok | error_reply().
truncate(#sd_handle{open_flag = undefined}, _, _) ->
    throw(?EPERM);
truncate(#sd_handle{open_flag = read}, _, _) -> throw(?EPERM);
truncate(SDHandle = #sd_handle{
    storage_id = StorageId,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}, Size, CurrentSize) ->
    {ok, HelperHandle} = session_helpers:get_helper(SessionId, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        helpers:truncate(HelperHandle, FileId, Size, CurrentSize)
    end, HelperHandle).

%%--------------------------------------------------------------------
%% @doc
%% Sets an extended attribute on a file.
%% @end
%%--------------------------------------------------------------------
-spec setxattr(handle(), Name :: binary(), Value :: binary(),
    Create :: boolean(), Replace :: boolean()) -> ok | error_reply().
setxattr(SDHandle = #sd_handle{
    storage_id = StorageId,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}, Name, Value, Create, Replace) ->
    {ok, HelperHandle} = session_helpers:get_helper(SessionId, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        helpers:setxattr(HelperHandle, FileId, Name, Value, Create, Replace)
    end, HelperHandle).

%%--------------------------------------------------------------------
%% @doc
%% Returns a value of an extended attribute for a specific file.
%% @end
%%--------------------------------------------------------------------
-spec getxattr(handle(), Name :: binary()) -> {ok, binary()} | error_reply().
getxattr(SDHandle = #sd_handle{
    storage_id = StorageId,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}, Name) ->
    {ok, HelperHandle} = session_helpers:get_helper(SessionId, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        helpers:getxattr(HelperHandle, FileId, Name)
    end, HelperHandle).

%%--------------------------------------------------------------------
%% @doc
%% Removes an extended attribute from a file.
%% @end
%%--------------------------------------------------------------------
-spec removexattr(handle(), Name :: binary()) -> ok | error_reply().
removexattr(SDHandle = #sd_handle{
    storage_id = StorageId,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}, Name) ->
    {ok, HelperHandle} = session_helpers:get_helper(SessionId, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        helpers:removexattr(HelperHandle, FileId, Name)
    end, HelperHandle).

%%--------------------------------------------------------------------
%% @doc
%% List extended attribute names for a specific file.
%% @end
%%--------------------------------------------------------------------
-spec listxattr(handle()) -> {ok, [binary()]} | error_reply().
listxattr(SDHandle = #sd_handle{
    storage_id = StorageId,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}) ->
    {ok, HelperHandle} = session_helpers:get_helper(SessionId, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        helpers:listxattr(HelperHandle, FileId)
    end, HelperHandle).

%%--------------------------------------------------------------------
%% @doc
%% Removes a file. CurrentSize specifies the current size of the file
%% known by the op-worker.
%% @end
%%--------------------------------------------------------------------
-spec unlink(handle(), CurrentSize :: integer()) -> ok | error_reply().
unlink(SDHandle = #sd_handle{
    storage_id = StorageId,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}, CurrentSize) ->
    {ok, HelperHandle} = session_helpers:get_helper(SessionId, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        case helpers:unlink(HelperHandle, FileId, CurrentSize) of
            ok -> ok;
            {error, ?ENOENT} -> ok
        end
    end, HelperHandle).

%%--------------------------------------------------------------------
%% @doc
%% Removes an empty directory.
%% @end
%%--------------------------------------------------------------------
-spec rmdir(handle()) -> ok | error_reply().
rmdir(SDHandle = #sd_handle{
    storage_id = StorageId,
    file = FileId,
    space_id = SpaceId,
    session_id = SessionId
}) ->
    {ok, HelperHandle} = session_helpers:get_helper(SessionId, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        helpers:rmdir(HelperHandle, FileId)
    end, HelperHandle).

%%--------------------------------------------------------------------
%% @doc
%% Assures that changes made on file are persistent.
%% @end
%%--------------------------------------------------------------------
-spec fsync(handle(), boolean()) -> ok | error_reply().
fsync(SDHandle = #sd_handle{file_handle = FileHandle}, DataOnly) ->
    ?RUN(SDHandle, fun() ->
        helpers:fsync(FileHandle, DataOnly)
    end, FileHandle).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec read_internal(handle(), non_neg_integer(), non_neg_integer()) -> {ok, binary()} | {error, term()}.
read_internal(SDHandle = #sd_handle{file_handle = FileHandle}, Offset, MaxSize) ->
    ?RUN(SDHandle, fun() ->
        helpers:read(FileHandle, Offset, MaxSize)
    end, FileHandle).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Opens file in read mode and checks necessary permissions.
%% @end
%%--------------------------------------------------------------------
-spec open_for_read(handle()) -> {ok, handle()} | error_reply().
open_for_read(SFMHandle) ->
    open_with_permissions_check(
        SFMHandle#sd_handle{session_id = ?ROOT_SESS_ID},
        [?read_object], read
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Opens file in write mode and checks necessary permissions.
%% @end
%%--------------------------------------------------------------------
-spec open_for_write(handle()) -> {ok, handle()} | error_reply().
open_for_write(SFMHandle) ->
    open_with_permissions_check(
        SFMHandle#sd_handle{session_id = ?ROOT_SESS_ID},
        [?write_object], write
    ).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Opens file in rdwr mode and checks necessary permissions.
%% @end
%%--------------------------------------------------------------------
-spec open_for_rdwr(handle()) -> {ok, handle()} | error_reply().
open_for_rdwr(SFMHandle) ->
    open_with_permissions_check(
        SFMHandle#sd_handle{session_id = ?ROOT_SESS_ID},
        [?read_object, ?write_object], rdwr
    ).

%%--------------------------------------------------------------------
%% @private
%% @equiv open/2, but with permission control
%% @end
%%--------------------------------------------------------------------
-spec open_with_permissions_check(handle(), [data_access_rights:requirement()],
    helpers:open_flag()) -> {ok, handle()} | error_reply().
open_with_permissions_check(#sd_handle{
    session_id = SessionId,
    space_id = SpaceId,
    file_uuid = FileUuid,
    share_id = ShareId
} = SFMHandle, AccessRequirements, OpenFlag) ->
    FileGuid = file_id:pack_share_guid(FileUuid, SpaceId, ShareId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    UserCtx = user_ctx:new(SessionId),

    % TODO VFS-5917
    fslogic_authz:ensure_authorized(UserCtx, FileCtx, AccessRequirements),
    open_insecure(SFMHandle, OpenFlag).

%%--------------------------------------------------------------------
%% @private
%% @equiv open/2, but without permission control
%% @end
%%--------------------------------------------------------------------
-spec open_insecure(handle(), OpenFlag :: helpers:open_flag()) ->
    {ok, handle()} | error_reply().
open_insecure(#sd_handle{
    storage_id = StorageId,
    file = FileId,
    session_id = SessionId,
    space_id = SpaceId
} = SDHandle, OpenFlag
) ->
    {ok, HelperHandle} = session_helpers:get_helper(SessionId, SpaceId, StorageId),
    ?RUN(SDHandle, fun() ->
        case helpers:open(HelperHandle, FileId, OpenFlag) of
            {ok, FileHandle} ->
                {ok, SDHandle#sd_handle{
                    file_handle = FileHandle,
                    open_flag = OpenFlag
                }};
            {error, Reason} ->
                {error, Reason}
        end
    end, HelperHandle).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns size of file, or 0 in case of error.
%% @end
%%--------------------------------------------------------------------
-spec get_size(file_meta:uuid(), od_space:id()) -> non_neg_integer().
get_size(FileUuid, _SpaceId) ->
    fslogic_location_cache:get_location_size(file_location:local_id(FileUuid), FileUuid).