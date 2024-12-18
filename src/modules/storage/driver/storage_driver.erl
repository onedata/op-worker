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
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

-export([new_handle/2, new_handle/3, new_handle/5, new_handle/6,
    set_size/1, calculate_size/1, increase_size/2,
    get_storage_file_id/1, get_storage_id/1]).
-export([mkdir/2, mkdir/3, mv/2, chmod/2, chown/3, link/2, readdir/3,
    get_child_handle/2, listobjects/4, flushbuffer/2,
    blocksize_for_path/1]).
-export([stat/1, read/3, write/3, create/2, create/3, open/2, open_for_write_insecure/1, release/1,
    truncate/3, unlink/2, fsync/2, rmdir/1, exists/1]).
-export([setxattr/5, getxattr/2, removexattr/2, listxattr/1]).
-export([open_at_creation/1]).
-export([infer_type/1]).

% Export for tests
-export([open_insecure/2]).

-type handle() :: #sd_handle{}.
-type handle_id() :: binary().
-type error_reply() :: {error, term()}.

-export_type([handle/0, handle_id/0, error_reply/0]).

-type fallback_strategy() :: no_fallback | retry_as_root | retry_as_root_and_chown.


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
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx), % TODO VFS-7447 - should we use referenced uuid?
    {StorageId, FileCtx2} = file_ctx:get_storage_id(FileCtx),
    case file_ctx:get_storage_file_id(FileCtx2, Generate) of
        {undefined, FileCtx3} ->
            {undefined, FileCtx3};
        {FileId, FileCtx3} ->
            ShareId = file_ctx:get_share_id_const(FileCtx3),
            {new_handle(SessionId, SpaceId, FileUuid, StorageId, FileId, ShareId), FileCtx3}
    end.


-spec new_handle(session:id(), od_space:id(), file_meta:uuid() | undefined, storage:id(), helpers:file_id()) ->
    handle().
new_handle(SessionId, SpaceId, FileUuid, StorageId, StorageFileId) ->
    new_handle(SessionId, SpaceId, FileUuid, StorageId, StorageFileId, undefined).


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
        file_uuid = FileUuid, % TODO VFS-7447 - should we use referenced uuid?
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
%% Calculates size of file represented by handle but does not cache result in handle.
%% @end
%%--------------------------------------------------------------------
-spec calculate_size(handle()) -> non_neg_integer().
calculate_size(#sd_handle{
    space_id = SpaceId,
    file_uuid = FileUuid
}) ->
    get_size(FileUuid, SpaceId).


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


-spec get_storage_id(handle()) -> storage:id().
get_storage_id(#sd_handle{storage_id = StorageId}) ->
    StorageId.


-spec get_storage_file_id(handle()) -> helpers:file_id().
get_storage_file_id(#sd_handle{file = StorageFileId}) ->
    StorageFileId.


%%--------------------------------------------------------------------
%% @doc
%% Opens the file. To use opened descriptor, pass returned handle to other functions.
%% File should be closed with release/1, but file will be closed automatically
%% when handle goes out of scope (term will be released by Erlang's GC).
%% NOTE: we are checking file access here to ensure the same handling between different
%% storages types (flat storage helpers do not check anything, even file existence, on the
%% open operation).
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


-spec open_for_write_insecure(handle()) -> {ok, handle()} | error_reply().
open_for_write_insecure(SDHandle) ->
    open_insecure(SDHandle#sd_handle{session_id = ?ROOT_SESS_ID}, write).


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
release(SDHandle) ->
    run_with_file_handle(SDHandle, fun(FileHandle) ->
        helpers:release(FileHandle)
    end).


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
mkdir(#sd_handle{file = FileId} = SDHandle, Mode, Recursive) ->
    run_with_helper_handle(retry_as_root_and_chown, SDHandle, fun(HelperHandle) ->
        case helpers:mkdir(HelperHandle, FileId, Mode) of
            ok ->
                ok;
            {error, ?ENOENT} when Recursive ->
                Tokens = filepath_utils:split(FileId),
                case Tokens of
                    [_] -> ok;
                    [_ | _] ->
                        LeafLess = filename:dirname(FileId),
                        case mkdir(SDHandle#sd_handle{file = LeafLess}, ?DEFAULT_DIR_PERMS, true) of
                            ok -> ok;
                            {error, ?EEXIST} -> ok;
                            ParentError ->
                                ?error("Cannot create parent for file ~tp, error ~tp",
                                    [FileId, ParentError]),
                                throw(ParentError)
                        end
                end,
                mkdir(SDHandle, Mode, false);
            {error, Reason} ->
                {error, Reason}
        end
    end, ?READWRITE).


%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location on storage.
%% @end
%%--------------------------------------------------------------------
-spec mv(FileHandleFrom :: handle(), FileTo :: helpers:file_id()) ->
    ok | error_reply().
mv(SDHandle = #sd_handle{file = FileFrom}, FileTo) ->
    run_with_helper_handle(retry_as_root_and_chown, SDHandle, fun(HelperHandle) ->
        helpers:rename(HelperHandle, FileFrom, FileTo)
    end, ?READWRITE).


%%--------------------------------------------------------------------
%% @doc
%% Changes the permissions of a file on storage.
%% @end
%%--------------------------------------------------------------------
-spec chmod(handle(), NewMode :: file_meta:posix_permissions()) ->
    ok | error_reply().
chmod(SDHandle = #sd_handle{file = FileId}, Mode) ->
    run_with_helper_handle(retry_as_root, SDHandle, fun(HelperHandle) ->
        helpers:chmod(HelperHandle, FileId, Mode)
    end, ?READWRITE).


%%--------------------------------------------------------------------
%% @doc
%% Changes owner of a file on storage.
%% NOTE!!! Currently supported only on POSIX and GLUSTERS helpers.
%% @end
%%--------------------------------------------------------------------
-spec chown(FileHandle :: handle(), non_neg_integer(), non_neg_integer()) -> ok | error_reply().
chown(SDHandle = #sd_handle{file = FileId, session_id = ?ROOT_SESS_ID}, Uid, Gid) ->
    run_with_helper_handle(no_fallback, SDHandle, fun(HelperHandle) ->
        helpers:chown(HelperHandle, FileId, Uid, Gid)
    end, ?READWRITE);
chown(_, _, _) ->
    throw(?EPERM).


%%--------------------------------------------------------------------
%% @doc
%% Creates a link on storage.
%% @end
%%--------------------------------------------------------------------
-spec link(FileHandleFrom :: handle(), FileTo :: helpers:file_id()) ->
    ok | error_reply().
link(SDHandle = #sd_handle{file = FileFrom}, FileTo) ->
    run_with_helper_handle(retry_as_root, SDHandle, fun(HelperHandle) ->
        helpers:link(HelperHandle, FileFrom, FileTo)
    end, ?READWRITE).


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes, reading them from storage.
%% @end
%%--------------------------------------------------------------------
-spec stat(FileHandle :: handle()) -> {ok, helpers:stat()} | error_reply().
stat(SDHandle = #sd_handle{file = FileId}) ->
    run_with_helper_handle(retry_as_root, SDHandle, fun(HelperHandle) ->
        helpers:getattr(HelperHandle, FileId)
    end).


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
readdir(SDHandle = #sd_handle{file = FileId}, Offset, Count) ->
    run_with_helper_handle(retry_as_root, SDHandle, fun(HelperHandle) ->
        helpers:readdir(HelperHandle, FileId, Offset, Count)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Returns list of file's children ids.
%% @end
%%--------------------------------------------------------------------
-spec listobjects(FileHandle :: handle(), Marker :: binary(), Offset :: non_neg_integer(),
    Count :: non_neg_integer()) ->
    {ok, {helpers:marker(), [{helpers:file_id(), helpers:stat()}]}} | error_reply().
listobjects(SDHandle = #sd_handle{file = FileId}, Marker, Offset, Count) ->
    run_with_helper_handle(retry_as_root, SDHandle, fun(HelperHandle) ->
        helpers:listobjects(HelperHandle, FileId, Marker, Offset, Count)
    end).


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
write(SDHandle, Offset, Buffer) ->
    run_with_file_handle(SDHandle, fun(FileHandle) ->
        helpers:write(FileHandle, Offset, Buffer)
    end, ?READWRITE).


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
read(SDHandle, Offset, MaxSize) ->
    run_with_file_handle(SDHandle, fun(_FileHandle) ->
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
            {error, Error} when Error == ?ENOENT orelse Error == ?EIO, Offset > 0 ->
                % some object storages return enoent or eio when trying to read bytes
                % out of file's range we must ensure that file exists
                case stat(SDHandle) of
                    {ok, _} ->
                        {ok, <<"">>};
                    {error, ?ENOENT} ->
                        {error, ?ENOENT}
                end;
            Error = {error, _} ->
                Error
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new regural file on storage.
%% @end
%%--------------------------------------------------------------------
-spec create(handle(), Mode :: non_neg_integer()) -> ok | error_reply().
create(SDHandle, Mode) ->
    create(SDHandle, Mode, reg).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file on storage.
%% @end
%%--------------------------------------------------------------------
-spec create(handle(), Mode :: non_neg_integer(), helpers:file_type_flag()) -> ok | error_reply().
create(#sd_handle{file = FileId} = SDHandle, Mode, FileTypeFlag) ->
    run_with_helper_handle(retry_as_root_and_chown, SDHandle, fun(HelperHandle) ->
        helpers:mknod(HelperHandle, FileId, Mode, FileTypeFlag)
    end, ?READWRITE).


%%--------------------------------------------------------------------
%% @doc
%% Truncates a file on storage to Size. CurrentSize specifies the
%% current size of the file known by the op-worker.
%% @end
%%--------------------------------------------------------------------
-spec truncate(handle(), Size :: integer(), CurrentSize :: non_neg_integer()) ->
    ok | error_reply().
truncate(#sd_handle{open_flag = undefined}, _, _) ->
    throw(?EPERM);
truncate(#sd_handle{open_flag = read}, _, _) -> throw(?EPERM);
truncate(SDHandle = #sd_handle{file = FileId}, Size, CurrentSize) ->
    run_with_helper_handle(retry_as_root, SDHandle, fun(HelperHandle) ->
        helpers:truncate(HelperHandle, FileId, Size, CurrentSize)
    end, ?READWRITE).


%%--------------------------------------------------------------------
%% @doc
%% Sets an extended attribute on a file.
%% @end
%%--------------------------------------------------------------------
-spec setxattr(handle(), Name :: binary(), Value :: binary(),
    Create :: boolean(), Replace :: boolean()) -> ok | error_reply().
setxattr(SDHandle = #sd_handle{file = FileId}, Name, Value, Create, Replace) ->
    run_with_helper_handle(retry_as_root, SDHandle, fun(HelperHandle) ->
        helpers:setxattr(HelperHandle, FileId, Name, Value, Create, Replace)
    end, ?READWRITE).


%%--------------------------------------------------------------------
%% @doc
%% Returns a value of an extended attribute for a specific file.
%% @end
%%--------------------------------------------------------------------
-spec getxattr(handle(), Name :: binary()) -> {ok, binary()} | error_reply().
getxattr(SDHandle = #sd_handle{file = FileId}, Name) ->
    run_with_helper_handle(retry_as_root, SDHandle, fun(HelperHandle) ->
        helpers:getxattr(HelperHandle, FileId, Name)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Removes an extended attribute from a file.
%% @end
%%--------------------------------------------------------------------
-spec removexattr(handle(), Name :: binary()) -> ok | error_reply().
removexattr(SDHandle = #sd_handle{file = FileId}, Name) ->
    run_with_helper_handle(retry_as_root, SDHandle, fun(HelperHandle) ->
        helpers:removexattr(HelperHandle, FileId, Name)
    end, ?READWRITE).


%%--------------------------------------------------------------------
%% @doc
%% List extended attribute names for a specific file.
%% @end
%%--------------------------------------------------------------------
-spec listxattr(handle()) -> {ok, [binary()]} | error_reply().
listxattr(SDHandle = #sd_handle{file = FileId}) ->
    run_with_helper_handle(retry_as_root, SDHandle, fun(HelperHandle) ->
        helpers:listxattr(HelperHandle, FileId)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Flush buffer of BufferedStorageHelper for specific file.
%% @end
%%--------------------------------------------------------------------
-spec flushbuffer(handle(), CurrentSize :: integer()) -> ok | error_reply().
flushbuffer(SDHandle = #sd_handle{file = FileId}, CurrentSize) ->
    run_with_helper_handle(retry_as_root, SDHandle, fun(HelperHandle) ->
        case helpers:flushbuffer(HelperHandle, FileId, CurrentSize) of
            ok -> ok;
            {error, ?ENOENT} -> ok;
            {error, ?EIO} -> ok;
            {error, __} = Error -> Error
        end
    end, ?READWRITE).


%%--------------------------------------------------------------------
%% @doc
%% Return block size optimal for writing to a specific file.
%% @end
%%--------------------------------------------------------------------
-spec blocksize_for_path(handle()) -> {ok, non_neg_integer()} | error_reply().
blocksize_for_path(SDHandle = #sd_handle{file = FileId}) ->
    run_with_helper_handle(retry_as_root, SDHandle, fun(HelperHandle) ->
        helpers:blocksize_for_path(HelperHandle, FileId)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Removes a file. CurrentSize specifies the current size of the file
%% known by the op-worker.
%% @end
%%--------------------------------------------------------------------
-spec unlink(handle(), CurrentSize :: integer()) -> ok | error_reply().
unlink(SDHandle = #sd_handle{file = FileId}, CurrentSize) ->
    run_with_helper_handle(retry_as_root, SDHandle, fun(HelperHandle) ->
        case helpers:unlink(HelperHandle, FileId, CurrentSize) of
            ok -> ok;
            {error, ?ENOENT} -> ok;
            {error, __} = Error -> Error
        end
    end, ?READWRITE).


%%--------------------------------------------------------------------
%% @doc
%% Removes an empty directory.
%% @end
%%--------------------------------------------------------------------
-spec rmdir(handle()) -> ok | error_reply().
rmdir(SDHandle = #sd_handle{file = FileId}) ->
    run_with_helper_handle(retry_as_root, SDHandle, fun(HelperHandle) ->
        helpers:rmdir(HelperHandle, FileId)
    end, ?READWRITE).


%%--------------------------------------------------------------------
%% @doc
%% Assures that changes made on file are persistent.
%% @end
%%--------------------------------------------------------------------
-spec fsync(handle(), boolean()) -> ok | error_reply().
fsync(SDHandle, DataOnly) ->
    run_with_file_handle(SDHandle, fun(FileHandle) ->
        helpers:fsync(FileHandle, DataOnly)
    end).


%%--------------------------------------------------------------------
%% @doc
%% Return type of file depending on its posix mode.
%% NOTE: currently only regular files and directories are supported
%% (hardlinks and symlinks are metadata-only structures).
%% @end
%%--------------------------------------------------------------------
-spec infer_type(Mode :: non_neg_integer()) -> {ok, onedata_file:type()} | ?ERROR_NOT_SUPPORTED.
infer_type(Mode) ->
    IsRegFile = (Mode band 8#100000) =/= 0,
    IsDir = (Mode band 8#40000) =/= 0,
    case {IsRegFile, IsDir} of
        {true, false} -> {ok, ?REGULAR_FILE_TYPE};
        {false, true} -> {ok, ?DIRECTORY_TYPE};
        {false, false} -> ?ERROR_NOT_SUPPORTED
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec read_internal(handle(), non_neg_integer(), non_neg_integer()) ->
    {ok, binary()} | {error, term()}.
read_internal(SDHandle, Offset, MaxSize) ->
    run_with_file_handle(SDHandle, fun(FileHandle) ->
        helpers:read(FileHandle, Offset, MaxSize)
    end).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Opens file in read mode and checks necessary permissions.
%% @end
%%--------------------------------------------------------------------
-spec open_for_read(handle()) -> {ok, handle()} | error_reply().
open_for_read(SDHandle) ->
    open_with_permissions_check(
        SDHandle#sd_handle{session_id = ?ROOT_SESS_ID},
        [?OPERATIONS(?read_object_mask)], read
    ).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Opens file in write mode and checks necessary permissions.
%% @end
%%--------------------------------------------------------------------
-spec open_for_write(handle()) -> {ok, handle()} | error_reply().
open_for_write(SDHandle) ->
    open_with_permissions_check(
        SDHandle#sd_handle{session_id = ?ROOT_SESS_ID},
        [?OPERATIONS(?write_object_mask)], write
    ).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Opens file in rdwr mode and checks necessary permissions.
%% @end
%%--------------------------------------------------------------------
-spec open_for_rdwr(handle()) -> {ok, handle()} | error_reply().
open_for_rdwr(SDHandle) ->
    open_with_permissions_check(
        SDHandle#sd_handle{session_id = ?ROOT_SESS_ID},
        [?OPERATIONS(?read_object_mask, ?write_object_mask)], rdwr
    ).


%%--------------------------------------------------------------------
%% @private
%% @equiv open/2, but with permission control
%% @end
%%--------------------------------------------------------------------
-spec open_with_permissions_check(handle(), [data_access_control:requirement()],
    helpers:open_flag()) -> {ok, handle()} | error_reply().
open_with_permissions_check(#sd_handle{
    session_id = SessionId,
    space_id = SpaceId,
    file_uuid = FileUuid,
    share_id = ShareId
} = SDHandle, AccessRequirements, OpenFlag) ->
    FileCtx = file_ctx:new_by_uuid(FileUuid, SpaceId, ShareId),
    UserCtx = user_ctx:new(SessionId),

    % TODO VFS-5917
    fslogic_authz:ensure_authorized(UserCtx, FileCtx, AccessRequirements),
    open_insecure(SDHandle, OpenFlag).


%%--------------------------------------------------------------------
%% @private
%% @equiv open/2, but without permission control
%% @end
%%--------------------------------------------------------------------
-spec open_insecure(handle(), OpenFlag :: helpers:open_flag()) ->
    {ok, handle()} | error_reply().
open_insecure(#sd_handle{file = FileId} = SDHandle, OpenFlag) ->
    run_with_helper_handle(retry_as_root_and_chown, SDHandle, fun(HelperHandle) ->
        case helpers:open(HelperHandle, FileId, OpenFlag) of
            {ok, FileHandle} ->
                {ok, SDHandle#sd_handle{
                    file_handle = FileHandle,
                    open_flag = OpenFlag
                }};
            {error, Reason} ->
                {error, Reason}
        end
    end).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns size of file, or 0 in case of error.
%% @end
%%--------------------------------------------------------------------
-spec get_size(file_meta:uuid(), od_space:id()) -> non_neg_integer().
get_size(FileUuid, _SpaceId) ->
    fslogic_location_cache:get_location_size(file_location:local_id(FileUuid), FileUuid).


%% @private
-spec run_with_file_handle(handle(), fun((helpers:file_handle()) -> Result)) ->
    Result when Result :: ok | {ok, term()} | {error, term()}.
run_with_file_handle(SDHandle, Fun) ->
    run_with_file_handle(SDHandle, Fun, ?READONLY).


%% @private
-spec run_with_file_handle(handle(), fun((helpers:file_handle()) -> Result), storage:access_type()) ->
    Result when Result :: ok | {ok, term()} | {error, term()}.
run_with_file_handle(SDHandle, Fun, SufficientAccessType) ->
    helpers_runner:run_with_file_handle_and_handle_error(SDHandle, Fun, SufficientAccessType).


%% @private
-spec run_with_helper_handle(
    fallback_strategy(),
    handle(),
    fun((helpers:helper_handle()) -> Result)
) ->
    Result when Result :: ok | {ok, term()} | {error, term()}.
run_with_helper_handle(FallbackStrategy, SDHandle, Fun) ->
    run_with_helper_handle(FallbackStrategy, SDHandle, Fun, ?READONLY).


%% @private
-spec run_with_helper_handle(
    fallback_strategy(),
    handle(),
    fun((helpers:helper_handle()) -> Result),
    storage:access_type()
) ->
    Result when Result :: ok | {ok, term()} | {error, term()}.
run_with_helper_handle(no_fallback, SDHandle, Fun, SufficientAccessType) ->
    helpers_runner:run_and_handle_error(SDHandle, Fun, SufficientAccessType);
run_with_helper_handle(FallbackStrategy, #sd_handle{
    session_id = SessionId,
    file_uuid = FileUuid,
    space_id = SpaceId
} = SDHandle, Fun, SufficientAccessType) ->
    case helpers_runner:run_and_handle_error(SDHandle, Fun, SufficientAccessType) of
        {error, Errno} = Error when Errno == ?EACCES orelse Errno == ?EPERM ->
            IsSpaceDir = fslogic_file_id:is_space_dir_uuid(FileUuid),
            IsSpaceOwner = session:is_space_owner(SessionId, SpaceId),

            % TODO VFS-11852 Consider fallback to ROOT for other users than space owner.
            % This may prevent EACCES errors in situations like:
            % 1. user created empty file - it was created in metadata but not on storage
            %    (lazy file creation)
            % 2. user change permissions for file and its parent directory to read only.
            % 3. user wants to read the file - file creation on storage (necessary to
            %    open file in read mode) will fail due to read only permissions.
            case IsSpaceOwner andalso not IsSpaceDir of
                true ->
                    FallbackResult = helpers_runner:run_and_handle_error(
                        SDHandle#sd_handle{session_id = ?ROOT_SESS_ID}, Fun, SufficientAccessType
                    ),
                    case FallbackStrategy of
                        retry_as_root ->
                            ok;
                        retry_as_root_and_chown ->
                            FileCtx = file_ctx:new_by_uuid(FileUuid, SpaceId),
                            files_to_chown:chown_or_defer(FileCtx)
                    end,
                    FallbackResult;
                false ->
                    Error
            end;
        Result ->
            Result
    end.
