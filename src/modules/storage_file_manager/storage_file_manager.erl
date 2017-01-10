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

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("storage_file_manager_errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("annotations/include/annotations.hrl").

-export([new_handle/5, new_handle/6, new_handle/7]).
-export([mkdir/2, mkdir/3, mv/2, chmod/2, chown/3, symlink/2, link/2, readdir/3]).
-export([stat/1, read/3, write/3, create/2, create/3, open/2, truncate/2, unlink/1,
    fsync/1]).
-export([open_at_creation/1]).

-type handle() :: #sfm_handle{}.

-export_type([handle/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv new_handle(SessionId, SpaceUUID, FileUUID, Storage, FileId, undefined).
%%--------------------------------------------------------------------
-spec new_handle(SessionId :: session:id(), SpaceUUID :: file_meta:uuid(), FileUUID :: file_meta:uuid() | undefined,
    Storage :: datastore:document(), FileId :: helpers:file()) ->
    handle().
new_handle(SessionId, SpaceUUID, FileUUID, Storage, FileId) ->
    new_handle(SessionId, SpaceUUID, FileUUID, Storage, FileId, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Opens the file. To used opened descriptor, pass returned handle to other functions.
%% File may and should be closed with release/1, but file will be closed automatically
%% when handle goes out of scope (term will be released by Erlang's GC).
%% Handle created by this function may not be used for remote files.
%% @end
%%--------------------------------------------------------------------
-spec new_handle(SessionId :: session:id(), SpaceUUID :: file_meta:uuid(), FileUUID :: file_meta:uuid() | undefined,
    Storage :: datastore:document(), FileId :: helpers:file(), ShareId :: od_share:id() | undefined) ->
    handle().
new_handle(SessionId, SpaceUUID, FileUUID, #document{} = Storage, FileId, ShareId) ->
    FSize =
        case FileUUID of
            undefined ->
                0;
            _ ->
                case catch fslogic_blocks:get_file_size({uuid, FileUUID}) of
                    Size when is_integer(Size) ->
                        Size;
                    _ -> 0
                end
        end,
    #sfm_handle{
        session_id = SessionId,
        space_uuid = SpaceUUID,
        file_uuid = FileUUID,
        file = FileId,
        provider_id = oneprovider:get_provider_id(),
        is_local = true,
        storage = Storage,
        file_size = FSize,
        share_id = ShareId
    }.

%%--------------------------------------------------------------------
%% @doc
%% Opens the file. To used opened descriptor, pass returned handle to other functions.
%% File may and should be closed with release/1, but file will be closed automatically
%% when handle goes out of scope (term will be released by Erlang's GC).
%% This function (not like new_handle/5) does not assume that given file is local.
%% Therefore handle created with this function may be used for remote files.
%% @end
%%--------------------------------------------------------------------
-spec new_handle(SessionId :: session:id(), SpaceUUID :: file_meta:uuid(), FileUUID :: file_meta:uuid(),
    StorageId :: storage:id(), FileId :: helpers:file(), od_share:id() | undefined, oneprovider:id()) ->
    handle().
new_handle(SessionId, SpaceUUID, FileUUID, StorageId, FileId, ShareId, ProviderId) ->
    {IsLocal, Storage, Size} =
        case oneprovider:get_provider_id() of
            ProviderId ->
                {ok, S} = storage:get(StorageId),
                FSize =
                    case catch fslogic_blocks:get_file_size({uuid, FileUUID}) of
                        Size0 when is_integer(Size0) ->
                            Size0;
                        _ -> 0
                    end,
                {true, S, FSize};
            _ ->
                {false, undefined, undefined}
        end,
    #sfm_handle{
        session_id = SessionId,
        space_uuid = SpaceUUID,
        file_uuid = FileUUID,
        file = FileId,
        provider_id = ProviderId,
        is_local = IsLocal,
        storage = Storage,
        storage_id = StorageId,
        file_size = Size,
        share_id = ShareId
    }.

%%--------------------------------------------------------------------
%% @doc
%% Opens the file. To used opened descriptor, pass returned handle to other functions.
%% File may and should be closed with release/1, but file will be closed automatically
%% when handle goes out of scope (term will be released by Erlang's GC).
%% @end
%%--------------------------------------------------------------------
-spec open(handle(), OpenFlag :: helpers:open_flag()) ->
    {ok, handle()} | logical_file_manager:error_reply().
open(#sfm_handle{is_local = true} = SFMHandle, read) ->
    open_for_read(SFMHandle);
open(#sfm_handle{is_local = true} = SFMHandle, write) ->
    open_for_write(SFMHandle);
open(#sfm_handle{is_local = true} = SFMHandle, rdwr) ->
    open_for_rdwr(SFMHandle);
open(#sfm_handle{is_local = false} = SFMHandle, _) ->
    {ok, SFMHandle}.

%%--------------------------------------------------------------------
%% @doc
%% Opens the file. To used opened descriptor, pass returned handle to other functions.
%% File may and should be closed with release/1, but file will be closed automatically
%% when handle goes out of scope (term will be released by Erlang's GC).
%% Bypasses permissions check to allow to open file at creation.
%% @end
% TODO - relese in spec - how (missing release function in sfm)?
%%--------------------------------------------------------------------
-spec open_at_creation(handle()) ->
    {ok, handle()} | logical_file_manager:error_reply().
open_at_creation(SFMHandle) ->
    open_impl(SFMHandle#sfm_handle{session_id = ?ROOT_SESS_ID}, rdwr).

%%--------------------------------------------------------------------
%% @doc
%% Creates a directory on storage.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(handle(), Mode :: non_neg_integer()) ->
    ok | logical_file_manager:error_reply().
mkdir(Handle, Mode) ->
    mkdir(Handle, Mode, false).

%%--------------------------------------------------------------------
%% @doc
%% Creates a directory on storage. Recursive states whether parent directories shall be also created.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(handle(), Mode :: non_neg_integer(), Recursive :: boolean()) ->
    ok | logical_file_manager:error_reply().
mkdir(#sfm_handle{is_local = true, storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId} = SFMHandle, Mode, Recursive) ->
    Noop = fun(_) -> ok end,

    {ok, HelperHandle} = session:get_helper(SessionId, SpaceUUID, Storage),

    case helpers:mkdir(HelperHandle, FileId, Mode) of
        ok ->
            ok;
        {error, enoent} when Recursive ->
            Tokens = fslogic_path:split(FileId),
            case Tokens of
                [_] -> ok;
                [_ | _] ->
                    LeafLess = fslogic_path:dirname(Tokens),
                    case mkdir(SFMHandle#sfm_handle{file = LeafLess}, ?AUTO_CREATED_PARENT_DIR_MODE, true) of
                        ok -> ok;
                        {error, eexist} -> ok;
                        ParentError ->
                            ?error("Cannot create parent for file ~p, error ~p", [FileId, ParentError]),
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
-spec mv(FileHandleFrom :: handle(), FileTo :: helpers:file()) ->
    ok | logical_file_manager:error_reply().
mv(#sfm_handle{storage = Storage, file = FileFrom, space_uuid = SpaceUUID, session_id = SessionId}, FileTo) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceUUID, Storage),
    helpers:rename(HelperHandle, FileFrom, FileTo).


%%--------------------------------------------------------------------
%% @doc
%% Changes the permissions of a file on storage.
%% @end
%%--------------------------------------------------------------------
-spec chmod(handle(), NewMode :: file_meta:posix_permissions()) ->
    ok | logical_file_manager:error_reply().
chmod(#sfm_handle{is_local = true, storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId}, Mode) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceUUID, Storage),
    helpers:chmod(HelperHandle, FileId, Mode).


%%--------------------------------------------------------------------
%% @doc
%% Changes owner of a file on storage.
%% @end
%%--------------------------------------------------------------------
-spec chown(FileHandle :: handle(), User :: user_id(), Group :: group_id()) ->
    ok | logical_file_manager:error_reply().
chown(#sfm_handle{storage = Storage, file = FileId, session_id = ?ROOT_SESS_ID,
    space_uuid = SpaceUUID}, UserId, SpaceId) ->
    {ok, HelperHandle} = session:get_helper(?ROOT_SESS_ID, SpaceUUID, Storage),
    {Uid, Gid} = luma:get_posix_user_ctx(UserId, SpaceId),
    helpers:chown(HelperHandle, FileId, Uid, Gid);
chown(_, _, _) ->
    throw(?EPERM).


%%--------------------------------------------------------------------
%% @doc
%% Creates a symbolic link on storage.
%% @end
%%--------------------------------------------------------------------
-spec symlink(Path :: binary(), TargetFileHandle :: handle()) ->
    {ok, file_meta:uuid()} | logical_file_manager:error_reply().
symlink(_Path, _TargetFileHandle) ->
    {ok, <<"">>}.

%%--------------------------------------------------------------------
%% @doc
%% Creates a link on storage.
%% @end
%%--------------------------------------------------------------------
-spec link(FileHandleFrom :: handle(), FileTo :: helpers:file()) ->
    ok | logical_file_manager:error_reply().
link(#sfm_handle{storage = Storage, file = FileFrom, space_uuid = SpaceUUID, session_id = SessionId}, FileTo) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceUUID, Storage),
    helpers:link(HelperHandle, FileFrom, FileTo).

%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes, reading them from storage.
%% @end
%%--------------------------------------------------------------------
-spec stat(FileHandle :: handle()) ->
    {ok, undefined} | logical_file_manager:error_reply().
stat(#sfm_handle{storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId}) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceUUID, Storage),
    helpers:getattr(HelperHandle, FileId).


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes, reading them from storage.
%% @end
%%--------------------------------------------------------------------
-spec readdir(FileHandle :: handle(), Offset :: non_neg_integer(), Count :: non_neg_integer()) ->
    {ok, [helpers:file()]} | logical_file_manager:error_reply().
readdir(#sfm_handle{storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId}, Offset, Count) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceUUID, Storage),
    helpers:readdir(HelperHandle, FileId, Offset, Count).


%%--------------------------------------------------------------------
%% @doc
%% Writes data to a file on storage. Returns number of written bytes.
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: handle(), Offset :: non_neg_integer(), Buffer :: binary()) ->
    {ok, non_neg_integer()} | logical_file_manager:error_reply().
write(#sfm_handle{is_local = true, open_flag = undefined}, _, _) ->
    throw(?EPERM);
write(#sfm_handle{is_local = true, open_flag = read}, _, _) -> throw(?EPERM);
write(#sfm_handle{space_uuid = SpaceUUID, is_local = true, file_handle = FileHandle, file_size = CSize}, Offset, Buffer) ->
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID),
    %% @todo: VFS-2086 handle sparse files
    space_quota:soft_assert_write(SpaceId, max(0, Offset + size(Buffer) - CSize)),
    helpers:write(FileHandle, Offset, Buffer);

write(#sfm_handle{is_local = false, session_id = SessionId, file_uuid = FileUUID, storage_id = SID, file = FID, space_uuid = SpaceUUID}, Offset, Data) ->
    FileGUID = fslogic_uuid:uuid_to_guid(FileUUID, fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID)),
    ProxyIORequest = #proxyio_request{
        parameters = #{?PROXYIO_PARAMETER_FILE_GUID => FileGUID}, storage_id = SID, file_id = FID,
        proxyio_request = #remote_write{byte_sequence = [#byte_sequence{offset = Offset, data = Data}]}},
    case worker_proxy:call(fslogic_worker, {proxyio_request, SessionId, ProxyIORequest}) of
        {ok, #proxyio_response{status = #status{code = ?OK}, proxyio_response = #remote_write_result{wrote = Wrote}}} ->
            {ok, Wrote};
        {ok, #proxyio_response{status = #status{code = Code}}} ->
            {error, Code}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file from storage.
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: handle(), Offset :: non_neg_integer(), MaxSize :: non_neg_integer()) ->
    {ok, binary()} | logical_file_manager:error_reply().
read(#sfm_handle{is_local = true, open_flag = undefined}, _, _) ->
    throw(?EPERM);
read(#sfm_handle{is_local = true, open_flag = write}, _, _) -> throw(?EPERM);
read(#sfm_handle{is_local = true, file_handle = FileHandle}, Offset, MaxSize) ->
    helpers:read(FileHandle, Offset, MaxSize);

read(#sfm_handle{is_local = false, session_id = SessionId, file_uuid = FileUUID, storage_id = SID, file = FID, space_uuid = SpaceUUID}, Offset, Size) ->
    FileGUID = fslogic_uuid:uuid_to_guid(FileUUID, fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID)),
    ProxyIORequest = #proxyio_request{
        parameters = #{?PROXYIO_PARAMETER_FILE_GUID => FileGUID}, storage_id = SID, file_id = FID,
        proxyio_request = #remote_read{offset = Offset, size = Size}},
    case worker_proxy:call(fslogic_worker, {proxyio_request, SessionId, ProxyIORequest}) of
        {ok, #proxyio_response{status = #status{code = ?OK}, proxyio_response = #remote_data{data = Data}}} ->
            {ok, Data};
        {ok, #proxyio_response{status = #status{code = Code}}} ->
            {error, Code}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file on storage.
%% @end
%%--------------------------------------------------------------------
-spec create(handle(), Mode :: non_neg_integer()) ->
    ok | logical_file_manager:error_reply().
create(Handle, Mode) ->
    create(Handle, Mode, false).

-spec create(handle(), Mode :: non_neg_integer(), Recursive :: boolean()) ->
    ok | logical_file_manager:error_reply().
create(#sfm_handle{is_local = true, storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId} = SFMHandle, Mode, Recursive) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceUUID, Storage),
    case helpers:mknod(HelperHandle, FileId, Mode, reg) of
        ok ->
            ok;
        {error, enoent} when Recursive ->
            Tokens = fslogic_path:split(FileId),
            LeafLess = fslogic_path:join(lists:sublist(Tokens, 1, length(Tokens) - 1)),
            ok =
                case mkdir(SFMHandle#sfm_handle{file = LeafLess}, ?AUTO_CREATED_PARENT_DIR_MODE, true) of
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
-spec truncate(handle(), Size :: integer()) ->
    ok | logical_file_manager:error_reply().
truncate(#sfm_handle{is_local = true, open_flag = undefined}, _) ->
    throw(?EPERM);
truncate(#sfm_handle{is_local = true, open_flag = read}, _) -> throw(?EPERM);
truncate(#sfm_handle{is_local = true, storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId}, Size) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceUUID, Storage),
    helpers:truncate(HelperHandle, FileId, Size).


%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%% @end
%%--------------------------------------------------------------------
-spec unlink(handle()) -> ok | logical_file_manager:error_reply().
unlink(#sfm_handle{is_local = true, storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId}) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceUUID, Storage),
    helpers:unlink(HelperHandle, FileId).

%%--------------------------------------------------------------------
%% @doc
%% Assures that changes made on file are persistent.
%% @end
%%--------------------------------------------------------------------
-spec fsync(handle()) -> ok | logical_file_manager:error_reply().
fsync(#sfm_handle{is_local = false}) ->
    ok;
fsync(#sfm_handle{file_handle = FileHandle}) ->
    helpers:fsync(FileHandle, true).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Opens file in read mode and checks necessary permissions.
%%--------------------------------------------------------------------
-spec open_for_read(handle()) ->
    {ok, handle()} | logical_file_manager:error_reply().
-check_permissions([{?read_object, 1}]).
open_for_read(SFMHandle) ->
    open_impl(SFMHandle#sfm_handle{session_id = ?ROOT_SESS_ID}, read).

%%--------------------------------------------------------------------
%% @doc Opens file in write mode and checks necessary permissions.
%%--------------------------------------------------------------------
-spec open_for_write(handle()) ->
    {ok, handle()} | logical_file_manager:error_reply().
-check_permissions([{?write_object, 1}]).
open_for_write(SFMHandle) ->
    open_impl(SFMHandle#sfm_handle{session_id = ?ROOT_SESS_ID}, write).

%%--------------------------------------------------------------------
%% @doc Opens file in rdwr mode and checks necessary permissions.
%%--------------------------------------------------------------------
-spec open_for_rdwr(handle()) ->
    {ok, handle()} | logical_file_manager:error_reply().
-check_permissions([{?read_object, 1}, {?write_object, 1}]).
open_for_rdwr(SFMHandle) ->
    open_impl(SFMHandle#sfm_handle{session_id = ?ROOT_SESS_ID}, rdwr).

%%--------------------------------------------------------------------
%% @doc
%% @equiv open/2, but without permission control
%% @end
%%--------------------------------------------------------------------
-spec open_impl(handle(), OpenFlag :: helpers:open_flag()) ->
    {ok, handle()} | logical_file_manager:error_reply().
open_impl(#sfm_handle{is_local = true, storage = Storage, file = FileId, session_id = SessionId, space_uuid = SpaceUUID} = SFMHandle, OpenFlag) ->
    {ok, HelperHandle} = session:get_helper(SessionId, SpaceUUID, Storage),
    case helpers:open(HelperHandle, FileId, OpenFlag) of
        {ok, FileHandle} ->
            {ok, SFMHandle#sfm_handle{file_handle = FileHandle, open_flag = OpenFlag}};
        {error, Reason} ->
            {error, Reason}
    end.
