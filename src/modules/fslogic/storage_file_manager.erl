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
-include("modules/fslogic/sfm_handle.hrl").
-include("modules/fslogic/helpers.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("storage_file_manager_errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("annotations/include/annotations.hrl").

-export([new_handle/5]).
-export([mkdir/2, mkdir/3, mv/2, chmod/2, chown/3, link/2]).
-export([stat/1, read/3, write/3, create/2, create/3, open/2, truncate/2, unlink/1,
    fsync/1]).

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
-spec new_handle(SessionId :: session:id(), SpaceUUID :: file_meta:uuid(), FileUUID :: file_meta:uuid(),
  Storage :: datastore:document(), FileId :: helpers:file()) ->
    handle().
new_handle(SessionId, SpaceUUID, FileUUID, Storage, FileId) ->
    #sfm_handle{
        session_id = SessionId,
        space_uuid = SpaceUUID,
        file_uuid = FileUUID,
        file = FileId,
        storage = Storage
    }.

%%--------------------------------------------------------------------
%% @doc
%% Opens the file. To used opened descriptor, pass returned handle to other functions.
%% File may and should be closed with release/1, but file will be closed automatically
%% when handle goes out of scope (term will be released by Erlang's GC).
%% @end
%%--------------------------------------------------------------------
-spec open(handle(), OpenMode :: helpers:open_mode()) ->
    {ok, handle()} | logical_file_manager:error_reply().
open(SFMHandle, read) ->
    open_for_read(SFMHandle);
open(SFMHandle, write) ->
    open_for_write(SFMHandle);
open(SFMHandle, rdwr) ->
    open_for_rdwr(SFMHandle).

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
mkdir(#sfm_handle{storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId} = SFMHandle, Mode, Recursive) ->
    Noop = fun(_) -> ok end,
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:set_user_ctx(HelperHandle, fslogic_storage:new_user_ctx(HelperInit, SessionId, SpaceUUID)),
    case helpers:mkdir(HelperHandle, FileId, Mode) of
        ok ->
            ok;
        {error, enoent} when Recursive ->
            Tokens = fslogic_path:split(FileId),
            case Tokens of
                [_] -> ok;
                [_ | _]  ->
                    LeafLess = fslogic_path:dirname(Tokens),
                    ok = mkdir(SFMHandle#sfm_handle{file = LeafLess}, ?AUTO_CREATED_PARENT_DIR_MODE, true)
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
%%
%% @end
%%--------------------------------------------------------------------
-spec mv(FileHandleFrom :: handle(), PathOnStorageTo :: file_meta:path()) ->
    ok | logical_file_manager:error_reply().
mv(_FileHandleFrom, _PathOnStorageTo) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Changes the permissions of a file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec chmod(handle(), NewMode :: file_meta:posix_permissions()) ->
    ok | logical_file_manager:error_reply().
chmod(#sfm_handle{storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId}, Mode) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:set_user_ctx(HelperHandle, fslogic_storage:new_user_ctx(HelperInit, SessionId, SpaceUUID)),
    helpers:chmod(HelperHandle, FileId, Mode).


%%--------------------------------------------------------------------
%% @doc
%% Changes owner of a file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec chown(FileHandle :: handle(), User :: user_id(), Group :: group_id()) ->
    ok | logical_file_manager:error_reply().
chown(#sfm_handle{storage = Storage, file = FileId, session_id = ?ROOT_SESS_ID}, UserId, SpaceId) ->
    {ok, #helper_init{name = StorageType} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),

    case StorageType of
        ?DIRECTIO_HELPER_NAME ->
            #posix_user_ctx{uid = Uid, gid = Gid} = fslogic_storage:get_posix_user_ctx(?DIRECTIO_HELPER_NAME, #identity{user_id = UserId}, SpaceUuid),
            helpers:chown(HelperHandle, FileId, Uid, Gid);
        _ ->
            ok
    end;
chown(_,_,_) ->
    throw(?EPERM).


%%--------------------------------------------------------------------
%% @doc
%% Creates a symbolic link on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec link(Path :: binary(), TargetFileHandle :: handle()) ->
    {ok, file_meta:uuid()} | logical_file_manager:error_reply().
link(_Path, _TargetFileHandle) ->
    {ok, <<"">>}.


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes, reading them from storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec stat(FileHandle :: handle()) ->
    {ok, undefined} | logical_file_manager:error_reply().
stat(_FileHandle) ->
    {ok, undefined}.


%%--------------------------------------------------------------------
%% @doc
%% Writes data to a file on storage. Returns number of written bytes.
%%
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: handle(), Offset :: non_neg_integer(), Buffer :: binary()) ->
    {ok, non_neg_integer()} | logical_file_manager:error_reply().
write(#sfm_handle{open_mode = undefined}, _, _) -> throw(?EPERM);
write(#sfm_handle{open_mode = read}, _, _) -> throw(?EPERM);
write(#sfm_handle{helper_handle = HelperHandle, file = File}, Offset, Buffer) ->
    helpers:write(HelperHandle, File, Offset, Buffer).


%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file from storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: handle(), Offset :: non_neg_integer(), MaxSize :: non_neg_integer()) ->
    {ok, binary()} | logical_file_manager:error_reply().
read(#sfm_handle{open_mode = undefined}, _, _) -> throw(?EPERM);
read(#sfm_handle{open_mode = write}, _, _) -> throw(?EPERM);
read(#sfm_handle{helper_handle = HelperHandle, file = File}, Offset, MaxSize) ->
    helpers:read(HelperHandle, File, Offset, MaxSize).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file on storage.
%%
%% @end
%%--------------------------------------------------------------------
-spec create(handle(), Mode :: non_neg_integer()) ->
    ok | logical_file_manager:error_reply().
create(Handle, Mode) ->
    create(Handle, Mode, false).

-spec create(handle(), Mode :: non_neg_integer(), Recursive :: boolean()) ->
    ok | logical_file_manager:error_reply().
create(#sfm_handle{storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId} = SFMHandle, Mode, Recursive) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:set_user_ctx(HelperHandle, fslogic_storage:new_user_ctx(HelperInit, SessionId, SpaceUUID)),
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
%%
%% @end
%%--------------------------------------------------------------------
-spec truncate(handle(), Size :: integer()) ->
    ok | logical_file_manager:error_reply().
truncate(#sfm_handle{open_mode = undefined}, _) -> throw(?EPERM);
truncate(#sfm_handle{open_mode = read}, _) -> throw(?EPERM);
truncate(#sfm_handle{storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId}, Size) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:set_user_ctx(HelperHandle, fslogic_storage:new_user_ctx(HelperInit, SessionId, SpaceUUID)),
    helpers:truncate(HelperHandle, FileId, Size).


%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec unlink(handle()) -> ok | logical_file_manager:error_reply().
unlink(#sfm_handle{storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId}) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:set_user_ctx(HelperHandle, fslogic_storage:new_user_ctx(HelperInit, SessionId, SpaceUUID)),
    helpers:unlink(HelperHandle, FileId).

%%--------------------------------------------------------------------
%% @doc
%% Assures that changes made on file are persistent.
%% @end
%%--------------------------------------------------------------------
-spec fsync(handle()) -> ok | logical_file_manager:error_reply().
fsync(#sfm_handle{storage = Storage, file = FileId, space_uuid = SpaceUUID, session_id = SessionId}) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:set_user_ctx(HelperHandle, fslogic_storage:new_user_ctx(HelperInit, SessionId, SpaceUUID)),
    helpers:fsync(HelperHandle, FileId, true).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Opens file in read mode and checks necessary permissions.
%%--------------------------------------------------------------------
-spec open_for_read(handle()) ->
    {ok, handle()} | logical_file_manager:error_reply().
-check_permissions([{?read_object, 1}]).
open_for_read(LfmHandle) ->
    open_impl(LfmHandle#sfm_handle{session_id = ?ROOT_SESS_ID}, read).

%%--------------------------------------------------------------------
%% @doc Opens file in write mode and checks necessary permissions.
%%--------------------------------------------------------------------
-spec open_for_write(handle()) ->
    {ok, handle()} | logical_file_manager:error_reply().
-check_permissions([{?write_object, 1}]).
open_for_write(LfmHandle) ->
    open_impl(LfmHandle#sfm_handle{session_id = ?ROOT_SESS_ID}, write).

%%--------------------------------------------------------------------
%% @doc Opens file in rdwr mode and checks necessary permissions.
%%--------------------------------------------------------------------
-spec open_for_rdwr(handle()) ->
    {ok, handle()} | logical_file_manager:error_reply().
-check_permissions([{?read_object, 1}, {?write_object, 1}]).
open_for_rdwr(LfmHandle) ->
    open_impl(LfmHandle#sfm_handle{session_id = ?ROOT_SESS_ID}, rdwr).

%%--------------------------------------------------------------------
%% @doc
%% @equiv open/2, but without permission control
%% @end
%%--------------------------------------------------------------------
-spec open_impl(handle(), OpenMode :: helpers:open_mode()) ->
    {ok, handle()} | logical_file_manager:error_reply().
open_impl(#sfm_handle{storage = Storage, file = FileId, session_id = SessionId, space_uuid = SpaceUUID} = SFMHandle, OpenMode) ->
    {ok, #helper_init{} = HelperInit} = fslogic_storage:select_helper(Storage),
    HelperHandle = helpers:new_handle(HelperInit),
    helpers:set_user_ctx(HelperHandle, fslogic_storage:new_user_ctx(HelperInit, SessionId, SpaceUUID)),
    case helpers:open(HelperHandle, FileId, OpenMode) of
        {ok, _} ->
            {ok, SFMHandle#sfm_handle{helper_handle = HelperHandle, open_mode = OpenMode}};
        {error, Reason} ->
            {error, Reason}
    end.