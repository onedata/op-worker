%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @todo simplify rename logic here
%%% @doc
%%% Utility functions for storage file manager module.
%%% @end
%%%--------------------------------------------------------------------
-module(sd_utils).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_sufix.hrl").
-include("proto/oneclient/common_messages.hrl").

-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([chmod_storage_file/3, rename_storage_file/6,
    create_delayed_storage_file/1, create_delayed_storage_file/4,
    delete_storage_file/2, create_parent_dirs/1, recursive_delete/2]).

% For spawning
-export([retry_dir_deletion/3]).

% Test API
-export([create_storage_file/3]).

-define(CLEANUP_MAX_RETRIES_NUM, 10).
-define(CLEANUP_DELAY, 5).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Change mode of storage files related with given file_meta.
%% @end
%%--------------------------------------------------------------------
-spec chmod_storage_file(user_ctx:ctx(), file_ctx:ctx(),
    file_meta:posix_permissions()) -> ok | {error, Reason :: term()} | no_return().
chmod_storage_file(UserCtx, FileCtx, Mode) ->
    SessId = user_ctx:get_session_id(UserCtx),
    case file_ctx:is_dir(FileCtx) of
        {true, _FileCtx2} ->
            ok;
        {false, FileCtx2} ->
            case storage_driver:new_handle(SessId, FileCtx2, false) of
                {undefined, _} ->
                    ok;
                {SDHandle, _} ->
                    case storage_driver:chmod(SDHandle, Mode) of
                        ok -> ok;
                        {error, ?ENOENT} -> ok;
                        {error, ?EROFS} -> {error, ?EROFS}
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Renames file on storage.
%% @end
%%--------------------------------------------------------------------
-spec rename_storage_file(session:id(), od_space:id(), storage:id(),
    file_meta:uuid(), helpers:file_id(), helpers:file_id()) -> ok | {error, term()}.
rename_storage_file(SessId, SpaceId, StorageId, FileUuid, SourceFileId, TargetFileId) ->
    %create target dir
    TargetDir = filename:dirname(TargetFileId),
    TargetDirHandle = storage_driver:new_handle(?ROOT_SESS_ID,
        SpaceId, undefined, StorageId, TargetDir, undefined),
    case storage_driver:mkdir(TargetDirHandle,
        ?AUTO_CREATED_PARENT_DIR_MODE, true)
    of
        ok ->
            ok;
        {error, ?EEXIST} ->
            ok
    end,

    case SourceFileId =/= TargetFileId of
        true ->
            SourceHandle = storage_driver:new_handle(SessId, SpaceId,
                FileUuid, StorageId, SourceFileId, undefined),
            storage_driver:mv(SourceHandle, TargetFileId);
            % TODO VFS-5290 - solution resutls in problems with sed
%%            TargetHandle = storage_driver:new_handle(SessId, SpaceId,
%%                FileUuid, Storage, TargetFileId, undefined),
%%
%%            case storage_driver:stat(TargetHandle) of
%%                {ok, _} ->
%%                    ?warning("Moving file into existing one, source ~p, target ~p",
%%                        [SourceFileId, TargetFileId]),
%%                    NewTargetFileId = ?CONFLICTING_STORAGE_FILE_NAME(TargetFileId, FileUuid),
%%                    storage_driver:mv(SourceHandle, NewTargetFileId);
%%                _ ->
%%                    storage_driver:mv(SourceHandle, TargetFileId)
%%            end;
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Create storage file if it hasn't been created yet (it has been delayed).
%% Creation is performed with root credentials.
%% @end
%%--------------------------------------------------------------------
-spec create_delayed_storage_file(file_ctx:ctx()) -> {file_meta:doc(), file_ctx:ctx()} | {error, cancelled}.
create_delayed_storage_file(FileCtx) ->
    create_delayed_storage_file(FileCtx, user_ctx:new(?ROOT_SESS_ID), false, true).

%%--------------------------------------------------------------------
%% @doc
%% Create storage file if it hasn't been created yet (it has been delayed)
%% @end
%%--------------------------------------------------------------------
-spec create_delayed_storage_file(file_ctx:ctx(), user_ctx:ctx(), boolean(), boolean()) ->
    {file_meta:doc(), file_ctx:ctx()} | {error, cancelled}.
create_delayed_storage_file(FileCtx, UserCtx, VerifyDeletionLink, CheckLocationExists) ->
    {#document{
        key = FileLocationId,
        value = #file_location{storage_file_created = StorageFileCreated}
    }, FileCtx2} = Ans = file_ctx:get_or_create_local_regular_file_location_doc(FileCtx, false, CheckLocationExists),

    case StorageFileCreated of
        false ->
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            % TODO VFS-5270
            replica_synchronizer:apply(FileCtx, fun() ->
                try
                    case location_and_link_utils:is_location_created(FileUuid, FileLocationId) of
                        true ->
                            Ans;
                        _ ->
                            FileCtx3 = ?MODULE:create_storage_file(UserCtx, FileCtx2, VerifyDeletionLink),
                            {StorageFileId, FileCtx4} = file_ctx:get_storage_file_id(FileCtx3),

                            {ok, #document{} = Doc} = location_and_link_utils:mark_location_created(
                                FileUuid, FileLocationId, StorageFileId),
                            {Doc, FileCtx4}
                    end
                catch
                    _:{badmatch,{error, not_found}} ->
                        {error, cancelled}
                end
            end);
        true ->
            Ans
    end.

%%--------------------------------------------------------------------
%% @doc
%% Create storage file.
%% @end
%%--------------------------------------------------------------------
-spec create_storage_file(user_ctx:ctx(), file_ctx:ctx(), boolean()) ->
    file_ctx:ctx().
create_storage_file(UserCtx, FileCtx, VerifyDeletionLink) ->
    {#document{value = #file_meta{mode = Mode, owner = OwnerUserId}}, FileCtx2} =
        file_ctx:get_file_doc(FileCtx),
    {Chown, SessId} = case user_ctx:get_user_id(UserCtx) =:= OwnerUserId of
        true -> {false, user_ctx:get_session_id(UserCtx)};
        _ -> {true, user_ctx:get_session_id(user_ctx:new(?ROOT_SESS_ID))}
    end,

    {SDHandle, FileCtx3} = storage_driver:new_handle(SessId, FileCtx2),
    {ok, FinalCtx} = case storage_driver:create(SDHandle, Mode) of
        {error, ?ENOENT} ->
            FileCtx4 = create_parent_dirs(FileCtx3),
            {storage_driver:create(SDHandle, Mode), FileCtx4};
        {error, ?EEXIST} ->
            handle_eexists(VerifyDeletionLink, SDHandle, Mode, FileCtx3, UserCtx);
         {error, ?EACCES} ->
            % eacces is possible because there is race condition
            % on creating and chowning parent dir
            % for this reason it is acceptable to try chowning parent once
            {ParentCtx, FileCtx4} = file_ctx:get_parent(FileCtx3, UserCtx),
            files_to_chown:chown_or_schedule_chowning(ParentCtx),
            {storage_driver:create(SDHandle, Mode), FileCtx4};
        ok ->
            {StorageDoc, FileCtx4} = file_ctx:get_storage_doc(FileCtx3),
            FileCtx6 = case storage_sync_worker:is_syncable_object_storage(StorageDoc) of
                true ->
                    {ParentCtx, FileCtx5} = file_ctx:get_parent(FileCtx4, UserCtx),
                    mark_parent_dirs_created_on_storage(ParentCtx, UserCtx),
                    FileCtx5;
                false ->
                    FileCtx4
            end,
            {ok, FileCtx6}
    end,

    case Chown of
        true -> files_to_chown:chown_or_schedule_chowning(FinalCtx);
        _ -> FinalCtx
    end.


%%--------------------------------------------------------------------
%% @doc
%% Removes file from storage.
%% @end
%%--------------------------------------------------------------------
-spec delete_storage_file(file_ctx:ctx(), user_ctx:ctx()) ->
    ok | {error, term()}.
delete_storage_file(FileCtx, UserCtx) ->
    SessId = user_ctx:get_session_id(UserCtx),
    case storage_driver:new_handle(SessId, FileCtx, false) of
        {undefined, _} ->
            {error, ?ENOENT};
        {SDHandle, _} ->
            {Size, _} = file_ctx:get_file_size(FileCtx),
            storage_driver:unlink(SDHandle, Size)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes given file. If it's a directory all its children will be
%% deleted to.
%% @end
%%--------------------------------------------------------------------
-spec recursive_delete(file_ctx:ctx(), user_ctx:ctx()) -> ok | {error, term()}.
recursive_delete(FileCtx, UserCtx) ->
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
    case IsDir of
        true ->
            {ok, ChunkSize} = application:get_env(?APP_NAME, ls_chunk_size),
            {ok, FileCtx3} = delete_children(FileCtx2, UserCtx, 0, ChunkSize),
            delete_storage_dir(FileCtx3, UserCtx);
        false ->
            delete_storage_file(FileCtx2, UserCtx)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes directory from storage.
%% @end
%%--------------------------------------------------------------------
-spec delete_storage_dir(file_ctx:ctx(), user_ctx:ctx()) ->
    ok | {error, term()}.
delete_storage_dir(FileCtx, UserCtx) ->
    SessId = user_ctx:get_session_id(UserCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    case storage_driver:new_handle(SessId, FileCtx, false) of
        {undefined, _} ->
            ok;
        {SDHandle, _} ->
            case storage_driver:rmdir(SDHandle) of
                ok ->
                    dir_location:delete(FileUuid);
                {error, ?ENOENT} ->
                    dir_location:delete(FileUuid);
                {error, ?ENOTEMPTY} ->
                    % todo VFS-4997
                    spawn(?MODULE, retry_dir_deletion, [SDHandle, FileUuid, 0]),
                    ok;
                {error,'Function not implemented'} = Error ->
                    % Some helpers do not support rmdir
                    ?debug("sd_utils:delete_storage_dir failed with ~p", [Error]),
                    ok;
                Error ->
                    ?error("sd_utils:delete_storage_dir failed with ~p", [Error]),
                    Error
            end
    end.

%%-------------------------------------------------------------------
%% @doc
%% This function is used to delay cleanup of directory on storage
%% if it fails with ENOTEMPTY.
%% @end
%%-------------------------------------------------------------------
-spec retry_dir_deletion(storage_driver:handle(), file_meta:uuid(),
    non_neg_integer()) -> ok  | {error, term()}.
retry_dir_deletion(#sd_handle{file = FileId, space_id = SpaceId}, _FileUuid, ?CLEANUP_MAX_RETRIES_NUM) ->
    ?error("Could not delete directory ~p on storage in space ~p", [FileId, SpaceId]);
retry_dir_deletion(SDHandle = #sd_handle{
    file = FileId,
    space_id = SpaceId
}, FileUuid, RetryNum) ->
    ?debug(
        "Delayed deletion of directory ~p on storage in space ~p. Retry number: ~p",
        [FileId, SpaceId, RetryNum + 1]),
    timer:sleep(timer:seconds(?CLEANUP_DELAY)),
    case storage_driver:rmdir(SDHandle) of
        ok ->
            dir_location:delete(FileUuid);
        {error, ?ENOENT} ->
            dir_location:delete(FileUuid);
        {error, ?ENOTEMPTY} ->
            retry_dir_deletion(SDHandle, FileUuid, RetryNum + 1);
        Error ->
            ?error(
                "Unexpected error when trying to delete directory ~p on storage in space ~p",
                [FileId, SpaceId]),
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates parent dir on storage
%% @end
%%--------------------------------------------------------------------
-spec create_parent_dirs(file_ctx:ctx()) -> file_ctx:ctx().
create_parent_dirs(FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {#document{key = StorageId}, FileCtx3} = file_ctx:get_storage_doc(FileCtx),
    {ParentCtx, FileCtx4} = file_ctx:get_parent(FileCtx3, undefined),
    create_parent_dirs(ParentCtx, [], SpaceId, StorageId),
    FileCtx4.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Tail recursive helper function of ?MODULE:create_parent_dirs/1
%% @end
%%-------------------------------------------------------------------
-spec create_parent_dirs(file_ctx:ctx(), [file_ctx:ctx()], od_space:id(), storage:id()) -> ok.
create_parent_dirs(FileCtx, ChildrenDirCtxs, SpaceId, StorageId) ->
    case file_ctx:is_space_dir_const(FileCtx) of
        true ->
            lists:foreach(fun(Ctx) ->
                create_dir(Ctx, SpaceId, StorageId)
            end, [FileCtx | ChildrenDirCtxs]);
        false ->
            %TODO VFS-4297 stop recursion when parent file exists on storage,
            %TODO currently recursion stops in space dir
            {ParentCtx, FileCtx2} = file_ctx:get_parent(FileCtx, undefined),
            create_parent_dirs(ParentCtx, [FileCtx2 | ChildrenDirCtxs], SpaceId, StorageId)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Creates directory on storage with suitable mode and owner.
%% @end
%%-------------------------------------------------------------------
-spec create_dir(file_ctx:ctx(), od_space:id(), storage:id()) -> file_ctx:ctx().
create_dir(FileCtx, SpaceId, StorageId) ->
    {FileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    SDHandle0 = storage_driver:new_handle(?ROOT_SESS_ID, SpaceId,
        undefined, StorageId, FileId, undefined),
    try
        {#document{value = #file_meta{
            mode = Mode}}, FileCtx3} = file_ctx:get_file_doc(FileCtx2),
        case file_ctx:is_space_dir_const(FileCtx3) of
            true ->
                mkdir_and_maybe_chown(SDHandle0, ?AUTO_CREATED_PARENT_DIR_MODE,
                    FileCtx3, false);
            false ->
                mkdir_and_maybe_chown(SDHandle0, Mode, FileCtx3, true)
        end
    catch
        Error:Reason ->
            ?error_stacktrace("Creating parent dir ~p failed due to ~p.
            Parent dir will be create with default mode.", [FileId, {Error, Reason}]),
            mkdir_and_maybe_chown(SDHandle0, ?AUTO_CREATED_PARENT_DIR_MODE, FileCtx, true)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Creates directory on storage and chowns it if ShouldChown flag is set to
%% true.
%% @end
%%-------------------------------------------------------------------
-spec mkdir_and_maybe_chown(storage_driver:handle(), non_neg_integer(),
    file_ctx:ctx(), boolean()) -> any().
mkdir_and_maybe_chown(SDHandle, Mode, FileCtx, ShouldChown) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case storage_driver:mkdir(SDHandle, Mode, false) of
        ok ->
            case dir_location:mark_dir_created_on_storage(FileUuid, SpaceId) of
                {ok, _} -> ok;
                % helpers on ceph and s3 always return ok on mkdir operation
                % so we have to handle situation when doc is already in db
                {error, already_exists} -> ok
            end;
        {error, ?EEXIST} -> ok
    end,
    case ShouldChown of
        true ->
            files_to_chown:chown_or_schedule_chowning(FileCtx);
        false ->
            ok
    end,
    FileCtx.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function recursively deletes all children of given directory.
%% @end
%%-------------------------------------------------------------------
-spec delete_children(file_ctx:ctx(), user_ctx:ctx(), non_neg_integer(),
    non_neg_integer()) -> {ok, file_ctx:ctx()}.
delete_children(FileCtx, UserCtx, Offset, ChunkSize) ->
    % todo VFS-4997
    {ChildrenCtxs, FileCtx2} = file_ctx:get_file_children(FileCtx,
        UserCtx, Offset, ChunkSize),
    lists:foreach(fun(ChildCtx) ->
        ok = case recursive_delete(ChildCtx, UserCtx) of
            ok -> ok;
            {error, ?ENOENT} -> ok;
            Error -> Error
        end
    end, ChildrenCtxs),
    case length(ChildrenCtxs) < ChunkSize of
        true ->
            {ok, FileCtx2};
        false ->
            delete_children(FileCtx2, UserCtx, Offset + ChunkSize, ChunkSize)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Creates file on storage with uuid as suffix
%% @end
%%-------------------------------------------------------------------
-spec create_storage_file_with_suffix(storage_driver:handle(),
    file_meta:posix_permissions()) -> {ok, helpers:file_id()}.
create_storage_file_with_suffix(#sd_handle{file_uuid = Uuid, file = FileId} = SDHandle, Mode) ->
    NewName = ?CONFLICTING_STORAGE_FILE_NAME(FileId, Uuid),
    SDHandle1 = SDHandle#sd_handle{file = NewName},
    
    ?debug("File ~p exists on storage, creating ~p instead", [FileId, NewName]),
    case storage_driver:create(SDHandle1, Mode) of
        ok ->
            {ok, NewName};
        Error ->
            Error
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Handles eexists error on storage.
%% @end
%%-------------------------------------------------------------------
-spec handle_eexists(boolean(), storage_driver:handle(),
    file_meta:posix_permissions(), file_ctx:ctx(),  user_ctx:ctx()) -> {ok, file_ctx:ctx()}.
handle_eexists(_VerifyDeletionLink, SDHandle, Mode, FileCtx, _UserCtx) ->
    {ok, StorageFileId} = create_storage_file_with_suffix(SDHandle, Mode),
    {ok, file_ctx:set_file_id(FileCtx, StorageFileId)}.
    % TODO VFS-5271 - handle conflicting directories
%%    case VerifyDeletionLink of
%%        false ->
%%            {ok, StorageFileId} = create_storage_file_with_suffix(SDHandle, Mode),
%%            {ok, file_ctx:set_file_id(FileCtx, StorageFileId)};
%%        _ ->
%%            {ParentCtx, FileCtx2} = file_ctx:get_parent(FileCtx, UserCtx),
%%            {FileName, FileCtx3} = file_ctx:get_aliased_name(FileCtx2, UserCtx),
%%            case location_and_link_utils:try_to_resolve_child_deletion_link(FileName, ParentCtx) of
%%                {error, not_found} ->
%%                    % Try once again to prevent races
%%                    storage_driver:create(SDHandle, Mode);
%%                {ok, _FileUuid} ->
%%                    {ok, StorageFileId} = create_storage_file_with_suffix(SDHandle, Mode),
%%                    {ok, file_ctx:set_file_id(FileCtx3, StorageFileId)}
%%            end
%%    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function is used to pretend that directory has been created
%% on S3 storage with storage_sync enabled. It is necessary, as S3 does
%% not create directories on storage and sync must be able to distinguish
%% remote directory that has never been synchronized with local directory.
%% @end
%%-------------------------------------------------------------------
-spec mark_parent_dirs_created_on_storage(file_ctx:ctx(), user_ctx:ctx()) -> ok.
mark_parent_dirs_created_on_storage(DirCtx, UserCtx) ->
    ParentCtxs = get_parent_dirs_not_created_on_storage(DirCtx, UserCtx, []),
    mark_parent_dirs_created_on_storage(ParentCtxs).

-spec get_parent_dirs_not_created_on_storage(file_ctx:ctx(), user_ctx:ctx(), [file_ctx:ctx()]) -> [file_ctx:ctx()].
get_parent_dirs_not_created_on_storage(DirCtx, UserCtx, ParentCtxs) ->
    case file_ctx:is_space_dir_const(DirCtx) of
        true ->
            ParentCtxs;
        false ->
            case file_ctx:get_dir_location_doc(DirCtx) of
                {DirLocation, DirCtx2} ->
                    case dir_location:is_storage_file_created(DirLocation) of
                        true ->
                            ParentCtxs;
                        false ->
                            {ParentCtx, DirCtx3} = file_ctx:get_parent(DirCtx2, UserCtx),
                            get_parent_dirs_not_created_on_storage(ParentCtx, UserCtx, [DirCtx3 | ParentCtxs])
                    end

            end
    end.

-spec mark_parent_dirs_created_on_storage([file_ctx:ctx()]) -> ok.
mark_parent_dirs_created_on_storage([]) ->
    ok;
mark_parent_dirs_created_on_storage([DirCtx | RestCtxs]) ->
    Uuid = file_ctx:get_uuid_const(DirCtx),
    SpaceId = file_ctx:get_space_id_const(DirCtx),
    dir_location:mark_dir_created_on_storage(Uuid, SpaceId),
    mark_parent_dirs_created_on_storage(RestCtxs).