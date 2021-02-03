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
%%%
%%% ATTENTION!!!
%%% Functions in this module should not operate on share guids and
%%% file contexts associated with share guids.
%%% @end
%%%--------------------------------------------------------------------
-module(sd_utils).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("proto/oneclient/common_messages.hrl").

-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([chmod/2, chmod/3, rename/7]).
-export([create_deferred/1, create_deferred/4, mkdir_deferred/2]).
-export([delete/2, unlink/2, rmdir/2]).


% Test API
-export([generic_create_deferred/3]).

-define(CLEANUP_MAX_RETRIES_NUM, 10).
-define(CLEANUP_DELAY, 5).

%%%===================================================================
%%% API
%%%===================================================================

-spec chmod(file_ctx:ctx(), file_meta:posix_permissions()) ->
    {ok, file_ctx:ctx()} | {error, Reason :: term()} | no_return().
chmod(FileCtx, Mode) ->
    chmod(user_ctx:new(?ROOT_SESS_ID), FileCtx, Mode).

%%--------------------------------------------------------------------
%% @doc
%% Change mode of storage files related with given file_meta.
%% @end
%%--------------------------------------------------------------------
-spec chmod(user_ctx:ctx(), file_ctx:ctx(),
    file_meta:posix_permissions()) -> {ok, file_ctx:ctx()} | {error, Reason :: term()} | no_return().
chmod(UserCtx, FileCtx, Mode) ->
    {IsReadonly, FileCtx2} = file_ctx:is_readonly_storage(FileCtx),
    case IsReadonly of
        true ->
            {ok, FileCtx2};
        false ->
            SessId = user_ctx:get_session_id(UserCtx),
            try
                case storage_driver:new_handle(SessId, FileCtx2, false) of
                    {undefined, FileCtx3} ->
                        {ok, FileCtx3};
                    {SDHandle, FileCtx3} ->
                        case storage_driver:chmod(SDHandle, Mode) of
                            ok -> {ok, FileCtx3};
                            {error, ?ENOENT} -> {ok, FileCtx3};
                            {error, ?EROFS} -> {error, ?EROFS}
                        end
                end
            catch
                throw:?ERROR_NOT_FOUND ->
                    {ok, FileCtx}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Renames file on storage.
%% @end
%%--------------------------------------------------------------------
-spec rename(user_ctx:ctx(), od_space:id(), storage:id(),
    file_meta:uuid(), helpers:file_id(), file_ctx:ctx() | undefined, helpers:file_id()) -> ok | {error, term()}.
rename(UserCtx, SpaceId, StorageId, FileUuid, SourceFileId, TargetParentCtx, TargetFileId) ->
    case TargetParentCtx =/= undefined of
        true ->
            TargetParentCtx2 = file_ctx:assert_not_readonly_storage(TargetParentCtx),
            TargetParentCtx3 = share_to_standard_file_ctx(TargetParentCtx2),
            % we know target parent uuid, so we can create parent directories with correct mode
            % ensure all target parent directories are created
            {ok, _} = mkdir_deferred(TargetParentCtx3, UserCtx);
        false ->
            % We don't know target parent uuid because it is a remote rename, check whether storage is readonly "manually"
            case storage:is_storage_readonly(StorageId, SpaceId) of
                true -> throw(?EROFS);
                false -> ok
            end,
            % Create parent directories with default mode
            TargetDir = filename:dirname(TargetFileId),
            TargetDirHandle = storage_driver:new_handle(?ROOT_SESS_ID, SpaceId, undefined, StorageId, TargetDir),
            case storage_driver:mkdir(TargetDirHandle, ?DEFAULT_DIR_PERMS, true) of
                ok -> ok;
                {error, ?EEXIST} -> ok
            end
    end,
    case SourceFileId =/= TargetFileId of
        true ->
            SessId = user_ctx:get_session_id(UserCtx),
            SourceHandle = storage_driver:new_handle(SessId, SpaceId, FileUuid, StorageId, SourceFileId),
            storage_driver:mv(SourceHandle, TargetFileId);
            % TODO VFS-5290 - solution results in problems with sed
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


-spec mkdir_deferred(file_ctx:ctx(), user_ctx:ctx()) -> {ok, file_ctx:ctx()}.
mkdir_deferred(FileCtx, UserCtx) ->
    case file_ctx:is_storage_file_created(FileCtx) of
        {false, FileCtx2} ->
            generic_create_deferred(UserCtx, FileCtx2, false);
        {true, FileCtx2} ->
            {ok, FileCtx2}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Create regular file on storage if it hasn't been created yet
%% (its creation has been deferred).
%% Creation is performed with root credentials.
%% @end
%%--------------------------------------------------------------------
-spec create_deferred(file_ctx:ctx()) -> {file_meta:doc(), file_ctx:ctx()} | {error, cancelled}.
create_deferred(FileCtx) ->
    create_deferred(FileCtx, user_ctx:new(?ROOT_SESS_ID), false, true).

%%--------------------------------------------------------------------
%% @doc
%% Create regular file on storage if it hasn't been created yet
%% (its creation has been deferred).
%% @end
%%--------------------------------------------------------------------
-spec create_deferred(file_ctx:ctx(), user_ctx:ctx(), boolean(), boolean()) ->
    {file_location:doc(), file_ctx:ctx()} | {error, cancelled}.
create_deferred(FileCtx, UserCtx, VerifyDeletionLink, CheckLocationExists) ->
    FileCtx2 = share_to_standard_file_ctx(FileCtx),
    {#document{
        key = FileLocationId,
        value = #file_location{storage_file_created = StorageFileCreated}
    }, FileCtx3} = Ans = file_ctx:get_or_create_local_regular_file_location_doc(FileCtx2, false, CheckLocationExists),

    case StorageFileCreated of
        false ->
            FileUuid = file_ctx:get_uuid_const(FileCtx3),
            % TODO VFS-5270
            replica_synchronizer:apply(FileCtx, fun() ->
                try
                    case fslogic_location:is_file_created(FileUuid, FileLocationId) of
                        true ->
                            Ans;
                        _ ->
                            {ok, FileCtx4} = sd_utils:generic_create_deferred(UserCtx, FileCtx3, VerifyDeletionLink),
                            {StorageFileId, FileCtx5} = file_ctx:get_storage_file_id(FileCtx4),
                            {ok, Doc} = fslogic_location:mark_file_created(FileUuid,
                                FileLocationId, StorageFileId),
                            {Doc, FileCtx5}
                    end
                catch
                    _:{badmatch,{error, not_found}} ->
                        {error, cancelled};
                    throw:Reason ->
                        {error, Reason}
                end
            end);
        true ->
            Ans
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates file (regular or directory !!!) on storage.
%% @end
%%--------------------------------------------------------------------
-spec generic_create_deferred(user_ctx:ctx(), file_ctx:ctx(), boolean()) -> {ok, file_ctx:ctx()}.
generic_create_deferred(UserCtx, FileCtx, VerifyDeletionLink) ->
    {ShouldChown, FileCtx2} = should_chown(UserCtx, FileCtx),
    SessId = case ShouldChown of
        true -> ?ROOT_SESS_ID;
        false -> user_ctx:get_session_id(UserCtx)
    end,
    {SDHandle, FileCtx3} = storage_driver:new_handle(SessId, FileCtx2),
    FinalResult = case create_storage_file(SDHandle, FileCtx3) of
        {error, ?ENOENT} ->
            FileCtx4 = create_missing_parent_dirs(UserCtx, FileCtx3),
            create_storage_file(SDHandle, FileCtx4);
        {error, ?EEXIST} ->
            handle_eexists(VerifyDeletionLink, UserCtx, SDHandle, FileCtx3);
         {error, ?EACCES} ->
            % eacces is possible because there is race condition
            % on creating and chowning parent dir
            % for this reason it is acceptable to try chowning parent once
             % TODO VFS-6432 in case of changing default credentials in LUMA we should not chown parent dir
            {ParentCtx, FileCtx4} = file_ctx:get_parent(FileCtx3, UserCtx),
             case file_ctx:is_root_dir_const(ParentCtx) of
                 true -> ok;
                 false -> files_to_chown:chown_or_defer(ParentCtx)
             end,
            create_storage_file(SDHandle, FileCtx4);
        {ok, FileCtx4} ->
            {Storage, FileCtx5} = file_ctx:get_storage(FileCtx4),
            Helper = storage:get_helper(Storage),
            HelperName = helper:get_name(Helper),
            case HelperName =:= ?S3_HELPER_NAME andalso helper:is_auto_import_supported(Helper) of
                true ->
                    % pretend that parent directories has been created
                    % this should only happen on synced S3 storage
                    {ParentCtx, FileCtx6} = file_ctx:get_parent(FileCtx5, UserCtx),
                    mark_parent_dirs_created_on_storage(ParentCtx, UserCtx),
                    {ok, FileCtx6};
                false ->
                    {ok, FileCtx5}
            end
    end,

    case FinalResult of
        {ok, FinalCtx}  ->
            case ShouldChown of
                true ->
                    {ok, files_to_chown:chown_or_defer(FinalCtx)};
                _ ->
                    {ok, FinalCtx}
            end;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes given file from storage.
%% @end
%%--------------------------------------------------------------------
-spec delete(file_ctx:ctx(), user_ctx:ctx()) -> {ok, file_ctx:ctx()} | {error, term()}.
delete(FileCtx, UserCtx) ->
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
    case IsDir of
        true -> rmdir(FileCtx2, UserCtx);
        false -> unlink(FileCtx2, UserCtx)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Removes regular file from storage.
%% @end
%%--------------------------------------------------------------------
-spec unlink(file_ctx:ctx(), user_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | {error, term()}.
unlink(FileCtx, UserCtx) ->
    FileCtx2 = share_to_standard_file_ctx(FileCtx),
    SessId = user_ctx:get_session_id(UserCtx),
    case storage_driver:new_handle(SessId, FileCtx2, false) of
        {undefined, _FileCtx3} ->
            {error, ?ENOENT};
        {SDHandle, FileCtx3} ->
            {Size, FileCtx4} = file_ctx:get_file_size(FileCtx3),
            case storage_driver:unlink(SDHandle, Size) of
                ok ->
                    {ok, FileCtx4};
                {error, _} = Error ->
                    Error
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes directory from storage.
%% @end
%%--------------------------------------------------------------------
-spec rmdir(file_ctx:ctx(), user_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | {error, term()}.
rmdir(DirCtx, UserCtx) ->
    DirCtx2 = share_to_standard_file_ctx(DirCtx),
    SessId = user_ctx:get_session_id(UserCtx),
    FileUuid = file_ctx:get_uuid_const(DirCtx2),
    case storage_driver:new_handle(SessId, DirCtx2, false) of
        {undefined, DirCtx3} ->
            {ok, DirCtx3};
        {SDHandle, DirCtx3} ->
            case storage_driver:rmdir(SDHandle) of
                ok ->
                    dir_location:mark_deleted_from_storage(FileUuid),
                    {ok, DirCtx3};
                {error, ?ENOENT} ->
                    dir_location:mark_deleted_from_storage(FileUuid),
                    {ok, DirCtx3};
                {error, ?ENOTEMPTY} = Error ->
                    ?debug("sd_utils:rmdir failed with ~p", [Error]),
                    Error;
                {error,'Function not implemented'} = Error ->
                    % Some helpers do not support rmdir
                    ?debug("sd_utils:rmdir failed with ~p", [Error]),
                    {ok, DirCtx3};
                Error ->
                    ?error("sd_utils:rmdir ~p ~p failed with ~p", [DirCtx, SDHandle, Error]),
                    Error
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec share_to_standard_file_ctx(file_ctx:ctx()) -> file_ctx:ctx().
share_to_standard_file_ctx(FileCtx) ->
    Guid = file_ctx:get_guid_const(FileCtx),
    case file_id:is_share_guid(Guid) of
        true ->
            Guid2 = file_id:share_guid_to_guid(Guid),
            file_ctx:new_by_guid(Guid2);
        false ->
            FileCtx
    end.


-spec create_storage_file(storage_driver:handle(), file_ctx:ctx()) -> {ok, file_ctx:ctx()} | {error, term()}.
create_storage_file(SDHandle, FileCtx) ->
    FileCtx2 = file_ctx:assert_not_readonly_storage(FileCtx),
    {FileDoc, FileCtx3} = file_ctx:get_file_doc(FileCtx2),
    Mode = file_meta:get_mode(FileDoc),
    case file_meta:get_type(FileDoc) of
        ?REGULAR_FILE_TYPE ->
            case storage_driver:create(SDHandle, Mode) of
                ok ->
                    truncate_created_file(FileCtx);
                Other ->
                    Other
            end;
        ?DIRECTORY_TYPE ->
            case storage_driver:mkdir(SDHandle, Mode) of
                ok -> {ok, FileCtx3};
                Error -> Error
            end
    end.

-spec truncate_created_file(file_ctx:ctx()) -> {ok, file_ctx:ctx()}.
truncate_created_file(FileCtx) ->
    try
        case file_ctx:get_file_size(FileCtx) of
            {0, FileCtx2} ->
                {ok, FileCtx2};
            {Size, FileCtx2} ->
                {SDHandle, FileCtx3} = storage_driver:new_handle(?ROOT_SESS_ID, FileCtx2),
                {ok, Handle} = storage_driver:open(SDHandle, write),
                ok = storage_driver:truncate(Handle, Size, 0),
                storage_driver:release(Handle),
                {ok, FileCtx3}
        end
    catch
        Error:Reason ->
            ?warning_stacktrace("Error truncating newly created storage file ~p: ~p:~p",
                [file_ctx:get_guid_const(FileCtx), Error, Reason]),
            {ok, FileCtx}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates missing parent directories on storage
%% @end
%%--------------------------------------------------------------------
-spec create_missing_parent_dirs(user_ctx:ctx(), file_ctx:ctx()) -> file_ctx:ctx().
create_missing_parent_dirs(UserCtx, FileCtx) ->
    case file_ctx:is_root_dir_const(FileCtx) of
        true ->
            FileCtx;
        false ->
            {ParentCtx, FileCtx2} = file_ctx:get_parent(FileCtx, undefined),
            create_missing_parent_dirs(UserCtx, ParentCtx, []),
            FileCtx2
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Tail recursive helper function of ?MODULE:create_missing_parent_dirs/1
%% @end
%%-------------------------------------------------------------------
-spec create_missing_parent_dirs(user_ctx:ctx(), file_ctx:ctx(), [file_ctx:ctx()]) -> ok.
create_missing_parent_dirs(UserCtx, FileCtx, ParentCtxsToCreate) ->
    IsSpaceDir = file_ctx:is_space_dir_const(FileCtx),
    {IsStorageFileCreated, FileCtx2} = file_ctx:is_storage_file_created(FileCtx),
    case IsStorageFileCreated or IsSpaceDir of
        true ->
            ParentCtxsToCreate2 = case IsStorageFileCreated of
                true -> ParentCtxsToCreate;
                false -> [FileCtx2 | ParentCtxsToCreate]
            end,
            lists:foreach(fun(Ctx) ->
                % create missing directories going down the file tree
                case create_missing_parent_dir(UserCtx, Ctx) of
                    {ok, _} -> ok;
                    {error, Reason} -> throw(Reason)
                end
            end, ParentCtxsToCreate2);
        false ->
            {ParentCtx, FileCtx3} = file_ctx:get_parent(FileCtx2, undefined),
            % Infinite loop possible if function is executed on space dir - this case stops such loop
            case file_ctx:get_uuid_const(FileCtx) =:= file_ctx:get_uuid_const(ParentCtx) of
                true ->
                    {Doc, _} = file_ctx:get_file_doc(FileCtx2),
                    ?info("Infinite loop detected on parent dirs creation for file ~p", [Doc]),
                    lists:foreach(fun(Ctx) ->
                        % create missing directories going down the file tree
                        case create_missing_parent_dir(UserCtx, Ctx) of
                            {ok, _} -> ok;
                            {error, Reason} -> throw(Reason)
                        end
                    end, [FileCtx | ParentCtxsToCreate]);
                false ->
                    create_missing_parent_dirs(UserCtx, ParentCtx, [FileCtx3 | ParentCtxsToCreate])
            end
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Creates directory on storage with suitable mode and owner.
%% @end
%%-------------------------------------------------------------------
-spec create_missing_parent_dir(user_ctx:ctx(), file_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | {error, term()}.
create_missing_parent_dir(UserCtx, FileCtx) ->
    {FileDoc, FileCtx3} = file_ctx:get_file_doc(FileCtx),
    mkdir_and_maybe_chown(UserCtx, FileCtx3, file_meta:get_mode(FileDoc)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Creates directory on storage and chowns it if ShouldChown flag is set to
%% true.
%% @end
%%-------------------------------------------------------------------
-spec mkdir_and_maybe_chown(user_ctx:ctx(), file_ctx:ctx(), file_meta:mode()) ->
    {ok, file_ctx:ctx()} | {error, term()}.
mkdir_and_maybe_chown(UserCtx, FileCtx, Mode) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {ShouldChown, FileCtx2} = should_chown(UserCtx, FileCtx),
    SessId = case ShouldChown of
        true -> ?ROOT_SESS_ID;
        false -> user_ctx:get_session_id(UserCtx)
    end,
    {SDHandle, FileCtx3} = storage_driver:new_handle(SessId, FileCtx2),
    Result = case storage_driver:mkdir(SDHandle, Mode, false) of
        ok ->
            FileCtx4 = maybe_set_guid_in_storage_sync_info(FileCtx3),
            {StorageFileId, FileCtx5} = file_ctx:get_storage_file_id(FileCtx4),
            case dir_location:mark_dir_created_on_storage(FileUuid, StorageFileId) of
                ok -> {ok, FileCtx5};
                % helpers on ceph and s3 always return ok on mkdir operation
                % so we have to handle situation when doc is already in db
                {error, already_exists} -> {ok, FileCtx5}
            end;
        {error, ?EEXIST} ->
            {ok, FileCtx3};
        OtherError ->
            OtherError
    end,

    case {Result, ShouldChown} of
        {{ok, FinalCtx}, true} ->
            {ok, files_to_chown:chown_or_defer(FinalCtx)};
        {{ok, FinalCtx}, false} ->
            {ok, FinalCtx};
        {Error, _} ->
            Error
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Handles eexists error on storage.
%% @end
%%-------------------------------------------------------------------
-spec handle_eexists(boolean(), user_ctx:ctx(), storage_driver:handle(),
    file_ctx:ctx()) -> {ok, file_ctx:ctx()}.
handle_eexists(VerifyDeletionLink, UserCtx, SDHandle, FileCtx) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    case file_meta:get_type(FileDoc) of
        ?REGULAR_FILE_TYPE -> handle_conflicting_file(VerifyDeletionLink, UserCtx, SDHandle, FileCtx);
        ?DIRECTORY_TYPE -> handle_conflicting_directory(FileCtx2)
    end.

-spec handle_conflicting_file(boolean(), user_ctx:ctx(), storage_driver:handle(), file_ctx:ctx()) ->
    {ok, file_ctx:ctx()}.
handle_conflicting_file(_VerifyDeletionLink, _UserCtx, SDHandle, FileCtx) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, StorageFileId} = create_storage_file_with_suffix(SDHandle, file_meta:get_mode(FileDoc)),
    {ok, file_ctx:set_file_id(FileCtx2, StorageFileId)}.


-spec handle_conflicting_directory(file_ctx:ctx()) -> {ok, file_ctx:ctx()}.
handle_conflicting_directory(FileCtx) ->
    % TODO VFS-5271 - handle conflicting directories
    %%    case VerifyDeletionLink of
    %%        false ->
    %%            {ok, StorageFileId} = create_storage_file_with_suffix(SDHandle, Mode),
    %%            {ok, file_ctx:set_file_id(FileCtx, StorageFileId)};
    %%        _ ->
    %%            {ParentCtx, FileCtx2} = file_ctx:get_parent(FileCtx, UserCtx),
    %%            {FileName, FileCtx3} = file_ctx:get_aliased_name(FileCtx2, UserCtx),
    %%            case link_utils:try_to_resolve_child_deletion_link(FileName, ParentCtx) of
    %%                {error, not_found} ->
    %%                    % Try once again to prevent races
    %%                    storage_driver:create(SDHandle, Mode);
    %%                {ok, _FileUuid} ->
    %%                    {ok, StorageFileId} = create_storage_file_with_suffwix(SDHandle, Mode),
    %%                    {ok, file_ctx:set_file_id(FileCtx3, StorageFileId)}
    %%            end
    %%    end.
    {ok, FileCtx}.

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
        ok -> {ok, NewName};
        Error -> Error
    end.

-spec should_chown(user_ctx:ctx(), file_ctx:ctx()) ->
    {boolean(), file_ctx:ctx()}.
should_chown(UserCtx, FileCtx) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    UserId = user_ctx:get_user_id(UserCtx),
    OwnerUserId = file_meta:get_owner(FileDoc),
    {StorageDoc, FileCtx3} = file_ctx:get_storage(FileCtx2),
    % file owner on storage should be changed if:
    %  * storage helper supports chown operation (is posix compatible) and
    %  * UserCtx is not associated with OwnerUserId
    IsOwner = UserId =:= OwnerUserId,
    ShouldChown = storage:is_posix_compatible(StorageDoc) andalso not IsOwner,
    {ShouldChown, FileCtx3}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function is used to pretend that directory has been created
%% on S3 storage with storage import enabled. It is necessary, as S3 does
%% not create directories on storage and sync must be able to distinguish
%% remote directory that has never been synchronized with local directory.
%% @end
%%-------------------------------------------------------------------
-spec mark_parent_dirs_created_on_storage(file_ctx:ctx(), user_ctx:ctx()) -> ok.
mark_parent_dirs_created_on_storage(DirCtx, UserCtx) ->
    ParentCtxs = get_parent_dirs_not_created_on_storage(DirCtx, UserCtx, []),
    {IsImported, DirCtx2} = file_ctx:is_imported_storage(DirCtx),
    {StorageId, _DirCtx3} = file_ctx:get_storage_id(DirCtx2),
    mark_parent_dirs_created_on_storage(ParentCtxs, StorageId, IsImported).

-spec get_parent_dirs_not_created_on_storage(file_ctx:ctx(), user_ctx:ctx(), [file_ctx:ctx()]) -> [file_ctx:ctx()].
get_parent_dirs_not_created_on_storage(DirCtx, UserCtx, ParentCtxs) ->
    case file_ctx:is_space_dir_const(DirCtx) of
        true ->
            ParentCtxs;
        false ->
            case file_ctx:is_storage_file_created(DirCtx) of
                {true, _DirCtx2} ->
                    ParentCtxs;
                {false, DirCtx2} ->
                    {ParentCtx, DirCtx3} = file_ctx:get_parent(DirCtx2, UserCtx),
                    get_parent_dirs_not_created_on_storage(ParentCtx, UserCtx, [DirCtx3 | ParentCtxs])
            end
    end.

-spec mark_parent_dirs_created_on_storage([file_ctx:ctx()], storage:id(), IsImportedStorage :: boolean()) -> ok.
mark_parent_dirs_created_on_storage([], _StorageId, _IsImportedStorage) ->
    ok;
mark_parent_dirs_created_on_storage([DirCtx | RestCtxs], StorageId, IsImportedStorage) ->
    Uuid = file_ctx:get_uuid_const(DirCtx),
    {StorageFileId, DirCtx2} = file_ctx:get_storage_file_id(DirCtx),
    case IsImportedStorage of
        true ->
            SpaceId = file_ctx:get_space_id_const(DirCtx2),
            FileGuid = file_ctx:get_guid_const(DirCtx2),
            storage_sync_info:maybe_set_guid(StorageFileId, SpaceId, StorageId, FileGuid);
        false ->
            ok
    end,
    dir_location:mark_dir_created_on_storage(Uuid, StorageFileId),
    mark_parent_dirs_created_on_storage(RestCtxs, StorageId, IsImportedStorage).


-spec maybe_set_guid_in_storage_sync_info(file_ctx:ctx()) -> file_ctx:ctx().
maybe_set_guid_in_storage_sync_info(FileCtx) ->
    {StorageId, FileCtx2} = file_ctx:get_storage_id(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx2),
    FileGuid = file_ctx:get_space_id_const(FileCtx2),
    {StorageFileId, FileCtx3} = file_ctx:get_storage_file_id(FileCtx2),
    storage_sync_info:maybe_set_guid(StorageFileId, SpaceId, StorageId, FileGuid),
    FileCtx3.