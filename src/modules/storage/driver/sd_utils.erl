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
-include("modules/fslogic/fslogic_suffix.hrl").
-include("proto/oneclient/common_messages.hrl").

-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([chmod_storage_file/3, rename_storage_file/7]).
-export([create_delayed_regular_file/1, create_delayed_regular_file/4, mkdir_delayed/2]).
-export([delete/2, delete_storage_file/2, delete_storage_dir/2]).


% Test API
-export([create_delayed_file/3]).

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
    case storage_driver:new_handle(SessId, FileCtx, false) of
        {undefined, _} ->
            ok;
        {SDHandle, _} ->
            case storage_driver:chmod(SDHandle, Mode) of
                ok -> ok;
                {error, ?ENOENT} -> ok;
                {error, ?EROFS} -> {error, ?EROFS}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Renames file on storage.
%% @end
%%--------------------------------------------------------------------
-spec rename_storage_file(user_ctx:ctx(), od_space:id(), storage:id(),
    file_meta:uuid(), helpers:file_id(), file_ctx:ctx() | undefined, helpers:file_id()) -> ok | {error, term()}.
rename_storage_file(UserCtx, SpaceId, StorageId, FileUuid, SourceFileId, TargetParentCtx, TargetFileId) ->
    case TargetParentCtx =/= undefined of
        true ->
            % we know target parent uuid, so we can create parent directories with correct mode
            % ensure all target parent directories are created
            {ok, _} = mkdir_delayed(TargetParentCtx, UserCtx);
        false ->
            % we don't know target parent uuid because it is a remote rename
            % create parent directories with default mode
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


-spec mkdir_delayed(file_ctx:ctx(), user_ctx:ctx()) -> {ok, file_ctx:ctx()}.
mkdir_delayed(FileCtx, UserCtx) ->
    case file_ctx:is_storage_file_created(FileCtx) of
        {false, FileCtx2} ->
            create_delayed_file(UserCtx, FileCtx2, false);
        {true, FileCtx2} ->
            {ok, FileCtx2}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Create storage file if it hasn't been created yet (it has been delayed).
%% Creation is performed with root credentials.
%% @end
%%--------------------------------------------------------------------
-spec create_delayed_regular_file(file_ctx:ctx()) -> {file_meta:doc(), file_ctx:ctx()} | {error, cancelled}.
create_delayed_regular_file(FileCtx) ->
    create_delayed_regular_file(FileCtx, user_ctx:new(?ROOT_SESS_ID), false, true).

%%--------------------------------------------------------------------
%% @doc
%% Create storage file if it hasn't been created yet (it has been delayed)
%% @end
%%--------------------------------------------------------------------
-spec create_delayed_regular_file(file_ctx:ctx(), user_ctx:ctx(), boolean(), boolean()) ->
    {file_location:doc(), file_ctx:ctx()} | {error, cancelled}.
create_delayed_regular_file(FileCtx, UserCtx, VerifyDeletionLink, CheckLocationExists) ->
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
                            {ok, FileCtx3} = sd_utils:create_delayed_file(UserCtx, FileCtx2, VerifyDeletionLink),
                            {StorageFileId, FileCtx4} = file_ctx:get_storage_file_id(FileCtx3),
                            {ok, Doc} = location_and_link_utils:mark_location_created(FileUuid,
                                FileLocationId, StorageFileId),
                            {Doc, FileCtx4}
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
%% Create file (regular or directory !!!) on storage.
%% @end
%%--------------------------------------------------------------------
-spec create_delayed_file(user_ctx:ctx(), file_ctx:ctx(), boolean()) -> {ok, file_ctx:ctx()}.
create_delayed_file(UserCtx, FileCtx, VerifyDeletionLink) ->
    {ShouldChown, SDHandle, FileCtx2} = check_if_should_schedule_chown_and_get_sd_handle(UserCtx, FileCtx),
    {#document{value = #file_meta{
        mode = Mode,
        type = FileType
    }}, FileCtx3} = file_ctx:get_file_doc(FileCtx2),
    FinalResult = case create_storage_file_appropriate_to_type(SDHandle, Mode, FileType) of
        {error, ?ENOENT} ->
            FileCtx4 = create_missing_parent_dirs(UserCtx, FileCtx3),
            {create_storage_file_appropriate_to_type(SDHandle, Mode, FileType), FileCtx4};
        {error, ?EEXIST} ->
            handle_eexists(VerifyDeletionLink, SDHandle, Mode, FileCtx3, UserCtx, FileType);
         {error, ?EACCES} ->
            % eacces is possible because there is race condition
            % on creating and chowning parent dir
            % for this reason it is acceptable to try chowning parent once
            {ParentCtx, FileCtx4} = file_ctx:get_parent(FileCtx3, UserCtx),
             case file_ctx:is_root_dir_const(ParentCtx) of
                 true -> ok;
                 false -> files_to_chown:chown_or_schedule_chowning(ParentCtx)
             end,
            {create_storage_file_appropriate_to_type(SDHandle, Mode, FileType), FileCtx4};
        ok ->
            {Storage, FileCtx4} = file_ctx:get_storage(FileCtx3),
            Helper = storage:get_helper(Storage),
            HelperName = helper:get_name(Helper),
            FileCtx6 = case HelperName =:= ?S3_HELPER_NAME andalso helper:is_sync_supported_on(Helper) of
                true ->
                    % pretend that parent directories has been created
                    % this should only happen on synced S3 storage
                    {ParentCtx, FileCtx5} = file_ctx:get_parent(FileCtx4, UserCtx),
                    mark_parent_dirs_created_on_storage(ParentCtx, UserCtx),
                    FileCtx5;
                false ->
                    FileCtx4
            end,
            {ok, FileCtx6}
    end,

    case FinalResult of
        {ok, FinalCtx}  ->
            case ShouldChown of
                true ->
                    {ok, files_to_chown:chown_or_schedule_chowning(FinalCtx)};
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
        true -> delete_storage_dir(FileCtx2, UserCtx);
        false -> delete_storage_file(FileCtx2, UserCtx)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Removes file from storage.
%% @end
%%--------------------------------------------------------------------
-spec delete_storage_file(file_ctx:ctx(), user_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | {error, term()}.
delete_storage_file(FileCtx, UserCtx) ->
    SessId = user_ctx:get_session_id(UserCtx),
    case storage_driver:new_handle(SessId, FileCtx, false) of
        {undefined, _FileCtx2} ->
            {error, ?ENOENT};
        {SDHandle, FileCtx2} ->
            {Size, FileCtx3} = file_ctx:get_file_size(FileCtx2),
            storage_driver:unlink(SDHandle, Size),
            {ok, FileCtx3}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes directory from storage.
%% @end
%%--------------------------------------------------------------------
-spec delete_storage_dir(file_ctx:ctx(), user_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | {error, term()}.
delete_storage_dir(DirCtx, UserCtx) ->
    SessId = user_ctx:get_session_id(UserCtx),
    FileUuid = file_ctx:get_uuid_const(DirCtx),
    case storage_driver:new_handle(SessId, DirCtx, false) of
        {undefined, FileCtx2} ->
            {ok, FileCtx2};
        {SDHandle, FileCtx2} ->
            case storage_driver:rmdir(SDHandle) of
                ok ->
                    dir_location:delete(FileUuid),
                    {ok, FileCtx2};
                {error, ?ENOENT} ->
                    dir_location:delete(FileUuid),
                    {ok, FileCtx2};
                {error, ?ENOTEMPTY} = Error ->
                    ?debug("sd_utils:delete_storage_dir failed with ~p", [Error]),
                    Error;
                {error,'Function not implemented'} = Error ->
                    % Some helpers do not support rmdir
                    ?debug("sd_utils:delete_storage_dir failed with ~p", [Error]),
                    {ok, FileCtx2};
                Error ->
                    ?error("sd_utils:delete_storage_dir ~p ~p failed with ~p", [DirCtx, SDHandle, Error]),
                    Error
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create_storage_file_appropriate_to_type(storage_driver:handle(), file_meta:mode(),
    file_meta:type()) -> ok | {error, term()}.
create_storage_file_appropriate_to_type(SFMHandle, Mode, ?REGULAR_FILE_TYPE) ->
    storage_driver:create(SFMHandle, Mode);
create_storage_file_appropriate_to_type(SFMHandle, Mode, ?DIRECTORY_TYPE) ->
    storage_driver:mkdir(SFMHandle, Mode).

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
            create_missing_parent_dirs(UserCtx, ParentCtx, [FileCtx3 | ParentCtxsToCreate])
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
%%    {FileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
%%    try
        {#document{value = #file_meta{mode = Mode}}, FileCtx3} = file_ctx:get_file_doc(FileCtx),
        R = mkdir_and_maybe_chown(UserCtx, FileCtx3, Mode),

        R.
%%    catch
%%        Error:Reason ->
%%            % TODO jk is this case really needed ??
%%            ?error_stacktrace(
%%                "Creating parent dir ~p failed due to ~p.~n"
%%                "Parent dir will be create with default mode.", [FileId, {Error, Reason}]),
%%            mkdir_and_maybe_chown(UserCtx, FileCtx, ?DEFAULT_DIR_MODE)
%%    end.

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
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {ShouldChown, SDHandle, FileCtx2} = check_if_should_schedule_chown_and_get_sd_handle(UserCtx, FileCtx),
    Result = case storage_driver:mkdir(SDHandle, Mode, false) of
        ok ->
            case dir_location:mark_dir_created_on_storage(FileUuid, SpaceId) of
                {ok, _} -> ok;
                % helpers on ceph and s3 always return ok on mkdir operation
                % so we have to handle situation when doc is already in db
                {error, already_exists} -> ok
            end;
        {error, ?EEXIST} ->
            ok;
        OtherError ->
            OtherError
    end,
    case {Result, ShouldChown} of
        {ok, true} ->
            {ok, files_to_chown:chown_or_schedule_chowning(FileCtx2)};
        {ok, false} ->
            {ok, FileCtx};
        {Error, _} ->
            Error
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Handles eexists error on storage.
%% @end
%%-------------------------------------------------------------------
-spec handle_eexists(boolean(), storage_driver:handle(), file_meta:posix_permissions(),
    file_ctx:ctx(),  user_ctx:ctx(), file_meta:type()) -> {ok, file_ctx:ctx()}.
handle_eexists(_VerifyDeletionLink, SDHandle, Mode, FileCtx, _UserCtx, ?REGULAR_FILE_TYPE) ->
    {ok, StorageFileId} = create_storage_file_with_suffix(SDHandle, Mode),
    {ok, file_ctx:set_file_id(FileCtx, StorageFileId)};
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
handle_eexists(_VerifyDeletionLink, _SFMHandle, _Mode, FileCtx, _UserCtx, _) ->
    % TODO VFS-5271 - handle conflicting directories
    {ok, FileCtx}.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Creates file on storage with uuid as suffix
%% @end
%%-------------------------------------------------------------------
-spec create_storage_file_with_suffix(storage_driver:handle(),
    file_meta:posix_permissions()) -> {ok, helpers:file_id()}.
create_storage_file_with_suffix(#sd_handle{file_uuid = Uuid, file = FileId} = SFMHandle, Mode) ->
    NewName = ?CONFLICTING_STORAGE_FILE_NAME(FileId, Uuid),
    SFMHandle1 = SFMHandle#sd_handle{file = NewName},
    ?debug("File ~p exists on storage, creating ~p instead", [FileId, NewName]),
    case storage_driver:create(SFMHandle1, Mode) of
        ok -> {ok, NewName};
        Error -> Error
    end.

% TODO ADD DOC
-spec check_if_should_schedule_chown_and_get_sd_handle(user_ctx:ctx(), file_ctx:ctx()) ->
    {boolean(), storage_driver:handle(), file_ctx:ctx()}.
check_if_should_schedule_chown_and_get_sd_handle(UserCtx, FileCtx) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    UserId = user_ctx:get_user_id(UserCtx),
    OwnerUserId = file_meta:get_owner(FileDoc),
    {IsOwner, SessId} = case UserId =:= OwnerUserId  of
        true ->
            % file does belong to user associated with UserCtx
            % there is now need to try change its owner as it will be created
            % with appropriate one
            {true, user_ctx:get_session_id(UserCtx)};
        _ ->
            % file does not belong to user associated with UserCtx
            % create it as ROOT and schedule chown
            {false, ?ROOT_SESS_ID}
    end,
    {StorageDoc, FileCtx3} = file_ctx:get_storage(FileCtx2),
    % file should be chowned if:
    %  * storage helper supports chown operation (is posix compatible) and
    %  * UserCtx is not associated with OwnerUserId
    ShouldChown = storage:is_posix_compatible(StorageDoc) andalso not IsOwner,
    {SDHandle, FileCtx4} = storage_driver:new_handle(SessId, FileCtx3),
    {ShouldChown, SDHandle, FileCtx4}.

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