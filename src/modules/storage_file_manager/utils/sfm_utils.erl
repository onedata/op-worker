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
-module(sfm_utils).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("proto/oneclient/common_messages.hrl").

-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([chmod_storage_file/3, rename_storage_file/6,
    create_delayed_storage_file/1, create_delayed_storage_file/4,
    delete_storage_file/2, create_parent_dirs/1, delete/2]).

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
            case storage_file_manager:new_handle(SessId, FileCtx2, false) of
                {undefined, _} ->
                    ok;
                {SFMHandle, _} ->
                    case storage_file_manager:chmod(SFMHandle, Mode) of
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
-spec rename_storage_file(session:id(), od_space:id(), storage:doc(),
    file_meta:uuid(), helpers:file_id(), helpers:file_id()) -> ok | {error, term()}.
rename_storage_file(SessId, SpaceId, Storage, FileUuid, SourceFileId, TargetFileId) ->
    %create target dir
    TargetDir = filename:dirname(TargetFileId),
    TargetDirHandle = storage_file_manager:new_handle(?ROOT_SESS_ID,
        SpaceId, undefined, Storage, TargetDir, undefined),
    case storage_file_manager:mkdir(TargetDirHandle,
        ?AUTO_CREATED_PARENT_DIR_MODE, true)
    of
        ok ->
            ok;
        {error, ?EEXIST} ->
            ok
    end,

    case SourceFileId =/= TargetFileId of
        true ->
            SourceHandle = storage_file_manager:new_handle(SessId, SpaceId,
                FileUuid, Storage, SourceFileId, undefined),
            storage_file_manager:mv(SourceHandle, TargetFileId);
            % TODO VFS-5290 - solution resutls in problems with sed
%%            TargetHandle = storage_file_manager:new_handle(SessId, SpaceId,
%%                FileUuid, Storage, TargetFileId, undefined),
%%
%%            case storage_file_manager:stat(TargetHandle) of
%%                {ok, _} ->
%%                    ?warning("Moving file into existing one, source ~p, target ~p",
%%                        [SourceFileId, TargetFileId]),
%%                    NewTargetFileId = ?CONFLICTING_STORAGE_FILE_NAME(TargetFileId, FileUuid),
%%                    storage_file_manager:mv(SourceHandle, NewTargetFileId);
%%                _ ->
%%                    storage_file_manager:mv(SourceHandle, TargetFileId)
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

    {SFMHandle, FileCtx3} = storage_file_manager:new_handle(SessId, FileCtx2),
    {ok, FinalCtx} = case storage_file_manager:create(SFMHandle, Mode) of
        {error, ?ENOENT} ->
            FileCtx4 = create_parent_dirs(FileCtx3),
            {storage_file_manager:create(SFMHandle, Mode), FileCtx4};
        {error, ?EEXIST} ->
            handle_eexists(VerifyDeletionLink, SFMHandle, Mode, FileCtx3, UserCtx);
         {error, ?EACCES} ->
            % eacces is possible because there is race condition
            % on creating and chowning parent dir
            % for this reason it is acceptable to try chowning parent once
            {ParentCtx, FileCtx4} = file_ctx:get_parent(FileCtx3, UserCtx),
            files_to_chown:chown_or_schedule_chowning(ParentCtx),
            {storage_file_manager:create(SFMHandle, Mode), FileCtx4};
        Other ->
            {Other, FileCtx3}
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
    {ok, file_ctx:ctx()} | {error, term()}.
delete_storage_file(FileCtx, UserCtx) ->
    SessId = user_ctx:get_session_id(UserCtx),
    case storage_file_manager:new_handle(SessId, FileCtx, false) of
        {undefined, _FileCtx2} ->
            {error, ?ENOENT};
        {SFMHandle, FileCtx2} ->
            {Size, FileCtx3} = file_ctx:get_file_size(FileCtx2),
            storage_file_manager:unlink(SFMHandle, Size),
            {ok, FileCtx3}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes given file. If it's a directory all its children will be
%% deleted to.
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
%% Removes directory from storage.
%% @end
%%--------------------------------------------------------------------
-spec delete_storage_dir(file_ctx:ctx(), user_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | {error, term()}.
delete_storage_dir(DirCtx, UserCtx) ->
    SessId = user_ctx:get_session_id(UserCtx),
    FileUuid = file_ctx:get_uuid_const(DirCtx),
    case storage_file_manager:new_handle(SessId, DirCtx, false) of
        {undefined, FileCtx2} ->
            {ok, FileCtx2};
        {SFMHandle, FileCtx2} ->
            case storage_file_manager:rmdir(SFMHandle) of
                ok ->
                    dir_location:delete(FileUuid),
                    {ok, FileCtx2};
                {error, ?ENOENT} ->
                    dir_location:delete(FileUuid),
                    {ok, FileCtx2};
                {error, ?ENOTEMPTY} = Error ->
                    ?debug("sfm_utils:delete_storage_dir failed with ~p", [Error]),
                    Error;
                {error,'Function not implemented'} = Error ->
                    % Some helpers do not support rmdir
                    ?debug("sfm_utils:delete_storage_dir failed with ~p", [Error]),
                    {ok, FileCtx2};
                Error ->
                    ?error("sfm_utils:delete_storage_dir failed with ~p", [Error]),
                    Error
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates parent dir on storage
%% @end
%%--------------------------------------------------------------------
-spec create_parent_dirs(file_ctx:ctx()) -> file_ctx:ctx().
create_parent_dirs(FileCtx) ->
    case file_ctx:is_root_dir_const(FileCtx) of
        true ->
            FileCtx;
        false ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            {Storage, FileCtx3} = file_ctx:get_storage_doc(FileCtx),
            {ParentCtx, FileCtx4} = file_ctx:get_parent(FileCtx3, undefined),
            create_parent_dirs(ParentCtx, [], SpaceId, Storage),
            FileCtx4
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Tail recursive helper function of ?MODULE:create_parent_dirs/1
%% @end
%%-------------------------------------------------------------------
-spec create_parent_dirs(file_ctx:ctx(), [file_ctx:ctx()], od_space:id(),
    storage:doc()) -> ok.
create_parent_dirs(FileCtx, ChildrenDirCtxs, SpaceId, Storage) ->
    case file_ctx:is_space_dir_const(FileCtx) of
        true ->
            lists:foreach(fun(Ctx) ->
                create_dir(Ctx, SpaceId, Storage)
            end, [FileCtx | ChildrenDirCtxs]);
        false ->
            %TODO VFS-4297 stop recursion when parent file exists on storage,
            %TODO currently recursion stops in space dir
            {ParentCtx, FileCtx2} = file_ctx:get_parent(FileCtx, undefined),
            % Infinite loop possible if function is executed on space dir - this case stops such loop
            case file_ctx:get_uuid_const(FileCtx) =:= file_ctx:get_uuid_const(ParentCtx) of
                true ->
                    {Doc, _} = file_ctx:get_file_doc(FileCtx2),
                    ?info("Infinite loop detected on parent dirs creation for file ~p", [Doc]),
                    lists:foreach(fun(Ctx) ->
                        create_dir(Ctx, SpaceId, Storage)
                    end, [FileCtx | ChildrenDirCtxs]);
                false ->
                    create_parent_dirs(ParentCtx, [FileCtx2 | ChildrenDirCtxs], SpaceId, Storage)
            end
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Creates directory on storage with suitable mode and owner.
%% @end
%%-------------------------------------------------------------------
-spec create_dir(file_ctx:ctx(), od_space:id(), storage:doc()) -> file_ctx:ctx().
create_dir(FileCtx, SpaceId, Storage) ->
    {FileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    SFMHandle0 = storage_file_manager:new_handle(?ROOT_SESS_ID, SpaceId,
        undefined, Storage, FileId, undefined),
    try
        {#document{value = #file_meta{
            mode = Mode}}, FileCtx3} = file_ctx:get_file_doc(FileCtx2),
        case file_ctx:is_space_dir_const(FileCtx3) of
            true ->
                mkdir_and_maybe_chown(SFMHandle0, ?AUTO_CREATED_PARENT_DIR_MODE,
                    FileCtx3, false);
            false ->
                mkdir_and_maybe_chown(SFMHandle0, Mode, FileCtx3, true)
        end
    catch
        Error:Reason ->
            ?error_stacktrace("Creating parent dir ~p failed due to ~p.
            Parent dir will be create with default mode.", [FileId, {Error, Reason}]),
            mkdir_and_maybe_chown(SFMHandle0, ?AUTO_CREATED_PARENT_DIR_MODE, FileCtx, true)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Creates directory on storage and chowns it if ShouldChown flag is set to
%% true.
%% @end
%%-------------------------------------------------------------------
-spec mkdir_and_maybe_chown(storage_file_manager:handle(), non_neg_integer(),
    file_ctx:ctx(), boolean()) -> any().
mkdir_and_maybe_chown(SFMHandle, Mode, FileCtx, ShouldChown) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    case storage_file_manager:mkdir(SFMHandle, Mode, false) of
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
%% Creates file on storage with uuid as suffix
%% @end
%%-------------------------------------------------------------------
-spec create_storage_file_with_suffix(storage_file_manager:handle(), 
    file_meta:posix_permissions()) -> {ok, helpers:file_id()}.
create_storage_file_with_suffix(#sfm_handle{file_uuid = Uuid, file = FileId} = SFMHandle, Mode) ->
    NewName = ?CONFLICTING_STORAGE_FILE_NAME(FileId, Uuid),
    SFMHandle1 = SFMHandle#sfm_handle{file = NewName},
    
    ?debug("File ~p exists on storage, creating ~p instead", [FileId, NewName]),
    case storage_file_manager:create(SFMHandle1, Mode) of
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
-spec handle_eexists(boolean(), storage_file_manager:handle(),
    file_meta:posix_permissions(), file_ctx:ctx(),  user_ctx:ctx()) -> {ok, file_ctx:ctx()}.
handle_eexists(_VerifyDeletionLink, SFMHandle, Mode, FileCtx, _UserCtx) ->
    {ok, StorageFileId} = create_storage_file_with_suffix(SFMHandle, Mode),
    {ok, file_ctx:set_file_id(FileCtx, StorageFileId)}.
    % TODO VFS-5271 - handle conflicting directories
%%    case VerifyDeletionLink of
%%        false ->
%%            {ok, StorageFileId} = create_storage_file_with_suffix(SFMHandle, Mode),
%%            {ok, file_ctx:set_file_id(FileCtx, StorageFileId)};
%%        _ ->
%%            {ParentCtx, FileCtx2} = file_ctx:get_parent(FileCtx, UserCtx),
%%            {FileName, FileCtx3} = file_ctx:get_aliased_name(FileCtx2, UserCtx),
%%            case location_and_link_utils:try_to_resolve_child_deletion_link(FileName, ParentCtx) of
%%                {error, not_found} ->
%%                    % Try once again to prevent races
%%                    storage_file_manager:create(SFMHandle, Mode);
%%                {ok, _FileUuid} ->
%%                    {ok, StorageFileId} = create_storage_file_with_suffix(SFMHandle, Mode),
%%                    {ok, file_ctx:set_file_id(FileCtx3, StorageFileId)}
%%            end
%%    end.