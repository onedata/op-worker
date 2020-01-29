%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% Util functions for file deletion.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_delete).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_sufix.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([check_if_opened_and_remove/4, remove_opened_file/1, remove_file/4,
    remove_file_handles/1, remove_auxiliary_documents/1, delete_all_opened_files/0]).

%% Test API
-export([process_file_links/4, get_open_file_handling_method/1]).

-define(RENAME_HANDLING_METHOD, rename).
-define(LINK_HANDLING_METHOD, deletion_link).
% TODO rename na ten górny
-define(DELETION_LINK_FLAG, deletion_link).

-type remove_file_meta_flag() :: boolean() | ?DELETION_LINK_FLAG.
-type handling_method() :: ?RENAME_HANDLING_METHOD | ?LINK_HANDLING_METHOD.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks if file is opened and deletes it or marks to be deleted.
%% @end
%%--------------------------------------------------------------------
-spec check_if_opened_and_remove(user_ctx:ctx(), file_ctx:ctx(),
    Silent :: boolean(), RemoteDelete :: boolean()) -> ok.
% TODO VFS-5268 - prevent reimport connected with remote delete
check_if_opened_and_remove(UserCtx, FileCtx, Silent, RemoteDelete) ->
    try
        FileUuid = file_ctx:get_uuid_const(FileCtx),
        case file_handles:exists(FileUuid) of
            true ->
                {HandlingMethod, FileCtx2} = ?MODULE:get_open_file_handling_method(FileCtx),
                RenameResult = rename_storage_file(FileCtx2, HandlingMethod),
                FileCtx3 = process_file_links(FileCtx2, UserCtx, RemoteDelete, HandlingMethod),
                ok = file_handles:mark_to_remove(FileCtx2),

                % Check once more to prevent race with last handle closing
                case {file_handles:exists(FileUuid), RenameResult} of
                    {true, _} ->
                        ok;
                    {false, {renamed, NewFileId, Size}} ->
                        delete_renamed_storage_file(FileCtx3, NewFileId, Size);
                    {false, _} ->
                        remove_opened_file(FileCtx3)
                end;
            _ ->
                ok = remove_file(FileCtx, UserCtx, true, not RemoteDelete)
        end,
        maybe_emit_event(FileCtx, UserCtx, Silent)
    catch
        _:{badmatch, {error, not_found}} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes opened file.
%% @end
%%--------------------------------------------------------------------
-spec remove_opened_file(file_ctx:ctx()) -> ok.
remove_opened_file(FileCtx) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    ok = remove_file(FileCtx, UserCtx, true, ?DELETION_LINK_FLAG).

%%--------------------------------------------------------------------
%% @doc
%% Deletes all opened files.
%% @end
%%--------------------------------------------------------------------
-spec delete_all_opened_files() -> ok.
delete_all_opened_files() ->
    case file_handles:list() of
        {ok, Docs} ->
            RemovedFiles = lists:filter(fun(#document{value = Handle}) ->
                Handle#file_handles.is_removed
            end, Docs),

            UserCtx = user_ctx:new(?ROOT_SESS_ID),
            lists:foreach(fun(#document{key = FileUuid} = Doc) ->
                try
                    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid),
                    FileCtx = file_ctx:new_by_guid(FileGuid),
                    ok = remove_file(FileCtx, UserCtx, true, ?DELETION_LINK_FLAG)
                catch
                    E1:E2 ->
                        ?warning_stacktrace("Cannot remove old opened file ~p: ~p:~p",
                            [Doc, E1, E2])
                end
            end, RemovedFiles),

            lists:foreach(fun(#document{key = FileUuid}) ->
                ok = file_handles:delete(FileUuid)
            end, Docs);
        Error ->
            ?error_stacktrace("Cannot clean open files descriptors - ~p", [Error])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes file handles
%% @end
%%--------------------------------------------------------------------
-spec remove_file_handles(file_ctx:ctx()) -> ok.
remove_file_handles(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    ok = file_handles:delete(FileUuid).

%%--------------------------------------------------------------------
%% @doc
%% Removes auxiliary documents connected with file.
%% @end
%%--------------------------------------------------------------------
-spec remove_auxiliary_documents(file_ctx:ctx()) -> ok.
remove_auxiliary_documents(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    ok = file_popularity:delete(FileUuid),
    ok = custom_metadata:delete(FileUuid),
    ok = times:delete(FileUuid),
    ok = transferred_file:clean_up(FileGuid).

%%--------------------------------------------------------------------
%% @doc
%% Removes file and file meta.
%% If parameter RemoveStorageFile is false, file will not be deleted
%% on storage.
%% Parameter DeleteMetadata verifies which metadata is deleted with file.
%% @end
%%--------------------------------------------------------------------
-spec remove_file(file_ctx:ctx(), user_ctx:ctx(), boolean(),
    remove_file_meta_flag()) -> ok.
remove_file(FileCtx, UserCtx, RemoveStorageFile, DeleteFileMeta) ->
    % TODO VFS-5270
    replica_synchronizer:apply(FileCtx, fun() ->
        FileCtx4 = case DeleteFileMeta of
            true ->
                {#document{value = #file_meta{
                    shares = Shares
                }}, FileCtx2} = file_ctx:get_file_doc(FileCtx),
                {ParentCtx, FileCtx3} = file_ctx:get_parent(FileCtx2, UserCtx),
                ok = delete_shares(UserCtx, Shares),

                fslogic_times:update_mtime_ctime(ParentCtx),
                FileCtx3;
            ?DELETION_LINK_FLAG ->
                FileCtx;
            _ ->
                FileCtx
        end,

        RemoveStorageFileResult = case RemoveStorageFile of
            true ->
                maybe_remove_file_on_storage(FileCtx4, UserCtx);
            _ -> ok
        end,

        FileCtx5 = case RemoveStorageFileResult of
            ok -> FileCtx4;
            {error, _} -> maybe_add_deletion_link(FileCtx4, UserCtx)
        end,

        FileUuid = file_ctx:get_uuid_const(FileCtx4),
        case {DeleteFileMeta, RemoveStorageFile, RemoveStorageFileResult} of
            {true, _, _} ->
                {FileDoc, _} = file_ctx:get_file_doc(FileCtx4),
                ok = fslogic_location_cache:delete_local_location(FileUuid),
                ok = file_meta:delete(FileDoc);
            {?DELETION_LINK_FLAG, _, ok} ->
                {HandlingMethod, FileCtx6} = fslogic_delete:get_open_file_handling_method(FileCtx5),
                {FileDoc, FileCtx7} = file_ctx:get_file_doc_including_deleted(FileCtx6),
                ok = fslogic_location_cache:delete_local_location(FileUuid),
                file_meta:delete_without_link(FileDoc), % do not match, document may not exist
                FileCtx8 = case HandlingMethod of
                    ?RENAME_HANDLING_METHOD -> FileCtx7;
                    ?LINK_HANDLING_METHOD -> remove_deletion_link(FileCtx7, UserCtx)
                end,
                try_to_delete_parent(FileCtx8, UserCtx);
            {?DELETION_LINK_FLAG, _, {error, _}} ->
                % TODO VFS-6082 deletion links are left forever when deleting file on storage failed
                ok = fslogic_location_cache:delete_local_location(FileUuid),
                {FileDoc, _FileCtx6} = file_ctx:get_file_doc_including_deleted(FileCtx5),
                file_meta:delete_without_link(FileDoc); % do not match, document may not exist
            {_, true, ok} ->
                ok = fslogic_location_cache:delete_local_location(FileUuid),
                try_to_delete_parent(FileCtx5, UserCtx);
            {_, true, {error, _}} ->
                ok = fslogic_location_cache:delete_local_location(FileUuid);
            _ ->
                ok
        end
    end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec maybe_add_deletion_link(file_ctx:ctx(), user_ctx:ctx()) -> file_ctx:ctx().
maybe_add_deletion_link(FileCtx, UserCtx) ->
    case file_ctx:is_space_dir_const(FileCtx) orelse file_ctx:is_root_dir_const(FileCtx) of
        true ->
            FileCtx;
        false ->
            {ParentGuid, FileCtx2} = file_ctx:get_parent_guid(FileCtx, UserCtx),
            {ParentUuid, _} = file_id:unpack_guid(ParentGuid),
            link_utils:add_deletion_link(FileCtx2, ParentUuid)
    end.


-spec try_to_delete_parent(file_ctx:ctx(), user_ctx:ctx()) -> ok.
try_to_delete_parent(FileCtx, UserCtx) ->
    {ParentCtx, _FileCtx2} = file_ctx:get_parent(FileCtx, UserCtx),
    try
        {ParentDoc, ParentCtx2} = file_ctx:get_file_doc_including_deleted(ParentCtx),
            case file_meta:is_deleted(ParentDoc) of
                true -> remove_file(ParentCtx2, UserCtx, true, ?DELETION_LINK_FLAG);
                false -> ok
            end
    catch
        error:{badmatch, {error, not_found}} ->
            ok
    end.

-spec remove_deletion_link(file_ctx:ctx(), user_ctx:ctx()) -> file_ctx:ctx().
remove_deletion_link(FileCtx, UserCtx) ->
    {ParentGuid, FileCtx2} = file_ctx:get_parent_guid(FileCtx, UserCtx),
    ParentUuid = file_id:guid_to_uuid(ParentGuid),
    link_utils:remove_deletion_link(FileCtx2, ParentUuid).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function adds a deletion_link for the file that is to be deleted.
%% It can also delete normal link from parent to the file.
%% @end
%%-------------------------------------------------------------------
-spec process_file_links(file_ctx:ctx(), user_ctx:ctx(), boolean(), handling_method()) -> file_ctx:ctx().
process_file_links(FileCtx, UserCtx, KeepParentLink, HandlingMethod) ->
    FileCtx2 = case HandlingMethod of
        ?LINK_HANDLING_METHOD ->
            maybe_add_deletion_link(FileCtx, UserCtx);
        _ ->
            FileCtx
    end,
    case KeepParentLink of
        false ->
            FileUuid = file_ctx:get_uuid_const(FileCtx2),
            Scope = file_ctx:get_space_id_const(FileCtx2),
            {FileName, FileCtx3} = file_ctx:get_aliased_name(FileCtx2, UserCtx),
            {ParentGuid, FileCtx4} = file_ctx:get_parent_guid(FileCtx3, UserCtx),
            ParentUuid = file_id:guid_to_uuid(ParentGuid),
            ok = file_meta:delete_child_link(ParentUuid, Scope, FileUuid, FileName),
            FileCtx4;
        _ ->
            FileCtx2
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes given file on storage if it exists.
%% Returns ok if file doesn't exist or if it was successfully deleted.
%% @end
%%--------------------------------------------------------------------
-spec maybe_remove_file_on_storage(file_ctx:ctx(), user_ctx:ctx()) -> ok | {error, term()}.
maybe_remove_file_on_storage(FileCtx, UserCtx) ->
    try
        case sfm_utils:recursive_delete(FileCtx, UserCtx) of
            ok -> ok;
            {error, ?ENOENT} -> ok;
            {error, _} = Error -> Error
        end
    catch
        throw:{delete_child_error, Error2} ->
            Error2;
        Error3:Reason3 ->
            FileGuid = file_ctx:get_guid_const(FileCtx),
            ?error_stacktrace("Unexpected error ~p:~p occured when deleting ~p", [Error3, Reason3, FileGuid]),
            {error, Reason3}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes given shares from oz and db.
%% @end
%%--------------------------------------------------------------------
-spec delete_shares(user_ctx:ctx(), [od_share:id()]) -> ok | no_return().
delete_shares(_UserCtx, []) ->
    ok;
delete_shares(UserCtx, Shares) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    [ok = share_logic:delete(SessionId, ShareId) || ShareId <- Shares],
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Emit file_removed event. If parameter Silent is true, event will not
%% be emitted.
%% @end
%%--------------------------------------------------------------------
-spec maybe_emit_event(file_ctx:ctx(), user_ctx:ctx(), boolean()) -> ok.
maybe_emit_event(FileCtx, UserCtx, false) ->
    SessId = user_ctx:get_session_id(UserCtx),
    fslogic_event_emitter:emit_file_removed(FileCtx, [SessId]),
    ok;
maybe_emit_event(_FileCtx, _UserCtx, _) ->
    ok.

-spec get_open_file_handling_method(file_ctx:ctx()) -> {handling_method(), file_ctx:ctx()}.
get_open_file_handling_method(FileCtx) ->
    {#document{
        value = #storage{helpers = [#helper{name = HelperName} | _]}
    }, FileCtx2} =
        file_ctx:get_storage_doc(FileCtx),
    case lists:member(HelperName,
        [?POSIX_HELPER_NAME, ?NULL_DEVICE_HELPER_NAME, ?GLUSTERFS_HELPER_NAME,
            ?WEBDAV_HELPER_NAME]) of
        true -> {?RENAME_HANDLING_METHOD, FileCtx2};
        _ -> {?LINK_HANDLING_METHOD, FileCtx2}
    end.

-spec rename_storage_file(file_ctx:ctx(), handling_method()) ->
    {renamed, helpers:file_id(), non_neg_integer()} | {error, ?ENOENT} | ignored.
rename_storage_file(FileCtx, ?RENAME_HANDLING_METHOD) ->
    SessId = ?ROOT_SESS_ID,
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {ok, Storage} = fslogic_storage:select_storage(SpaceId),
    {FileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    {Size, _} = file_ctx:get_file_size(FileCtx2),
    TargetFileId = filename:join(?DELETED_OPENED_FILES_DIR, FileGuid),

    Handle = storage_file_manager:new_handle(SessId, SpaceId, FileUuid, Storage, FileId, undefined),
    case rename_storage_file(Handle, FileUuid, TargetFileId, Size) of
        {error, ?ENOENT} ->
            ensure_dir_for_deleted_files_created(SessId, SpaceId, FileUuid, Storage),
            rename_storage_file(Handle, FileUuid, TargetFileId, Size);
        Other ->
            Other
    end;
rename_storage_file(_, _) ->
    ignored.

-spec ensure_dir_for_deleted_files_created(session:id(), od_space:id(), file_meta:uuid(), Storage :: datastore:doc()) ->
    ok.
ensure_dir_for_deleted_files_created(SessId, SpaceId, FileUuid, Storage) ->
    RootHandle = storage_file_manager:new_handle(SessId, SpaceId, FileUuid,
        Storage, ?DELETED_OPENED_FILES_DIR, undefined),
    case storage_file_manager:mkdir(RootHandle, 8#777, false) of
        ok -> ok;
        {error, ?EEXIST} -> ok
    end.

-spec rename_storage_file(storage_file_manager:handle(), file_meta:uuid(), helpers:file_id(), non_neg_integer()) ->
    {renamed, helpers:file_id(), non_neg_integer()} | {error, ?ENOENT}.
rename_storage_file(Handle, FileUuid, TargetFileId, Size) ->
    case storage_file_manager:mv(Handle, TargetFileId) of
        ok ->
            LocId = file_location:local_id(FileUuid),
            fslogic_location_cache:update_location(FileUuid, LocId, fun(FileLocation) ->
                {ok, FileLocation#file_location{file_id = TargetFileId}}
            end, false),
            {renamed, TargetFileId, Size};
        {error, ?ENOENT} = Error->
            Error
    end.

-spec delete_renamed_storage_file(file_ctx:ctx(), helpers:file_id(), non_neg_integer()) -> ok.
delete_renamed_storage_file(FileCtx, FileId, Size) ->
    SessId = ?ROOT_SESS_ID,
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    ShareId = file_ctx:get_share_id_const(FileCtx),
    {Storage, _FileCtx2} = file_ctx:get_storage_doc(FileCtx),
    SFMHandle = storage_file_manager:new_handle(SessId, SpaceId, FileUuid, Storage, FileId, ShareId),
    ok = storage_file_manager:unlink(SFMHandle, Size).