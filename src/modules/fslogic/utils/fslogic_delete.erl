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
-include("modules/fslogic/fslogic_suffix.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    delete_file_locally/3,
    handle_remotely_deleted_file/1,
    handle_release_of_deleted_file/2,
    handle_file_deleted_on_imported_storage/1]).
-export([cleanup_opened_files/0]).
-export([remove_local_associated_documents/1]).

%% Test API
-export([delete_parent_link/2, get_open_file_handling_method/1]).

% Macros defining methods of handling opened files
-define(RENAME_HANDLING_METHOD, rename).
-define(LINK_HANDLING_METHOD, deletion_link).
-type opened_file_handling_method() :: ?RENAME_HANDLING_METHOD | ?LINK_HANDLING_METHOD.

%% Macros defining scopes of deleting docs associated with file.
-define(LOCAL_DOCS, local_docs).
-define(ALL_DOCS, all_docs).
-type docs_deletion_scope() :: ?LOCAL_DOCS | ?ALL_DOCS.

%% Macros defining modes of deleting docs associated with file.
-define(SINGLE_STEP_DEL(DocsDeletionScope), {single_step_deletion, DocsDeletionScope}).
-define(TWO_STEP_DEL_INIT, two_step_deletion_init).
-define(TWO_STEP_DEL_FIN(DocsDeletionScope), {two_step_deletion_fin, DocsDeletionScope}).

%% @formatter:off
-type docs_deletion_mode() :: ?SINGLE_STEP_DEL(docs_deletion_scope()) | ?TWO_STEP_DEL_INIT |
                              ?TWO_STEP_DEL_FIN(docs_deletion_scope()).
%% @formatter:on

%%%===================================================================
%%% API
%%%===================================================================

-spec delete_file_locally(user_ctx:ctx(), file_ctx:ctx(), boolean()) -> ok.
delete_file_locally(UserCtx, FileCtx, Silent) ->
    check_if_opened_and_remove(UserCtx, FileCtx, Silent, ?ALL_DOCS).

-spec handle_remotely_deleted_file(file_ctx:ctx()) -> ok.
handle_remotely_deleted_file(FileCtx) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    check_if_opened_and_remove(UserCtx, FileCtx, false, ?LOCAL_DOCS).

-spec handle_release_of_deleted_file(file_ctx:ctx(), file_handles:removal_status()) -> ok.
handle_release_of_deleted_file(FileCtx, RemovalStatus) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    DocsDeletionScope = removal_status_to_docs_deletion_scope(RemovalStatus),
    ok = remove_file(FileCtx, UserCtx, true, ?TWO_STEP_DEL_FIN(DocsDeletionScope)).

    -spec handle_file_deleted_on_imported_storage(file_ctx:ctx()) -> ok.
handle_file_deleted_on_imported_storage(FileCtx) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    ok = remove_file(FileCtx, UserCtx, false, ?SINGLE_STEP_DEL(?ALL_DOCS)),
    fslogic_event_emitter:emit_file_removed(FileCtx, []),
    remove_file_handles(FileCtx).

%%--------------------------------------------------------------------
%% @doc
%% This function checks which out of opened files are marked as removed
%% and deletes them.
%% It also removes handles for each opened file.
%% @end
%%--------------------------------------------------------------------
-spec cleanup_opened_files() -> ok.
cleanup_opened_files() ->
    case file_handles:list() of
        {ok, Docs} ->
            RemovedFiles = lists:filter(fun(Doc) -> file_handles:is_removed(Doc) end, Docs),
            UserCtx = user_ctx:new(?ROOT_SESS_ID),
            lists:foreach(fun(#document{key = FileUuid} = Doc) ->
                try
                    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid),
                    FileCtx = file_ctx:new_by_guid(FileGuid),
                    ok = remove_file(FileCtx, UserCtx, true, ?TWO_STEP_DEL_FIN(?ALL_DOCS))
                catch
                    E1:E2 ->
                        ?warning_stacktrace("Cannot remove old opened file ~p: ~p:~p", [Doc, E1, E2])
                end
            end, RemovedFiles),

            lists:foreach(fun(#document{key = FileUuid}) ->
                ok = file_handles:delete(FileUuid)
            end, Docs);
        Error ->
            ?error("Cannot clean open files descriptors - ~p", [Error])
    end.

-spec remove_local_associated_documents(file_ctx:ctx()) -> ok.
remove_local_associated_documents(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    % TODO VFS-6114 delete storage_sync_info here?
    ok = file_qos:clean_up(FileCtx),
    ok = file_meta_posthooks:delete(FileUuid),
    ok = file_popularity:delete(FileUuid).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if file is opened and deletes it or marks to be deleted.
%% @end
%%--------------------------------------------------------------------
-spec check_if_opened_and_remove(user_ctx:ctx(), file_ctx:ctx(), boolean(), docs_deletion_scope()) -> ok.
% TODO VFS-5268 - prevent reimport connected with remote delete
check_if_opened_and_remove(UserCtx, FileCtx, Silent, DocsDeletionScope) ->
    try
        FileUuid = file_ctx:get_uuid_const(FileCtx),
        case file_handles:is_file_opened(FileUuid) of
            true ->
                handle_opened_file(FileCtx, UserCtx, DocsDeletionScope);
            _ ->
                ok = remove_file(FileCtx, UserCtx, true, ?SINGLE_STEP_DEL(DocsDeletionScope))
        end,
        maybe_emit_event(FileCtx, UserCtx, Silent)
    catch
        _:{badmatch, {error, not_found}} ->
            ok
    end.

-spec handle_opened_file(file_ctx:ctx(), user_ctx:ctx(), docs_deletion_scope()) -> ok.
handle_opened_file(FileCtx, UserCtx, DocsDeletionScope) ->
    ok = remove_file(FileCtx, UserCtx, false, ?TWO_STEP_DEL_INIT),
    {HandlingMethod, FileCtx2} = fslogic_delete:get_open_file_handling_method(FileCtx),
    FileCtx3 = custom_handle_opened_file(FileCtx2, UserCtx, DocsDeletionScope, HandlingMethod),
    RemovalStatus = docs_deletion_scope_to_removal_status(DocsDeletionScope),
    ok = file_handles:mark_to_remove(FileCtx3, RemovalStatus),
    FileUuid = file_ctx:get_uuid_const(FileCtx3),
    % Check once more to prevent race with last handle being closed
    case file_handles:is_file_opened(FileUuid) of
        true -> ok;
        false -> handle_release_of_deleted_file(FileCtx3, RemovalStatus)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes file and file meta.
%% If parameter RemoveStorageFile is false, file will not be deleted
%% on storage.
%% Parameter DeleteMode verifies which metadata is deleted with file.
%% @end
%%--------------------------------------------------------------------
-spec remove_file(file_ctx:ctx(), user_ctx:ctx(), boolean(), docs_deletion_mode()) -> ok.
remove_file(FileCtx, UserCtx, RemoveStorageFile, DeleteMode) ->
    % TODO VFS-5270
    replica_synchronizer:apply(FileCtx, fun() ->
        {RemoveStorageFileResult, FileCtx3} = case RemoveStorageFile of
            true ->
                % TODO VFS-6091 if there is a race between deleting nonempty directory
                % and creating the new one with the same name, the deletion link will stay forever
                case maybe_remove_file_on_storage(FileCtx, UserCtx) of
                    {ok, FileCtx2} ->
                        {ok, FileCtx2};
                    Error = {error, _} ->
                        % add deletion_link even if open_file_handling method is rename
                        % this way we are sure that remotely deleted file won't be reimported
                        % even if it hasn't been deleted because it's not empty yet
                        % TODO VFS-6082 deletion links are left forever when deleting file on storage failed
                        FileCtx2 = maybe_add_deletion_link(FileCtx, UserCtx),
                        {Error, FileCtx2}
                end;
            false ->
                {ignored, FileCtx}
        end,

        case DeleteMode of
            ?SINGLE_STEP_DEL(?ALL_DOCS) ->
                FileCtx4 = update_parent_timestamps(UserCtx, FileCtx3),
                {FileDoc, FileCtx5} = file_ctx:get_file_doc(FileCtx4),
                FileCtx6 = delete_storage_sync_info(FileCtx5),
                % TODO VFS-6094 currently, we remove file_location even if remove on storage fails
                ok = delete_location(FileCtx),
                ok = file_meta:delete(FileDoc),
                remove_associated_documents(FileCtx6),
                FileCtx7 = maybe_remove_deletion_link(FileCtx6, UserCtx, RemoveStorageFileResult),
                maybe_try_to_delete_parent(FileCtx7, UserCtx, RemoveStorageFileResult, ?ALL_DOCS);
            ?TWO_STEP_DEL_INIT ->
                % TODO VFS-6114 maybe delete file_meta and associated documents here?
                FileCtx5 = update_parent_timestamps(UserCtx, FileCtx3),
                delete_storage_sync_info(FileCtx5),
                ok;
            ?TWO_STEP_DEL_FIN(DocsDeletionScope) ->
                {FileDoc, FileCtx4} = file_ctx:get_file_doc_including_deleted(FileCtx3),
                ok = delete_location(FileCtx),
                file_meta:delete_without_link(FileDoc), % do not match, document may not exist
                case DocsDeletionScope of
                    ?ALL_DOCS -> remove_associated_documents(FileCtx4);
                    ?LOCAL_DOCS-> remove_local_associated_documents(FileCtx4)
                end,
                % remove deletion_link even if open_file_handling method is rename
                % as deletion_link may have been created when error occurred on deleting file on storage
                FileCtx5 = maybe_remove_deletion_link(FileCtx4, UserCtx, RemoveStorageFileResult),
                maybe_try_to_delete_parent(FileCtx5, UserCtx, RemoveStorageFileResult, DocsDeletionScope);
            ?SINGLE_STEP_DEL(?LOCAL_DOCS) ->
                FileCtx4 = delete_storage_sync_info(FileCtx3),
                ok = delete_location(FileCtx),
                remove_local_associated_documents(FileCtx4),
                maybe_try_to_delete_parent(FileCtx4, UserCtx, RemoveStorageFileResult, ?LOCAL_DOCS)
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% This function performs some extra actions considering removal of
%% an opened file, accordingly to HandlingMethod.
%% NOTE!!! For ?RENAME_HANDLING_METHOD it updates file's storage_file_id
%% in returned FileCtx.
%% @end
%%--------------------------------------------------------------------
-spec custom_handle_opened_file(file_ctx:ctx(), user_ctx:ctx(), docs_deletion_scope(), opened_file_handling_method()) ->
    file_ctx:ctx().
custom_handle_opened_file(FileCtx, UserCtx, DocsDeletionScope, ?RENAME_HANDLING_METHOD) ->
    FileCtx3 = case maybe_rename_storage_file(FileCtx) of
        {ok, FileCtx2} -> FileCtx2;
        {error, _} -> FileCtx
    end,
    % TODO VFS-6114 maybe we should call maybe_try_to_delete_parent/3 here?
    maybe_delete_parent_link(FileCtx3, UserCtx, DocsDeletionScope == ?LOCAL_DOCS);
custom_handle_opened_file(FileCtx, UserCtx, ?LOCAL_DOCS, ?LINK_HANDLING_METHOD) ->
    maybe_add_deletion_link(FileCtx, UserCtx);
custom_handle_opened_file(FileCtx, UserCtx, _DocsDeletionScope, ?LINK_HANDLING_METHOD) ->
    FileCtx2 = maybe_add_deletion_link(FileCtx, UserCtx),
    delete_parent_link(FileCtx2, UserCtx).


-spec maybe_try_to_delete_parent(file_ctx:ctx(), user_ctx:ctx(),
    RemoveStorageFileResult :: ok | ignored | {error, term()}, docs_deletion_scope()) -> ok.
maybe_try_to_delete_parent(FileCtx, UserCtx, ok, DocsDeletionScope) ->
    {ParentCtx, _FileCtx2} = file_ctx:get_parent(FileCtx, UserCtx),
    try
        {ParentDoc, ParentCtx2} = file_ctx:get_file_doc_including_deleted(ParentCtx),
            case file_meta:is_deleted(ParentDoc) of
                true ->
                    % use ?TWO_STEP_DEL_FIN mode because it handles case when file_meta is already deleted
                    remove_file(ParentCtx2, UserCtx, true, ?TWO_STEP_DEL_FIN(DocsDeletionScope));
                false ->
                    ok
            end
    catch
        error:{badmatch, {error, not_found}} ->
            ok
    end;
maybe_try_to_delete_parent(_FileCtx, _UserCtx, _, _) ->
    ok.


-spec maybe_add_deletion_link(file_ctx:ctx(), user_ctx:ctx()) -> file_ctx:ctx().
maybe_add_deletion_link(FileCtx, UserCtx) ->
    case file_ctx:is_space_dir_const(FileCtx) orelse file_ctx:is_root_dir_const(FileCtx) of
        true ->
            % this case should never happen
            ?warning("Adding deletion link for space or root directory is not allowed"),
            FileCtx;
        false ->
            {ParentGuid, FileCtx2} = file_ctx:get_parent_guid(FileCtx, UserCtx),
            {ParentUuid, _} = file_id:unpack_guid(ParentGuid),
            link_utils:add_deletion_link(FileCtx2, ParentUuid)
    end.


-spec maybe_remove_deletion_link(file_ctx:ctx(), user_ctx:ctx(), RemoveStorageFileResult :: ok | ignored | {error, term()}) ->
    file_ctx:ctx().
maybe_remove_deletion_link(FileCtx, UserCtx, ok) ->
    {ParentGuid, FileCtx2} = file_ctx:get_parent_guid(FileCtx, UserCtx),
    ParentUuid = file_id:guid_to_uuid(ParentGuid),
    link_utils:remove_deletion_link(FileCtx2, ParentUuid);
maybe_remove_deletion_link(FileCtx, _UserCtx, _) ->
    % TODO VFS-6082 deletion links are left forever when deleting file on storage failed
    FileCtx.

-spec delete_parent_link(file_ctx:ctx(), user_ctx:ctx()) -> file_ctx:ctx().
delete_parent_link(FileCtx, UserCtx) ->
    maybe_delete_parent_link(FileCtx, UserCtx, false).

-spec maybe_delete_parent_link(file_ctx:ctx(), user_ctx:ctx(), KeepParentLink :: boolean()) -> file_ctx:ctx().
maybe_delete_parent_link(FileCtx, _UserCtx, true) ->
    FileCtx;
maybe_delete_parent_link(FileCtx, UserCtx, false) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    Scope = file_ctx:get_space_id_const(FileCtx),
    {FileName, FileCtx3} = file_ctx:get_aliased_name(FileCtx, UserCtx),
    {ParentGuid, FileCtx4} = file_ctx:get_parent_guid(FileCtx3, UserCtx),
    ParentUuid = file_id:guid_to_uuid(ParentGuid),
    ok = file_meta:delete_child_link(ParentUuid, Scope, FileUuid, FileName),
    FileCtx4.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes given file on storage if it exists.
%% Returns ok if file doesn't exist or if it was successfully deleted.
%% @end
%%--------------------------------------------------------------------
-spec maybe_remove_file_on_storage(file_ctx:ctx(), user_ctx:ctx()) -> {ok, file_ctx:ctx()} | {error, term()}.
maybe_remove_file_on_storage(FileCtx, UserCtx) ->
    try
        case file_ctx:is_readonly_storage(FileCtx) of
            {true, FileCtx2} ->
                {ok, FileCtx2};
            {false, FileCtx2} ->
                case sd_utils:delete(FileCtx2, UserCtx) of
                    {ok, FileCtx3} -> {ok, FileCtx3};
                    {error, ?ENOENT} -> {ok, FileCtx2};
                    {error, _} = OtherError -> OtherError
                end
        end
    catch
        Error:Reason ->
            FileGuid = file_ctx:get_guid_const(FileCtx),
            ?error_stacktrace("Unexpected error ~p:~p occured when deleting ~p", [Error, Reason, FileGuid]),
            {error, Reason}
    end.

-spec update_parent_timestamps(user_ctx:ctx(), file_ctx:ctx()) -> file_ctx:ctx().
update_parent_timestamps(UserCtx, FileCtx) ->
    try
        {ParentCtx, FileCtx2} = file_ctx:get_parent(FileCtx, UserCtx),
        fslogic_times:update_mtime_ctime(ParentCtx),
        FileCtx2
    catch
        error:{badmatch, {error, not_found}} ->
            FileCtx
    end.

-spec delete_storage_sync_info(file_ctx:ctx()) -> file_ctx:ctx().
delete_storage_sync_info(FileCtx) ->
    try
        case file_ctx:is_imported_storage(FileCtx) of
            {true, FileCtx2} ->
                {StorageFileId, FileCtx3} = file_ctx:get_storage_file_id(FileCtx2),
                SpaceId = file_ctx:get_space_id_const(FileCtx3),
                storage_sync_info:delete(StorageFileId, SpaceId),
                FileCtx3;
            {false, FileCtx2} ->
                FileCtx2
        end
    catch
        error:{badmatch, {error, not_found}} ->
            FileCtx
    end.

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

-spec get_open_file_handling_method(file_ctx:ctx()) -> {opened_file_handling_method(), file_ctx:ctx()}.
get_open_file_handling_method(FileCtx) ->
    {Storage, FileCtx2} = file_ctx:get_storage(FileCtx),
    Helper = storage:get_helper(Storage),
    case helper:is_rename_supported(Helper) of
        true -> {?RENAME_HANDLING_METHOD, FileCtx2};
        _ -> {?LINK_HANDLING_METHOD, FileCtx2}
    end.

-spec maybe_rename_storage_file(file_ctx:ctx()) -> {ok, file_ctx:ctx()} | {error, ?ENOENT}.
maybe_rename_storage_file(FileCtx) ->
    {SourceFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    TargetFileId = filename:join(?DELETED_OPENED_FILES_DIR, FileGuid),
    case rename_storage_file(FileCtx2, SourceFileId, TargetFileId) of
        {error, ?ENOENT} ->
            SpaceId = file_ctx:get_space_id_const(FileCtx2),
            ensure_dir_for_deleted_files_created(SpaceId),
            rename_storage_file(FileCtx2, SourceFileId, TargetFileId);
        Other ->
            Other
    end.

-spec ensure_dir_for_deleted_files_created(od_space:id()) -> ok.
ensure_dir_for_deleted_files_created(SpaceId) ->
    {ok, StorageId} = space_logic:get_local_supporting_storage(SpaceId),
    RootHandle = storage_driver:new_handle(?ROOT_SESS_ID, SpaceId, undefined, StorageId, ?DELETED_OPENED_FILES_DIR),
    case storage_driver:mkdir(RootHandle, ?DELETED_OPENED_FILES_DIR_MODE, false) of
        ok -> ok;
        {error, ?EEXIST} -> ok
    end.

-spec rename_storage_file(file_ctx:ctx(), helpers:file_id(), helpers:file_id()) ->
    {ok, file_ctx:ctx()} | {error, term()}.
rename_storage_file(FileCtx, SourceFileId, TargetFileId) ->
    % ensure SourceFileId is set in FileCtx
    FileCtx2 = file_ctx:set_file_id(FileCtx, SourceFileId),
    {SDHandle, FileCtx3} = storage_driver:new_handle(?ROOT_SESS_ID, FileCtx2 ),
    FileUuid = file_ctx:get_uuid_const(FileCtx3),
    case init_file_location_rename(FileUuid, TargetFileId) of
        {ok, #document{value = NewFL}} ->
            case storage_driver:mv(SDHandle, TargetFileId) of
                ok ->
                    FinalFL = case finalize_file_location_rename(FileUuid) of
                        {ok, #document{value = NewFL2}} -> NewFL2;
                        {error, not_found} -> NewFL
                    end,
                    fslogic_event_emitter:emit_file_location_changed(FinalFL#file_location{blocks = []}, [], 0, 0),
                    {ok, file_ctx:set_file_id(FileCtx3, TargetFileId)};
                {error, ?ENOENT} = Error ->
                    Error
            end;
        {error, not_found} = Error2->
            Error2
    end.

-spec init_file_location_rename(file_meta:uuid(), helpers:file_id()) ->
    {ok, file_location:doc()} | {error, term()}.
init_file_location_rename(FileUuid, TargetFileId) ->
    LocId = file_location:local_id(FileUuid),
    fslogic_location_cache:update_location(FileUuid, LocId,
        fun(FL = #file_location{file_id = FileId}) ->
            {ok, FL#file_location{
                file_id = TargetFileId,
                rename_src_file_id = FileId
            }}
        end, false).

-spec finalize_file_location_rename(file_meta:uuid()) -> {ok, file_location:doc()} | {error, term()}.
finalize_file_location_rename(FileUuid) ->
    LocId = file_location:local_id(FileUuid),
    fslogic_location_cache:update_location(FileUuid, LocId, fun(FileLocation) ->
        {ok, FileLocation#file_location{rename_src_file_id = undefined}}
    end, false).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes associated documents connected with file.
%% @end
%%--------------------------------------------------------------------
-spec remove_associated_documents(file_ctx:ctx()) -> ok.
remove_associated_documents(FileCtx) ->
    remove_synced_associated_documents(FileCtx),
    remove_local_associated_documents(FileCtx).


-spec remove_synced_associated_documents(file_ctx:ctx()) -> ok.
remove_synced_associated_documents(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    ok = custom_metadata:delete(FileUuid),
    ok = times:delete(FileUuid),
    ok = transferred_file:clean_up(FileGuid),
    ok = file_qos:delete_associated_entries(FileUuid).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes file handles
%% @end
%%--------------------------------------------------------------------
-spec remove_file_handles(file_ctx:ctx()) -> ok.
remove_file_handles(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    ok = file_handles:delete(FileUuid).

-spec removal_status_to_docs_deletion_scope(file_handles:removal_status()) -> docs_deletion_scope().
removal_status_to_docs_deletion_scope(?LOCAL_REMOVE) -> ?ALL_DOCS;
removal_status_to_docs_deletion_scope(?REMOTE_REMOVE) -> ?LOCAL_DOCS.

-spec docs_deletion_scope_to_removal_status(docs_deletion_scope()) -> file_handles:removal_status().
docs_deletion_scope_to_removal_status(?LOCAL_DOCS) -> ?REMOTE_REMOVE;
docs_deletion_scope_to_removal_status(?ALL_DOCS) -> ?LOCAL_REMOVE.

-spec delete_location(file_ctx:ctx()) -> ok.
delete_location(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    case file_ctx:is_dir(FileCtx) of
        {true, _} ->
            case dir_location:delete(FileUuid) of
                ok -> ok;
                {error, not_found} -> ok
            end;
        {false, _} ->
            ok = fslogic_location_cache:delete_local_location(FileUuid)
    end.