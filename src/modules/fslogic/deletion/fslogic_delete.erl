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
-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    delete_file_locally/4,
    handle_remotely_deleted_file/1,
    handle_release_of_deleted_file/2,
    handle_file_deleted_on_imported_storage/1,
    cleanup_file/2
]).
-export([cleanup_opened_files/0]).

%% Test API
-export([delete_parent_link/2, get_open_file_handling_method/1]).


% Below type determines strategies for handling deletion of opened files.
% ?RENAME_DELETED means that an opened file is moved to a special directory with name ?DELETED_OPENED_FILES_DIR
% ?SET_DELETION_MARKER means that a deletion_marker is added for the file (see deletion_marker.erl)
-type opened_file_deletion_method() :: ?RENAME_DELETED | ?SET_DELETION_MARKER.

%% Macros defining scopes of deleting docs associated with file.
% ?LOCAL_DOCS scope is used in case of remote deletion
% ?ALL_DOCS scope is used in case of local deletion
-define(LOCAL_DOCS, local_docs).
-define(ALL_DOCS, all_docs).
-type docs_deletion_scope() :: ?LOCAL_DOCS | ?ALL_DOCS.

%% Macros defining types of delete procedures
%% Single step deletion is performed in case of directories or files that are closed.
%% Two step deletion is performed in case of opened files.
-define(SINGLE_STEP_DEL, single_step_deletion).
-define(TWO_STEP_DEL_INIT, two_step_deletion_init).
-define(TWO_STEP_DEL_FIN, two_step_deletion_fin).
-type deletion_type() :: ?SINGLE_STEP_DEL | ?TWO_STEP_DEL_INIT | ?TWO_STEP_DEL_FIN.

% Record describing specification of deletion procedure
-record(deletion_spec, {
    type :: deletion_type(),
    scope :: docs_deletion_scope()
}).

-define(SPEC(Type, Scope), #deletion_spec{
    type = Type,
    scope = Scope
}).
-type spec() :: #deletion_spec{}.


%%%===================================================================
%%% API and file-deletion flow functions
%%%===================================================================

-spec delete_file_locally(user_ctx:ctx(), file_ctx:ctx(), od_provider:id(), boolean()) -> ok.
delete_file_locally(UserCtx, FileCtx, Creator, Silent) ->
    file_qos:cleanup_reference_related_documents(FileCtx),
    % TODO VFS-7448 - test events production
    case {file_ctx:is_link_const(FileCtx), oneprovider:is_self(Creator)} of
        {false, _} ->
            check_references_and_remove(UserCtx, FileCtx, Silent);
        {true, true} ->
            % Only creator can delete hardlink to prevent races on references
            % in #file_meta{} and allow conflicts resolution
            delete_hardlink_locally(UserCtx, FileCtx, Silent);
        {true, false} ->
            % Hardlink will be deleted by creator in dbsync hook
            delete_parent_link(FileCtx, UserCtx),
            ok
    end.

%% @private
-spec check_references_and_remove(user_ctx:ctx(), file_ctx:ctx(), boolean()) -> ok.
check_references_and_remove(UserCtx, FileCtx, Silent) ->
    % TODO VFS-7436 - handle deletion links for hardlinks to integrate with sync
    case inspect_references(FileCtx) of
        no_references_left ->
            remove_or_handle_opened_file(UserCtx, FileCtx, Silent, ?ALL_DOCS);
        has_at_least_one_reference ->
            % There are hardlinks to file - do not delete documents, remove only link
            delete_parent_link(FileCtx, UserCtx),
            ok
    end.

%% @private
-spec delete_hardlink_locally(user_ctx:ctx(), file_ctx:ctx(), boolean()) -> ok.
delete_hardlink_locally(UserCtx, FileCtx, Silent) ->
    % TODO VFS-7436 - handle deletion links for hardlinks to integrate with sync
    case deregister_link_and_inspect_references(FileCtx) of
        no_references_left ->
            remove_or_handle_opened_file(UserCtx, FileCtx, Silent, ?ALL_DOCS),
            % File meta for original file has not been deleted because hardlink existed - delete it now
            delete_referenced_file_meta(FileCtx);
        has_at_least_one_reference ->
            FileCtx2 = delete_parent_link(FileCtx, UserCtx),
            delete_file_meta(FileCtx2),
            ok
    end.


-spec handle_remotely_deleted_file(file_ctx:ctx()) -> ok.
handle_remotely_deleted_file(FileCtx) ->
    file_qos:cleanup_reference_related_documents(FileCtx),
    % TODO VFS-7445 - test race between hardlink and original file file_meta documents
    % when last hardlink is deleted and file has been deleted before
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    Type = file_meta:get_type(FileDoc),
    Creator = file_meta:get_provider_id(FileDoc),
    case {Type, oneprovider:is_self(Creator)} of
        {?LINK_TYPE, true} ->
            % Hardlink created by this provider has been deleted
            handle_remotely_deleted_local_hardlink(FileCtx2);
        _ ->
            % Hardlink created by other provider or regular file has been
            % deleted - check if local documents should be cleaned
            case inspect_references(FileCtx2) of
                no_references_left ->
                    UserCtx = user_ctx:new(?ROOT_SESS_ID),
                    remove_or_handle_opened_file(UserCtx, FileCtx2, false, ?LOCAL_DOCS);
                has_at_least_one_reference ->
                    ok
            end
    end.

%% @private
-spec handle_remotely_deleted_local_hardlink(file_ctx:ctx()) -> ok.
handle_remotely_deleted_local_hardlink(FileCtx) ->
    case deregister_link_and_inspect_references(FileCtx) of
        no_references_left ->
            delete_file_meta(FileCtx), % Delete hardlink document
            UserCtx = user_ctx:new(?ROOT_SESS_ID),
            % Delete documents connected with original file as deleted
            % hardlink is last reference to data
            remove_or_handle_opened_file(UserCtx, FileCtx, false, ?LOCAL_DOCS);
        has_at_least_one_reference ->
            delete_file_meta(FileCtx),
            ok
    end.


-spec handle_release_of_deleted_file(file_ctx:ctx(), file_handles:removal_status()) -> ok.
handle_release_of_deleted_file(FileCtx, RemovalStatus) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    DocsDeletionScope = removal_status_to_docs_deletion_scope(RemovalStatus),
    ok = remove_file(FileCtx, UserCtx, true, ?SPEC(?TWO_STEP_DEL_FIN, DocsDeletionScope)).


-spec handle_file_deleted_on_imported_storage(file_ctx:ctx()) -> ok.
handle_file_deleted_on_imported_storage(FileCtx) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    ok = remove_file(FileCtx, UserCtx, false, ?SPEC(?SINGLE_STEP_DEL, ?ALL_DOCS)),
    fslogic_event_emitter:emit_file_removed(FileCtx, []),
    remove_file_handles(FileCtx).


-spec cleanup_file(file_ctx:ctx(), boolean()) -> ok.
cleanup_file(FileCtx, RemoveStorageFile) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    ok = remove_file(FileCtx, UserCtx, RemoveStorageFile, ?SPEC(?SINGLE_STEP_DEL, ?LOCAL_DOCS)).


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
                    ok = remove_file(FileCtx, UserCtx, true, ?SPEC(?TWO_STEP_DEL_FIN, ?ALL_DOCS))
                catch
                    E1:E2:Stacktrace ->
                        ?warning_stacktrace("Cannot remove old opened file ~p: ~p:~p", [Doc, E1, E2], Stacktrace)
                end
            end, RemovedFiles),

            lists:foreach(fun(#document{key = FileUuid}) ->
                ok = file_handles:delete(FileUuid)
            end, Docs);
        Error ->
            ?error("Cannot clean open files descriptors - ~p", [Error])
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec deregister_link_and_inspect_references(file_ctx:ctx()) -> file_meta_hardlinks:references_presence().
deregister_link_and_inspect_references(FileCtx) ->
    LinkUuid = file_ctx:get_logical_uuid_const(FileCtx),
    FileUuid = file_ctx:get_referenced_uuid_const(FileCtx),
    {ok, ReferencesPresence} = file_meta_hardlinks:deregister(FileUuid, LinkUuid), % VFS-7444 - maybe update doc in FileCtx
    ReferencesPresence.

-spec inspect_references(file_ctx:ctx()) -> file_meta_hardlinks:references_presence().
inspect_references(FileCtx) ->
    FileUuid = file_ctx:get_referenced_uuid_const(FileCtx),
    file_meta_hardlinks:inspect_references(FileUuid).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if file is opened and deletes it or marks to be deleted.
%% @end
%%--------------------------------------------------------------------
-spec remove_or_handle_opened_file(user_ctx:ctx(), file_ctx:ctx(), boolean(), docs_deletion_scope()) -> ok.
% TODO VFS-5268 - prevent reimport connected with remote delete
remove_or_handle_opened_file(UserCtx, FileCtx, Silent, DocsDeletionScope) ->
    try
        case file_ctx:is_dir(FileCtx) of
            {true, FileCtx2} ->
                ok = remove_file(FileCtx2, UserCtx, true, ?SPEC(?SINGLE_STEP_DEL, DocsDeletionScope)),
                maybe_emit_event(FileCtx2, UserCtx, Silent);
            {false, FileCtx2} ->
                FileUuid = file_ctx:get_logical_uuid_const(FileCtx2),
                case file_handles:is_file_opened(FileUuid) of
                    true ->
                        handle_opened_file(FileCtx2, UserCtx, DocsDeletionScope);
                    _ ->
                        ok = remove_file(FileCtx2, UserCtx, true, ?SPEC(?SINGLE_STEP_DEL, DocsDeletionScope))
                end,
                maybe_emit_event(FileCtx2, UserCtx, Silent)
        end
    catch
        _:{badmatch, {error, not_found}} ->
            ok
    end.


-spec handle_opened_file(file_ctx:ctx(), user_ctx:ctx(), docs_deletion_scope()) -> ok.
handle_opened_file(FileCtx, UserCtx, DocsDeletionScope) ->
    ok = remove_file(FileCtx, UserCtx, false, ?SPEC(?TWO_STEP_DEL_INIT, DocsDeletionScope)),
    {HandlingMethod, FileCtx2} = fslogic_delete:get_open_file_handling_method(FileCtx),
    FileCtx3 = custom_handle_opened_file(FileCtx2, UserCtx, DocsDeletionScope, HandlingMethod),
    RemovalStatus = docs_deletion_scope_to_removal_status(DocsDeletionScope),
    ok = file_handles:mark_to_remove(FileCtx3, RemovalStatus),
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx3),
    % Check once more to prevent race with last handle being closed
    case file_handles:is_file_opened(FileUuid) of
        true ->
            ok;
        false ->
            handle_release_of_deleted_file(FileCtx3, RemovalStatus)
    end.


%%--------------------------------------------------------------------
%% @doc
%% This function performs some extra actions considering removal of
%% an opened file, accordingly to HandlingMethod.
%% NOTE!!! For ?RENAME_HANDLING_METHOD it updates file's storage_file_id
%% in returned FileCtx.
%% @end
%%--------------------------------------------------------------------
-spec custom_handle_opened_file(file_ctx:ctx(), user_ctx:ctx(), docs_deletion_scope(), opened_file_deletion_method()) ->
    file_ctx:ctx().
custom_handle_opened_file(FileCtx, UserCtx, DocsDeletionScope, ?RENAME_DELETED) ->
    FileCtx3 = case maybe_rename_storage_file(FileCtx) of
        {ok, FileCtx2} -> FileCtx2;
        {error, _} -> FileCtx
    end,
    % TODO VFS-6114 maybe we should call maybe_try_to_delete_parent/3 here?
    maybe_delete_parent_link(FileCtx3, UserCtx, DocsDeletionScope == ?LOCAL_DOCS);
custom_handle_opened_file(FileCtx, UserCtx, ?LOCAL_DOCS, ?SET_DELETION_MARKER) ->
    maybe_add_deletion_marker(FileCtx, UserCtx);
custom_handle_opened_file(FileCtx, UserCtx, _DocsDeletionScope, ?SET_DELETION_MARKER) ->
    FileCtx2 = maybe_add_deletion_marker(FileCtx, UserCtx),
    delete_parent_link(FileCtx2, UserCtx).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes file and file meta.
%% If parameter RemoveStorageFile is false, file will not be deleted
%% on storage.
%% Parameter DeletionSpec determines which metadata should be deleted.
%% @end
%%--------------------------------------------------------------------
-spec remove_file(file_ctx:ctx(), user_ctx:ctx(), boolean(), spec()) -> ok | {error, term()}.
remove_file(FileCtx, UserCtx, RemoveStorageFile, DeletionSpec) ->
    % TODO VFS-5270
    replica_synchronizer:apply(FileCtx, fun() ->
        case maybe_delete_storage_file(FileCtx, UserCtx, RemoveStorageFile) of
            {ok, FileCtx2} ->
                delete_file_metadata(FileCtx2, UserCtx, DeletionSpec, true);
            {error, ?ENOTEMPTY} ->
                delete_file_metadata(FileCtx, UserCtx, DeletionSpec, false);
            {error, _} = Error ->
                Error
        end
        % TODO VFS-7138 failure to delete file from storage should result in whole procedure failure
    end).


-spec maybe_delete_storage_file(file_ctx:ctx(), user_ctx:ctx(), boolean()) ->
    {ok, file_ctx:ctx()} | {error, term()}.
maybe_delete_storage_file(FileCtx, UserCtx, RemoveStorageFile) ->
    {IsReadonly, FileCtx2} = file_ctx:is_readonly_storage(FileCtx),
    case RemoveStorageFile andalso not IsReadonly of
        true ->
           delete_storage_file(FileCtx2, UserCtx);
        false ->
            {ok, FileCtx2}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes given file on storage if it exists.
%% Returns ok if file doesn't exist or if it was successfully deleted.
%% @end
%%--------------------------------------------------------------------
-spec delete_storage_file(file_ctx:ctx(), user_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | {error, term()}.
delete_storage_file(FileCtx, UserCtx) ->
    try
        case sd_utils:delete(FileCtx, UserCtx) of
            {ok, FileCtx2} ->
                {ok, FileCtx2};
            {error, ?ENOENT} ->
                {ok, FileCtx};
            {error, ?ENOTEMPTY} = Error ->
                % do not log ENOTEMPTY as it can happen in case of deleting directory with
                % child that is still opened or in case of race on remote deletion
                Error;
            {error, _} = OtherError ->
                log_storage_file_deletion_error(FileCtx, OtherError, false),
                OtherError
        end
    catch
        Class:Reason:Stacktrace ->
            log_storage_file_deletion_error(FileCtx, {Class, Reason}, {true, Stacktrace}),
            {error, Reason}
    end.


-spec delete_file_metadata(file_ctx:ctx(), user_ctx:ctx(), spec(), boolean()) -> ok.
delete_file_metadata(FileCtx, UserCtx, ?SPEC(?SINGLE_STEP_DEL, ?ALL_DOCS), StorageFileDeleted) ->
    FileCtx2 = update_parent_timestamps(UserCtx, FileCtx),
    % TODO VFS-6094 currently, we remove file_location even if remove on storage fails
    % get StorageFileId before location is deleted as it's stored in file_location doc
    {StorageFileId, FileCtx3} = file_ctx:get_storage_file_id(FileCtx2),
    FileCtx4 = delete_location(FileCtx3),
    FileCtx5 = delete_file_meta(FileCtx4),
    remove_associated_documents(FileCtx5, StorageFileDeleted, StorageFileId),
    FileCtx6 = remove_deletion_marker(FileCtx5, UserCtx, StorageFileId),
    maybe_try_to_delete_parent(FileCtx6, UserCtx, ?ALL_DOCS, StorageFileId);
delete_file_metadata(FileCtx, UserCtx, ?SPEC(?SINGLE_STEP_DEL, ?LOCAL_DOCS), StorageFileDeleted) ->
    % get StorageFileId before location is deleted as it's stored in file_location doc
    {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    FileCtx3 = delete_location(FileCtx2),
    remove_local_associated_documents(FileCtx3, StorageFileDeleted, StorageFileId),
    maybe_try_to_delete_parent(FileCtx3, UserCtx, ?LOCAL_DOCS, StorageFileId);
delete_file_metadata(FileCtx, UserCtx, ?SPEC(?TWO_STEP_DEL_INIT, _DocsDeletionScope), _StorageFileDeleted) ->
    % TODO VFS-6114 maybe delete file_meta and associated documents here?
    update_parent_timestamps(UserCtx, FileCtx),
    ok;
delete_file_metadata(FileCtx, UserCtx, ?SPEC(?TWO_STEP_DEL_FIN, DocsDeletionScope), StorageFileDeleted) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),
    % get StorageFileId before location is deleted as it's stored in file_location doc
    {StorageFileId, FileCtx3} = file_ctx:get_storage_file_id(FileCtx2),
    FileCtx4 = delete_location(FileCtx3),
    file_meta:delete_without_link(FileDoc), % do not match, document may not exist
    case DocsDeletionScope of
        ?ALL_DOCS ->
            remove_associated_documents(FileCtx4, StorageFileDeleted, StorageFileId),
            % remove deletion marker even if open_file_handling method is rename
            % as deletion marker may have been created when error occurred on deleting file on storage
            FileCtx5 = remove_deletion_marker(FileCtx4, UserCtx, StorageFileId),
            maybe_try_to_delete_parent(FileCtx5, UserCtx, DocsDeletionScope, StorageFileId);
        ?LOCAL_DOCS->
            remove_local_associated_documents(FileCtx4, StorageFileDeleted, StorageFileId),
            % remove deletion marker even if open_file_handling method is rename
            % as deletion marker may have been created when error occurred on deleting file on storage
            FileCtx5 = remove_deletion_marker(FileCtx4, UserCtx, StorageFileId),
            maybe_try_to_delete_parent(FileCtx5, UserCtx, DocsDeletionScope, StorageFileId)
    end.


-spec maybe_try_to_delete_parent(file_ctx:ctx(), user_ctx:ctx(), docs_deletion_scope(), helpers:file_id()) -> ok.
maybe_try_to_delete_parent(_FileCtx, _UserCtx, _DocsDeletionScope, ?DELETED_OPENED_FILES_DIR) ->
    ok;
maybe_try_to_delete_parent(FileCtx, UserCtx, DocsDeletionScope, StorageFileId) ->
    {ParentCtx, _FileCtx2} = files_tree:get_parent(FileCtx, UserCtx),
    try
        {ParentDoc, ParentCtx2} = file_ctx:get_file_doc_including_deleted(ParentCtx),
            case file_meta:is_deleted(ParentDoc) of
                true ->
                    % set directory storage_file_id basing on child's file_id as parent dir_location
                    % may have already been deleted
                    ParentCtx3 = file_ctx:set_file_id(ParentCtx2, filename:dirname(StorageFileId)),
                    % use ?TWO_STEP_DEL_FIN mode because it handles case when file_meta is already deleted
                    remove_file(ParentCtx3, UserCtx, true, ?SPEC(?TWO_STEP_DEL_FIN, DocsDeletionScope));
                false ->
                    ok
            end
    catch
        error:{badmatch, {error, not_found}} ->
            ok
    end.


-spec maybe_add_deletion_marker(file_ctx:ctx(), user_ctx:ctx()) -> file_ctx:ctx().
maybe_add_deletion_marker(FileCtx, UserCtx) ->
    case file_ctx:is_space_dir_const(FileCtx) orelse file_ctx:is_root_dir_const(FileCtx) of
        true ->
            % this case should never happen
            ?warning("Adding deletion marker for space or root directory is not allowed"),
            FileCtx;
        false ->
            case file_ctx:is_imported_storage(FileCtx) of
                {true, FileCtx2} ->
                    {ParentGuid, FileCtx3} = files_tree:get_parent_guid_if_not_root_dir(FileCtx2, UserCtx),
                    {ParentUuid, _} = file_id:unpack_guid(ParentGuid),
                    deletion_marker:add(ParentUuid, FileCtx3);
                {false, FileCtx2} ->
                    FileCtx2
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes deletion marker on imported storages.
%% StorageFileId must be passed as argument, cannot be calculated using
%% FileCtx, as file_location may have already been deleted and we don't
%% want file_ctx to calculate StorageFileId basing on current CanonicalPath.
%% @end
%%--------------------------------------------------------------------
-spec remove_deletion_marker(file_ctx:ctx(), user_ctx:ctx(), helpers:file_id()) -> file_ctx:ctx().
remove_deletion_marker(FileCtx, UserCtx, StorageFileId) ->
    % TODO VFS-7377 use file_location:get_deleted instead of passing StorageFileId
    case file_ctx:is_imported_storage(FileCtx) of
        {true, FileCtx2} ->
            {ParentGuid, FileCtx3} = files_tree:get_parent_guid_if_not_root_dir(FileCtx2, UserCtx),
            ParentUuid = file_id:guid_to_uuid(ParentGuid),
            deletion_marker:remove_by_name(ParentUuid, filename:basename(StorageFileId)),
            FileCtx3;
        {false, FileCtx2} ->
            FileCtx2
    end.


-spec delete_parent_link(file_ctx:ctx(), user_ctx:ctx()) -> file_ctx:ctx().
delete_parent_link(FileCtx, UserCtx) ->
    maybe_delete_parent_link(FileCtx, UserCtx, false).


-spec maybe_delete_parent_link(file_ctx:ctx(), user_ctx:ctx(), KeepParentLink :: boolean()) -> file_ctx:ctx().
maybe_delete_parent_link(FileCtx, _UserCtx, true) ->
    FileCtx;
maybe_delete_parent_link(FileCtx, UserCtx, false) ->
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    Scope = file_ctx:get_space_id_const(FileCtx),
    {FileName, FileCtx3} = file_ctx:get_aliased_name(FileCtx, UserCtx),
    {ParentGuid, FileCtx4} = files_tree:get_parent_guid_if_not_root_dir(FileCtx3, UserCtx),
    ParentUuid = file_id:guid_to_uuid(ParentGuid),
    ok = file_meta_forest:delete(ParentUuid, Scope, FileName, FileUuid),
    FileCtx4.


-spec delete_file_meta(file_ctx:ctx()) -> file_ctx:ctx().
delete_file_meta(FileCtx) ->
    % use get_file_doc_including_deleted because doc can be marked as deleted
    % inside file_meta record and get_file_doc could fail
    FileCtx2 = dataset_api:handle_file_deleted(FileCtx),
    {FileDoc, FileCtx3} = file_ctx:get_file_doc_including_deleted(FileCtx2),
    ok = file_meta:delete(FileDoc),
    FileCtx3.

-spec delete_referenced_file_meta(file_ctx:ctx()) -> file_ctx:ctx().
delete_referenced_file_meta(FileCtx) ->
    delete_file_meta(file_ctx:ensure_based_on_referenced_guid(FileCtx)).


-spec update_parent_timestamps(user_ctx:ctx(), file_ctx:ctx()) -> file_ctx:ctx().
update_parent_timestamps(UserCtx, FileCtx) ->
    try
        {ParentCtx, FileCtx2} = files_tree:get_parent(FileCtx, UserCtx),
        fslogic_times:update_mtime_ctime(ParentCtx),
        FileCtx2
    catch
        error:{badmatch, {error, not_found}} ->
            FileCtx
    end.


-spec maybe_delete_storage_sync_info(file_ctx:ctx(), helpers:file_id()) -> file_ctx:ctx().
maybe_delete_storage_sync_info(FileCtx, StorageFileId) ->
    try
        case file_ctx:is_imported_storage(FileCtx) of
            {true, FileCtx2} ->
                SpaceId = file_ctx:get_space_id_const(FileCtx2),
                storage_sync_info:delete(StorageFileId, SpaceId),
                FileCtx2;
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


-spec get_open_file_handling_method(file_ctx:ctx()) -> {opened_file_deletion_method(), file_ctx:ctx()}.
get_open_file_handling_method(FileCtx) ->
    {Storage, FileCtx2} = file_ctx:get_storage(FileCtx),
    Helper = storage:get_helper(Storage),
    case helper:is_rename_supported(Helper) of
        true -> {?RENAME_DELETED, FileCtx2};
        _ -> {?SET_DELETION_MARKER, FileCtx2}
    end.


-spec maybe_rename_storage_file(file_ctx:ctx()) -> {ok, file_ctx:ctx()} | {error, ?ENOENT}.
maybe_rename_storage_file(FileCtx) ->
    {SourceFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    FileGuid = file_ctx:get_referenced_guid_const(FileCtx),
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
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx3),
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
-spec remove_associated_documents(file_ctx:ctx(), boolean(), helpers:file_id()) -> ok.
remove_associated_documents(FileCtx, StorageFileDeleted, StorageFileId) ->
    % TODO VFS-7377 use file_location:get_deleted instead of passing StorageFileId
    remove_synced_associated_documents(FileCtx),
    remove_local_associated_documents(FileCtx, StorageFileDeleted, StorageFileId).


-spec remove_synced_associated_documents(file_ctx:ctx()) -> ok.
remove_synced_associated_documents(FileCtx) ->
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    ok = custom_metadata:delete(FileUuid),
    ok = times:delete(FileUuid),
    ok = transferred_file:clean_up(FileGuid),
    ok = archive_recall:delete_synced_docs(FileUuid),
    ok = file_qos:delete_associated_entries_on_no_references(FileCtx).


-spec remove_local_associated_documents(file_ctx:ctx(), boolean(), helpers:file_id()) -> ok.
remove_local_associated_documents(FileCtx, StorageFileDeleted, StorageFileId) ->
    % TODO VFS-7377 use file_location:get_deleted instead of passing StorageFileId
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    StorageFileDeleted andalso maybe_delete_storage_sync_info(FileCtx, StorageFileId),
    ok = file_meta_posthooks:delete(FileUuid),
    ok = file_qos:cleanup_on_no_reference(FileCtx),
    ok = archive_recall:delete_local_docs(FileUuid),
    ok = file_popularity:delete(FileUuid).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes file handles
%% @end
%%--------------------------------------------------------------------
-spec remove_file_handles(file_ctx:ctx()) -> ok.
remove_file_handles(FileCtx) ->
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    ok = file_handles:delete(FileUuid).


-spec removal_status_to_docs_deletion_scope(file_handles:removal_status()) -> docs_deletion_scope().
removal_status_to_docs_deletion_scope(?LOCAL_REMOVE) -> ?ALL_DOCS;
removal_status_to_docs_deletion_scope(?REMOTE_REMOVE) -> ?LOCAL_DOCS.


-spec docs_deletion_scope_to_removal_status(docs_deletion_scope()) -> file_handles:removal_status().
docs_deletion_scope_to_removal_status(?LOCAL_DOCS) -> ?REMOTE_REMOVE;
docs_deletion_scope_to_removal_status(?ALL_DOCS) -> ?LOCAL_REMOVE.


-spec delete_location(file_ctx:ctx()) -> file_ctx:ctx().
delete_location(FileCtx) ->
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
    case IsDir of
        true ->
            case dir_location:delete(FileUuid) of
                ok -> ok;
                {error, not_found} -> ok
            end;
        false ->
            fslogic_location_cache:force_flush(FileUuid),
            ok = fslogic_location_cache:delete_local_location(FileUuid)
    end,
    FileCtx2.


-spec log_storage_file_deletion_error(file_ctx:ctx(), term(), false | {true, list()}) -> ok.
log_storage_file_deletion_error(FileCtx, Error, IncludeStacktrace) ->
    {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    {StorageId, FileCtx3} = file_ctx:get_storage_id(FileCtx2),
    FileGuid = file_ctx:get_logical_guid_const(FileCtx3),
    Format = "Deleting file ~s on storage ~s with guid ~s failed due to ~p.",
    Args = [StorageFileId, StorageId, FileGuid, Error],
    case IncludeStacktrace of
        {true, Stacktrace} -> ?error_stacktrace(Format, Args, Stacktrace);
        false -> ?error(Format, Args)
    end.