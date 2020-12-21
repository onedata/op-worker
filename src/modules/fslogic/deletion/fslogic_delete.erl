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
    handle_file_deleted_on_imported_storage/1,
    cleanup_file/2
]).
-export([cleanup_opened_files/0]).

%% Test API
-export([delete_parent_link/2, get_open_file_handling_method/1]).


-type opened_file_handling_method() :: ?RENAME_HANDLING_METHOD | ?MARKER_HANDLING_METHOD.

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
    scope :: docs_deletion_scope() % todo a moze jednak info czy remote czy local zlecenie?
    % below flag determines whether deletion is performed as a result of child deletion
}).

-define(SPEC(Type, Scope), #deletion_spec{
    type = Type,
    scope = Scope
}).
-type spec() :: #deletion_spec{}.


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
                ok = remove_file(FileCtx, UserCtx, true, ?SPEC(?SINGLE_STEP_DEL, DocsDeletionScope))
        end,
        maybe_emit_event(FileCtx, UserCtx, Silent)
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
    FileUuid = file_ctx:get_uuid_const(FileCtx3),
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
-spec custom_handle_opened_file(file_ctx:ctx(), user_ctx:ctx(), docs_deletion_scope(), opened_file_handling_method()) ->
    file_ctx:ctx().
custom_handle_opened_file(FileCtx, UserCtx, DocsDeletionScope, ?RENAME_HANDLING_METHOD) ->
    FileCtx3 = case maybe_rename_storage_file(FileCtx) of
        {ok, FileCtx2} -> FileCtx2;
        {error, _} -> FileCtx
    end,
    % TODO VFS-6114 maybe we should call maybe_try_to_delete_parent/3 here?
    maybe_delete_parent_link(FileCtx3, UserCtx, DocsDeletionScope == ?LOCAL_DOCS);
custom_handle_opened_file(FileCtx, UserCtx, ?LOCAL_DOCS, ?MARKER_HANDLING_METHOD) ->
    maybe_add_deletion_marker(FileCtx, UserCtx);
custom_handle_opened_file(FileCtx, UserCtx, _DocsDeletionScope, ?MARKER_HANDLING_METHOD) ->
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
                ?error(delete_storage_file_error_msg(FileCtx, OtherError)),
                OtherError
        end
    catch
        Class:Reason ->
            % fetching stacktrace before log is ugly but
            % io_lib:format called by delete_storage_file_error_msg
            % messes with stacktrace which causes log to be irrelevant
            Stacktrace = erlang:get_stacktrace(),
            ?error(delete_storage_file_error_msg(FileCtx, {Class, Reason}) ++ "~nStacktrace:~n    ~p", [Stacktrace]),
%%            {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
%%            {StorageId, FileCtx3} = file_ctx:get_storage_id(FileCtx2),
%%            FileGuid = file_ctx:get_guid_const(FileCtx3),
%%            ?error_stacktrace("Deleting file ~s on storage ~s with guid ~s failed due to ~p:~p.", [
%%                StorageFileId, StorageId, FileGuid, Error, Reason
%%            ]),
            {error, Reason}
    end.


-spec delete_file_metadata(file_ctx:ctx(), user_ctx:ctx(), spec(), boolean()) -> ok.
delete_file_metadata(FileCtx, UserCtx, DeletionSpec, StorageFileDeleted) ->
    case DeletionSpec of
        ?SPEC(?SINGLE_STEP_DEL, ?ALL_DOCS) ->
            FileCtx2 = delete_shares_and_update_parent_timestamps(UserCtx, FileCtx),
            % TODO VFS-6094 currently, we remove file_location even if remove on storage fails
            FileCtx3 = delete_location(FileCtx2),
            FileCtx4 = delete_file_meta(FileCtx3),
            remove_associated_documents(FileCtx4, StorageFileDeleted),
            FileCtx8 = remove_deletion_marker(FileCtx4, UserCtx),
            maybe_try_to_delete_parent(FileCtx8, UserCtx, ?ALL_DOCS);
        ?SPEC(?TWO_STEP_DEL_INIT, _DocsDeletionScope) ->
            % TODO VFS-6114 maybe delete file_meta and associated documents here?
            delete_shares_and_update_parent_timestamps(UserCtx, FileCtx),
            ok;
        ?SPEC(?TWO_STEP_DEL_FIN, DocsDeletionScope) ->
            {FileDoc, FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),
            FileCtx3 = delete_location(FileCtx2),
            file_meta:delete_without_link(FileDoc), % do not match, document may not exist
            case DocsDeletionScope of
                ?ALL_DOCS ->
                    remove_associated_documents(FileCtx3, StorageFileDeleted),
                    % remove deletion marker even if open_file_handling method is rename
                    % as deletion marker may have been created when error occurred on deleting file on storage
                    FileCtx4 = remove_deletion_marker(FileCtx3, UserCtx),
                    maybe_try_to_delete_parent(FileCtx4, UserCtx, DocsDeletionScope);
                ?LOCAL_DOCS->
                    remove_local_associated_documents(FileCtx3, StorageFileDeleted),
                    % remove deletion marker even if open_file_handling method is rename
                    % as deletion marker may have been created when error occurred on deleting file on storage
                    FileCtx4 = remove_deletion_marker(FileCtx3, UserCtx),
                    maybe_try_to_delete_parent(FileCtx4, UserCtx, DocsDeletionScope)
            end;
        ?SPEC(?SINGLE_STEP_DEL, ?LOCAL_DOCS) ->
            FileCtx2 = delete_location(FileCtx),
            remove_local_associated_documents(FileCtx2, StorageFileDeleted),
            maybe_try_to_delete_parent(FileCtx2, UserCtx, ?LOCAL_DOCS)
    end.


-spec maybe_try_to_delete_parent(file_ctx:ctx(), user_ctx:ctx(), docs_deletion_scope()) -> ok.
maybe_try_to_delete_parent(FileCtx, UserCtx, DocsDeletionScope) ->
    {ParentCtx, _FileCtx2} = file_ctx:get_parent(FileCtx, UserCtx),
    try
        {ParentDoc, ParentCtx2} = file_ctx:get_file_doc_including_deleted(ParentCtx),
            case file_meta:is_deleted(ParentDoc) of
                true ->
                    % use ?TWO_STEP_DEL_FIN mode because it handles case when file_meta is already deleted
                    remove_file(ParentCtx2, UserCtx, true, ?SPEC(?TWO_STEP_DEL_FIN, DocsDeletionScope));
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
                    {ParentGuid, FileCtx3} = file_ctx:get_parent_guid(FileCtx2, UserCtx),
                    {ParentUuid, _} = file_id:unpack_guid(ParentGuid),
                    deletion_marker:add(ParentUuid, FileCtx3);
                {false, FileCtx2} ->
                    FileCtx2
            end
    end.


-spec remove_deletion_marker(file_ctx:ctx(), user_ctx:ctx()) -> file_ctx:ctx().
remove_deletion_marker(FileCtx, UserCtx) ->
    case file_ctx:is_imported_storage(FileCtx) of
        {true, FileCtx2} ->
            {ParentGuid, FileCtx3} = file_ctx:get_parent_guid(FileCtx2, UserCtx),
            ParentUuid = file_id:guid_to_uuid(ParentGuid),
            deletion_marker:remove(ParentUuid, FileCtx3);
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
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    Scope = file_ctx:get_space_id_const(FileCtx),
    {FileName, FileCtx3} = file_ctx:get_aliased_name(FileCtx, UserCtx),
    {ParentGuid, FileCtx4} = file_ctx:get_parent_guid(FileCtx3, UserCtx),
    ParentUuid = file_id:guid_to_uuid(ParentGuid),
    ok = file_meta_links:delete(ParentUuid, Scope, FileName, FileUuid),
    FileCtx4.


-spec delete_storage_file_error_msg(file_ctx:ctx(), term()) -> string().
delete_storage_file_error_msg(FileCtx, Error) ->
    {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    {StorageId, FileCtx3} = file_ctx:get_storage_id(FileCtx2),
    FileGuid = file_ctx:get_guid_const(FileCtx3),
    str_utils:format("Deleting file ~s on storage ~s with guid ~s failed due to ~p.", [
        StorageFileId, StorageId, FileGuid, Error
    ]).


-spec delete_file_meta(file_ctx:ctx()) -> file_ctx:ctx().
delete_file_meta(FileCtx) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    ok = file_meta:delete(FileDoc),
    FileCtx2.


-spec delete_shares_and_update_parent_timestamps(user_ctx:ctx(), file_ctx:ctx()) -> file_ctx:ctx().
delete_shares_and_update_parent_timestamps(UserCtx, FileCtx) ->
    try
        FileCtx2 = delete_shares(UserCtx, FileCtx),
        {ParentCtx, FileCtx3} = file_ctx:get_parent(FileCtx2, UserCtx),
        fslogic_times:update_mtime_ctime(ParentCtx),
        FileCtx3
    catch
        error:{badmatch, {error, not_found}} ->
            FileCtx
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes shares from oz and db.
%% @end
%%--------------------------------------------------------------------
-spec delete_shares(user_ctx:ctx(), file_ctx:ctx()) -> file_ctx:ctx().
delete_shares(UserCtx, FileCtx) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    Shares = file_meta:get_shares(FileDoc),
    SessionId = user_ctx:get_session_id(UserCtx),
    [ok = share_logic:delete(SessionId, ShareId) || ShareId <- Shares],
    FileCtx2.


-spec maybe_delete_storage_sync_info(file_ctx:ctx(), StorageFileDeleted :: boolean()) -> file_ctx:ctx().
maybe_delete_storage_sync_info(FileCtx, false) ->
    FileCtx;
maybe_delete_storage_sync_info(FileCtx, true) ->
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
        _ -> {?MARKER_HANDLING_METHOD, FileCtx2}
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
    {ok, StorageId} = space_logic:get_local_storage_id(SpaceId),
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
-spec remove_associated_documents(file_ctx:ctx(), boolean()) -> ok.
remove_associated_documents(FileCtx, StorageFileDeleted) ->
    remove_synced_associated_documents(FileCtx),
    remove_local_associated_documents(FileCtx, StorageFileDeleted).


-spec remove_synced_associated_documents(file_ctx:ctx()) -> ok.
remove_synced_associated_documents(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    ok = custom_metadata:delete(FileUuid),
    ok = times:delete(FileUuid),
    ok = transferred_file:clean_up(FileGuid),
    ok = file_qos:delete_associated_entries(FileUuid).


-spec remove_local_associated_documents(file_ctx:ctx(), boolean()) -> ok.
remove_local_associated_documents(FileCtx, StorageFileDeleted) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    maybe_delete_storage_sync_info(FileCtx, StorageFileDeleted),
    ok = file_qos:clean_up(FileCtx),
    ok = file_meta_posthooks:delete(FileUuid),
    ok = file_popularity:delete(FileUuid).


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


-spec delete_location(file_ctx:ctx()) -> file_ctx:ctx().
delete_location(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
    case IsDir of
        true ->
            case dir_location:delete(FileUuid) of
                ok -> ok;
                {error, not_found} -> ok
            end;
        false ->
            fslogic_location_cache:force_flush(FileUuid),
            fslogic_location_cache:clear_local_blocks(FileCtx),
            ok = fslogic_location_cache:delete_local_location(FileUuid)
    end,
    FileCtx2.