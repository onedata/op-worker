%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for syncing a single file found on storage
%%% to Onedata filesystem.
%%% For more info please see doc of process_file/2 function.
%%% TODO VFS-6508 abstract mechanisms used by sync and file_registration to a separate module
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_engine).
-author("Jakub Kudzia").

-include("modules/storage/import/storage_import.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("global_definitions.hrl").
-include("modules/storage/traverse/storage_traverse.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([find_direct_parent_and_sync_file/2, sync_file/2, create_file_meta_and_handle_conflicts/6]).

% exported for mocking in CT tests
-export([import_file_unsafe/2, check_location_and_maybe_sync/3]).

-define(FILE_LOCATION_ATTR_NAME, file_location).
-define(MODE_ATTR_NAME, mode).
-define(TIMESTAMPS_ATTR_NAME, timestamps).
-define(OWNER_ATTR_NAME, owner).
-define(NFS4_ACL_ATTR_NAME, nfs4_acl).

-type result() :: ?FILE_CREATED | ?FILE_MODIFIED | ?FILE_UNMODIFIED | ?FILE_PROCESSING_FAILED.
%% @formatter:off
-type file_attr_name() :: ?FILE_LOCATION_ATTR_NAME | ?MODE_ATTR_NAME | ?TIMESTAMPS_ATTR_NAME |
                          ?OWNER_ATTR_NAME | ?NFS4_ACL_ATTR_NAME.
%% @formatter:on
-type info() :: storage_sync_traverse:info().


-export_type([result/0, file_attr_name/0]).

-define(CREATE_MISSING_PARENT_CRITICAL_SECTION(ParentUUid, MissingParentName, Function),
    critical_section:run({create_missing_parent, ParentUuid, MissingParentName}, Function)).
-define(REIMPORT_WITH_MISSING_LINK, op_worker:get_env(reimport_with_missing_link, first_scan_only)).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Processes storage file associated with StorageFileCtx.
%% It returns the tuple {Result, FileCtx, StorageFileCtx} where:
%%    * Result is one of:
%%      ** ?IMPORTED - which means that file was imported to the Onedata file-system
%%      ** ?UPDATED  - which means that file had already existed in the Onedata file system
%%                   and that update of its attributes on storage was detected and reflected
%%                   in the Onedata file-system attributes
%%      ** ?PROCESSED - which means that file has been successfully processed but
%%                     no changes were introduced to the Onedata file system.
%%                     This might have happened if file hadn't been changed since last scan or if
%%                     it was being deleted or if it was being replicated in the very moment of scan.
%%                     and that it hasn't changed on storage since last scan
%%      ** ?FILE_PROCESSING_FAILED - which means that an error occurred during processing the file
%%    * FileCtx is a:
%%      ** file_ctx:ctx() - the file in the Onedata file system which is associated
%%         with the file referenced by StorageFileCtx.
%%         This happens when the Result is one of imported/updated/processed (no changes).
%%      ** undefined - if the Result was one of processed/failed
%%    * StorageFileCtx which is updated StorageFileCtx passed to the function.
%% @end
%%--------------------------------------------------------------------
-spec find_direct_parent_and_sync_file(storage_file_ctx:ctx(), info()) ->
    {result(), file_ctx:ctx() | undefined, storage_file_ctx:ctx()} | {error, term()}.
find_direct_parent_and_sync_file(StorageFileCtx, Info) ->
    case find_direct_parent_and_ensure_all_parents_exist(StorageFileCtx, Info) of
        {ok, Info2} ->
            sync_file(StorageFileCtx, Info2);
        {error, ?ENOENT} ->
            {?FILE_UNMODIFIED, undefined, StorageFileCtx}
    end.

-spec sync_file(storage_file_ctx:ctx(), info()) -> {result(),
    file_ctx:ctx() | undefined, storage_file_ctx:ctx()} | {error, term()}.
sync_file(StorageFileCtx, Info = #{parent_ctx := ParentCtx}) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    FileName = storage_file_ctx:get_file_name_const(StorageFileCtx),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    SpaceCtx = file_ctx:new_by_guid(SpaceGuid),
    ParentUuid = file_ctx:get_logical_uuid_const(ParentCtx),
    IsManualImport = maps:get(manual, Info, false),
    case file_ctx:is_root_dir_const(ParentCtx) of
        true ->
            check_file_meta_and_maybe_sync(StorageFileCtx, SpaceCtx, Info, true);
        false ->
            % check whether FileName of processed file is in the form
            % FileName = <<FileBaseName, ?CONFLICTING_STORAGE_FILE_SUFFIX_SEPARATOR, FileUuid>>
            % such files are created by the system if it's impossible to create file FileBaseName on storage
            % i.e. when file with the same name was deleted but there is still an open handle to it
            {HasSuffix, FileUuid, FileBaseName} = case is_suffixed(FileName) of
                {true, StorageUuid, StorageBaseName} -> {true, StorageUuid, StorageBaseName};
                false -> {false, undefined, FileName}
            end,

            case map_to_existing_file_uuid(ParentUuid, FileName, FileBaseName, FileUuid) of
                {error, Reason} ->
                    % Link from Parent to FileBaseName is missing.
                    % We must check deletion marker to ensure that file may be synced.
                    % Deletion markers are removed if and only if file was successfully deleted from storage.
                    case deletion_marker:check(ParentUuid, FileName) of
                        {error, not_found} when HasSuffix =:= false ->
                            % We must ensure whether file is still on storage at the very moment
                            % to avoid stat/delete race.
                            % Race could happen if previous stat was performed before file was
                            % deleted from the system and if links (and file) were deleted
                            % before we checked the links.
                            % maybe_import_file/2 will perform the check.
                            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
                            SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
                            case IsManualImport of
                                true ->
                                    maybe_import_file(StorageFileCtx, Info);
                                false ->
                                    case storage_sync_info:get(StorageFileId, SpaceId) of
                                        {error, not_found} ->
                                            maybe_import_file(StorageFileCtx, Info);
                                        {ok, SSIDoc} ->
                                            case {storage_sync_info:get_guid(SSIDoc), Reason} of
                                                {undefined, _} ->
                                                    maybe_import_file(StorageFileCtx, Info);
                                                {_Guid, not_found} ->
                                                    case {?REIMPORT_WITH_MISSING_LINK, Info} of
                                                        {true, _} ->
                                                            maybe_import_file(StorageFileCtx, Info);
                                                        {first_scan_only, #{scan_num := 1}} ->
                                                            maybe_import_file(StorageFileCtx, Info);
                                                        _ ->
                                                            {?FILE_UNMODIFIED, undefined, StorageFileCtx}
                                                    end;
                                                {Guid, {conflicting_uuids, ConflictingUuids}} ->
                                                    % Check if storage_sync_info points to other conflicting file
                                                    case lists:member(file_id:guid_to_uuid(Guid), ConflictingUuids) of
                                                        true -> maybe_import_file(StorageFileCtx, Info);
                                                        false -> {?FILE_UNMODIFIED, undefined, StorageFileCtx}
                                                    end
                                            end
                                    end
                            end;
                        {error, not_found} ->
                            % This file was created as a result of access via uuid to remotely created
                            % (link is not synchronized yet)
                            {?FILE_UNMODIFIED, undefined, StorageFileCtx};
                        {ok, _FileUuid} ->
                            % deletion marker exists, it means that deletion of the file has been scheduled
                            % we may ignore this file
                            {?FILE_UNMODIFIED, undefined, StorageFileCtx}
                    end;
                {ok, ResolvedUuid} ->
                    case deletion_marker:check(ParentUuid, FileName) of
                        {error, not_found} ->
                            FileCtx = file_ctx:new_by_uuid(ResolvedUuid, SpaceId),
                            % call by module to mock in tests
                            storage_import_engine:check_location_and_maybe_sync(StorageFileCtx, FileCtx, Info);
                        {ok, _} ->
                            {?FILE_UNMODIFIED, undefined, StorageFileCtx}
                    end
            end
    end.


-spec map_to_existing_file_uuid(file_meta:uuid(), file_meta:name(), file_meta:name(), file_meta:uuid() | undefined) ->
    {ok, file_meta:uuid()} | {error, not_found | {conflicting_uuids, [file_meta:uuid()]}}.
map_to_existing_file_uuid(ParentUuid, FileName, FileBaseName, undefined) ->
    case file_meta:get_matching_child_uuids_with_tree_ids(ParentUuid, all, FileBaseName) of
        {ok, UuidsWithTreeIds} ->
            Filtered = lists:filter(fun({FileUuid, _}) ->
                % NOTE: both file_location and dir_location have to be analyzed regardless of file's type on storage
                % as its type may have been changed between scans (file may have been deleted and replaced with dir
                % or vice versa)
                case file_location:get_local(FileUuid) of
                    {ok, #document{value = #file_location{
                        storage_file_created = true,
                        file_id = FileId
                    }}} ->
                        binary:longest_common_suffix([FileId, FileName]) =:= size(FileName);
                    (_) ->
                        case dir_location:get(FileUuid) of
                            {ok, #document{value = #dir_location{
                                storage_file_created = true,
                                storage_file_id = FileId
                            }}} ->
                                binary:longest_common_suffix([FileId, FileName]) =:= size(FileName);
                            (_) ->
                                false
                        end
                end
            end, UuidsWithTreeIds),
            case Filtered of
                [{FileUuid, _}] -> {ok, FileUuid};
                _ -> {error, {conflicting_uuids, lists:map(fun({FileUuid, _}) -> FileUuid end, UuidsWithTreeIds)}}
            end;
        {error, not_found} ->
            {error, not_found}
    end;
map_to_existing_file_uuid(ParentUuid, _FileName, FileBaseName, ExpectedFileUuid) ->
    case file_meta:get_matching_child_uuids_with_tree_ids(ParentUuid, all, FileBaseName) of
        {ok, UuidsWithTreeIds} ->
            case lists:any(fun({FileUuid, _}) -> FileUuid =:= ExpectedFileUuid end, UuidsWithTreeIds) of
                true -> {ok, ExpectedFileUuid};
                false -> {error, {conflicting_uuids, lists:map(fun({FileUuid, _}) -> FileUuid end, UuidsWithTreeIds)}}
            end;
        {error, not_found} ->
            {error, not_found}
    end.


-spec create_file_meta_and_handle_conflicts(file_meta:uuid(), file_meta:name(), file_meta:mode(), od_user:id(), file_meta:uuid(),
    od_space:id()) -> {ok, file_ctx:ctx()} | {error, term()}.
create_file_meta_and_handle_conflicts(FileUuid, FileName, Mode, OwnerId, ParentUuid, SpaceId) ->
    create_file_meta_and_handle_conflicts(FileUuid, FileName, Mode, OwnerId, ParentUuid, SpaceId, #{}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec find_direct_parent_and_ensure_all_parents_exist(storage_file_ctx:ctx(), info()) -> {ok, info()} | {error, term()}.
find_direct_parent_and_ensure_all_parents_exist(StorageFileCtx, Info = #{space_storage_file_id := SpaceStorageFileId}) ->
    case storage_file_ctx:get_storage_file_id_const(StorageFileCtx) =:= SpaceStorageFileId of
        true ->
            {ok, Info};
        false ->
            SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
            SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
            ParentCtx = file_ctx:new_by_guid(SpaceGuid),
            % ParentCtx may not be associated with direct parent of the file.
            % This is caused by the fact that on object storages, file structure is flat
            % and all files are "direct" children of the space directory.
            {ParentStorageFileId, ParentCtx2} = file_ctx:get_storage_file_id(ParentCtx),
            ParentStorageFileIdTokens = filepath_utils:split(ParentStorageFileId),
            % Path to the direct parent of the child can be acquired from the file's path.
            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
            DirectParentStorageFileId = filename:dirname(StorageFileId),
            DirectParentStorageFileIdTokens = filepath_utils:split(DirectParentStorageFileId),
            % compare tokens of both parents' paths
            MissingParentTokens = DirectParentStorageFileIdTokens -- ParentStorageFileIdTokens,
            SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
            StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
            % do not perform stat on storage as the directory does no exist on storage
            ParentStorageFileCtx = flat_storage_iterator:get_virtual_directory_ctx(ParentStorageFileId, SpaceId, StorageId),
            Info2 = Info#{parent_ctx => ParentCtx2},
            ensure_all_parents_exist_and_are_dirs(ParentStorageFileCtx, Info2, MissingParentTokens)
    end.

-spec ensure_all_parents_exist_and_are_dirs(storage_file_ctx:ctx(), info(), [helpers:file_id()]) -> {ok, info()} | {error, term()}.
ensure_all_parents_exist_and_are_dirs(_ParentStorageFileCtx, Info, []) ->
    {ok, Info};
ensure_all_parents_exist_and_are_dirs(ParentStorageFileCtx, Info, [MissingParentName | Rest]) ->
    ParentStorageFileId = storage_file_ctx:get_storage_file_id_const(ParentStorageFileCtx),
    SpaceId = storage_file_ctx:get_space_id_const(ParentStorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(ParentStorageFileCtx),
    MissingParentStorageFileId = filename:join([ParentStorageFileId, MissingParentName]),
    % do not perform stat on storage as the directory does no exist on storage
    MissingParentStorageCtx = flat_storage_iterator:get_virtual_directory_ctx(MissingParentStorageFileId, SpaceId,
        StorageId),
    case ensure_parent_exist_and_is_dir(MissingParentName, MissingParentStorageCtx, Info) of
        undefined ->
            {error, ?ENOENT};
        MissingParentCtx ->
            ensure_all_parents_exist_and_are_dirs(MissingParentStorageCtx, Info#{parent_ctx => MissingParentCtx}, Rest)
    end.

-spec ensure_parent_exist_and_is_dir(file_meta:name(), storage_file_ctx:ctx(), info()) -> file_ctx:ctx() | undefined.
ensure_parent_exist_and_is_dir(MissingParentName, MissingParentStorageCtx, Info) ->
    ensure_parent_exist_and_is_dir(MissingParentName, MissingParentStorageCtx, Info, false).


-spec ensure_parent_exist_and_is_dir(file_meta:name(), storage_file_ctx:ctx(), info(), InCriticalSection :: boolean()) ->
    file_ctx:ctx() | undefined.
ensure_parent_exist_and_is_dir(MissingParentName, MissingParentStorageCtx, Info, false) ->
    ParentCtx = maps:get(parent_ctx, Info),
    case get_child_safe(ParentCtx, MissingParentName) of
        {ok, MissingParentCtx} ->
            % MissingParentName is child of ParentCtx
            % we must ensure whether it is a directory
            case file_ctx:is_dir(MissingParentCtx) of
                {true, MissingParentCtx2} ->
                    MissingParentCtx2;
                {false, _} ->
                    % MissingParent is not a directory, it means that regular file was deleted and directory with the
                    % same name was created on storage. We muse delete stalled file.
                    ensure_parent_exist_and_is_dir(MissingParentName, MissingParentStorageCtx, Info, true)
            end;
        {error, ?ENOENT} ->
            ensure_missing_parent_exist(MissingParentName, MissingParentStorageCtx, Info)
    end;
ensure_parent_exist_and_is_dir(MissingParentName, MissingParentStorageCtx, Info, true) ->
    ParentCtx = maps:get(parent_ctx, Info),
    ParentUuid = file_ctx:get_logical_uuid_const(ParentCtx),
    ?CREATE_MISSING_PARENT_CRITICAL_SECTION(ParentUuid, MissingParentName, fun() ->
        % check whether directory was not created by other process before entering critical section
        case get_child_safe(ParentCtx, MissingParentName) of
            {ok, MissingParentCtx2} ->
                case file_ctx:is_dir(MissingParentCtx2) of
                    {true, MissingParentCtx3} ->
                        MissingParentCtx3;
                    {false, _} ->
                        % if it's not a directory first delete stalled file, and create missing parent
                        {?FILE_CREATED, MissingParentCtx4} =
                            delete_stalled_file_and_create_missing_parent(MissingParentStorageCtx,
                                MissingParentCtx2, Info),
                            MissingParentCtx4
                end;
            {error, ?ENOENT} ->
                undefined
        end
    end).


-spec ensure_missing_parent_exist(file_meta:name(), storage_file_ctx:ctx(), info()) -> file_ctx:ctx() | undefined.
ensure_missing_parent_exist(MissingParentName, MissingParentStorageCtx, Info = #{parent_ctx := ParentCtx}) ->
    ParentUuid = file_ctx:get_logical_uuid_const(ParentCtx),
    % TODO VFS-5881 get rid of this critical section
    % this critical section is to avoid race on creating missing parent by call to import_file function
    % in case of simultaneous creation of missing parent file_meta, one of syncing processes
    % would get {error, already_exists} which is handled in import_file as a race between syncing process
    % and creating a file via lfm. As a result there will be 2 parents created, one with suffix for
    % conflicted files. Critical section is used to avoid this situation.
    ?CREATE_MISSING_PARENT_CRITICAL_SECTION(ParentUuid, MissingParentName,
        fun() ->
            case get_child_safe(ParentCtx, MissingParentName) of
                {ok, MissingParentCtx} -> MissingParentCtx;
                {error, ?ENOENT} ->
                    case create_missing_parent(MissingParentStorageCtx, Info) of
                        {?FILE_CREATED, MissingParentCtx} -> MissingParentCtx;
                        {error, ?ENOENT} -> undefined
                    end
            end
        end
    ).

-spec get_child_safe(file_ctx:ctx(), file_meta:name()) -> {ok, file_ctx:ctx()} | {error, term()}.
get_child_safe(FileCtx, ChildName) ->
    try
        {ChildCtx, _} = file_tree:get_child(FileCtx, ChildName, user_ctx:new(?ROOT_SESS_ID)),
        {ok, ChildCtx}
    catch
        error:{badmatch,{error,not_found}} ->
            {error, ?ENOENT};
        throw:?ENOENT ->
            {error, ?ENOENT}
    end.


-spec check_location_and_maybe_sync(storage_file_ctx:ctx(), file_ctx:ctx(), info()) ->
    {result(), file_ctx:ctx() | undefined, storage_file_ctx:ctx()} | {error, term()}.
check_location_and_maybe_sync(StorageFileCtx, FileCtx, Info) ->
    {InferTypeAns, StorageFileCtx2} = get_file_type(StorageFileCtx),
    case InferTypeAns of
        {ok, ?DIRECTORY_TYPE} ->
            check_dir_location_and_maybe_sync(StorageFileCtx2, FileCtx, Info);
        {ok, ?REGULAR_FILE_TYPE} ->
            check_file_location_and_maybe_sync(StorageFileCtx2, FileCtx, Info);
        ?ERROR_NOT_SUPPORTED ->
            {?FILE_UNMODIFIED, undefined, StorageFileCtx2}
    end.


-spec check_dir_location_and_maybe_sync(storage_file_ctx:ctx(), file_ctx:ctx(), info()) ->
    {result(), file_ctx:ctx() | undefined, storage_file_ctx:ctx()} | {error, term()}.
check_dir_location_and_maybe_sync(StorageFileCtx, FileCtx, Info) ->
    check_dir_location_and_maybe_sync(StorageFileCtx, FileCtx, Info, false).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function checks dir_location associated with passed FileCtx
%% to determine whether the file can by synchronised.
%% CheckType flag determines whether file_location associated with
%% given FileCtx has already been checked.
%% If StorageFileIsRegularFile == true file_location has been checked and
%% has not been found.
%% else file_location has not been checked yet.
%% @end
%%-------------------------------------------------------------------
-spec check_dir_location_and_maybe_sync(storage_file_ctx:ctx(), file_ctx:ctx(), 
    info(), boolean()) -> {result(), file_ctx:ctx() | undefined, storage_file_ctx:ctx()} | {error, term()}.
check_dir_location_and_maybe_sync(StorageFileCtx, FileCtx, Info, StorageFileIsRegularFile) ->
    StorageFileCreated = case file_ctx:get_dir_location_doc_const(FileCtx) of
        undefined -> false;
        DirLocation -> dir_location:is_storage_file_created(DirLocation)
    end,
    case  StorageFileCreated or StorageFileIsRegularFile of
        true ->
            check_file_meta_and_maybe_sync(StorageFileCtx, FileCtx, Info, StorageFileCreated);
        false ->
            % dir_location does not exist, check whether file_location exist
            % as file with the same name may had been deleted and
            % directory with the same name created
            check_file_location_and_maybe_sync(StorageFileCtx, FileCtx, Info, true)
    end.


-spec check_file_location_and_maybe_sync(storage_file_ctx:ctx(), file_ctx:ctx(), info()) ->
    {result(), file_ctx:ctx() | undefined, storage_file_ctx:ctx()} | {error, term()}.
check_file_location_and_maybe_sync(Job, FileCtx, Info) ->
    check_file_location_and_maybe_sync(Job, FileCtx, Info, false).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function analyses file_location associated with passed FileCtx
%% to determine whether the file can by synchronised, and if so whether
%% it should be imported or updated.
%% StorageFileIsDir flag determines whether dir_location associated with
%% given FileCtx has already been checked.
%% If StorageFileIsDir == true dir_location has been checked and has not been found.
%% else dir_location has not been checked yet.
%% @end
%%-------------------------------------------------------------------
-spec check_file_location_and_maybe_sync(storage_file_ctx:ctx(), file_ctx:ctx(), info(), boolean()) ->
    {result(), file_ctx:ctx() | undefined, storage_file_ctx:ctx()} | {error, term()}.
check_file_location_and_maybe_sync(StorageFileCtx, FileCtx, Info, StorageFileIsDir) ->
    % Get only two blocks - it is enough to verify if file can be imported
    case file_ctx:get_local_file_location_doc_const(FileCtx, {blocks_num, 2}) of
        FLDoc = #document{
            value = #file_location{
                file_id = FileId,
                rename_src_file_id = RenameSrcFileId,
                size = Size
            }} ->
            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
            case {FileId =:= StorageFileId, RenameSrcFileId =:= StorageFileId} of
                {_, true} ->
                    % file is being renamed at the moment, ignore it
                    {?FILE_UNMODIFIED, FileCtx, StorageFileCtx};
                {true, false} ->
                    case fslogic_location_cache:get_blocks(FLDoc, #{count => 2}) of
                        [#file_block{offset = 0, size = Size}] ->
                            check_file_meta_and_maybe_sync(StorageFileCtx, FileCtx, Info, true);
                        [] when Size =:= 0 ->
                            check_file_meta_and_maybe_sync(StorageFileCtx, FileCtx, Info, true);
                        _ when StorageFileIsDir ->
                            % file must have been deleted and directory
                            % with the same name recreated
                            check_file_meta_and_maybe_sync(StorageFileCtx, FileCtx, Info, true);
                        _ ->
                            % file is not fully replicated (not in one block), ignore it
                            {?FILE_UNMODIFIED, FileCtx, StorageFileCtx}
                    end;
                {false, false} ->
                    % This may happen in 2 cases:
                    %   * when there was a conflict between creation of file on storage and by remote provider
                    %     in such case, if file was replicated from the remote provider
                    %     it must have been created on storage with a suffix and therefore file ids do not match.
                    %   * when file has been moved because it was deleted and still opened
                    %     in such case, its file_id in file_location has been changed
                    %     such file should be ignored
                    % To determine which case it is, maybe_import_file will check whether file is still on storage.
                    maybe_import_file(StorageFileCtx, Info)
            end;
        undefined ->
            % This may happen in the following cases:
            %  * File has just been deleted by lfm, in such case it won't be imported as
            %    maybe_import_file checks whether file is still on storage.
            %  * There was a conflict between creation of file on storage and by remote provider.
            %    Links has been synchronized so we have uuid, but file_location has not been synchronized yet.
            %    We may import this file with a IMPORTED suffix.
            %  * Directory with the same name as given file was deleted on storage, and the file was created between
            %    consecutive scans.
            case StorageFileIsDir of
                true ->
                    % dir_location was not found and file_location either
                    check_file_meta_and_maybe_sync(StorageFileCtx, FileCtx, Info, false);
                false ->
                    % Check whether dir_location exists for this file to determine whether
                    % it's 3rd of the above cases.
                    check_dir_location_and_maybe_sync(StorageFileCtx, FileCtx, Info, true)
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if file (which metadata exists in onedata) is synchronized.
%% @end
%%--------------------------------------------------------------------
-spec check_file_meta_and_maybe_sync(storage_file_ctx:ctx(), file_ctx:ctx(), info(), boolean()) ->
    {result(), file_ctx:ctx() | undefined, storage_file_ctx:ctx()} | {error, term()}.
check_file_meta_and_maybe_sync(StorageFileCtx, FileCtx, Info, StorageFileCreated) ->
    try
        case get_attr_including_deleted(FileCtx) of
            {ok, #file_attr{is_deleted = true} = _FileAttr} ->
                {?FILE_UNMODIFIED, undefined, StorageFileCtx};
            {ok, #file_attr{is_deleted = false} = FileAttr} ->
                check_file_type_and_maybe_sync(StorageFileCtx, FileAttr, FileCtx, Info, StorageFileCreated);
            {error, ?ENOENT} ->
                maybe_import_file(StorageFileCtx, Info)
        end
    catch
        throw:?ENOENT ->
            {error, ?ENOENT}
    end.

-spec check_file_type_and_maybe_sync(storage_file_ctx:ctx(), #file_attr{}, file_ctx:ctx(), info(),
    boolean()) -> {result(), file_ctx:ctx() | undefined, storage_file_ctx:ctx()} | {error, term()}.
check_file_type_and_maybe_sync(StorageFileCtx, FileAttr = #file_attr{type = FileMetaType}, FileCtx, Info, StorageFileCreated) ->
    {InferTypeAns, StorageFileCtx2} = get_file_type(StorageFileCtx),
    case InferTypeAns of
        {ok, StorageFileType} ->
            case {StorageFileType, FileMetaType, StorageFileCreated} of
                {Type, Type, true} ->
                    maybe_update_file(StorageFileCtx2, FileAttr, FileCtx, Info);
                {_Type, _OtherType, true} ->
                    import_file_recreated_with_different_type(StorageFileCtx2, FileCtx, Info);
                {?REGULAR_FILE_TYPE, ?REGULAR_FILE_TYPE, false} ->
                    maybe_import_file(StorageFileCtx2, Info);
                {?DIRECTORY_TYPE, ?DIRECTORY_TYPE, false} ->
                    maybe_update_file(StorageFileCtx2, FileAttr, FileCtx, Info);
                {?REGULAR_FILE_TYPE, ?DIRECTORY_TYPE, false} ->
                    maybe_import_file(StorageFileCtx2, Info);
                {?DIRECTORY_TYPE, ?REGULAR_FILE_TYPE, false} ->
                    {?FILE_UNMODIFIED, undefined, StorageFileCtx2}
            end;
        ?ERROR_NOT_SUPPORTED ->
            {?FILE_UNMODIFIED, undefined, StorageFileCtx2}
    end.

-spec import_file_recreated_with_different_type(storage_file_ctx:ctx(), file_ctx:ctx(), info()) ->
    {result(), file_ctx:ctx() | undefined, storage_file_ctx:ctx()}.
import_file_recreated_with_different_type(StorageFileCtx, FileCtx, Info) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    storage_import_monitoring:increment_queue_length_histograms(SpaceId, 1),
    storage_import_deletion:delete_file_and_update_counters(FileCtx, SpaceId, StorageId),
    maybe_import_file(StorageFileCtx, Info).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This functions is used to create missing parent on object storages.
%% It's used when import detected that regular file was deleted from
%% storage and directory was created with the same name.
%% It first deletes the stalled file and then creates metadata for
%% directory.
%% @end
%%-------------------------------------------------------------------
-spec delete_stalled_file_and_create_missing_parent(storage_file_ctx:ctx(), file_ctx:ctx(), info()) ->
    {result(), file_ctx:ctx() | undefined}.
delete_stalled_file_and_create_missing_parent(StorageFileCtx, FileCtx, Info) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    storage_import_monitoring:increment_queue_length_histograms(SpaceId, 1),
    storage_import_deletion:delete_file_and_update_counters(FileCtx, SpaceId, StorageId),
    create_missing_parent(StorageFileCtx, Info).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This functions import the file, if it hasn't been synchronized yet.
%% It checks whether file that is to be imported is still visible on
%% the storage.
%% @end
%%-------------------------------------------------------------------
-spec maybe_import_file(storage_file_ctx:ctx(), info()) ->
    {result(), file_ctx:ctx() | undefined, storage_file_ctx:ctx()} | {error, term()}.
maybe_import_file(StorageFileCtx, Info) ->
    SDHandle = storage_file_ctx:get_handle_const(StorageFileCtx),
    % We must ensure that there was no race with deleting file.
    % We check whether file that we found on storage and that we want to import
    % is not associated with file that has been deleted from the system.
    VerifyExistence = maps:get(verify_existence, Info, true),
    case VerifyExistence of
        true ->
            case storage_driver:exists(SDHandle) of
                true -> import_file(StorageFileCtx, Info);
                false -> {?FILE_UNMODIFIED, undefined, StorageFileCtx}
            end;
        false ->
            import_file(StorageFileCtx, Info)
    end.

-spec import_file(storage_file_ctx:ctx(), info()) ->
    {result(), file_ctx:ctx(), storage_file_ctx:ctx()} | {error, term()}.
import_file(StorageFileCtx, Info = #{parent_ctx := ParentCtx}) ->
    try
        storage_import_engine:import_file_unsafe(StorageFileCtx, Info)
    catch
        throw:?ENOENT ->
            rollback_file_creation(ParentCtx, StorageFileCtx),
            {error, ?ENOENT};
        Error:Reason:Stacktrace ->
            FileName = storage_file_ctx:get_file_name_const(StorageFileCtx),
            SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
            StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
            ?error_stacktrace("importing file ~p on storage ~p in space ~p failed due to ~w:~w",
                [FileName, StorageId, SpaceId, Error, Reason], Stacktrace),
            rollback_file_creation(ParentCtx, StorageFileCtx),
            {error, Reason}
    end.


-spec create_missing_parent(storage_file_ctx:ctx(), info()) ->
    {result(), file_ctx:ctx()} | {error, term()}.
create_missing_parent(StorageFileCtx, Info = #{parent_ctx := ParentCtx}) ->
    try
        create_missing_parent_unsafe(StorageFileCtx, Info)
    catch
        throw:?ENOENT ->
            rollback_file_creation(ParentCtx, StorageFileCtx),
            {error, ?ENOENT};
        Error:Reason:Stacktrace ->
            FileName = storage_file_ctx:get_file_name_const(StorageFileCtx),
            SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
            StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
            ?error_stacktrace("importing file ~p on storage ~p in space ~p failed due to ~w:~w",
                [FileName, StorageId, SpaceId, Error, Reason], Stacktrace),
            rollback_file_creation(ParentCtx, StorageFileCtx),
            {error, Reason}
    end.


-spec rollback_file_creation(file_ctx:ctx(), storage_file_ctx:ctx()) -> ok.
rollback_file_creation(ParentCtx, StorageFileCtx) ->
    % importing file has crashed, perform best effort cleanup
    % try to delete both normal as also as conflicting file name
    FileName = storage_file_ctx:get_file_name_const(StorageFileCtx),
    FileName2 = ?IMPORTED_CONFLICTING_FILE_NAME(FileName),
    try_to_delete_file(ParentCtx, FileName),
    try_to_delete_file(ParentCtx, FileName2).


-spec try_to_delete_file(file_ctx:ctx(), file_meta:name()) -> ok.
try_to_delete_file(ParentCtx, ChildName) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    try
        {FileCtx, _} = file_tree:get_child(ParentCtx, ChildName, UserCtx),
        fslogic_delete:handle_file_deleted_on_imported_storage(FileCtx)
    catch
        throw:?ENOENT ->
            ParentUuid = file_ctx:get_logical_uuid_const(ParentCtx),
            SpaceId = file_ctx:get_space_id_const(ParentCtx),
            case canonical_path:to_uuid(ParentUuid, ChildName) of
                {ok, FileUuid} ->
                    file_meta_forest:delete(ParentUuid, SpaceId, ChildName, FileUuid);
                {error, not_found} ->
                    ok
            end
    end.

-spec import_file_unsafe(storage_file_ctx:ctx(), info()) ->
    {result(), file_ctx:ctx(), storage_file_ctx:ctx()}.
import_file_unsafe(StorageFileCtx, Info = #{parent_ctx := ParentCtx}) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    {OwnerId, StorageFileCtx2} = get_owner_id(StorageFileCtx),
    ParentUuid = file_ctx:get_logical_uuid_const(ParentCtx),
    FileUuid = datastore_key:new(),
    case create_location(FileUuid, StorageFileCtx2, OwnerId) of
        {ok, StorageFileCtx3} ->
            FileName = storage_file_ctx:get_file_name_const(StorageFileCtx3),
            {#statbuf{st_mode = Mode} = StatBuf, StorageFileCtx4} = storage_file_ctx:stat(StorageFileCtx3),
            {ok, FileCtx} = create_file_meta_and_handle_conflicts(FileUuid, FileName, Mode, OwnerId,
                ParentUuid, SpaceId, Info),
            % Size could not be updated in statistic as file_meta was created after file_location.
            % As a result file_meta_posthooks have been created during file_location creation - execute them now.
            file_meta_posthooks:execute_hooks(FileUuid, doc),
            {ok, StorageFileCtx5} = create_times_from_stat_timestamps(FileUuid, StorageFileCtx4),
            ParentGuid = file_ctx:get_logical_guid_const(ParentCtx),
            {ok, FileType} = storage_driver:infer_type(Mode),
            dir_size_stats:report_file_created(FileType, ParentGuid),
            dir_update_time_stats:report_update_of_dir(ParentGuid, StatBuf),
            {ok, StorageFileCtx6} = maybe_import_nfs4_acl(FileCtx, StorageFileCtx5, Info),
            {CanonicalPath, FileCtx2} = file_ctx:get_canonical_path(FileCtx),
            SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
            storage_import_logger:log_creation(StorageFileId, CanonicalPath, FileUuid, SpaceId),
            {?FILE_CREATED, FileCtx2, StorageFileCtx6};
        {?ERROR_NOT_SUPPORTED, StorageFileCtx3} ->
            {?FILE_UNMODIFIED, undefined, StorageFileCtx3}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This functions creates metadata for missing parent of file on
%% object storage.
%% @end
%%-------------------------------------------------------------------
-spec create_missing_parent_unsafe(storage_file_ctx:ctx(), info()) ->
    {result(), file_ctx:ctx()}.
create_missing_parent_unsafe(StorageFileCtx, #{parent_ctx := ParentCtx}) ->
    ParentName = storage_file_ctx:get_file_name_const(StorageFileCtx),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    {ok, FileCtx} = file_registration:create_missing_directory(ParentCtx, ParentName, ?SPACE_OWNER_ID(SpaceId)),
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    set_times_for_dir_using_current_time(FileUuid, SpaceId),
    dir_size_stats:report_file_created(?DIRECTORY_TYPE, file_ctx:get_logical_guid_const(FileCtx)),
    {CanonicalPath, FileCtx2} = file_ctx:get_canonical_path(FileCtx),
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    storage_import_logger:log_creation(StorageFileId, CanonicalPath, FileUuid, SpaceId),
    {?FILE_CREATED, FileCtx2}.


-spec create_location(file_meta:uuid(), storage_file_ctx:ctx(), od_user:id()) ->
    {ok | ?ERROR_NOT_SUPPORTED, storage_file_ctx:ctx()}.
create_location(FileUuid, StorageFileCtx, OwnerId) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    {#statbuf{
        st_mode = Mode,
        st_mtime = MTime
    }, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
    case storage_driver:infer_type(Mode) of
        {ok, ?REGULAR_FILE_TYPE} ->
            Guid = file_id:pack_guid(FileUuid, SpaceId),
            StatTimestamp = storage_file_ctx:get_stat_timestamp_const(StorageFileCtx2),
            storage_sync_info:update_mtime(StorageFileId, SpaceId, Guid, MTime, StatTimestamp),
            create_file_location(FileUuid, OwnerId, StorageFileCtx2);
        {ok, ?DIRECTORY_TYPE} ->
            create_dir_location(FileUuid, StorageFileCtx2);
        ?ERROR_NOT_SUPPORTED ->
            {?ERROR_NOT_SUPPORTED, StorageFileCtx2}

    end.

-spec create_dir_location(file_meta:uuid(), storage_file_ctx:ctx()) -> {ok, storage_file_ctx:ctx()}.
create_dir_location(FileUuid, StorageFileCtx) ->
    {Storage, StorageFileCtx2} = storage_file_ctx:get_storage(StorageFileCtx),
    Helper = storage:get_helper(Storage),
    {SyncedGid, StorageFileCtx4} = case helper:is_posix_compatible(Helper) of
        true ->
            {#statbuf{st_gid = StGid}, StorageFileCtx3} = storage_file_ctx:stat(StorageFileCtx2),
            {StGid, StorageFileCtx3};
        false ->
            {undefined, StorageFileCtx2}
    end,
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx4),
    ok = dir_location:mark_dir_synced_from_storage(FileUuid, StorageFileId, SyncedGid),
    {ok, StorageFileCtx4}.


-spec create_file_location(file_meta:uuid(), od_user:id(), storage_file_ctx:ctx()) -> {ok, storage_file_ctx:ctx()}.
create_file_location(FileUuid, OwnerId, StorageFileCtx) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    {Storage, StorageFileCtx2} = storage_file_ctx:get_storage(StorageFileCtx),
    IsPosix = helper:is_posix_compatible(storage:get_helper(Storage)),
    {#statbuf{
        st_gid = StGid,
        st_size = StSize
    }, StorageFileCtx3} = storage_file_ctx:stat(StorageFileCtx2),
    SyncedGid = case IsPosix of
        true -> StGid;
        false -> undefined
    end,
    ok = fslogic_location:create_imported_file_doc(SpaceId, StorageId, FileUuid, StorageFileId,
        StSize, OwnerId, SyncedGid),
    {ok, StorageFileCtx3}.


-spec get_attr_including_deleted(file_ctx:ctx()) -> {ok, #file_attr{}} | {error, term()}.
get_attr_including_deleted(FileCtx) ->
    try
        #fuse_response{
            status = #status{code = ?OK},
            fuse_response = FileAttr
        } =
            attr_req:get_file_attr_insecure(user_ctx:new(?ROOT_SESS_ID), FileCtx, #{
                allow_deleted_files => true,
                name_conflicts_resolution_policy => allow_name_conflicts,
                attributes => [?attr_is_deleted | ?ONECLIENT_ATTRS]
            }),
        {ok, FileAttr}
    catch
        Class:Reason:Stacktrace ->
            #status{code = Error} = fslogic_errors:gen_status_message(Reason),
            FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            ?debug_exception(?autoformat([FileUuid, SpaceId, Error]), Class, Reason, Stacktrace),
            {error, Error}
    end.


-spec create_file_meta_and_handle_conflicts(file_meta:uuid(), file_meta:name(), file_meta:mode(), od_user:id(),
    file_meta:uuid(), od_space:id(), storage_sync_traverse:info()) -> {ok, file_ctx:ctx()} | {error, term()}.
create_file_meta_and_handle_conflicts(FileUuid, FileName, Mode, OwnerId, ParentUuid, SpaceId, Info) ->
    {ok, FileType} = storage_driver:infer_type(Mode),
    IteratorType = maps:get(iterator_type, Info, undefined),
    FileDoc = prepare_file_meta_doc(FileUuid, FileName, Mode, OwnerId, ParentUuid, SpaceId),
    CreationResult = case file_meta:create({uuid, ParentUuid}, FileDoc) of
        {error, already_exists}
            when IteratorType =:= ?FLAT_ITERATOR
            andalso FileType =:= ?DIRECTORY_TYPE
        ->
            % TODO VFS-6476 how to prevent conflicts on creating directories on s3?
            {ok, FileDoc};
        {error, already_exists} ->
            % There are 2 cases possible here:
            %  * there was race with creating file by lfm
            %  * there is a stalled link

            % resolve existing uuid
            {ok, FileUuid2, TreeId} = file_meta:get_child_uuid_and_tree_id(ParentUuid, FileName),
            case file_meta:get({uuid, FileUuid2}) of
                {ok, _ConflictingFileDoc} ->
                    create_conflicting_file_meta(FileDoc, ParentUuid);
                {error, not_found} ->
                    case TreeId =:= oneprovider:get_id() of
                        true ->
                            % FileUuid2 was found in local links tree
                            % which means that it is a stalled link because file_meta should be created
                            % before adding the link.
                            ?warning(
                                "Stalled file_meta link ~p from parent ~p pointing to uuid ~p detected. "
                                "The link will be deleted", [FileName, ParentUuid, FileUuid2]),
                            ok = file_meta_forest:delete(ParentUuid, SpaceId, FileName, FileUuid2),
                            stalled_link;
                        false ->
                            % FileUuid2 was found in a remote links tree.
                            % There are 2 cases possible here:
                            %  * file_meta hasn't been synchronized yet
                            %  * there is a stalled link in a remote provider's tree
                            % Currently we cannot distinguish above 2 situations so we assume that there is a
                            % conflicting file.
                            % It would be possible when we fetching remote document on demand is implemented.
                            % TODO VFS-6509 fetch file_meta from remote provider to distinguish aforementioned situations
                            create_conflicting_file_meta(FileDoc, ParentUuid)
                    end
            end;
        {ok, FinalDoc} ->
            {ok, FinalDoc}
    end,
    case CreationResult of
        {ok, FinalDoc2} ->
            FileCtx = file_ctx:new_by_doc(FinalDoc2, SpaceId),
            case FileType of
                ?DIRECTORY_TYPE ->
                    ok;
                _ ->
                    ok = file_popularity:update_size(FileCtx)
            end,
            ok = fslogic_event_emitter:emit_file_attr_changed_with_replication_status(FileCtx, true, []),
            {ok, FileCtx};
        stalled_link ->
            create_file_meta_and_handle_conflicts(FileUuid, FileName, Mode, OwnerId, ParentUuid, SpaceId, Info)
    end.


-spec create_conflicting_file_meta(file_meta:doc(), file_meta:uuid()) -> {ok, file_meta:doc()}.
create_conflicting_file_meta(FileDoc, ParentUuid) ->
    create_conflicting_file_meta(FileDoc, ParentUuid, ?IMPORTED_CONFLICTING_FILE_DEFAULT_NUMBER).


-spec create_conflicting_file_meta(file_meta:doc(), file_meta:uuid(), non_neg_integer()) -> {ok, file_meta:doc()}.
create_conflicting_file_meta(FileDoc, ParentUuid, ConflictNumber) ->
    OriginalName = file_meta:get_name(FileDoc),
    FileDoc2 = file_meta:set_name(FileDoc, ?IMPORTED_CONFLICTING_FILE_NAME(OriginalName, oneprovider:get_id(), ConflictNumber)),
    % do not check for conflicting links in other providers' trees
    case file_meta:create({uuid, ParentUuid}, FileDoc2) of
        {ok, FileDocFinal} ->
            {ok, FileDocFinal};
        {error, already_exists} ->
            % if there was conflict on creating file with suffix, bump the ConflictNumber and try again
            create_conflicting_file_meta(FileDoc, ParentUuid, ConflictNumber + 1)
    end.


-spec prepare_file_meta_doc(file_meta:uuid(), file_meta:name(), file_meta:mode(), od_user:id(),
    file_meta:uuid(), od_space:id()) -> file_meta:doc().
prepare_file_meta_doc(FileUuid, FileName, Mode, OwnerId, ParentUuid, SpaceId) ->
    {ok, Type} = storage_driver:infer_type(Mode),
    file_meta:new_doc(FileUuid, FileName, Type, Mode band 8#1777, OwnerId, ParentUuid, SpaceId).

-spec create_times_from_stat_timestamps(file_meta:uuid(), storage_file_ctx:ctx()) ->
    {ok, storage_file_ctx:ctx()}.
create_times_from_stat_timestamps(FileUuid, StorageFileCtx) ->
    {#statbuf{
        st_mtime = StMtime,
        st_atime = StAtime,
        st_ctime = StCtime
    }, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx2),
    times:save(FileUuid, SpaceId, StAtime, StMtime, StCtime),
    {ok, StorageFileCtx2}.


-spec set_times_for_dir_using_current_time(file_meta:uuid(), od_space:id()) -> ok.
set_times_for_dir_using_current_time(FileUuid, SpaceId) ->
    CurrentTime = global_clock:timestamp_seconds(),
    times:save(FileUuid, SpaceId, CurrentTime, CurrentTime, CurrentTime),
    dir_update_time_stats:report_update_of_dir(file_id:pack_guid(FileUuid, SpaceId), CurrentTime).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns owner id of given file, acquired from reverse LUMA.
%% On POSIX incompatible storage returns virtual ?SPACE_OWNER_ID(SpaceId)
%% @end
%%-------------------------------------------------------------------
-spec get_owner_id(storage_file_ctx:ctx()) -> {od_user:id(), storage_file_ctx:ctx()}.
get_owner_id(StorageFileCtx) ->
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    case storage:is_posix_compatible(StorageId) of
        true ->
            {StatBuf, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
            Uid = min(StatBuf#statbuf.st_uid, ?UID_MAX),
            {ok, OwnerId} = luma:map_uid_to_onedata_user(Uid, SpaceId, StorageId),
            {OwnerId, StorageFileCtx2};
        false ->
            {?SPACE_OWNER_ID(SpaceId), StorageFileCtx}
    end.


-spec maybe_import_nfs4_acl(file_ctx:ctx(), storage_file_ctx:ctx(), storage_sync_traverse:info()) ->
    {ok, storage_file_ctx:ctx()}.
maybe_import_nfs4_acl(FileCtx, StorageFileCtx, #{is_posix_storage := true, sync_acl := true}) ->
    import_nfs4_acl(FileCtx, StorageFileCtx);
maybe_import_nfs4_acl(_FileCtx, StorageFileCtx, _Info) ->
    {ok, StorageFileCtx}.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Import file's nfs4 ACL.
%% @end
%%-------------------------------------------------------------------
-spec import_nfs4_acl(file_ctx:ctx(), storage_file_ctx:ctx()) -> {ok, storage_file_ctx:ctx()}.
import_nfs4_acl(FileCtx, StorageFileCtx) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    Helper = storage:get_helper(StorageId),
    case not file_ctx:is_space_dir_const(FileCtx) andalso helper:is_nfs4_acl_supported(Helper) of
        false ->
            ok;
        true->
            try
                {ACLBin, StorageFileCtx2} = storage_file_ctx:get_nfs4_acl(StorageFileCtx),
                {ok, NormalizedACL} = storage_import_acl:decode_and_normalize(ACLBin, StorageId),
                {SanitizedAcl, FileCtx2} = sanitize_acl(NormalizedACL, FileCtx),
                #provider_response{status = #status{code = ?OK}} =
                    acl_req:set_acl(UserCtx, FileCtx2, SanitizedAcl),
                {ok, StorageFileCtx2}
            catch
                throw:Reason
                    when Reason =:= ?ENOTSUP
                    orelse Reason =:= ?ENOENT
                    orelse Reason =:= ?ENODATA
                ->
                    {ok, StorageFileCtx}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates mode, times, size, file_location and ACLs of already imported file.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_file(storage_file_ctx:ctx(), #file_attr{}, file_ctx:ctx(),
    info()) -> {result(), file_ctx:ctx(), storage_file_ctx:ctx()} | {error, term()}.
maybe_update_file(StorageFileCtx, _FileAttr, FileCtx, #{detect_modifications := false}) ->
    {?FILE_UNMODIFIED, FileCtx, StorageFileCtx};
maybe_update_file(StorageFileCtx, FileAttr, FileCtx, Info) ->
    try
        maybe_update_attrs(StorageFileCtx, FileAttr, FileCtx, Info)
    catch
        error:{badmatch, {error, not_found}} ->
            {?FILE_UNMODIFIED, FileCtx, StorageFileCtx};
        throw:?ENOENT ->
            {?FILE_UNMODIFIED, FileCtx, StorageFileCtx};
        Error:Reason:Stacktrace ->
            FileName = storage_file_ctx:get_file_name_const(StorageFileCtx),
            SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
            ?error_stacktrace(
                "storage_sync_engine:maybe_update_file file for file ~p in space ~p"
                " failed due to ~w:~w",
                [FileName, SpaceId, Error, Reason], Stacktrace),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates mode, times and size of already imported file.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_attrs(storage_file_ctx:ctx(), #file_attr{}, file_ctx:ctx(), info()) ->
    {result(), file_ctx:ctx(), storage_file_ctx:ctx()}.
maybe_update_attrs(StorageFileCtx, FileAttr, FileCtx, Info = #{parent_ctx := ParentCtx}) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),
    {ok, ProtectionFlags} = dataset_eff_cache:get_eff_protection_flags(FileDoc),

    % If ProtectionFlags are set, modification won't be reflected in the database
    % Attrs will be checked anyway to determine whether protected file has changed on storage.
    % If it has changed, it will be marks in storage_sync_info document so that
    % attrs hash won't be updated in the database.
    % Thanks to that, it will be possible to update the file when protection flag will be finally unset.
    ShouldUpdate = ProtectionFlags =:= ?no_flags_mask,

    ProcessAttrsFoldFun = fun(MaybeUpdateAttrFun, {StorageFileCtxAcc, FileCtxAcc, ModifiedAttrsAcc}) ->
        {Modified, FileCtxOut, StorageFileCtxOut, AttrName} =
            MaybeUpdateAttrFun(StorageFileCtxAcc, FileAttr, FileCtxAcc, Info, ShouldUpdate),
        ModifiedAttrsOut = case Modified of
            true -> [AttrName | ModifiedAttrsAcc];
            false -> ModifiedAttrsAcc
        end,
        {StorageFileCtxOut, FileCtxOut, ModifiedAttrsOut}
    end,

    {StorageFileCtx2, FileCtx3, ModifiedAttrs} = lists:foldl(ProcessAttrsFoldFun, {StorageFileCtx, FileCtx2, []}, [
       fun maybe_update_file_location/5,
       fun maybe_update_mode/5,
       fun maybe_update_times/5,
       fun maybe_update_owner/5,
       fun maybe_update_nfs4_acl/5
    ]),

    case {ModifiedAttrs, ShouldUpdate}  of
        {[], _} ->
            {?FILE_UNMODIFIED, FileCtx3, StorageFileCtx2};
        {ModifiedAttrs, true} ->
            SpaceId = file_ctx:get_space_id_const(FileCtx3),
            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx2),
            {CanonicalPath, FileCtx4} = file_ctx:get_canonical_path(FileCtx3),
            FileUuid = file_ctx:get_logical_uuid_const(FileCtx4),
            storage_import_logger:log_modification(StorageFileId, CanonicalPath, FileUuid, SpaceId, ModifiedAttrs),
            fslogic_event_emitter:emit_file_attr_changed_with_replication_status(FileCtx4, true, []),
            case file_ctx:is_dir(FileCtx4) of
                {true, FileCtx5} ->
                    {?FILE_MODIFIED, FileCtx5, StorageFileCtx2};
                {false, FileCtx5} ->
                    file_popularity:update_size(FileCtx5),
                    {?FILE_MODIFIED, FileCtx5, StorageFileCtx2}
            end;
        {ModifiedAttrs, false} ->
            SpaceId = file_ctx:get_space_id_const(FileCtx3),
            {ParentStorageFileId, _} = file_ctx:get_storage_file_id(ParentCtx),
            ok = storage_sync_info:mark_protected_child_has_changed(ParentStorageFileId, SpaceId),
            {?FILE_UNMODIFIED, FileCtx3, StorageFileCtx2}
    end.

-spec maybe_update_file_location(storage_file_ctx:ctx(), #file_attr{}, file_ctx:ctx(), info(), ShouldUpdate :: boolean()) ->
    {Modified :: boolean(), file_ctx:ctx(), storage_file_ctx:ctx(), file_attr_name()}.
maybe_update_file_location(StorageFileCtx, _FileAttr, FileCtx, _Info, ShouldUpdate) ->
    case file_ctx:is_dir(FileCtx) of
        {true, FileCtx2} ->
            {false, FileCtx2, StorageFileCtx, ?FILE_LOCATION_ATTR_NAME};
        {false, FileCtx2} ->
            maybe_update_file_location(StorageFileCtx, FileCtx2, ShouldUpdate)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's size if it has changed since last import.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_file_location(storage_file_ctx:ctx(), file_ctx:ctx(), ShouldUpdate :: boolean()) ->
    {Updated :: boolean(), file_ctx:ctx(), storage_file_ctx:ctx(), file_attr_name()}.
maybe_update_file_location(StorageFileCtx, FileCtx, ShouldUpdate) ->
    case file_ctx:get_local_file_location_doc_const(FileCtx) of
        undefined -> {false, FileCtx, StorageFileCtx, ?FILE_LOCATION_ATTR_NAME};
        FileLocationDoc -> maybe_update_file_location(StorageFileCtx, FileCtx, FileLocationDoc, ShouldUpdate)
    end.

-spec maybe_update_file_location(storage_file_ctx:ctx(), file_ctx:ctx(), file_location:doc(), ShouldUpdate :: boolean()) ->
    {Updated :: boolean(), file_ctx:ctx(), storage_file_ctx:ctx(), file_attr_name()}.
maybe_update_file_location(StorageFileCtx, FileCtx, FileLocationDoc, ShouldUpdate) ->
    {#statbuf{st_mtime = StMtime, st_size = StSize}, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx2),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx2),
    StorageSyncInfo = case storage_sync_info:get(StorageFileId, SpaceId) of
        {error, not_found} -> undefined;
        {ok, SSI} -> SSI
    end,
    {Size, FileCtx2} = file_ctx:get_local_storage_file_size(FileCtx),
    {{_, _, MTime}, FileCtx3} = file_ctx:get_times(FileCtx2),
    NewLastStat = storage_file_ctx:get_stat_timestamp_const(StorageFileCtx),
    LastReplicationTimestamp = file_location:get_last_replication_timestamp(FileLocationDoc),
    {FileDoc, FileCtx4} = file_ctx:get_file_doc(FileCtx3),
    ProviderId = file_meta:get_provider_id(FileDoc),
    IsLocallyCreatedFile = oneprovider:get_id() =:= ProviderId,
    Result2 = case {IsLocallyCreatedFile, LastReplicationTimestamp, StorageSyncInfo} of
        %todo VFS-4847 refactor this case, use when wherever possible
        {false, undefined, _} ->
            % file created remotely and not yet replicated
            % empty file on storage was created after 'open' operation
            false;

        {true, undefined, undefined} when MTime < StMtime ->
            % file created locally and modified on storage
            ShouldUpdate andalso fslogic_location:update_imported_file_doc(FileCtx4, StSize),
            true;

        {true, undefined, undefined} ->
            % file created locally and not modified on storage
            false;

        {true, undefined, #document{value = #storage_sync_info{
            mtime = LastMtime,
            last_stat = LastStat
        }}} when LastMtime =:= StMtime
            andalso Size =:= StSize
            andalso LastStat > StMtime
        ->
            % file not replicated and already handled because LastStat > StMtime
            false;

        {true, undefined, #document{value = #storage_sync_info{}}} ->
            case (MTime < StMtime) or (Size =/= StSize) of
                true ->
                    ShouldUpdate andalso fslogic_location:update_imported_file_doc(FileCtx4, StSize),
                    true;
                false ->
                    false
            end;

        {_, _, undefined} ->
            case LastReplicationTimestamp < StMtime of
                true ->
                    % file was modified after replication and has never been synced
                    case (MTime < StMtime) of
                        true ->
                            % file was modified on storage
                            ShouldUpdate andalso fslogic_location:update_imported_file_doc(FileCtx3, StSize),
                            true;
                        false ->
                            % file was modified via onedata
                            false
                    end;
                false ->
                    % file was replicated
                    false
            end;

        {_, _, #document{value = #storage_sync_info{
            mtime = LastMtime,
            last_stat = LastStat
        }}} when LastMtime =:= StMtime
            andalso Size =:= StSize
            andalso LastStat > StMtime
        ->
            % file replicated and already handled because LastStat > StMtime
            false;

        {_, _, #document{value = #storage_sync_info{}}} ->
            case LastReplicationTimestamp < StMtime of
                true ->
                    % file was modified after replication
                    case (MTime < StMtime) of
                        true ->
                            %there was modified on storage
                            ShouldUpdate andalso fslogic_location:update_imported_file_doc(FileCtx4, StSize),
                            true;
                        false ->
                            % file was modified via onedata
                            false
                    end;
                false ->
                    % file was replicated
                    false
            end
    end,
    Guid = file_ctx:get_logical_guid_const(FileCtx4),
    storage_sync_info:update_mtime(StorageFileId, SpaceId, Guid, StMtime, NewLastStat),
    {Result2, FileCtx4, StorageFileCtx2, ?FILE_LOCATION_ATTR_NAME}.

-spec maybe_update_mode(storage_file_ctx:ctx(), #file_attr{}, file_ctx:ctx(), info(), ShouldUpdate :: boolean()) ->
    {Modified :: boolean(), file_ctx:ctx(), storage_file_ctx:ctx(), file_attr_name()}.
maybe_update_mode(StorageFileCtx, #file_attr{mode = OldMode}, FileCtx, _Info, ShouldUpdate) ->
    {#statbuf{st_mode = Mode}, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
    Result = case file_ctx:is_space_dir_const(FileCtx) of
        false ->
            case Mode band 8#1777 of
                OldMode ->
                    false;
                NewMode ->
                    ShouldUpdate andalso update_mode(FileCtx, NewMode),
                    true
            end;
        _ ->
            false
    end,
    {Result, FileCtx, StorageFileCtx2, ?MODE_ATTR_NAME}.

-spec update_mode(file_ctx:ctx(), file_meta:mode()) -> ok.
update_mode(FileCtx, NewMode) ->
    case file_ctx:is_space_dir_const(FileCtx) of
        true ->
            ok;
        _ ->
            attr_req:chmod_attrs_only_insecure(FileCtx, NewMode),
            ok
    end.

-spec maybe_update_times(storage_file_ctx:ctx(), #file_attr{}, file_ctx:ctx(), info(), ShouldUpdate :: boolean()) ->
    {Modified :: boolean(), file_ctx:ctx(), storage_file_ctx:ctx(), file_attr_name()}.
maybe_update_times(StorageFileCtx, #file_attr{mtime = MTime, ctime = CTime}, FileCtx, _Info, ShouldUpdate) ->
    {StorageStat = #statbuf{
        st_mtime = StorageMTime,
        st_ctime = StorageCTime
    }, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
    Modified = case MTime >= StorageMTime andalso CTime >= StorageCTime of
        true ->
            false;
        false ->
            ShouldUpdate andalso update_times(FileCtx, StorageStat),
            true
    end,
    {Modified, FileCtx, StorageFileCtx2, ?TIMESTAMPS_ATTR_NAME}.

-spec update_times(file_ctx:ctx(), helpers:stat()) -> ok.
update_times(FileCtx, #statbuf{st_atime = StorageATime, st_mtime = StorageMTime, st_ctime = StorageCTime}) ->
    ok = fslogic_times:update_times_and_emit(FileCtx,
        fun(T = #times{atime = ATime, mtime = MTime, ctime = CTime}) ->
            {ok, T#times{
                atime = max(StorageATime, ATime),
                mtime = max(StorageMTime, MTime),
                ctime = max(StorageCTime, CTime)
            }}
        end
    ).

-spec maybe_update_owner(storage_file_ctx:ctx(), #file_attr{}, file_ctx:ctx(), info(), ShouldUpdate :: boolean()) ->
    {Modified :: boolean(), file_ctx:ctx(), storage_file_ctx:ctx(), file_attr_name()}.
maybe_update_owner(StorageFileCtx, #file_attr{}, FileCtx, #{is_posix_storage := false},_) ->
    {false, FileCtx, StorageFileCtx, ?OWNER_ATTR_NAME};
maybe_update_owner(StorageFileCtx, #file_attr{owner_id = OldOwnerId}, FileCtx, _Info, ShouldUpdate) ->
    {Modified, StorageFileCtx3} = case file_ctx:is_space_dir_const(FileCtx) of
        true -> {false, StorageFileCtx};
        false ->
            case get_owner_id(StorageFileCtx) of
                {OldOwnerId, StorageFileCtx2} ->
                    {false, StorageFileCtx2};
                {NewOwnerId, StorageFileCtx2} ->
                    ShouldUpdate andalso update_owner(FileCtx, NewOwnerId),
                    {true, StorageFileCtx2}
            end
    end,
    {Modified, FileCtx, StorageFileCtx3, ?OWNER_ATTR_NAME}.

-spec update_owner(file_ctx:ctx(), od_user:id()) -> ok.
update_owner(FileCtx, NewOwnerId) ->
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    ok = ?extract_ok(file_meta:update(FileUuid, fun(FileMeta = #file_meta{}) ->
        {ok, FileMeta#file_meta{owner = NewOwnerId}}
    end)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's nfs4 ACL if it has CHANGED.
%% @end
%%-------------------------------------------------------------------
-spec maybe_update_nfs4_acl(storage_file_ctx:ctx(), #file_attr{}, file_ctx:ctx(), info(), ShouldUpdate :: boolean()) ->
    {Modified :: boolean(), file_ctx:ctx(), storage_file_ctx:ctx(), file_attr_name()}.
maybe_update_nfs4_acl(StorageFileCtx, _FileAttr, FileCtx, #{is_posix_storage := false}, _) ->
    {false, FileCtx, StorageFileCtx, ?NFS4_ACL_ATTR_NAME};
maybe_update_nfs4_acl(StorageFileCtx, _FileAttr, FileCtx, #{sync_acl := false}, _) ->
    {false, FileCtx, StorageFileCtx, ?NFS4_ACL_ATTR_NAME};
maybe_update_nfs4_acl(StorageFileCtx, _FileAttr, FileCtx, #{sync_acl := true}, ShouldUpdate) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    Helper = storage:get_helper(StorageId),
    case not file_ctx:is_space_dir_const(FileCtx) andalso helper:is_nfs4_acl_supported(Helper) of
        false ->
            {false, FileCtx, StorageFileCtx, ?NFS4_ACL_ATTR_NAME};
        true ->
            #provider_response{provider_response = ACL} = acl_req:get_acl(UserCtx, FileCtx),
            try
                {ACLBin, StorageFileCtx2} = storage_file_ctx:get_nfs4_acl(StorageFileCtx),
                {ok, NormalizedNewACL} = storage_import_acl:decode_and_normalize(ACLBin, StorageId),
                {SanitizedAcl, FileCtx2} = sanitize_acl(NormalizedNewACL, FileCtx),
                case #acl{value = SanitizedAcl} of
                    ACL ->
                        {false, FileCtx2, StorageFileCtx2, ?NFS4_ACL_ATTR_NAME};
                    _ ->
                        case ShouldUpdate of
                            true ->
                                #provider_response{status = #status{code = ?OK}} =
                                    acl_req:set_acl(UserCtx, FileCtx2, SanitizedAcl);
                            false ->
                                ok
                        end,
                        {true, FileCtx2, StorageFileCtx2, ?NFS4_ACL_ATTR_NAME}
                end
            catch
                throw:Reason
                    when Reason =:= ?ENOTSUP
                    orelse Reason =:= ?ENOENT
                    orelse Reason =:= ?ENODATA
                ->
                    {false, FileCtx, StorageFileCtx, ?NFS4_ACL_ATTR_NAME}
            end
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Filters given acl leaving only `allow` and `deny` aces. Also disables
%% unknown/unsupported flags (aceflags) and operations (acemask).
%% @end
%%-------------------------------------------------------------------
-spec sanitize_acl(acl:acl(), file_ctx:ctx()) -> {acl:acl(), file_ctx:ctx()}.
sanitize_acl(Acl, FileCtx) ->
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),
    AllPerms = case IsDir of
        true -> ?all_container_perms_mask;
        false -> ?all_object_perms_mask
    end,

    SanitizedAcl = lists:filtermap(fun(#access_control_entity{
        acetype = Type,
        aceflags = Flags,
        acemask = Mask
    } = Ace) ->
        case lists:member(Type, [?allow_mask, ?deny_mask]) of
            true ->
                {true, Ace#access_control_entity{
                    aceflags = Flags band ?identifier_group_mask,
                    acemask = Mask band AllPerms
                }};
            false ->
                false
        end
    end, Acl),

    {SanitizedAcl, FileCtx2}.

-spec is_suffixed(file_meta:name()) -> {true, file_meta:uuid(), file_meta:name()} | false.
is_suffixed(FileName) ->
    Tokens = binary:split(FileName, ?CONFLICTING_STORAGE_FILE_SUFFIX_SEPARATOR, [global]),
    case lists:reverse(Tokens) of
        [FileUuid | Tokens2] when FileUuid =/= <<>>, Tokens2 =/= [], Tokens2 =/= [<<>>] ->
            % Check if FileUuid is existing uuid - not part of a file_name
            case file_meta:get_including_deleted(FileUuid) of
                {ok, _} ->
                    FileNameWithoutSuffix = binary:part(
                        FileName, 0, size(FileName) - size(FileUuid) - size(?CONFLICTING_STORAGE_FILE_SUFFIX_SEPARATOR)),
                    {true, FileUuid, FileNameWithoutSuffix};
                {error, not_found} ->
                    false
            end;
        _ ->
            false
    end.

%% @private
-spec get_file_type(storage_file_ctx:ctx()) -> {{ok, onedata_file:type()} | ?ERROR_NOT_SUPPORTED, storage_file_ctx:ctx()}.
get_file_type(StorageFileCtx) ->
    {#statbuf{st_mode = StMode}, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
    InferTypeAns = storage_driver:infer_type(StMode),
    {InferTypeAns, StorageFileCtx2}.