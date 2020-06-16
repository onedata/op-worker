%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements simple_scan strategy, used by storage_import
%%% and storage_update.
%%% It recursively traverses filesystem and add jobs for importing/updating
%%% to pool.
%%% @end
%%%-------------------------------------------------------------------
-module(simple_scan).
-author("Jakub Kudzia").

-include("modules/storage_sync/strategy_config.hrl").
-include("modules/storage_sync/storage_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include("global_definitions.hrl").
-include("proto/oneprovider/provider_messages.hrl").

-type job_result() :: imported | updated | processed | deleted | failed.

-export_type([job_result/0]).

%% API
-export([run/1]).

%% Internal functions exported for storage_update
-export([
    maybe_update_file/3, import_children/5,
    generate_jobs_for_importing_children/4, import_regular_subfiles/1
]).

%% exported for mocking in tests
-export([check_location_and_maybe_sync/2, import_file/1]).

%%--------------------------------------------------------------------
%% @doc
%% calls ?MODULE:run_internal/1 and catches exceptions
%% @end
%%--------------------------------------------------------------------
-spec run(space_strategy:job()) ->
    {space_strategy:job_result(), [space_strategy:job()]}.
run(Job = #space_strategy_job{data = #{
    file_name := FileName,
    space_id := SpaceId,
    storage_id := StorageId
}}) ->
    try
        run_internal(Job)
    catch
        Error:Reason ->
            ?error_stacktrace("simple_scan:run for file ~p in space ~p failed due to ~p:~p",
                [FileName, SpaceId, Error, Reason]
            ),
            storage_sync_monitoring:mark_failed_file(SpaceId, StorageId),
            {{error, Reason}, []}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Implementation for 'simple_scan' strategy.
%% @end
%%--------------------------------------------------------------------
-spec run_internal(space_strategy:job()) ->
    {space_strategy:job_result(), [space_strategy:job()]}.
run_internal(Job = #space_strategy_job{
    data = #{
        storage_file_ctx := StorageFileCtx
    }}) when StorageFileCtx =/= undefined ->
    maybe_sync_storage_file_and_children(Job);
run_internal(Job = #space_strategy_job{
    data = Data = #{
        parent_ctx := ParentCtx,
        file_name := FileName,
        space_id := SpaceId,
        storage_id := StorageId
    }}) ->

    {CanonicalPath, ParentCtx2} = file_ctx:get_child_canonical_path(ParentCtx, FileName),
    StorageFileCtx = storage_file_ctx:new(CanonicalPath, SpaceId, StorageId),
    StatResult = try
        storage_file_ctx:get_stat_buf(StorageFileCtx)
    catch
        throw:?ENOENT ->
            ?error_stacktrace("simple_scan failed due to ~p for file ~p in space ~p",
                [?ENOENT, CanonicalPath, SpaceId]),
            {error, ?ENOENT}
    end,

    case StatResult of
        Error = {error, _} ->
            storage_sync_monitoring:mark_failed_file(SpaceId, StorageId),
            {Error, []};
        {_StatBuf, StorageFileCtx2} ->
            Data2 = Data#{
                parent_ctx => ParentCtx2,
                storage_file_ctx => StorageFileCtx2
            },
            run_internal(Job#space_strategy_job{data = Data2})
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Synchronizes file associated with SFMHandle and returns jobs for
%% synchronizing a batch of its children (if it's a directory).
%% File may be a regular file or a directory.
%% @end
%%--------------------------------------------------------------------
-spec maybe_sync_storage_file_and_children(space_strategy:job()) ->
    {space_strategy:job_result(), [space_strategy:job()]}.
maybe_sync_storage_file_and_children(Job0 = #space_strategy_job{
    data = #{
        space_id := SpaceId,
        storage_id := StorageId,
        storage_file_ctx := StorageFileCtx0
    }
}) ->
    {#statbuf{
        st_mtime = StorageMtime,
        st_mode = Mode
    }, StorageFileCtx1} = storage_file_ctx:get_stat_buf(StorageFileCtx0),

    Job1 = space_strategy:update_job_data(storage_file_ctx, StorageFileCtx1, Job0),
    {LocalResult, FileCtx, Job2} = maybe_sync_storage_file(Job1),

    case LocalResult of
        {error, Reason} ->
            storage_sync_monitoring:mark_failed_file(SpaceId, StorageId),
            {{error, Reason}, []};
        _ ->
            Data2 = Job2#space_strategy_job.data,
            Offset = maps:get(dir_offset, Data2, 0),
            Type = file_meta:type(Mode),

            Job3 = case Offset of
                0 ->
                    %remember mtime to save after importing all subfiles
                    space_strategy:update_job_data(mtime, StorageMtime, Job2);
                _ -> Job2
            end,
            Module = storage_sync_utils:module(Job3),
            SubJobs = delegate(Module, import_children, [Job3, Type, Offset, FileCtx, ?DIR_BATCH], 5),

            LocalResult2 = case LocalResult of
                imported ->
                    storage_sync_monitoring:mark_imported_file(SpaceId, StorageId),
                    ok;
                updated ->
                    storage_sync_monitoring:mark_updated_file(SpaceId, StorageId),
                    ok;
                processed ->
                    storage_sync_monitoring:mark_processed_file(SpaceId, StorageId),
                    ok;
                ok ->
                    ok
            end,
            {LocalResult2, SubJobs}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Synchronizes file associated with SFMHandle.
%% @end
%%--------------------------------------------------------------------
-spec maybe_sync_storage_file(space_strategy:job()) ->
    {space_strategy:job_result(), file_ctx:ctx() | undefined, space_strategy:job()}.
maybe_sync_storage_file(Job = #space_strategy_job{
    data = #{
        file_name := FileName,
        parent_ctx := ParentCtx,
        space_id := SpaceId
}}) ->
    {HasSuffix, FileUuid, FileBaseName} = case is_suffixed(FileName) of
        {true, StorageUuid, StorageBaseName} -> {true, StorageUuid, StorageBaseName};
        false -> {false, undefined, FileName}
    end,

    case link_utils:try_to_resolve_child_link(FileBaseName, ParentCtx) of
        {error, not_found} ->
            case link_utils:try_to_resolve_child_deletion_link(FileName, ParentCtx) of
                {error, not_found} ->
                    case HasSuffix of
                        true ->
                            {ParentStorageFileId, _} = file_ctx:get_storage_file_id(ParentCtx),
                            Path = filename:join([ParentStorageFileId, FileName]),
                            ?error("Deletion link for ~p is unexpectedly missing", [Path]),
                            {processed, undefined, Job};
                        false ->
                            maybe_import_file(Job)
                    end;
                {ok, _FileUuid} ->
                    {processed, undefined, Job}
            end;
        {ok, ResolvedUuid} ->
            FileUuid2 = utils:ensure_defined(FileUuid, undefined, ResolvedUuid),
            case link_utils:try_to_resolve_child_deletion_link(FileName, ParentCtx) of
                {error, not_found} ->
                    FileGuid = file_id:pack_guid(FileUuid2, SpaceId),
                    FileCtx = file_ctx:new_by_guid(FileGuid),
                    simple_scan:check_location_and_maybe_sync(Job, FileCtx);
                {ok, _} ->
                    {processed, undefined, Job}
            end
    end.


-spec check_location_and_maybe_sync(space_strategy:job(), file_ctx:ctx()) ->
    {space_strategy:job_result(), file_ctx:ctx() | undefined, space_strategy:job()}.
check_location_and_maybe_sync(Job = #space_strategy_job{
    data = #{storage_file_ctx := StorageFileCtx}
}, FileCtx) ->
    {#statbuf{st_mode = StMode}, StorageFileCtx2} = storage_file_ctx:get_stat_buf(StorageFileCtx),
    Job2 = space_strategy:update_job_data(storage_file_ctx, StorageFileCtx2, Job),
    case file_meta:type(StMode) of
        ?DIRECTORY_TYPE ->
            check_dir_location_and_maybe_sync(Job2, FileCtx);
        ?REGULAR_FILE_TYPE ->
            check_file_location_and_maybe_sync(Job2, FileCtx)
    end.


-spec check_dir_location_and_maybe_sync(space_strategy:job(), file_ctx:ctx()) ->
    {space_strategy:job_result(), file_ctx:ctx() | undefined, space_strategy:job()}.
check_dir_location_and_maybe_sync(Job, FileCtx) ->
    check_dir_location_and_maybe_sync(Job, FileCtx, false).

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
-spec check_dir_location_and_maybe_sync(space_strategy:job(), file_ctx:ctx(), boolean()) ->
    {space_strategy:job_result(), file_ctx:ctx() | undefined, space_strategy:job()}.
check_dir_location_and_maybe_sync(Job, FileCtx, StorageFileIsRegularFile) ->
    {DirLocation, _} = file_ctx:get_dir_location_doc(FileCtx),
    StorageFileCreated = dir_location:is_storage_file_created(DirLocation),
    case  StorageFileCreated or StorageFileIsRegularFile of
        true ->
            check_file_meta_and_maybe_sync(Job, FileCtx, StorageFileCreated);
        false ->
            % dir_location does not exist, check whether file_location exist
            % as file with the same name may had been deleted and
            % directory with the same name created
            check_file_location_and_maybe_sync(Job, FileCtx, true)
    end.


-spec check_file_location_and_maybe_sync(space_strategy:job(), file_ctx:ctx()) ->
    {space_strategy:job_result(), file_ctx:ctx() | undefined, space_strategy:job()}.
check_file_location_and_maybe_sync(Job, FileCtx) ->
    check_file_location_and_maybe_sync(Job, FileCtx, false).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function analyses file_location associated with passed FileCtx
%% to determine whether the file can by synchronised, and if so whether
%% it should be imported or updated.
%% CheckType flag determines whether dir_location associated with
%% given FileCtx has already been checked.
%% If StorageFileIsDir == true dir_location has been checked and has not been found.
%% else dir_location has not been checked yet.
%% @end
%%-------------------------------------------------------------------
-spec check_file_location_and_maybe_sync(space_strategy:job(), file_ctx:ctx(), boolean()) ->
    {space_strategy:job_result(), file_ctx:ctx() | undefined, space_strategy:job()}.
check_file_location_and_maybe_sync(Job = #space_strategy_job{data = #{
    storage_id := StorageId,
    parent_ctx := ParentCtx,
    file_name := FileName
}}, FileCtx, StorageFileIsDir) ->
    % Get only two blocks - it is enough to verify if file can be imported
    case file_ctx:get_local_file_location_doc(FileCtx, {blocks_num, 2}) of
        {FLDoc = #document{
            value = #file_location{
                file_id = FileId,
                rename_src_file_id = RenameSrcFileId,
                storage_id = StorageId,
                size = Size
            }}, _} ->
            {ParentStorageFileId, _ParentCtx2} = file_ctx:get_storage_file_id(ParentCtx),
            StorageFileId = filename:join([ParentStorageFileId, FileName]),
            case {FileId =:= StorageFileId, RenameSrcFileId =:= StorageFileId} of
                {_, true} ->
                    % file is being renamed at the moment, ignore it
                    {processed, FileCtx, Job};
                {true, false} ->
                    case fslogic_location_cache:get_blocks(FLDoc, #{count => 2}) of
                        [#file_block{offset = 0, size = Size}] ->
                            check_file_meta_and_maybe_sync(Job, FileCtx, true);
                        [] when Size =:= 0 ->
                            check_file_meta_and_maybe_sync(Job, FileCtx, true);
                        _ when StorageFileIsDir ->
                            % file must have been deleted and directory
                            % with the same name recreated
                            check_file_meta_and_maybe_sync(Job, FileCtx, true);
                        _ ->
                            % file is not fully replicated (not in one block), ignore it
                            {processed, FileCtx, Job}
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
                    maybe_import_file(Job)
            end;
        {undefined, _} ->
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
                    check_file_meta_and_maybe_sync(Job, FileCtx, false);
                false ->
                    % Check whether dir_location exists for this file to determine whether
                    % it's 3rd of the above cases.
                    check_dir_location_and_maybe_sync(Job, FileCtx, true)
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if file (which metadata exists in onedata) is synchronized.
%% @end
%%--------------------------------------------------------------------
-spec check_file_meta_and_maybe_sync(space_strategy:job(), file_ctx:ctx(), boolean()) ->
    {space_strategy:job_result(), file_ctx:ctx() | undefined, space_strategy:job()}.
check_file_meta_and_maybe_sync(Job = #space_strategy_job{}, FileCtx, StorageFileCreated) ->
    case get_attr_including_deleted(FileCtx) of
        {ok, _FileAttr, true} ->
            {processed, undefined, Job};
        {ok, FileAttr, false} ->
            check_file_type_and_maybe_sync(Job, FileAttr, FileCtx, StorageFileCreated);
        {error, ?ENOENT} ->
            maybe_import_file(Job)
    end.

-spec check_file_type_and_maybe_sync(space_strategy:job(), #file_attr{}, file_ctx:ctx(), boolean()) ->
    {space_strategy:job_result(), file_ctx:ctx() | undefined, space_strategy:job()}.
check_file_type_and_maybe_sync(Job = #space_strategy_job{data = #{storage_file_ctx := StorageFileCtx}},
    FileAttr = #file_attr{type = FileMetaType}, FileCtx, StorageFileCreated
) ->
    {#statbuf{st_mode = StMode}, StorageFileCtx2} = storage_file_ctx:get_stat_buf(StorageFileCtx),
    Job2 = space_strategy:update_job_data(storage_file_ctx, StorageFileCtx2, Job),
    StorageFileType = file_meta:type(StMode),
    case {StorageFileType, FileMetaType, StorageFileCreated} of
        {Type, Type, true} ->
            CallbackModule = storage_sync_utils:module(Job2),
            delegate(CallbackModule, maybe_update_file, [Job2, FileAttr, FileCtx], 3);
        {_Type, _OtherType, true} ->
            import_file_recreated_with_different_type(Job2, FileCtx);
        {?REGULAR_FILE_TYPE, ?REGULAR_FILE_TYPE, false} ->
            maybe_import_file(Job2);
        {?DIRECTORY_TYPE, ?DIRECTORY_TYPE, false} ->
            CallbackModule = storage_sync_utils:module(Job2),
            delegate(CallbackModule, maybe_update_file, [Job2, FileAttr, FileCtx], 3);
        {?REGULAR_FILE_TYPE, ?DIRECTORY_TYPE, false} ->
            maybe_import_file(Job2);
        {?DIRECTORY_TYPE, ?REGULAR_FILE_TYPE, false} ->
            {processed, undefined, Job2}
    end.

-spec import_file_recreated_with_different_type(space_strategy:job(), file_ctx:ctx()) ->
    {space_strategy:job_result(), file_ctx:ctx() | undefined, space_strategy:job()}.
import_file_recreated_with_different_type(Job = #space_strategy_job{
    data = #{
        space_id := SpaceId,
        storage_id := StorageId
    }
}, FileCtx) ->
    storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, 1),
    full_update:delete_file_and_update_counters(FileCtx, SpaceId, StorageId),
    maybe_import_file(Job).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% This functions import the file, if it hasn't been synchronized yet.
%% It checks whether file that is to be imported is still visible on
%% the storage.
%% @end
%%-------------------------------------------------------------------
-spec maybe_import_file(space_strategy:job()) ->
    {space_strategy:job_result(), file_ctx:ctx() | undefined, space_strategy:job()}.
maybe_import_file(Job = #space_strategy_job{
    data = #{
        file_name := FileName,
        storage_file_ctx := StorageFileCtx,
        parent_ctx := ParentCtx,
        space_id := SpaceId
}}) ->
    {ParentStorageId, _} = file_ctx:get_storage_file_id(ParentCtx),
    FileStorageId = filename:join([ParentStorageId, FileName]),
    {SFMHandle, StorageFileCtx2} = storage_file_ctx:get_handle(StorageFileCtx),
    Job2 = space_strategy:update_job_data(storage_file_ctx, StorageFileCtx2, Job),
    % We must ensure that there was no race with deleting file.
    % We check whether file that we found on storage and that we want to import
    % is not associated with file that has been deleted from the system.
    case {storage_sync_info:get(FileStorageId, SpaceId), is_still_on_storage(SFMHandle)} of
        {{error, _}, true} ->
            {LocalResult, FileCtx} = import_file_safe(Job2),
            {LocalResult, FileCtx, Job};
        _ ->
            {processed, undefined, Job}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Imports given storage file to onedata filesystem.
%% @end
%%--------------------------------------------------------------------
-spec import_file_safe(space_strategy:job()) -> {space_strategy:job_result(), file_ctx:ctx()}.
import_file_safe(Job = #space_strategy_job{
    data = #{
        file_name := FileName,
        space_id := SpaceId,
        parent_ctx := ParentCtx
    }}) ->
    try
        simple_scan:import_file(Job)
    catch
        Error:Reason ->
            ?error_stacktrace("importing file ~p in space ~p failed with ~p:~p", [FileName, SpaceId, Error, Reason]),
            full_update:delete_file(FileName, ParentCtx),
            {{error, Reason}, undefined}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Imports given storage file to onedata filesystem.
%% @end
%%--------------------------------------------------------------------
-spec import_file(space_strategy:job()) -> {job_result(), file_ctx:ctx()}.
import_file(#space_strategy_job{
    strategy_args = Args,
    data = #{
        file_name := FileName,
        space_id := SpaceId,
        storage_id := StorageId,
        parent_ctx := ParentCtx,
        storage_file_ctx := StorageFileCtx
    }}) ->
    {StatBuf, StorageFileCtx2} = storage_file_ctx:get_stat_buf(StorageFileCtx),
    #statbuf{
        st_mode = Mode,
        st_atime = ATime,
        st_ctime = CTime,
        st_mtime = MTime,
        st_size = FSize
    } = StatBuf,
    {OwnerId, StorageFileCtx3} = get_owner_id(StorageFileCtx2),
    {GroupId, StorageFileCtx4} = get_group_owner_id(StorageFileCtx3),
    ParentUuid = file_ctx:get_uuid_const(ParentCtx),
    FileUuid = datastore_key:new(),
    {ParentStorageFileId, ParentCtx2} = file_ctx:get_storage_file_id(ParentCtx),
    {ParentCanonicalPath, _} = file_ctx:get_canonical_path(ParentCtx2),
    StorageFileId = filename:join([ParentStorageFileId, FileName]),
    CanonicalPath = filename:join([ParentCanonicalPath, FileName]),

    case file_meta:type(Mode) of
        ?REGULAR_FILE_TYPE ->
            StatTimestamp = storage_file_ctx:get_stat_timestamp_const(StorageFileCtx4),

            storage_sync_info:create_or_update(StorageFileId, fun(SSI) ->
                {ok, SSI#storage_sync_info{
                    mtime = MTime,
                    last_stat = StatTimestamp
                }}
            end, SpaceId),
            ok = location_and_link_utils:create_imported_file_location(
                SpaceId, StorageId, FileUuid, StorageFileId, FSize, OwnerId);
        _ ->
            {ok, _} = dir_location:mark_dir_created_on_storage(FileUuid, SpaceId)
    end,

    {ok, FileUuid} = create_file_meta(FileUuid, FileName, Mode, OwnerId,
        GroupId, ParentUuid, SpaceId),
    {ok, _} = create_times(FileUuid, MTime, ATime, CTime, SpaceId),
    SyncAcl = maps:get(sync_acl, Args, false),
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    case SyncAcl of
        true ->
            ok = import_nfs4_acl(FileCtx, StorageFileCtx4);
        _ ->
            ok
    end,
    storage_sync_utils:log_import(StorageFileId, CanonicalPath, FileUuid, SpaceId),
    {imported, FileCtx}.


%%--------------------------------------------------------------------
%% @doc
%% If file is a directory, this function prepares jobs for importing
%% its children. Otherwise it does nothing.
%% @end
%%--------------------------------------------------------------------
-spec import_children(space_strategy:job(), file_meta:type(),
    Offset :: non_neg_integer(), file_ctx:ctx(), non_neg_integer()) ->
    [space_strategy:job()].
import_children(_Job, _Type, _Offset, undefined, _BatchSize) ->
    [];
import_children(Job = #space_strategy_job{
    strategy_args = Args,
    strategy_type = StrategyType,
    data = #{
        max_depth := MaxDepth,
        storage_file_ctx := StorageFileCtx,
        mtime := Mtime,
        space_id := SpaceId,
        storage_id := StorageId
    }},
    ?DIRECTORY_TYPE, Offset, FileCtx, BatchSize
) when MaxDepth > 0 ->
    BatchKey = Offset div BatchSize,
    {ChildrenStorageCtxsBatch1, _} =
        storage_file_ctx:get_children_ctxs_batch(StorageFileCtx, Offset, BatchSize),
    SyncAcl = maps:get(sync_acl, Args, false),
    {BatchHash, ChildrenStorageCtxsBatch2} =
        storage_sync_changes:count_files_attrs_hash(ChildrenStorageCtxsBatch1, SyncAcl),
    {FilesJobs, DirsJobs} = generate_jobs_for_importing_children(Job, Offset,
        FileCtx, ChildrenStorageCtxsBatch2),
    FilesToHandleNum = length(FilesJobs) + length(DirsJobs),
    storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, FilesToHandleNum),
    FilesResults = import_regular_subfiles(FilesJobs),
    {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    case StrategyType:strategy_merge_result(FilesJobs, FilesResults) of
        ok ->
            FileUuid = file_ctx:get_uuid_const(FileCtx2),
            case storage_sync_utils:all_children_imported(DirsJobs, FileUuid) of
                true ->
                    storage_sync_info:create_or_update(StorageFileId,
                        fun(SSI = #storage_sync_info{children_attrs_hashes = CAH}) ->
                            {ok, SSI#storage_sync_info{
                                mtime = Mtime,
                                children_attrs_hashes = CAH#{BatchKey => BatchHash}
                            }}
                        end, SpaceId);
                _ ->
                    storage_sync_info:create_or_update(StorageFileId,
                        fun(SSI = #storage_sync_info{children_attrs_hashes = CAH}) ->
                            {ok, SSI#storage_sync_info{
                                children_attrs_hashes = CAH#{BatchKey => BatchHash}
                            }}
                        end, SpaceId)
            end;
        _ -> ok
    end,
    DirsJobs;
import_children(#space_strategy_job{}, _Type, _Offset, _FileCtx, _) ->
    [].

%%-------------------------------------------------------------------
%% @doc
%% Imports regular files associated with FilesJobs
%% @end
%%-------------------------------------------------------------------
-spec import_regular_subfiles([space_strategy:job()]) -> [space_strategy:job_result()].
import_regular_subfiles(FilesJobs) ->
    utils:pmap(fun(Job) ->
        worker_pool:call(?STORAGE_SYNC_FILE_POOL_NAME, {?MODULE, run, [Job]},
            worker_pool:default_strategy(), ?FILES_IMPORT_TIMEOUT)
    end, FilesJobs).


%%--------------------------------------------------------------------
%% @doc
%% Generates jobs for importing children of given directory to onedata filesystem.
%% @end
%%--------------------------------------------------------------------
-spec generate_jobs_for_importing_children(space_strategy:job(),
    non_neg_integer(), file_ctx:ctx(), [storage_file_ctx:ctx()]) ->
    {[space_strategy:job()], [space_strategy:job()]}.
generate_jobs_for_importing_children(#space_strategy_job{}, _Offset, _FileCtx, []) ->
    {[], []};
generate_jobs_for_importing_children(Job = #space_strategy_job{}, Offset,
    FileCtx, ChildrenStorageCtxsBatch
) ->
    {DirsOnly, Job2} = space_strategy:take_from_job_data(import_dirs_only, Job, false),
    Jobs = {FileJobs, DirJobs} =
        generate_jobs_for_subfiles(Job2, ChildrenStorageCtxsBatch, FileCtx, DirsOnly),
    case length(ChildrenStorageCtxsBatch) < ?DIR_BATCH of
        true ->
            Jobs;
        false ->
            Job3 = space_strategy:update_job_data(dir_offset,
                Offset + length(ChildrenStorageCtxsBatch), Job2),
            {FileJobs, [Job3 | DirJobs]}
    end.

%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Generate jobs for subfiles of given directory. If DirsOnly is true
%%% only jobs subdirectories will be generated.
%%% @end
%%%-------------------------------------------------------------------
-spec generate_jobs_for_subfiles(space_strategy:job(), [storage_file_ctx:ctx()],
    file_ctx:ctx(), boolean()) -> {[space_strategy:job()], [space_strategy:job()]}.
generate_jobs_for_subfiles(Job = #space_strategy_job{
    data = Data = #{max_depth := MaxDepth}
},
    ChildrenStorageCtxsBatch, FileCtx, DirsOnly
) ->
    ChildrenData = Data#{
        dir_offset => 0,
        parent_ctx => FileCtx,
        max_depth => MaxDepth - 1
    },

    lists:foldr(fun(ChildStorageCtx, AccIn = {FileJobsIn, DirJobsIn}) ->
        try
            {#statbuf{st_mode = Mode}, ChildStorageCtx2} =
                storage_file_ctx:get_stat_buf(ChildStorageCtx),
            FileName = storage_file_ctx:get_file_id_const(ChildStorageCtx2),
            case file_meta:is_hidden(FileName) of
                false ->
                    case file_meta:type(Mode) of
                        ?DIRECTORY_TYPE ->
                            ChildStorageCtx3 = storage_file_ctx:reset(ChildStorageCtx2),
                            {FileJobsIn, [
                                new_job(Job, ChildrenData, ChildStorageCtx3)
                                | DirJobsIn
                            ]};
                        ?REGULAR_FILE_TYPE when DirsOnly ->
                            AccIn;
                        ?REGULAR_FILE_TYPE ->
                            ChildStorageCtx3 = storage_file_ctx:reset(ChildStorageCtx2),
                            {[new_job(Job, ChildrenData, ChildStorageCtx3)
                                | FileJobsIn
                            ], DirJobsIn}
                    end;
                _ ->
                    AccIn
            end
        catch
            throw:?ENOENT ->
                FileId = storage_file_ctx:get_file_id_const(ChildStorageCtx),
                {ParentPath, _} = file_ctx:get_canonical_path(FileCtx),
                ?debug_stacktrace(
                    "File ~p not found when generating jobs for syncing children of ~p",
                    [FileId, ParentPath]
                ),
                AccIn
        end
    end, {[], []}, ChildrenStorageCtxsBatch).


%%%-------------------------------------------------------------------
%%% @private
%%% @doc
%%% Returns new job basing on old Job, setting Data and
%%% changing file_name and storage_file_ctx in Data.
%%% @end
%%%-------------------------------------------------------------------
-spec new_job(space_strategy:job(), space_strategy:job_data(),
    storage_file_ctx:ctx()) -> space_strategy:job().
new_job(Job, Data, StorageFileCtx) ->
    FileId = storage_file_ctx:get_file_id_const(StorageFileCtx),
    Job#space_strategy_job{
        data = Data#{
            file_name => filename:basename(FileId),
            storage_file_ctx => StorageFileCtx
        }}.

%%--------------------------------------------------------------------
%% @doc
%% Updates mode, times and size of already imported file.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_file(space_strategy:job(), #file_attr{},
    file_ctx:ctx()) -> {space_strategy:job_result(), file_ctx:ctx(), space_strategy:job()}.
maybe_update_file(Job = #space_strategy_job{
    strategy_args = Args,
    data = #{
        file_name := FileName,
        space_id := SpaceId,
        storage_file_ctx := StorageFileCtx
    }
}, FileAttr, FileCtx) ->
    try
        SyncAcl = maps:get(sync_acl, Args, false),
        Result = maybe_update_attrs(FileAttr, FileCtx, StorageFileCtx, SyncAcl),
        {Result, FileCtx, Job}
    catch
        Error:Reason ->
            ?error_stacktrace("simple_scan:maybe_update_file file for file ~p in space ~p failed due to ~p:~p",
                [FileName, SpaceId, Error, Reason]),
            {{error, Reason}, Job}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates mode, times and size of already imported file.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_attrs(#file_attr{}, file_ctx:ctx(), storage_file_ctx:ctx(), boolean()) -> job_result().
maybe_update_attrs(FileAttr, FileCtx, StorageFileCtx, SyncAcl) ->
    {FileStat = #statbuf{st_mode = StMode}, StorageFileCtx2} = storage_file_ctx:get_stat_buf(StorageFileCtx),
    StorageFileType = file_meta:type(StMode),
    ResultsWithAttrNames = [
        {maybe_update_file_location(FileStat, FileCtx, StorageFileType, StorageFileCtx2), file_location},
        {maybe_update_mode(FileAttr, FileStat, FileCtx), mode},
        {maybe_update_times(FileAttr, FileStat, FileCtx), timestamps},
        {maybe_update_owner(FileAttr, StorageFileCtx2, FileCtx), owner},
        {maybe_update_nfs4_acl(StorageFileCtx2, FileCtx, SyncAcl), nfs4_acl}
    ],
    case filter_updated_attrs(ResultsWithAttrNames) of
        [] ->
            processed;
        UpdatedAttrs ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
            {CanonicalPath, FileCtx3} = file_ctx:get_canonical_path(FileCtx2),
            FileUuid = file_ctx:get_uuid_const(FileCtx3),
            storage_sync_utils:log_update(StorageFileId, CanonicalPath, FileUuid, SpaceId, UpdatedAttrs),
            fslogic_event_emitter:emit_file_attr_changed(FileCtx2, []),
            updated
    end.

-spec filter_updated_attrs([{updated | not_updated, atom()}]) -> [atom()].
filter_updated_attrs(ResultsWithAttrNames) ->
    lists:filtermap(fun
        ({updated, AttrName}) ->
            {true, AttrName};
        ({not_updated, _}) ->
            false
    end, ResultsWithAttrNames).

-spec get_attr_including_deleted(file_ctx:ctx()) -> {ok, #file_attr{}} | {error, term()}.
get_attr_including_deleted(FileCtx) ->
    try
        {#fuse_response{
            status = #status{code = ?OK},
            fuse_response = FileAttr
        }, _, IsDeleted} =
            attr_req:get_file_attr_and_conflicts(user_ctx:new(?ROOT_SESS_ID), FileCtx, true, true, false),
        {ok, FileAttr, IsDeleted}
    catch
        _:Reason ->
            #status{code = Error} = fslogic_errors:gen_status_message(Reason),
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            ?debug_stacktrace(
                "Error {error, ~p} occured when getting attr of file: ~p during storage sync procedure in space: ~p.",
                [Error, FileUuid, SpaceId]
            ),
            {error, Error}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's size if it has changed since last import.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_file_location(#statbuf{}, file_ctx:ctx(), file_meta:type(), storage_file_ctx:ctx()) ->
    updated | not_updated.
maybe_update_file_location(#statbuf{}, _FileCtx, ?DIRECTORY_TYPE, _StorageFileCtx) ->
    not_updated;
maybe_update_file_location(#statbuf{st_mtime = StMtime, st_size = StSize},
    FileCtx, ?REGULAR_FILE_TYPE, StorageFileCtx
) ->
    {StorageSyncInfo, FileCtx2} = file_ctx:get_storage_sync_info(FileCtx),
    {Size, FileCtx3} = file_ctx:get_local_storage_file_size(FileCtx2),
    {{_, _, MTime}, FileCtx4} = file_ctx:get_times(FileCtx3),
    {FileDoc, FileCtx5} = file_ctx:get_file_doc(FileCtx4),
    ProviderId = file_meta:get_provider_id(FileDoc),
    FileUuid = file_ctx:get_uuid_const(FileCtx5),
    SpaceId = file_ctx:get_space_id_const(FileCtx5),
    IsLocallyCreatedFile = oneprovider:get_id() =:= ProviderId,
    NewLastStat = storage_file_ctx:get_stat_timestamp_const(StorageFileCtx),
    LocationId = file_location:local_id(FileUuid),
    {ok, #document{
        value = #file_location{
            last_replication_timestamp = LastReplicationTimestamp
    }}} = fslogic_location_cache:get_location(LocationId, FileUuid),

    Result2 = case {IsLocallyCreatedFile, LastReplicationTimestamp, StorageSyncInfo} of
        %todo VFS-4847 refactor this case, use when wherever possible
        {false, undefined, _} ->
            % remote file created on storage by open and not yet replicated
            not_updated;

        {true, undefined, undefined} when MTime < StMtime ->
            % file created locally and modified on storage
            location_and_link_utils:update_imported_file_location(FileCtx5, StSize),
            updated;

        {true, undefined, undefined} ->
            % file created locally and not modified on storage
            not_updated;

        {true, undefined, #document{value = #storage_sync_info{
            mtime = LastMtime,
            last_stat = LastStat
        }}} when LastMtime =:= StMtime
            andalso Size =:= StSize
            andalso LastStat > StMtime
        ->
            % file not replicated and already handled because LastStat > StMtime
            not_updated;

        {true, undefined, #document{value = #storage_sync_info{}}} ->
            case (MTime < StMtime) or (Size =/= StSize) of
                true ->
                    location_and_link_utils:update_imported_file_location(FileCtx5, StSize),
                    updated;
                false ->
                    not_updated
            end;

        {_, _, undefined} ->
            case LastReplicationTimestamp < StMtime of
                true ->
                    % file was modified after replication and has never been synced
                    case (MTime < StMtime) of
                        true ->
                            % file was modified on storage
                            location_and_link_utils:update_imported_file_location(FileCtx5, StSize),
                            updated;
                        false ->
                            % file was modified via onedata
                            not_updated
                    end;
                false ->
                    % file was replicated
                    not_updated
            end;

        {_, _, #document{value = #storage_sync_info{
            mtime = LastMtime,
            last_stat = LastStat
        }}} when LastMtime =:= StMtime
            andalso Size =:= StSize
            andalso LastStat > StMtime
        ->
            % file replicated and already handled because LastStat > StMtime
            not_updated;

        {_, _, #document{value = #storage_sync_info{}}} ->
            case LastReplicationTimestamp < StMtime of
                true ->
                    % file was modified after replication
                    case (MTime < StMtime) of
                        true ->
                            %there was modified on storage
                            location_and_link_utils:update_imported_file_location(FileCtx5, StSize),
                            updated;
                        false ->
                            % file was modified via onedata
                            not_updated
                    end;
                false ->
                    % file was replicated
                    not_updated
            end
    end,
    {StorageFileId, _} = file_ctx:get_storage_file_id(FileCtx5),
    storage_sync_info:create_or_update(StorageFileId, fun(SSI) ->
        {ok, SSI#storage_sync_info{
            mtime = StMtime,
            last_stat = NewLastStat
        }}
    end, SpaceId),
    Result2.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's mode if it has changed.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_mode(#file_attr{}, #statbuf{}, file_ctx:ctx()) -> updated | not_updated.
maybe_update_mode(#file_attr{mode = OldMode}, #statbuf{st_mode = Mode}, FileCtx) ->
    case file_ctx:is_space_dir_const(FileCtx) of
        false ->
            case Mode band 8#1777 of
                OldMode ->
                    not_updated;
                NewMode ->
                    update_mode(FileCtx, NewMode),
                    updated
            end;
        _ ->
            not_updated
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's mode.
%% @end
%%--------------------------------------------------------------------
-spec update_mode(file_ctx:ctx(), file_meta:mode()) ->
    ok | fslogic_worker:fuse_response().
update_mode(FileCtx, NewMode) ->
    case file_ctx:is_space_dir_const(FileCtx) of
        true ->
            ok;
        _ ->
            ok = attr_req:chmod_attrs_only_insecure(FileCtx, NewMode)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's times if they've changed.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_times(#file_attr{}, #statbuf{}, file_ctx:ctx()) -> updated | not_updated.
maybe_update_times(#file_attr{atime = ATime, mtime = MTime, ctime = CTime},
    #statbuf{st_atime = StorageATime, st_mtime = StorageMTime, st_ctime = StorageCTime},
    _FileCtx
)
    when ATime >= StorageATime
    andalso MTime >= StorageMTime
    andalso CTime >= StorageCTime ->
    not_updated;
maybe_update_times(_FileAttr,
    #statbuf{st_atime = StorageATime, st_mtime = StorageMTime, st_ctime = StorageCTime},
    FileCtx
) ->
    ok = fslogic_times:update_times_and_emit(FileCtx,
        fun(T = #times{
            atime = ATime,
            mtime = MTime,
            ctime = CTime
        }) ->
            {ok, T#times{
                atime = max(StorageATime, ATime),
                mtime = max(StorageMTime, MTime),
                ctime = max(StorageCTime, CTime)
            }}
        end),
    updated.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's owner if it has changed.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_owner(#file_attr{}, storage_file_ctx:ctx(),
    file_ctx:ctx()) -> updated | not_updated.
maybe_update_owner(#file_attr{owner_id = OldOwnerId}, StorageFileCtx, FileCtx) ->
    case file_ctx:is_space_dir_const(FileCtx) of
        true -> not_updated;
        _ ->
            case get_owner_id(StorageFileCtx) of
                {OldOwnerId, _StorageFileCtx2} ->
                    not_updated;
                {NewOwnerId, _StorageFileCtx2} ->
                    FileUuid = file_ctx:get_uuid_const(FileCtx),
                    {ok, _} = file_meta:update(FileUuid, fun(FileMeta = #file_meta{}) ->
                        {ok, FileMeta#file_meta{owner = NewOwnerId}}
                    end),
                    updated
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates file meta
%% @end
%%--------------------------------------------------------------------
-spec create_file_meta(file_meta:uuid() | undefined, file_meta:name(),
    file_meta:mode(), od_user:id(), undefined | od_group:id(), file_meta:uuid(),
    od_space:id()) -> {ok, file_meta:uuid()}.
create_file_meta(FileUuid, FileName, Mode, OwnerId, GroupId, ParentUuid, SpaceId) ->
    FileMetaDoc = file_meta:new_doc(FileUuid, FileName, file_meta:type(Mode),
        Mode band 8#1777, OwnerId, GroupId, ParentUuid, SpaceId),
    {ok, FileUuid} = case file_meta:create({uuid, ParentUuid}, FileMetaDoc) of
        {error, already_exists} ->
            FileName2 = ?IMPORTED_CONFLICTING_FILE_NAME(FileName),
            FileMetaDoc2 = file_meta:new_doc(FileUuid, FileName2, file_meta:type(Mode),
                Mode band 8#1777, OwnerId, GroupId, ParentUuid, SpaceId),
            file_meta:create({uuid, ParentUuid}, FileMetaDoc2);
        Other ->
            Other
    end,
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, []),
    {ok, FileUuid}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates times
%% @end
%%--------------------------------------------------------------------
-spec create_times(file_meta:uuid(), times:time(), times:time(), times:time(),
    od_space:id()) ->
    {ok, datastore:key()}.
create_times(FileUuid, MTime, ATime, CTime, SpaceId) ->
    times:save(#document{
        key = FileUuid,
        value = #times{
            mtime = MTime,
            atime = ATime,
            ctime = CTime
        },
        scope = SpaceId}
    ).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function delegates execution of given Function to Module.
%% If Module doesn't have matching function, function from this ?MODULE
%% is called by default.
%% @end
%%-------------------------------------------------------------------
-spec delegate(atom(), atom(), [term()], non_neg_integer()) -> term().
delegate(Module, Function, Args, Arity) ->
    case erlang:function_exported(Module, Function, Arity) of
        true ->
            erlang:apply(Module, Function, Args);
        _ ->
            erlang:apply(?MODULE, Function, Args)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns owner id of given file, acquired from reverse LUMA.
%% @end
%%-------------------------------------------------------------------
-spec get_owner_id(storage_file_ctx:ctx()) -> {od_user:id(), storage_file_ctx:ctx()}.
get_owner_id(StorageFileCtx) ->
    {StatBuf, StorageFileCtx2} = storage_file_ctx:get_stat_buf(StorageFileCtx),
    #statbuf{st_uid = Uid} = StatBuf,
    {StorageDoc, StorageFileCtx3} = storage_file_ctx:get_storage_doc(StorageFileCtx2),
    {ok, OwnerId} = reverse_luma:get_user_id(Uid, StorageDoc),
    {OwnerId, StorageFileCtx3}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns owner id of given file, acquired from reverse LUMA.
%% @end
%%-------------------------------------------------------------------
-spec get_group_owner_id(storage_file_ctx:ctx()) ->
    {od_group:id() | undefined, storage_file_ctx:ctx()}.
get_group_owner_id(StorageFileCtx) ->
    {StatBuf, StorageFileCtx2} = storage_file_ctx:get_stat_buf(StorageFileCtx),
    #statbuf{st_gid = Gid} = StatBuf,
    {StorageDoc, StorageFileCtx3} = storage_file_ctx:get_storage_doc(StorageFileCtx2),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx3),
    try
        {ok, GroupId} = reverse_luma:get_group_id(Gid, SpaceId, StorageDoc),
        {GroupId, StorageFileCtx3}
    catch
        _:Reason ->
            ?error_stacktrace("Resolving group with Gid ~p failed due to ~p", [Gid, Reason]),
            {undefined, StorageFileCtx3}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Import file's nfs4 ACL if it has changed.
%% @end
%%-------------------------------------------------------------------
-spec import_nfs4_acl(file_ctx:ctx(), storage_file_ctx:ctx()) -> ok.
import_nfs4_acl(FileCtx, StorageFileCtx) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    case file_ctx:is_space_dir_const(FileCtx) of
        true ->
            ok;
        false ->
            try
                {ACLBin, _} = storage_file_ctx:get_nfs4_acl(StorageFileCtx),
                {ok, NormalizedACL} = nfs4_acl:decode_and_normalize(ACLBin, StorageFileCtx),
                #provider_response{status = #status{code = ?OK}} =
                    acl_req:set_acl(UserCtx, FileCtx, NormalizedACL, true, false),
                ok
            catch
                throw:?ENOTSUP ->
                    ok;
                throw:?ENOENT ->
                    ok;
                throw:?ENODATA ->
                    ok
            end
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's nfs4 ACL if it has changed.
%% @end
%%-------------------------------------------------------------------
-spec maybe_update_nfs4_acl(storage_file_ctx:ctx(), file_ctx:ctx(),
    SyncAcl :: boolean()) -> updated | not_updated.
maybe_update_nfs4_acl(_StorageFileCtx, _FileCtx, false) ->
    not_updated;
maybe_update_nfs4_acl(StorageFileCtx, FileCtx, true) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    case file_ctx:is_space_dir_const(FileCtx) of
        true ->
            not_updated;
        false ->
            #provider_response{provider_response = ACL} = acl_req:get_acl(UserCtx, FileCtx),
            try
                {ACLBin, _} = storage_file_ctx:get_nfs4_acl(StorageFileCtx),
                {ok, NormalizedNewACL} = nfs4_acl:decode_and_normalize(ACLBin, StorageFileCtx),
                case NormalizedNewACL of
                    ACL ->
                        not_updated;
                    _ ->
                        #provider_response{status = #status{code = ?OK}} =
                            acl_req:set_acl(UserCtx, FileCtx, NormalizedNewACL, false, false),
                        updated
                end
            catch
                throw:?ENOTSUP ->
                    not_updated;
                throw:?ENOENT ->
                    not_updated;
                throw:?ENODATA ->
                    not_updated
            end
    end.

-spec is_suffixed(file_meta:name()) -> {true, file_meta:uuid(), file_meta:name()} | false.
is_suffixed(FileName) ->
    Tokens = binary:split(FileName, ?CONFLICTING_STORAGE_FILE_SUFFIX_SEPARATOR, [global]),
    case lists:reverse(Tokens) of
        [FileName] ->
            false;
        [FileUuid | Tokens2] ->
            FileName2 = list_to_binary(lists:reverse(Tokens2)),
            {true, FileUuid, FileName2}
    end.

-spec is_still_on_storage(storage_file_manager:handle()) -> boolean().
is_still_on_storage(SFMHandle) ->
    case storage_file_manager:stat(SFMHandle) of
        {ok, _} -> true;
        {error, ?ENOENT} -> false
    end.
