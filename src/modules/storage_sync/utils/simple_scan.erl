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
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include("global_definitions.hrl").
-include("proto/oneprovider/provider_messages.hrl").

-type job_result() :: imported | updated | processed | deleted | failed.

-export_type([job_result/0]).

%% API
-export([run/1, maybe_import_storage_file_and_children/1,
    maybe_import_storage_file/1, import_children/5,
    handle_already_imported_file/3, generate_jobs_for_importing_children/4,
    import_regular_subfiles/1, import_file_safe/2, import_file/2]).

%%--------------------------------------------------------------------
%% @doc
%% calls ?MODULE:run_internal/1 and catches exceptions
%% @end
%%--------------------------------------------------------------------
-spec run(space_strategy:job()) ->
    {space_strategy:job_result(), [space_strategy:job()]}.
run(Job =  #space_strategy_job{data = #{
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

%%--------------------------------------------------------------------
%% @doc
%% Imports file associated with SFMHandle that hasn't been imported yet.
%% File may be space's dir.
%% @end
%%--------------------------------------------------------------------
-spec maybe_import_storage_file_and_children(space_strategy:job()) ->
    {space_strategy:job_result(), [space_strategy:job()]}.
maybe_import_storage_file_and_children(Job0 = #space_strategy_job{
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
    {LocalResult, FileCtx, Job2} = maybe_import_storage_file(Job1),

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
%% @doc
%% Imports file associated with SFMHandle that hasn't been imported yet.
%% File is, for sure, not space's dir.
%% @end
%%--------------------------------------------------------------------
-spec maybe_import_storage_file(space_strategy:job()) ->
    {space_strategy:job_result(), file:ctx(), space_strategy:job()} | no_return().
maybe_import_storage_file(Job = #space_strategy_job{
    data = #{
        file_name := FileName,
        parent_ctx := ParentCtx,
        storage_file_ctx := StorageFileCtx,
        space_id := SpaceId,
        storage_id := StorageId
}}) ->
    {#statbuf{st_mode = Mode, st_mtime = StMTime}, StorageFileCtx2} =
        storage_file_ctx:get_stat_buf(StorageFileCtx),
    FileType = file_meta:type(Mode),
    Job2 = space_strategy:update_job_data(storage_file_ctx, StorageFileCtx2, Job),
    case try_to_resolve_child_link(FileName, ParentCtx) of
        {error, not_found} ->
            {ParentStorageId, _} = file_ctx:get_storage_file_id(ParentCtx),
            FileStorageId = filename:join([ParentStorageId, FileName]),
            case storage_sync_info:get(FileStorageId, SpaceId) of
                {error, _} ->
                    {LocalResult, FileCtx} = import_file_safe(Job2, undefined),
                    {LocalResult, FileCtx, Job2};
                {ok, #document{value = #storage_sync_info{mtime = LastMTime}}}
                    when StMTime =:= LastMTime
                ->
                    {processed, undefined, Job2};
                _ ->
                    {LocalResult, FileCtx} = import_file_safe(Job2, undefined),
                    {LocalResult, FileCtx, Job2}
            end;
        {ok, FileUuid} ->
            FileGuid = fslogic_uuid:uuid_to_guid(FileUuid, SpaceId),
            FileCtx = file_ctx:new_by_guid(FileGuid),
            case FileType of
                ?DIRECTORY_TYPE ->
                    maybe_import_file_with_existing_metadata(Job2, FileCtx);
                ?REGULAR_FILE_TYPE ->
                    % Get only two blocks - it is enough to verify if file can be imported
                    case fslogic_location_cache:get_location(
                        file_location:id(FileUuid, oneprovider:get_id()), FileUuid, {blocks_num, 2}) of
                        {ok, #document{
                            value = #file_location{
                                storage_id = StorageId,
                                size = Size
                            }} = FL} ->
                            case fslogic_location_cache:get_blocks(FL, #{count => 2}) of
                                [#file_block{offset = 0, size = Size}] ->
                                    maybe_import_file_with_existing_metadata(Job2, FileCtx);
                                [] when Size =:= 0 ->
                                    maybe_import_file_with_existing_metadata(Job2, FileCtx);
                                _ ->
                                    {processed, FileCtx, Job2}
                            end;
                        _Other ->
                            {processed, FileCtx, Job2}
                    end
            end
    end.

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
    {BatchHash, ChildrenStorageCtxsBatch2} =
        storage_sync_changes:count_files_attrs_hash(ChildrenStorageCtxsBatch1),
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
    maybe_import_storage_file_and_children(Job);
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
            ?error_stacktrace("simple_scan failed due to ~p for file ~p in space ~p", [?ENOENT, CanonicalPath, SpaceId]),
            {error, ?ENOENT}
    end,

    case StatResult of
        Error = {error, _} ->
            storage_sync_monitoring:mark_failed_file(SpaceId, StorageId),
            {Error, []};
        {_StatBuf, StorageFileCtx2}  ->
            Data2 = Data#{
                parent_ctx => ParentCtx2,
                storage_file_ctx => StorageFileCtx2
            },
            run_internal(Job#space_strategy_job{data = Data2})
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function tries to resolve child with name FileName of
%% directory associated with ParentCtx.
%% @end
%%-------------------------------------------------------------------
-spec try_to_resolve_child_link(file_meta:name(), file_ctx:ctx()) ->
    {ok, file_meta:uuid()} | {error, term()}.
try_to_resolve_child_link(FileName, ParentCtx) ->
    ParentUuid = file_ctx:get_uuid_const(ParentCtx),
    fslogic_path:to_uuid(ParentUuid, FileName).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if file (which metadata exists in onedata) is fully imported
%% (.i.e. for regular files checks if its file_location exists).
%% @end
%%--------------------------------------------------------------------
-spec maybe_import_file_with_existing_metadata(space_strategy:job(), file_ctx:ctx()) ->
    {space_strategy:job_result(), file_ctx:ctx(), space_strategy:job()}.
maybe_import_file_with_existing_metadata(Job = #space_strategy_job{}, FileCtx) ->
    CallbackModule = storage_sync_utils:module(Job),
    #fuse_response{
        status = #status{code = StatusCode},
        fuse_response = FileAttr
    } = get_attr(FileCtx),
    case StatusCode of
        ?OK ->
            {LocalResult, Job2} = delegate(CallbackModule, handle_already_imported_file, [
                Job, FileAttr, FileCtx], 3),
            {LocalResult, FileCtx, Job2};
        ErrorCode when
            ErrorCode =:= ?ENOENT;
            ErrorCode =:= ?EAGAIN
        ->
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            {LocalResult, FileCtx3} = import_file_safe(Job, FileUuid),
            {LocalResult, FileCtx3, Job}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Imports given storage file to onedata filesystem.
%% @end
%%--------------------------------------------------------------------
-spec import_file_safe(space_strategy:job(), file_meta:uuid() | undefined) ->
    {space_strategy:job_result(), file_ctx:ctx()}| no_return().
import_file_safe(Job = #space_strategy_job{
    data = #{
        file_name := FileName,
        space_id := SpaceId,
        parent_ctx := ParentCtx
}}, FileUuid) ->
    try
        ?MODULE:import_file(Job, FileUuid)
    catch
        Error:Reason ->
            ?error_stacktrace("importing file ~p in space ~p failed with ~p:~p", [FileName, SpaceId, Error, Reason]),
            full_update:delete_imported_file(FileName, ParentCtx),
            {{error, Reason}, undefined}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Imports given storage file to onedata filesystem.
%% @end
%%--------------------------------------------------------------------
-spec import_file(space_strategy:job(), file_meta:uuid() | undefined) ->
    {job_result(), file_ctx:ctx()}| no_return().
import_file(#space_strategy_job{
    strategy_args = Args,
    data = #{
        file_name := FileName,
        space_id := SpaceId,
        storage_id := StorageId,
        parent_ctx := ParentCtx,
        storage_file_ctx := StorageFileCtx
}}, FileUuid) ->
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
    {ok, FileUuid2} = create_file_meta(FileUuid, FileName, Mode, OwnerId,
        GroupId, FSize, ParentUuid, SpaceId),
    {ok, _} = create_times(FileUuid2, MTime, ATime, CTime, SpaceId),
    {ParentPath, _} = file_ctx:get_storage_file_id(ParentCtx),
    CanonicalPath = filename:join([ParentPath, FileName]),
    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid2, SpaceId),
    FileCtx = file_ctx:new_by_guid(FileGuid),
    {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    case file_meta:type(Mode) of
        ?REGULAR_FILE_TYPE ->
            StatTimestamp = storage_file_ctx:get_stat_timestamp_const(StorageFileCtx4),
            storage_sync_info:create_or_update(StorageFileId, fun(SSI) ->
                {ok, SSI#storage_sync_info{
                    mtime = MTime,
                    last_stat = StatTimestamp
                }}
            end, SpaceId),
            ok = create_file_location(SpaceId, StorageId, FileUuid2, CanonicalPath, FSize);
        _ ->
            {ok, _} = dir_location:mark_dir_created_on_storage(FileUuid2, SpaceId)
    end,
    SyncAcl = maps:get(sync_acl, Args, false),
    case SyncAcl of
        true ->
            ok = import_nfs4_acl(FileCtx2, StorageFileCtx4);
        _ ->
            ok
    end,
    ?debug("Import storage file ~p", [{StorageFileId, CanonicalPath}]),
    storage_sync_utils:log_import(StorageFileId, SpaceId),
    {imported, FileCtx2}.

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
                ?warning_stacktrace(
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
-spec handle_already_imported_file(space_strategy:job(), #file_attr{},
    file_ctx:ctx()) -> {space_strategy:job_result(), space_strategy:job()}.
handle_already_imported_file(Job = #space_strategy_job{
    strategy_args = Args,
    data = #{
        file_name := FileName,
        space_id := SpaceId,
        storage_file_ctx := StorageFileCtx
    }
}, FileAttr, FileCtx) ->

    SyncAcl = maps:get(sync_acl, Args, false),
    try
        {#statbuf{st_mode = Mode}, StorageFileCtx2} = storage_file_ctx:get_stat_buf(StorageFileCtx),
        Result = maybe_update_attrs(FileAttr, FileCtx, StorageFileCtx2, Mode, SyncAcl),
        {Result, Job}
    catch
        Error:Reason ->
            ?error_stacktrace("simple_scan:handle_already_imported file for file ~p in space ~p failed due to ~p:~p",
                [FileName, SpaceId, Error, Reason]),
            {{error, Reason}, Job}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates mode, times and size of already imported file.
%% @end
%%--------------------------------------------------------------------
-spec maybe_update_attrs(#file_attr{}, file_ctx:ctx(), storage_file_ctx:ctx(),
    file_meta:mode(), boolean()) -> job_result().
maybe_update_attrs(FileAttr, FileCtx, StorageFileCtx, Mode, SyncAcl) ->
    {FileStat, StorageFileCtx2} = storage_file_ctx:get_stat_buf(StorageFileCtx),
    Results = [
        maybe_update_file_location(FileStat, FileCtx, file_meta:type(Mode), StorageFileCtx2),
        maybe_update_mode(FileAttr, FileStat, FileCtx),
        maybe_update_times(FileAttr, FileStat, FileCtx),
        maybe_update_owner(FileAttr, StorageFileCtx2, FileCtx),
        maybe_update_nfs4_acl(StorageFileCtx2, FileCtx, SyncAcl)
    ],
    case lists:member(updated, Results) of
        true ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            {StorageFileId, _FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
            storage_sync_utils:log_update(StorageFileId, SpaceId),
            updated;
        false ->
            processed
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get file attr, catching all exceptions and returning always fuse_response
%% @end
%%--------------------------------------------------------------------
-spec get_attr(file_ctx:ctx()) -> fslogic_worker:fuse_response().
get_attr(FileCtx) ->
    try
        attr_req:get_file_attr_insecure(user_ctx:new(?ROOT_SESS_ID), FileCtx)
    catch
        _:Error ->
            #fuse_response{status = fslogic_errors:gen_status_message(Error)}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates file's size if it has changed since last import.
%% @end
%%--------------------------------------------------------------------
maybe_update_file_location(#statbuf{}, _FileCtx, ?DIRECTORY_TYPE, _StorageFileCtx) ->
    not_updated;
maybe_update_file_location(#statbuf{st_mtime = StMtime, st_size = StSize},
    FileCtx, ?REGULAR_FILE_TYPE, StorageFileCtx
) ->
    {StorageSyncInfo, FileCtx2} = file_ctx:get_storage_sync_info(FileCtx),
    {Size, FileCtx3} = file_ctx:get_local_storage_file_size(FileCtx2),
    {{_, _ , MTime}, FileCtx4} = file_ctx:get_times(FileCtx3),
    FileUuid = file_ctx:get_uuid_const(FileCtx4),
    SpaceId = file_ctx:get_space_id_const(FileCtx4),
    NewLastStat = storage_file_ctx:get_stat_timestamp_const(StorageFileCtx),
    LocationId = file_location:local_id(FileUuid),
    {ok, #document{
        value = #file_location{
            last_replication_timestamp = LastReplicationTimestamp
    }}} = fslogic_location_cache:get_location(LocationId, FileUuid),

    Result2 = case {LastReplicationTimestamp, StorageSyncInfo} of
        %todo VFS-4847 refactor this case, use when wherever possible
        {undefined, undefined} when MTime < StMtime ->
            % file created locally and modified on storage
            update_file_location(FileCtx4, StSize),
            updated;

        {undefined, undefined} ->
            % file created locally and not modified on storage
            not_updated;

        {undefined, #document{value = #storage_sync_info{
            mtime = LastMtime,
            last_stat = LastStat
        }}} when LastMtime =:= StMtime
            andalso Size =:= StSize
            andalso LastStat > StMtime
        ->
            % file not replicated and already handled because LastStat > StMtime
            not_updated;

        {undefined, #document{value = #storage_sync_info{}}} ->
            case (MTime < StMtime) or (Size =/= StSize) of
                true ->
                    update_file_location(FileCtx4, StSize),
                    updated;
                false ->
                    not_updated
            end;

        {_, undefined} ->
            case LastReplicationTimestamp < StMtime of
                true ->
                    % file was modified after replication and has never been synced
                    case (MTime < StMtime) of
                        true ->
                            % file was modified on storage
                            update_file_location(FileCtx4, StSize),
                            updated;
                        false ->
                            % file was modified via onedata
                            not_updated
                    end;
                false ->
                    % file was replicated
                    not_updated
            end;

        {_, #document{value = #storage_sync_info{
            mtime = LastMtime,
            last_stat = LastStat
        }}} when LastMtime =:= StMtime
            andalso Size =:= StSize
            andalso LastStat > StMtime
        ->
            % file replicated and already handled because LastStat > StMtime
            not_updated;

        {_, #document{value = #storage_sync_info{}}} ->
            case LastReplicationTimestamp < StMtime of
                true ->
                    % file was modified after replication
                    case (MTime < StMtime) of
                        true ->
                            %there was modified on storage
                            update_file_location(FileCtx4, StSize),
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
    {StorageFileId, _} = file_ctx:get_storage_file_id(FileCtx4),
    storage_sync_info:create_or_update(StorageFileId, fun(SSI) ->
        {ok, SSI#storage_sync_info{
            mtime = StMtime,
            last_stat = NewLastStat
        }}
    end, SpaceId),
    Result2.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Updates file_location
%% @end
%%-------------------------------------------------------------------
-spec update_file_location(file_ctx:ctx(), non_neg_integer()) -> ok.
update_file_location(FileCtx, StorageSize) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    NewFileBlocks = create_file_blocks(StorageSize),
    replica_updater:update(FileCtx, NewFileBlocks, StorageSize, true),
    ok = lfm_event_emmiter:emit_file_written(
        FileGuid, NewFileBlocks, StorageSize, {exclude, ?ROOT_SESS_ID}).

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
) when
    ATime >= StorageATime,
    MTime >= StorageMTime,
    CTime >= StorageCTime
->
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
    file_meta:mode(), od_user:id(), undefined | od_group:id(), file_meta:size(),
    file_meta:uuid(), od_space:id()) -> {ok, file_meta:uuid()}.
create_file_meta(FileUuid, FileName, Mode, OwnerId, GroupId, FileSize,
    ParentUuid, SpaceId
) ->
    FileMetaDoc = file_meta:new_doc(FileUuid, FileName, file_meta:type(Mode),
        Mode band 8#1777, OwnerId, GroupId, FileSize, ParentUuid, SpaceId),
    case FileUuid of
        undefined ->
            file_meta:create({uuid, ParentUuid}, FileMetaDoc);
        _ ->
            file_meta:save(FileMetaDoc, false)
    end.

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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates file_location
%% @end
%%--------------------------------------------------------------------
-spec create_file_location(od_space:id(), storage:id(), file_meta:uuid(),
    file_meta:path(), file_meta:size()) -> ok.
create_file_location(SpaceId, StorageId, FileUuid, CanonicalPath, Size) ->
    Location = #file_location{
        provider_id = oneprovider:get_id(),
        file_id = CanonicalPath,
        storage_id = StorageId,
        uuid = FileUuid,
        space_id = SpaceId,
        size = Size,
        storage_file_created = true
    },
    LocationDoc = #document{
        key = file_location:local_id(FileUuid),
        value = Location,
        scope = SpaceId
    },
    LocationDoc2 = fslogic_location_cache:set_blocks(LocationDoc, create_file_blocks(Size)),
    {ok, _LocId} = file_location:save_and_bump_version(LocationDoc2),
    ok.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns list containing one block with given size.
%% Is Size == 0 returns empty list.
%% @end
%%-------------------------------------------------------------------
-spec create_file_blocks(non_neg_integer()) -> fslogic_blocks:blocks().
create_file_blocks(0) -> [];
create_file_blocks(Size) -> [#file_block{offset = 0, size = Size}].

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
                    not_updated
            end
    end.