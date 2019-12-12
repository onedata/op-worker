%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for traversing scanned storages.
%%% It uses storage_traverse framework to traverse all files visible on storage
%%% and to schedule jobs for synchronizing them.
%%% The mechanism uses the following helper modules:
%%%   * storage_sync_engine - each storage file is synchronized by call
%%%                         to storage_sync_engine:process_file/2 function
%%%   * storage_sync_links - helper module which implements links used by storage_sync to compare
%%%                         lists of files on storage with lists of files in the Onedata system
%%%   * storage_sync_deletion - module responsible for detecting which files in the
%%%                         synchronized space were deleted on storage and therefore should be
%%%                         deleted from the Onedata file system
%%%   * storage_sync_monitoring - this module is used to store data for monitoring
%%%                         performance of storage_sync
%%%   * storage_sync_info - a helper module that implements a model that is
%%%                         used store information (timestamp of last stat operation, last synced mtime,
%%%                         last computed hash of children attrs) required by sync to determine
%%%                         whether there were changes introduced to file or children
%%%                         files on storage since last scan.
%%%   * storage_sync_hash - helper module used to compute hashes of children file attributes.
%%%                         Those hashes are used to improve sync performance by allowing to easily
%%%                         decide whether files have been modified on storage.
%%%   * storage_sync_logger - helper module used for writing information
%%%                         about sync scans to audit log file
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_traverse).
-author("Jakub Kudzia").

-behaviour(traverse_behaviour).
-behaviour(storage_traverse).

-include("global_definitions.hrl").
-include("modules/storage/traverse/storage_traverse.hrl").
-include("modules/storage/sync/storage_sync.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%%%===================================================================
%%% Types
%%%===================================================================

-type master_job() :: storage_traverse:master_job().
-type slave_job() :: storage_traverse:slave_job().
-type scan_status() :: not_started | in_progress | finished.
-type info() :: #{atom() => term()}.

%%%===================================================================
%%% Exports
%%%===================================================================

%% Types
-export_type([scan_status/0, info/0, master_job/0, slave_job/0]).

%% API
-export([init_pool/0, stop_pool/0, run_import/3, run_update/3, cancel/2]).

%% Pool callbacks
-export([do_master_job/2, do_slave_job/2, get_job/1, update_job_progress/5, to_string/1,
    task_started/2, task_finished/2, task_canceled/2]).

%% storage_traverse callbacks
-export([reset_info/1, get_next_batch_job_prehook/1, get_children_master_job_prehook/1, get_fold_children_fun/1]).

%% exported for tests
-export([has_mtime_changed/2, run/4, run_deletion_scan/5]).

%%%===================================================================
%%% Macros
%%%===================================================================

-define(POOL, ?MODULE).
-define(POOL_BIN, atom_to_binary(?POOL, utf8)).
-define(TASK_ID_SEP, <<"###">>).
-define(TASK_ID_PREFIX, <<"storage_sync">>).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_pool() -> ok.
init_pool() ->
    % Get pool limits from app.config
    MasterJobsLimit = application:get_env(?APP_NAME, storage_sync_master_jobs_limit, 10),
    SlaveJobsLimit = application:get_env(?APP_NAME, storage_sync_slave_workers_limit, 10),
    ParallelSyncedSpacesLimit = application:get_env(?APP_NAME, storage_sync_parallel_synced_spaces_limit, 10),
    storage_traverse:init(?POOL, MasterJobsLimit, SlaveJobsLimit, ParallelSyncedSpacesLimit).

-spec stop_pool() -> ok.
stop_pool() ->
    storage_traverse:stop(?POOL).

-spec run_import(od_space:id(), od_storage:id(), space_strategies:import_config()) -> ok.
run_import(SpaceId, StorageId, ImportConfig) ->
    CurrentTimestamp = time_utils:cluster_time_seconds(),
    {ok, SSM} = storage_sync_monitoring:prepare_new_import_scan(SpaceId, StorageId, CurrentTimestamp),
    {ok, ScansNum} = storage_sync_monitoring:get_finished_scans_num(SSM),
    ?debug("Starting storage_sync scan no. 1 for space: ~p and storage: ~p", [SpaceId, StorageId]),
    run(SpaceId, StorageId, ScansNum + 1, ImportConfig).

-spec run_update(od_space:id(), od_storage:id(), space_strategies:update_config()) -> ok.
run_update(SpaceId, StorageId, UpdateConfig) ->
    CurrentTimestamp = time_utils:cluster_time_seconds(),
    {ok, SSM} = storage_sync_monitoring:prepare_new_update_scan(SpaceId, StorageId, CurrentTimestamp),
    {ok, ScansNum} = storage_sync_monitoring:get_finished_scans_num(SSM),
    ?debug("Starting storage_sync scan no. ~p for space: ~p and storage: ~p", [ScansNum + 1, SpaceId, StorageId]),
    run(SpaceId, StorageId, ScansNum + 1, UpdateConfig).

-spec cancel(od_space:id(), od_storage:id()) -> ok.
cancel(SpaceId, StorageId) ->
    case storage_sync_monitoring:get_finished_scans_num(SpaceId, StorageId) of
        {ok, ScansNum} ->
            traverse:cancel(?POOL_BIN, encode_task_id(SpaceId, StorageId, ScansNum + 1));
        {error, ?ENOENT} ->
            ?warning("Cannot cancel storage sync for space ~p and storage ~p as it is not configured"),
            ok
    end.

%===================================================================
% Pool callbacks
%===================================================================

-spec do_master_job(master_job(), traverse:master_job_extended_args()) -> {ok, traverse:master_job_map()}.
do_master_job(DeletionJob = #storage_traverse_master{info = #{detect_deletions := true}}, Args) ->
    storage_sync_deletion:do_master_job(DeletionJob, Args);
do_master_job(TraverseJob = #storage_traverse_master{info = #{scan_num := 1}}, Args) ->
    do_import_master_job(TraverseJob, Args);
do_master_job(TraverseJob = #storage_traverse_master{info = #{scan_num := ScanNum}}, Args) when ScanNum >= 1 ->
    do_update_master_job(TraverseJob, Args).

-spec do_slave_job(slave_job(), traverse:id()) -> ok | {error, term()}.
do_slave_job(Job = #storage_traverse_slave{info = #{detect_deletions := true}}, Task) ->
    storage_sync_deletion:do_slave_job(Job, Task);
do_slave_job(#storage_traverse_slave{
    storage_file_ctx = StorageFileCtx,
    info = Info
}, _Task) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    case process_storage_file(StorageFileCtx, Info) of
        {ok, {SyncResult, _, _}} ->
            increase_counter(SyncResult, SpaceId, StorageId);
        Error = {error, _} ->
            storage_sync_monitoring:mark_failed_file(SpaceId, StorageId),
            Error
    end.

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    master_job(), traverse:pool(), traverse:id(), traverse:job_status()) ->
    {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(ID, Job, Pool, TaskID, Status) ->
    storage_traverse:update_job_progress(ID, Job, Pool, TaskID, Status).

-spec get_job(traverse:job_id()) ->
    {ok, master_job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    storage_traverse:get_job(DocOrID).

-spec task_started(traverse:id(), traverse:pool()) -> ok.
task_started(TaskId, _PoolName) ->
    {SpaceId, _StorageId, ScanNum} = decode_task_id(TaskId),
    storage_sync_logger:log_scan_started(SpaceId, ScanNum, TaskId).

-spec task_finished(traverse:id(), traverse:pool()) -> ok.
task_finished(TaskId, _PoolName) ->
    {SpaceId, StorageId, ScanNum} = decode_task_id(TaskId),
    scan_finished(SpaceId, StorageId),
    storage_sync_logger:log_scan_finished(SpaceId, ScanNum, TaskId).

-spec task_canceled(traverse:id(), traverse:pool()) -> ok.
task_canceled(TaskId, _PoolName) ->
    {SpaceId, StorageId, ScanNum} = decode_task_id(TaskId),
    scan_finished(SpaceId, StorageId),
    storage_sync_logger:log_scan_cancelled(SpaceId, ScanNum, TaskId).

-spec to_string(slave_job() | master_job()) -> binary().
to_string(undefined) ->
    <<"\nundefined\n">>;
to_string(#storage_traverse_slave{
    storage_file_ctx = StorageFileCtx,
    info = #{scan_num := ScanNum}
}) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    str_utils:format_bin(
        " ~nStorage sync slave job:~n"
        "    file: ~p~n"
        "    storage: ~p~n"
        "    space: ~p~n"
        "    scan no.: ~p~n",
        [StorageFileId, StorageId, SpaceId, ScanNum]);
to_string(#storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    info = #{scan_num := ScanNum},
    offset = Offset,
    marker = Marker
}) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    str_utils:format_bin(
        " ~nStorage sync master job:~n"
        "    file: ~p~n"
        "    storage: ~p~n"
        "    space: ~p~n"
        "    scan no.: ~p~n"
        "    offset: ~p~n"
        "    marker: ~p~n",
        [StorageFileId, StorageId, SpaceId, ScanNum, Offset, Marker]).

%%%===================================================================
%%% storage_traverse callbacks
%%%===================================================================

-spec reset_info(master_job()) -> info().
reset_info(#storage_traverse_master{info = Info}) ->
    maps:without([storage_sync_info_doc, file_ctx, add_deletion_detection_link, detect_deletions], Info).

-spec get_next_batch_job_prehook(info()) -> storage_traverse:next_batch_job_prehook().
get_next_batch_job_prehook(_TraverseInfo) ->
    fun(#storage_traverse_master{storage_file_ctx = StorageFileCtx}) ->
        StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
        SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
        storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, 1),
        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
        storage_sync_info:increase_batches_to_process(StorageFileId, SpaceId)
    end.

-spec get_children_master_job_prehook(info()) -> storage_traverse:children_master_job_prehook().
get_children_master_job_prehook(_TraverseInfo) ->
    fun(#storage_traverse_master{storage_file_ctx = StorageFileCtx}) ->
        SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
        storage_sync_info:init_batch_counters(StorageFileId, SpaceId)
    end.

-spec get_fold_children_fun(info()) -> storage_traverse:fold_children_fun().
get_fold_children_fun(TraverseInfo) ->
    SyncAcl = maps:get(sync_acl, TraverseInfo, false),
    fun(StorageFileCtx, Info, HashAcc) ->
        {Hash, StorageFileCtx2} = storage_sync_hash:compute_file_attrs_hash(StorageFileCtx, SyncAcl),
        maybe_add_deletion_detection_link(StorageFileCtx, Info),
        {[Hash | HashAcc], StorageFileCtx2}
    end.

%%%===================================================================
%%% Functions exported for tests
%%%===================================================================

-spec has_mtime_changed(undefined | storage_sync_info:doc(), storage_file_ctx:ctx()) -> boolean().
has_mtime_changed(undefined, _StorageFileCtx) ->
    true;
has_mtime_changed(SSIDoc, StorageFileCtx) ->
    {#statbuf{st_mtime = STMtime}, _} = storage_file_ctx:stat(StorageFileCtx),
    storage_sync_info:get_mtime(SSIDoc) =/= STMtime.

-spec run_deletion_scan(storage_file_ctx:ctx(), non_neg_integer(), space_strategies:config(), file_ctx:ctx(), boolean())
        -> ok.
run_deletion_scan(StorageFileCtx, ScanNum, Config, FileCtx, UpdateSyncCounters) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    SpaceStorageFileId = storage_file_id:space_dir_id(SpaceId, StorageId),
    storage_sync_info:init_batch_counters(SpaceStorageFileId, SpaceId),
    TaskId = encode_task_id(SpaceId, StorageId, ScanNum),
    {MaxDepth, Config2} = maps:take(max_depth, Config),
    TraverseInfo = Config2#{
        scan_num => ScanNum,
        parent_ctx => file_ctx:new_root_ctx(),
        storage_type => storage:get_type(StorageId),
        space_storage_file_id => SpaceStorageFileId,
        file_ctx => FileCtx,
        detect_deletions => true,
        update_sync_counters => UpdateSyncCounters,
        sync_links_token => #link_token{},
        sync_links_children => [],
        file_meta_token => #link_token{},
        file_meta_children => []
    },
    RunOpts = #{
        execute_slave_on_dir => false,
        async_children_master_jobs => true,
        async_next_batch_job => true,
        next_batch_job_prehook => get_next_batch_job_prehook(TraverseInfo),
        children_master_job_prehook => get_children_master_job_prehook(TraverseInfo),
        batch_size => ?SYNC_DIR_BATCH_SIZE,
        max_depth => MaxDepth,
        fold_children_fun => get_fold_children_fun(TraverseInfo),
        fold_children_init => []
    },
    storage_traverse:run(?POOL, TaskId, SpaceId, StorageId, TraverseInfo, RunOpts).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec run(od_space:id(), od_storage:id(), non_neg_integer(), space_strategies:config()) -> ok.
run(SpaceId, StorageId, ScanNum, Config) ->
    SpaceStorageFileId = storage_file_id:space_dir_id(SpaceId, StorageId),
    storage_sync_info:init_batch_counters(SpaceStorageFileId, SpaceId),
    TaskId = encode_task_id(SpaceId, StorageId, ScanNum),
    {MaxDepth, Config2} = maps:take(max_depth, Config),
    TraverseInfo = Config2#{
        scan_num => ScanNum,
        parent_ctx => file_ctx:new_root_ctx(),
        storage_type => storage:get_type(StorageId),
        space_storage_file_id => SpaceStorageFileId
    },
    RunOpts = #{
        execute_slave_on_dir => false,
        async_children_master_jobs => true,
        async_next_batch_job => true,
        next_batch_job_prehook => get_next_batch_job_prehook(TraverseInfo),
        children_master_job_prehook => get_children_master_job_prehook(TraverseInfo),
        batch_size => ?SYNC_DIR_BATCH_SIZE,
        max_depth => MaxDepth,
        fold_children_fun => get_fold_children_fun(TraverseInfo),
        fold_children_init => []
    },
    storage_traverse:run(?POOL, TaskId, SpaceId, StorageId, TraverseInfo, RunOpts).

-spec do_import_master_job(master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} | {error, term()}.
do_import_master_job(TraverseJob = #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    info = Info,
    offset = Offset,
    batch_size = BatchSize
}, Args) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    % we perform slave job on directory manually to ensure that it is executed before jobs
    % for next batches are scheduled asynchronously
    case do_slave_job_on_directory(TraverseJob) of
        {ok, {SyncResult, FileCtx, StorageFileCtx2}} ->
            increase_counter(SyncResult, SpaceId, StorageId),
            % stat result will be cached in StorageFileCtx
            % we perform stat here to ensure that jobs for all batches for given directory
            % will be scheduled with the same stat result
            {#statbuf{st_mtime = MTime}, StorageFileCtx3} = storage_file_ctx:stat(StorageFileCtx2),
            TraverseJob2 = TraverseJob#storage_traverse_master{
                storage_file_ctx = StorageFileCtx3,
                info = Info#{file_ctx => FileCtx}
            },
            case storage_traverse:do_master_job(TraverseJob2, Args) of
                {ok, MasterJobMap, HashesReversed} ->
                    SlaveJobsNum = length(maps:get(slave_jobs, MasterJobMap, [])),
                    AsyncMasterJobsNum = length(maps:get(async_master_jobs, MasterJobMap, [])),
                    ToProcess = SlaveJobsNum + AsyncMasterJobsNum,
                    storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, ToProcess),
                    FinishCallback = ?ON_SUCCESSFUL_SLAVE_JOBS(fun() ->
                        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
                        BatchHash = storage_sync_hash:hash(lists:reverse(HashesReversed)),
                        storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, MTime, Offset, BatchSize, BatchHash)
                    end),
                    {ok, MasterJobMap#{finish_callback => FinishCallback}};
                Error = {error, _} ->
                    Error
            end;
        {error, ?ENOENT} ->
            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
            case storage_file_id:space_dir_id(SpaceId, StorageId) of
                StorageFileId ->
                    % space dir may have not been created on storage yet
                    storage_sync_monitoring:mark_updated_file(SpaceId, StorageId),
                    {ok, #{}};
                _OtherStorageFileId ->
                    storage_sync_monitoring:mark_failed_file(SpaceId, StorageId),
                    {error, ?ENOENT}
            end;
        Error2 = {error, _} ->
            storage_sync_monitoring:mark_failed_file(SpaceId, StorageId),
            Error2
    end.

-spec do_update_master_job(master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} | {error, term()}.
do_update_master_job(TraverseJob = #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    info = Info = #{
        delete_enable := DeleteEnable,
        write_once := WriteOnce,
        parent_ctx := ParentCtx
    }
}, Args) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    case do_slave_job_on_directory(TraverseJob) of
        {ok, {SyncResult, FileCtx, StorageFileCtx2}} ->
            SSIDoc = get_storage_sync_info_doc(TraverseJob),
            % stat result will be cached in StorageFileCtx
            % we perform stat here to ensure that jobs for all batches for given directory
            % will be scheduled with the same stat result
            {#statbuf{}, StorageFileCtx3} = storage_file_ctx:stat(StorageFileCtx2),
            MTimeHasChanged = storage_sync_traverse:has_mtime_changed(SSIDoc, StorageFileCtx3),
            TraverseJob2 = TraverseJob#storage_traverse_master{
                storage_file_ctx = StorageFileCtx3,
                info = Info2 = Info#{
                    file_ctx => FileCtx,
                    storage_sync_info_doc => SSIDoc
                }
            },
            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx3),
            increase_counter(SyncResult, SpaceId, StorageId),

            case {MTimeHasChanged, DeleteEnable, WriteOnce} of
                {true, true, _} ->
                    TraverseJob3 = TraverseJob2#storage_traverse_master{info = Info2#{add_deletion_detection_link => true}},
                    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
                    traverse(TraverseJob3, Args, true);
                {false, _, true} ->
                    % WriteOnce option is enabled and MTime of directory has not changed, therefore
                    % we are sure that hash computed out of children (only regular files) attributes
                    % mustn't have changed
                    traverse_only_directories(TraverseJob2#storage_traverse_master{fold_enabled = false}, Args);
                {_, _, _} ->
                    % Hash of children attrs might have changed, therefore it must be computed
                    traverse(TraverseJob2, Args, false)
            end;
        {error, ?ENOENT} ->
            % directory might have been deleted
            FileName = storage_file_ctx:get_file_name_const(StorageFileCtx),
            storage_sync_monitoring:mark_updated_file(SpaceId, StorageId),
            {FileCtx, ParentCtx2} = file_ctx:get_child(ParentCtx, FileName, user_ctx:new(?ROOT_SESS_ID)),
            FinishCallback = fun(#{master_job_starter_callback := MasterJobCallback}, _SlavesDescription) ->
                storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, 1),
                MasterJobCallback([storage_sync_deletion:get_master_job(TraverseJob#storage_traverse_master{
                    info = Info#{
                        file_ctx => FileCtx,
                        parent_ctx => ParentCtx2
                    }})])
            end,
            {ok, #{finish_callback => FinishCallback}};
        Error = {error, _} ->
            storage_sync_monitoring:mark_failed_file(SpaceId, StorageId),
            Error
    end.

-spec traverse_only_directories(master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} | {error, term()}.
traverse_only_directories(TraverseJob, Args) ->
    case storage_traverse:do_master_job(TraverseJob, Args) of
        {ok, MasterJobMap} ->
            schedule_jobs_for_directories_only(TraverseJob, MasterJobMap, false);
        Error = {error, _} ->
            Error
    end.

-spec traverse(master_job(), traverse:master_job_extended_args(), boolean()) ->
    {ok, traverse:master_job_map()} | {error, term()}.
traverse(TraverseJob = #storage_traverse_master{
    offset = Offset,
    batch_size = BatchSize,
    info = #{storage_sync_info_doc := SSIDoc}
}, Args, DeleteEnable) ->
    case storage_traverse:do_master_job(TraverseJob, Args) of
        {ok, MasterJobMap, HashesReversed} ->
            BatchHash = storage_sync_hash:hash(lists:reverse(HashesReversed)),
            case storage_sync_hash:children_attrs_hash_has_changed(BatchHash, Offset, BatchSize, SSIDoc) of
                true ->
                    schedule_jobs_for_all_files(TraverseJob, MasterJobMap, BatchHash, DeleteEnable);
                false ->
                    % Hash hasn't changed, therefore we can schedule jobs only for directories
                    schedule_jobs_for_directories_only(TraverseJob, MasterJobMap, DeleteEnable)
            end;
        Error = {error, _} ->
            Error
    end.


-spec schedule_jobs_for_directories_only(master_job(), traverse:master_job_map(), boolean()) ->
    {ok, traverse:master_job_map()}.
schedule_jobs_for_directories_only(#storage_traverse_master{storage_file_ctx = StorageFileCtx}, MasterJobMap, false) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    MasterJobMap2 = maps:remove(slave_jobs, MasterJobMap),
    {#statbuf{st_mtime = STMtime}, _StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
    FinishCallback = fun(_MasterJobExtendedArgs, _SlaveJobsDescription) ->
        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
        storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, STMtime)
    end,
    AsyncMasterJobs = maps:get(async_master_jobs, MasterJobMap2, []),
    ToProcess = length(AsyncMasterJobs),
    storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, ToProcess),
    {ok, MasterJobMap2#{finish_callback => FinishCallback}};
schedule_jobs_for_directories_only(TraverseJob = #storage_traverse_master{storage_file_ctx = StorageFileCtx},
    MasterJobMap, true
) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    MasterJobMap2 = maps:remove(slave_jobs, MasterJobMap),
    {#statbuf{st_mtime = STMtime}, _StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),

    FinishCallback = fun(#{master_job_starter_callback := MasterJobCallback}, SlavesDescription) ->
        case maps:get(slave_jobs_failed, SlavesDescription) of
            0 ->
                StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
                {#statbuf{st_mtime = STMtime}, _} = storage_file_ctx:stat(StorageFileCtx),
                {ok, SSI} = storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, STMtime),
                case storage_sync_info:are_all_batches_processed(SSI) of
                    true ->
                        storage_sync_info:increase_batches_to_process(StorageFileId, SpaceId),
                        storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, 1),
                        MasterJobCallback([storage_sync_deletion:get_master_job(TraverseJob)]);
                    false ->
                        ok
                end;
            _ ->
                ok
        end
    end,
    AsyncMasterJobs = maps:get(async_master_jobs, MasterJobMap2, []),
    ToProcess = length(AsyncMasterJobs),
    storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, ToProcess),
    {ok, MasterJobMap2#{finish_callback => FinishCallback}}.

-spec schedule_jobs_for_all_files(master_job(), traverse:master_job_map(), storage_sync_hash:hash(),
    DeleteEnable :: boolean()) -> {ok, traverse:master_job_map()}.
schedule_jobs_for_all_files(#storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    offset = Offset,
    batch_size = BatchSize
}, MasterJobMap, BatchHash, false
) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    AsyncMasterJobs = maps:get(async_master_jobs, MasterJobMap, []),
    SlaveJobs = maps:get(slave_jobs, MasterJobMap, []),
    ToProcess = length(AsyncMasterJobs) + length(SlaveJobs),
    storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, ToProcess),
    FinishCallback = ?ON_SUCCESSFUL_SLAVE_JOBS(fun() ->
        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
        {#statbuf{st_mtime = STMtime}, _} = storage_file_ctx:stat(StorageFileCtx),
        storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, STMtime, Offset, BatchSize, BatchHash)
    end),
    {ok, MasterJobMap#{finish_callback => FinishCallback}};
schedule_jobs_for_all_files(TraverseJob = #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    offset = Offset,
    batch_size = BatchSize
}, MasterJobMap, BatchHash, true
) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    AsyncMasterJobs = maps:get(async_master_jobs, MasterJobMap, []),
    SlaveJobs = maps:get(slave_jobs, MasterJobMap, []),
    ToProcess = length(AsyncMasterJobs) + length(SlaveJobs),
    storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, ToProcess),
    FinishCallback = fun(#{master_job_starter_callback := MasterJobCallback}, SlavesDescription) ->
        case maps:get(slave_jobs_failed, SlavesDescription) of
            0 ->
                StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
                {#statbuf{st_mtime = STMtime}, _} = storage_file_ctx:stat(StorageFileCtx),
                {ok, SSI} = storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, STMtime, Offset, BatchSize, BatchHash, false),
                case storage_sync_info:are_all_batches_processed(SSI) of
                    true ->
                        storage_sync_info:increase_batches_to_process(StorageFileId, SpaceId),
                        storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, 1),
                        MasterJobCallback([storage_sync_deletion:get_master_job(TraverseJob)]);
                    false ->
                        ok
                end;
            _ ->
                ok
        end
    end,
    {ok, MasterJobMap#{finish_callback => FinishCallback}}.

-spec encode_task_id(od_space:id(), od_storage:id(), non_neg_integer()) -> traverse:id().
encode_task_id(SpaceId, StorageId, ScanNum) ->
    str_utils:join_binary([?TASK_ID_PREFIX, SpaceId, StorageId, integer_to_binary(ScanNum)], ?TASK_ID_SEP).

-spec decode_task_id(traverse:id()) -> {od_space:id(), od_storage:id(), non_neg_integer()}.
decode_task_id(TaskId) ->
    [_Prefix, SpaceId, StorageId, ScanNum] = binary:split(TaskId, ?TASK_ID_SEP, [global]),
    {SpaceId, StorageId, binary_to_integer(ScanNum)}.

-spec do_slave_job_on_directory(master_job()) ->
    {ok, {storage_sync_engine:result(), file_ctx:ctx(), storage_file_ctx:ctx()}} | {error, term()}.
do_slave_job_on_directory(#storage_traverse_master{
    offset = 0,
    storage_file_ctx = StorageFileCtx,
    info = Info
}) ->
    % slave job on dir should be executed just once
    process_storage_file(StorageFileCtx, Info);
do_slave_job_on_directory(#storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    info = #{file_ctx := FileCtx}
}) ->
    {ok, {processed, FileCtx, StorageFileCtx}}.

-spec process_storage_file(storage_file_ctx:ctx(), info()) ->
    {ok, storage_sync_engine:result()} | {error, term()}.
process_storage_file(StorageFileCtx, Info) ->
    % processed file can be a regular file or a directory
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    try
        case storage_sync_engine:process_file(StorageFileCtx, Info) of
            Result = {SyncResult, _, _}
                when SyncResult =:= imported
                orelse SyncResult =:= updated
                orelse SyncResult =:= processed
            ->
                {ok, Result};
            Error = {error, _} ->
                ?error(
                    "Syncing file ~p on storage ~p supported by space ~p failed due to ~w.",
                    [StorageFileId, StorageId, SpaceId, Error]),
                Error
        end
    catch
        throw:?ENOENT ->
            ?warning_stacktrace(
                "Syncing file ~p on storage ~p supported by space ~p failed due to ENOENT.",
                [StorageFileId, StorageId, SpaceId]),
            {error, ?ENOENT};
        Error2:Reason2 ->
            ?error_stacktrace(
                "Syncing file ~p on storage ~p supported by space ~p failed due to ~w:~w.",
                [StorageFileId, StorageId, SpaceId, Error2, Reason2]),
            {error, {Error2, Reason2}}
    end.

-spec increase_counter(storage_sync_engine:result(), od_space:id(), od_storage:id()) -> ok.
increase_counter(imported, SpaceId, StorageId) ->
    storage_sync_monitoring:mark_imported_file(SpaceId, StorageId);
increase_counter(updated, SpaceId, StorageId) ->
    storage_sync_monitoring:mark_updated_file(SpaceId, StorageId);
increase_counter(processed, SpaceId, StorageId) ->
    storage_sync_monitoring:mark_processed_file(SpaceId, StorageId).

-spec get_storage_sync_info_doc(master_job()) -> storage_sync_info:doc().
get_storage_sync_info_doc(#storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    offset = 0
}) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    % storage_sync_info doc is fetch just once per directory, for its first batch
    case storage_sync_info:get(StorageFileId, SpaceId) of
        {error, not_found} -> undefined;
        {ok, SSIDoc} -> SSIDoc
    end;
get_storage_sync_info_doc(#storage_traverse_master{
    info = #{storage_sync_info_doc := SSIDoc}
}) ->
    % for other batches it is cached in #storage_traverse.info map
    SSIDoc.

-spec maybe_add_deletion_detection_link(storage_file_ctx:ctx(), info()) -> ok.
maybe_add_deletion_detection_link(StorageFileCtx, Info = #{space_storage_file_id := SpaceStorageFileId}) ->
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    ParentStorageFileCtx = storage_file_ctx:get_parent_ctx_const(StorageFileCtx),
    ParentStorageFileId = storage_file_ctx:get_storage_file_id_const(ParentStorageFileCtx),
    {ParentStorageFileId2, MarkLeaves} = case maps:get(storage_type, Info) of
        ?OBJECT_STORAGE ->
            %% When storage_sync is run on object storages we set MarkLeaves parameter to true
            %% in the below call which allows to determine when link is associated with
            %% a regular file which is necessary for storage_sync_deletion on object storages
            {SpaceStorageFileId, true};
        ?BLOCK_STORAGE ->
            %% On the other hand, on block storages MarkLeaves = false.
            %% storage_sync_deletion on block storages processes only direct children of a directory
            %% so it does not need to traverse recursively down the file structure.
            %% It also allows to create storage_sync_link for directory and its children simultaneously
            %% as we are sure that value of a subdirectory link won't be set to undefined.
            {ParentStorageFileId, false}
    end,
    case maps:get(add_deletion_detection_link, Info, false) of
        true ->
            storage_sync_links:add_link_recursive(ParentStorageFileId2, StorageId, StorageFileId, MarkLeaves);
        _ ->
            ok
    end.

-spec scan_finished(od_space:id(), od_storage:id()) -> ok.
scan_finished(SpaceId, StorageId) ->
    SpaceStorageFileId = storage_file_id:space_dir_id(SpaceId, StorageId),
    ok = storage_sync_links:delete_recursive(SpaceStorageFileId, StorageId),
    storage_sync_monitoring:mark_finished_scan(SpaceId, StorageId),
    ok.