%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for auto traversing imported storages which
%%% support spaces with ?AUTO mode of storage import.
%%% For more info on storage import go to storage_import module.
%%%
%%% It uses storage_traverse framework to traverse all files visible on storage
%%% and to schedule jobs for synchronizing them.
%%%
%%% The mechanism uses the following helper modules:
%%%   * storage_import_engine - each storage file is synchronized by call
%%%                         to storage_import_engine:process_file/2 function
%%%   * storage_sync_links - helper module which implements links used by storage import to compare
%%%                         lists of files on storage with lists of files in the Onedata system
%%%   * storage_import_deletion - module responsible for detecting which files in the
%%%                         synchronized space were deleted on storage and therefore should be
%%%                         deleted from the Onedata file system
%%%   * storage_import_monitoring - this module is used to store data for monitoring
%%%                         performance of storage import
%%%   * storage_sync_info - a helper module that implements a model that is
%%%                         used store information (timestamp of last stat operation, last synced mtime,
%%%                         last computed hash of children attrs) required by sync to determine
%%%                         whether there were changes introduced to file or children
%%%                         files on storage since last scan.
%%%   * storage_import_hash - helper module used to compute hashes of children file attributes.
%%%                         Those hashes are used to improve sync performance by allowing to easily
%%%                         decide whether files have been modified on storage.
%%%   * storage_import_logger - helper module used for writing information
%%%                         about sync scans to audit log file
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_traverse).
-author("Jakub Kudzia").

-behaviour(traverse_behaviour).
-behaviour(storage_traverse).

-include("global_definitions.hrl").
-include("modules/storage/traverse/storage_traverse.hrl").
-include("modules/storage/import/storage_import.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/acl.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%%%===================================================================
%%% Types
%%%===================================================================

-type master_job() :: storage_traverse:master_job().
-type slave_job() :: storage_traverse:slave_job().
-type info() :: #{atom() => term()}.

%%%===================================================================
%%% Exports
%%%===================================================================

%% Types
-export_type([info/0, master_job/0, slave_job/0]).

%% API
-export([init_pool/0, stop_pool/0, run_scan/1, run_scan/2, cancel/1, fix_stalled_scans/1]).

%% Pool callbacks
-export([do_master_job/2, do_slave_job/2, get_job/1, update_job_progress/5, to_string/1,
    task_started/2, task_finished/2, task_canceled/2]).

%% storage_traverse callbacks
-export([reset_info/1, get_next_batch_job_prehook/1, get_children_master_job_prehook/1,
    get_fold_children_fun/1, on_cancel/3]).

%% exported for tests
-export([has_mtime_changed/2, run/4, run_deletion_scan/4]).

%%%===================================================================
%%% Macros
%%%===================================================================

-define(POOL, ?MODULE).
-define(POOL_BIN, atom_to_binary(?POOL, utf8)).
-define(TASK_ID_SEP, <<"###">>).
-define(TASK_ID_PREFIX, <<"storage_sync">>).

-define(BATCH_SIZE, op_worker:get_env(storage_import_dir_batch_size, 1000)).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_pool() -> ok.
init_pool() ->
    % Get pool limits from app.config
    MasterJobsLimit = op_worker:get_env(storage_import_master_jobs_limit, 10),
    SlaveJobsLimit = op_worker:get_env(storage_import_slave_workers_limit, 10),
    ParallelSyncedSpacesLimit = op_worker:get_env(storage_import_parallel_synced_spaces_limit, 10),
    storage_traverse:init(?POOL, MasterJobsLimit, SlaveJobsLimit, ParallelSyncedSpacesLimit, ?MODULE).

-spec stop_pool() -> ok.
stop_pool() ->
    storage_traverse:stop(?POOL).


-spec run_scan(od_space:id()) -> ok | {error, term()}.
run_scan(SpaceId) ->
    run_scan(SpaceId, undefined).

-spec run_scan(od_space:id(), storage_import:scan_config() | undefined) -> ok | {error, term()}.
run_scan(SpaceId, ScanConfig) ->
    {ok, StorageId} = space_logic:get_local_supporting_storage(SpaceId),
    case storage_import_monitoring:prepare_new_scan(SpaceId) of
        {ok, SSM} ->
            {ok, ScansNum} = storage_import_monitoring:get_finished_scans_num(SSM),
            ?debug("Starting auto storage import scan no. ~tp for space: ~tp and storage: ~tp", [ScansNum + 1, SpaceId, StorageId]),
            ScanConfigMap = ensure_scan_config_is_map(SpaceId, ScanConfig),
            try
                run(SpaceId, StorageId, ScansNum + 1, ScanConfigMap)
            catch
                Class:Reason:Stacktrace  ->
                    scan_finished(SpaceId, StorageId, true),
                    ?examine_exception(Class, Reason, Stacktrace)
            end;
        {error, already_started} = Error ->
            Error
    end.

-spec cancel(od_space:id()) -> ok | {error, term()}.
cancel(SpaceId) ->
    {ok, StorageId} = space_logic:get_local_supporting_storage(SpaceId),
    case storage_import_monitoring:get(SpaceId) of
        {ok, SSMDoc} ->
            case storage_import_monitoring:is_scan_in_progress(SSMDoc) of
                true ->
                    storage_import_monitoring:set_aborting_status(SpaceId),
                    {ok, ScansNum} = storage_import_monitoring:get_finished_scans_num(SSMDoc),
                    traverse:cancel(?POOL_BIN, encode_task_id(SpaceId, StorageId, ScansNum + 1));
                false ->
                    {error, not_found}
            end;
        {error, not_found} ->
            ?debug("Cannot cancel auto storage import scan for space ~tp and storage ~tp as it is not configured",
                [SpaceId, StorageId]),
            {error, not_found}
    end.


%%--------------------------------------------------------------------
%% @doc
%% This function takes list of provider's spaces (SpaceIds) as an argument.
%%
%% First it lists ongoing storage_sync_traverse tasks and extracts ids of
%% corresponding spaces. The resulting list of space ids is called
%% CorrectSpacesWithStalledScans because scans for these spaces will be
%% restarted by the traverse mechanism.
%%
%% Then it filters provider's SpaceIds according to the storage_import_monitoring
%% data so that only auto imported spaces with stalled (in progress) scans
%% are left. The resulting list is called SpacesWithStalledScans as these
%% are ALL spaces for which scans are marked as "in progress".
%%
%% Next, spaces that exist ONLY in the SpacesWithStalledScans list
%% and are MISSING in the CorrectSpacesWithStalledScans are extracted.
%% The resulting list is called BrokenSpacesWithStalledScans as these
%% are ids of spaces for which traverse_task links are missing and they
%% won't be restarted by the traverse mechanism.
%%
%% Eventually, storage_import_monitoring docs are fixed for each space from
%% BrokenSpacesWithStalledScans which means that their scans
%% as marked as "finished".
%% @end
%%--------------------------------------------------------------------
-spec fix_stalled_scans([od_space:id()]) -> ok | {error, term()}.
fix_stalled_scans(SpaceIds) ->
    case storage_traverse:list_ongoing_tasks(?POOL_BIN) of
        {ok, StalledScanIds} ->
            % correct below means that there exists a traverse_task link and document for given scan
            % and that the scan will be restarted by traverse
            CorrectSpacesWithStalledScans = lists:filtermap(fun(StalledScanId) ->
                case traverse_task:get(?POOL_BIN, StalledScanId) of
                    {ok, _} -> {true, element(1, decode_task_id(StalledScanId))};
                    {error, not_found} -> false
                end
            end, StalledScanIds),
            SpacesWithStalledScans = lists:filter(fun(SpaceId) ->
                storage_import:is_auto_imported(SpaceId) andalso storage_import_monitoring:is_scan_in_progress(SpaceId)
            end, SpaceIds),
            BrokenSpacesWithStalledScans = SpacesWithStalledScans -- CorrectSpacesWithStalledScans,
            lists:foreach(fun(SpaceId) ->
                storage_import_monitoring:mark_finished_scan(SpaceId, true),
                ?info("Aborted stalled scan of auto storage import in the space ~ts", [SpaceId])
            end, BrokenSpacesWithStalledScans);
        {error, _} = Error ->
            Error
    end.


%===================================================================
% Pool callbacks
%===================================================================

-spec do_master_job(master_job(), traverse:master_job_extended_args()) -> {ok, traverse:master_job_map()}.
do_master_job(TraverseJob = #storage_traverse_master{storage_file_ctx = StorageFileCtx}, Args) ->
    try
        do_master_job_unsafe(TraverseJob, Args)
    catch
        throw:?ENOENT ->
            SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
            storage_import_monitoring:mark_unmodified_file(SpaceId),
            {ok, #{}}
    end.

-spec do_slave_job(slave_job(), traverse:id()) -> ok | {error, term()}.
do_slave_job(Job = #storage_traverse_slave{info = #{deletion_job := true}}, Task) ->
    storage_import_deletion:do_slave_job(Job, Task);
do_slave_job(#storage_traverse_slave{
    storage_file_ctx = StorageFileCtx,
    info = Info
}, _Task) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    case process_storage_file(StorageFileCtx, Info) of
        {ok, {SyncResult, _, _}} ->
            increment_counter(SyncResult, SpaceId);
        {error, ?ENOENT} ->
            storage_import_monitoring:mark_unmodified_file(SpaceId),
            ok;
        Error = {error, _} ->
            storage_import_monitoring:mark_failed_file(SpaceId),
            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
            storage_import_logger:log_failure(StorageFileId, Error, SpaceId),
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
    storage_import_monitoring:mark_started_scan(SpaceId),
    storage_import_logger:log_scan_started(SpaceId, ScanNum, TaskId),
    auto_storage_import_worker:notify_started_scan(SpaceId).

-spec task_finished(traverse:id(), traverse:pool()) -> ok.
task_finished(TaskId, _PoolName) ->
    {SpaceId, StorageId, ScanNum} = decode_task_id(TaskId),
    scan_finished(SpaceId, StorageId, false),
    storage_import_logger:log_scan_finished(SpaceId, ScanNum, TaskId).

-spec task_canceled(traverse:id(), traverse:pool()) -> ok.
task_canceled(TaskId, _PoolName) ->
    {SpaceId, StorageId, ScanNum} = decode_task_id(TaskId),
    scan_finished(SpaceId, StorageId, true),
    storage_import_logger:log_scan_cancelled(SpaceId, ScanNum, TaskId).

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
        " ~nStorage import slave job:~n"
        "    file: ~tp~n"
        "    storage: ~tp~n"
        "    space: ~tp~n"
        "    scan no.: ~tp~n",
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
        " ~nStorage import master job:~n"
        "    file: ~tp~n"
        "    storage: ~tp~n"
        "    space: ~tp~n"
        "    scan no.: ~tp~n"
        "    offset: ~tp~n"
        "    marker: ~tp~n",
        [StorageFileId, StorageId, SpaceId, ScanNum, Offset, Marker]).

%%%===================================================================
%%% storage_traverse callbacks
%%%===================================================================

-spec reset_info(master_job()) -> info().
reset_info(#storage_traverse_master{info = Info}) ->
    maps:without([storage_sync_info_doc, file_ctx, add_deletion_detection_link, deletion_job], Info).

-spec get_next_batch_job_prehook(info()) -> storage_traverse:next_batch_job_prehook().
get_next_batch_job_prehook(_TraverseInfo) ->
    fun(#storage_traverse_master{storage_file_ctx = StorageFileCtx}) ->
        SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
        storage_import_monitoring:increment_queue_length_histograms(SpaceId, 1),
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
        {Hash, StorageFileCtx2} = storage_import_hash:compute_file_attrs_hash(StorageFileCtx, SyncAcl),
        maybe_add_deletion_detection_link(StorageFileCtx, Info),
        {[Hash | HashAcc], StorageFileCtx2}
    end.


-spec on_cancel(non_neg_integer(), non_neg_integer(), storage_file_ctx:ctx()) -> ok.
on_cancel(MasterJobsDelegated, SlaveJobsDelegated, StorageFileCtx) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    storage_import_monitoring:mark_unmodified_files(SpaceId, MasterJobsDelegated + SlaveJobsDelegated).

%%%===================================================================
%%% Functions exported for tests
%%%===================================================================

-spec has_mtime_changed(undefined | storage_sync_info:doc(), storage_file_ctx:ctx()) -> boolean().
has_mtime_changed(undefined, _StorageFileCtx) ->
    true;
has_mtime_changed(SSIDoc, StorageFileCtx) ->
    {#statbuf{st_mtime = STMtime}, _} = storage_file_ctx:stat(StorageFileCtx),
    storage_sync_info:get_mtime(SSIDoc) =/= STMtime.

-spec run_deletion_scan(storage_file_ctx:ctx(), non_neg_integer(), storage_import:scan_config_map(), file_ctx:ctx()) -> ok.
run_deletion_scan(StorageFileCtx, ScanNum, Config, FileCtx) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    SpaceStorageFileId = storage_file_id:space_dir_id(SpaceId, StorageId),
    storage_sync_info:init_batch_counters(SpaceStorageFileId, SpaceId),
    TaskId = encode_task_id(SpaceId, StorageId, ScanNum),
    {MaxDepth, Config2} = maps:take(max_depth, Config),
    TraverseInfo = Config2#{
        scan_num => ScanNum,
        parent_ctx => file_ctx:new_root_ctx(),
        is_posix_storage => storage:is_posix_compatible(StorageId),
        iterator_type => storage_traverse:get_iterator(StorageId),
        space_storage_file_id => SpaceStorageFileId,
        file_ctx => FileCtx,
        deletion_job => true,
        sync_links_token => #link_token{},
        sync_links_children => [],
        file_meta_token => undefined,
        file_meta_children => [],
        manual => false
    },
    RunOpts = #{
        execute_slave_on_dir => false,
        async_children_master_jobs => true,
        async_next_batch_job => true,
        next_batch_job_prehook => get_next_batch_job_prehook(TraverseInfo),
        children_master_job_prehook => get_children_master_job_prehook(TraverseInfo),
        batch_size => ?BATCH_SIZE,
        max_depth => MaxDepth,
        fold_children_fun => get_fold_children_fun(TraverseInfo),
        fold_children_init => []
    },
    storage_traverse:run(?POOL, TaskId, SpaceId, StorageId, TraverseInfo, RunOpts).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec do_master_job_unsafe(master_job(), traverse:master_job_extended_args()) -> {ok, traverse:master_job_map()}.
do_master_job_unsafe(DeletionJob = #storage_traverse_master{info = #{deletion_job := true}}, Args) ->
    storage_import_deletion:do_master_job(DeletionJob, Args);
do_master_job_unsafe(TraverseJob = #storage_traverse_master{info = #{scan_num := 1}}, Args) ->
    do_import_master_job(TraverseJob, Args);
do_master_job_unsafe(TraverseJob = #storage_traverse_master{info = #{scan_num := ScanNum}}, Args) when ScanNum >= 1 ->
    do_update_master_job(TraverseJob, Args).

-spec run(od_space:id(), storage:id(), non_neg_integer(), storage_import:scan_config_map()) -> ok.
run(SpaceId, StorageId, ScanNum, Config) ->
    SpaceStorageFileId = storage_file_id:space_dir_id(SpaceId, StorageId),
    storage_sync_info:init_batch_counters(SpaceStorageFileId, SpaceId),
    TaskId = encode_task_id(SpaceId, StorageId, ScanNum),
    {MaxDepth, Config2} = maps:take(max_depth, Config),
    TraverseInfo = Config2#{
        scan_num => ScanNum,
        parent_ctx => file_ctx:new_root_ctx(),
        is_posix_storage => storage:is_posix_compatible(StorageId),
        iterator_type => storage_traverse:get_iterator(StorageId),
        space_storage_file_id => SpaceStorageFileId,
        manual => false
    },
    RunOpts = #{
        execute_slave_on_dir => false,
        async_children_master_jobs => true,
        async_next_batch_job => true,
        next_batch_job_prehook => get_next_batch_job_prehook(TraverseInfo),
        children_master_job_prehook => get_children_master_job_prehook(TraverseInfo),
        batch_size => ?BATCH_SIZE,
        max_depth => MaxDepth,
        fold_children_fun => get_fold_children_fun(TraverseInfo),
        fold_children_init => []
    },
    storage_traverse:run(?POOL, TaskId, SpaceId, StorageId, TraverseInfo, RunOpts).

-spec do_import_master_job(master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} | {error, term()}.
do_import_master_job(TraverseJob = #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    info = Info = #{parent_ctx := ParentCtx},
    offset = Offset,
    batch_size = BatchSize,
    depth = Depth,
    max_depth = MaxDepth
}, Args) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    % we perform slave job on directory manually to ensure that it is executed before jobs
    % for next batches are scheduled asynchronously
    case do_slave_job_on_directory(TraverseJob) of
        {ok, {SyncResult, undefined, _StorageFileCtx2}} ->
            increment_counter(SyncResult, SpaceId),
            {ok, #{}};
        {ok, {SyncResult, FileCtx, StorageFileCtx2}} ->
            increment_counter(SyncResult, SpaceId),
            % stat result will be cached in StorageFileCtx
            % we perform stat here to ensure that jobs for all batches for given directory
            % will be scheduled with the same stat result
            {#statbuf{st_mtime = MTime}, StorageFileCtx3} = storage_file_ctx:stat(StorageFileCtx2),
            TraverseJob2 = TraverseJob#storage_traverse_master{
                storage_file_ctx = StorageFileCtx3,
                info = Info#{
                    file_ctx => FileCtx,
                    % this job will be used to generated children jobs so set current FileCtx as parent
                    parent_ctx => FileCtx
                }
            },
            case storage_traverse:do_master_job(TraverseJob2, Args) of
                {ok, MasterJobMap, HashesReversed} ->
                    SlaveJobsNum = length(maps:get(slave_jobs, MasterJobMap, [])),
                    AsyncMasterJobsNum = length(maps:get(async_master_jobs, MasterJobMap, [])),
                    ToProcess = SlaveJobsNum + AsyncMasterJobsNum,
                    storage_import_monitoring:increment_queue_length_histograms(SpaceId, ToProcess),
                    Guid = file_ctx:get_logical_guid_const(FileCtx),
                    FinishCallback = ?ON_SUCCESSFUL_SLAVE_JOBS(fun() ->
                        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
                        case Depth =:= MaxDepth of
                            true ->
                                storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, Guid, undefined);
                            false ->
                                BatchHash = storage_import_hash:hash(lists:reverse(HashesReversed)),
                                storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, Guid, MTime, Offset,
                                    BatchSize, BatchHash)
                        end
                    end),
                    {ok, MasterJobMap#{finish_callback => FinishCallback}};
                {error, ?ENOENT} ->
                    {ok, #{}};
                Error = {error, _} ->
                    Error
            end;
        Error2 = {error, ?ENOENT} ->
            % directory might have been deleted after it was listed in parent's master job
            case file_ctx:is_root_dir_const(ParentCtx) of
                true ->
                    % Space directory wasn't found on storage
                    ?error("Storage import has failed because directory of space ~ts was not found on storage ~ts.",
                        [SpaceId, StorageId]),
                    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
                    storage_import_logger:log_failure(StorageFileId, Error2, SpaceId),
                    storage_import_monitoring:mark_failed_file(SpaceId),
                    {ok, #{}};
                false ->
                    storage_import_monitoring:mark_unmodified_file(SpaceId),
                    {error, ?ENOENT}
            end;
        Error2 = {error, _} ->
            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
            storage_import_logger:log_failure(StorageFileId, Error2, SpaceId),
            storage_import_monitoring:mark_failed_file(SpaceId),
            Error2
    end.

-spec do_update_master_job(master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} | {error, term()}.
do_update_master_job(TraverseJob = #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    info = Info = #{
        detect_deletions := DetectDeletions,
        detect_modifications := DetectModifications,
        parent_ctx := ParentCtx
    }
}, Args) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    case do_slave_job_on_directory(TraverseJob) of
        {ok, {SyncResult, undefined, _StorageFileCtx2}} ->
            increment_counter(SyncResult, SpaceId),
            {ok, #{}};
        {ok, {SyncResult, FileCtx, StorageFileCtx2}} ->
            {FileDoc, FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),
            {ok, ProtectionFlags} = dataset_eff_cache:get_eff_protection_flags(FileDoc),
            case ProtectionFlags =/= ?no_flags_mask of
                true ->
                    % do not schedule jobs for children as directory is protected
                    {ok, #{}};
                false ->
                    SSIDoc = get_storage_sync_info_doc(TraverseJob),
                    % stat result will be cached in StorageFileCtx
                    % we perform stat here to ensure that jobs for all batches for given directory
                    % will be scheduled with the same stat result
                    {#statbuf{}, StorageFileCtx3} = storage_file_ctx:stat(StorageFileCtx2),
                    MTimeHasChanged = storage_sync_traverse:has_mtime_changed(SSIDoc, StorageFileCtx3),
                    TraverseJob2 = TraverseJob#storage_traverse_master{
                        storage_file_ctx = StorageFileCtx3,
                        info = Info2 = Info#{
                            file_ctx => FileCtx2,
                            storage_sync_info_doc => SSIDoc,
                            % this job will be used to generated children jobs so set current FileCtx as parent
                            parent_ctx => FileCtx2
                        }
                    },
                    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx3),
                    increment_counter(SyncResult, SpaceId),

                    case {MTimeHasChanged, DetectDeletions, DetectModifications} of
                        {true, true, _} ->
                            TraverseJob3 = TraverseJob2#storage_traverse_master{info = Info2#{add_deletion_detection_link => true}},
                            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
                            traverse(TraverseJob3, Args, true);
                        {false, _, false} ->
                            % DetectModifications option is disabled and MTime of directory has not changed, therefore
                            % we are sure that hash computed out of children (only regular files) attributes
                            % mustn't have changed
                            traverse_only_directories(TraverseJob2#storage_traverse_master{fold_enabled = false}, Args);
                        {_, _, _} ->
                            % Hash of children attrs might have changed, therefore it must be computed
                            % Do not detect deletions as (MtimeHasChanged and DetectDeletions) = false
                            traverse(TraverseJob2, Args, false)
                    end
            end;
        Error = {error, ?ENOENT} ->
            % directory might have been deleted after it was listed in parent's master job
            case file_ctx:is_root_dir_const(ParentCtx) of
                true ->
                    % space directory is missing
                    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
                    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
                    ?error("Storage import has failed because directory of space ~ts was not found on storage ~ts.",
                        [SpaceId, StorageId]),
                    storage_import_logger:log_failure(StorageFileId, Error, SpaceId),
                    storage_import_monitoring:mark_failed_file(SpaceId),
                    {ok, #{}};
                false ->
                    % directory was deleted after listing it on storage
                    % ensure it's deleted from the space
                    FileName = storage_file_ctx:get_file_name_const(StorageFileCtx),
                    storage_import_monitoring:mark_processed_job(SpaceId),
                    {FileCtx, ParentCtx2} = file_tree:get_child(ParentCtx, FileName, user_ctx:new(?ROOT_SESS_ID)),
                    FinishCallback = fun(#{master_job_starter_callback := MasterJobCallback}, _SlavesDescription) ->
                        storage_import_monitoring:increment_queue_length_histograms(SpaceId, 1),
                        MasterJobCallback(#{
                            jobs => [
                                storage_import_deletion:get_master_job(
                                    TraverseJob#storage_traverse_master{
                                        info = Info#{
                                            file_ctx => FileCtx,
                                            parent_ctx => ParentCtx2
                                        }})
                            ],
                            cancel_callback => fun(_CancelDescription) ->
                                storage_import_monitoring:mark_unmodified_file(SpaceId)
                            end
                        })
                    end,
                    {ok, #{finish_callback => FinishCallback}}
            end;
        Error = {error, _} ->
            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
            storage_import_logger:log_failure(StorageFileId, Error, SpaceId),
            storage_import_monitoring:mark_failed_file(SpaceId),
            Error
    end.

-spec traverse_only_directories(master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} | {error, term()}.
traverse_only_directories(TraverseJob, Args) ->
    case storage_traverse:do_master_job(TraverseJob, Args) of
        {ok, MasterJobMap} ->
            schedule_jobs_for_directories_only(TraverseJob, MasterJobMap, false);
        {error, ?ENOENT} ->
            {ok, #{}};
        Error = {error, _} ->
            Error
    end.

-spec traverse(master_job(), traverse:master_job_extended_args(), boolean()) ->
    {ok, traverse:master_job_map()} | {error, term()}.
traverse(TraverseJob = #storage_traverse_master{
    offset = Offset,
    batch_size = BatchSize,
    info = #{storage_sync_info_doc := SSIDoc}
}, Args, DetectDeletions) ->
    case storage_traverse:do_master_job(TraverseJob, Args) of
        {ok, MasterJobMap, HashesReversed} ->
            BatchHash = storage_import_hash:hash(lists:reverse(HashesReversed)),
            case storage_import_hash:children_attrs_hash_has_changed(BatchHash, Offset, BatchSize, SSIDoc) of
                true ->
                    schedule_jobs_for_all_files(TraverseJob, MasterJobMap, BatchHash, DetectDeletions);
                false ->
                    % Hash hasn't changed, therefore we can schedule jobs only for directories
                    schedule_jobs_for_directories_only(TraverseJob, MasterJobMap, DetectDeletions)
            end;
        {error, ?ENOENT} ->
            {ok, #{}};
        Error = {error, _} ->
            Error
    end.


-spec schedule_jobs_for_directories_only(master_job(), traverse:master_job_map(), boolean()) ->
    {ok, traverse:master_job_map()}.
schedule_jobs_for_directories_only(#storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    depth = Depth,
    max_depth = MaxDepth,
    info = #{file_ctx := FileCtx}
}, MasterJobMap, false) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    {IgnoredSlaveJobsNum, MasterJobMap2} =  case maps:take(slave_jobs, MasterJobMap) of
        error -> {0, MasterJobMap};
        {SlaveJobs, MJM} -> {length(SlaveJobs), MJM}
    end,
    {#statbuf{st_mtime = STMtime}, _StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
    FinishCallback = fun(_MasterJobExtendedArgs, _SlaveJobsDescription) ->
        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
        Guid = file_ctx:get_logical_guid_const(FileCtx),
        case Depth =:= MaxDepth of
            true ->
                storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, Guid, undefined);
            false ->
                storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, Guid, STMtime)
        end
    end,
    AsyncMasterJobs = maps:get(async_master_jobs, MasterJobMap2, []),
    ToProcess = length(AsyncMasterJobs),
    storage_import_monitoring:mark_unmodified_files_and_increment_queue_length_histograms(
        SpaceId, IgnoredSlaveJobsNum, ToProcess),
    {ok, MasterJobMap2#{finish_callback => FinishCallback}};
schedule_jobs_for_directories_only(TraverseJob = #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    depth = Depth,
    max_depth = MaxDepth,
    info = #{file_ctx := FileCtx}
},
    MasterJobMap, true
) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    {IgnoredSlaveJobsNum, MasterJobMap2} =  case maps:take(slave_jobs, MasterJobMap) of
        error -> {0, MasterJobMap};
        {SlaveJobs, MJM} -> {length(SlaveJobs), MJM}
    end,
    {#statbuf{st_mtime = STMtime}, _StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),

    FinishCallback = fun(#{master_job_starter_callback := MasterJobCallback}, SlavesDescription) ->
        case maps:get(slave_jobs_failed, SlavesDescription) of
            0 ->
                StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
                Guid = file_ctx:get_logical_guid_const(FileCtx),
                {#statbuf{st_mtime = STMtime}, _} = storage_file_ctx:stat(StorageFileCtx),
                {ok, SSI} = case Depth =:= MaxDepth of
                    true ->
                        storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, Guid, undefined);
                    false ->
                        storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, Guid, STMtime, undefined, undefined, undefined, false)
                end,
                case storage_sync_info:are_all_batches_processed(SSI) of
                    true ->
                        storage_sync_info:increase_batches_to_process(StorageFileId, SpaceId),
                        storage_import_monitoring:increment_queue_length_histograms(SpaceId, 1),
                        MasterJobCallback(#{
                            jobs => [storage_import_deletion:get_master_job(TraverseJob)],
                            cancel_callback => fun(_CancelDescription) ->
                                storage_import_monitoring:mark_unmodified_file(SpaceId)
                            end
                        });
                    false ->
                        ok
                end;
            _ ->
                ok
        end
    end,
    AsyncMasterJobs = maps:get(async_master_jobs, MasterJobMap2, []),
    ToProcess = length(AsyncMasterJobs),
    storage_import_monitoring:mark_unmodified_files_and_increment_queue_length_histograms(
        SpaceId, IgnoredSlaveJobsNum, ToProcess),
    {ok, MasterJobMap2#{finish_callback => FinishCallback}}.

-spec schedule_jobs_for_all_files(master_job(), traverse:master_job_map(), storage_import_hash:hash(),
    DetectDeletions :: boolean()) -> {ok, traverse:master_job_map()}.
schedule_jobs_for_all_files(#storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    offset = Offset,
    batch_size = BatchSize,
    depth = Depth,
    max_depth = MaxDepth,
    info = #{file_ctx := FileCtx}
}, MasterJobMap, BatchHash, false
) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    AsyncMasterJobs = maps:get(async_master_jobs, MasterJobMap, []),
    SlaveJobs = maps:get(slave_jobs, MasterJobMap, []),
    ToProcess = length(AsyncMasterJobs) + length(SlaveJobs),
    storage_import_monitoring:increment_queue_length_histograms(SpaceId, ToProcess),
    Guid = file_ctx:get_logical_guid_const(FileCtx),
    FinishCallback = ?ON_SUCCESSFUL_SLAVE_JOBS(fun() ->
        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
        case Depth =:= MaxDepth of
            true ->
                storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, Guid, undefined);
            false ->
                {#statbuf{st_mtime = STMtime}, _} = storage_file_ctx:stat(StorageFileCtx),
                storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, Guid, STMtime, Offset, BatchSize, BatchHash)
        end
    end),
    {ok, MasterJobMap#{finish_callback => FinishCallback}};
schedule_jobs_for_all_files(TraverseJob = #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    offset = Offset,
    batch_size = BatchSize,
    depth = Depth,
    max_depth = MaxDepth,
    info = #{file_ctx := FileCtx}
}, MasterJobMap, BatchHash, true
) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    AsyncMasterJobs = maps:get(async_master_jobs, MasterJobMap, []),
    SlaveJobs = maps:get(slave_jobs, MasterJobMap, []),
    ToProcess = length(AsyncMasterJobs) + length(SlaveJobs),
    storage_import_monitoring:increment_queue_length_histograms(SpaceId, ToProcess),
    FinishCallback = fun(#{master_job_starter_callback := MasterJobCallback}, SlavesDescription) ->
        case maps:get(slave_jobs_failed, SlavesDescription) of
            0 ->
                StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
                Guid = file_ctx:get_logical_guid_const(FileCtx),
                {#statbuf{st_mtime = STMtime}, _} = storage_file_ctx:stat(StorageFileCtx),
                {ok, SSI} = case Depth =:= MaxDepth of
                    true ->
                        storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, Guid, undefined);
                    false ->
                        storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, Guid, STMtime, Offset, BatchSize, BatchHash, false)
                end,
                case storage_sync_info:are_all_batches_processed(SSI) of
                    true ->
                        storage_sync_info:increase_batches_to_process(StorageFileId, SpaceId),
                        storage_import_monitoring:increment_queue_length_histograms(SpaceId, 1),
                        MasterJobCallback(#{
                            jobs => [storage_import_deletion:get_master_job(TraverseJob)],
                            cancel_callback => fun(_CancelDescription) ->
                                storage_import_monitoring:mark_unmodified_file(SpaceId)
                            end
                        });
                    false ->
                        ok
                end;
            _ ->
                ok
        end
    end,
    {ok, MasterJobMap#{finish_callback => FinishCallback}}.

-spec encode_task_id(od_space:id(), storage:id(), non_neg_integer()) -> traverse:id().
encode_task_id(SpaceId, StorageId, ScanNum) ->
    str_utils:join_binary([?TASK_ID_PREFIX, SpaceId, StorageId, integer_to_binary(ScanNum)], ?TASK_ID_SEP).

-spec decode_task_id(traverse:id()) -> {od_space:id(), storage:id(), non_neg_integer()}.
decode_task_id(TaskId) ->
    [_Prefix, SpaceId, StorageId, ScanNum] = binary:split(TaskId, ?TASK_ID_SEP, [global]),
    {SpaceId, StorageId, binary_to_integer(ScanNum)}.

-spec do_slave_job_on_directory(master_job()) ->
    {ok, {storage_import_engine:result(), file_ctx:ctx() | undefined, storage_file_ctx:ctx()}} | {error, term()}.
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
    {ok, {?FILE_UNMODIFIED, FileCtx, StorageFileCtx}}.

-spec process_storage_file(storage_file_ctx:ctx(), info()) ->
    {ok, {storage_import_engine:result(), file_ctx:ctx(), storage_file_ctx:ctx()}} | {error, term()}.
process_storage_file(StorageFileCtx, Info) ->
    % processed file can be a regular file or a directory
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    try
        case storage_import_engine:find_direct_parent_and_sync_file(StorageFileCtx, Info) of
            Result = {SyncResult, _, _}
                when SyncResult =:= ?FILE_CREATED
                orelse SyncResult =:= ?FILE_MODIFIED
                orelse SyncResult =:= ?FILE_UNMODIFIED
            ->
                {ok, Result};
            {error, ?ENOENT} ->
                {error, ?ENOENT};
            Error = {error, _} ->
                ?error(
                    "Syncing file ~tp on storage ~tp supporting space ~tp failed due to ~w.",
                    [StorageFileId, StorageId, SpaceId, Error]),
                Error
        end
    catch
        throw:?ENOENT ->
            {error, ?ENOENT};
        Error2:Reason2:Stacktrace2 ->
            ?error_stacktrace(
                "Syncing file ~tp on storage ~tp supporting space ~tp failed due to ~w:~w.",
                [StorageFileId, StorageId, SpaceId, Error2, Reason2], Stacktrace2),
            {error, {Error2, Reason2}}
    end.


-spec increment_counter(storage_import_engine:result(), od_space:id()) -> ok.
increment_counter(?FILE_CREATED, SpaceId) ->
    storage_import_monitoring:mark_created_file(SpaceId);
increment_counter(?FILE_MODIFIED, SpaceId) ->
    storage_import_monitoring:mark_modified_file(SpaceId);
increment_counter(?FILE_UNMODIFIED, SpaceId) ->
    storage_import_monitoring:mark_unmodified_file(SpaceId).


-spec get_storage_sync_info_doc(master_job()) -> storage_sync_info:doc() | undefined.
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
    case maps:get(add_deletion_detection_link, Info, false) of
        true ->
            StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
            ParentStorageFileCtx = storage_file_ctx:get_parent_ctx_const(StorageFileCtx),
            ParentStorageFileId = storage_file_ctx:get_storage_file_id_const(ParentStorageFileCtx),
            {ParentStorageFileId2, MarkLeaves} = case maps:get(iterator_type, Info) of
                ?FLAT_ITERATOR ->
                    %% When storage import is run on object storages we set MarkLeaves parameter to true
                    %% in the below call which allows to determine when link is associated with
                    %% a regular file which is necessary for storage_import_deletion on object storages
                    {SpaceStorageFileId, true};
                ?TREE_ITERATOR ->
                    %% On the other hand, on block storages MarkLeaves = false.
                    %% storage_import_deletion on block storages processes only direct children of a directory
                    %% so it does not need to traverse recursively down the file structure.
                    %% It also allows to create storage_sync_link for directory and its children simultaneously
                    %% as we are sure that value of a subdirectory link won't be set to undefined.
                    {ParentStorageFileId, false}
            end,
            storage_sync_links:add_link_recursive(ParentStorageFileId2, StorageId, StorageFileId, MarkLeaves);
        _ ->
            ok
    end.

-spec scan_finished(od_space:id(), storage:id(), boolean()) -> ok.
scan_finished(SpaceId, StorageId, Aborted) ->
    SpaceStorageFileId = storage_file_id:space_dir_id(SpaceId, StorageId),
    ok = storage_sync_links:delete_recursive(SpaceStorageFileId, StorageId),
    storage_import_monitoring:mark_finished_scan(SpaceId, Aborted),
    auto_storage_import_worker:notify_finished_scan(SpaceId),
    ok.


-spec ensure_scan_config_is_map(od_space:id(), storage_import:scan_config() | undefined) ->
    storage_import:scan_config_map().
ensure_scan_config_is_map(SpaceId, undefined) ->
    {ok, AutoConfig} = storage_import_config:get_auto_config(SpaceId),
    auto_storage_import_config:to_map(AutoConfig);
ensure_scan_config_is_map(_SpaceId, AutoConfig) ->
    auto_storage_import_config:to_map(AutoConfig).