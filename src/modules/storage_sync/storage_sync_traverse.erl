%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for traversing scanned storages.
%%% It uses traverse framework.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_traverse).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/storage_traverse/storage_traverse.hrl").
-include("modules/storage_sync/storage_sync.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%%%===================================================================
%%% Types
%%%===================================================================

-type job() :: storage_traverse:job().
-type scan_status() :: not_started | in_progress | finished.
-type info() :: #{atom() => term()}.

%%%===================================================================
%%% Exports
%%%===================================================================

%% Types
-export_type([scan_status/0, info/0]).

%% API
-export([init_pool/0, stop_pool/0, run/4, run_import/3, run_update/3, cancel/2]).

%% Pool callbacks
-export([do_master_job/2, do_slave_job/2, get_job/1, update_job_progress/5, to_string/1,
    task_started/1, task_finished/1, task_canceled/1]).

%% storage_traverse callbacks
-export([reset_info/1, next_batch_job_prehook/1, children_master_job_prehook/1, compute_fun/1]).

%% exported for tests
-export([mtime_has_changed/2]).

%%%===================================================================
%%% Macros
%%%===================================================================

-define(POOL, ?MODULE).
-define(POOL_BIN, atom_to_binary(?POOL, utf8)).
-define(TASK_ID_SEP, <<"###">>).
-define(TASK_ID_PREFIX, <<"storage_sync">>).

-define(ON_SUCCESSFUL_SLAVE_JOBS(Callback),
    fun(_MasterJobExtendedArgs, SlavesDescription) ->
        case maps:get(slave_jobs_failed, SlavesDescription) of
            0 -> Callback();
            _ -> ok
        end
    end
).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_pool() -> ok.
init_pool() ->
    % Get pool limits from app.config
    MasterJobsLimit = application:get_env(?APP_NAME, storage_sync_master_jobs_limit, 10),
    SlaveJobsLimit = application:get_env(?APP_NAME, storage_sync_slave_workers_limit, 10),
    ParallelOrdersLimit = application:get_env(?APP_NAME, storage_sync_parallel_orders_limit, 10),
    storage_traverse:init(?POOL, MasterJobsLimit, SlaveJobsLimit, ParallelOrdersLimit).

-spec stop_pool() -> ok.
stop_pool() ->
    storage_traverse:stop(?POOL).

-spec run_import(od_space:id(), storage:id(), space_strategies:import_config()) -> ok.
run_import(SpaceId, StorageId, ImportConfig) ->
    CurrentTimestamp = time_utils:cluster_time_seconds(),
    {ok, SSM} = storage_sync_monitoring:prepare_new_import_scan(SpaceId, StorageId, CurrentTimestamp),
    {ok, ScansNum} = storage_sync_monitoring:get_scans_num(SSM),
    ?debug("Starting storage_sync scan no. 1 for space: ~p and storage: ~p", [SpaceId, StorageId]),
    run(SpaceId, StorageId, ScansNum + 1, ImportConfig).

-spec run_update(od_space:id(), storage:id(), space_strategies:update_config()) -> ok.
run_update(SpaceId, StorageId, UpdateConfig) ->
    CurrentTimestamp = time_utils:cluster_time_seconds(),
    {ok, SSM} = storage_sync_monitoring:prepare_new_update_scan(SpaceId, StorageId, CurrentTimestamp),
    {ok, ScansNum} = storage_sync_monitoring:get_scans_num(SSM),
    ?debug("Starting storage_sync scan no. ~p for space: ~p and storage: ~p", [ScansNum + 1, SpaceId, StorageId]),
    run(SpaceId, StorageId, ScansNum + 1, UpdateConfig).

-spec run(od_space:id(), storage:id(), non_neg_integer(), space_strategies:config()) -> ok.
run(SpaceId, StorageId, ScanNum, Config) ->
    TaskId = encode_task_id(SpaceId, StorageId, ScanNum),
    {MaxDepth, Config2} = maps:take(max_depth, Config),
    TraverseInfo = Config2#{
        scan_num => ScanNum,
        parent_ctx => file_ctx:new_root_ctx(),
        storage_type => storage:type(StorageId)
    },
    RunOpts = #{
        execute_slave_on_dir => false,
        async_master_jobs => true,
        async_next_batch_job => true,
        next_batch_job_prehook => next_batch_job_prehook(TraverseInfo),
        children_master_job_prehook => children_master_job_prehook(TraverseInfo),
        batch_size => ?SYNC_DIR_BATCH_SIZE,
        max_depth => MaxDepth,
        compute_fun => compute_fun(TraverseInfo),
        compute_init => []
    },
    storage_traverse:run(?POOL, TaskId, SpaceId, StorageId, TraverseInfo, RunOpts).

-spec cancel(od_space:id(), storage:id()) -> ok.
cancel(SpaceId, StorageId) ->
    case storage_sync_monitoring:get_scans_num(SpaceId, StorageId) of
        {ok, ScansNum} ->
            traverse:cancel(?POOL_BIN, encode_task_id(SpaceId, StorageId, ScansNum + 1));
        {error, ?ENOENT} ->
            ?warning("Cannot cancel storage sync for space ~p and storage ~p as it is not configured"),
            ok
    end.

%===================================================================
% Pool callbacks
%===================================================================

-spec do_master_job(job(), traverse:master_job_extended_args()) -> {ok, traverse:master_job_map()}.
do_master_job(TraverseJob = #storage_traverse{info = #{scan_num := 1}}, Args) ->
    do_import_master_job(TraverseJob, Args);
do_master_job(TraverseJob = #storage_traverse{info = #{scan_num := ScanNum}}, Args) when ScanNum >= 1 ->
    do_update_master_job(TraverseJob, Args).

-spec do_slave_job({storage_file_ctx:ctx(), info()}, traverse:id()) -> ok | {error, term()}.
do_slave_job({StorageFileCtx, Info}, _Task) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    case process_storage_file(StorageFileCtx, Info) of
        {ok, {SyncResult, _, _}} ->
            increase_suitable_counter(SyncResult, SpaceId, StorageId);
        Error = {error, _} ->
            storage_sync_monitoring:mark_failed_file(SpaceId, StorageId),
            Error
    end.

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    traverse:job(), traverse:pool(), traverse:id(), traverse:job_status()) ->
    {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(ID, Job, Pool, TaskID, Status) ->
    storage_traverse:update_job_progress(ID, Job, Pool, TaskID, Status, ?MODULE).

-spec get_job(traverse:job_id()) ->
    {ok, traverse:job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    storage_traverse:get_job(DocOrID).

-spec task_started(traverse:id()) -> ok.
task_started(TaskId) ->
    {SpaceId, _StorageId, ScanNum} = decode_task_id(TaskId),
    ?debug("Storage sync scan ~p started", [TaskId]),
    storage_sync_logger:log_scan_started(SpaceId, ScanNum).

-spec task_finished(traverse:id()) -> ok.
task_finished(TaskId) ->
    {SpaceId, StorageId, ScanNum} = decode_task_id(TaskId),
    SpaceStorageFileId = filename_mapping:space_dir_path(SpaceId, StorageId),
    ok = storage_sync_links:delete_recursive(SpaceStorageFileId, SpaceId, StorageId),
    storage_sync_monitoring:mark_finished_scan(SpaceId, StorageId),
    ?debug("Storage sync scan ~p finished", [TaskId]),
    storage_sync_logger:log_scan_finished(SpaceId, ScanNum).

-spec task_canceled(traverse:id()) -> ok.
task_canceled(TaskId) ->
    {SpaceId, StorageId, ScanNum} = decode_task_id(TaskId),
    SpaceStorageFileId = filename_mapping:space_dir_path(SpaceId, StorageId),
    ok = storage_sync_links:delete_recursive(SpaceStorageFileId, SpaceId, StorageId),
    storage_sync_monitoring:mark_finished_scan(SpaceId, StorageId),
    ?debug("Storage sync scan ~p canceled", [TaskId]),
    storage_sync_logger:log_scan_cancelled(SpaceId, ScanNum).

-spec to_string({storage_file_ctx:ctx(), info()} | job()) -> {ok, binary()}.
to_string({StorageFileCtx, #{scan_num := ScanNum}}) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    {ok, str_utils:format_bin(
        " ~nStorage sync slave job:~n"
        "    file: ~p~n"
        "    storage: ~p~n"
        "    space: ~p~n"
        "    scan no.: ~p~n",
        [StorageFileId, StorageId, SpaceId, ScanNum])};
to_string(#storage_traverse{
    storage_file_ctx = StorageFileCtx,
    info = #{scan_num := ScanNum},
    offset = Offset,
    marker = Marker
}) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    {ok, str_utils:format_bin(
        " ~nStorage sync master job:~n"
        "    file: ~p~n"
        "    storage: ~p~n"
        "    space: ~p~n"
        "    scan no.: ~p~n"
        "    offset: ~p~n"
        "    marker: ~p~n",
        [StorageFileId, StorageId, SpaceId, ScanNum, Offset, Marker])}.

%%%===================================================================
%%% storage_traverse callbacks
%%%===================================================================

-spec reset_info(job()) -> info().
reset_info(#storage_traverse{info = Info}) ->
    Info2 = maps:remove(storage_sync_info_doc, Info),
    Info3 = maps:remove(file_ctx, Info2),
    Info4 = maps:remove(add_deletion_detection_link, Info3),
    maps:remove(detect_deletions, Info4).

-spec next_batch_job_prehook(info()) -> storage_traverse:next_batch_job_prehook().
next_batch_job_prehook(_TraverseInfo) ->
    fun(#storage_traverse{
        storage_file_ctx = StorageFileCtx,
        space_id = SpaceId,
        storage_doc = #document{key = StorageId}
    }) ->
        storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, 1),
        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
        storage_sync_info:increase_batches_to_process(StorageFileId, SpaceId)
    end.

-spec children_master_job_prehook(info()) -> storage_traverse:children_batch_job_prehook().
children_master_job_prehook(_TraverseInfo) ->
    fun(#storage_traverse{
        storage_file_ctx = StorageFileCtx,
        space_id = SpaceId
    }) ->
        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
        storage_sync_info:init_new_scan(StorageFileId, SpaceId)
    end.

-spec compute_fun(info()) -> storage_traverse:compute().
compute_fun(TraverseInfo) ->
    SyncAcl = maps:get(sync_acl, TraverseInfo, false),
    fun(StorageFileCtx, Info, HashAcc) ->
        {Hash, StorageFileCtx2} = storage_sync_hash:compute_file_attrs_hash(StorageFileCtx, SyncAcl),
        maybe_add_deletion_detection_link(StorageFileCtx, Info),
        {[Hash | HashAcc], StorageFileCtx2}
    end.

%%%===================================================================
%%% Functions exported for tests
%%%===================================================================

-spec mtime_has_changed(undefined | storage_sync_info:doc(), storage_file_ctx:ctx()) -> boolean().
mtime_has_changed(undefined, _StorageFileCtx) ->
    true;
mtime_has_changed(SSIDoc, StorageFileCtx) ->
    {#statbuf{st_mtime = STMtime}, _} = storage_file_ctx:stat(StorageFileCtx),
    storage_sync_info:get_mtime(SSIDoc) =/= STMtime.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec do_import_master_job(job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} | {error, term()}.
do_import_master_job(TraverseJob = #storage_traverse{
    space_id = SpaceId,
    storage_doc = #document{key = StorageId},
    storage_file_ctx = StorageFileCtx,
    info = Info,
    offset = Offset,
    batch_size = BatchSize
}, Args) ->
    % we perform slave job on directory manually to ensure that it is executed before jobs
    % for next batches are scheduled asynchronously
    case do_slave_job_on_directory(TraverseJob) of
        {ok, {SyncResult, FileCtx, StorageFileCtx2}} ->
            increase_suitable_counter(SyncResult, SpaceId, StorageId),
            % stat result will be cached in StorageFileCtx
            % we perform stat here to ensure that jobs for all batches for given directory
            % will be scheduled with the same stat result
            {#statbuf{st_mtime = MTime}, StorageFileCtx3} = storage_file_ctx:stat(StorageFileCtx2),
            TraverseJob2 = TraverseJob#storage_traverse{
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
                        BatchKey = batch_key(Offset, BatchSize),
                        storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, MTime, BatchKey, BatchHash, true)
                    end),
                    {ok, MasterJobMap#{finish_callback => FinishCallback}};
                Error = {error, _} ->
                    Error
            end;
        {error, ?ENOENT} ->
            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
            case filename_mapping:space_dir_path(SpaceId, StorageId) of
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

-spec do_update_master_job(job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} | {error, term()}.
do_update_master_job(#storage_traverse{
    storage_file_ctx = StorageFileCtx,
    info = #{
        detect_deletions := true,
        file_ctx := FileCtx,
        storage_type := StorageType
    }
}, _Args) ->
    % detect deletions flag is enabled
    ok = storage_sync_deletion:run(StorageFileCtx, FileCtx, StorageType, true, false),
    {ok, #{}};
do_update_master_job(TraverseJob = #storage_traverse{
    space_id = SpaceId,
    storage_doc = #document{key = StorageId},
    storage_file_ctx = StorageFileCtx,
    info = Info = #{
        delete_enable := DeleteEnable,
        write_once := WriteOnce,
        parent_ctx := ParentCtx
    }
}, Args) ->
    case do_slave_job_on_directory(TraverseJob) of
        {ok, {SyncResult, FileCtx, StorageFileCtx2}} ->
            SSIDoc = get_storage_sync_info_doc(TraverseJob),
            % stat result will be cached in StorageFileCtx
            % we perform stat here to ensure that jobs for all batches for given directory
            % will be scheduled with the same stat result
            {#statbuf{st_mtime = STMtime}, StorageFileCtx3} = storage_file_ctx:stat(StorageFileCtx2),
            MTimeHasChanged = storage_sync_traverse:mtime_has_changed(SSIDoc, StorageFileCtx3),
            TraverseJob2 = TraverseJob#storage_traverse{
                storage_file_ctx = StorageFileCtx3,
                info = Info2 = Info#{
                    file_ctx => FileCtx,
                    storage_sync_info_doc => SSIDoc
                }
            },
            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx3),
            increase_suitable_counter(SyncResult, SpaceId, StorageId),

            case {MTimeHasChanged, DeleteEnable, WriteOnce} of
                {true, true, _} ->
                    TraverseJob3 = TraverseJob2#storage_traverse{
                      info = Info3 =  Info2#{add_deletion_detection_link => true}
                    },
                    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
                    {ok, MasterJobMap} = traverse(TraverseJob3, Args, false),
                    FinishCallback = fun(#{master_job_starter_callback := MasterJobCallback}, SlavesDescription) ->
                        case maps:get(slave_jobs_failed, SlavesDescription) of
                            0 ->
                                {ok, SSIDoc2} = storage_sync_info:mark_processed_batch(StorageFileId,
                                    SpaceId, STMtime, undefined, undefined, true, false),
                                case storage_sync_info:all_batches_processed(SSIDoc2) of
                                    true ->
                                        storage_sync_info:increase_batches_to_process(StorageFileId, SpaceId),
                                        MasterJobCallback([TraverseJob3#storage_traverse{info = Info3#{
                                            detect_deletions => true
                                        }}]);
                                    false ->
                                        ok
                                end;
                            _ ->
                                ok
                        end
                    end,
                    {ok, MasterJobMap#{finish_callback => FinishCallback}};
                {false, _, true} ->
                    % WriteOnce option is enabled and MTime of directory has not changed, therefore
                    % we are sure that hash computed out of children (only regular files) attributes
                    % mustn't have changed
                    traverse_only_directories(TraverseJob2#storage_traverse{compute_enabled = false}, Args);
                {_, _, _} ->
                    % Hash of children attrs might have changed, therefore it must be computed
                    traverse(TraverseJob2, Args, true)
            end;
        {error, ?ENOENT} ->
            % directory might have been deleted
            FileName = storage_file_ctx:get_file_name_const(StorageFileCtx),
            storage_sync_monitoring:mark_updated_file(SpaceId, StorageId),
            {FileCtx, ParentCtx2} = file_ctx:get_child(ParentCtx, FileName, user_ctx:new(?ROOT_SESS_ID)),
            FinishCallback = fun(#{master_job_starter_callback := MasterJobCallback}, _SlavesDescription) ->
                MasterJobCallback([TraverseJob#storage_traverse{info = Info#{
                    detect_deletions => true,
                    file_ctx => FileCtx,
                    parent_ctx => ParentCtx2
                }}])
            end,
            {ok, #{finish_callback => FinishCallback}};
        Error = {error, _} ->
            storage_sync_monitoring:mark_failed_file(SpaceId, StorageId),
            Error
    end.

-spec traverse_only_directories(job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} | {error, term()}.
traverse_only_directories(TraverseJob = #storage_traverse{storage_file_ctx = StorageFileCtx}, Args) ->
    case storage_traverse:do_master_job(TraverseJob, Args) of
        {ok, MasterJobMap} ->
            schedule_jobs_for_directories_only(MasterJobMap, StorageFileCtx);
        Error = {error, _} ->
            Error
    end.

-spec traverse(job(), traverse:master_job_extended_args(), boolean()) ->
    {ok, traverse:master_job_map()} | {error, term()}.
traverse(TraverseJob = #storage_traverse{
    storage_file_ctx = StorageFileCtx,
    offset = Offset,
    batch_size = BatchSize,
    info = #{storage_sync_info_doc := SSIDoc}
}, Args, UpdateDelayedHashesWhenFinished) ->
    case storage_traverse:do_master_job(TraverseJob, Args) of
        {ok, MasterJobMap, HashesReversed} ->
            BatchHash = storage_sync_hash:hash(lists:reverse(HashesReversed)),
            BatchKey = batch_key(Offset, BatchSize),
            case storage_sync_hash:children_attrs_hash_has_changed(SSIDoc, BatchHash, BatchKey) of
                true ->
                    schedule_jobs_for_all_files(MasterJobMap, StorageFileCtx, BatchKey, BatchHash,
                        UpdateDelayedHashesWhenFinished);
                false ->
                    % Hash hasn't changed, therefore we can schedule jobs only for directories
                    schedule_jobs_for_directories_only(MasterJobMap, StorageFileCtx)
            end;
        Error = {error, _} ->
            Error
    end.

-spec schedule_jobs_for_directories_only(traverse:master_job_map(), storage_file_ctx:ctx()) ->
    {ok, traverse:master_job_map()}.
schedule_jobs_for_directories_only(MasterJobMap, StorageFileCtx) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    MasterJobMap2 = maps:remove(slave_jobs, MasterJobMap),
    {#statbuf{st_mtime = STMtime}, _StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
    FinishCallback = fun(_MasterJobExtendedArgs, _SlaveJobsDescription) ->
        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
        storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, STMtime, undefined, undefined, true)
    end,
    AsyncMasterJobs = maps:get(async_master_jobs, MasterJobMap2, []),
    ToProcess = length(AsyncMasterJobs),
    storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, ToProcess),
    {ok, MasterJobMap2#{finish_callback => FinishCallback}}.

-spec schedule_jobs_for_all_files(traverse:master_job_map(), storage_file_ctx:ctx(), non_neg_integer(),
    storage_sync_hash:hash(), boolean()) -> {ok, traverse:master_job_map()}.
schedule_jobs_for_all_files(MasterJobMap, StorageFileCtx, BatchKey, BatchHash, UpdateDelayedHashesWhenFinished) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    AsyncMasterJobs = maps:get(async_master_jobs, MasterJobMap, []),
    SlaveJobs = maps:get(slave_jobs, MasterJobMap, []),
    ToProcess = length(AsyncMasterJobs) + length(SlaveJobs),
    storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, ToProcess),
    FinishCallback = ?ON_SUCCESSFUL_SLAVE_JOBS(fun() ->
        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
        {#statbuf{st_mtime = STMtime}, _} = storage_file_ctx:stat(StorageFileCtx),
        storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, STMtime, BatchKey, BatchHash,
            true, UpdateDelayedHashesWhenFinished)
    end),
    {ok, MasterJobMap#{finish_callback => FinishCallback}}.

-spec encode_task_id(od_space:id(), storage:id(), non_neg_integer()) -> traverse:id().
encode_task_id(SpaceId, StorageId, ScanNum) ->
    str_utils:join_binary([?TASK_ID_PREFIX, SpaceId, StorageId, integer_to_binary(ScanNum)], ?TASK_ID_SEP).

-spec decode_task_id(traverse:id()) -> {od_space:id(), storage:id(), non_neg_integer()}.
decode_task_id(TaskId) ->
    [_Prefix, SpaceId, StorageId, ScanNum] = binary:split(TaskId, ?TASK_ID_SEP, [global]),
    {SpaceId, StorageId, binary_to_integer(ScanNum)}.

-spec do_slave_job_on_directory(job()) ->
    {ok, {storage_sync_engine:result(), file_ctx:ctx(), storage_file_ctx:ctx()}} | {error, term()}.
do_slave_job_on_directory(#storage_traverse{
    offset = 0,
    storage_file_ctx = StorageFileCtx,
    info = Info
}) ->
    % slave job on dir should be executed just once
    process_storage_file(StorageFileCtx, Info);
do_slave_job_on_directory(#storage_traverse{
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

-spec batch_key(non_neg_integer(), non_neg_integer()) -> non_neg_integer().
batch_key(Offset, Length) ->
    Offset div Length.

-spec increase_suitable_counter(storage_sync_engine:result(), od_space:id(), storage:id()) -> ok.
increase_suitable_counter(imported, SpaceId, StorageId) ->
    storage_sync_monitoring:mark_imported_file(SpaceId, StorageId);
increase_suitable_counter(updated, SpaceId, StorageId) ->
    storage_sync_monitoring:mark_updated_file(SpaceId, StorageId);
increase_suitable_counter(processed, SpaceId, StorageId) ->
    storage_sync_monitoring:mark_processed_file(SpaceId, StorageId).

-spec get_storage_sync_info_doc(job()) -> storage_sync_info:doc().
get_storage_sync_info_doc(#storage_traverse{
    space_id = SpaceId,
    storage_file_ctx = StorageFileCtx,
    offset = 0
}) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    % storage_sync_info doc is fetch just once per directory, for its first batch
    case storage_sync_info:get(StorageFileId, SpaceId) of
        {error, not_found} -> undefined;
        {ok, SSIDoc} -> SSIDoc
    end;
get_storage_sync_info_doc(#storage_traverse{
    info = #{storage_sync_info_doc := SSIDoc}
}) ->
    % for other batches it is cached in #storage_traverse.info map
    SSIDoc.

-spec maybe_add_deletion_detection_link(storage_file_ctx:ctx(), info()) -> ok.
maybe_add_deletion_detection_link(StorageFileCtx, Info) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    ParentStorageFileCtx = storage_file_ctx:get_parent_ctx_const(StorageFileCtx),
    ParentStorageFileId = storage_file_ctx:get_storage_file_id_const(ParentStorageFileCtx),
    {ParentStorageFileId2, MarkLeaves} = case maps:get(storage_type, Info) of
        ?OBJECT_STORAGE ->
            %% When storage_sync is run on object storages we set MarkLeaves parameter to true
            %% in the below call which allows to determine when link is associated with
            %% a regular file which is necessary for storage_sync_deletion on object storages
            {filename_mapping:space_dir_path(SpaceId, StorageId), true};
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
            storage_sync_links:add_link_recursive(
                ParentStorageFileId2, SpaceId, StorageId, StorageFileId, MarkLeaves);
        _ ->
            ok
    end.