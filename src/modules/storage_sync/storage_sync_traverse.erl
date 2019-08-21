%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for handling space strategies. Defines base data types.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_traverse).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("modules/storage_traverse/storage_traverse.hrl").
-include("modules/storage_sync/storage_sync.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include_lib("ctool/include/logging.hrl").



%%%===================================================================
%%% Types
%%%===================================================================
% TODO CLEANUP TYPES

-type type() :: storage_update | storage_import. % todo use it


-type arguments() :: #{
max_depth => non_neg_integer(),
sync_acl => boolean(),
scan_interval => non_neg_integer(),
write_once => boolean(),
delete_enable => boolean()
}.

-type details() :: {boolean(), map()}. % todo

-type sync_mode() :: todo_delete. % todo delete



% new types goes here
-type scan_status() :: not_started | in_progress | finished.
-type sync_config() :: space_strategies:sync_config().
-type sync_configs() :: space_strategies:sync_configs().


%%%===================================================================
%%% Exports
%%%===================================================================

%% Types
-export_type([arguments/0, details/0, type/0]).
-export_type([sync_mode/0]).


-export_type([scan_status/0]).

%% API
-export([
    init_pool/0, stop_pool/0, run/4, do_master_job/2,
    do_slave_job/2, get_job/1, update_job_progress/5, to_string/1, task_started/1, task_finished/1, run_import/3, run_update/3, reset_info/1]).

-define(POOL, ?MODULE).
%%-define(TASK_ID(SpaceId, StorageId, ScanNum),
%%    <<"storage_sync###", SpaceId/binary, "###", StorageId/binary, "###", (integer_to_binary(ScanNum))/binary>>
%%).
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


%TODO DO MASTER JOB I DO SLAVE JOB powinny wolac odpowieni e funkcje ze storage_import albo storage_update


init_pool() ->
    % Get pool limits from app.config
    MasterJobsLimit = application:get_env(?APP_NAME, storage_sync_master_jobs_limit, 10),
    SlaveJobsLimit = application:get_env(?APP_NAME, storage_sync_slave_workers_limit, 10),
    ParallelOrdersLimit = application:get_env(?APP_NAME, storage_sync_parallel_orders_limit, 10),
    storage_traverse:init(?POOL, MasterJobsLimit, SlaveJobsLimit, ParallelOrdersLimit).

stop_pool() ->
    storage_traverse:stop(?POOL).

run_import(SpaceId, StorageId, ImportConfig) ->
    CurrentTimestamp = time_utils:cluster_time_seconds(),
    {ok, SSM} = storage_sync_monitoring:prepare_new_import_scan(SpaceId, StorageId, CurrentTimestamp),
    ScansNum = storage_sync_monitoring:get_scans_num(SSM),
    ?debug("Starting storage_sync scan no. 1 for space: ~p and storage: ~p", [SpaceId, StorageId]),
    Config = ImportConfig#{
        sync_callback_module => storage_import % todo maybe it should by storage_sync_traverse:type() ????
    },
    run(SpaceId, StorageId, ScansNum + 1, Config).

run_update(SpaceId, StorageId, UpdateConfig) ->
    CurrentTimestamp = time_utils:cluster_time_seconds(),
    {ok, SSM} = storage_sync_monitoring:prepare_new_update_scan(SpaceId, StorageId, CurrentTimestamp),
    ScansNum = storage_sync_monitoring:get_scans_num(SSM),
    ?debug("Starting storage_sync scan no. ~p for space: ~p and storage: ~p", [ScansNum + 1, SpaceId, StorageId]),
    Config = UpdateConfig#{
        sync_callback_module => storage_update % todo maybe it should by storage_sync_traverse:type() ????
    },
    run(SpaceId, StorageId, ScansNum + 1, Config).


run(SpaceId, StorageId, ScanNum, Config) ->
    TaskId = encode_task_id(SpaceId, StorageId, ScanNum),

    {MaxDepth, Config2} = maps:take(max_depth, Config),
    SyncAcl = maps:get(sync_acl, Config2),

    RunOpts = #{
        batch_size => ?SYNC_DIR_BATCH_SIZE,
        async_master_jobs => true,
        execute_slave_on_dir => false,
        async_next_batch_job => true,
        next_batch_job_prehook => next_batch_job_prehook(),
        children_master_job_prehook => children_master_job_prehook(),
        max_depth => MaxDepth,
        compute_fun => compute_hash_fun(SyncAcl),
        compute_init => []
    },
    TraverseInfo = Config2#{parent_ctx => file_ctx:new_root_ctx()},
    storage_traverse:run(?POOL, TaskId, SpaceId, StorageId, TraverseInfo, RunOpts).

%===================================================================
% Pool callbacks
%===================================================================

do_master_job(TraverseJob = #storage_traverse{info = #{sync_callback_module := storage_import}}, Args) ->
    do_import_master_job(TraverseJob, Args);
do_master_job(TraverseJob = #storage_traverse{info = #{sync_callback_module := storage_update}}, Args) ->
    do_update_master_job(TraverseJob, Args).


do_slave_job({StorageFileCtx, Info}, _Task) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    case process_storage_file(StorageFileCtx, Info) of
        {ok, {SyncResult, _, _}} ->
            increase_according_counter(SyncResult, SpaceId, StorageId);
        Error = {error, _} ->
            storage_sync_monitoring:mark_failed_file(SpaceId, StorageId),
            Error
    end.

update_job_progress(ID, Job, Pool, TaskID, Status) ->
    % todo
%%        ?alert("update_job_progress ~p", [{ID, Job, Pool, TaskID, Status}]),
    storage_traverse:update_job_progress(ID, Job, Pool, TaskID, Status).


get_job(DocOrID) ->
    % todo
    %%    ?alert("get_job ~p", [DocOrID]),
    storage_traverse:get_job(DocOrID).

task_started(TaskId) ->
    ?alert("Storage sync scan ~p started", [TaskId]). %todo debug

task_finished(TaskId) ->
    {SpaceId, StorageId, _} = decode_task_id(TaskId),
    storage_sync_monitoring:mark_finished_scan(SpaceId, StorageId),
    ?alert("Storage sync scan ~p finished", [TaskId]). %todo debug


%%%===================================================================
%%% Internal functions
%%%===================================================================

do_import_master_job(TraverseJob = #storage_traverse{
    space_id = SpaceId,
    storage_doc = #document{key = StorageId},
    storage_file_ctx = StorageFileCtx,
    info = Info,
    offset = Offset,
    batch_size = BatchSize
}, Args) ->
    % todo komentarz robimy recznie slave_joba zeby miec pewnosc, ze jak zostana wrzucone kolejne
    % batche async to slave job juz bedzie wczesniej zrobiony
    case do_slave_job_on_directory(TraverseJob) of
        {ok, {SyncResult, FileCtx, StorageFileCtx2}} ->
            increase_according_counter(SyncResult, SpaceId, StorageId),
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
                    ?emergency("INCREASE BY: ~p", [{ToProcess, SlaveJobsNum, AsyncMasterJobsNum}]),
                    storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, ToProcess),

                    FinishCallback = ?ON_SUCCESSFUL_SLAVE_JOBS(fun() ->
                        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
                        BatchHash = storage_sync_changes:hash(lists:reverse(HashesReversed)),
                        BatchKey = batch_key(Offset, BatchSize),
                        Result = storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, MTime, BatchKey, BatchHash, true),
%%                        ?alert("CALLBACK FINISHED: ~p for file ~p with offset ~p", [Result, StorageFileId, Offset]),
                        Result
                    end),

                    {ok, MasterJobMap#{finish_callback => FinishCallback}};
                Error = {error, _} ->
                    Error
            end;
        Error2 = {error, _} ->
            storage_sync_monitoring:mark_failed_file(SpaceId, StorageId),
            Error2
    end.

do_update_master_job(TraverseJob = #storage_traverse{info = #{detect_deletions := true}}, Args) ->
    ?emergency("UDALO SIE !!!");
%%do_update_master_job(TraverseJob = #storage_traverse{
%%    storage_file_ctx = StorageFileCtx,
%%    space_id = SpaceId,
%%    info = #{detect_deletions_scan := true}
%%}, Args) ->
%%    % caly ten function_clause moze byc w case w kolejnym function-cllause bo zawsze, dla kazdego batcha do niego wejdziemy chyba
%%    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
%%    {ok, MasterJobMap} = traverse(TraverseJob, Args),
%%    FinishCallback = fun(#{master_job_callback := MasterJobCallback}, SlavesDescription) ->
%%        case maps:get(slave_jobs_failed, SlavesDescription) of
%%            0 ->
%%                {ok, SSIDoc} = storage_sync_info:increase_batches_processed(StorageFileId, SpaceId),
%%                case storage_sync_info:all_batches_processed(SSIDoc) of
%%                    true ->
%%                        MasterJobCallback({detect_deletions, TraverseJob});
%%                    false ->
%%                        ok
%%                end;
%%            _ ->
%%                % todo what about errors in slaves?
%%                ok
%%        end
%%    end,
%%    {ok, MasterJobMap#{finish_callback => FinishCallback}};
do_update_master_job(TraverseJob = #storage_traverse{
    space_id = SpaceId,
    storage_doc = #document{key = StorageId},
    offset = Offset,
    storage_file_ctx = StorageFileCtx,
    info = Info = #{
        delete_enable := DeleteEnable,
        write_once := WriteOnce
    }
}, Args) ->
    % todo refactor this function
    % todo trzeba brac pod uwage delete enable i write once no czy jest hash wyliczony w ogóle !!!
    case do_slave_job_on_directory(TraverseJob) of
        {ok, R = {SyncResult, FileCtx, StorageFileCtx2}} ->
            % todo increase counter later ???
            SSIDoc = get_storage_sync_info_doc(TraverseJob),
            % stat result will be cached in StorageFileCtx
            % we perform stat here to ensure that jobs for all batches for given directory
            % will be scheduled with the same stat result
            {#statbuf{st_mtime = STMtime}, StorageFileCtx3} = storage_file_ctx:stat(StorageFileCtx2),
            {MTimeHasChanged, StorageFileCtx4} = storage_sync_changes:mtime_has_changed(SSIDoc, StorageFileCtx3),
            TraverseJob2 = TraverseJob#storage_traverse{
                storage_file_ctx = StorageFileCtx4,
                info = Info2 = Info#{
                    file_ctx => FileCtx,
                    storage_sync_info_doc => SSIDoc
                }
            },
            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx4),
            increase_according_counter(SyncResult, SpaceId, StorageId),
%%            ?alert("~p", [{MTimeHasChanged, DeleteEnable, WriteOnce}]),
            case {MTimeHasChanged, DeleteEnable, WriteOnce} of
                {true, true, _} ->
%%                    do_update_master_job(TraverseJob2#storage_traverse{info = Info2#{detect_deletions_scan => true}}, Args);
                    TraverseJob3 = TraverseJob2#storage_traverse{
                      info = Info3 =  Info2#{add_deletion_detection_link => true}
                    },
                    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
                    {ok, MasterJobMap} = traverse(TraverseJob3, Args, false),

                    FinishCallback = fun(#{master_job_starter_callback := MasterJobCallback}, SlavesDescription) ->
                        case maps:get(slave_jobs_failed, SlavesDescription) of
                            0 ->
                                {ok, SSIDoc2} = storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, STMtime, undefined, undefined, true, false),
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
                                % todo what about errors in slaves?
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
        Error = {error, _} ->
            storage_sync_monitoring:mark_failed_file(SpaceId, StorageId),
            Error
    end.



traverse_only_directories(TraverseJob = #storage_traverse{storage_file_ctx = StorageFileCtx}, Args) ->
    case storage_traverse:do_master_job(TraverseJob, Args) of
        {ok, MasterJobMap} ->
            schedule_jobs_for_directories_only(MasterJobMap, StorageFileCtx);
        Error = {error, _} ->
            Error
    end.

traverse(TraverseJob = #storage_traverse{
    storage_file_ctx = StorageFileCtx,
    offset = Offset,
    batch_size = BatchSize,
    info = #{storage_sync_info_doc := SSIDoc}
}, Args, UpdateDelayedHashesWhenFinished) ->
    case storage_traverse:do_master_job(TraverseJob, Args) of
        {ok, MasterJobMap, HashesReversed} ->
            BatchHash = storage_sync_changes:hash(lists:reverse(HashesReversed)),
            BatchKey = batch_key(Offset, BatchSize),
            case storage_sync_changes:children_attrs_hash_has_changed(SSIDoc, BatchHash, BatchKey) of
                true ->
                    schedule_jobs_for_all_files(MasterJobMap, StorageFileCtx, BatchKey, BatchHash, UpdateDelayedHashesWhenFinished);
                false ->
                    % Hash hasn't changed, therefore we can schedule jobs only for directories
                    schedule_jobs_for_directories_only(MasterJobMap, StorageFileCtx)
            end;
        Error = {error, _} ->
            Error
    end.


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
        storage_sync_info:mark_processed_batch(StorageFileId, SpaceId, STMtime, BatchKey, BatchHash, true, UpdateDelayedHashesWhenFinished)
    end),
    {ok, MasterJobMap#{finish_callback => FinishCallback}}.

encode_task_id(SpaceId, StorageId, ScanNum) ->
    str_utils:join_binary([?TASK_ID_PREFIX, SpaceId, StorageId, integer_to_binary(ScanNum)], ?TASK_ID_SEP).

decode_task_id(TaskId) ->
    [_Prefix, SpaceId, StorageId, ScanNum] = binary:split(TaskId, ?TASK_ID_SEP, [global]),
    {SpaceId, StorageId, binary_to_integer(ScanNum)}.

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

process_storage_file(StorageFileCtx, Info) ->
    % processed file can be a regular file or a directory
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    try
        case storage_sync_engine:maybe_sync_storage_file(StorageFileCtx, Info) of
            Result = {SyncResult, _, _}
                when SyncResult =:= imported
                orelse SyncResult =:= updated
                orelse SyncResult =:= processed
            ->
                maybe_add_deletion_detection_link(StorageFileCtx, Info),
                {ok, Result};
            Error = {error, _} ->
                ?error(
                    "Syncing file ~p on storage ~p supported by space ~p failed due to ~w.",
                    [StorageFileId, StorageId, SpaceId, Error]),
                Error
        end
    catch
        Error2:Reason2 ->
            ?error_stacktrace(
                "Syncing file ~p on storage ~p supported by space ~p failed due to ~w:~w.",
                [StorageFileId, StorageId, SpaceId, Error2, Reason2]),
            {error, {Error2, Reason2}}
    end.

next_batch_job_prehook() ->
    fun(#storage_traverse{
        storage_file_ctx = StorageFileCtx,
        space_id = SpaceId,
        offset = Offset,
        storage_doc = #document{key = StorageId}
    }) ->
        storage_sync_monitoring:increase_to_process_counter(SpaceId, StorageId, 1),
        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
%%        ?alert("INCREASED TO PROCESS COUNTER FOR FILE ~p with offset ~p", [StorageFileId, Offset]),
        storage_sync_info:increase_batches_to_process(StorageFileId, SpaceId)
end.

children_master_job_prehook() ->
    fun(#storage_traverse{
        storage_file_ctx = StorageFileCtx,
        space_id = SpaceId
    }) ->
        StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
        storage_sync_info:init_new_scan(StorageFileId, SpaceId)
    end.

compute_hash_fun(SyncAcl) ->
    fun(StorageFileCtx, Info, HashAcc) ->
        {Hash, StorageFileCtx2} = storage_sync_changes:compute_file_attrs_hash(StorageFileCtx, SyncAcl),
        maybe_add_deletion_detection_link(StorageFileCtx, Info),
        {[Hash | HashAcc], StorageFileCtx2}
    end.

reset_info(#storage_traverse{info = Info}) ->
    Info2 = maps:remove(storage_sync_info_doc, Info),
    Info3 = maps:remove(file_ctx, Info2),
    Info4 = maps:remove(add_deletion_detection_link, Info3),
    maps:remove(detect_deletions, Info4).


to_string({StorageFileCtx, #{sync_callback_module := CallbackModule}}) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    {ok, str_utils:format_bin(
        " ~nStorage sync slave job:~n"
        "    file: ~p~n"
        "    storage: ~p~n"
        "    space: ~p~n"
        "    mode: ~p~n",
        [StorageFileId, StorageId, SpaceId, CallbackModule])};
to_string(#storage_traverse{
    storage_file_ctx = StorageFileCtx,
    info = #{sync_callback_module := CallbackModule},
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
        "    mode: ~p~n"
        "    offset: ~p~n"
        "    marker: ~p~n",
        [StorageFileId, StorageId, SpaceId, CallbackModule, Offset, Marker])};
to_string(OtherJob) ->
    {ok, str_utils:format_bin("~p", [OtherJob])}.

batch_key(Offset, Length) ->
    Offset div Length.

increase_according_counter(imported, SpaceId, StorageId) ->
    storage_sync_monitoring:mark_imported_file(SpaceId, StorageId);
increase_according_counter(updated, SpaceId, StorageId) ->
    storage_sync_monitoring:mark_updated_file(SpaceId, StorageId);
increase_according_counter(processed, SpaceId, StorageId) ->
    storage_sync_monitoring:mark_processed_file(SpaceId, StorageId).

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

maybe_add_deletion_detection_link(StorageFileCtx, Info) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
%%    ?alert("DBG: ~p", [DetectDeletions]),
    % todo uwaga na space_dir przy pobieraniu parenta, cos sie moze pochrzanic
    ParentStorageFileCtx = storage_file_ctx:get_parent_ctx_const(StorageFileCtx),
    ParentStorageFileId = storage_file_ctx:get_storage_file_id_const(ParentStorageFileCtx),
    % todo tutaj musze miec informacje o tym czy traverse jest po block czy object storage'u
    MarkLeaves = case maps:get(storage_type, Info) of
        object -> true;
        block -> false
    end,
    case maps:get(add_deletion_detection_link, Info, false) of
        true -> storage_sync_links:add_link(ParentStorageFileId, SpaceId, StorageId, StorageFileId, MarkLeaves);
        _ -> ok
    end.