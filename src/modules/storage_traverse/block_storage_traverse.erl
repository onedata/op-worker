%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(block_storage_traverse).
-author("Jakub Kudzia").


-include("global_definitions.hrl").
-include("modules/storage_traverse/storage_traverse.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% storage_traverse callbacks
-export([init_type_specific_opts/2, get_children_batch/1, generate_master_and_slave_jobs/3, batch_id/1, fold/3]).

-type batch_id() :: {Offset :: non_neg_integer(), Size :: non_neg_integer()}.

-export_type([batch_id/0]).

%%%===================================================================
%%% storage_traverse callbacks
%%%===================================================================

-spec init_type_specific_opts(storage_traverse:job(), storage_traverse:opts()) -> storage_traverse:job().
init_type_specific_opts(StorageTraverse = #storage_traverse{}, _Opts) ->
    StorageTraverse.

-spec get_children_batch(storage_traverse:job()) -> {ok, [helper:file_id()]} | {error, term()}.
get_children_batch(#storage_traverse{max_depth = 0}) ->
    {ok, []};
get_children_batch(#storage_traverse{
    storage_file_ctx = StorageFileCtx,
    offset = Offset,
    batch_size = BatchSize
}) ->
%%    ?alert("BatchSize: ~p", [BatchSize]),
    Handle = storage_file_ctx:get_handle_const(StorageFileCtx),
    storage_file_manager:readdir(Handle, Offset, BatchSize).

fold(TraverseJob = #storage_traverse{max_depth = 0}, _Fun, Init) ->
    {Init, TraverseJob};
fold(TraverseJob = #storage_traverse{
    storage_file_ctx = StorageFileCtx,
    offset = Offset,
    batch_size = BatchSize
}, Fun, Init) ->
    Handle = storage_file_ctx:get_handle_const(StorageFileCtx),
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    case storage_file_manager:readdir(Handle, Offset, BatchSize) of
        {ok, ChildrenNames} ->
            Result = lists:foldl(fun(ChildName, Acc) ->
                ChildStorageFileId = filename:join(StorageFileId, ChildName),
                Fun(ChildStorageFileId, Acc)
            end, Init, ChildrenNames),
            case length(ChildrenNames) < BatchSize of
                true ->
                    {ok, Result};
                false ->
                    TraverseJob2 = TraverseJob#storage_traverse{offset = Offset + length(ChildrenNames)},
                    fold(TraverseJob2, Fun, Result)
            end;
        Error = {error, _} ->
            Error
    end.

-spec generate_master_and_slave_jobs(storage_traverse:job(), [helpers:file_id()],
    traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} | {ok, traverse:master_job_map()}.
generate_master_and_slave_jobs(#storage_traverse{
    storage_file_ctx = StorageFileCtx,
    max_depth = 0,
    execute_slave_on_dir = true,
    info = Info
}, _, _) ->
    {ok, #{sequential_slave_jobs => [{StorageFileCtx, Info}]}};
generate_master_and_slave_jobs(#storage_traverse{
    max_depth = 0,
    execute_slave_on_dir = false
}, _, _) ->
    {ok, #{}};
generate_master_and_slave_jobs(TraverseJob = #storage_traverse{
    storage_file_ctx = StorageFileCtx,
    space_id = SpaceId,
    storage_doc = #document{key = StorageId},
    execute_slave_on_dir = OnDir,
    async_master_jobs = AsyncMasterJobs,
    batch_size = BatchSize,
    async_next_batch_job = AsyncNextBatchJob,
    next_batch_job_prehook = NextBatchJobPrehook,
    children_master_job_prehook = ChildMasterJobPrehook,
    compute_fun = ComputeFun,
    compute_init = ComputeInit,
    compute_enabled = ComputeEnabled,
    offset = Offset,
    info = Info
}, ChildrenNames, #{master_job_starter_callback := MasterJobStarterCallback}) ->
    MasterJobs = case length(ChildrenNames) < BatchSize of
        false ->
            % it is not the last batch
            NextBatchTraverseJob = TraverseJob#storage_traverse{
                offset = Offset + length(ChildrenNames)
            },
            NextBatchJobPrehook(NextBatchTraverseJob),
            case AsyncNextBatchJob of
                true ->
                    % schedule job for next batch in this directory asynchronously
                    MasterJobStarterCallback([NextBatchTraverseJob]),
                    [];
                false ->
                    % job for next batch in this directory will be scheduled with children master jobs
                    [NextBatchTraverseJob]
            end;
        true ->
            []
    end,
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    ResetInfo = storage_traverse:reset_info(TraverseJob),
    {MasterJobsRev, SlaveJobsRev, ComputeResult} = lists:foldl(
        fun(ChildName, Acc = {MasterJobsIn, SlaveJobsIn, ComputeAcc}) ->
            case file_meta:is_hidden(ChildName) of
                true ->
                    Acc;
                false ->
                    ChildStorageFileId = filename:join([StorageFileId, ChildName]),
                    ChildCtx = storage_file_ctx:new(ChildStorageFileId, SpaceId, StorageId),
                    {#statbuf{st_mode = Mode}, ChildCtx2} = storage_file_ctx:stat(ChildCtx),
                    {ComputePartialResult, ChildCtx3} =
                        compute(ComputeFun, ChildCtx2, Info, ComputeAcc, ComputeEnabled),
    %%                ?alert("PARTIAL RESULT: ~p", [ComputePartialResult]),
                    case file_meta:type(Mode) of
                        ?REGULAR_FILE_TYPE ->
                            % todo job should be created by callback function
                            {MasterJobsIn, [{ChildCtx3, ResetInfo} | SlaveJobsIn], ComputePartialResult};
                        ?DIRECTORY_TYPE ->
                            % todo callback ???
                            ChildMasterJob = get_child_master_job(ChildCtx3, TraverseJob, ResetInfo),
                            ChildMasterJobPrehook(ChildMasterJob),
                            {[ChildMasterJob | MasterJobsIn], SlaveJobsIn, ComputePartialResult}
                    end
            end
        end, {[], [], ComputeInit}, ChildrenNames),

    SeqSlaveJobs = case {OnDir, Offset} of
        {true, 0} -> [{StorageFileCtx, Info}]; %execute slave job only once per directory
        _ -> []
    end,
    MasterJobsKey = case AsyncMasterJobs of
        true -> async_master_jobs;
        false -> master_jobs
    end,
    MasterJobsMap = #{
        MasterJobsKey => MasterJobs ++ lists:reverse(MasterJobsRev),
        sequential_slave_jobs => SeqSlaveJobs,
        slave_jobs => lists:reverse(SlaveJobsRev)
    },
    case ComputeFun =:= undefined orelse ComputeEnabled =:= false of
        true -> {ok, MasterJobsMap};
        false -> {ok, MasterJobsMap, ComputeResult}
    end.

-spec get_child_master_job(storage_file_ctx:ctx(), storage_traverse:job(), storage_traverse:info()) -> storage_traverse:job().
get_child_master_job(ChildCtx, Job = #storage_traverse{max_depth = MaxDepth}, ChildInfo)->
    Job#storage_traverse{
        storage_file_ctx = ChildCtx,
        info = ChildInfo,
        max_depth = MaxDepth - 1,
        offset = 0,
        compute_enabled = true
    }.

-spec batch_id(storage_traverse:job()) -> batch_id().
batch_id(#storage_traverse{offset = Offset, batch_size = Size}) ->
    {Offset, Size}.

-spec compute(undefined | storage_traverse:compute(), storage_file_ctx:ctx(),
    storage_traverse:info(), term(), boolean()) -> term().
compute(_, _StorageFileCtx, _Info, ComputeAcc, false) ->
    {ComputeAcc, _StorageFileCtx};
compute(undefined, _StorageFileCtx, _Info, ComputeAcc, _) ->
    {ComputeAcc, _StorageFileCtx};
compute(ComputeFun, StorageFileCtx, Info, ComputeAcc, true) ->
    ComputeFun(StorageFileCtx, Info, ComputeAcc).