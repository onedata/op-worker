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
-module(object_storage_traverse).   % todo rename to canonical-object?
-author("Jakub Kudzia").


-include("global_definitions.hrl").
-include("modules/storage_traverse/storage_traverse.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").


%% storage_traverse callbacks
-export([init_type_specific_opts/2, get_children_batch/1, generate_master_and_slave_jobs/3, batch_id/1, fold/3]).

-type batch_id() :: {Offset :: non_neg_integer(), Marker :: helpers:marker(), Size :: non_neg_integer()}.

-export_type([batch_id/0]).

%%%===================================================================
%%% storage_traverse callbacks
%%%===================================================================

-spec init_type_specific_opts(storage_traverse:job(), storage_traverse:opts()) -> storage_traverse:job().
init_type_specific_opts(StorageTraverse = #storage_traverse{
    space_id = SpaceId,
    storage_doc = #document{key = StorageId}
}, Opts) ->
    case maps:get(marker, Opts, undefined) of
        undefined ->
            StorageTraverse#storage_traverse{marker = filename_mapping:space_dir_path(SpaceId, StorageId)};
        Marker ->
            StorageTraverse#storage_traverse{marker = Marker}
    end.

-spec get_children_batch(storage_traverse:job()) -> {ok, [helper:file_id()]} | {error, term()}.
get_children_batch(#storage_traverse{max_depth = 0}) ->
    {ok, []};
get_children_batch(#storage_traverse{
    storage_file_ctx = StorageFileCtx,
    offset = Offset,
    batch_size = BatchSize,
    marker = Marker
}) ->
    Handle = storage_file_ctx:get_handle_const(StorageFileCtx),
    storage_file_manager:listobjects(Handle, Marker, Offset, BatchSize).

fold(StorageTraverse = #storage_traverse{max_depth = 0}, _Fun, Init) ->
    {Init, StorageTraverse};
fold(TraverseJob = #storage_traverse{
    storage_file_ctx = StorageFileCtx,
    offset = Offset,
    batch_size = BatchSize,
    marker = Marker,
    max_depth = MaxDepth
}, Fun, Init) ->
    Handle = storage_file_ctx:get_handle_const(StorageFileCtx),
    case storage_file_manager:listobjects(Handle, Marker, Offset, BatchSize) of
        {ok, ChildrenIds} ->
            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
            FileTokens = filename:split(StorageFileId),
            {Result, NewMarker} = lists:foldl(fun(ChildStorageFileId, {Acc, _LastChildId}) ->
                ChildTokens = filename:split(ChildStorageFileId),
                case length(ChildTokens) - length(FileTokens) > MaxDepth of
                    true ->
                        {Acc, ChildStorageFileId};
                    false ->
                        {Fun(ChildStorageFileId, Acc), ChildStorageFileId}
                end
            end, {Init, Marker}, ChildrenIds),
            TraverseJob2 = TraverseJob#storage_traverse{
                offset = Offset + length(ChildrenIds),
                marker = NewMarker
            },
            case length(ChildrenIds) < BatchSize of
                true ->
                    {ok, Result};
                false ->
                    fold(TraverseJob2, Fun, Result)
            end;
        Error = {error, _} ->
            Error
    end.



-spec generate_master_and_slave_jobs(storage_traverse:job(),
    traverse:master_job_extended_args(), [helpers:file_id()]) -> {ok, traverse:master_job_map()}.
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
    offset = Offset,
    async_master_jobs = AsyncMasterJobs,
    batch_size = BatchSize,
    async_next_batch_job = AsyncNextBatchJob,
    next_batch_job_prehook = AsyncNextBatchJobPrehook,
    max_depth = MaxDepth,
    compute_fun = ComputeFun,
    compute_init = ComputeInit,
    compute_enabled = ComputeEnabled,
    info = Info
}, ChildrenIds, #{master_job_starter_callback := MasterJobStarterCallback}) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    FileTokens = filename:split(StorageFileId),
    ResetInfo = storage_traverse:reset_info(TraverseJob),
    {SlaveJobsRev, LastChildId, ComputeResult} = lists:foldl(fun(ChildStorageFileId, {SlaveJobsIn, _LastChildId, ComputeAcc}) ->
        ChildTokens = filename:split(ChildStorageFileId),
        ChildName = filename:basename(ChildStorageFileId),
        case {length(ChildTokens) - length(FileTokens) > MaxDepth, file_meta:is_hidden(ChildName)} of
            {false, false} ->
                ChildCtx = storage_file_ctx:new(ChildStorageFileId, SpaceId, StorageId),
                {ComputePartialResult, ChildCtx2} = compute(ComputeFun, ChildCtx, Info, ComputeAcc, ComputeEnabled),
                {[{ChildCtx2, ResetInfo} | SlaveJobsIn], ChildStorageFileId, ComputePartialResult};
            _ ->
                {SlaveJobsIn, ChildStorageFileId, ComputeAcc}
        end
    end, {[], undefined, ComputeInit}, ChildrenIds),


    MasterJobs = case length(ChildrenIds) < BatchSize of
        true -> [];
        false ->
            % it is not the last batch
            NextBatchTraverseJob = TraverseJob#storage_traverse{
                offset = Offset + length(ChildrenIds),
                marker = LastChildId
            },
            AsyncNextBatchJobPrehook(NextBatchTraverseJob),
            case AsyncNextBatchJob of
                true ->
                    % schedule job for next batch in this directory asynchronously
                    MasterJobStarterCallback([NextBatchTraverseJob]),
                    [];
                false ->
                    % job for next batch in this directory will be scheduled with children master jobs
                    [NextBatchTraverseJob]
            end
    end,
    SeqSlaveJobs = case {OnDir, Offset} of
        {true, 0} -> [{StorageFileCtx, Info}]; %execute slave job only once per directory
        _ -> []
    end,
    MasterJobsKey = case AsyncMasterJobs of
        true -> async_master_jobs;
        false -> master_jobs
    end,
    MasterJobMap = #{
        MasterJobsKey => MasterJobs,
        sequential_slave_jobs => SeqSlaveJobs,
        slave_jobs => lists:reverse(SlaveJobsRev)
    },
    case ComputeFun =:= undefined orelse ComputeEnabled =:= false of
        true ->
            {ok, MasterJobMap};
        false ->
            {ok, MasterJobMap, ComputeResult}
    end.

-spec batch_id(storage_traverse:job()) -> batch_id().
batch_id(#storage_traverse{offset = Offset, marker = Marker, batch_size = Size}) ->
    {Offset, Marker, Size}.

-spec compute(undefined | storage_traverse:compute(), storage_file_ctx:ctx(),
    storage_traverse:info(), term(), boolean()) -> term().
compute(_, StorageFileCtx, _Info, ComputeAcc, false) ->
    {ComputeAcc, StorageFileCtx};
compute(undefined, StorageFileCtx, _Info, ComputeAcc, _) ->
    {ComputeAcc, StorageFileCtx};
compute(ComputeFun, StorageFileCtx, Info, ComputeAcc, true) ->
    ComputeFun(StorageFileCtx, Info, ComputeAcc).