%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This modules implements functions for traversing files on storage.
%%% It uses traverse framework.
%%% To use storage_traverse, new callback module has to be defined
%%% (see traverse_behaviour.erl from cluster_worker) that
%%% uses callbacks defined in this module and additionally provides
%%% do_slave_job function implementation.
%%% Next, pool and tasks are started using init and run functions from this module.
%%% The traverse jobs (see traverse.erl for jobs definition) are persisted
%%% using storage_traverse_job datastore model.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_traverse).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/storage_traverse/storage_traverse.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([init/4, stop/1, run/5, run/6, get_iterator/1]).

%% @formatter:off
%% Traverse callbacks
-export([do_master_job/2, get_job/1, update_job_progress/5]).

-type pool() :: traverse:pool() | atom().
-type info() :: term().
-type master_job() :: #storage_traverse_master{}.
-type slave_job() :: #storage_traverse_slave{}.
-type children_batch() :: [{helpers:file_id(), Depth :: non_neg_integer()}].
-type iterator() :: block_storage_iterator | canonical_object_storage_iterator.
-type next_batch_job_prehook() :: fun((TraverseJob :: master_job()) -> ok).
-type children_master_job_prehook() :: fun((TraverseJob :: master_job()) -> ok).
-type compute_init() :: term().
-type compute_result() :: term().
-type callback_module() :: module().

-type compute() :: fun(
    (StorageFileCtx :: storage_file_ctx:ctx(), Info :: info(), ComputeAcc :: term()) ->
    {ComputeResult :: term(), UpdatedStorageFileCtx :: storage_file_ctx:ctx()}
).

% opts that can be passed from calling method
-type run_opts() :: #{
    % offset from which children files are listed in order to produce master and slave jobs
    offset => non_neg_integer(),
    % size of batch used to list children files on storage
    batch_size => non_neg_integer(),
    %% argument passed to helpers:listobjects/5 function, see helper.erl
    marker => undefined | helpers:marker(),
    % max depth of directory tree structure that will be processed
    max_depth => non_neg_integer(),
    % flag that informs whether slave_job should be scheduled on directories
    execute_slave_on_dir => boolean(),
    % flag that informs whether children master jobs should be scheduled asynchronously
    async_master_jobs => boolean(),
    % flag that informs whether job for processing next batch of given directory should be scheduled asynchronously
    async_next_batch_job => boolean(),
    % prehook executed before scheduling job for processing next batch of given directory
    next_batch_job_prehook => next_batch_job_prehook(),
    % prehook executed before scheduling job for processing children directory
    children_master_job_prehook => children_master_job_prehook(),
    % custom function that is called on each listed child
    compute_fun => undefined | compute(),
    % initial argument for compute function
    compute_init => term(),
    % allows to disable compute for specific batch, by default its enabled, but compute_fun must be defined
    compute_enabled => boolean()
}.
%% @formatter:on

-export_type([run_opts/0, info/0, master_job/0, slave_job/0, children_batch/0, next_batch_job_prehook/0, compute/0,
    children_master_job_prehook/0, iterator/0, compute_init/0, compute_result/0, callback_module/0]).

%%%===================================================================
%%% Definitions of optional storage_traverse behaviour callbacks
%%%===================================================================
-callback reset_info(master_job()) -> info().

-callback get_next_batch_job_prehook(info()) -> next_batch_job_prehook().

-callback get_children_master_job_prehook(info()) -> children_master_job_prehook().

-callback get_compute_fun(info()) -> compute_result().

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init(traverse:pool() | atom(), non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok.
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) when is_atom(Pool) ->
    init(atom_to_binary(Pool, utf8), MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit);
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) ->
    traverse:init_pool(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit,
        #{executor => oneprovider:get_id_or_undefined()}).

-spec stop(pool() | atom()) -> any().
stop(Pool) when is_atom(Pool) ->
    stop(atom_to_binary(Pool, utf8));
stop(Pool) ->
    traverse:stop_pool(Pool).

-spec run(pool(), od_space:id(), storage:id(), info(), run_opts()) -> ok.
run(Pool, SpaceId, StorageId, TraverseInfo, RunOpts) ->
    run(Pool, undefined, SpaceId, StorageId, TraverseInfo, RunOpts).

-spec run(pool(), traverse:id() | undefined, od_space:id(), storage:id(), info(), run_opts()) -> ok.
run(Pool, TaskId, SpaceId, StorageId, TraverseInfo, RunOpts) when is_atom(Pool) ->
    run(atom_to_binary(Pool, utf8), TaskId, SpaceId, StorageId, TraverseInfo, RunOpts);
run(Pool, TaskId, SpaceId, StorageId, TraverseInfo, RunOpts) ->
    RootStorageFileId = storage_file_id:space_id(SpaceId, StorageId),
    RootStorageFileCtx = storage_file_ctx:new(RootStorageFileId, SpaceId, StorageId),
    StorageType = storage:get_type(StorageId),
    DefinedTaskId = case TaskId =:= undefined of
        true -> datastore_utils:gen_key();
        false -> TaskId
    end,
    Iterator = get_iterator(StorageType),
    ChildrenMasterJobPrehook = maps:get(children_master_job_prehook, RunOpts, ?DEFAULT_CHILDREN_BATCH_JOB_PREHOOK),
    StorageTraverse = #storage_traverse_master{
        storage_file_ctx = RootStorageFileCtx,
        iterator = Iterator,
        execute_slave_on_dir = maps:get(execute_slave_on_dir, RunOpts, ?DEFAULT_EXECUTE_SLAVE_ON_DIR),
        async_next_batch_job = maps:get(async_next_batch_job, RunOpts, ?DEFAULT_ASYNC_NEXT_BATCH_JOB),
        async_master_jobs = maps:get(async_master_jobs, RunOpts, ?DEFAULT_ASYNC_MASTER_JOBS),
        offset = maps:get(offset, RunOpts, 0),
        batch_size = maps:get(batch_size, RunOpts, ?DEFAULT_BATCH_SIZE),
        marker = maps:get(marker, RunOpts, undefined),
        max_depth = maps:get(max_depth, RunOpts, ?DEFAULT_MAX_DEPTH),
        next_batch_job_prehook = maps:get(next_batch_job_prehook, RunOpts, ?DEFAULT_NEXT_BATCH_JOB_PREHOOK),
        children_master_job_prehook = ChildrenMasterJobPrehook,
        compute_fun = maps:get(compute_fun, RunOpts, undefined),
        compute_init = maps:get(compute_init, RunOpts, undefined),
        callback_module = binary_to_atom(Pool, utf8),
        info = TraverseInfo
    },
    StorageTraverse2 = Iterator:init(StorageTraverse, RunOpts),
    ChildrenMasterJobPrehook(StorageTraverse2),
    traverse:run(Pool, DefinedTaskId, StorageTraverse2).

%%%===================================================================
%%% Pool callbacks
%%%===================================================================

-spec do_master_job(master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} |{ok, traverse:master_job_map(), term()} | {error, term()}.
do_master_job(StorageTraverse = #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    iterator = Iterator,
    depth = Depth,
    max_depth = MaxDepth
}, Args) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    case Iterator:get_children_batch(StorageTraverse) of
        {ok, ChildrenIdsWithDepths, StorageTraverse2} ->
            generate_master_and_slave_jobs(StorageTraverse2, ChildrenIdsWithDepths, Args);
        Error = {error, ?ENOENT} ->
            ?alert("Depth: ~p, MaxDepth = ~p", [Depth, MaxDepth]),
            ?warning("Getting children of ~p on storage ~p failed due to ~w", [StorageFileId, StorageId, Error]),
            Error;
        Error = {error, _} ->
            ?error("Getting children of ~p on storage ~p failed due to ~w", [StorageFileId, StorageId, Error]),
            Error
    end.

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    traverse:job(), traverse:pool(), traverse:id(), traverse:job_status()) ->
    {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(main_job, _Job, _Pool, _TaskId, _Status) ->
    {ok, datastore_utils:gen_key()};
update_job_progress(Id, Job, Pool, TaskId, Status)
    when Status =:= waiting
    orelse Status =:= on_pool
    ->
    storage_traverse_job:save_master_job(Id, Job, Pool, TaskId);
update_job_progress(Id, _Job, _Pool, _TaskId, _Status) ->
    ok = storage_traverse_job:delete_master_job(Id),
    {ok, Id}.

-spec get_job(traverse:job_id()) ->
    {ok, traverse:job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    storage_traverse_job:get_master_job(DocOrID).

%%%===================================================================
%%% Internal functions
%%%===================================================================

reset_info(#storage_traverse_master{callback_module = CallbackModule, info = Info}, Args) ->
    case erlang:function_exported(CallbackModule, reset_info, length(Args)) of
        true -> apply(CallbackModule, reset_info, Args);
        false -> Info
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns module implementing storage_iterator associated
%% with passed helper type.
%% @end
%%-------------------------------------------------------------------
-spec get_iterator(helper:type()) -> iterator().
get_iterator(?BLOCK_STORAGE) ->
    block_storage_iterator;
get_iterator(?OBJECT_STORAGE) ->
    canonical_object_storage_iterator.

-spec generate_master_and_slave_jobs(master_job(), [{helpers:file_id(), Depth :: non_neg_integer()}],
    traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} |
    {ok, traverse:master_job_map(), compute_result()}.
generate_master_and_slave_jobs(#storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    max_depth = MaxDepth,
    depth = MaxDepth,
    execute_slave_on_dir = true,
    info = Info
}, _ChildrenIds, _Args) ->
    {ok, #{sequential_slave_jobs => [get_slave_job(StorageFileCtx, Info)]}};
generate_master_and_slave_jobs(#storage_traverse_master{
    depth = MaxDepth,
    max_depth = MaxDepth,
    execute_slave_on_dir = false
}, _ChildrenIds, _Args) ->
    {ok, #{}};
generate_master_and_slave_jobs(StorageTraverse = #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    execute_slave_on_dir = OnDir,
    async_master_jobs = AsyncMasterJobs,
    offset = Offset,
    compute_fun = ComputeFun,
    compute_enabled = ComputeEnabled,
    info = Info
}, ChildrenBatch, Args) ->
    MasterJobs = maybe_schedule_next_batch_job(StorageTraverse, ChildrenBatch, Args),
    {MasterJobs2, SlaveJobs, ComputeResult} = process_children_batch(StorageTraverse, ChildrenBatch),

    SeqSlaveJobs = case {OnDir, Offset =:= length(ChildrenBatch)} of
        {true, true} -> [get_slave_job(StorageFileCtx, Info)]; %execute slave job only once per directory
        _ -> []
    end,
    MasterJobsKey = case AsyncMasterJobs of
        true -> async_master_jobs;
        false -> master_jobs
    end,
    MasterJobsMap =  #{
        MasterJobsKey => MasterJobs ++ MasterJobs2,
        sequential_slave_jobs => SeqSlaveJobs,
        slave_jobs => SlaveJobs
    },
    case ComputeFun =:= undefined orelse ComputeEnabled =:= false of
        true -> {ok, MasterJobsMap};
        false -> {ok, MasterJobsMap, ComputeResult}
    end.

-spec maybe_schedule_next_batch_job(master_job(), children_batch(), traverse:master_job_extended_args()) -> [master_job()].
maybe_schedule_next_batch_job(TraverseJob = #storage_traverse_master{
    batch_size = BatchSize,
    async_next_batch_job = AsyncNextBatchJob,
    next_batch_job_prehook = NextBatchJobPrehook
}, ChildrenBatch, #{master_job_starter_callback := MasterJobStarterCallback})
    when length(ChildrenBatch) =:= BatchSize ->

    % it is not the last batch
    NextBatchJobPrehook(TraverseJob),
    case AsyncNextBatchJob of
        true ->
            % schedule job for next batch in this directory asynchronously
            MasterJobStarterCallback([TraverseJob]),
            [];
        false ->
            % job for next batch in this directory will be scheduled with children master jobs
            [TraverseJob]
    end;
maybe_schedule_next_batch_job(_TraverseJob, _ChildrenBatch, _Args) ->
    [].


-spec process_children_batch(master_job(), children_batch()) -> {[master_job()], [slave_job()], compute_result()}.
process_children_batch(StorageTraverse = #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    iterator = Iterator,
    max_depth = MaxDepth,
    children_master_job_prehook = ChildrenMasterJobPrehook,
    compute_fun = ComputeFun,
    compute_init = ComputeInit,
    compute_enabled = ComputeEnabled,
    info = Info
}, ChildrenBatch) ->
    ResetInfo = reset_info(StorageTraverse, [StorageTraverse]),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    {MasterJobsRev, SlaveJobsRev, ComputeResult} = lists:foldl(
        fun({ChildStorageFileId, ChildDepth}, Acc = {MasterJobsIn, SlaveJobsIn, ComputeAcc}) ->
            ChildName = filename:basename(ChildStorageFileId),
            case {ChildDepth =< MaxDepth, file_meta:is_hidden(ChildName)} of
                {true, false} ->
                    ChildCtx = storage_file_ctx:new(ChildStorageFileId, SpaceId, StorageId),
                    {ComputePartialResult, ChildCtx2} = compute(ComputeFun, ChildCtx, Info, ComputeAcc, ComputeEnabled),
                    case Iterator:is_dir(ChildCtx2) of
                        {false, ChildCtx3} ->
                            {MasterJobsIn, [get_slave_job(ChildCtx3, ResetInfo) | SlaveJobsIn], ComputePartialResult};
                        {true, ChildCtx3} ->
                            ChildMasterJob = get_child_master_job(ChildCtx3, StorageTraverse, ResetInfo, ChildDepth),
                            ChildrenMasterJobPrehook(ChildMasterJob),
                            {[ChildMasterJob | MasterJobsIn], SlaveJobsIn, ComputePartialResult}
                    end;
                _ ->
                    Acc
            end
        end, {[], [], ComputeInit}, ChildrenBatch),

    {lists:reverse(MasterJobsRev), lists:reverse(SlaveJobsRev), ComputeResult}.

%%-------------------------------------------------------------------
%% @doc
%% Calls ComputeFun on ChildCtx if it id defined and if ComputeEnabled == true.
%% @end
%%-------------------------------------------------------------------
-spec compute(undefined | compute(), ChildCtx :: storage_file_ctx:ctx(), info(), compute_init(), ComputeEnabled :: boolean()) ->
    {compute_result(), ChildCtx2 :: storage_file_ctx:ctx()}.
compute(_ComputeFun, _StorageFileCtx, _Info, ComputeAcc, false) ->
    {ComputeAcc, _StorageFileCtx};
compute(undefined, _StorageFileCtx, _Info, ComputeAcc, _ComputeEnabled) ->
    {ComputeAcc, _StorageFileCtx};
compute(ComputeFun, StorageFileCtx, Info, ComputeAcc, true) ->
    ComputeFun(StorageFileCtx, Info, ComputeAcc).

-spec get_child_master_job(storage_file_ctx:ctx(), master_job(), info(), non_neg_integer()) -> master_job().
get_child_master_job(ChildCtx, StorageTraverse, ChildInfo, Depth) ->
    StorageTraverse#storage_traverse_master{
        storage_file_ctx = ChildCtx,
        info = ChildInfo,
        depth = Depth,
        offset = 0,
        compute_enabled = true
    }.

-spec get_slave_job(storage_file_ctx:ctx(), info()) -> slave_job().
get_slave_job(StorageFileCtx, Info) ->
    #storage_traverse_slave{
        storage_file_ctx = StorageFileCtx,
        info = Info
    }.