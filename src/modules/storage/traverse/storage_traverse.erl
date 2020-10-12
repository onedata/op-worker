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
-include("modules/storage/traverse/storage_traverse.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([init/4, stop/1, run/5, run/6, get_iterator/1, list_ongoing_tasks/1]).

%% @formatter:off
%% Traverse callbacks
-export([do_master_job/2, get_job/1, update_job_progress/5]).

-type pool() :: traverse:pool() | atom().
-type info() :: term(). % additional info used by specific module that uses storage_traverse framework
-type master_job() :: #storage_traverse_master{}.
-type slave_job() :: #storage_traverse_slave{}.
-type children_batch() :: [{storage_file_ctx:ctx(), Depth :: non_neg_integer()}].
-type iterator_type() :: ?FLAT_ITERATOR | ?TREE_ITERATOR.
-type next_batch_job_prehook() :: fun((TraverseJob :: master_job()) -> ok).
-type children_master_job_prehook() :: fun((TraverseJob :: master_job()) -> ok).
-type fold_children_init() :: term().
-type fold_children_result() :: term().
-type callback_module() :: module().

-type fold_children_fun() :: fun(
    (StorageFileCtx :: storage_file_ctx:ctx(), Info :: info(), Acc :: term()) ->
    {Result :: term(), UpdatedStorageFileCtx :: storage_file_ctx:ctx()}
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
    async_children_master_jobs => boolean(),
    % flag that informs whether job for processing next batch of given directory should be scheduled asynchronously
    async_next_batch_job => boolean(),
    % prehook executed before scheduling job for processing next batch of given directory
    next_batch_job_prehook => next_batch_job_prehook(),
    % prehook executed before scheduling job for processing children directory
    children_master_job_prehook => children_master_job_prehook(),
    % custom function that is called on each listed child
    % result from one call is passed to next call in the same batch
    % final result is returned from ?MODULE:do_master_job
    fold_children_fun => undefined | fold_children_fun(),
    % initial argument for compute function
    fold_children_init => term(),
    % allows to disable compute for specific batch, by default its enabled, but fold_children_fun must be defined
    fold_children_enabled => boolean()
}.
%% @formatter:on

-export_type([run_opts/0, info/0, master_job/0, slave_job/0, children_batch/0, next_batch_job_prehook/0, fold_children_fun/0,
    children_master_job_prehook/0, iterator_type/0, fold_children_init/0, fold_children_result/0, callback_module/0]).

%%%===================================================================
%%% Definitions of optional storage_traverse behaviour callbacks
%%%===================================================================

-callback reset_info(master_job()) -> info().

-callback get_next_batch_job_prehook(info()) -> next_batch_job_prehook().

-callback get_children_master_job_prehook(info()) -> children_master_job_prehook().

-callback get_fold_children_fun(info()) -> fold_children_result().

-callback on_cancel(MasterJobsDelegated :: non_neg_integer(), SlaveJobsDelegated :: non_neg_integer(),
    storage_file_ctx:ctx()) -> ok.

-optional_callbacks([on_cancel/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init(traverse:pool() | atom(), non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok.
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) when is_atom(Pool) ->
    init(atom_to_binary(Pool, utf8), MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit);
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) ->
    traverse:init_pool(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit).

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
    RootStorageFileId = storage_file_id:space_dir_id(SpaceId, StorageId),
    Iterator = get_iterator(StorageId),
    RootStorageFileCtx = Iterator:init_root_storage_file_ctx(RootStorageFileId, SpaceId, StorageId),
    DefinedTaskId = case TaskId =:= undefined of
        true -> datastore_key:new();
        false -> TaskId
    end,
    ChildrenMasterJobPrehook = maps:get(children_master_job_prehook, RunOpts, ?DEFAULT_CHILDREN_BATCH_JOB_PREHOOK),
    StorageTraverse = #storage_traverse_master{
        storage_file_ctx = RootStorageFileCtx,
        iterator_module = Iterator,
        execute_slave_on_dir = maps:get(execute_slave_on_dir, RunOpts, ?DEFAULT_EXECUTE_SLAVE_ON_DIR),
        async_next_batch_job = maps:get(async_next_batch_job, RunOpts, ?DEFAULT_ASYNC_NEXT_BATCH_JOB),
        async_children_master_jobs = maps:get(async_children_master_jobs, RunOpts, ?DEFAULT_ASYNC_CHILDREN_MASTER_JOBS),
        offset = maps:get(offset, RunOpts, 0),
        batch_size = maps:get(batch_size, RunOpts, ?DEFAULT_BATCH_SIZE),
        marker = maps:get(marker, RunOpts, ?DEFAULT_MARKER),
        max_depth = maps:get(max_depth, RunOpts, ?DEFAULT_MAX_DEPTH),
        next_batch_job_prehook = maps:get(next_batch_job_prehook, RunOpts, ?DEFAULT_NEXT_BATCH_JOB_PREHOOK),
        children_master_job_prehook = ChildrenMasterJobPrehook,
        fold_children_fun = maps:get(fold_children_fun, RunOpts, undefined),
        fold_init = maps:get(fold_children_init, RunOpts, undefined),
        callback_module = binary_to_atom(Pool, utf8),
        info = TraverseInfo
    },
    ChildrenMasterJobPrehook(StorageTraverse),
    traverse:run(Pool, DefinedTaskId, StorageTraverse).

%%-------------------------------------------------------------------
%% @doc
%% Returns type of iterator, which is also the name of a module
%% implementing storage_iterator behaviour.
%% Iterator type is associated with helper type.
%% @end
%%-------------------------------------------------------------------
-spec get_iterator(storage:id()) -> iterator_type().
get_iterator(Storage) ->
    HelperName = storage:get_helper_name(Storage),
    case HelperName of
        ?POSIX_HELPER_NAME -> ?TREE_ITERATOR;
        ?WEBDAV_HELPER_NAME -> ?TREE_ITERATOR;
        ?XROOTD_HELPER_NAME -> ?TREE_ITERATOR;
        ?HTTP_HELPER_NAME -> ?TREE_ITERATOR;
        ?GLUSTERFS_HELPER_NAME -> ?TREE_ITERATOR;
        ?NULL_DEVICE_HELPER_NAME -> ?TREE_ITERATOR;
        ?CEPH_HELPER_NAME -> ?FLAT_ITERATOR;
        ?CEPHRADOS_HELPER_NAME -> ?FLAT_ITERATOR;
        ?S3_HELPER_NAME -> ?FLAT_ITERATOR;
        ?SWIFT_HELPER_NAME -> ?FLAT_ITERATOR
    end.


-spec list_ongoing_tasks(pool()) -> {ok, [traverse:id()]} | {error, term()}.
list_ongoing_tasks(Pool) when is_atom(Pool) ->
    list_ongoing_tasks(atom_to_binary(Pool, utf8));
list_ongoing_tasks(Pool) when is_binary(Pool) ->
    case traverse_task_list:list(Pool, ongoing) of
        {ok, TaskIds, _} -> {ok, TaskIds};
        {error, _} = Error -> Error
    end.

%%%===================================================================
%%% Pool callbacks
%%%===================================================================

-spec do_master_job(master_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} |{ok, traverse:master_job_map(), fold_children_result()} | {error, term()}.
do_master_job(MasterJob = #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    iterator_module = Iterator
}, Args) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    case Iterator:get_children_and_next_batch_job(MasterJob) of
        {ok, ChildrenBatch, NextBatchMasterJob} ->
            generate_master_and_slave_jobs(MasterJob, NextBatchMasterJob, ChildrenBatch, Args);
        Error = {error, ?ENOENT} ->
            ?debug("Getting children of ~p on storage ~p failed due to ~w", [StorageFileId, StorageId, Error]),
            Error;
        Error = {error, _} ->
            ?error("Getting children of ~p on storage ~p failed due to ~w", [StorageFileId, StorageId, Error]),
            Error
    end.

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    traverse:job(), traverse:pool(), traverse:id(), traverse:job_status()) ->
    {ok, traverse:job_id()}  | {error, term()}.
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

reset_info(MasterJob = #storage_traverse_master{callback_module = CallbackModule, info = Info}) ->
    case erlang:function_exported(CallbackModule, reset_info, 1) of
        true -> apply(CallbackModule, reset_info, [MasterJob]);
        false -> Info
    end.

-spec generate_master_and_slave_jobs(master_job(), master_job() | undefined, children_batch(),
    traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} |
    {ok, traverse:master_job_map(), fold_children_result()}.
generate_master_and_slave_jobs(#storage_traverse_master{
    callback_module = CallbackModule,
    storage_file_ctx = StorageFileCtx,
    max_depth = MaxDepth,
    depth = MaxDepth,
    execute_slave_on_dir = true,
    fold_children_fun = FoldChildrenFun,
    fold_enabled = FoldChildrenEnabled,
    fold_init = FoldInit,
    info = Info
}, _NextBatchMaterJob, _ChildrenIds, _Args) ->
    MasterJobsMap = #{sequential_slave_jobs => [get_slave_job(StorageFileCtx, Info)]},
    MasterJobsMap2 = add_cancel_callback(CallbackModule, MasterJobsMap, StorageFileCtx),
    case FoldChildrenFun =:= undefined orelse FoldChildrenEnabled =:= false of
        true -> {ok, MasterJobsMap2};
        false -> {ok, MasterJobsMap2, FoldInit}
    end;
generate_master_and_slave_jobs(#storage_traverse_master{
    callback_module = CallbackModule,
    storage_file_ctx = StorageFileCtx,
    depth = MaxDepth,
    max_depth = MaxDepth,
    execute_slave_on_dir = false,
    fold_children_fun = FoldChildrenFun,
    fold_enabled = FoldChildrenEnabled,
    fold_init = FoldInit
}, _NextBatchMaterJob, _ChildrenIds, _Args) ->
    MasterJobsMap = add_cancel_callback(CallbackModule, #{}, StorageFileCtx),
    case FoldChildrenFun =:= undefined orelse FoldChildrenEnabled =:= false of
        true -> {ok, MasterJobsMap};
        false -> {ok, MasterJobsMap, FoldInit}
    end;
generate_master_and_slave_jobs(CurrentMasterJob = #storage_traverse_master{
    callback_module = CallbackModule,
    storage_file_ctx = StorageFileCtx,
    execute_slave_on_dir = OnDir,
    async_children_master_jobs = AsyncChildrenMasterJobs,
    offset = Offset,
    fold_children_fun = FoldChildrenFun,
    fold_enabled = FoldChildrenEnabled,
    info = Info
}, NextBatchMaterJob, ChildrenBatch, Args) ->
    MasterJobs = maybe_schedule_next_batch_job(CurrentMasterJob, NextBatchMaterJob, Args),
    {MasterJobs2, SlaveJobs, ComputeResult} = process_children_batch(CurrentMasterJob, ChildrenBatch),

    SeqSlaveJobs = case {OnDir, Offset =:= 0} of
        {true, true} -> [get_slave_job(StorageFileCtx, Info)]; %execute slave job only once per directory
        _ -> []
    end,
    MasterJobsKey = case AsyncChildrenMasterJobs of
        true -> async_master_jobs;
        false -> master_jobs
    end,
    MasterJobsMap =  add_cancel_callback(CallbackModule, #{
        MasterJobsKey => MasterJobs ++ MasterJobs2,
        sequential_slave_jobs => SeqSlaveJobs,
        slave_jobs => SlaveJobs
    }, Info),
    MasterJobsMap2 = add_cancel_callback(CallbackModule, MasterJobsMap, StorageFileCtx),
    case FoldChildrenFun =:= undefined orelse FoldChildrenEnabled =:= false of
        true -> {ok, MasterJobsMap2};
        false -> {ok, MasterJobsMap2, ComputeResult}
    end.

-spec maybe_schedule_next_batch_job(master_job(), master_job() | undefined, traverse:master_job_extended_args()) ->
    [master_job()].
maybe_schedule_next_batch_job(_CurrentMasterJob, undefined, _Args) ->
    [];
maybe_schedule_next_batch_job(#storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    callback_module = CallbackModule,
    async_next_batch_job = AsyncNextBatchJob,
    next_batch_job_prehook = NextBatchJobPrehook
}, NextBatchMasterJob = #storage_traverse_master{}, #{master_job_starter_callback := MasterJobStarterCallback}) ->
    % it is not the last batch
    NextBatchJobPrehook(NextBatchMasterJob),
    case AsyncNextBatchJob of
        true ->
            % schedule job for next batch in this directory asynchronously
            MasterJobStarterCallback(#{
                jobs => [NextBatchMasterJob],
                cancel_callback => fun(_CancelDescription) ->
                    call_on_cancel_callback(CallbackModule, 1, 0, StorageFileCtx)
                end
            }),
            [];
        false ->
            % job for next batch in this directory will be scheduled with children master jobs
            [NextBatchMasterJob]
    end;
maybe_schedule_next_batch_job(_CurrentMasterJob, _NextBatchMasterJob, _Args) ->
    [].


-spec process_children_batch(master_job(), children_batch()) -> {[master_job()], [slave_job()], fold_children_result()}.
process_children_batch(CurrentMasterJob = #storage_traverse_master{
    iterator_module = Iterator,
    max_depth = MaxDepth,
    children_master_job_prehook = ChildrenMasterJobPrehook,
    fold_children_fun = FoldChildrenFun,
    fold_init = FoldChildrenInit,
    fold_enabled = FoldChildrenEnabled,
    info = Info
}, ChildrenBatch) ->
    ResetInfo = reset_info(CurrentMasterJob),
    {MasterJobsRev, SlaveJobsRev, ComputeResult} = lists:foldl(
        fun({ChildCtx, ChildDepth}, Acc = {MasterJobsIn, SlaveJobsIn, ComputeAcc}) ->
            ChildStorageFileId = storage_file_ctx:get_storage_file_id_const(ChildCtx),
            ChildName = filename:basename(ChildStorageFileId),
            case {ChildDepth =< MaxDepth, file_meta:is_hidden(ChildName)} of
                {true, false} ->
                    {ComputePartialResult, ChildCtx2} = compute(FoldChildrenFun, ChildCtx, Info, ComputeAcc,
                        FoldChildrenEnabled),
                    case Iterator:should_generate_master_job(ChildCtx2) of
                        {false, ChildCtx3} ->
                            {MasterJobsIn, [get_slave_job(ChildCtx3, ResetInfo) | SlaveJobsIn], ComputePartialResult};
                        {true, ChildCtx3} ->
                            ChildMasterJob = get_child_master_job(ChildCtx3, CurrentMasterJob, ResetInfo, ChildDepth),
                            ChildrenMasterJobPrehook(ChildMasterJob),
                            {[ChildMasterJob | MasterJobsIn], SlaveJobsIn, ComputePartialResult}
                    end;
                _ ->
                    Acc
            end
        end, {[], [], FoldChildrenInit}, ChildrenBatch),

    {lists:reverse(MasterJobsRev), lists:reverse(SlaveJobsRev), ComputeResult}.

%%-------------------------------------------------------------------
%% @doc
%% Calls FoldChildrenFun on ChildCtx if it id defined and if ComputeEnabled == true.
%% @end
%%-------------------------------------------------------------------
-spec compute(undefined | fold_children_fun(), ChildCtx :: storage_file_ctx:ctx(), info(), fold_children_result(), ComputeEnabled :: boolean()) ->
    {fold_children_result(), ChildCtx2 :: storage_file_ctx:ctx()}.
compute(_FoldChildrenFun, _StorageFileCtx, _Info, FoldChildrenResult, false) ->
    {FoldChildrenResult, _StorageFileCtx};
compute(undefined, _StorageFileCtx, _Info, FoldChildrenResult, _ComputeEnabled) ->
    {FoldChildrenResult, _StorageFileCtx};
compute(FoldChildrenFun, StorageFileCtx, Info, FoldChildrenResult, true) ->
    FoldChildrenFun(StorageFileCtx, Info, FoldChildrenResult).

-spec get_child_master_job(storage_file_ctx:ctx(), master_job(), info(), non_neg_integer()) -> master_job().
get_child_master_job(ChildCtx, StorageTraverse, ChildInfo, Depth) ->
    StorageTraverse#storage_traverse_master{
        storage_file_ctx = ChildCtx,
        info = ChildInfo,
        depth = Depth,
        offset = 0,
        fold_enabled = true
    }.

-spec get_slave_job(storage_file_ctx:ctx(), info()) -> slave_job().
get_slave_job(StorageFileCtx, Info) ->
    #storage_traverse_slave{
        storage_file_ctx = StorageFileCtx,
        info = Info
    }.


-spec add_cancel_callback(callback_module(), traverse:master_job_map(), storage_file_ctx:ctx()) ->
    traverse:master_job_map().
add_cancel_callback(CallbackModule, MasterJobsMap, StorageFileCtx) ->
    MasterJobsMap#{
        cancel_callback => fun(Description) ->
            MasterJobsDelegated = -1 * ((maps:get(master_jobs_delegated, Description)) + 1),
            SlaveJobsDelegated = -1 * maps:get(slave_jobs_delegated, Description),
            call_on_cancel_callback(CallbackModule, MasterJobsDelegated, SlaveJobsDelegated, StorageFileCtx)
        end
    }.


-spec call_on_cancel_callback(callback_module(), non_neg_integer(), non_neg_integer(), storage_file_ctx:ctx()) ->
    traverse:master_job_map().
call_on_cancel_callback(CallbackModule, MasterJobsDelegated, SlaveJobsDelegated, StorageFileCtx) ->
    case erlang:function_exported(CallbackModule, on_cancel, 3) of
        true ->
            CallbackModule:on_cancel(MasterJobsDelegated, SlaveJobsDelegated, StorageFileCtx);
        false ->
            ok
    end.