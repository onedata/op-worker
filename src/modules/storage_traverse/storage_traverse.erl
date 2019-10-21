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
%%% To use storage_traverse, new callback module has to be defined (see traverse_behaviour.erl from cluster_worker) that
%%% uses callbacks defined in this module and additionally provides do_slave_job function implementation.
%%% Next, pool and tasks are started using init and run functions from this module.
%%% The traverse jobs (see traverse.erl for jobs definition) are persisted using tree_traverse_job datastore model
%%% which stores jobs locally and synchronizes main job for each task between providers (to allow tasks execution
%%% on other provider resources).
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
-export([init/4, stop/1, run/5, run/6, reset_info/1, storage_type_callback_module/1]).

%% Traverse callbacks
-export([do_master_job/2, get_job/1, update_job_progress/6]).


-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TASK_ID, <<(?POOL_NAME)/binary, "_", (datastore_utils:gen_key())/binary>>).

-type pool() :: traverse:pool() | atom().
-type info() :: term().
-type job() :: #storage_traverse{}.
-type storage_type_callback_module() :: block_storage_traverse | canonical_object_storage_traverse.

%% @formatter:off
-type next_batch_job_prehook() :: fun( (TraverseJob :: job()) -> term()).
-type children_batch_job_prehook() :: fun( (TraverseJob :: job()) -> term()).

-type compute() :: fun(
    (StorageFileCtx :: storage_file_ctx:ctx(), Info :: info(), ComputeAcc :: term()) ->
    {ComputeResult :: term(), UpdatedStorageFileCtx :: storage_file_ctx:ctx()}
).

% opts that can be passed from calling method
-type run_opts() :: #{
    offset => non_neg_integer(),
    batch_size => non_neg_integer(),
    marker => undefined | helpers:marker(),
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
    children_master_job_prehook => children_batch_job_prehook(),
    % custom function that is called on each listed child
    compute_fun => undefined | compute(),
    % initial argument for compute function
    compute_init => term(),
    % allows to disable compute for specific batch, by default its enabled, but compute_fun must be defined
    compute_enabled => boolean()
}.

%% @formatter:on

-export_type([run_opts/0, opts/0, info/0, job/0, next_batch_job_prehook/0, compute/0,
    children_batch_job_prehook/0, storage_type_callback_module/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init(traverse:pool() | atom(), non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok.
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) when is_atom(Pool) ->
    init(atom_to_binary(Pool, utf8), MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit);
init(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit) ->
    traverse:init_pool(Pool, MasterJobsNum, SlaveJobsNum, ParallelOrdersLimit,
        #{executor => oneprovider:get_id_or_undefined()}).

-spec stop(traverse:pool() | atom()) -> any().
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
    StorageFileId = filename_mapping:space_dir_path(SpaceId, StorageId),
    RootStorageFileCtx = storage_file_ctx:new(StorageFileId, SpaceId, StorageId),
    {ok, StorageDoc} = storage:get(StorageId),
    StorageType = storage:type(StorageDoc),
    DefinedTaskId = case TaskId =:= undefined of
        true -> datastore_utils:gen_key();
        false -> TaskId
    end,
    StorageTypeModule = storage_type_callback_module(StorageType),
    ChildrenMasterJobPrehook = maps:get(children_master_job_prehook, RunOpts, ?DEFAULT_CHILDREN_BATCH_JOB_PREHOOK),
    StorageTraverse = #storage_traverse{
        storage_file_ctx = RootStorageFileCtx,
        space_id = SpaceId,
        storage_doc = StorageDoc,
        storage_type_module = StorageTypeModule,
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
    StorageTraverse2 = StorageTypeModule:init_type_specific_opts(StorageTraverse, RunOpts),
    ChildrenMasterJobPrehook(StorageTraverse2),
    traverse:run(Pool, DefinedTaskId, StorageTraverse2).

-spec storage_type_callback_module(helper:type()) -> module().
storage_type_callback_module(?BLOCK_STORAGE) ->
    block_storage_traverse;
storage_type_callback_module(?OBJECT_STORAGE) ->
    canonical_object_storage_traverse.

%%%===================================================================
%%% Functions for custom modules
%%%===================================================================

-spec reset_info(job()) -> info().
reset_info(TraverseJob = #storage_traverse{callback_module = CallbackModule, info = Info}) ->
    case erlang:function_exported(CallbackModule, reset_info, 1) of
        true ->
            apply(CallbackModule, reset_info, [TraverseJob]);
        false ->
            Info
    end.

%%%===================================================================
%%% Pool callbacks
%%%===================================================================

-spec do_master_job(job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} |{ok, traverse:master_job_map(), term()} | {error, term()}.
do_master_job(TraverseJob = #storage_traverse{
    storage_file_ctx = StorageFileCtx,
    storage_type_module = StorageTypeModule,
    storage_doc = #document{key = StorageId}
}, Args) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    case StorageTypeModule:get_children_batch(TraverseJob) of
        {ok, ChildrenIds} ->
            StorageTypeModule:generate_master_and_slave_jobs(TraverseJob, ChildrenIds, Args);
        Error = {error, ?ENOENT} ->
            ?warning("Getting children of ~p on storage ~p failed due to ~w", [StorageFileId, StorageId, Error]),
            Error;
        Error = {error, _} ->
            ?error("Getting children of ~p on storage ~p failed due to ~w", [StorageFileId, StorageId, Error]),
            Error
    end.

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    traverse:job(), traverse:pool(), traverse:id(), traverse:job_status(), traverse:callback_module()) ->
    {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(main_job, _Job, _Pool, _TaskId, _Status, _CallbackModule) ->
    {ok, datastore_utils:gen_key()};
update_job_progress(Id, Job, Pool, TaskId, Status, CallbackModule)
    when Status =:= waiting
    orelse Status =:= on_pool
    ->
    storage_traverse_job:save_master_job(Id, Job, Pool, TaskId, CallbackModule);
update_job_progress(Id, _Job, _Pool, _TaskId, _Status, _CallbackModule) ->
    ok = storage_traverse_job:delete_master_job(Id),
    {ok, Id}.

-spec get_job(traverse:job_id()) ->
    {ok, traverse:job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    storage_traverse_job:get_master_job(DocOrID).