%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% WRITEME
% todo describe compute_fun
%%% @end
%%%-------------------------------------------------------------------
-module(storage_traverse).
-author("Jakub Kudzia").


-include("global_definitions.hrl").
-include("modules/storage_traverse/storage_traverse.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% API
-export([init/4, stop/1, run/5, run/6, reset_info/1, fold/3]).

%% Traverse callbacks
-export([do_master_job/2, get_job/1, update_job_progress/5]).

%% Util functions
-export([get_storage_file_ctx/1, get_space_id/1, get_storage_id/1]).

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TASK_ID, <<(?POOL_NAME)/binary, "_", (datastore_utils:gen_key())/binary>>).

-type pool() :: traverse:pool() | atom().
-type info() :: term().
-type job() :: #storage_traverse{}.

%% @formatter:off
-type next_batch_job_prehook() :: fun( (TraverseJob :: job()) -> term()).
-type children_batch_job_prehook() :: fun( (TraverseJob :: job()) -> term()).

-type compute() :: fun(
    (StorageFileCtx :: storage_file_ctx:ctx(), ComputeAcc :: term()) ->
    {ComputeResult :: term(), UpdatedStorageFileCtx :: storage_file_ctx:ctx()}
).

% opts that can be passed from calling method
-type run_opts() :: #{
    execute_slave_on_dir => boolean(),
    async_master_jobs => boolean(),
    offset => non_neg_integer(),
    batch_size => non_neg_integer(),
    marker => helpers:marker(),
    max_depth => non_neg_integer()
}.

-type opts() :: #{
    offset := non_neg_integer(),
    batch_size := non_neg_integer(),
    marker => helpers:marker(),
    max_depth := non_neg_integer()
}.
%% @formatter:on

-export_type([run_opts/0, opts/0, info/0, job/0, next_batch_job_prehook/0, compute/0, children_batch_job_prehook/0]).

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
        % todo use defaults here
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

-spec fold(job(),
    fun((ChildStorageFileId :: helpers:file_id(), Acc :: term()) -> Result :: term()),
    term()) -> {ok, term()} | {error, term()}.
fold(StorageTraverse = #storage_traverse{storage_type_module = StorageTypeModule}, Fun, Init) ->
    StorageTypeModule:fold(StorageTraverse, Fun, Init).

%%%===================================================================
%%% Functions for custom modules TODO
%%%===================================================================

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

-spec do_master_job(job(), traverse:id()) -> {ok, traverse:master_job_map()} | {error, term()}.
do_master_job(TraverseJob = #storage_traverse{
    storage_file_ctx = StorageFileCtx,
    storage_type_module = StorageTypeModule,
    storage_doc = #document{key = StorageId}
}, Args) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    case StorageTypeModule:get_children_batch(TraverseJob) of
        {ok, ChildrenIds} ->
%%            ?alert("CHILDREN ~p", [ChildrenIds]),
            StorageTypeModule:generate_master_and_slave_jobs(TraverseJob, ChildrenIds, Args);
        Error = {error, ?ENOENT} ->
            ?warning("Getting children of ~p on storage ~p failed due to ~w", [StorageFileId, StorageId, Error]),
            Error;
        Error = {error, _} ->
            ?error("Getting children of ~p on storage ~p failed due to ~w", [StorageFileId, StorageId, Error]),
            Error
    end.

-spec update_job_progress(undefined | main_job | traverse:job_id(),
    traverse:job(), traverse:pool(), traverse:id(), traverse:job_status()) ->
    {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(ID, Job, Pool, TaskID, Status) ->
    % TODO
    %%    ?alert("update_job_progress ~p", [{ID, Job, Pool, TaskID, Status}]),
    ID2 = list_to_binary(ref_to_list(make_ref())),
    {ok, ID2}.

-spec get_job(traverse:job_id()) ->
    {ok, traverse:job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    % TODO
    %%    ?alert("update_job_progress ~p", [{ID, Job, Pool, TaskID, Status}]),
    ok.

%%===================================================================
%% Util functions
%%===================================================================

get_storage_file_ctx(#storage_traverse{storage_file_ctx = StorageFileCtx}) ->
    StorageFileCtx.

get_space_id(#storage_traverse{space_id = SpaceId}) ->
    SpaceId.

get_storage_id(#storage_traverse{storage_doc = #document{key = StorageId}}) ->
    StorageId.

%%===================================================================
%% Internal functions
%%===================================================================

-spec storage_type_callback_module(helper:type()) -> module().
storage_type_callback_module(?BLOCK_STORAGE) ->
    block_storage_traverse;
storage_type_callback_module(?OBJECT_STORAGE) ->
    object_storage_traverse.