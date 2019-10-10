%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains implementation of helper model used by
%%% storage_sync.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_traverse_job).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/storage_traverse/storage_traverse.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([save_master_job/5, delete_master_job/1, get_master_job/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type key() :: datastore:key().
-type record() :: #storage_traverse_job{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([doc/0]).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

-spec save_master_job(undefined | datastore:key(), storage_traverse:job(), traverse:pool(), traverse:id(),
    traverse:callback_module()) -> {ok, key()} | {error, term()}.
save_master_job(Key, StorageTraverseJob = #storage_traverse{space_id = SpaceId}, Pool, TaskId, CallbackModule) ->
    ?extract_key(datastore_model:save(?CTX, #document{
        key = Key,
        value = create_record(Pool, TaskId, CallbackModule, StorageTraverseJob),
        scope = SpaceId
    })).

-spec delete_master_job(datastore:key()) -> ok | {error, term()}.
delete_master_job(Key) ->
    datastore_model:delete(?CTX, Key).

-spec get_master_job(key() | doc()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), traverse:id()}  | {error, term()}.
get_master_job(#document{value = #storage_traverse_job{
    pool = Pool,
    task_id = TaskID,
    callback_module = CallbackModule,
    storage_file_id = StorageFileId,
    space_id = SpaceId,
    storage_id = StorageId,
    offset = Offset,
    batch_size = BatchSize,
    marker = Marker,
    max_depth = MaxDepth,
    execute_slave_on_dir = ExecuteSlaveOnDir,
    async_master_jobs = AsyncMasterJobs,
    async_next_batch_job = AsyncNextBatchJob,
    compute_init = ComputeInit,
    compute_enabled = ComputeEnabled,
    info = Info
}}) ->
    {ok, Doc} = storage:get(StorageId),
    StorageType = storage:type(Doc),
    StorageTypeModule = storage_traverse:storage_type_callback_module(StorageType),
    TraverseInfo = binary_to_term(Info),
    Job = #storage_traverse{
        storage_file_ctx = storage_file_ctx:new(StorageFileId, SpaceId, StorageId),
        space_id = SpaceId,
        storage_doc = Doc,
        storage_type_module = StorageTypeModule,
        offset = Offset,
        batch_size = BatchSize,
        marker = Marker,
        max_depth = MaxDepth,
        next_batch_job_prehook = next_batch_job_prehook(CallbackModule, TraverseInfo),
        children_master_job_prehook = children_master_job_prehook(CallbackModule, TraverseInfo),
        compute_fun = compute_fun(CallbackModule, TraverseInfo),
        execute_slave_on_dir = ExecuteSlaveOnDir,
        async_master_jobs = AsyncMasterJobs,
        async_next_batch_job = AsyncNextBatchJob,
        compute_init = ComputeInit,
        compute_enabled = ComputeEnabled,
        callback_module = CallbackModule,
        info = TraverseInfo
    },
    {ok, Job, Pool, TaskID};
get_master_job(Key) ->
    case datastore_model:get(?CTX#{include_deleted => true}, Key) of
        {ok, Doc} ->
            get_master_job(Doc);
        Other ->
            Other
    end.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context used by datastore and dbsync.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {pool, string},
        {task_id, string},
        {callback_module, atom},
        {storage_file_id, string},
        {space_id, string},
        {storage_id, string},
        {offset, integer},
        {batch_size, integer},
        {marker, string},
        {max_depth, integer},
        {execute_slave_on_dir, boolean},
        {async_master_jobs, boolean},
        {async_next_batch_job, boolean},
        {compute_init, term},
        {compute_enabled, boolean},
        {info, binary}
    ]}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create_record(traverse:pool(), traverse:callback_module(), traverse:id(), storage_traverse:job()) ->
    record().
create_record(Pool, TaskId, CallbackModule, #storage_traverse{
    storage_file_ctx = StorageFileCtx,
    space_id = SpaceId,
    storage_doc = #document{key = StorageId},
    offset = Offset,
    batch_size = BatchSize,
    marker = Marker,
    max_depth = MaxDepth,
    execute_slave_on_dir = ExecuteSlaveOnDir,
    async_master_jobs = AsyncMasterJobs,
    async_next_batch_job = AsyncNextBatchJob,
    compute_init = ComputeInit,
    compute_enabled = ComputeEnabled,
    info = Info
}) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    #storage_traverse_job{
        pool = Pool,
        task_id = TaskId,
        callback_module = CallbackModule,
        storage_file_id = StorageFileId,
        space_id = SpaceId,
        storage_id = StorageId,
        offset = Offset,
        batch_size = BatchSize,
        marker = Marker,
        max_depth = MaxDepth,
        execute_slave_on_dir = ExecuteSlaveOnDir,
        async_master_jobs = AsyncMasterJobs,
        async_next_batch_job = AsyncNextBatchJob,
        compute_init = ComputeInit,
        compute_enabled = ComputeEnabled,
        info = term_to_binary(Info)
    }.

-spec next_batch_job_prehook(traverse:callback_module(), storage_traverse:info()) ->
    storage_traverse:next_batch_job_prehook().
next_batch_job_prehook(CallbackModule, TraverseInfo) ->
    case erlang:function_exported(CallbackModule, next_batch_job_prehook, 1) of
        true ->
            CallbackModule:next_batch_job_prehook(TraverseInfo);
        false ->
            ?DEFAULT_NEXT_BATCH_JOB_PREHOOK
    end.

-spec children_master_job_prehook(traverse:callback_module(), storage_traverse:info()) ->
    storage_traverse:children_batch_job_prehook().
children_master_job_prehook(CallbackModule, TraverseInfo) ->
    case erlang:function_exported(CallbackModule, children_master_job_prehook, 1) of
        true ->
            CallbackModule:children_master_job_prehook(TraverseInfo);
        false ->
            ?DEFAULT_CHILDREN_BATCH_JOB_PREHOOK
    end.

-spec compute_fun(traverse:callback_module(), storage_traverse:info()) ->
    storage_traverse:compute() | undefined.
compute_fun(CallbackModule, TraverseInfo) ->
    case erlang:function_exported(CallbackModule, compute_fun, 1) of
        true ->
            CallbackModule:compute_fun(TraverseInfo);
        false ->
            undefined
    end.