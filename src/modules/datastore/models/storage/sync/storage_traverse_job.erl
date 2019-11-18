%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Model for persisting storage_traverse jobs.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_traverse_job).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/storage/traverse/storage_traverse.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([save_master_job/4, delete_master_job/1, get_master_job/1]).

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

-spec save_master_job(undefined | datastore:key(), storage_traverse:master_job(), traverse:pool(), traverse:id()) ->
    {ok, key()} | {error, term()}.
save_master_job(Key, StorageTraverseMaster = #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    callback_module = CallbackModule
}, Pool, TaskId) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    % if Key == main_job, set Key to undefined so that datastore will generate key
    Key2 = utils:ensure_defined(Key, main_job, undefined),
    ?extract_key(datastore_model:save(?CTX, #document{
        key = Key2,
        value = create_record(Pool, TaskId, CallbackModule, StorageTraverseMaster),
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
    iterator_module = IteratorModule,
    offset = Offset,
    batch_size = BatchSize,
    marker = Marker,
    max_depth = MaxDepth,
    execute_slave_on_dir = ExecuteSlaveOnDir,
    async_children_master_jobs = AsyncChildrenMasterJobs,
    async_next_batch_job = AsyncNextBatchJob,
    fold_children_init = FoldChildrenInit,
    fold_children_enabled = FoldChildrenEnabled,
    info = Info
}}) ->
    TraverseInfo = binary_to_term(Info),
    Job = #storage_traverse_master{
        storage_file_ctx = storage_file_ctx:new(StorageFileId, SpaceId, StorageId),
        iterator_module = IteratorModule,
        offset = Offset,
        batch_size = BatchSize,
        marker = Marker,
        max_depth = MaxDepth,
        next_batch_job_prehook = get_next_batch_job_prehook(CallbackModule, TraverseInfo),
        children_master_job_prehook = get_children_master_job_prehook(CallbackModule, TraverseInfo),
        fold_children_fun = get_fold_children_fun(CallbackModule, TraverseInfo),
        execute_slave_on_dir = ExecuteSlaveOnDir,
        async_children_master_jobs = AsyncChildrenMasterJobs,
        async_next_batch_job = AsyncNextBatchJob,
        fold_init = FoldChildrenInit,
        fold_enabled = FoldChildrenEnabled,
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
        {iterator_module, atom},
        {offset, integer},
        {batch_size, integer},
        {marker, string},
        {max_depth, integer},
        {execute_slave_on_dir, boolean},
        {async_children_master_jobs, boolean},
        {async_next_batch_job, boolean},
        {fold_children_init, term},
        {fold_children_enabled, boolean},
        {info, binary}
    ]}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create_record(traverse:pool(), traverse:callback_module(), traverse:id(), storage_traverse:master_job()) ->
    record().
create_record(Pool, TaskId, CallbackModule, #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    iterator_module = IteratorModule,
    offset = Offset,
    batch_size = BatchSize,
    marker = Marker,
    max_depth = MaxDepth,
    execute_slave_on_dir = ExecuteSlaveOnDir,
    async_children_master_jobs = AsyncChildrenMasterJobs,
    async_next_batch_job = AsyncNextBatchJob,
    fold_init = FoldChildrenInit,
    fold_enabled = FoldChildrenEnabled,
    info = Info
}) ->
    StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    #storage_traverse_job{
        pool = Pool,
        task_id = TaskId,
        callback_module = CallbackModule,
        storage_file_id = StorageFileId,
        space_id = SpaceId,
        storage_id = StorageId,
        iterator_module = IteratorModule,
        offset = Offset,
        batch_size = BatchSize,
        marker = Marker,
        max_depth = MaxDepth,
        execute_slave_on_dir = ExecuteSlaveOnDir,
        async_children_master_jobs = AsyncChildrenMasterJobs,
        async_next_batch_job = AsyncNextBatchJob,
        fold_children_init = FoldChildrenInit,
        fold_children_enabled = FoldChildrenEnabled,
        info = term_to_binary(Info)
    }.

-spec get_next_batch_job_prehook(traverse:callback_module(), storage_traverse:info()) ->
    storage_traverse:next_batch_job_prehook().
get_next_batch_job_prehook(CallbackModule, TraverseInfo) ->
    case erlang:function_exported(CallbackModule, get_next_batch_job_prehook, 1) of
        true ->
            CallbackModule:get_next_batch_job_prehook(TraverseInfo);
        false ->
            ?DEFAULT_NEXT_BATCH_JOB_PREHOOK
    end.

-spec get_children_master_job_prehook(traverse:callback_module(), storage_traverse:info()) ->
    storage_traverse:children_master_job_prehook().
get_children_master_job_prehook(CallbackModule, TraverseInfo) ->
    case erlang:function_exported(CallbackModule, get_children_master_job_prehook, 1) of
        true ->
            CallbackModule:get_children_master_job_prehook(TraverseInfo);
        false ->
            ?DEFAULT_CHILDREN_BATCH_JOB_PREHOOK
    end.

-spec get_fold_children_fun(traverse:callback_module(), storage_traverse:info()) ->
    storage_traverse:fold_children_fun() | undefined.
get_fold_children_fun(CallbackModule, TraverseInfo) ->
    case erlang:function_exported(CallbackModule, get_fold_children_fun, 1) of
        true ->
            CallbackModule:get_fold_children_fun(TraverseInfo);
        false ->
            undefined
    end.