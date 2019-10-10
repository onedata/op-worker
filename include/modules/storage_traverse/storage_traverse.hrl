%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Header file for modules using storage_traverse pool.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(STORAGE_TRAVERSE_HRL).
-define(STORAGE_TRAVERSE_HRL, 1).

-include("global_definitions.hrl").

-define(DEFAULT_EXECUTE_SLAVE_ON_DIR, false).
-define(DEFAULT_ASYNC_MASTER_JOBS, false).
-define(DEFAULT_ASYNC_NEXT_BATCH_JOB, false).
-define(DEFAULT_OFFSET, 0).
-define(DEFAULT_BATCH_SIZE, application:get_env(?APP_NAME, storage_traverse_batch_size, 100)).
-define(DEFAULT_MAX_DEPTH, 65535).
-define(DEFAULT_NEXT_BATCH_JOB_PREHOOK, fun(_StorageTraverse) -> ok end).
-define(DEFAULT_CHILDREN_BATCH_JOB_PREHOOK, fun(_StorageTraverse) -> ok end).

-record(storage_traverse, {
    storage_file_ctx :: storage_file_ctx:ctx(),
    space_id :: od_space:id(),
    storage_doc :: storage:doc(),
    storage_type_module :: storage_traverse:storage_type_callback_module(),
    execute_slave_on_dir = ?DEFAULT_EXECUTE_SLAVE_ON_DIR :: boolean(),
    async_master_jobs = ?DEFAULT_ASYNC_MASTER_JOBS :: boolean(),
    async_next_batch_job = ?DEFAULT_ASYNC_NEXT_BATCH_JOB :: boolean(),
    next_batch_job_prehook = ?DEFAULT_NEXT_BATCH_JOB_PREHOOK :: storage_traverse:next_batch_job_prehook(),
    children_master_job_prehook = ?DEFAULT_CHILDREN_BATCH_JOB_PREHOOK :: storage_traverse:children_batch_job_prehook(),
    offset = ?DEFAULT_OFFSET :: non_neg_integer(),
    batch_size = ?DEFAULT_BATCH_SIZE :: non_neg_integer(),
    marker :: undefined | helpers:marker(),
    max_depth = ?DEFAULT_MAX_DEPTH :: non_neg_integer(),
    % custom function that is called on each listed child
    compute_fun :: undefined | storage_traverse:compute(),
    compute_init :: term(),
    % allows to disable compute for specific batch, by default its enabled, but compute_fun must be defined
    compute_enabled = true :: boolean(),
    callback_module :: traverse:callback_module(),
    info :: storage_traverse:info()
}).

-endif.