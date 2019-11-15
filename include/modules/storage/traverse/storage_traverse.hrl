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
-define(DEFAULT_ASYNC_CHILDREN_MASTER_JOBS, false).
-define(DEFAULT_ASYNC_NEXT_BATCH_JOB, false).
-define(DEFAULT_OFFSET, 0).
-define(DEFAULT_BATCH_SIZE, application:get_env(?APP_NAME, storage_traverse_batch_size, 100)).
-define(DEFAULT_MAX_DEPTH, 65535).
-define(DEFAULT_NEXT_BATCH_JOB_PREHOOK, fun(_StorageTraverse) -> ok end).
-define(DEFAULT_CHILDREN_BATCH_JOB_PREHOOK, fun(_StorageTraverse) -> ok end).

% record defining master job executed by traverse framework
% master jobs defined by #storage_traverse record are associated with directories found on storage
% master jobs are used to produce next master jobs and slave jobs for traverse framework
-record(storage_traverse_master, {
    % opaque data structure storing information about file on storage, working as a cache
    % master job defined by #storage_traverse{} is associated with file represented by StorageFileCtx
    storage_file_ctx :: storage_file_ctx:ctx(),
    % callback module that uses storage_traverse framework
    % the module must implement functions required by traverse.erl
    callback_module :: traverse:callback_module(),
    % storage specific module that encapsulates details of listing files on specific storage helpers
    iterator :: storage_traverse:iterator(),
    % offset from which children files are listed in order to produce master and slave jobs
    offset = ?DEFAULT_OFFSET :: non_neg_integer(),
    % size of batch used to list children files on storage
    batch_size = ?DEFAULT_BATCH_SIZE :: non_neg_integer(),
    % marker passed to helpers:listobjects function when iterator=canonical_object_storage_iterator
    marker :: undefined | helpers:marker(),
    % depth of directory in the tree structure
    depth = 0 :: non_neg_integer(),
    % max depth of directory tree structure that will be processed
    max_depth = ?DEFAULT_MAX_DEPTH :: non_neg_integer(),
    % flag that informs whether slave_job should be scheduled on directories
    execute_slave_on_dir = ?DEFAULT_EXECUTE_SLAVE_ON_DIR :: boolean(),
    % flag that informs whether children master jobs should be scheduled asynchronously
    async_children_master_jobs = ?DEFAULT_ASYNC_CHILDREN_MASTER_JOBS :: boolean(),
    % flag that informs whether job for processing next batch of given directory should be scheduled asynchronously
    async_next_batch_job = ?DEFAULT_ASYNC_NEXT_BATCH_JOB :: boolean(),
    % prehook executed before scheduling job for processing next batch of given directory
    next_batch_job_prehook = ?DEFAULT_NEXT_BATCH_JOB_PREHOOK :: storage_traverse:next_batch_job_prehook(),
    % prehook executed before scheduling job for processing children directory
    children_master_job_prehook = ?DEFAULT_CHILDREN_BATCH_JOB_PREHOOK :: storage_traverse:children_master_job_prehook(),
    % custom function that is called on each listed child
    compute_fun :: undefined | storage_traverse:compute(),
    % initial argument for compute function (see storage_traverse.erl for more info)
    compute_init :: term(),
    % allows to disable compute for specific batch, by default its enabled, but compute_fun must be defined
    compute_enabled = true :: boolean(),
    info :: storage_traverse:info()
}).

-record(storage_traverse_slave, {
    storage_file_ctx :: storage_file_ctx:ctx() | undefined,
    info :: storage_traverse:info()
}).

-define(ON_SUCCESSFUL_SLAVE_JOBS(Callback),
    fun(_MasterJobExtendedArgs, SlavesDescription) ->
        case maps:get(slave_jobs_failed, SlavesDescription) of
            0 -> Callback();
            _ -> ok
        end
    end
).

-endif.