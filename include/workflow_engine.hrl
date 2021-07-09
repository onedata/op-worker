%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros used in modules connected to workflow_engine
%%% @end
%%%-------------------------------------------------------------------
-ifndef(WORKFLOW_HRL).
-define(WORKFLOW_HRL, 1).

%%%===================================================================
%%% DEFAULTS
%%%===================================================================

-define(DEFAULT_ASYNC_CALL_POOL_ID, <<"def_call_pool">>).

%%%===================================================================
%%% Record describing execution on pool's process
%%% (processing of job or job's result)
%%%===================================================================

% TODO VFS-7919 - better name for record and subject_id field
-record(execution_spec, {
    handler :: workflow_handler:handler(),
    context :: workflow_engine:execution_context(),
    task_id :: workflow_engine:task_id(),
    task_spec :: workflow_engine:task_spec(),
    subject_id :: workflow_engine:subject_id(),
    job_identifier :: workflow_jobs:job_identifier()
}).

%%%===================================================================
%%% Macros describing possible execution status updates
%%%===================================================================

-define(SYNC_CALL, sync_call).
-define(ASYNC_CALL_STARTED, async_call_started).
-define(ASYNC_CALL_FINISHED, async_call_finished).
-define(ASYNC_RESULT_PROCESSED, async_result_processed).

%%%===================================================================
%%% Macros describing possible results of item processing
%%%===================================================================

-define(SUCCESS, success).
-define(FAILURE, failure).

%%%===================================================================
%%% Macros describing possible types of processing on pool
%%%===================================================================

% TODO VFS-7919 - better macros names
-define(JOB_PROCESSING, job_processing).
-define(ASYNC_RESULT_PROCESSING, async_result_processing).

%%%===================================================================
%%% Macros used to describe processing of parallel box's jobs
%%%===================================================================

-define(NO_JOBS_LEFT_FOR_PARALLEL_BOX, no_jobs_left_for_parallel_box).
-define(AT_LEAST_ONE_JOB_LEFT_FOR_PARALLEL_BOX, at_least_one_job_left_for_parallel_box).

%%%===================================================================
%%% Macros used to describe actions and errors
%%%===================================================================

% Macros used to control workflow_engine actions
-define(END_EXECUTION(Handler, Context, LaneIndex, ErrorEncountered),
    {end_execution, Handler, Context, LaneIndex, ErrorEncountered}).
-define(END_EXECUTION_AFTER_PREPARATION_ERROR(Handler, Context),
    {end_execution_after_preparation_error, Handler, Context}).
-define(DEFER_EXECUTION, defer_execution).
-define(PREPARE_EXECUTION(Handler, ExecutionContext),
    {prepare_execution, Handler, ExecutionContext}).

% errors returned by workflow_engine_state to control workflow_engine
-define(WF_ERROR_ALL_SLOTS_USED, {error, all_slots_used}).
-define(WF_ERROR_ALREADY_REMOVED, {error, already_removed}).

% errors used by workflow_execution_state to control workflow_engine
-define(WF_ERROR_PREPARATION_FAILED, {error, preparation_failed}).
-define(WF_ERROR_NO_WAITING_ITEMS, {error, no_waiting_items}).
-define(WF_ERROR_RACE_CONDITION, {error, race_condition}).
-define(WF_ERROR_ITEM_PROCESSING_FINISHED(Item, SuccessOrFailure),
    {error, {item_processing_finished, Item, SuccessOrFailure}}).

% errors returned by workflow_async_call_pool to control workflow_engine
-define(WF_ERROR_LIMIT_REACHED, {error, limit_reached}).

% errors returned by workflow_jobs to control jobs scheduling
-define(WF_ERROR_JOB_NOT_FOUND, {error, job_not_found}).

% errors connected with timeouts verification
-define(WF_ERROR_NO_TIMEOUTS_UPDATED, {error, no_timeouts_updated}).

% errors connected with result encoding/decoding/waiting
-define(WF_ERROR_MALFORMED_REQUEST, {error, malformed_request}).
-define(WF_ERROR_TIMEOUT, {error, timeout}).

-endif.
