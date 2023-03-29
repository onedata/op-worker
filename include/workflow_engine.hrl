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
    task_spec :: workflow_engine:task_spec() | undefined, % for streamed_task_data processing spec is not required
    subject_id :: workflow_engine:subject_id(),
    job_identifier :: workflow_jobs:job_identifier() | streamed_task_data
}).

%%%===================================================================
%%% Macros describing possible execution status updates
%%%===================================================================

-define(SYNC_CALL, sync_call).
-define(ASYNC_CALL_STARTED, async_call_started).
-define(ASYNC_CALL_ENDED, async_call_ended).
-define(ASYNC_RESULT_PROCESSED, async_result_processed).

%%%===================================================================
%%% Macros describing possible results of item processing
%%%===================================================================

-define(SUCCESS, success).
-define(FAILURE, failure).

%%%===================================================================
%%% Macros describing mode of snapshots creation
%%%===================================================================

-define(ALL_ITEMS, all_items).
-define(UNTIL_FIRST_FAILURE, until_first_failure).

%%%===================================================================
%%% Macros describing possible types of processing on pool
%%%===================================================================

% TODO VFS-7919 - better macros names
-define(JOB_PROCESSING, job_processing).
-define(ASYNC_RESULT_PROCESSING, async_result_processing).

%%%===================================================================
%%% Macros defining statuses of lane preparation and workflow execution
%%%===================================================================

-define(NOT_PREPARED, not_prepared).
-define(PREPARED, prepared).
-define(RESUMING(ResumeType, Iterator, ResumeData), {resuming, ResumeType, Iterator, ResumeData}).
-define(RESUMING_FROM_ITERATOR(Iterator), ?RESUMING(from_iterator, Iterator, undefined)).
-define(RESUMING_FROM_DUMP(Iterator, TaskIndexMap), ?RESUMING(from_dump, Iterator, TaskIndexMap)).
-define(PREPARING, preparing).
-define(PREPARED_IN_ADVANCE, prepared_in_advance).
-define(PREPARATION_FAILED, preparation_failed).

-define(EXECUTING, executing).
-define(EXECUTION_CANCELLED, execution_cancelled).
-define(EXECUTION_ENDED, execution_ended).
-define(EXECUTION_ENDED_WITH_EXCEPTION, execution_ended_with_exception).

-record(execution_cancelled, {
    execution_step = lane_execution :: lane_execution | lane_prepare | waiting_on_next_lane_prepare | finishing_execution,
    has_lane_preparation_failed = false :: boolean(),
    is_interrupted = false :: boolean(),
    % Calls count is incremented when cancel is initialized and decremented when it is
    % marked as finished. Workflow can be finished only when calls count is decremented to 0
    call_count :: non_neg_integer(),
    % Some callbacks cannot be executed when calls count is higher than 0.
    % They will be executed when calls count is decremented to 0.
    callbacks_to_execute = [] :: [workflow_execution_state:callback_to_exectute()],
    abrupt_stop_reason :: workflow_handler:abrupt_stop_reason() | undefined
}).

%%%===================================================================
%%% Macros used to describe processing of parallel box's jobs
%%%===================================================================

-define(NO_JOBS_LEFT_FOR_PARALLEL_BOX, no_jobs_left_for_parallel_box).
-define(AT_LEAST_ONE_JOB_LEFT_FOR_PARALLEL_BOX, at_least_one_job_left_for_parallel_box).

%%%===================================================================
%%% Macros describing possible results of
%%% workflow_handler:handle_lane_execution_stopped/3 callback
%%%===================================================================

-define(CONTINUE(NextLaneId, LaneIdToBePreparedInAdvance), {continue, NextLaneId, LaneIdToBePreparedInAdvance}).
-define(END_EXECUTION, end_execution).

%%%===================================================================
%%% Macros describing lane preparation modes
%%%===================================================================

-define(PREPARE_CURRENT, prepare_current).
-define(PREPARE_IN_ADVANCE, prepare_in_advance).

%%%===================================================================
%%% Macros and records used to describe actions and errors
%%%===================================================================

% Macros and records used to control workflow_engine actions
-record(execution_ended, {
    handler :: workflow_handler:handler(),
    context :: workflow_engine:execution_context(),
    final_callback = handle_workflow_execution_stopped :: handle_workflow_execution_stopped |
        {handle_workflow_abruptly_stopped, workflow_handler:abrupt_stop_reason()},
    lane_callbacks = false ::
        {true, workflow_engine:lane_id(), workflow_engine:execution_context(), [workflow_engine:task_id()]} | false
}).
-define(DEFER_EXECUTION, defer_execution).
-define(RETRY_EXECUTION, retry_execution).
-define(PREPARE_LANE_EXECUTION(Handler, ExecutionContext, LaneId, PreparationMode, InitType),
    {prepare_lane_execution, Handler, ExecutionContext, LaneId, PreparationMode, InitType}).

% errors returned by workflow_engine_state to control workflow_engine
-define(WF_ERROR_ALL_SLOTS_USED, {error, all_slots_used}).
-define(WF_ERROR_ALREADY_REMOVED, {error, already_removed}).

% errors used by workflow_execution_state to control workflow_engine
-define(WF_ERROR_PREPARATION_FAILED, {error, preparation_failed}).
-define(WF_ERROR_NO_WAITING_ITEMS, {error, no_waiting_items}).
-define(WF_ERROR_RACE_CONDITION, {error, race_condition}).
-define(WF_ERROR_ITEM_PROCESSING_ENDED(Item, SuccessOrFailure),
    {error, {item_processing_ended, Item, SuccessOrFailure}}).
-define(WF_ERROR_CANCEL_NOT_INITIALIZED, {error, cancel_not_initialized}).
-define(WF_ERROR_WORKFLOW_INTERRUPTED(EngineId), {error, {workflow_interrupted, EngineId}}).
-define(WF_ERROR_PRED_NOT_MEET, {error, pred_not_meet}).
-define(WF_ERROR_WRONG_EXECUTION_STATUS, {error, wrong_execution_status}).

% errors returned by workflow_async_call_pool to control workflow_engine
-define(WF_ERROR_LIMIT_REACHED, {error, limit_reached}).

% errors returned by workflow_jobs to control jobs scheduling
-define(WF_ERROR_JOB_NOT_FOUND, {error, job_not_found}).
-define(WF_ERROR_ITERATION_FINISHED, {error, iteration_finished}).

% errors connected with timeouts verification
-define(WF_ERROR_NO_TIMEOUTS_UPDATED, {error, no_timeouts_updated}).

-endif.
