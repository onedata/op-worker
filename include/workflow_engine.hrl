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

-define(DEFAULT_ASYNC_CALL_POOL_ID, <<"def_call_pol">>).

%%%===================================================================
%%% Record describing job to be executed
%%%===================================================================

-record(job_execution_spec, {
    handler :: workflow_handler:handler(),
    context :: workflow_engine:execution_context(),
    task_id :: workflow_engine:task_id(),
    task_spec :: workflow_engine:task_spec(),
    item_id :: workflow_cached_item:id(),
    job_identifier :: workflow_jobs:job_identifier()
}).

%%%===================================================================
%%% Macros describing possible execution status updates
%%%===================================================================

-define(SYNC_CALL, sync_call).
-define(ASYNC_CALL_STARTED, async_call_started).
-define(ASYNC_CALL_FINISHED, async_call_finished).

%%%===================================================================
%%% Macros used to describe processing of parallel box's jobs
%%%===================================================================

-define(NO_JOBS_LEFT_FOR_PARALLEL_BOX, no_jobs_left_for_parallel_box).
-define(AT_LEAST_ONE_JOB_LEFT_FOR_PARALLEL_BOX, at_least_one_job_left_for_parallel_box).

%%%===================================================================
%%% Macros used to describe actions and errors
%%%===================================================================

% Macros used to control workflow_engine actions
-define(END_EXECUTION_AND_NOTIFY(Handler, Context, LaneIndex),
    {end_execution_and_notify, Handler, Context, LaneIndex}).
-define(END_EXECUTION, end_execution).
-define(DEFER_EXECUTION, defer_execution).
-define(PREPARE_EXECUTION(Handler, ExecutionContext),
    {prepare_execution, Handler, ExecutionContext}).

% errors returned by workflow_engine_state to control workflow_engine
-define(WF_ERROR_ALL_SLOTS_USED, {error, all_slots_used}).
-define(WF_ERROR_ALREADY_REMOVED, {error, already_removed}).

% errors used by workflow_execution_state to control workflow_engine
-define(WF_ERROR_NO_WAITING_ITEMS, {error, no_waiting_items}).
-define(WF_ERROR_RACE_CONDITION, {error, race_condition}).
-define(WF_ERROR_UNKNOWN_JOB, {error, unknown_job}).
-define(WF_ERROR_ALREADY_FINISHED(Ans), {error, {already_finished, Ans}}).
-define(WF_ERROR_ITEM_PROCESSING_FINISHED(Item), {error, {item_processing_finished, Item}}).

% errors returned by workflow_async_call_pool to control workflow_engine
-define(WF_ERROR_LIMIT_REACHED, {error, limit_reached}).

-endif.
