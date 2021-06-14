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

-define(DEFAULT_ASYNC_CALL_POOL_ID, <<"DEFAULT_ASYNC_CALL_POOL_ID">>).

%%%===================================================================
%%% Macros used to return task processing results
%%%===================================================================

-define(PROCESSING_FINISHED, finished).

%%%===================================================================
%%% Macros used to describe processing of parallel box's jobs
%%%===================================================================

-define(NO_JOBS_LEFT_FOR_PARALLEL_BOX, no_jobs_left_for_parallel_box).
-define(AT_LEAST_ONE_JOB_LEFT_FOR_PARALLEL_BOX, at_least_one_job_left_for_parallel_box).

%%%===================================================================
%%% Macros used to describe actions and errors
%%%===================================================================

% Macros used to control workflow_engine actions
-define(END_EXECUTION, end_execution).
-define(DEFER_EXECUTION, defer_execution).

% errors returned by workflow_engine_state to control workflow_engine
-define(WF_ERROR_ALL_SLOTS_USED, {error, all_slots_used}).

% errors used by workflow_execution_state to control workflow_engine
-define(WF_ERROR_ONGOING_ITEMS_ONLY, {error, ongoing_items_only}).
-define(WF_ERROR_RACE_CONDITION, {error, race_condition}).
-define(WF_ERROR_UNKNOWN_REFERENCE, {error, unknown_reference}).
-define(WF_ERROR_ALREADY_FINISHED(Ans), {error, {already_finished, Ans}}).
-define(WF_ERROR_ITEM_PROCESSING_FINISHED(Item), {error, {item_processing_finished, Item}}).

% errors returned by workflow_async_call_pool to control workflow_engine
-define(WF_ERROR_LIMIT_REACHED, {error, limit_reached}).

-endif.
