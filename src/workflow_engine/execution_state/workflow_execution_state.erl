%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for management of jobs connected with single workflow execution.
%%% It is used by processes of workflow_engine
%%% that synchronize on model's documents update dividing between
%%% themselves jobs to execute.
%%% TODO VFS-7551 - check if module responsible for workflows' lists checks lists' consistency on cluster restart
%%% (item can be in more than one list after node crash)
%%% TODO VFS-7551 - lists of workflow should be per space
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_execution_state).
-author("Michal Wrzeszcz").

-include("workflow_engine.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([init/2, prepare_next_job/1, report_execution_finish/4, report_limit_reached_error/2,
    check_timeouts/1, reset_keepalive_timer/2]).

% Helper record to group fields containing information about lane currently being executed
-record(current_lane, {
    lane_index :: index(),
    parallel_boxes_count = 0 :: non_neg_integer(),
    parallel_boxes_spec :: workflow_definition:boxes_map()
}).

% Records used to provide additional information about document update procedure 
% (see #workflow_execution_state.update_report)
-record(task_prepared_report, {
    job_identifier :: workflow_jobs:job_identifier(),
    task_id :: task_executor:task_id(),
    item_id :: workflow_cached_item:id() | undefined
}).
-record(items_processed_report, {
    lane_index :: index(),
    last_finished_item_index :: index(),
    finished_iterator :: workflow_store:iterator()
}).

-type index() :: non_neg_integer(). % scheduling is based on positions of elements (items, parallel_boxes, tasks)
                                    % to allow executions of tasks in chosen order
-type state() :: #workflow_execution_state{}.
-type current_lane() :: #current_lane{}.

% Definitions of possible errors
-define(WF_ERROR_LANE_ALREADY_PREPARED, {error, lane_already_prepared}).
-define(WF_ERROR_LANE_CHANGED, {error, lane_changed}).
-define(WF_ERROR_EXECUTION_FINISHED, {error, execution_finished}).
-define(WF_ERROR_LANE_FINISHED(LaneIndex), {error, {lane_finished, LaneIndex}}).
-define(WF_ERROR_NO_CACHED_ITEMS(LaneIndex, IterationStep), {error, {no_cached_items, LaneIndex, IterationStep}}).

-type update_fun() :: datastore_doc:diff(state()).
-type no_items_error() :: ?WF_ERROR_ONGOING_ITEMS_ONLY | ?WF_ERROR_EXECUTION_FINISHED | ?WF_ERROR_LANE_FINISHED(index()).
% Type used to return additional information about document update procedure
% (see #workflow_execution_state.update_report)
-type update_report() :: #task_prepared_report{} | #items_processed_report{} | no_items_error().

-export_type([index/0, current_lane/0, update_report/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(workflow_engine:execution_id(), non_neg_integer()) -> ok.
init(ExecutionId, LaneCount) ->
    % TODO VFS-7551 - destroy when execution is ended - should we keep something for restarts?
    LaneSpec = workflow_definition:get_lane(ExecutionId, 1),
    {ok, Record} = prepare_lane(#workflow_execution_state{lane_count = LaneCount}, 1, LaneSpec),
    Doc = #document{key = ExecutionId, value = Record},
    {ok, _} = datastore_model:create(?CTX, Doc),
    ok.

-spec prepare_next_job(workflow_engine:execution_id()) ->
    {ok, task_executor:task_id(), workflow_cached_item:id(), workflow_jobs:job_identifier()} |
    ?END_EXECUTION | ?DEFER_EXECUTION.
prepare_next_job(ExecutionId) ->
    % TODO VFS-7551 - check quota for async jobs and do nothing if it is exceeded
    case prepare_next_job_for_current_lane(ExecutionId) of
        {ok, _, _, _} = OkAns ->
            OkAns;
        ?WF_ERROR_EXECUTION_FINISHED ->
            ?END_EXECUTION;
        ?WF_ERROR_LANE_FINISHED(LaneIndex) ->
            prepare_lane(ExecutionId, LaneIndex + 1),
            prepare_next_job(ExecutionId);
        ?WF_ERROR_ONGOING_ITEMS_ONLY ->
            ?DEFER_EXECUTION
    end.

-spec report_execution_finish(
    workflow_engine:execution_id(),
    task_executor:task_id(),
    workflow_jobs:job_identifier() | task_executor:async_ref(),
    task_executor:result()
) -> ok.
report_execution_finish(ExecutionId, Task, RefOrJobIdentifier, Ans) ->
    case task_executor:get_calls_counter_id(ExecutionId, Task) of
        undefined -> ok;
        CallsCounterId -> workflow_async_call_pool:decrement_slot_usage(CallsCounterId)
    end,

    case update(ExecutionId, fun(State) ->
        report_execution_finish(State, RefOrJobIdentifier, Ans)
    end) of
        {ok, #document{value = #workflow_execution_state{update_report = #items_processed_report{lane_index = LaneIndex,
            last_finished_item_index = ItemIndex, finished_iterator = IteratorToSave}}}} ->
            workflow_iterator_snapshot:save(ExecutionId, LaneIndex, ItemIndex, IteratorToSave);
        {ok, _} ->
            ok
    end.

-spec report_limit_reached_error(workflow_engine:execution_id(), workflow_jobs:job_identifier()) -> ok.
report_limit_reached_error(ExecutionId, JobIdentifier) ->
    {ok, _} = update(ExecutionId, fun(State) -> pause_job(State, JobIdentifier) end),
    ok.

-spec check_timeouts(workflow_engine:execution_id()) -> ok.
check_timeouts(ExecutionId) ->
    case datastore_model:get(?CTX, ExecutionId) of
        {ok, #document{value = #workflow_execution_state{jobs = Jobs}}} ->
            case workflow_jobs:check_timeouts(Jobs) of
                {ok, NewJobs} -> update(ExecutionId, fun(State) -> update_jobs(State, NewJobs) end);
                ?ERROR_NOT_FOUND -> ok
            end;
        ?ERROR_NOT_FOUND ->
            ok
    end.

-spec reset_keepalive_timer(workflow_engine:execution_id(), task_executor:async_ref()) -> ok.
reset_keepalive_timer(ExecutionId, Ref) ->
    {ok, _} = update(ExecutionId, fun(State) ->
        reset_keepalive_timer_internal(State, Ref)
    end),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec prepare_lane(workflow_engine:execution_id(), index()) -> ok.
prepare_lane(ExecutionId, LaneIndex) ->
    LaneSpec = workflow_definition:get_lane(ExecutionId, LaneIndex),
    case update(ExecutionId, fun(State) ->
        prepare_lane(State, LaneIndex, LaneSpec)
    end) of
        {ok, _} -> ok;
        ?WF_ERROR_LANE_ALREADY_PREPARED -> ok
    end.

-spec prepare_next_job_for_current_lane(workflow_engine:execution_id()) ->
    {ok, task_executor:task_id(), workflow_cached_item:id(), workflow_jobs:job_identifier()} | no_items_error().
prepare_next_job_for_current_lane(ExecutionId) ->
    case update(ExecutionId, fun prepare_next_waiting_job/1) of
        {ok, #document{value = #workflow_execution_state{update_report = #task_prepared_report{
            job_identifier = JobIdentifier, task_id = TaskId, item_id = ItemId}}}} ->
            {ok, TaskId, ItemId, JobIdentifier};
        ?WF_ERROR_NO_CACHED_ITEMS(LaneIndex, IterationStep) ->
            prepare_next_job_using_iterator(ExecutionId, IterationStep, LaneIndex);
        {error, _} = Error ->
            Error
    end.

-spec prepare_next_job_using_iterator(workflow_engine:execution_id(), workflow_iteration_state:iteration_step(), index()) ->
    {ok, task_executor:task_id(), workflow_cached_item:id(), workflow_jobs:job_identifier()} | no_items_error().
prepare_next_job_using_iterator(ExecutionId, CurrentIterationStep, LaneIndex) ->
    case workflow_store:get_next(workflow_iteration_state:get_iterator(CurrentIterationStep)) of
        {ok, NextItem, ParallelBoxToStart, NextIterator} ->
            NextItemId = workflow_cached_item:put(NextItem), % TODO VFS-7551 return (to engine) item instead of item_id in this case (engine must read from cache when we have item here)
            case update(ExecutionId, fun(State) ->
                register_and_prepare_new_item(
                    State, NextItemId, ParallelBoxToStart, CurrentIterationStep, NextIterator)
            end) of
                {ok, #document{value = #workflow_execution_state{update_report = #task_prepared_report{
                    job_identifier = JobIdentifier, task_id = TaskId}}}} ->
                    {ok, TaskId, NextItemId, JobIdentifier};
                ?WF_ERROR_RACE_CONDITION ->
                    prepare_next_job_for_current_lane(ExecutionId)
            end;
        none ->
            case update(ExecutionId, fun(State) -> handle_iteration_finished(State, LaneIndex) end) of
                {ok, #document{value = #workflow_execution_state{update_report = #task_prepared_report{
                    job_identifier = JobIdentifier, task_id = TaskId, item_id = ItemId}}}} ->
                    {ok, TaskId, ItemId, JobIdentifier}; % Some task has ended in parallel
                                                         % and generated new tasks
                {ok, #document{value = #workflow_execution_state{update_report = {error, _} = UpdateReport}}} ->
                    UpdateReport; % Nothing to prepare
                ?WF_ERROR_LANE_CHANGED ->
                    prepare_next_job_for_current_lane(ExecutionId)
            end
    end.

-spec update(workflow_engine:execution_id(), update_fun()) -> {ok, state()} | {error, term()}.
update(ExecutionId, UpdateFun) ->
    % TODO VFS-7551 - should we add try/catch or allow functions fail?
    datastore_model:update(?CTX, ExecutionId, fun(State) ->
        UpdateFun(State#workflow_execution_state{update_report = undefined})
    end).

%%%===================================================================
%%% Functions updating record
%%%===================================================================

-spec prepare_lane(state(), index(), workflow_definition:lane_spec()) ->
    {ok, state()} | ?WF_ERROR_LANE_ALREADY_PREPARED.
prepare_lane(#workflow_execution_state{current_lane = #current_lane{lane_index = LaneIndex}},
    LaneIndex, _LaneSpec) ->
    ?WF_ERROR_LANE_ALREADY_PREPARED;
prepare_lane(State, LaneIndex, LaneSpec) ->
    ParallelBoxes = maps:get(parallel_boxes, LaneSpec),
    {ok, State#workflow_execution_state{
        current_lane = #current_lane{
            lane_index = LaneIndex,
            parallel_boxes_count = maps:size(ParallelBoxes),
            parallel_boxes_spec = ParallelBoxes
        },
        iteration_state = workflow_iteration_state:init(maps:get(iterator, LaneSpec)),
        jobs = workflow_jobs:init()
    }}.

-spec register_and_prepare_new_item(
    state(),
    workflow_cached_item:id(),
    index(),
    workflow_iteration_state:iteration_step(),
    workflow_store:iterator()
) ->
    {ok, state()} | ?WF_ERROR_RACE_CONDITION.
register_and_prepare_new_item(State = #workflow_execution_state{
    jobs = Jobs,
    iteration_state = IterationState,
    current_lane = #current_lane{parallel_boxes_spec = BoxesSpec}
}, NewItemId, ParallelBoxToStart, PrevIterationStep, NewIterator) ->
    % TODO VFS-7551 - it may be needed to allow registration of waiting items as a result of async call processing finish
    case workflow_iteration_state:register_new_step(IterationState, PrevIterationStep, NewItemId, NewIterator) of
        ?WF_ERROR_RACE_CONDITION = Error ->
            Error;
        {NewItemIndex, NewIterationState} ->
            {NewJobs, ToStart} = workflow_jobs:populate_with_jobs_for_item(
                Jobs, NewItemIndex, ParallelBoxToStart, BoxesSpec),
            {ok, State#workflow_execution_state{
                update_report = #task_prepared_report{job_identifier = ToStart,
                    task_id = workflow_jobs:get_task_id(ToStart, BoxesSpec)},
                iteration_state = NewIterationState,
                jobs = NewJobs
            }}
    end.

-spec pause_job(state(), workflow_jobs:job_identifier()) -> {ok, state()}.
pause_job(State = #workflow_execution_state{jobs = Jobs}, JobIdentifier) ->
    {ok, State#workflow_execution_state{
        jobs = workflow_jobs:pause_job(Jobs, JobIdentifier)
    }}.

-spec update_jobs(state(), workflow_jobs:jobs()) -> {ok, state()}.
update_jobs(State, Jobs) ->
    {ok, State#workflow_execution_state{
        jobs = Jobs
    }}.

-spec handle_iteration_finished(state(), index()) -> {ok, state()} | ?WF_ERROR_LANE_CHANGED.
handle_iteration_finished(State = #workflow_execution_state{
    current_lane = #current_lane{lane_index = LaneIndex},
    iteration_state = IterationState
}, LaneIndex) ->
    State2 = State#workflow_execution_state{
        iteration_state = workflow_iteration_state:handle_iteration_finished(IterationState)},
    case prepare_next_waiting_job(State2) of
        {ok, _} = OkAns -> OkAns;
        Error -> {ok, State2#workflow_execution_state{update_report = Error}}
    end;
handle_iteration_finished(_State, _LaneIndex) ->
    ?WF_ERROR_LANE_CHANGED.

-spec report_execution_finish(
    state(), 
    workflow_jobs:job_identifier() | task_executor:async_ref(), 
    task_executor:result()
) -> {ok, state()}.
report_execution_finish(State = #workflow_execution_state{
    jobs = Jobs
}, RefOrJobIdentifier, Ans) ->
    case workflow_jobs:ensure_job_identifier_or_cache_ans(Jobs, RefOrJobIdentifier, Ans) of
        {?WF_ERROR_UNKNOWN_REFERENCE, NewJobs} ->
            {ok, State#workflow_execution_state{jobs = NewJobs}};
        {{ok, JobIdentifier}, NewJobs} ->
            report_job_execution_finish(State#workflow_execution_state{jobs = NewJobs}, JobIdentifier, Ans)
    end.

-spec prepare_next_waiting_job(state()) ->
    {ok, state()} | no_items_error() | ?WF_ERROR_NO_CACHED_ITEMS(index(), workflow_iteration_state:iteration_step()).
prepare_next_waiting_job(State = #workflow_execution_state{
    jobs = Jobs,
    iteration_state = IterationState,
    current_lane = #current_lane{lane_index = LaneIndex, parallel_boxes_spec = BoxesSpec}
}) ->
    case workflow_jobs:prepare_next_waiting_job(Jobs) of
        {ok, JobIdentifier, NewJobs} ->
            TaskId = workflow_jobs:get_task_id(JobIdentifier, BoxesSpec),
            ItemId = workflow_jobs:get_item_id(JobIdentifier, IterationState),
            {ok, State#workflow_execution_state{
                update_report = #task_prepared_report{job_identifier = JobIdentifier, task_id = TaskId, item_id = ItemId},
                jobs = NewJobs
            }};
        Error ->
            case workflow_iteration_state:get_last_registered_step(IterationState) of
                undefined -> % iteration has finished
                    handle_no_waiting_items_error(State, Error);
                IterationStep ->
                    ?WF_ERROR_NO_CACHED_ITEMS(LaneIndex, IterationStep)
            end
    end.

-spec prepare_next_parallel_box(state(), workflow_jobs:job_identifier()) -> {ok, state()}.
prepare_next_parallel_box(State = #workflow_execution_state{
    current_lane = #current_lane{
        parallel_boxes_count = BoxCount,
        parallel_boxes_spec = BoxesSpec,
        lane_index = LaneIndex
    },
    jobs = Jobs,
    iteration_state = IterationState
}, JobIdentifier) ->
    case workflow_jobs:prepare_next_parallel_box(Jobs, JobIdentifier, BoxesSpec, BoxCount) of
        {ok, NewJobs} ->
            {ok, State#workflow_execution_state{jobs = NewJobs}};
        ?WF_ERROR_ITEM_PROCESSING_FINISHED(ItemIndex) ->
            case workflow_iteration_state:handle_step_finish(IterationState, ItemIndex) of
                {NewIterationState, undefined} ->
                    {ok, State#workflow_execution_state{iteration_state = NewIterationState}};
                {NewIterationState, IteratorToSave} ->
                    {ok, State#workflow_execution_state{
                        iteration_state = NewIterationState,
                        update_report = #items_processed_report{lane_index = LaneIndex,
                            last_finished_item_index = ItemIndex, finished_iterator = IteratorToSave}
                    }}
            end
    end.

-spec reset_keepalive_timer_internal(state(), task_executor:async_ref()) -> {ok, state()}.
reset_keepalive_timer_internal(State = #workflow_execution_state{
    jobs = Jobs
}, Ref) ->
    {ok, State#workflow_execution_state{
        jobs = workflow_jobs:reset_keepalive_timer(Jobs, Ref)
    }}.

-spec report_job_execution_finish(state(), workflow_jobs:job_identifier(), task_executor:result()) -> {ok, state()}.
report_job_execution_finish(State = #workflow_execution_state{
    jobs = Jobs
}, JobIdentifier, Ans) ->
    case Ans of
        ok ->
            % TODO - should we protect against unknown keys? can they appear
            {NewJobs2, RemainingForBox} = workflow_jobs:mark_ongoing_job_finished(Jobs, JobIdentifier),
            % TODO VFS-7551 - Delete unused iterators and save information used for restarts
            case RemainingForBox of
                ?AT_LEAST_ONE_JOB_LEFT_FOR_PARALLEL_BOX ->
                    {ok, State#workflow_execution_state{jobs = NewJobs2}};
                ?NO_JOBS_LEFT_FOR_PARALLEL_BOX ->
                    prepare_next_parallel_box(State#workflow_execution_state{jobs = NewJobs2}, JobIdentifier)
            end;
        error ->
            FinalJobs = workflow_jobs:register_failure(Jobs, JobIdentifier),
            {ok, State#workflow_execution_state{jobs = FinalJobs}};
        {async, TaskId, Ref, KeepaliveTimeout} ->
            case workflow_jobs:register_async_call(Jobs, JobIdentifier, TaskId, Ref, KeepaliveTimeout) of
                {ok, FinalJobs} ->
                    {ok, State#workflow_execution_state{jobs = FinalJobs}};
                {?WF_ERROR_ALREADY_FINISHED(FinalAns), FinalJobs} ->
                    report_execution_finish(State#workflow_execution_state{jobs = FinalJobs}, JobIdentifier, FinalAns)
            end
    end.

-spec handle_no_waiting_items_error(state(), ?WF_ERROR_ONGOING_ITEMS_ONLY | ?ERROR_NOT_FOUND) -> no_items_error().
handle_no_waiting_items_error(#workflow_execution_state{
    current_lane = #current_lane{lane_index = LaneIndex},
    lane_count = LaneCount
}, Error) ->
    case {Error, LaneIndex =:= LaneCount} of
        {?WF_ERROR_ONGOING_ITEMS_ONLY, _} -> ?WF_ERROR_ONGOING_ITEMS_ONLY;
        {?ERROR_NOT_FOUND, true} -> ?WF_ERROR_EXECUTION_FINISHED;
        {?ERROR_NOT_FOUND, false} -> ?WF_ERROR_LANE_FINISHED(LaneIndex)
    end.