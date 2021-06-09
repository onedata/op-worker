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
-export([init/3, init_using_snapshot/3, prepare_next_job/1,
    report_execution_status_update/4, report_execution_prepared/4, report_limit_reached_error/2,
    check_timeouts/1, reset_keepalive_timer/2, get_result_processing_data/2]).
%% Test API
-export([get_lane_index/1]).

% Helper record to group fields containing information about lane currently being executed
-record(current_lane, {
    lane_index :: index(),
    is_last :: boolean(),
    parallel_boxes_count = 0 :: non_neg_integer(),
    parallel_boxes_spec :: boxes_map()
}).

% Macros and records used to provide additional information about document update procedure
% (see #workflow_execution_state.update_report)
-define(LANE_SET_TO_BE_PREPARED, lane_set_to_be_prepared).
-record(job_prepared_report, {
    job_identifier :: workflow_jobs:job_identifier(),
    task_id :: workflow_engine:task_id(),
    task_spec :: workflow_engine:task_spec(),
    item_id :: workflow_cached_item:id() | undefined
}).
-record(items_processed_report, {
    lane_index :: index(),
    last_finished_item_index :: index(),
    finished_iterator :: iterator:iterator()
}).

-type index() :: non_neg_integer(). % scheduling is based on positions of elements (items, parallel_boxes, tasks)
                                    % to allow executions of tasks in chosen order
-type state() :: #workflow_execution_state{}.

% Macros and types connected with caching of lane definition using current_lane field
-define(LANE_NOT_PREPARED, not_prepared).
-define(PREPARING_LANE, preparing).
-define(LANE_PREPARATION_FAILED, preparation_failed).
-type current_lane() :: #current_lane{}.
-type lane_preparation_status() :: ?LANE_NOT_PREPARED | ?PREPARING_LANE | ?LANE_PREPARATION_FAILED.

%% @formatter:off
-type boxes_map() :: #{
    BoxIndex :: index() => #{
        TaskIndex :: index() => {workflow_engine:task_id(), workflow_engine:task_spec()}
    }
}.
%% @formatter:on

% Definitions of possible errors
-define(WF_ERROR_LANE_ALREADY_PREPARED, {error, lane_already_prepared}).
-define(WF_ERROR_LANE_CHANGED, {error, lane_changed}).
-define(WF_ERROR_EXECUTION_FINISHED(Handler, Context, LaneIndex),
    {error, {execution_finished, Handler, Context, LaneIndex}}).
-define(WF_ERROR_LANE_FINISHED(LaneIndex, Handler, Context), {error, {lane_finished, LaneIndex, Handler, Context}}).
-define(WF_ERROR_NO_CACHED_ITEMS(LaneIndex, IterationStep, Context),
    {error, {no_cached_items, LaneIndex, IterationStep, Context}}).
-define(WF_ERROR_EXECUTION_PREPARATION_FAILED, {error, execution_preparation_failed}).

-type update_fun() :: datastore_doc:diff(state()).
-type no_items_error() :: ?WF_ERROR_NO_WAITING_ITEMS |
    ?WF_ERROR_EXECUTION_FINISHED(workflow_handler:handler(), workflow_engine:execution_context(), index()) |
    ?WF_ERROR_LANE_FINISHED(index(), workflow_handler:handler(), workflow_engine:execution_context()).
% Type used to return additional information about document update procedure
% (see #workflow_execution_state.update_report)
-type update_report() :: ?LANE_SET_TO_BE_PREPARED | #job_prepared_report{} | #items_processed_report{} |
    no_items_error().

-export_type([index/0, current_lane/0, lane_preparation_status/0, boxes_map/0, update_report/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(workflow_engine:execution_id(), workflow_handler:handler(), workflow_engine:execution_context()) -> ok.
init(ExecutionId, Handler, Context) ->
    % TODO VFS-7551 - destroy when execution is ended
    Doc = #document{key = ExecutionId, value = #workflow_execution_state{handler = Handler, context = Context}},
    {ok, _} = datastore_model:save(?CTX, Doc),
    ok.

-spec init_using_snapshot(
    workflow_engine:execution_id(), workflow_handler:handler(), workflow_engine:execution_context()) -> ok.
init_using_snapshot(ExecutionId, Handler, Context) ->
    case workflow_iterator_snapshot:get(ExecutionId) of
        {ok, LaneIndex, Iterator} ->
            {_InitialIterator, CurrentLaneSpec} =
                get_initial_iterator_and_lane_spec(ExecutionId, Handler, Context, LaneIndex),
            {ok, Record} = prepare_lane(
                #workflow_execution_state{handler = Handler, context = Context}, CurrentLaneSpec, Iterator),
            Doc = #document{key = ExecutionId, value = Record},
            {ok, _} = datastore_model:save(?CTX, Doc),
            ok;
        ?ERROR_NOT_FOUND ->
            init(ExecutionId, Handler, Context)
    end.

-spec prepare_next_job(workflow_engine:execution_id()) ->
    {ok, workflow_engine:job_execution_spec()} |
    ?END_EXECUTION_AND_NOTIFY(workflow_handler:handler(), workflow_engine:execution_context(), index()) |
    ?PREPARE_EXECUTION(workflow_handler:handler(), workflow_engine:execution_context()) | ?DEFER_EXECUTION |
    ?END_EXECUTION.
prepare_next_job(ExecutionId) ->
    % TODO VFS-7551 - check quota for async jobs and do nothing if it is exceeded
    case prepare_next_job_for_current_lane(ExecutionId) of
        {ok, _} = OkAns ->
            OkAns;
        ?WF_ERROR_LANE_FINISHED(LaneIndex, Handler, ExecutionContext) ->
            prepare_lane(ExecutionId, Handler, ExecutionContext, LaneIndex + 1),
            prepare_next_job(ExecutionId);
        ?WF_ERROR_NO_WAITING_ITEMS ->
            ?DEFER_EXECUTION;
        ?PREPARE_EXECUTION(Handler, ExecutionContext) ->
            ?PREPARE_EXECUTION(Handler, ExecutionContext);
        ?WF_ERROR_EXECUTION_FINISHED(Handler, Context, LaneIndex) ->
            ?END_EXECUTION_AND_NOTIFY(Handler, Context, LaneIndex);
        ?WF_ERROR_EXECUTION_PREPARATION_FAILED ->
            ?END_EXECUTION
    end.

-spec report_execution_status_update(
    workflow_engine:execution_id(),
    workflow_jobs:job_identifier(),
    workflow_engine:execution_status_update_type(),
    workflow_engine:processing_result()
) -> ok.
report_execution_status_update(ExecutionId, JobIdentifier, UpdateType, Ans) ->
    case update(ExecutionId, fun(State) ->
        report_execution_status_update_internal(State, JobIdentifier, UpdateType, Ans)
    end) of
        {ok, #document{value = #workflow_execution_state{update_report = #items_processed_report{lane_index = LaneIndex,
            last_finished_item_index = ItemIndex, finished_iterator = IteratorToSave}}}} ->
            workflow_iterator_snapshot:save(ExecutionId, LaneIndex, ItemIndex, IteratorToSave);
        {ok, _} ->
            ok
    end.

-spec report_execution_prepared(
    workflow_engine:execution_id(),
    workflow_handler:handler(),
    workflow_engine:execution_context(),
    workflow_handler:callback_execution_result()
) -> ok.
report_execution_prepared(ExecutionId, Handler, ExecutionContext, ok) ->
    {ok, IteratorToSave} = prepare_lane(ExecutionId, Handler, ExecutionContext, 1),
    workflow_iterator_snapshot:save(ExecutionId, 0, 0, IteratorToSave);
report_execution_prepared(ExecutionId, _Handler, _ExecutionContext, error) ->
    {ok, _} = update(ExecutionId, fun(State) ->
        handle_preparation_failure(State)
    end),
    ok.

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

-spec reset_keepalive_timer(workflow_engine:execution_id(), workflow_jobs:job_identifier()) -> ok.
reset_keepalive_timer(ExecutionId, JobIdentifier) ->
    {ok, _} = update(ExecutionId, fun(State) ->
        reset_keepalive_timer_internal(State, JobIdentifier)
    end),
    ok.

-spec get_result_processing_data(workflow_engine:execution_id(), workflow_jobs:job_identifier()) ->
    {workflow_handler:handler(), workflow_engine:execution_context(), workflow_engine:task_id()}.
get_result_processing_data(ExecutionId, JobIdentifier) ->
    {ok, #document{value = #workflow_execution_state{
        handler = Handler,
        context = Context,
        current_lane = #current_lane{parallel_boxes_spec = BoxesSpec}
    }}} = datastore_model:get(?CTX, ExecutionId),
    {TaskId, _TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxesSpec),
    {Handler, Context, TaskId}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_initial_iterator_and_lane_spec(
    workflow_engine:execution_id(),
    workflow_handler:handler(),
    workflow_engine:execution_context(),
    index()
) -> {iterator:iterator(), current_lane()}.
get_initial_iterator_and_lane_spec(ExecutionId, Handler, Context, LaneIndex) ->
    {ok, #{
        parallel_boxes := Boxes,
        iterator := Iterator,
        is_last := IsLast
    }} = Handler:get_lane_spec(ExecutionId, Context, LaneIndex),

    {_, BoxesMap} = lists:foldl(fun(BoxSpec, {BoxIndex, BoxesAcc}) ->
        {_, Tasks} = maps:fold(fun(TaskId, TaskSpec, {TaskIndex, TaskAcc}) ->
            {TaskIndex + 1, TaskAcc#{TaskIndex => {TaskId, TaskSpec}}}
        end, {1, #{}}, BoxSpec),
        {BoxIndex + 1, BoxesAcc#{BoxIndex => Tasks}}
    end, {1, #{}}, Boxes),

    {
        Iterator,
        #current_lane{
            lane_index = LaneIndex,
            is_last = IsLast,
            parallel_boxes_count = length(Boxes),
            parallel_boxes_spec = BoxesMap
        }
    }.

-spec prepare_lane(
    workflow_engine:execution_id(),
    workflow_handler:handler(),
    workflow_engine:execution_context(),
    index()
) -> {ok, iterator:iterator()}.
prepare_lane(ExecutionId, Handler, Context, LaneIndex) ->
    {Iterator, CurrentLaneSpec} =
        get_initial_iterator_and_lane_spec(ExecutionId, Handler, Context, LaneIndex),
    case update(ExecutionId, fun(State) ->
        prepare_lane(State, CurrentLaneSpec, Iterator)
    end) of
        {ok, _} ->
            case LaneIndex > 1 of
                true -> Handler:handle_lane_execution_ended(ExecutionId, Context, LaneIndex - 1);
                false -> ok
            end,
            {ok, Iterator};
        ?WF_ERROR_LANE_ALREADY_PREPARED ->
            {ok, Iterator}
    end.

-spec prepare_next_job_for_current_lane(workflow_engine:execution_id()) ->
    {ok, workflow_engine:job_execution_spec()} |
    ?PREPARE_EXECUTION(workflow_handler:handler(), workflow_engine:execution_context()) |
    no_items_error() | ?WF_ERROR_EXECUTION_PREPARATION_FAILED.
prepare_next_job_for_current_lane(ExecutionId) ->
    case update(ExecutionId, fun prepare_next_waiting_job/1) of
        {ok, #document{value = #workflow_execution_state{update_report = ?LANE_SET_TO_BE_PREPARED,
            handler = Handler, context = ExecutionContext}}} ->
            ?PREPARE_EXECUTION(Handler, ExecutionContext);
        {ok, #document{value = #workflow_execution_state{update_report = #job_prepared_report{
            job_identifier = JobIdentifier, task_id = TaskId, task_spec = TaskSpec, item_id = ItemId},
            handler = Handler, context = ExecutionContext}}} ->
            {ok, #job_execution_spec{
                handler = Handler,
                context = ExecutionContext,
                task_id = TaskId,
                task_spec = TaskSpec,
                item_it = ItemId,
                job_identifier = JobIdentifier
            }};
        ?WF_ERROR_NO_CACHED_ITEMS(LaneIndex, IterationStep, Context) ->
            prepare_next_job_using_iterator(ExecutionId, IterationStep, LaneIndex, Context);
        {error, _} = Error ->
            Error
    end.

-spec prepare_next_job_using_iterator(workflow_engine:execution_id(), workflow_iteration_state:iteration_step(),
    index(), workflow_engine:execution_context()) -> {ok, workflow_engine:job_execution_spec()} | no_items_error().
prepare_next_job_using_iterator(ExecutionId, CurrentIterationStep, LaneIndex, Context) ->
    case iterator:get_next(Context, workflow_iteration_state:get_iterator(CurrentIterationStep)) of
        {ok, NextItem, NextIterator} ->
            ParallelBoxToStart = 1, % TODO VFS-7551 - get ParallelBoxToStart from iterator
            NextItemId = workflow_cached_item:put(NextItem), % TODO VFS-7551 return (to engine) item instead of item_id in this case (engine must read from cache when we have item here)
            case update(ExecutionId, fun(State) ->
                register_and_prepare_new_item(
                    State, NextItemId, ParallelBoxToStart, CurrentIterationStep, NextIterator)
            end) of
                {ok, #document{value = #workflow_execution_state{update_report = #job_prepared_report{
                    job_identifier = JobIdentifier, task_id = TaskId, task_spec = TaskSpec},
                    handler = Handler, context = ExecutionContext}}} ->
                    {ok, #job_execution_spec{
                        handler = Handler,
                        context = ExecutionContext,
                        task_id = TaskId,
                        task_spec = TaskSpec,
                        item_it = NextItemId,
                        job_identifier = JobIdentifier
                    }};
                ?WF_ERROR_RACE_CONDITION ->
                    prepare_next_job_for_current_lane(ExecutionId)
            end;
        stop ->
            case update(ExecutionId, fun(State) -> handle_iteration_finished(State, LaneIndex) end) of
                {ok, #document{value = #workflow_execution_state{update_report = #job_prepared_report{
                    job_identifier = JobIdentifier, task_id = TaskId, task_spec = TaskSpec, item_id = ItemId},
                    handler = Handler, context = ExecutionContext}}} ->
                    % Some task has ended in parallel and generated new tasks
                    {ok, #job_execution_spec{
                        handler = Handler,
                        context = ExecutionContext,
                        task_id = TaskId,
                        task_spec = TaskSpec,
                        item_it = ItemId,
                        job_identifier = JobIdentifier
                    }};
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

-spec handle_preparation_failure(state()) -> {ok, state()}.
handle_preparation_failure(State) ->
    {ok, State#workflow_execution_state{
        current_lane = ?LANE_PREPARATION_FAILED
    }}.

-spec prepare_lane(state(), current_lane(), iterator:iterator()) ->
    {ok, state()} | ?WF_ERROR_LANE_ALREADY_PREPARED.
prepare_lane(#workflow_execution_state{current_lane = #current_lane{lane_index = LaneIndex}},
    #current_lane{lane_index = LaneIndex} = _NextLaneSpec, _Iterator) ->
    ?WF_ERROR_LANE_ALREADY_PREPARED;
prepare_lane(State, NextLaneSpec, Iterator) ->
    {ok, State#workflow_execution_state{
        current_lane = NextLaneSpec,
        iteration_state = workflow_iteration_state:init(Iterator),
        jobs = workflow_jobs:init()
    }}.

-spec register_and_prepare_new_item(
    state(),
    workflow_cached_item:id(),
    index(),
    workflow_iteration_state:iteration_step(),
    iterator:iterator()
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
            {TaskId, TaskSpec} = workflow_jobs:get_task_details(ToStart, BoxesSpec),
                {ok, State#workflow_execution_state{
                update_report = #job_prepared_report{job_identifier = ToStart,
                    task_id = TaskId, task_spec = TaskSpec},
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

-spec report_execution_status_update_internal(
    state(),
    workflow_jobs:job_identifier(),
    workflow_engine:execution_status_update_type(),
    workflow_engine:processing_result()
) -> {ok, state()}.
report_execution_status_update_internal(State = #workflow_execution_state{
    jobs = Jobs
}, JobIdentifier, ?ASYNC_CALL_STARTED, {ok, KeepaliveTimeout}) ->
    case workflow_jobs:register_async_call(Jobs, JobIdentifier, KeepaliveTimeout) of
        {ok, FinalJobs} ->
            {ok, State#workflow_execution_state{jobs = FinalJobs}};
        {?WF_ERROR_ALREADY_FINISHED(FinalAns), FinalJobs} ->
            report_execution_status_update_internal(
                State#workflow_execution_state{jobs = FinalJobs}, JobIdentifier, ?ASYNC_CALL_FINISHED, FinalAns)
    end;
report_execution_status_update_internal(State = #workflow_execution_state{
    jobs = Jobs
}, JobIdentifier, ?ASYNC_CALL_FINISHED, Ans) ->
    case workflow_jobs:remove_pending_async_job(Jobs, JobIdentifier, Ans) of
        {ok, NewJobs} ->
            report_job_finish(State#workflow_execution_state{jobs = NewJobs}, JobIdentifier, Ans);
        {?WF_ERROR_UNKNOWN_JOB, NewJobs} ->
            {ok, State#workflow_execution_state{jobs = NewJobs}}
    end;
report_execution_status_update_internal(State, JobIdentifier, _UpdateType, Ans) ->
    report_job_finish(State, JobIdentifier, Ans).

-spec prepare_next_waiting_job(state()) ->
    {ok, state()} | no_items_error() |
    ?WF_ERROR_NO_CACHED_ITEMS(index(), workflow_iteration_state:iteration_step(), workflow_engine:execution_context()) |
    ?WF_ERROR_EXECUTION_PREPARATION_FAILED.
prepare_next_waiting_job(State = #workflow_execution_state{
    context = Context,
    jobs = Jobs,
    iteration_state = IterationState,
    current_lane = #current_lane{lane_index = LaneIndex, parallel_boxes_spec = BoxesSpec}
}) ->
    case workflow_jobs:prepare_next_waiting_job(Jobs) of
        {ok, JobIdentifier, NewJobs} ->
            {TaskId, TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxesSpec),
            ItemId = workflow_jobs:get_item_id(JobIdentifier, IterationState),
            {ok, State#workflow_execution_state{
                update_report = #job_prepared_report{job_identifier = JobIdentifier,
                    task_id = TaskId, task_spec = TaskSpec, item_id = ItemId},
                jobs = NewJobs
            }};
        Error ->
            case workflow_iteration_state:get_last_registered_step(IterationState) of
                undefined -> % iteration has finished
                    handle_no_waiting_items_error(State, Error);
                IterationStep ->
                    ?WF_ERROR_NO_CACHED_ITEMS(LaneIndex, IterationStep, Context)
            end
    end;
prepare_next_waiting_job(State = #workflow_execution_state{
    current_lane = ?LANE_NOT_PREPARED
}) ->
    {ok, State#workflow_execution_state{current_lane = ?PREPARING_LANE, update_report = ?LANE_SET_TO_BE_PREPARED}};
prepare_next_waiting_job(#workflow_execution_state{
    current_lane = ?PREPARING_LANE
}) ->
    ?WF_ERROR_NO_WAITING_ITEMS;
prepare_next_waiting_job(#workflow_execution_state{
    current_lane = ?LANE_PREPARATION_FAILED
}) ->
    ?WF_ERROR_EXECUTION_PREPARATION_FAILED.

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

-spec reset_keepalive_timer_internal(state(), workflow_jobs:job_identifier()) -> {ok, state()}.
reset_keepalive_timer_internal(State = #workflow_execution_state{
    jobs = Jobs
}, JobIdentifier) ->
    {ok, State#workflow_execution_state{
        jobs = workflow_jobs:reset_keepalive_timer(Jobs, JobIdentifier)
    }}.

-spec report_job_finish(state(), workflow_jobs:job_identifier(), workflow_engine:processing_result()) -> {ok, state()}.
report_job_finish(State = #workflow_execution_state{
    jobs = Jobs
}, JobIdentifier, ok) ->
    % TODO VFS-7551 - should we protect against unknown keys? can they appear
    {NewJobs2, RemainingForBox} = workflow_jobs:mark_ongoing_job_finished(Jobs, JobIdentifier),
    % TODO VFS-7551 - Delete unused iterators and save information used for restarts
    case RemainingForBox of
        ?AT_LEAST_ONE_JOB_LEFT_FOR_PARALLEL_BOX ->
            {ok, State#workflow_execution_state{jobs = NewJobs2}};
        ?NO_JOBS_LEFT_FOR_PARALLEL_BOX ->
            prepare_next_parallel_box(State#workflow_execution_state{jobs = NewJobs2}, JobIdentifier)
    end;
report_job_finish(State = #workflow_execution_state{
    jobs = Jobs
}, JobIdentifier, error) ->
    FinalJobs = workflow_jobs:register_failure(Jobs, JobIdentifier),
    {ok, State#workflow_execution_state{jobs = FinalJobs}}.

-spec handle_no_waiting_items_error(state(), ?WF_ERROR_NO_WAITING_ITEMS | ?ERROR_NOT_FOUND) -> no_items_error().
handle_no_waiting_items_error(#workflow_execution_state{
    current_lane = #current_lane{lane_index = LaneIndex, is_last = IsLast},
    handler = Handler,
    context = Context
}, Error) ->
    case {Error, IsLast} of
        {?WF_ERROR_NO_WAITING_ITEMS, _} -> ?WF_ERROR_NO_WAITING_ITEMS;
        {?ERROR_NOT_FOUND, true} -> ?WF_ERROR_EXECUTION_FINISHED(Handler, Context, LaneIndex);
        {?ERROR_NOT_FOUND, false} -> ?WF_ERROR_LANE_FINISHED(LaneIndex, Handler, Context)
    end.

%%%===================================================================
%%% Test API
%%%===================================================================

-spec get_lane_index(workflow_engine:execution_id()) -> {ok, index()} | {error, term()}.
get_lane_index(ExecutionId) ->
    case datastore_model:get(?CTX, ExecutionId) of
        {ok, #document{value = #workflow_execution_state{current_lane = #current_lane{lane_index = Index}}}} ->
            {ok, Index};
        Error ->
            Error
    end.