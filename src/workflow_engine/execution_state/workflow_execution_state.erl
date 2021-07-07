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
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_execution_state).
-author("Michal Wrzeszcz").

-include("workflow_engine.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/3, init_using_snapshot/3, cleanup/1, prepare_next_job/1,
    report_execution_status_update/4, report_execution_prepared/4, report_limit_reached_error/2,
    check_timeouts/1, reset_keepalive_timer/2, get_result_processing_data/2]).
%% Test API
-export([is_finished_and_cleaned/2, get_lane_index/1, get_item_id/2]).

% Helper record to group fields containing information about lane currently being executed
-record(current_lane, {
    lane_index :: index(),
    is_last :: boolean(),
    parallel_boxes_count = 0 :: non_neg_integer(),
    parallel_boxes_spec :: boxes_map()
}).

% Macros and records used to provide additional information about document update procedure
% (see #workflow_execution_state.update_report)
-define(EXECUTION_SET_TO_BE_PREPARED, execution_set_to_be_prepared).
-record(job_prepared_report, {
    job_identifier :: workflow_jobs:job_identifier(),
    task_id :: workflow_engine:task_id(),
    task_spec :: workflow_engine:task_spec(),
    subject_id :: workflow_engine:subject_id()
}).
-record(items_processed_report, {
    lane_index :: index(),
    last_finished_item_index :: index(),
    item_id_to_snapshot :: workflow_cached_item:id() | undefined,
    item_ids_to_delete :: [workflow_cached_item:id()],
    notify_task_finished :: boolean()
}).
-define(TASK_PROCESSED_REPORT(NotifyTaskFinished), {task_processed_report, NotifyTaskFinished}).
-define(JOBS_EXPIRED(AsyncPoolsChanges), {jobs_expired, AsyncPoolsChanges}).

% Definitions of possible errors
-define(WF_ERROR_LANE_ALREADY_PREPARED, {error, lane_already_prepared}).
-define(WF_ERROR_LANE_CHANGED, {error, lane_changed}).
-define(WF_ERROR_EXECUTION_FINISHED(Handler, Context, LaneIndex, ErrorEncountered),
    {error, {execution_finished, Handler, Context, LaneIndex, ErrorEncountered}}).
-define(WF_ERROR_LANE_FINISHED(LaneIndex, Handler, Context), {error, {lane_finished, LaneIndex, Handler, Context}}).
-define(WF_ERROR_NO_CACHED_ITEMS(LaneIndex, ItemIndex, IterationStep, Context),
    {error, {no_cached_items, LaneIndex, ItemIndex, IterationStep, Context}}).
-define(WF_ERROR_EXECUTION_PREPARATION_FAILED(Handler, Context),
    {error, {execution_preparation_failed, Handler, Context}}).
-define(WF_ERROR_NOTHING_CHANGED, {error, nothing_changed}).
-define(WF_ERROR_ITERATION_FAILED, {error, iteration_failed}).

-type index() :: non_neg_integer(). % scheduling is based on positions of elements (items, parallel_boxes, tasks)
                                    % to allow executions of tasks in chosen order
-type iteration_step() :: {workflow_cached_item:id(), iterator:iterator()}.
-type iteration_status() :: iteration_step() | undefined | ?WF_ERROR_ITERATION_FAILED.
-type state() :: #workflow_execution_state{}.
-type doc() :: datastore_doc:doc(state()).

% Macros and types connected with preparation of execution (before first lane is started)
-define(NOT_PREPARED, not_prepared).
-define(PREPARING, preparing).
-define(PREPARATION_FAILED, preparation_failed).
-define(PREPARATION_SUCCESSFUL, preparation_successful).
-type preparation_status() :: ?NOT_PREPARED | ?PREPARING | ?PREPARATION_FAILED | ?PREPARATION_SUCCESSFUL.
% TODO VFS-7919 better type name
-type handler_execution_or_cached_async_result() ::
    workflow_engine:handler_execution_result() | workflow_cached_async_result:result_ref().

-type current_lane() :: #current_lane{}.
-type async_pools_slots_to_free() :: #{workflow_async_call_pool:id() => non_neg_integer()}.

%% @formatter:off
-type boxes_map() :: #{
    BoxIndex :: index() => #{
        TaskIndex :: index() => {workflow_engine:task_id(), workflow_engine:task_spec()}
    }
}.
%% @formatter:on

-type update_fun() :: datastore_doc:diff(state()).
-type no_items_error() :: ?WF_ERROR_NO_WAITING_ITEMS |
    ?WF_ERROR_EXECUTION_FINISHED(workflow_handler:handler(), workflow_engine:execution_context(), index(), boolean()) |
    ?WF_ERROR_LANE_FINISHED(index(), workflow_handler:handler(), workflow_engine:execution_context()).
% Type used to return additional information about document update procedure
% (see #workflow_execution_state.update_report)
-type update_report() :: ?EXECUTION_SET_TO_BE_PREPARED | #job_prepared_report{} | #items_processed_report{} |
    ?TASK_PROCESSED_REPORT(boolean()) | ?JOBS_EXPIRED(async_pools_slots_to_free()) | no_items_error().

-export_type([index/0, iteration_status/0, current_lane/0, preparation_status/0, boxes_map/0, update_report/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(workflow_engine:execution_id(), workflow_handler:handler(), workflow_engine:execution_context()) -> ok.
init(ExecutionId, Handler, Context) ->
    Doc = #document{key = ExecutionId, value = #workflow_execution_state{handler = Handler, context = Context}},
    {ok, _} = datastore_model:save(?CTX, Doc),
    ok.

-spec init_using_snapshot(
    workflow_engine:execution_id(), workflow_handler:handler(), workflow_engine:execution_context()) ->
    ok | ?WF_ERROR_PREPARATION_FAILED.
init_using_snapshot(ExecutionId, Handler, Context) ->
    case workflow_iterator_snapshot:get(ExecutionId) of
        {ok, LaneIndex, Iterator} ->
            case get_initial_iterator_and_lane_spec(ExecutionId, Handler, Context, LaneIndex) of
                {ok, _InitialIterator, CurrentLaneSpec} ->
                    NextIterationStep = get_next_iterator(Context, Iterator, ExecutionId),
                    {ok, Record} = prepare_lane(
                        #workflow_execution_state{handler = Handler, context = Context},
                        CurrentLaneSpec, NextIterationStep),
                    Doc = #document{key = ExecutionId, value = Record},
                    {ok, _} = datastore_model:save(?CTX, Doc),
                    ok;
                ?WF_ERROR_PREPARATION_FAILED ->
                    ?WF_ERROR_PREPARATION_FAILED
            end;
        ?ERROR_NOT_FOUND ->
            init(ExecutionId, Handler, Context)
    end.

-spec cleanup(workflow_engine:execution_id()) -> ok.
cleanup(ExecutionId) ->
    ok = datastore_model:delete(?CTX, ExecutionId).

-spec prepare_next_job(workflow_engine:execution_id()) ->
    {ok, workflow_engine:execution_spec()} |
    ?END_EXECUTION(workflow_handler:handler(), workflow_engine:execution_context(), index(), boolean()) |
    ?PREPARE_EXECUTION(workflow_handler:handler(), workflow_engine:execution_context()) | ?DEFER_EXECUTION |
    ?END_EXECUTION_AFTER_PREPARATION_ERROR(workflow_handler:handler(), workflow_engine:execution_context()).
prepare_next_job(ExecutionId) ->
    % TODO VFS-7787 - check quota for async jobs and do nothing if it is exceeded
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
        ?WF_ERROR_EXECUTION_FINISHED(Handler, Context, LaneIndex, ErrorEncountered) ->
            ?END_EXECUTION(Handler, Context, LaneIndex, ErrorEncountered);
        ?WF_ERROR_EXECUTION_PREPARATION_FAILED(Handler, Context) ->
            ?END_EXECUTION_AFTER_PREPARATION_ERROR(Handler, Context)
    end.

-spec report_execution_status_update(
    workflow_engine:execution_id(),
    workflow_jobs:job_identifier(),
    workflow_engine:processing_stage(),
    workflow_engine:processing_result()
) -> workflow_engine:task_spec() | ?WF_ERROR_JOB_NOT_FOUND.
report_execution_status_update(ExecutionId, JobIdentifier, UpdateType, Ans) ->
    CachedAns = case UpdateType of
        ?ASYNC_CALL_FINISHED -> workflow_cached_async_result:put(Ans);
        _ -> Ans
    end,

    {UpdatedDoc, NotifyTaskExecutionEnded} = case update(ExecutionId, fun(State) ->
        report_execution_status_update_internal(State, JobIdentifier, UpdateType, CachedAns)
    end) of
        {ok, Doc = #document{value = #workflow_execution_state{update_report = #items_processed_report{
            item_id_to_snapshot = undefined,
            item_ids_to_delete = ItemIdsToDelete,
            notify_task_finished = NotifyTaskFinished
        }}}} ->
            lists:foreach(fun(ItemId) -> workflow_cached_item:delete(ItemId) end, ItemIdsToDelete),
            {Doc, NotifyTaskFinished};
        {ok, Doc = #document{value = #workflow_execution_state{update_report = #items_processed_report{
            lane_index = LaneIndex,
            last_finished_item_index = ItemIndex,
            item_id_to_snapshot = ItemIdToSnapshot,
            item_ids_to_delete = ItemIdsToDelete,
            notify_task_finished = NotifyTaskFinished
        }}}} ->
            IteratorToSave = workflow_cached_item:get_iterator(ItemIdToSnapshot),
            workflow_iterator_snapshot:save(ExecutionId, LaneIndex, ItemIndex, IteratorToSave),
            lists:foreach(fun(ItemId) -> workflow_cached_item:delete(ItemId) end, ItemIdsToDelete),
            {Doc, NotifyTaskFinished};
        {ok, Doc = #document{value = #workflow_execution_state{
            update_report = ?TASK_PROCESSED_REPORT(NotifyTaskFinished)
        }}} ->
            {Doc, NotifyTaskFinished};
        {ok, Doc} ->
            {Doc, false};
        ?WF_ERROR_JOB_NOT_FOUND ->
            ?debug("Result for not found job ~p of execution ~p", [JobIdentifier, ExecutionId]),
            {?WF_ERROR_JOB_NOT_FOUND, false};
        ?ERROR_NOT_FOUND ->
            ?debug("Result for job ~p of ended execution ~p", [JobIdentifier, ExecutionId]),
            {?WF_ERROR_JOB_NOT_FOUND, false}
    end,

    case UpdatedDoc of
        ?WF_ERROR_JOB_NOT_FOUND ->
            ?WF_ERROR_JOB_NOT_FOUND; % Error occurred - no task can be connected to result
        _ ->
            maybe_notify_task_execution_ended(UpdatedDoc, JobIdentifier, NotifyTaskExecutionEnded),

            #document{value = #workflow_execution_state{
                current_lane = #current_lane{parallel_boxes_spec = BoxesSpec}
            }} = UpdatedDoc,
            {_TaskId, TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxesSpec),
            TaskSpec
    end.

-spec report_execution_prepared(
    workflow_engine:execution_id(),
    workflow_handler:handler(),
    workflow_engine:execution_context(),
    workflow_handler:handler_execution_result()
) -> ok.
report_execution_prepared(ExecutionId, Handler, ExecutionContext, ok) ->
    case prepare_lane(ExecutionId, Handler, ExecutionContext, 1) of
        {ok, IteratorToSave} -> workflow_iterator_snapshot:save(ExecutionId, 0, 0, IteratorToSave);
        ?WF_ERROR_PREPARATION_FAILED -> ok
    end;
report_execution_prepared(ExecutionId, _Handler, _ExecutionContext, error) ->
    {ok, _} = update(ExecutionId, fun(State) ->
        handle_preparation_failure(State)
    end),
    ok.

-spec report_limit_reached_error(workflow_engine:execution_id(), workflow_jobs:job_identifier()) -> ok.
report_limit_reached_error(ExecutionId, JobIdentifier) ->
    {ok, _} = update(ExecutionId, fun(State) -> pause_job(State, JobIdentifier) end),
    ok.

-spec check_timeouts(workflow_engine:execution_id()) -> TimeoutAppeared :: boolean().
check_timeouts(ExecutionId) ->
    case update(ExecutionId, fun check_timeouts_internal/1) of
        {ok, #document{value = #workflow_execution_state{update_report = ?JOBS_EXPIRED(AsyncPoolsSlotsToFree)}}} ->
            lists:foreach(fun({AsyncPoolId, SlotsToFreeCount}) ->
                workflow_async_call_pool:decrement_slot_usage(AsyncPoolId, SlotsToFreeCount)
            end, maps:to_list(AsyncPoolsSlotsToFree)),
            maps:size(AsyncPoolsSlotsToFree) =/= 0;
        ?WF_ERROR_NOTHING_CHANGED  ->
            false
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
) -> {ok, iterator:iterator(), current_lane()} | ?WF_ERROR_PREPARATION_FAILED.
get_initial_iterator_and_lane_spec(ExecutionId, Handler, Context, LaneIndex) ->
    try
        case Handler:get_lane_spec(ExecutionId, Context, LaneIndex) of
            {ok, #{
                parallel_boxes := Boxes,
                iterator := Iterator,
                is_last := IsLast
            } = Lane} ->
                case Boxes of
                    [] ->
                        % workflow_jobs require at least one parallel_boxes in lane
                        ?error("No parallel boxes for lane ~p of execution id: ~p", [Lane, ExecutionId]),
                        ?WF_ERROR_PREPARATION_FAILED;
                    _ ->
                        BoxesMap = lists:foldl(fun({BoxIndex, BoxSpec}, BoxesAcc) ->
                            Tasks = lists:foldl(fun({TaskIndex, {TaskId, TaskSpec}}, TaskAcc) ->
                                TaskAcc#{TaskIndex => {TaskId, TaskSpec}}
                            end, #{}, lists_utils:enumerate(maps:to_list(BoxSpec))),
                            BoxesAcc#{BoxIndex => Tasks}
                        end, #{}, lists_utils:enumerate(Boxes)),

                        {
                            ok,
                            Iterator,
                            #current_lane{
                                lane_index = LaneIndex,
                                is_last = IsLast,
                                parallel_boxes_count = length(Boxes),
                                parallel_boxes_spec = BoxesMap
                            }
                        }
                end;
            error ->
                ?WF_ERROR_PREPARATION_FAILED
        end
    catch
        Error:Reason  ->
            ?error_stacktrace("Unexpected error preparing lane ~p (execution ~p): ~p:~p",
                [LaneIndex, ExecutionId, Error, Reason]),
            ?WF_ERROR_PREPARATION_FAILED
    end.

-spec prepare_lane(
    workflow_engine:execution_id(),
    workflow_handler:handler(),
    workflow_engine:execution_context(),
    index()
) -> {ok, iterator:iterator()} | ?WF_ERROR_PREPARATION_FAILED.
prepare_lane(ExecutionId, Handler, Context, LaneIndex) ->
    case LaneIndex > 1 of
        true ->
            workflow_engine:call_handler(ExecutionId, Context, Handler, handle_lane_execution_ended, [LaneIndex - 1]);
        false ->
            ok
    end,
    case get_initial_iterator_and_lane_spec(ExecutionId, Handler, Context, LaneIndex) of
        {ok, Iterator, CurrentLaneSpec} ->
            NextIterationStep = get_next_iterator(Context, Iterator, ExecutionId),
            case update(ExecutionId, fun(State) ->
                prepare_lane(State, CurrentLaneSpec, NextIterationStep)
            end) of
                {ok, _} ->
                    {ok, Iterator};
                ?WF_ERROR_LANE_ALREADY_PREPARED ->
                    case NextIterationStep of
                        undefined -> ok;
                        {ItemId, _} -> workflow_cached_item:delete(ItemId)
                    end,
                    {ok, Iterator}
            end;
        ?WF_ERROR_PREPARATION_FAILED ->
            {ok, _} = update(ExecutionId, fun(State) ->
                handle_preparation_failure(State)
            end),
            ?WF_ERROR_PREPARATION_FAILED
    end.

-spec prepare_next_job_for_current_lane(workflow_engine:execution_id()) ->
    {ok, workflow_engine:execution_spec()} |
    ?PREPARE_EXECUTION(workflow_handler:handler(), workflow_engine:execution_context()) | no_items_error() |
    ?WF_ERROR_EXECUTION_PREPARATION_FAILED(workflow_handler:handler(), workflow_engine:execution_context()).
prepare_next_job_for_current_lane(ExecutionId) ->
    case update(ExecutionId, fun prepare_next_waiting_job/1) of
        {ok, #document{value = #workflow_execution_state{update_report = ?EXECUTION_SET_TO_BE_PREPARED,
            handler = Handler, context = ExecutionContext}}} ->
            ?PREPARE_EXECUTION(Handler, ExecutionContext);
        {ok, #document{value = #workflow_execution_state{update_report = #job_prepared_report{
            job_identifier = JobIdentifier, task_id = TaskId, task_spec = TaskSpec, subject_id = SubjectId},
            handler = Handler, context = ExecutionContext}}} ->
            {ok, #execution_spec{
                handler = Handler,
                context = ExecutionContext,
                task_id = TaskId,
                task_spec = TaskSpec,
                subject_id = SubjectId,
                job_identifier = JobIdentifier
            }};
        ?WF_ERROR_NO_CACHED_ITEMS(LaneIndex, ItemIndex, IterationStep, Context) ->
            prepare_next_job_using_iterator(ExecutionId, ItemIndex, IterationStep, LaneIndex, Context);
        {error, _} = Error ->
            Error
    end.

-spec prepare_next_job_using_iterator(workflow_engine:execution_id(), index(), iteration_status(),
    index(), workflow_engine:execution_context()) -> {ok, workflow_engine:execution_spec()} | no_items_error().
prepare_next_job_using_iterator(ExecutionId, ItemIndex, CurrentIterationStep, LaneIndex, Context) ->
    NextIterationStep = case CurrentIterationStep of
        undefined ->
            undefined;
        ?WF_ERROR_ITERATION_FAILED ->
            undefined;
        {_, CurrentIterator} ->
            % TODO VFS-7787 return (to engine) item instead of item_id in this case (engine must read from cache when we have item here)
            % Maybe generate item_id using index (there will be no need to translate job to datastore key)?
            get_next_iterator(Context, CurrentIterator, ExecutionId)
    end,

    ParallelBoxToStart = 1, % TODO VFS-7788 - get ParallelBoxToStart from iterator
    case update(ExecutionId, fun(State) ->
        handle_next_iteration_step(State, LaneIndex, ItemIndex, NextIterationStep, ParallelBoxToStart)
    end) of
        {ok, #document{value = #workflow_execution_state{update_report = #job_prepared_report{
            job_identifier = JobIdentifier, task_id = TaskId, task_spec = TaskSpec, subject_id = SubjectId},
            handler = Handler, context = ExecutionContext}}} ->
            {ok, #execution_spec{
                handler = Handler,
                context = ExecutionContext,
                task_id = TaskId,
                task_spec = TaskSpec,
                subject_id = SubjectId,
                job_identifier = JobIdentifier
            }};
        {ok, #document{value = #workflow_execution_state{update_report = {error, _} = UpdateReport}}} ->
            UpdateReport; % Nothing to prepare
        ?WF_ERROR_LANE_CHANGED ->
            prepare_next_job_for_current_lane(ExecutionId);
        ?WF_ERROR_RACE_CONDITION ->
            case NextIterationStep of
                undefined -> ok;
                ?WF_ERROR_ITERATION_FAILED -> ok;
                {ItemId, _} -> workflow_cached_item:delete(ItemId)
            end,
            prepare_next_job_for_current_lane(ExecutionId)
    end.

-spec maybe_notify_task_execution_ended(doc(), workflow_jobs:job_identifier(), boolean()) -> ok.
maybe_notify_task_execution_ended(_Doc, _JobIdentifier, false = _NotifyTaskExecutionEnded) ->
    ok;
maybe_notify_task_execution_ended(#document{key = ExecutionId, value = #workflow_execution_state{
    handler = Handler,
    context = Context,
    current_lane = #current_lane{parallel_boxes_spec = BoxesSpec}
}}, JobIdentifier, true = _NotifyTaskExecutionEnded) ->
    {TaskId, _TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxesSpec),
    workflow_engine:call_handler(ExecutionId, Context, Handler, handle_task_execution_ended, [TaskId]),
    ok.

-spec update(workflow_engine:execution_id(), update_fun()) -> {ok, state()} | {error, term()}.
update(ExecutionId, UpdateFun) ->
    % TODO VFS-7787 - should we add try/catch or allow functions fail?
    datastore_model:update(?CTX, ExecutionId, fun(State) ->
        UpdateFun(State#workflow_execution_state{update_report = undefined})
    end).

-spec get_next_iterator(workflow_engine:execution_context(), iterator:iterator(), workflow_engine:execution_id()) ->
    iteration_status().
get_next_iterator(Context, Iterator, ExecutionId) ->
    try
        case iterator:get_next(Context, Iterator) of
            {ok, NextItem, NextIterator} ->
                {workflow_cached_item:put(NextItem, NextIterator), NextIterator};
            stop ->
                undefined
        end
    catch
        Error:Reason ->
            ?error_stacktrace("Unexpected error getting next iterator (execution ~p): ~p:~p",
                [ExecutionId, Error, Reason]),
            ?WF_ERROR_ITERATION_FAILED
    end.

%%%===================================================================
%%% Functions updating record
%%%===================================================================

-spec handle_preparation_failure(state()) -> {ok, state()}.
handle_preparation_failure(State) ->
    {ok, State#workflow_execution_state{
        preparation_status = ?PREPARATION_FAILED
    }}.

-spec prepare_lane(state(), current_lane(), iteration_status()) ->
    {ok, state()} | ?WF_ERROR_LANE_ALREADY_PREPARED.
prepare_lane(#workflow_execution_state{current_lane = #current_lane{lane_index = LaneIndex}},
    #current_lane{lane_index = LaneIndex} = _NextLaneSpec, _PrefetchedIterationStep) ->
    ?WF_ERROR_LANE_ALREADY_PREPARED;
prepare_lane(State, NextLaneSpec, PrefetchedIterationStep) ->
    {ok, State#workflow_execution_state{
        preparation_status = ?PREPARATION_SUCCESSFUL,
        current_lane = NextLaneSpec,
        iteration_state = workflow_iteration_state:init(),
        prefetched_iteration_step = PrefetchedIterationStep,
        jobs = workflow_jobs:init()
    }}.

-spec handle_next_iteration_step(
    state(),
    index(),
    index(),
    iteration_status(),
    index()
) ->
    {ok, state()} | ?WF_ERROR_RACE_CONDITION | ?WF_ERROR_LANE_CHANGED.
handle_next_iteration_step(State = #workflow_execution_state{
    jobs = Jobs,
    iteration_state = IterationState,
    prefetched_iteration_step = PrefetchedIterationStep,
    current_lane = #current_lane{lane_index = LaneIndex, parallel_boxes_spec = BoxesSpec}
}, LaneIndex, PrevItemIndex, NextIterationStep, ParallelBoxToStart) ->
    CanUsePrefetchedIterationStep = case PrefetchedIterationStep of
        undefined -> false;
        ?WF_ERROR_ITERATION_FAILED -> false;
        _ -> true
    end,

    case {CanUsePrefetchedIterationStep, NextIterationStep} of
        {false, undefined} ->
            State2 = State#workflow_execution_state{
                iteration_state = workflow_iteration_state:handle_iteration_finished(IterationState)},
            case prepare_next_waiting_job(State2) of
                {ok, _} = OkAns -> OkAns;
                Error -> {ok, State2#workflow_execution_state{update_report = Error}}
            end;
        {false, _} ->
            % TODO VFS-7787 - maybe call handle_iteration_finished/1 when NextIterationStep
            % is undefined first time (next case) to prevent this race
            ?WF_ERROR_RACE_CONDITION;
        {true, _} ->
            {PrefetchedItemId, _PrefetchedIterator} = PrefetchedIterationStep,
            % TODO VFS-7789 - it may be needed to allow registration of waiting items as a result of async call processing finish
            case workflow_iteration_state:register_new_item(
                IterationState, PrevItemIndex, PrefetchedItemId) of
                ?WF_ERROR_RACE_CONDITION = Error ->
                    Error;
                {NewItemIndex, NewIterationState} ->
                    {NewJobs, ToStart} = workflow_jobs:populate_with_jobs_for_item(
                        Jobs, NewItemIndex, ParallelBoxToStart, BoxesSpec),
                    FinalJobs = case NextIterationStep of
                        undefined -> workflow_jobs:build_tasks_tree(NewJobs);
                        _ -> NewJobs
                    end,
                    {TaskId, TaskSpec} = workflow_jobs:get_task_details(ToStart, BoxesSpec),
                    {ok, State#workflow_execution_state{
                        update_report = #job_prepared_report{job_identifier = ToStart,
                            task_id = TaskId, task_spec = TaskSpec, subject_id = PrefetchedItemId},
                        iteration_state = NewIterationState,
                        prefetched_iteration_step = NextIterationStep,
                        jobs = FinalJobs
                    }}
            end
    end;
handle_next_iteration_step(_State, _LaneIndex, _PrevItemIndex, _NextIterationStep, _ParallelBoxToStart) ->
    ?WF_ERROR_LANE_CHANGED.

-spec pause_job(state(), workflow_jobs:job_identifier()) -> {ok, state()}.
pause_job(State = #workflow_execution_state{jobs = Jobs}, JobIdentifier) ->
    {ok, State#workflow_execution_state{
        jobs = workflow_jobs:pause_job(Jobs, JobIdentifier)
    }}.

-spec check_timeouts_internal(state()) -> {ok, state()} | ?WF_ERROR_NOTHING_CHANGED.
check_timeouts_internal(State = #workflow_execution_state{
    jobs = Jobs,
    current_lane = #current_lane{parallel_boxes_spec = BoxesSpec}
}) ->
    % TODO VFS-7788 - check if task is expired (do it outside tp process)
    {?WF_ERROR_NO_TIMEOUTS_UPDATED, ExpiredJobsIdentifiers} = workflow_jobs:check_timeouts(Jobs),

    case length(ExpiredJobsIdentifiers) of
        0 ->
            ?WF_ERROR_NOTHING_CHANGED;
        _ ->
            {FinalState, AsyncPoolsSlotsToFree} = lists:foldl(fun(JobIdentifier, {TmpState, TmpAsyncPoolsSlotsToFree}) ->
                {ok, NewTmpState} = report_execution_status_update_internal(
                    TmpState, JobIdentifier, ?ASYNC_CALL_FINISHED, ?WF_ERROR_TIMEOUT),

                {_, TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxesSpec),
                NewTmpAsyncPoolsSlotsToFree = lists:foldl(fun(AsyncPoolId, InternalTmpAsyncPoolsChange) ->
                    TmpChange = maps:get(AsyncPoolId, InternalTmpAsyncPoolsChange, 0),
                    InternalTmpAsyncPoolsChange#{AsyncPoolId => TmpChange + 1}
                end, TmpAsyncPoolsSlotsToFree, workflow_engine:get_async_call_pools(TaskSpec)),
                {NewTmpState, NewTmpAsyncPoolsSlotsToFree}
            end, {State, #{}}, ExpiredJobsIdentifiers),

            {ok, FinalState#workflow_execution_state{update_report = ?JOBS_EXPIRED(AsyncPoolsSlotsToFree)}}
    end.

-spec report_execution_status_update_internal(
    state(),
    workflow_jobs:job_identifier(),
    workflow_engine:processing_stage(),
    handler_execution_or_cached_async_result()
) -> {ok, state()} | ?WF_ERROR_JOB_NOT_FOUND.
report_execution_status_update_internal(State = #workflow_execution_state{
    jobs = Jobs
}, JobIdentifier, ?ASYNC_CALL_STARTED, {ok, KeepaliveTimeout}) ->
    {ok, State#workflow_execution_state{
        jobs = workflow_jobs:register_async_call(Jobs, JobIdentifier, KeepaliveTimeout)}};
report_execution_status_update_internal(State = #workflow_execution_state{
    jobs = Jobs
}, JobIdentifier, ?ASYNC_CALL_FINISHED, CachedResultId) ->
    case workflow_jobs:register_async_job_finish(Jobs, JobIdentifier, CachedResultId) of
        {ok, NewJobs} -> {ok, State#workflow_execution_state{jobs = NewJobs}};
        ?WF_ERROR_JOB_NOT_FOUND -> ?WF_ERROR_JOB_NOT_FOUND
    end;
report_execution_status_update_internal(State, JobIdentifier, _UpdateType, Ans) ->
    report_job_finish(State, JobIdentifier, Ans).

-spec prepare_next_waiting_job(state()) ->
    {ok, state()} | no_items_error() |
    ?WF_ERROR_NO_CACHED_ITEMS(index(), index(), iteration_status(), workflow_engine:execution_context()) |
    ?WF_ERROR_EXECUTION_PREPARATION_FAILED(workflow_handler:handler(), workflow_engine:execution_context()).
prepare_next_waiting_job(State = #workflow_execution_state{
    preparation_status = ?PREPARATION_SUCCESSFUL,
    context = Context,
    jobs = Jobs,
    iteration_state = IterationState,
    prefetched_iteration_step = PrefetchedIterationStep,
    current_lane = #current_lane{lane_index = LaneIndex, parallel_boxes_spec = BoxesSpec}
}) ->
    case workflow_jobs:prepare_next_waiting_job(Jobs) of
        {ok, JobIdentifier, NewJobs} ->
            {TaskId, TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxesSpec),
            % Use old `Jobs` as subject is no longer present in `NewJobs` (job is not waiting anymore)
            SubjectId = workflow_jobs:get_subject_id(JobIdentifier, Jobs, IterationState),
            {ok, State#workflow_execution_state{
                update_report = #job_prepared_report{job_identifier = JobIdentifier,
                    task_id = TaskId, task_spec = TaskSpec, subject_id = SubjectId},
                jobs = NewJobs
            }};
        Error ->
            case workflow_iteration_state:get_last_registered_item_index(IterationState) of
                undefined -> % iteration has finished
                    handle_no_waiting_items_error(State, Error);
                ItemIndex ->
                    ?WF_ERROR_NO_CACHED_ITEMS(LaneIndex, ItemIndex, PrefetchedIterationStep, Context)
            end
    end;
prepare_next_waiting_job(State = #workflow_execution_state{
    preparation_status = ?NOT_PREPARED
}) ->
    {ok, State#workflow_execution_state{preparation_status = ?PREPARING, update_report = ?EXECUTION_SET_TO_BE_PREPARED}};
prepare_next_waiting_job(#workflow_execution_state{
    preparation_status = ?PREPARING
}) ->
    ?WF_ERROR_NO_WAITING_ITEMS;
prepare_next_waiting_job(#workflow_execution_state{
    preparation_status = ?PREPARATION_FAILED,
    handler = Handler,
    context = Context
}) ->
    ?WF_ERROR_EXECUTION_PREPARATION_FAILED(Handler, Context).

-spec prepare_next_parallel_box(state(), workflow_jobs:job_identifier()) -> {ok, state()}.
prepare_next_parallel_box(State = #workflow_execution_state{
    current_lane = #current_lane{
        parallel_boxes_count = BoxCount,
        parallel_boxes_spec = BoxesSpec,
        lane_index = LaneIndex
    },
    lowest_failed_job_identifier = LowestFailedJobIdentifier,
    jobs = Jobs,
    iteration_state = IterationState
}, JobIdentifier) ->
    case workflow_jobs:prepare_next_parallel_box(Jobs, JobIdentifier, BoxesSpec, BoxCount) of
        {ok, NewJobs} ->
            {ok, State#workflow_execution_state{
                jobs = NewJobs,
                update_report = ?TASK_PROCESSED_REPORT(workflow_jobs:is_task_finished(NewJobs, JobIdentifier))
            }};
        {?WF_ERROR_ITEM_PROCESSING_FINISHED(ItemIndex, SuccessOrFailure), NewJobs} ->
            {NewIterationState, ItemIdToSnapshot, ItemIdsToDelete} =
                workflow_iteration_state:handle_item_processed(IterationState, ItemIndex, SuccessOrFailure),
            FinalItemIdToSnapshot = case LowestFailedJobIdentifier of
                undefined ->
                    ItemIdToSnapshot;
                _ ->
                    case workflow_jobs:is_previous(JobIdentifier, LowestFailedJobIdentifier) of
                        true -> ItemIdToSnapshot;
                        false -> undefined
                    end
            end,
            NotifyTaskFinished = workflow_jobs:is_task_finished(NewJobs, JobIdentifier),
            {ok, State#workflow_execution_state{
                jobs = NewJobs,
                iteration_state = NewIterationState,
                update_report = #items_processed_report{lane_index = LaneIndex, last_finished_item_index = ItemIndex,
                    item_id_to_snapshot = FinalItemIdToSnapshot, item_ids_to_delete = ItemIdsToDelete,
                    notify_task_finished = NotifyTaskFinished}
            }}
    end.

-spec reset_keepalive_timer_internal(state(), workflow_jobs:job_identifier()) -> {ok, state()}.
reset_keepalive_timer_internal(State = #workflow_execution_state{
    jobs = Jobs
}, JobIdentifier) ->
    {ok, State#workflow_execution_state{
        jobs = workflow_jobs:reset_keepalive_timer(Jobs, JobIdentifier)
    }}.

-spec report_job_finish(state(), workflow_jobs:job_identifier(), workflow_handler:handler_execution_result()) ->
    {ok, state()}.
report_job_finish(State = #workflow_execution_state{
    jobs = Jobs
}, JobIdentifier, ok) ->
    % TODO VFS-7787 - should we protect against unknown keys? can they appear
    {NewJobs2, RemainingForBox} = workflow_jobs:mark_ongoing_job_finished(Jobs, JobIdentifier),
    case RemainingForBox of
        ?AT_LEAST_ONE_JOB_LEFT_FOR_PARALLEL_BOX ->
            {ok, State#workflow_execution_state{
                jobs = NewJobs2,
                update_report = ?TASK_PROCESSED_REPORT(workflow_jobs:is_task_finished(NewJobs2, JobIdentifier))
            }};
        ?NO_JOBS_LEFT_FOR_PARALLEL_BOX ->
            prepare_next_parallel_box(State#workflow_execution_state{jobs = NewJobs2}, JobIdentifier)
    end;
report_job_finish(State = #workflow_execution_state{
    jobs = Jobs,
    lowest_failed_job_identifier = LowestFailedJobIdentifier
}, JobIdentifier, error) ->
    {FinalJobs, RemainingForBox} = workflow_jobs:register_failure(Jobs, JobIdentifier),
    State2 = case LowestFailedJobIdentifier of
        undefined ->
            State#workflow_execution_state{jobs = FinalJobs, lowest_failed_job_identifier = JobIdentifier};
        _ ->
            case workflow_jobs:is_previous(LowestFailedJobIdentifier, JobIdentifier) of
                true -> State#workflow_execution_state{jobs = FinalJobs};
                false -> State#workflow_execution_state{jobs = FinalJobs, lowest_failed_job_identifier = JobIdentifier}
            end
    end,
    case RemainingForBox of
        ?AT_LEAST_ONE_JOB_LEFT_FOR_PARALLEL_BOX ->
            {ok, State2#workflow_execution_state{
                update_report = ?TASK_PROCESSED_REPORT(workflow_jobs:is_task_finished(FinalJobs, JobIdentifier))
            }};
        ?NO_JOBS_LEFT_FOR_PARALLEL_BOX ->
            % Call prepare_next_parallel_box/2 to delete metadata for failed item
            prepare_next_parallel_box(State2, JobIdentifier)
    end.

-spec handle_no_waiting_items_error(state(), ?WF_ERROR_NO_WAITING_ITEMS | ?ERROR_NOT_FOUND) -> no_items_error().
handle_no_waiting_items_error(#workflow_execution_state{
    current_lane = #current_lane{lane_index = LaneIndex, is_last = IsLast},
    lowest_failed_job_identifier = LowestFailedJobIdentifier,
    prefetched_iteration_step = PrefetchedIterationStep,
    handler = Handler,
    context = Context
}, Error) ->
    HasErrorEncountered = case {LowestFailedJobIdentifier, PrefetchedIterationStep} of
        {undefined, undefined} -> false;
        _ -> true
    end,
    case {Error, IsLast orelse HasErrorEncountered} of
        {?WF_ERROR_NO_WAITING_ITEMS, _} -> ?WF_ERROR_NO_WAITING_ITEMS;
        {?ERROR_NOT_FOUND, true} -> ?WF_ERROR_EXECUTION_FINISHED(Handler, Context, LaneIndex, HasErrorEncountered);
        {?ERROR_NOT_FOUND, false} -> ?WF_ERROR_LANE_FINISHED(LaneIndex, Handler, Context)
    end.

%%%===================================================================
%%% Test API
%%%===================================================================

-spec is_finished_and_cleaned(workflow_engine:execution_id(), index()) ->
    true | {false, state()} | ?WF_ERROR_LANE_CHANGED.
is_finished_and_cleaned(ExecutionId, LaneIndex) ->
    case datastore_model:get(?CTX, ExecutionId) of
        {ok, #document{value = #workflow_execution_state{current_lane = #current_lane{lane_index = LaneIndex}} = Record}} ->
            #workflow_execution_state{jobs = Jobs, iteration_state = IterationState} = Record,
            case {workflow_jobs:is_empty(Jobs), workflow_iteration_state:is_finished_and_cleaned(IterationState)} of
                {true, true} -> true;
                _ -> {false, Record}
            end;
        {ok, #document{value = #workflow_execution_state{current_lane = #current_lane{lane_index = Index}}}}
            when Index > LaneIndex ->
            ?WF_ERROR_LANE_CHANGED;
        {ok, #document{value = Record}} ->
            {false, Record}
    end.

-spec get_lane_index(workflow_engine:execution_id()) -> {ok, index()} | {error, term()}.
get_lane_index(ExecutionId) ->
    case datastore_model:get(?CTX, ExecutionId) of
        {ok, #document{value = #workflow_execution_state{current_lane = #current_lane{lane_index = Index}}}} ->
            {ok, Index};
        Error ->
            Error
    end.

-spec get_item_id(workflow_engine:execution_id(), workflow_jobs:job_identifier()) -> workflow_cached_item:id().
get_item_id(ExecutionId, JobIdentifier) ->
    {ok, #document{value = #workflow_execution_state{iteration_state = IterationState}}} =
        datastore_model:get(?CTX, ExecutionId),
    workflow_jobs:get_item_id(JobIdentifier, IterationState).