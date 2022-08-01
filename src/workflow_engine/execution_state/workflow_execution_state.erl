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
-export([init/6, restart_from_snapshot/6, cancel/1, cleanup/1, prepare_next_job/1,
    report_execution_status_update/4, report_lane_execution_prepared/4, report_limit_reached_error/2,
    report_new_streamed_task_data/3, report_streamed_task_data_processed/4, mark_all_streamed_task_data_received/3,
    check_timeouts/1, reset_keepalive_timer/2, get_result_processing_data/2]).
%% Test API
-export([is_finished_and_cleaned/2, get_item_id/2, get_current_lane_context/1]).

% Helper record to group fields containing information about lane currently being executed
-record(current_lane, {
    index :: index(), % TODO VFS-7919 - after introduction of id, index is used during test and by snapshot to verify
                      % necessity of callback execution - consider deletion of this field
    id :: workflow_engine:lane_id() | undefined,
    execution_context :: workflow_engine:execution_context() | undefined,
    parallel_box_count = 0 :: non_neg_integer() | undefined,
    parallel_box_specs :: boxes_map() | undefined,
    failure_count_to_cancel :: non_neg_integer() | undefined
}).

% Helper record to group fields containing information about lane that probably will be
% executed after current lane is successfully finished (next lane can be changed as a result of
% workflow_handler:handle_lane_execution_ended/3 execution)
-record(next_lane, {
    id :: workflow_engine:lane_id() | undefined,
    lane_spec :: workflow_engine:lane_spec() | undefined
}).

% Macros and records used to provide additional information about document update procedure
% (see #workflow_execution_state.update_report)
-define(LANE_DESIGNATED_FOR_PREPARATION, lane_designated_for_preparation).
-define(LANE_DESIGNATED_FOR_PREPARATION_IN_ADVANCE, lane_designated_for_preparation_in_advance).
-record(job_prepared_report, {
    job_identifier :: workflow_jobs:job_identifier() | streamed_task_data,
    task_id :: workflow_engine:task_id(),
    task_spec :: workflow_engine:task_spec() | undefined, % for task_data processing spec is not required
    subject_id :: workflow_engine:subject_id()
}).
-record(items_processed_report, {
    lane_index :: index(),
    lane_id :: workflow_engine:id(),
    last_finished_item_index :: index(),
    item_id_to_report_error :: workflow_cached_item:id() | undefined,
    item_id_to_snapshot :: workflow_cached_item:id() | undefined,
    item_ids_to_delete :: [workflow_cached_item:id()],
    task_statuses_to_report :: [{workflow_jobs:job_identifier(), task_status()}]
}).
-define(TASK_PROCESSED_REPORT(TaskStatus), {task_processed_report, TaskStatus}).
-define(EXECUTION_CANCELLED_REPORT(ItemIdsToDelete, TaskIds), {execution_canceled_report, ItemIdsToDelete, TaskIds}).
-define(EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT(TaskIds), {execution_canceled_with_open_streams_report, TaskIds}).
-define(EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT(Handler, LaneContext, TaskIds),
    {execution_canceled_with_open_streams_report, Handler, LaneContext, TaskIds}).
-define(LANE_READY_TO_BE_FINISHED_REPORT(LaneId, LaneContext), {lane_ready_to_be_finished_report, LaneId, LaneContext}).
-define(JOBS_EXPIRED_REPORT(AsyncPoolsChanges), {jobs_expired_report, AsyncPoolsChanges}).
-define(LANE_PREPARED_REPORT(Lane), {lane_prepared_report, Lane}).

% Definitions of possible errors
-define(WF_ERROR_LANE_ALREADY_PREPARED, {error, lane_already_prepared}).
-define(WF_ERROR_LANE_CHANGED, {error, lane_changed}).
-define(WF_ERROR_LANE_EXECUTION_CANCELLED(Handler, CancelledLaneId, CancelledLaneContext, TaskIds),
    {error, {lane_execution_cancelled, Handler, CancelledLaneId, CancelledLaneContext, TaskIds}}).
-define(WF_ERROR_NO_CACHED_ITEMS(LaneIndex, ItemIndex, IterationStep, Context),
    {error, {no_cached_items, LaneIndex, ItemIndex, IterationStep, Context}}).
-define(WF_ERROR_NOTHING_CHANGED, {error, nothing_changed}).
-define(WF_ERROR_ITERATION_FAILED, {error, iteration_failed}).
-define(WF_ERROR_CURRENT_LANE, {error, current_lane}).
-define(WF_ERROR_UNKNOWN_LANE, {error, unknown_lane}).

-type index() :: non_neg_integer(). % scheduling is based on positions of elements (items, parallel_boxes, tasks)
                                    % to allow executions of tasks in chosen order
-type iteration_step() :: {workflow_cached_item:id(), iterator:iterator()}.
-type iteration_status() :: iteration_step() | undefined | ?WF_ERROR_ITERATION_FAILED.
-type task_status() :: ongoing | waiting_for_data_stream_finalization | ended.
-type state() :: #workflow_execution_state{}.
-type doc() :: datastore_doc:doc(state()).

-type execution_status() :: ?NOT_PREPARED | ?PREPARING | ?PREPARED_IN_ADVANCE | ?PREPARATION_FAILED |
    ?PREPARATION_CANCELLED | ?EXECUTING | ?EXECUTION_CANCELLED | ?EXECUTION_ENDED | ?WAITING_FOR_NEXT_LANE_PREPARATION_END.
-type next_lane_preparation_status() :: ?NOT_PREPARED | ?PREPARING | ?PREPARED_IN_ADVANCE | ?PREPARATION_FAILED.
% TODO VFS-7919 better type name
-type handler_execution_or_cached_async_result() ::
    workflow_engine:handler_execution_result() | workflow_cached_async_result:result_ref().

-type current_lane() :: #current_lane{}.
-type next_lane() :: #next_lane{}.
-type async_pools_slots_to_free() :: #{workflow_async_call_pool:id() => non_neg_integer()}.

%% @formatter:off
-type boxes_map() :: #{
    BoxIndex :: index() => #{
        TaskIndex :: index() => {workflow_engine:task_id(), workflow_engine:task_spec()}
    }
}.
%% @formatter:on

-type update_fun() :: datastore_doc:diff(state()).
-define(WF_ERROR_EXECUTION_ENDED(Description), {error, {execution_ended, Description}}).
-type no_items_error() :: ?WF_ERROR_NO_WAITING_ITEMS | ?WF_ERROR_EXECUTION_ENDED(#execution_ended{}).
% Type used to return additional information about document update procedure
% (see #workflow_execution_state.update_report)
-type update_report() :: ?LANE_DESIGNATED_FOR_PREPARATION | ?LANE_DESIGNATED_FOR_PREPARATION_IN_ADVANCE |
    #job_prepared_report{} | #items_processed_report{} | ?LANE_PREPARED_REPORT(next_lane()) |
    ?TASK_PROCESSED_REPORT(task_status()) | ?JOBS_EXPIRED_REPORT(async_pools_slots_to_free()) |
    ?LANE_READY_TO_BE_FINISHED_REPORT(workflow_engine:lane_id(), workflow_engine:execution_context()) |
    ?EXECUTION_CANCELLED_REPORT([workflow_cached_item:id()], [workflow_engine:task_id()]) |
    ?EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT([workflow_engine:task_id()]) | no_items_error().

-define(CALLBACKS_ON_CANCEL_SELECTOR, callbacks_on_cancel).
-define(CALLBACKS_ON_STREAMS_CANCEL_SELECTOR, callbacks_on_stream_cancel).
-define(CALLBACKS_ON_EMPTY_LANE_SELECTOR, callbacks_on_empty_lane).
-type callback_selector() :: workflow_jobs:job_identifier() | workflow_cached_item:id() |
    ?CALLBACKS_ON_CANCEL_SELECTOR | ?CALLBACKS_ON_STREAMS_CANCEL_SELECTOR | ?CALLBACKS_ON_EMPTY_LANE_SELECTOR.

-export_type([index/0, iteration_status/0, current_lane/0, next_lane/0, execution_status/0,
    next_lane_preparation_status/0, boxes_map/0, update_report/0, callback_selector/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(workflow_engine:execution_id(), workflow_engine:id(), workflow_handler:handler(), workflow_engine:execution_context(),
    workflow_engine:lane_id() | undefined, workflow_engine:lane_id() | undefined) -> ok | ?WF_ERROR_PREPARATION_FAILED.
init(_ExecutionId, _EngineId, _Handler, _Context, undefined, _NextLaneId) ->
    ?WF_ERROR_PREPARATION_FAILED; % FirstLaneId does not have to be defined only when execution is restarted from snapshot
init(ExecutionId, EngineId, Handler, Context, FirstLaneId, NextLaneId) ->
    workflow_iterator_snapshot:cleanup(ExecutionId),
    Doc = #document{key = ExecutionId, value = #workflow_execution_state{
        engine_id = EngineId,
        handler = Handler,
        initial_context = Context,
        current_lane = #current_lane{index = 1, id = FirstLaneId},
        next_lane = #next_lane{id = NextLaneId}
    }},
    {ok, _} = datastore_model:save(?CTX, Doc),
    ok.

-spec restart_from_snapshot(
    workflow_engine:execution_id(), workflow_engine:id(), workflow_handler:handler(), workflow_engine:execution_context(),
    workflow_engine:lane_id() | undefined, workflow_engine:lane_id() | undefined) -> ok | ?WF_ERROR_PREPARATION_FAILED.
restart_from_snapshot(ExecutionId, EngineId, Handler, Context, InitialLaneId, InitialNextLaneId) ->
    case workflow_iterator_snapshot:get(ExecutionId) of
        {ok, #workflow_iterator_snapshot{
            lane_index = LaneIndex, lane_id = LaneId,
            iterator = Iterator,
            next_lane_id = NextLaneId}
        } ->
            try
                % TODO VFS-7919 - maybe move restart callback on pool
                case workflow_engine:call_handler(ExecutionId, Context, Handler, restart_lane, [LaneId]) of
                    {ok, LaneSpec} ->
                        Doc = #document{key = ExecutionId, value = #workflow_execution_state{
                            engine_id = EngineId,
                            handler = Handler,
                            initial_context = Context,
                            current_lane = #current_lane{index = LaneIndex, id = LaneId},
                            next_lane = #next_lane{id = NextLaneId}}
                        },
                        {ok, _} = datastore_model:save(?CTX, Doc),
                        case finish_lane_preparation(ExecutionId, LaneSpec#{iterator => Iterator}) of
                            {ok, _, _, _} ->
                                ok;
                            ?WF_ERROR_LANE_ALREADY_PREPARED ->
                                ok;
                            ?WF_ERROR_PREPARATION_FAILED ->
                                cleanup(ExecutionId),
                                ?WF_ERROR_PREPARATION_FAILED
                        end;
                    error ->
                        ?WF_ERROR_PREPARATION_FAILED
                end
            catch
                Error:Reason:Stacktrace ->
                    ?error_stacktrace(
                        "Unexpected error restarting lane ~p (id ~p, execution ~p): ~p:~p",
                        [LaneIndex, LaneId, ExecutionId, Error, Reason],
                        Stacktrace
                    ),
                    ?WF_ERROR_PREPARATION_FAILED
            end;
        ?ERROR_NOT_FOUND ->
            init(ExecutionId, EngineId, Handler, Context, InitialLaneId, InitialNextLaneId)
    end.

-spec cancel(workflow_engine:execution_id()) -> ok.
cancel(ExecutionId) ->
    {ok, _} = update(ExecutionId, fun(State) ->
        handle_execution_cancel(State)
    end),
    ok.

-spec cleanup(workflow_engine:execution_id()) -> ok.
cleanup(ExecutionId) ->
    ok = datastore_model:delete(?CTX, ExecutionId).

-spec prepare_next_job(workflow_engine:execution_id()) ->
    {ok, workflow_engine:execution_spec()} | ?DEFER_EXECUTION | ?RETRY_EXECUTION | ?ERROR_NOT_FOUND | #execution_ended{} |
    ?PREPARE_LANE_EXECUTION(workflow_handler:handler(), workflow_engine:execution_context(),
        workflow_engine:lane_id(), workflow_engine:preparation_mode()).
prepare_next_job(ExecutionId) ->
    % TODO VFS-7787 - check quota for async jobs and do nothing if it is exceeded
    case prepare_next_job_for_current_lane(ExecutionId) of
        {ok, _} = OkAns ->
            OkAns;
        ?WF_ERROR_NO_WAITING_ITEMS ->
            ?DEFER_EXECUTION;
        ?PREPARE_LANE_EXECUTION(Handler, ExecutionContext, LaneId, PreparationMode) ->
            ?PREPARE_LANE_EXECUTION(Handler, ExecutionContext, LaneId, PreparationMode);
        ?WF_ERROR_EXECUTION_ENDED(ExecutionEnded) ->
            ExecutionEnded;
        ?WF_ERROR_LANE_EXECUTION_CANCELLED(Handler, CancelledLaneId, CancelledLaneContext, TaskIds) ->
            workflow_engine:call_handlers_for_cancelled_lane(
                ExecutionId, Handler, CancelledLaneContext, CancelledLaneId, TaskIds),
            {ok, _} = update(ExecutionId, fun(State) ->
                remove_pending_callback(State, ?CALLBACKS_ON_CANCEL_SELECTOR)
            end),
            % If next lane is being prepared in advance and preparation of this lane is finished before execution of
            % handlers_for_cancelled_lane, workflow_execution_ended handler will not be called - retry to call it
            ?RETRY_EXECUTION;
        ?EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT(Context, Handler, TaskIdsWithStreamsToFinish) ->
            lists:foreach(fun(TaskId) ->
                workflow_engine:call_handler(ExecutionId, Context, Handler, handle_task_results_processed_for_all_items, [TaskId])
            end, TaskIdsWithStreamsToFinish),
            {ok, _} = update(ExecutionId, fun(State) ->
                remove_pending_callback(State, ?CALLBACKS_ON_STREAMS_CANCEL_SELECTOR)
            end),
            ?RETRY_EXECUTION;
        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND % Race with execution deletion
    end.

-spec report_execution_status_update(
    workflow_engine:execution_id(),
    workflow_jobs:job_identifier(),
    workflow_engine:processing_stage(),
    workflow_engine:processing_result()
) -> workflow_engine:task_spec() | ?WF_ERROR_JOB_NOT_FOUND.
report_execution_status_update(ExecutionId, JobIdentifier, UpdateType, Ans) ->
    CachedAns = case UpdateType of
        ?ASYNC_CALL_ENDED -> workflow_cached_async_result:put(Ans);
        _ -> Ans
    end,

    {UpdatedDoc, ItemIdToReportError, ItemToReportError, TaskStatusesToReport} = case update(ExecutionId, fun(State) ->
        report_execution_status_update_internal(State, JobIdentifier, UpdateType, CachedAns)
    end) of
        {ok, Doc = #document{value = #workflow_execution_state{update_report = #items_processed_report{
            item_id_to_report_error = IdToReportError,
            item_id_to_snapshot = undefined,
            item_ids_to_delete = ItemIdsToDelete,
            task_statuses_to_report = ReportedTaskStatuses
        }}}} ->
            CachedItemToReportError = workflow_cached_item:get_item(IdToReportError),
            lists:foreach(fun workflow_cached_item:delete/1, ItemIdsToDelete),
            {Doc, IdToReportError, CachedItemToReportError, ReportedTaskStatuses};
        {ok, Doc = #document{value = #workflow_execution_state{
            next_lane = #next_lane{
                id = NextLaneId
            }, update_report = #items_processed_report{
                lane_index = LaneIndex,
                lane_id = LaneId,
                last_finished_item_index = ItemIndex,
                item_id_to_report_error = IdToReportError,
                item_id_to_snapshot = ItemIdToSnapshot,
                item_ids_to_delete = ItemIdsToDelete,
                task_statuses_to_report = ReportedTaskStatuses
            }
        }}} ->
            CachedItemToReportError = workflow_cached_item:get_item(IdToReportError),
            IteratorToSave = workflow_cached_item:get_iterator(ItemIdToSnapshot),
            workflow_iterator_snapshot:save(
                ExecutionId, LaneIndex, LaneId, ItemIndex, IteratorToSave, NextLaneId),
            lists:foreach(fun workflow_cached_item:delete/1, ItemIdsToDelete),
            {Doc, IdToReportError, CachedItemToReportError, ReportedTaskStatuses};
        {ok, Doc = #document{value = #workflow_execution_state{
            update_report = ?TASK_PROCESSED_REPORT(ReportedTaskStatus)
        }}} ->
            {Doc, undefined, undefined, [{JobIdentifier, ReportedTaskStatus}]};
        {ok, Doc} ->
            {Doc, undefined, undefined, []};
        ?WF_ERROR_JOB_NOT_FOUND ->
            ?debug("Result for not found job ~p of execution ~p", [JobIdentifier, ExecutionId]),
            {?WF_ERROR_JOB_NOT_FOUND, undefined, undefined, []};
        ?ERROR_NOT_FOUND ->
            ?debug("Result for job ~p of ended execution ~p", [JobIdentifier, ExecutionId]),
            {?WF_ERROR_JOB_NOT_FOUND, undefined, undefined, []}
    end,

    case UpdatedDoc of
        ?WF_ERROR_JOB_NOT_FOUND ->
            ?WF_ERROR_JOB_NOT_FOUND; % Error occurred - no task can be connected to result
        _ ->
            maybe_report_item_error(UpdatedDoc, ItemIdToReportError, ItemToReportError),
            % TODO VFS-8456 - maybe execute notification handlers also on pool?
            lists:foreach(fun({Identifier, TaskStatus}) ->
                maybe_notify_task_execution_ended(UpdatedDoc, Identifier, TaskStatus)
            end, TaskStatusesToReport),

            #document{value = #workflow_execution_state{
                current_lane = #current_lane{parallel_box_specs = BoxSpecs}
            }} = UpdatedDoc,
            {_TaskId, TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxSpecs),
            TaskSpec
    end.

-spec report_lane_execution_prepared(
    workflow_engine:execution_id(),
    workflow_engine:lane_id(),
    workflow_engine:preparation_mode(),
    workflow_handler:prepare_lane_result()
) -> ok.
report_lane_execution_prepared(ExecutionId, _LaneId, ?PREPARE_CURRENT, {ok, LaneSpec}) ->
    case finish_lane_preparation(ExecutionId, LaneSpec) of
        {ok, #current_lane{index = LaneIndex, id = LaneId}, IteratorToSave, NextLaneId} ->
            workflow_iterator_snapshot:save(ExecutionId, LaneIndex, LaneId, 0, IteratorToSave, NextLaneId);
        ?WF_ERROR_LANE_ALREADY_PREPARED -> ok;
        ?WF_ERROR_PREPARATION_FAILED -> ok
    end;
report_lane_execution_prepared(ExecutionId, LaneId, ?PREPARE_IN_ADVANCE, {ok, LaneSpec} = Ans) ->
    case update(ExecutionId, fun(State) -> finish_lane_preparation_in_advance(State, LaneId, LaneSpec) end) of
        {ok, _} -> ok;
        ?WF_ERROR_UNKNOWN_LANE -> ok;
        ?WF_ERROR_CURRENT_LANE -> report_lane_execution_prepared(ExecutionId, LaneId, ?PREPARE_CURRENT, Ans)
    end;
report_lane_execution_prepared(ExecutionId, LaneId, LaneType, error) ->
    case update(ExecutionId, fun(State) -> handle_lane_preparation_failure(State, LaneId, LaneType) end) of
        {ok, _} -> ok;
        ?WF_ERROR_UNKNOWN_LANE -> ok
    end.

-spec report_limit_reached_error(workflow_engine:execution_id(), workflow_jobs:job_identifier()) -> ok.
report_limit_reached_error(ExecutionId, JobIdentifier) ->
    {ok, _} = update(ExecutionId, fun(State) -> pause_job(State, JobIdentifier) end),
    ok.


-spec report_new_streamed_task_data(workflow_engine:execution_id(), workflow_engine:task_id(),
    workflow_engine:streamed_task_data()) -> {ok, workflow_engine:id()}.
report_new_streamed_task_data(ExecutionId, TaskId, TaskData) ->
    CachedTaskDataId = workflow_cached_task_data:put(TaskData),
    {ok, #document{value = #workflow_execution_state{engine_id = EngineId}}} =
        update(ExecutionId, fun(State = #workflow_execution_state{tasks_data_registry = TasksDataRegistry}) ->
            {ok, State#workflow_execution_state{
                tasks_data_registry = workflow_tasks_data_registry:put(TaskId, CachedTaskDataId, TasksDataRegistry)
            }}
        end),
    {ok, EngineId}.


-spec report_streamed_task_data_processed(workflow_engine:execution_id(), workflow_engine:task_id(),
    workflow_cached_task_data:id(), workflow_handler:handler_execution_result()) -> ok.
report_streamed_task_data_processed(ExecutionId, TaskId, CachedTaskDataId, HandlerExecutionResult) ->
    {ok, #document{value = #workflow_execution_state{
        update_report = ?TASK_PROCESSED_REPORT(TaskStatus),
        handler = Handler,
        current_lane = #current_lane{execution_context = Context}
    }}} =
        update(ExecutionId, fun
            (State) when HandlerExecutionResult =:= ok ->
                {ok, mark_task_data_processed(TaskId, CachedTaskDataId, State)};
            (State) ->
                {ok, mark_task_data_processed(TaskId, CachedTaskDataId, State#workflow_execution_state{
                    execution_status = ?EXECUTION_CANCELLED
                })}
        end),

    case TaskStatus of
        ended ->
            workflow_engine:call_handler(ExecutionId, Context, Handler, handle_task_execution_ended, [TaskId]),
            {ok, _} = update(ExecutionId, fun(State) -> remove_pending_callback(State, TaskId) end),
            ok;
        ongoing ->
            ok
    end.

-spec mark_all_streamed_task_data_received(workflow_engine:execution_id(), workflow_engine:task_id(),
    workflow_engine:stream_closing_result()) -> {ok, workflow_engine:id()}.
mark_all_streamed_task_data_received(ExecutionId, TaskId, success) ->
    {ok, #document{value = #workflow_execution_state{
        engine_id = EngineId,
        update_report = ?TASK_PROCESSED_REPORT(TaskStatus),
        handler = Handler,
        current_lane = #current_lane{execution_context = Context}
    }}} =
        update(ExecutionId, fun(State) -> {ok, mark_all_streamed_task_data_received_internal(TaskId, State)} end),

    case TaskStatus of
        ended ->
            workflow_engine:call_handler(ExecutionId, Context, Handler, handle_task_execution_ended, [TaskId]),
            {ok, _} = update(ExecutionId, fun(State) -> remove_pending_callback(State, TaskId) end),
            ok;
        waiting_for_data_stream_finalization ->
            ok
    end,

    {ok, EngineId};
mark_all_streamed_task_data_received(ExecutionId, TaskId, {failure, Error}) ->
    report_new_streamed_task_data(ExecutionId, TaskId, Error),
    % Error have been reported - mark with success
    mark_all_streamed_task_data_received(ExecutionId, TaskId, success).

-spec check_timeouts(workflow_engine:execution_id()) -> TimeoutAppeared :: boolean().
check_timeouts(ExecutionId) ->
    case update(ExecutionId, fun check_timeouts_internal/1) of
        {ok, #document{value = #workflow_execution_state{update_report = ?JOBS_EXPIRED_REPORT(AsyncPoolsSlotsToFree)}}} ->
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
        current_lane = #current_lane{parallel_box_specs = BoxSpecs, execution_context = Context}
    }}} = datastore_model:get(?CTX, ExecutionId),
    {TaskId, _TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxSpecs),
    {Handler, Context, TaskId}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec finish_lane_preparation(
    workflow_engine:execution_id(),
    workflow_engine:lane_spec()
) ->
    {ok, current_lane(), iterator:iterator(), workflow_engine:lane_id() | undefined} |
    ?WF_ERROR_PREPARATION_FAILED | ?WF_ERROR_LANE_ALREADY_PREPARED.
finish_lane_preparation(ExecutionId,
    #{
        parallel_boxes := Boxes,
        iterator := Iterator,
        execution_context := LaneExecutionContext
    } = LaneSpec
) ->
    case Boxes of
        [] ->
            % workflow_jobs require at least one parallel_boxes in lane
            ?error("No parallel boxes for lane ~p of execution id: ~p", [LaneSpec, ExecutionId]),
            {ok, _} = update(ExecutionId, fun(State) -> handle_lane_preparation_failure(State, undefined, ?PREPARE_CURRENT) end),
            ?WF_ERROR_PREPARATION_FAILED;
        _ ->
            BoxesMap = lists:foldl(fun({BoxIndex, BoxSpec}, BoxesAcc) ->
                Tasks = lists:foldl(fun({TaskIndex, {TaskId, TaskSpec}}, TaskAcc) ->
                    TaskAcc#{TaskIndex => {TaskId, TaskSpec}}
                end, #{}, lists_utils:enumerate(maps:to_list(BoxSpec))),
                BoxesAcc#{BoxIndex => Tasks}
            end, #{}, lists_utils:enumerate(Boxes)),

            NextIterationStep = get_next_iterator(LaneExecutionContext, Iterator, ExecutionId),
            FailureCountToCancel = maps:get(failure_count_to_cancel, LaneSpec, undefined),
            case update(ExecutionId, fun(State) ->
                finish_lane_preparation_internal(State, BoxesMap, LaneExecutionContext, NextIterationStep, FailureCountToCancel)
            end) of
                {ok, #document{value = #workflow_execution_state{
                    execution_status = ?EXECUTION_CANCELLED, current_lane = CurrentLane,
                    next_lane = #next_lane{id = NextLaneId}
                }}} ->
                    {ok, CurrentLane, Iterator, NextLaneId};
                {ok, #document{value = #workflow_execution_state{
                    current_lane = CurrentLane,
                    next_lane = #next_lane{id = NextLaneId}
                } = UpdatedState}} when NextIterationStep =:= undefined ->
                    call_callbacks_for_empty_lane(ExecutionId, UpdatedState),
                    {ok, CurrentLane, Iterator, NextLaneId};
                {ok, #document{value = #workflow_execution_state{
                    current_lane = CurrentLane,
                    next_lane = #next_lane{id = NextLaneId}
                }}} ->
                    {ok, CurrentLane, Iterator, NextLaneId};
                ?WF_ERROR_LANE_ALREADY_PREPARED ->
                    maybe_delete_prefetched_iteration_step(NextIterationStep),
                    ?WF_ERROR_LANE_ALREADY_PREPARED
            end
    end.

-spec call_callbacks_for_empty_lane(workflow_engine:execution_id(), state()) -> ok.
call_callbacks_for_empty_lane(ExecutionId, State = #workflow_execution_state{
    handler = Handler,
    current_lane = #current_lane{execution_context = Context, parallel_box_specs = BoxesMap}
}) ->
    TaskIds = get_task_ids(BoxesMap),
    TaskIdsWithStreams = get_task_ids_with_streams(State),
    workflow_engine:call_handle_task_results_processed_for_all_items_for_all_tasks(
        ExecutionId, Handler, Context, TaskIdsWithStreams),
    workflow_engine:call_handle_task_execution_ended_for_all_tasks(ExecutionId, Handler, Context,
        TaskIds -- TaskIdsWithStreams),

    {ok, _} = update(ExecutionId, fun(StateToUpdate) ->
        remove_pending_callback(StateToUpdate, ?CALLBACKS_ON_EMPTY_LANE_SELECTOR)
    end),
    ok.

-spec get_task_ids(boxes_map()) -> [workflow_engine:task_id()].
get_task_ids(BoxesMap) ->
    lists:flatten(lists:map(fun(BoxIndex) ->
        BoxSpec = maps:get(BoxIndex, BoxesMap),
        lists:map(fun(TaskIndex) ->
            {TaskId, _TaskSpec} = maps:get(TaskIndex, BoxSpec),
            TaskId
        end, lists:seq(1, maps:size(BoxSpec)))
    end, lists:seq(1, maps:size(BoxesMap)))).

-spec prepare_next_job_for_current_lane(workflow_engine:execution_id()) ->
    {ok, workflow_engine:execution_spec()} | no_items_error() |
    ?PREPARE_LANE_EXECUTION(
        workflow_handler:handler(), workflow_engine:execution_context(),
        workflow_engine:lane_id(), workflow_engine:preparation_mode()
    ) | ?WF_ERROR_LANE_EXECUTION_CANCELLED(
        workflow_handler:handler(), workflow_engine:lane_id(),
        workflow_engine:execution_context(), [workflow_engine:task_id()]
    ) | ?EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT(
        workflow_handler:handler(), workflow_engine:execution_context(), [workflow_engine:task_id()]
    ) | ?ERROR_NOT_FOUND.
prepare_next_job_for_current_lane(ExecutionId) ->
    case update(ExecutionId, fun prepare_next_waiting_job/1) of
        {ok, #document{value = State}} ->
            handle_state_update_after_job_preparation(ExecutionId, State);
        ?WF_ERROR_NO_CACHED_ITEMS(LaneIndex, ItemIndex, IterationStep, Context) ->
            prepare_next_job_using_iterator(ExecutionId, ItemIndex, IterationStep, LaneIndex, Context);
        {error, _} = Error ->
            Error
    end.

-spec prepare_next_job_using_iterator(workflow_engine:execution_id(), index(), iteration_status(),
    index(), workflow_engine:execution_context()) -> {ok, workflow_engine:execution_spec()} | no_items_error() |
    ?WF_ERROR_LANE_EXECUTION_CANCELLED(
        workflow_handler:handler(), workflow_engine:lane_id(),
        workflow_engine:execution_context(), [workflow_engine:task_id()]
    ) | ?EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT(
        workflow_handler:handler(), workflow_engine:execution_context(), [workflow_engine:task_id()]
    ) | ?ERROR_NOT_FOUND.
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
        {ok, #document{value = State}} ->
            handle_state_update_after_job_preparation(ExecutionId, State);
        ?WF_ERROR_LANE_CHANGED ->
            maybe_delete_prefetched_iteration_step(NextIterationStep),
            prepare_next_job_for_current_lane(ExecutionId);
        ?WF_ERROR_RACE_CONDITION ->
            maybe_delete_prefetched_iteration_step(NextIterationStep),
            prepare_next_job_for_current_lane(ExecutionId);
        ?ERROR_NOT_FOUND -> % Race with execution deletion
            maybe_delete_prefetched_iteration_step(NextIterationStep),
            ?ERROR_NOT_FOUND
    end.


-spec handle_state_update_after_job_preparation(workflow_engine:execution_id(), state()) ->
    {ok, workflow_engine:execution_spec()} | no_items_error() |
    ?PREPARE_LANE_EXECUTION(
        workflow_handler:handler(), workflow_engine:execution_context(),
        workflow_engine:lane_id(), workflow_engine:preparation_mode()
    ) | ?WF_ERROR_LANE_EXECUTION_CANCELLED(
        workflow_handler:handler(), workflow_engine:lane_id(),
        workflow_engine:execution_context(), [workflow_engine:task_id()]
    ) | ?EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT(
        workflow_handler:handler(), workflow_engine:execution_context(), [workflow_engine:task_id()]
    ) | ?ERROR_NOT_FOUND.
handle_state_update_after_job_preparation(_ExecutionId, #workflow_execution_state{
    update_report = #job_prepared_report{
        job_identifier = JobIdentifier,
        task_id = TaskId,
        task_spec = TaskSpec,
        subject_id = SubjectId
    },
    handler = Handler,
    current_lane = #current_lane{execution_context = LaneContext}
}) ->
    {ok, #execution_spec{
        handler = Handler,
        context = LaneContext,
        task_id = TaskId,
        task_spec = TaskSpec,
        subject_id = SubjectId,
        job_identifier = JobIdentifier
    }};
handle_state_update_after_job_preparation(_ExecutionId, #workflow_execution_state{
    update_report = ?LANE_DESIGNATED_FOR_PREPARATION,
    handler = Handler,
    initial_context = ExecutionContext,
    current_lane = #current_lane{id = LaneId}}
) ->
    ?PREPARE_LANE_EXECUTION(Handler, ExecutionContext, LaneId, ?PREPARE_CURRENT);
handle_state_update_after_job_preparation(_ExecutionId, #workflow_execution_state{
    update_report = ?LANE_DESIGNATED_FOR_PREPARATION_IN_ADVANCE,
    handler = Handler,
    initial_context = ExecutionContext,
    next_lane = #next_lane{id = LaneId}
}) ->
    ?PREPARE_LANE_EXECUTION(Handler, ExecutionContext, LaneId, ?PREPARE_IN_ADVANCE);
handle_state_update_after_job_preparation(ExecutionId, #workflow_execution_state{
    update_report = ?LANE_READY_TO_BE_FINISHED_REPORT(FinishedLaneId, LaneContext),
    handler = Handler,
    initial_context = ExecutionContext,
    next_lane_preparation_status = NextLaneStatus
}) ->
    case workflow_engine:call_handler(ExecutionId, LaneContext, Handler, handle_lane_execution_ended, [FinishedLaneId]) of
        ?CONTINUE(NextLaneId, LaneIdToBePreparedInAdvance) ->
            case update(ExecutionId, fun(State) -> set_current_lane(State, NextLaneId, LaneIdToBePreparedInAdvance) end) of
                {ok, #document{value = #workflow_execution_state{
                    execution_status = ?PREPARED_IN_ADVANCE,
                    update_report = ?LANE_PREPARED_REPORT(#next_lane{
                        id = LaneId,
                        lane_spec = LaneSpec
                    })
                }}} ->
                    report_lane_execution_prepared(ExecutionId, LaneId, ?PREPARE_CURRENT, {ok, LaneSpec}),
                    prepare_next_job_for_current_lane(ExecutionId);
                {ok, #document{value = #workflow_execution_state{
                    execution_status = ?PREPARING,
                    update_report = ?LANE_DESIGNATED_FOR_PREPARATION
                }}} ->
                    ?PREPARE_LANE_EXECUTION(Handler, ExecutionContext, NextLaneId, ?PREPARE_CURRENT);
                {ok, #document{value = #workflow_execution_state{
                    execution_status = ?PREPARATION_FAILED
                }}} ->
                    ?WF_ERROR_EXECUTION_ENDED(#execution_ended{handler = Handler, context = ExecutionContext});
                {ok, _} ->
                    ?WF_ERROR_NO_WAITING_ITEMS
            end;
        % Other possible answers are ?END_EXECUTION or error - error is logged by workflow_engine:call_handler/5 function
        _ when NextLaneStatus =:= ?PREPARING ->
            case update(ExecutionId, fun maybe_wait_for_preparation_in_advace/1) of
                ?WF_ERROR_LANE_ALREADY_PREPARED ->
                    ?WF_ERROR_EXECUTION_ENDED(#execution_ended{handler = Handler, context = ExecutionContext});
                {ok, _} ->
                    ?WF_ERROR_NO_WAITING_ITEMS
            end;
        _ ->
            ?WF_ERROR_EXECUTION_ENDED(#execution_ended{handler = Handler, context = ExecutionContext})
    end;
handle_state_update_after_job_preparation(_ExecutionId, #workflow_execution_state{
    update_report = ?EXECUTION_CANCELLED_REPORT(ItemIdsToDelete, TaskIdsToFinish),
    current_lane = #current_lane{id = LaneId, execution_context = LaneContext},
    execution_status = ExecutionStatus,
    handler = Handler,
    initial_context = ExecutionContext,
    prefetched_iteration_step = PrefetchedIterationStep
}) ->
    lists:foreach(fun workflow_cached_item:delete/1, ItemIdsToDelete),
    case PrefetchedIterationStep of
        undefined -> ok;
        ?WF_ERROR_ITERATION_FAILED -> ok;
        {PrefetchedItemId, _} -> workflow_cached_item:delete(PrefetchedItemId)
    end,
    % TODO VFS-7787 - test cancel during lane_execution_finished callback execution
    case ExecutionStatus of
        ?WAITING_FOR_NEXT_LANE_PREPARATION_END ->
            ?WF_ERROR_LANE_EXECUTION_CANCELLED(Handler, LaneId, LaneContext, TaskIdsToFinish);
        _ ->
            ?WF_ERROR_EXECUTION_ENDED(#execution_ended{handler = Handler, context = ExecutionContext,
                reason = ?EXECUTION_CANCELLED, callbacks_data = {LaneId, LaneContext, TaskIdsToFinish}})
    end;
handle_state_update_after_job_preparation(_ExecutionId, #workflow_execution_state{
    update_report = ?EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT(TaskIdsWithStreamsToFinish),
    current_lane = #current_lane{execution_context = LaneContext},
    handler = Handler
}) ->
    ?EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT(LaneContext, Handler, TaskIdsWithStreamsToFinish);
handle_state_update_after_job_preparation(_ExecutionId, #workflow_execution_state{
    update_report = {error, _} = UpdateReport
}) ->
    UpdateReport.

-spec maybe_report_item_error(doc(), workflow_cached_item:id() | undefined, iterator:item() | undefined) -> ok.
maybe_report_item_error(_Doc, undefined = _ItemIdToReportError, undefined = _ItemToReportError) ->
    ok;
maybe_report_item_error(#document{key = ExecutionId, value = #workflow_execution_state{
    handler = Handler,
    current_lane = #current_lane{execution_context = Context}
}}, ItemIdToReportError, ItemToReportError) ->
    workflow_engine:call_handler(ExecutionId, Context, Handler, report_item_error, [ItemToReportError]),
    {ok, _} = update(ExecutionId, fun(State) -> remove_pending_callback(State, ItemIdToReportError) end),
    ok.

-spec maybe_notify_task_execution_ended(doc(), workflow_jobs:job_identifier(), task_status()) -> ok.
maybe_notify_task_execution_ended(_Doc, _JobIdentifier, ongoing) ->
    ok;
maybe_notify_task_execution_ended(#document{key = ExecutionId, value = #workflow_execution_state{
    handler = Handler,
    current_lane = #current_lane{parallel_box_specs = BoxSpecs, execution_context = Context}
}}, JobIdentifier, waiting_for_data_stream_finalization) ->
    {TaskId, _TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxSpecs),
    workflow_engine:call_handler(ExecutionId, Context, Handler, handle_task_results_processed_for_all_items, [TaskId]),
    {ok, _} = update(ExecutionId, fun(State) -> remove_pending_callback(State, JobIdentifier) end),
    ok;
maybe_notify_task_execution_ended(#document{key = ExecutionId, value = #workflow_execution_state{
    handler = Handler,
    current_lane = #current_lane{parallel_box_specs = BoxSpecs, execution_context = Context}
}}, JobIdentifier, ended) ->
    {TaskId, _TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxSpecs),
    workflow_engine:call_handler(ExecutionId, Context, Handler, handle_task_execution_ended, [TaskId]),
    {ok, _} = update(ExecutionId, fun(State) -> remove_pending_callback(State, JobIdentifier) end),
    ok.

-spec update(workflow_engine:execution_id(), update_fun()) -> {ok, doc()} | {error, term()}.
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
        Error:Reason:Stacktrace ->
            ?error_stacktrace(
                "Unexpected error getting next iterator (execution ~p): ~p:~p",
                [ExecutionId, Error, Reason],
                Stacktrace
            ),
            ?WF_ERROR_ITERATION_FAILED
    end.


-spec maybe_delete_prefetched_iteration_step(iteration_status()) -> ok.
maybe_delete_prefetched_iteration_step(undefined) ->
    ok;
maybe_delete_prefetched_iteration_step(?WF_ERROR_ITERATION_FAILED) ->
    ok;
maybe_delete_prefetched_iteration_step({ItemId, _}) ->
    workflow_cached_item:delete(ItemId).


%%%===================================================================
%%% Functions updating record
%%%===================================================================

-spec set_current_lane(state(), workflow_engine:lane_id(), workflow_engine:lane_id() | undefined) -> {ok, state()}.
set_current_lane(#workflow_execution_state{
    current_lane = CurrentLane,
    next_lane = #next_lane{id = NextLaneId} = NextLane,
    next_lane_preparation_status = NextLaneStatus
} = State, NewLaneId, LaneIdToBePreparedInAdvance) ->
    {NewExecutionStatus, NewUpdateReport} = case NewLaneId of
        NextLaneId when NextLaneStatus == ?PREPARED_IN_ADVANCE ->
            {NextLaneStatus, ?LANE_PREPARED_REPORT(NextLane)};
        NextLaneId when NextLaneStatus =/= ?NOT_PREPARED ->
            {NextLaneStatus, undefined};
        _ ->
            {?PREPARING, ?LANE_DESIGNATED_FOR_PREPARATION}
    end,

    {NewNextLane, NewNextLaneStatus} = case LaneIdToBePreparedInAdvance of
        NextLaneId -> {NextLane, NextLaneStatus};
        _ -> {#next_lane{id = LaneIdToBePreparedInAdvance}, ?NOT_PREPARED}
    end,

    {ok, State#workflow_execution_state{
        current_lane = CurrentLane#current_lane{id = NewLaneId},
        execution_status = NewExecutionStatus,
        failed_job_count = 0,
        next_lane_preparation_status = NewNextLaneStatus,
        next_lane = NewNextLane,
        update_report = NewUpdateReport
    }}.

-spec handle_execution_cancel(state()) -> {ok, state()}.
handle_execution_cancel(#workflow_execution_state{execution_status = ?PREPARING} = State) ->
    {ok, State#workflow_execution_state{execution_status = ?PREPARATION_CANCELLED}};
handle_execution_cancel(#workflow_execution_state{execution_status = ?PREPARATION_FAILED} = State) ->
    % TODO VFS-7787 Return error to prevent document update
    {ok, State};
handle_execution_cancel(State) ->
    {ok, State#workflow_execution_state{execution_status = ?EXECUTION_CANCELLED}}.

-spec handle_lane_preparation_failure(state(), workflow_engine:lane_id() | undefined,
    workflow_engine:preparation_mode()) -> {ok, state()} | ?WF_ERROR_UNKNOWN_LANE.
handle_lane_preparation_failure(
    #workflow_execution_state{
        next_lane_preparation_status = ?PREPARING
    } = State, _LaneId, ?PREPARE_CURRENT) ->
    {ok, State#workflow_execution_state{execution_status = ?WAITING_FOR_NEXT_LANE_PREPARATION_END}};
handle_lane_preparation_failure(State, _LaneId, ?PREPARE_CURRENT) ->
    {ok, State#workflow_execution_state{execution_status = ?PREPARATION_FAILED}};
handle_lane_preparation_failure(
    #workflow_execution_state{
        execution_status = ?WAITING_FOR_NEXT_LANE_PREPARATION_END,
        next_lane = #next_lane{
            id = LaneId
        }
    } = State, LaneId, _LaneType) ->
    {ok, State#workflow_execution_state{
        execution_status = ?EXECUTION_ENDED,
        next_lane_preparation_status = ?PREPARATION_FAILED
    }};
handle_lane_preparation_failure(
    #workflow_execution_state{next_lane = #next_lane{
        id = LaneId
    }} = State, LaneId, ?PREPARE_IN_ADVANCE) ->
    {ok, State#workflow_execution_state{next_lane_preparation_status = ?PREPARATION_FAILED}};
handle_lane_preparation_failure(
    #workflow_execution_state{current_lane = #current_lane{
        id = LaneId % Previous lane is finished and lane prepared in advanced is now current_lane
    }} = State, LaneId, ?PREPARE_IN_ADVANCE) ->
    {ok, State#workflow_execution_state{execution_status = ?PREPARATION_FAILED}};
handle_lane_preparation_failure(_State, _LaneId, ?PREPARE_IN_ADVANCE) ->
    ?WF_ERROR_UNKNOWN_LANE. % Previous lane is finished and other lane is set to be executed - ignore prepared lane

-spec finish_lane_preparation_internal(state(), boxes_map(), workflow_engine:execution_context(),
    iteration_status(), non_neg_integer()) -> {ok, state()} | ?WF_ERROR_LANE_ALREADY_PREPARED.
finish_lane_preparation_internal(
    #workflow_execution_state{
        execution_status = ?PREPARATION_CANCELLED,
        current_lane = #current_lane{parallel_box_specs = undefined} = CurrentLane
    } = State, BoxesMap, LaneExecutionContext, PrefetchedIterationStep, _FailureCountToCancel) ->
    {ok, State#workflow_execution_state{
        execution_status = ?EXECUTION_CANCELLED,
        current_lane = CurrentLane#current_lane{execution_context = LaneExecutionContext, parallel_box_specs = BoxesMap},
        iteration_state = workflow_iteration_state:init(),
        prefetched_iteration_step = PrefetchedIterationStep,
        jobs = workflow_jobs:init(),
        tasks_data_registry = workflow_tasks_data_registry:empty()
    }};
finish_lane_preparation_internal(
    #workflow_execution_state{
        current_lane = #current_lane{parallel_box_specs = undefined} = CurrentLane
    } = State, BoxesMap, LaneExecutionContext, PrefetchedIterationStep, FailureCountToCancel) ->
    UpdatedCurrentLane = CurrentLane#current_lane{
        execution_context = LaneExecutionContext,
        parallel_box_count = maps:size(BoxesMap),
        parallel_box_specs = BoxesMap,
        failure_count_to_cancel = FailureCountToCancel
    },
    PendingCallbacks = case PrefetchedIterationStep of
        undefined -> [?CALLBACKS_ON_EMPTY_LANE_SELECTOR];
        _ -> []
    end,

    {ok, State#workflow_execution_state{
        execution_status = ?EXECUTING,
        current_lane = UpdatedCurrentLane,
        iteration_state = workflow_iteration_state:init(),
        prefetched_iteration_step = PrefetchedIterationStep,
        jobs = workflow_jobs:init(),
        tasks_data_registry = workflow_tasks_data_registry:empty(),
        pending_callbacks = PendingCallbacks
    }};
finish_lane_preparation_internal(_State, _BoxesMap, _LaneExecutionContext,
    _PrefetchedIterationStep, _FailureCountToCancel) ->
    ?WF_ERROR_LANE_ALREADY_PREPARED.

-spec finish_lane_preparation_in_advance(state(), workflow_engine:lane_id(), workflow_engine:lane_spec()) ->
    {ok, state()} | ?WF_ERROR_CURRENT_LANE | ?WF_ERROR_UNKNOWN_LANE.
finish_lane_preparation_in_advance(
    #workflow_execution_state{
        execution_status = ?WAITING_FOR_NEXT_LANE_PREPARATION_END,
        next_lane = #next_lane{
            id = LaneId
        } = Lane
    } = State, LaneId, LaneSpec) ->
    {ok, State#workflow_execution_state{
        execution_status = ?EXECUTION_ENDED,
        next_lane_preparation_status = ?PREPARED_IN_ADVANCE,
        next_lane = Lane#next_lane{
            lane_spec = LaneSpec
        }
    }};
finish_lane_preparation_in_advance(
    #workflow_execution_state{next_lane = #next_lane{
        id = LaneId
    } = Lane} = State, LaneId, LaneSpec) ->
    {ok, State#workflow_execution_state{
        next_lane_preparation_status = ?PREPARED_IN_ADVANCE,
        next_lane = Lane#next_lane{
            lane_spec = LaneSpec
        }
    }};
finish_lane_preparation_in_advance(
    #workflow_execution_state{current_lane = #current_lane{
        id = LaneId % Previous lane is finished and lane prepared in advanced is now current_lane
    }},
    LaneId, _LaneSpec) ->
    ?WF_ERROR_CURRENT_LANE;
finish_lane_preparation_in_advance(_State, _LaneId, _LaneSpec) ->
    ?WF_ERROR_UNKNOWN_LANE. % Previous lane is finished and other lane is set to be executed - ignore prepared lane

-spec maybe_wait_for_preparation_in_advace(state()) -> {ok, state()} | ?WF_ERROR_LANE_ALREADY_PREPARED.
maybe_wait_for_preparation_in_advace(#workflow_execution_state{
    next_lane_preparation_status = ?PREPARING
} = State) ->
    {ok, State#workflow_execution_state{execution_status = ?WAITING_FOR_NEXT_LANE_PREPARATION_END}};
maybe_wait_for_preparation_in_advace(_State) ->
    ?WF_ERROR_LANE_ALREADY_PREPARED.

-spec handle_next_iteration_step(
    state(),
    index(),
    index(),
    iteration_status(),
    index()
) ->
    {ok, state()} | ?WF_ERROR_RACE_CONDITION | ?WF_ERROR_LANE_CHANGED.
handle_next_iteration_step(#workflow_execution_state{
    execution_status = ?EXECUTION_CANCELLED
}, _LaneIndex, _PrevItemIndex, _NextIterationStep, _ParallelBoxToStart) ->
    ?WF_ERROR_RACE_CONDITION;
handle_next_iteration_step(State = #workflow_execution_state{
    jobs = Jobs,
    iteration_state = IterationState,
    prefetched_iteration_step = PrefetchedIterationStep,
    current_lane = #current_lane{index = LaneIndex, parallel_box_specs = BoxSpecs}
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
                        Jobs, NewItemIndex, ParallelBoxToStart, BoxSpecs),
                    FinalJobs = case NextIterationStep of
                        undefined -> workflow_jobs:build_tasks_tree(NewJobs);
                        _ -> NewJobs
                    end,
                    {TaskId, TaskSpec} = workflow_jobs:get_task_details(ToStart, BoxSpecs),
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
check_timeouts_internal(#workflow_execution_state{current_lane = #current_lane{parallel_box_specs = undefined}}) ->
    ?WF_ERROR_NOTHING_CHANGED;
check_timeouts_internal(State = #workflow_execution_state{
    jobs = Jobs,
    current_lane = #current_lane{parallel_box_specs = BoxSpecs}
}) ->
    % TODO VFS-7788 - check if task is expired (do it outside tp process)
    {?WF_ERROR_NO_TIMEOUTS_UPDATED, ExpiredJobsIdentifiers} = workflow_jobs:check_timeouts(Jobs),

    case length(ExpiredJobsIdentifiers) of
        0 ->
            ?WF_ERROR_NOTHING_CHANGED;
        _ ->
            {FinalState, AsyncPoolsSlotsToFree} = lists:foldl(fun(JobIdentifier, {TmpState, TmpAsyncPoolsSlotsToFree}) ->
                {ok, NewTmpState} = report_execution_status_update_internal(
                    TmpState, JobIdentifier, ?ASYNC_CALL_ENDED, ?ERROR_TIMEOUT),

                {_, TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxSpecs),
                NewTmpAsyncPoolsSlotsToFree = lists:foldl(fun(AsyncPoolId, InternalTmpAsyncPoolsChange) ->
                    TmpChange = maps:get(AsyncPoolId, InternalTmpAsyncPoolsChange, 0),
                    InternalTmpAsyncPoolsChange#{AsyncPoolId => TmpChange + 1}
                end, TmpAsyncPoolsSlotsToFree, workflow_engine:get_async_call_pools(TaskSpec)),
                {NewTmpState, NewTmpAsyncPoolsSlotsToFree}
            end, {State, #{}}, ExpiredJobsIdentifiers),

            {ok, FinalState#workflow_execution_state{update_report = ?JOBS_EXPIRED_REPORT(AsyncPoolsSlotsToFree)}}
    end.

-spec report_execution_status_update_internal(
    state(),
    workflow_jobs:job_identifier(),
    workflow_engine:processing_stage(),
    handler_execution_or_cached_async_result()
) -> {ok, state()} | ?WF_ERROR_JOB_NOT_FOUND.
report_execution_status_update_internal(State = #workflow_execution_state{
    engine_id = EngineId,
    jobs = Jobs
}, JobIdentifier, ?ASYNC_CALL_STARTED, {ok, KeepaliveTimeout}) ->
    {ok, State#workflow_execution_state{
        jobs = workflow_jobs:register_async_call(EngineId, Jobs, JobIdentifier, KeepaliveTimeout)}};
report_execution_status_update_internal(State = #workflow_execution_state{
    jobs = Jobs
}, JobIdentifier, ?ASYNC_CALL_ENDED, CachedResultId) ->
    case workflow_jobs:register_async_job_finish(Jobs, JobIdentifier, CachedResultId) of
        {ok, NewJobs} -> {ok, State#workflow_execution_state{jobs = NewJobs}};
        ?WF_ERROR_JOB_NOT_FOUND -> ?WF_ERROR_JOB_NOT_FOUND
    end;
report_execution_status_update_internal(State, JobIdentifier, _UpdateType, Ans) ->
    report_job_finish(State, JobIdentifier, Ans).

-spec prepare_next_waiting_job(state()) ->
    {ok, state()} | no_items_error() |
    ?WF_ERROR_NO_CACHED_ITEMS(index(), index(), iteration_status(), workflow_engine:execution_context()).
prepare_next_waiting_job(State = #workflow_execution_state{
    execution_status = ?NOT_PREPARED
}) ->
    {ok, State#workflow_execution_state{execution_status = ?PREPARING, update_report = ?LANE_DESIGNATED_FOR_PREPARATION}};
prepare_next_waiting_job(State = #workflow_execution_state{
    execution_status = Status,
    next_lane_preparation_status = ?NOT_PREPARED,
    next_lane = #next_lane{id = LaneId}
}) when LaneId =/= undefined andalso (Status =:= ?EXECUTING orelse Status =:= ?PREPARING) ->
    {ok, State#workflow_execution_state{
        next_lane_preparation_status = ?PREPARING,
        update_report = ?LANE_DESIGNATED_FOR_PREPARATION_IN_ADVANCE
    }};
prepare_next_waiting_job(#workflow_execution_state{
    execution_status = ?EXECUTING,
    failed_job_count = FailedJobCount,
    current_lane = #current_lane{failure_count_to_cancel = FailureCountToCancel}
} = State) when FailureCountToCancel =/= undefined andalso FailedJobCount >= FailureCountToCancel ->
    prepare_next_waiting_job(State#workflow_execution_state{execution_status = ?EXECUTION_CANCELLED});
prepare_next_waiting_job(State = #workflow_execution_state{
    execution_status = ?EXECUTING,
    jobs = Jobs,
    iteration_state = IterationState,
    prefetched_iteration_step = PrefetchedIterationStep,
    current_lane = #current_lane{index = LaneIndex, parallel_box_specs = BoxSpecs, execution_context = Context}
}) ->
    case prepare_next_streamed_task_data(State) of
        {ok, UpdatedState} ->
            {ok, UpdatedState};
        ?ERROR_NOT_FOUND ->
            case workflow_jobs:prepare_next_waiting_job(Jobs) of
                {ok, JobIdentifier, NewJobs} ->
                    {TaskId, TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxSpecs),
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
            end
    end;
prepare_next_waiting_job(#workflow_execution_state{
    execution_status = ?PREPARING
}) ->
    ?WF_ERROR_NO_WAITING_ITEMS;
prepare_next_waiting_job(#workflow_execution_state{
    execution_status = ?PREPARED_IN_ADVANCE
}) ->
    ?WF_ERROR_NO_WAITING_ITEMS;
prepare_next_waiting_job(#workflow_execution_state{
    execution_status = ?PREPARATION_CANCELLED
}) ->
    ?WF_ERROR_NO_WAITING_ITEMS;
prepare_next_waiting_job(#workflow_execution_state{
    execution_status = ?WAITING_FOR_NEXT_LANE_PREPARATION_END
}) ->
    ?WF_ERROR_NO_WAITING_ITEMS;
prepare_next_waiting_job(#workflow_execution_state{
    execution_status = ?EXECUTION_ENDED,
    pending_callbacks = [],
    current_lane = #current_lane{id = undefined},
    handler = Handler,
    initial_context = ExecutionContext
}) ->
    ?WF_ERROR_EXECUTION_ENDED(#execution_ended{handler = Handler, context = ExecutionContext});
prepare_next_waiting_job(#workflow_execution_state{
    execution_status = ?EXECUTION_ENDED,
    pending_callbacks = [],
    handler = Handler,
    initial_context = ExecutionContext
}) ->
    ?WF_ERROR_EXECUTION_ENDED(#execution_ended{
        handler = Handler, context = ExecutionContext, reason = ?EXECUTION_CANCELLED});
prepare_next_waiting_job(#workflow_execution_state{
    execution_status = ?EXECUTION_ENDED
}) ->
    ?WF_ERROR_NO_WAITING_ITEMS;
prepare_next_waiting_job(#workflow_execution_state{
    execution_status = ?PREPARATION_FAILED,
    handler = Handler,
    initial_context = Context
}) ->
    ?WF_ERROR_EXECUTION_ENDED(#execution_ended{handler = Handler, context = Context});
prepare_next_waiting_job(State = #workflow_execution_state{
    execution_status = ?EXECUTION_CANCELLED,
    jobs = Jobs,
    iteration_state = IterationState,
    current_lane = #current_lane{parallel_box_specs = BoxSpecs}
}) ->
    case workflow_jobs:prepare_next_waiting_result(Jobs) of
        {{ok, JobIdentifier}, NewJobs} ->
            {TaskId, TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxSpecs),
            % Use old `Jobs` as subject is no longer present in `NewJobs` (job is not waiting anymore)
            SubjectId = workflow_jobs:get_subject_id(JobIdentifier, Jobs, IterationState),
            {ok, State#workflow_execution_state{
                update_report = #job_prepared_report{job_identifier = JobIdentifier,
                    task_id = TaskId, task_spec = TaskSpec, subject_id = SubjectId},
                jobs = NewJobs
            }};
        {?ERROR_NOT_FOUND, NewJobs} ->
            verify_ongoing_when_execution_is_cancelled(?ERROR_NOT_FOUND, NewJobs, State);
        ?WF_ERROR_ITERATION_FINISHED ->
            verify_ongoing_when_execution_is_cancelled(?WF_ERROR_ITERATION_FINISHED, Jobs, State)
    end.

-spec verify_ongoing_when_execution_is_cancelled(?ERROR_NOT_FOUND | ?WF_ERROR_ITERATION_FINISHED,
    workflow_jobs:jobs(), state()) -> {ok, state()} | ?WF_ERROR_NO_WAITING_ITEMS.
verify_ongoing_when_execution_is_cancelled(Error, NewJobs, State = #workflow_execution_state{
    pending_callbacks = PendingCallbacks
}) ->
    case {workflow_jobs:has_ongoing_jobs(NewJobs) orelse PendingCallbacks =/= [], Error} of
        {true, ?ERROR_NOT_FOUND} ->
            {ok, State#workflow_execution_state{jobs = NewJobs, update_report = ?WF_ERROR_NO_WAITING_ITEMS}};
        {true, ?WF_ERROR_ITERATION_FINISHED} ->
            ?WF_ERROR_NO_WAITING_ITEMS;
        _ ->
            {TaskIdsWithStreamsToNotify, TaskIdsWithoutStreamsToNotify} = get_task_ids_to_notify_on_cancel(State),
            case claim_execution_of_cancellation_procedures(State#workflow_execution_state{jobs = NewJobs}, TaskIdsWithStreamsToNotify) of
                {ok, UpdatedState} ->
                    {ok, UpdatedState};
                nothing_to_claim ->
                    verify_streams_when_execution_is_cancelled(
                        State#workflow_execution_state{jobs = NewJobs}, TaskIdsWithoutStreamsToNotify)
            end
    end.

-spec verify_streams_when_execution_is_cancelled(state(), [workflow_engine:task_id()]) ->
    {ok, state()} | ?WF_ERROR_NO_WAITING_ITEMS.
verify_streams_when_execution_is_cancelled(State = #workflow_execution_state{
    iteration_state = IterationState,
    next_lane_preparation_status = NextLaneStatus,
    pending_callbacks = PendingCallbacks
}, TaskIdsToNotify) ->
    case prepare_next_streamed_task_data(State) of
        {ok, UpdatedState} ->
            {ok, UpdatedState};
        ?ERROR_NOT_FOUND ->
            case are_all_data_streams_finalized(State) of
                true when NextLaneStatus =:= ?PREPARING ->
                    ItemIds = workflow_iteration_state:get_all_item_ids(IterationState),
                    {ok, State#workflow_execution_state{
                        iteration_state = workflow_iteration_state:init(),
                        execution_status = ?WAITING_FOR_NEXT_LANE_PREPARATION_END,
                        update_report = ?EXECUTION_CANCELLED_REPORT(ItemIds, TaskIdsToNotify),
                        pending_callbacks = [?CALLBACKS_ON_CANCEL_SELECTOR | PendingCallbacks]
                    }};
                true ->
                    ItemIds = workflow_iteration_state:get_all_item_ids(IterationState),
                    {ok, State#workflow_execution_state{
                        iteration_state = workflow_iteration_state:init(),
                        update_report = ?EXECUTION_CANCELLED_REPORT(ItemIds, TaskIdsToNotify),
                        pending_callbacks = [?CALLBACKS_ON_CANCEL_SELECTOR | PendingCallbacks]}};
                false ->
                    ?WF_ERROR_NO_WAITING_ITEMS
            end
    end.

-spec claim_execution_of_cancellation_procedures(state(), [workflow_engine:task_id()]) -> {ok, state()} | nothing_to_claim.
claim_execution_of_cancellation_procedures(State = #workflow_execution_state{
    pending_callbacks = PendingCallbacks,
    tasks_data_registry = TasksDataRegistry
}, TaskIdsWithStreamsToFinish) ->
    case workflow_tasks_data_registry:claim_execution_of_cancellation_procedures(TasksDataRegistry) of
        {ok, UpdatedTasksDataRegistry} when TaskIdsWithStreamsToFinish =/= [] ->
            {ok, State#workflow_execution_state{
                tasks_data_registry = UpdatedTasksDataRegistry,
                update_report = ?EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT(TaskIdsWithStreamsToFinish),
                pending_callbacks = [?CALLBACKS_ON_STREAMS_CANCEL_SELECTOR | PendingCallbacks]
            }};
        _ ->
            nothing_to_claim
    end.

-spec get_task_ids_to_notify_on_cancel(state()) -> {
    TaskIdsWithStreamsToNotify :: [workflow_engine:task_id()],
    TaskIdsWithoutStreamsToNotify :: [workflow_engine:task_id()]
}.
get_task_ids_to_notify_on_cancel(State = #workflow_execution_state{
    current_lane = #current_lane{
        parallel_box_specs = BoxSpecs
    },
    jobs = Jobs
}) ->
    TaskIdsToNotify = lists:flatten(lists:map(fun(BoxIndex) ->
        BoxSpec = maps:get(BoxIndex, BoxSpecs),
        lists:filtermap(fun(TaskIndex) ->
            {TaskId, _TaskSpec} = maps:get(TaskIndex, BoxSpec),
            case workflow_jobs:is_task_finished(Jobs, BoxIndex, TaskIndex) of
                true -> false;
                false -> {true, TaskId}
            end
        end, lists:seq(1, maps:size(BoxSpec)))
    end, lists:seq(1, maps:size(BoxSpecs)))),

    TaskIdsWithStreams = get_task_ids_with_streams(State),
    TaskIdsWithStreamsToNotify = lists_utils:intersect(TaskIdsToNotify, TaskIdsWithStreams),
    TaskIdsWithoutStreamsToNotify = TaskIdsToNotify -- TaskIdsWithStreamsToNotify,
    {TaskIdsWithStreamsToNotify, TaskIdsWithoutStreamsToNotify}.


-spec get_task_ids_with_streams(state()) -> [workflow_engine:task_id()].
get_task_ids_with_streams(#workflow_execution_state{
    current_lane = #current_lane{
        parallel_box_specs = BoxSpecs
    }
}) ->
    lists:filtermap(fun
        ({TaskId, #{data_stream_enabled := true}}) -> {true, TaskId};
        (_) -> false
    end, lists:flatmap(fun(BoxSpec) -> maps:values(BoxSpec) end, maps:values(BoxSpecs))).

-spec are_all_data_streams_finalized(state()) -> boolean().
are_all_data_streams_finalized(State = #workflow_execution_state{tasks_data_registry = TasksDataRegistry}) ->
    lists:all(fun(TaskId) ->
        workflow_tasks_data_registry:is_stream_finalized(TaskId, TasksDataRegistry)
    end, get_task_ids_with_streams(State)).


-spec prepare_next_parallel_box(state(), workflow_jobs:job_identifier()) -> {ok, state()}.
prepare_next_parallel_box(State = #workflow_execution_state{
    current_lane = #current_lane{
        parallel_box_count = BoxCount,
        parallel_box_specs = BoxSpecs,
        index = LaneIndex,
        id = LaneId
    },
    lowest_failed_job_identifier = LowestFailedJobIdentifier,
    jobs = Jobs,
    iteration_state = IterationState
}, JobIdentifier) ->
    case workflow_jobs:prepare_next_parallel_box(Jobs, JobIdentifier, BoxSpecs, BoxCount) of
        {ok, NewJobs} ->
            State2 = State#workflow_execution_state{jobs = NewJobs},
            TaskStatus = get_task_status(JobIdentifier, State2),
            {ok, add_if_callback_is_pending(State2#workflow_execution_state{
                update_report = ?TASK_PROCESSED_REPORT(TaskStatus)
            }, JobIdentifier, TaskStatus =/= ongoing)};
        {?WF_ERROR_ITEM_PROCESSING_ENDED(ItemIndex, SuccessOrFailure), NewJobs} ->
            {NewIterationState, ItemIdToReportError, ItemIdToSnapshot, ItemIdsToDelete} =
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
            State2 = State#workflow_execution_state{jobs = NewJobs, iteration_state = NewIterationState},
            State3 = add_if_callback_is_pending(State2, ItemIdToReportError, ItemIdToReportError =/= undefined),

            JobIdentifiersToCheck = case SuccessOrFailure of
                ?SUCCESS ->
                    [JobIdentifier];
                ?FAILURE ->
                    [JobIdentifier |
                        workflow_jobs:get_identfiers_for_next_parallel_boxes(JobIdentifier, BoxSpecs, BoxCount)]
            end,

            TaskStatusesToReport = lists:map(fun(JobIdentifierToCheck) ->
                {JobIdentifierToCheck, get_task_status(JobIdentifierToCheck, State3)}
            end, JobIdentifiersToCheck),

            State4 = lists:foldl(fun({Identifier, TaskStatus}, StateAcc) ->
                add_if_callback_is_pending(StateAcc, Identifier, TaskStatus =/= ongoing)
            end, State3, TaskStatusesToReport),

            {ok, State4#workflow_execution_state{
                update_report = #items_processed_report{lane_index = LaneIndex, lane_id = LaneId,
                    last_finished_item_index = ItemIndex,
                    item_id_to_report_error = ItemIdToReportError, item_id_to_snapshot = FinalItemIdToSnapshot,
                    item_ids_to_delete = ItemIdsToDelete, task_statuses_to_report = TaskStatusesToReport}
            }}
    end.

-spec prepare_next_streamed_task_data(state()) -> {ok, state()} | ?ERROR_NOT_FOUND.
prepare_next_streamed_task_data(State = #workflow_execution_state{tasks_data_registry = TasksDataRegistry}) ->
    case workflow_tasks_data_registry:take_for_processing(TasksDataRegistry) of
        {ok, TaskId, CachedTaskDataId, UpdatedTasksDataRegistry} ->
            {ok, State#workflow_execution_state{
                update_report = #job_prepared_report{job_identifier = streamed_task_data,
                    task_id = TaskId, subject_id = CachedTaskDataId},
                tasks_data_registry = UpdatedTasksDataRegistry
            }};
        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND
    end.

-spec get_task_status(workflow_jobs:job_identifier(), state()) -> task_status().
get_task_status(JobIdentifier, #workflow_execution_state{
    current_lane = #current_lane{
        parallel_box_specs = BoxSpecs
    },
    jobs = Jobs
}) ->
    case workflow_jobs:is_task_finished(Jobs, JobIdentifier) of
        true ->
            {_TaskId, TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxSpecs),
            case maps:get(data_stream_enabled, TaskSpec, false) of
                true -> waiting_for_data_stream_finalization;
                false -> ended
            end;
        false ->
            ongoing
    end.

-spec mark_task_data_processed(workflow_engine:task_id(), workflow_cached_task_data:id(), state()) -> state().
mark_task_data_processed(TaskId, CachedTaskDataId, State = #workflow_execution_state{tasks_data_registry = TasksDataRegistry}) ->
    UpdatedTasksDataRegistry = workflow_tasks_data_registry:mark_processed(TaskId, CachedTaskDataId, TasksDataRegistry),
    TaskStatus = case  workflow_tasks_data_registry:is_stream_finalized(TaskId, UpdatedTasksDataRegistry) of
        true -> ended;
        false -> ongoing
    end,

    add_if_callback_is_pending(State#workflow_execution_state{
        tasks_data_registry = UpdatedTasksDataRegistry,
        update_report = ?TASK_PROCESSED_REPORT(TaskStatus)
    }, TaskId, TaskStatus =:= ended).

mark_all_streamed_task_data_received_internal(TaskId, State = #workflow_execution_state{tasks_data_registry = TasksDataRegistry}) ->
    UpdatedTasksDataRegistry = workflow_tasks_data_registry:mark_all_task_data_received(TaskId, TasksDataRegistry),
    TaskStatus = case  workflow_tasks_data_registry:is_stream_finalized(TaskId, UpdatedTasksDataRegistry) of
        true -> ended;
        false -> waiting_for_data_stream_finalization
    end,

    add_if_callback_is_pending(State#workflow_execution_state{
        tasks_data_registry = UpdatedTasksDataRegistry,
        update_report = ?TASK_PROCESSED_REPORT(TaskStatus)
    }, TaskId, TaskStatus =:= ended).

-spec add_if_callback_is_pending(state(), callback_selector(), boolean()) -> state().
add_if_callback_is_pending(#workflow_execution_state{pending_callbacks = PendingCallbacks} = State,
    CallbackSelector, true = _IsCallbackPending) ->
    State#workflow_execution_state{pending_callbacks = [CallbackSelector | PendingCallbacks]};
add_if_callback_is_pending(State, _CallbackSelector, false = _IsCallbackPending) ->
    State.

-spec remove_pending_callback(state(), callback_selector()) -> {ok, state()}.
remove_pending_callback(#workflow_execution_state{pending_callbacks = PendingCallbacks} = State,
    CallbackSelector) ->
    {ok, State#workflow_execution_state{pending_callbacks = PendingCallbacks -- [CallbackSelector]}}.

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
            State2 = State#workflow_execution_state{jobs = NewJobs2},
            TaskStatus = get_task_status(JobIdentifier, State2),
            {ok, add_if_callback_is_pending(State2#workflow_execution_state{
                update_report = ?TASK_PROCESSED_REPORT(TaskStatus)
            }, JobIdentifier, TaskStatus =/= ongoing)};
        ?NO_JOBS_LEFT_FOR_PARALLEL_BOX ->
            prepare_next_parallel_box(State#workflow_execution_state{jobs = NewJobs2}, JobIdentifier)
    end;
report_job_finish(State = #workflow_execution_state{
    jobs = Jobs,
    lowest_failed_job_identifier = LowestFailedJobIdentifier,
    failed_job_count = FailedJobCount
}, JobIdentifier, error) ->
    {FinalJobs, RemainingForBox} = workflow_jobs:register_failure(Jobs, JobIdentifier),
    State2 = case LowestFailedJobIdentifier of
        undefined ->
            State#workflow_execution_state{lowest_failed_job_identifier = JobIdentifier};
        _ ->
            case workflow_jobs:is_previous(LowestFailedJobIdentifier, JobIdentifier) of
                true -> State;
                false -> State#workflow_execution_state{lowest_failed_job_identifier = JobIdentifier}
            end
    end,
    State3 = State2#workflow_execution_state{jobs = FinalJobs, failed_job_count = FailedJobCount + 1},
    case RemainingForBox of
        ?AT_LEAST_ONE_JOB_LEFT_FOR_PARALLEL_BOX ->
            TaskStatus = get_task_status(JobIdentifier, State3),
            {ok, add_if_callback_is_pending(State3#workflow_execution_state{
                update_report = ?TASK_PROCESSED_REPORT(TaskStatus)
            }, JobIdentifier, TaskStatus =/= ongoing)};
        ?NO_JOBS_LEFT_FOR_PARALLEL_BOX ->
            % Call prepare_next_parallel_box/2 to delete metadata for failed item
            prepare_next_parallel_box(State3, JobIdentifier)
    end.

-spec handle_no_waiting_items_error(state(), ?WF_ERROR_NO_WAITING_ITEMS | ?ERROR_NOT_FOUND) -> no_items_error().
handle_no_waiting_items_error(#workflow_execution_state{current_lane = #current_lane{id = undefined}}, _Error) ->
    ?WF_ERROR_NO_WAITING_ITEMS;
handle_no_waiting_items_error(#workflow_execution_state{pending_callbacks = Waiting}, _Error) when Waiting =/= [] ->
    ?WF_ERROR_NO_WAITING_ITEMS;
handle_no_waiting_items_error(_State, ?WF_ERROR_NO_WAITING_ITEMS) ->
    ?WF_ERROR_NO_WAITING_ITEMS;
handle_no_waiting_items_error(#workflow_execution_state{
    prefetched_iteration_step = ?WF_ERROR_ITERATION_FAILED,
    handler = Handler,
    initial_context = Context,
    current_lane = #current_lane{id = LaneId, execution_context = LaneContext},
    next_lane_preparation_status = NextLaneStatus,
    pending_callbacks = PendingCallbacks
} = State, ?ERROR_NOT_FOUND) ->
    {TaskIdsWithStreamsToNotify, TaskIdsWithoutStreamsToNotify} = get_task_ids_to_notify_on_cancel(State),
    case claim_execution_of_cancellation_procedures(State, TaskIdsWithStreamsToNotify) of
        {ok, UpdatedState} ->
            {ok, UpdatedState};
        nothing_to_claim ->
            case are_all_data_streams_finalized(State) of
                true when NextLaneStatus =:= ?PREPARING ->
                    {ok, State#workflow_execution_state{
                        iteration_state = workflow_iteration_state:init(),
                        execution_status = ?WAITING_FOR_NEXT_LANE_PREPARATION_END,
                        update_report = ?EXECUTION_CANCELLED_REPORT([], TaskIdsWithoutStreamsToNotify),
                        pending_callbacks = [?CALLBACKS_ON_CANCEL_SELECTOR | PendingCallbacks]
                    }};
                true ->
                    ?WF_ERROR_EXECUTION_ENDED(#execution_ended{handler = Handler, context = Context, reason = ?EXECUTION_CANCELLED,
                        callbacks_data = {LaneId, LaneContext, TaskIdsWithoutStreamsToNotify}});
                false ->
                    ?WF_ERROR_NO_WAITING_ITEMS
            end
    end;
handle_no_waiting_items_error(#workflow_execution_state{
    current_lane = #current_lane{
        index = LaneIndex,
        id = LaneId,
        execution_context = LaneContext
    }
} = State, ?ERROR_NOT_FOUND) ->
    case are_all_data_streams_finalized(State) of
        true ->
            {ok, State#workflow_execution_state{
                current_lane = #current_lane{index = LaneIndex + 1},
                failed_job_count = 0,
                execution_status = ?PREPARING, update_report = ?LANE_READY_TO_BE_FINISHED_REPORT(LaneId, LaneContext)}};
        false ->
            ?WF_ERROR_NO_WAITING_ITEMS
    end.


%%%===================================================================
%%% Test API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks if lane is finished and lane data is cleared. Current state() is returned for better output in tests
%% when an assertion fails.
%% @end
%%--------------------------------------------------------------------
-spec is_finished_and_cleaned(workflow_engine:execution_id(), index()) -> true | {false, state()}.
is_finished_and_cleaned(ExecutionId, LaneIndex) ->
    case datastore_model:get(?CTX, ExecutionId) of
        {ok, #document{value = #workflow_execution_state{
            current_lane = #current_lane{index = Index, id = undefined},
            jobs = Jobs,
            iteration_state = IterationState,
            tasks_data_registry = TasksDataRegistry
        } = Record}} when Index > LaneIndex ->
            case workflow_jobs:is_empty(Jobs) andalso
                workflow_iteration_state:is_finished_and_cleaned(IterationState) andalso
                workflow_tasks_data_registry:is_empty(TasksDataRegistry)
            of
                true -> true;
                false -> {false, Record}
            end;
        {ok, #document{value = #workflow_execution_state{
            current_lane = #current_lane{index = LaneIndex},
            execution_status = ExecutionStatus
        } = Record}} when ExecutionStatus =:= ?EXECUTION_CANCELLED orelse
            ExecutionStatus =:= ?WAITING_FOR_NEXT_LANE_PREPARATION_END ->
            is_canceled_execution_finished_and_cleaned(Record);
        {ok, #document{value = #workflow_execution_state{
            current_lane = #current_lane{index = LaneIndex},
            execution_status = ?EXECUTION_ENDED,
            next_lane_preparation_status = ?PREPARED_IN_ADVANCE,
            next_lane = #next_lane{}
        } = Record}} ->
            % ?EXECUTION_ENDED with next line ?PREPARED_IN_ADVANCE is possible only after cancelation
            is_canceled_execution_finished_and_cleaned(Record);
        {ok, #document{value = #workflow_execution_state{
            current_lane = #current_lane{index = LaneIndex},
            execution_status = ?EXECUTING,
            prefetched_iteration_step = ?WF_ERROR_ITERATION_FAILED
        } = Record}} ->
            % Iteration failure cancels execution
            is_canceled_execution_finished_and_cleaned(Record);
        {ok, #document{value = Record}} ->
            {false, Record}
    end.


%% @private
-spec is_canceled_execution_finished_and_cleaned(state()) -> true | {false, state()}.
is_canceled_execution_finished_and_cleaned(#workflow_execution_state{
    jobs = Jobs,
    iteration_state = IterationState,
    tasks_data_registry = TasksDataRegistry
} = State) ->
    HasWaitingResults = case workflow_jobs:prepare_next_waiting_result(Jobs) of
        {{ok, _}, _} -> true;
        _ -> false
    end,
    case not workflow_jobs:has_ongoing_jobs(Jobs) andalso not HasWaitingResults andalso
        workflow_iteration_state:is_finished_and_cleaned(IterationState) andalso
        workflow_tasks_data_registry:is_empty(TasksDataRegistry)
    of
        true -> true;
        false -> {false, State}
    end.


-spec get_item_id(workflow_engine:execution_id(), workflow_jobs:job_identifier()) -> workflow_cached_item:id().
get_item_id(ExecutionId, JobIdentifier) ->
    {ok, #document{value = #workflow_execution_state{iteration_state = IterationState}}} =
        datastore_model:get(?CTX, ExecutionId),
    workflow_jobs:get_item_id(JobIdentifier, IterationState).

-spec get_current_lane_context(workflow_engine:execution_id()) -> workflow_engine:execution_context().
get_current_lane_context(ExecutionId) ->
    {ok, #document{value = #workflow_execution_state{current_lane = #current_lane{execution_context = Context}}}} =
        datastore_model:get(?CTX, ExecutionId),
    Context.