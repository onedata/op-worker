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

-compile({no_auto_import, [get/1]}).


%% API
-export([init/7, resume_from_snapshot/7, init_cancel/2, finish_cancel/1, wait_for_pending_callbacks/1,
    handle_exception/5, execute_exception_handler_if_waiting/1, abandon/2, cleanup/1, prepare_next_job/1,
    report_execution_status_update/4, report_lane_execution_prepared/6, pause_job/2,
    report_new_streamed_task_data/3, report_streamed_task_data_processed/4, mark_all_streamed_task_data_received/3,
    check_timeouts/1, reset_keepalive_timer/2, get_result_processing_data/2, get/1, save/1, get_boxes_map/1]).
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
% workflow_handler:handle_lane_execution_stopped/3 execution)
-record(next_lane, {
    id :: workflow_engine:lane_id() | undefined,
    lane_spec :: workflow_engine:lane_spec() | undefined
}).


-define(EXECUTION_CANCELLED(CallCount), #execution_cancelled{call_count = CallCount}).
-define(PREPARATION_CANCELLED(CallCount), #execution_cancelled{execution_step = lane_prepare, call_count = CallCount}).
-define(WAITING_FOR_NEXT_LANE_PREPARATION_END(IsInterrupted), #execution_cancelled{callbacks_to_execute = [],
    execution_step = waiting_on_next_lane_prepare, call_count = 0, is_interrupted = IsInterrupted}).
-define(WAITING_FOR_ERROR_HANDLING_OR_NEXT_LANE_PREPARATION_END(IsInterrupted), #execution_cancelled{
    execution_step = waiting_on_next_lane_prepare, call_count = 0, is_interrupted = IsInterrupted}).
-define(EXECUTION_ENDED_AFTER_CANCEL(IsInterrupted, Reason), #execution_cancelled{
    execution_step = finishing_execution, call_count = 0, callbacks_to_execute = [],
    is_interrupted = IsInterrupted, abrupt_stop_reason = Reason}).


% Macros and records used to provide additional information about document update procedure
% (see #workflow_execution_state.update_report)
-type init_type() :: prepare | ?RESUMING_FROM_ITERATOR(iterator:iterator()) |
    ?RESUMING_FROM_DUMP(iterator:iterator(), workflow_execution_state_dump:task_index_map()).
-export_type([init_type/0]).
-define(LANE_DESIGNATED_FOR_INIT(InitType), {lane_designated_for_init, InitType}).
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
-define(EXECUTION_CANCELLED_REPORT(ItemIdsToDelete, TaskIds),
    ?EXECUTION_CANCELLED_REPORT(ItemIdsToDelete, TaskIds, [], [])).
-define(EXECUTION_CANCELLED_REPORT(ItemIdsToDelete, TaskIds, ResultIds, TaskDataIds),
    {execution_canceled_report, ItemIdsToDelete, TaskIds, ResultIds, TaskDataIds}).
-define(EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT(TaskIds), {execution_canceled_with_open_streams_report, TaskIds}).
-define(EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT(Handler, LaneContext, TaskIds),
    {execution_canceled_with_open_streams_report, Handler, LaneContext, TaskIds}).
-define(LANE_READY_TO_BE_FINISHED_REPORT(LaneId, LaneContext), {lane_ready_to_be_finished_report, LaneId, LaneContext}).
-define(JOBS_EXPIRED_REPORT(AsyncPoolsChanges), {jobs_expired_report, AsyncPoolsChanges}).
-define(LANE_PREPARED_REPORT(Lane), {lane_prepared_report, Lane}).
-define(EXECUTE_DELAYED_CALLBACKS(Callbacks), {execute_delayed_callback, Callbacks}).
-define(ITEM_RESUMED, item_resumed).
-define(DELETE_ITEM, delete_item).
-define(DELETE_ITEM(ItemId), {delete_item, ItemId}).
-define(EXECUTE_CALLBACKS_ON_PARTIALLY_ENDED_LANE(WaitingOrOngoingTasksIndexes),
    {execute_callbacks_on_partially_ended_lane, WaitingOrOngoingTasksIndexes}).

% Definitions of possible errors
-define(WF_ERROR_LANE_ALREADY_PREPARED, {error, lane_already_prepared}).
-define(WF_ERROR_LANE_CHANGED, {error, lane_changed}).
-define(WF_ERROR_LANE_EXECUTION_CANCELLED(Handler, CancelledLaneId, CancelledLaneContext, TaskIds),
    {error, {lane_execution_cancelled, Handler, CancelledLaneId, CancelledLaneContext, TaskIds}}).
-define(WF_ERROR_NO_CACHED_ITEMS(LaneIndex, ItemIndex, IterationStep, Handler, Context),
    {error, {no_cached_items, LaneIndex, ItemIndex, IterationStep, Handler, Context}}).
-define(WF_ERROR_NOTHING_CHANGED, {error, nothing_changed}).
-define(WF_ERROR_CURRENT_LANE, {error, current_lane}).
-define(WF_ERROR_UNKNOWN_LANE, {error, unknown_lane}).
-define(WF_ERROR_EXCEPTION_APPEARED(EngineId), {error, {exception_appeared, EngineId}}).

-type index() :: non_neg_integer(). % scheduling is based on positions of elements (items, parallel_boxes, tasks)
                                    % to allow executions of tasks in chosen order
-type incarnation_tag() :: binary().
-type snapshot_mode() :: ?ALL_ITEMS | ?UNTIL_FIRST_FAILURE.
-type iteration_step() :: {workflow_cached_item:id(), iterator:iterator()}.
-type iteration_status() :: iteration_step() | undefined.
-type task_status() :: ongoing | waiting_for_data_stream_finalization | ended.
-type state() :: #workflow_execution_state{}.
-type doc() :: datastore_doc:doc(state()).
-export_type([state/0, doc/0]).

-type execution_status() :: ?NOT_PREPARED | ?PREPARING | ?RESUMING_FROM_ITERATOR(iterator:iterator()) |
    ?RESUMING_FROM_DUMP(iterator:iterator(), workflow_execution_state_dump:task_index_map()) |?PREPARED_IN_ADVANCE |
    ?PREPARATION_FAILED | ?EXECUTING | #execution_cancelled{}.
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
-type update_report() :: ?LANE_DESIGNATED_FOR_INIT(init_type()) |
    ?LANE_DESIGNATED_FOR_PREPARATION_IN_ADVANCE |
    #job_prepared_report{} | #items_processed_report{} | ?LANE_PREPARED_REPORT(next_lane()) |
    ?TASK_PROCESSED_REPORT(task_status()) | ?JOBS_EXPIRED_REPORT(async_pools_slots_to_free()) |
    ?LANE_READY_TO_BE_FINISHED_REPORT(workflow_engine:lane_id(), workflow_engine:execution_context()) |
    ?EXECUTION_CANCELLED_REPORT([workflow_cached_item:id()], [workflow_engine:task_id()],
        [workflow_cached_async_result:result_ref()], [workflow_cached_task_data:id()]) |
    ?EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT([workflow_engine:task_id()]) | no_items_error() |
    ?EXECUTE_DELAYED_CALLBACKS([callback_to_exectute()]) | ?ITEM_RESUMED |
    ?EXECUTE_CALLBACKS_ON_PARTIALLY_ENDED_LANE(#{workflow_jobs:job_identifier() => [workflow_jobs:job_identifier()]}) |
    ?DELETE_ITEM | ?DELETE_ITEM(workflow_cached_item:id()).

-define(CALLBACKS_ON_CANCEL_SELECTOR, callbacks_on_cancel).
-define(CALLBACKS_ON_STREAMS_CANCEL_SELECTOR, callbacks_on_stream_cancel).
-define(CALLBACKS_ON_EMPTY_LANE_SELECTOR, callbacks_on_empty_lane).
-define(CALLBACKS_ON_PARTIALLY_ENDED_LANE_SELECTOR, callbacks_on_partially_ended_lane).
-define(CALLBACK_ON_EXCEPTION, callback_on_exception).
-type callback_selector() :: workflow_jobs:job_identifier() | workflow_engine:task_id() | workflow_cached_item:id() |
    ?CALLBACKS_ON_CANCEL_SELECTOR | ?CALLBACKS_ON_STREAMS_CANCEL_SELECTOR | ?CALLBACKS_ON_EMPTY_LANE_SELECTOR |
    ?CALLBACKS_ON_PARTIALLY_ENDED_LANE_SELECTOR | ?CALLBACK_ON_EXCEPTION.
-type callback_to_exectute() :: {function(), callback_selector(), Args:: list()}.
-export_type([callback_to_exectute/0]).

-export_type([index/0, incarnation_tag/0, snapshot_mode/0, iteration_status/0, current_lane/0, next_lane/0,
    execution_status/0, next_lane_preparation_status/0, boxes_map/0, update_report/0, callback_selector/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec init(workflow_engine:execution_id(), workflow_engine:id(), workflow_handler:handler(), workflow_engine:execution_context(),
    workflow_engine:lane_id() | undefined, workflow_engine:lane_id() | undefined, snapshot_mode()) ->
    ok | ?WF_ERROR_PREPARATION_FAILED.
init(_ExecutionId, _EngineId, _Handler, _Context, undefined, _NextLaneId, _SnapshotMode) ->
    ?WF_ERROR_PREPARATION_FAILED; % FirstLaneId does not have to be defined only when execution is resumed from snapshot
init(ExecutionId, EngineId, Handler, Context, FirstLaneId, NextLaneId, SnapshotMode) ->
    workflow_iterator_snapshot:cleanup(ExecutionId),
    Doc = #document{key = ExecutionId, value = #workflow_execution_state{
        engine_id = EngineId,
        handler = Handler,
        initial_context = Context,
        incarnation_tag = gen_incarnation_tag(),
        snapshot_mode = SnapshotMode,
        current_lane = #current_lane{index = 1, id = FirstLaneId},
        next_lane = #next_lane{id = NextLaneId}
    }},
    save(Doc),
    workflow_iterator_snapshot:save(ExecutionId, 1, FirstLaneId, 0, undefined, NextLaneId).

-spec resume_from_snapshot(
    workflow_engine:execution_id(), workflow_engine:id(), workflow_handler:handler(), workflow_engine:execution_context(),
    workflow_engine:lane_id() | undefined, workflow_engine:lane_id() | undefined, snapshot_mode()) ->
    ok | ?WF_ERROR_PREPARATION_FAILED.
resume_from_snapshot(ExecutionId, EngineId, Handler, Context, InitialLaneId, InitialNextLaneId, SnapshotMode) ->
    case workflow_iterator_snapshot:get(ExecutionId) of
        {ok, #workflow_iterator_snapshot{
            lane_id = undefined,
            iterator = undefined
        }} ->
            % Previous execution ended with no lane to run
            workflow_execution_state_dump:delete(ExecutionId),
            init(ExecutionId, EngineId, Handler, Context, InitialLaneId, InitialNextLaneId, SnapshotMode);
        {ok, #workflow_iterator_snapshot{
            lane_index = LaneIndex, lane_id = LaneId,
            iterator = undefined,
            next_lane_id = NextLaneId
        }} ->
            Doc = #document{key = ExecutionId, value = #workflow_execution_state{
                engine_id = EngineId,
                handler = Handler,
                initial_context = Context,
                incarnation_tag = gen_incarnation_tag(),
                snapshot_mode = SnapshotMode,
                current_lane = #current_lane{index = LaneIndex, id = LaneId},
                next_lane = #next_lane{id = NextLaneId}}
            },
            workflow_execution_state_dump:delete(ExecutionId),
            save(Doc);
        {ok, #workflow_iterator_snapshot{
            lane_index = LaneIndex, lane_id = LaneId,
            iterator = Iterator,
            next_lane_id = NextLaneId
        }} ->
            Doc = #document{key = ExecutionId, value = #workflow_execution_state{
                execution_status = ?RESUMING_FROM_ITERATOR(Iterator),
                engine_id = EngineId,
                handler = Handler,
                initial_context = Context,
                incarnation_tag = gen_incarnation_tag(),
                snapshot_mode = SnapshotMode,
                current_lane = #current_lane{index = LaneIndex, id = LaneId},
                next_lane = #next_lane{id = NextLaneId}}
            },
            case workflow_execution_state_dump:restore_workflow_execution_state_from_dump(Doc, Iterator) of
                ok -> ok;
                ?ERROR_NOT_FOUND -> save(Doc)
            end;
        ?ERROR_NOT_FOUND ->
            init(ExecutionId, EngineId, Handler, Context, InitialLaneId, InitialNextLaneId, SnapshotMode)
    end.

-spec init_cancel(workflow_engine:execution_id(), workflow_engine:cancel_pred()) -> ok | ?WF_ERROR_PRED_NOT_MEET.
init_cancel(ExecutionId, Pred) ->
    case update(ExecutionId, fun(State) -> handle_execution_cancel_init(State, Pred) end) of
        {ok, _} -> ok;
        ?WF_ERROR_PRED_NOT_MEET -> ?WF_ERROR_PRED_NOT_MEET
    end.

-spec finish_cancel(workflow_engine:execution_id()) -> {ok, workflow_engine:id()} | ?WF_ERROR_CANCEL_NOT_INITIALIZED.
finish_cancel(ExecutionId) ->
    case update(ExecutionId, fun handle_execution_cancel_finish/1) of
        {ok, #document{value = #workflow_execution_state{engine_id = EngineId}}} -> {ok, EngineId};
        ?WF_ERROR_CANCEL_NOT_INITIALIZED -> ?WF_ERROR_CANCEL_NOT_INITIALIZED
    end.

-spec wait_for_pending_callbacks(workflow_engine:execution_id()) -> ok.
wait_for_pending_callbacks(ExecutionId) ->
    case get(ExecutionId) of
        {ok, #workflow_execution_state{pending_callbacks = []}} ->
            ok;
        {ok, _} ->
            timer:sleep(200),
            wait_for_pending_callbacks(ExecutionId);
        ?ERROR_NOT_FOUND ->
            ok
    end.

-spec handle_exception(workflow_engine:execution_id(), workflow_engine:execution_context(),
    throw | error | exit, term(), list()) -> ok.
handle_exception(ExecutionId, Context, ErrorType, Reason, Stacktrace) ->
    {ok, _} = update(ExecutionId, fun(State) -> mark_exception_appeared(State, Context, ErrorType, Reason, Stacktrace) end),
    ok.

-spec execute_exception_handler_if_waiting(workflow_engine:execution_id()) ->
    {executed, workflow_handler:abrupt_stop_reason()} | not_waiting.
execute_exception_handler_if_waiting(ExecutionId) ->
    case update(ExecutionId, fun check_waiting_exception_handler/1) of
        {ok, #document{value = #workflow_execution_state{
            handler = Handler,
            update_report = ?EXECUTE_DELAYED_CALLBACKS([{_, _, [Context, ErrorType, Reason, Stacktrace]}])
        }}} ->
            {
                executed,
                workflow_engine:execute_exception_handler(ExecutionId, Context, Handler, ErrorType, Reason, Stacktrace)
            };
        ?ERROR_NOT_FOUND ->
            not_waiting
    end.

-spec abandon(workflow_engine:execution_id(), workflow_handler:abrupt_stop_reason()) -> ok.
abandon(ExecutionId, InterruptReason) ->
    {ok, _} = update(ExecutionId, fun(State) -> mark_workflow_abandoned(State, InterruptReason) end),
    ok.

-spec cleanup(workflow_engine:execution_id()) -> ok.
cleanup(ExecutionId) ->
    ok = datastore_model:delete(?CTX, ExecutionId).

-spec prepare_next_job(workflow_engine:execution_id()) ->
    {ok, workflow_engine:execution_spec()} | ?DEFER_EXECUTION | ?RETRY_EXECUTION | ?ERROR_NOT_FOUND | #execution_ended{} |
    ?PREPARE_LANE_EXECUTION(workflow_handler:handler(), workflow_engine:execution_context(),
        workflow_engine:lane_id(), workflow_engine:preparation_mode(), init_type()).
prepare_next_job(ExecutionId) ->
    % TODO VFS-7787 - check quota for async jobs and do nothing if it is exceeded
    case prepare_next_job_for_current_lane(ExecutionId) of
        {ok, _} = OkAns ->
            OkAns;
        ?WF_ERROR_NO_WAITING_ITEMS ->
            ?DEFER_EXECUTION;
        ?PREPARE_LANE_EXECUTION(Handler, ExecutionContext, LaneId, PreparationMode, InitType) ->
            ?PREPARE_LANE_EXECUTION(Handler, ExecutionContext, LaneId, PreparationMode, InitType);
        ?WF_ERROR_EXECUTION_ENDED(ExecutionEnded) ->
            ExecutionEnded;
        ?WF_ERROR_LANE_EXECUTION_CANCELLED(Handler, CancelledLaneId, CancelledLaneContext, TaskIds) ->
            % TODO VFS-9993 - test if exception handler is called when exception appears here
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
) -> {ok, workflow_engine:id(), workflow_engine:task_spec()} | ?WF_ERROR_JOB_NOT_FOUND.
report_execution_status_update(ExecutionId, JobIdentifier, UpdateType, Ans) ->
    CachedAns = case UpdateType of
        ?ASYNC_CALL_ENDED -> workflow_cached_async_result:put(Ans);
        _ -> Ans
    end,

    {UpdatedDocOrError, ItemIdToReportError, TaskStatusesToReport} = case update(ExecutionId, fun(State) ->
        report_execution_status_update_internal(State, JobIdentifier, UpdateType, CachedAns)
    end) of
        {ok, Doc = #document{value = #workflow_execution_state{update_report = #items_processed_report{
            item_id_to_report_error = IdToReportError,
            item_id_to_snapshot = undefined,
            item_ids_to_delete = ItemIdsToDelete,
            task_statuses_to_report = ReportedTaskStatuses
        }}}} ->
            lists:foreach(fun(ItemId) -> maybe_delete_item(ExecutionId, ItemId) end, ItemIdsToDelete),
            {Doc, IdToReportError, ReportedTaskStatuses};
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
            IteratorToSave = workflow_cached_item:get_iterator(ItemIdToSnapshot),
            workflow_iterator_snapshot:save(
                ExecutionId, LaneIndex, LaneId, ItemIndex, IteratorToSave, NextLaneId),
            maybe_delete_item(ExecutionId, ItemIdToSnapshot),
            lists:foreach(fun(ItemId) -> maybe_delete_item(ExecutionId, ItemId) end, ItemIdsToDelete),
            {Doc, IdToReportError, ReportedTaskStatuses};
        {ok, Doc = #document{value = #workflow_execution_state{
            update_report = ?TASK_PROCESSED_REPORT(ReportedTaskStatus)
        }}} ->
            {Doc, undefined, [{JobIdentifier, ReportedTaskStatus}]};
        {ok, Doc} ->
            {Doc, undefined, []};
        % Possible errors when result of async task that ended with timeout appeared
        ?WF_ERROR_JOB_NOT_FOUND ->
            ?debug("Result for not found job ~p of execution ~p", [JobIdentifier, ExecutionId]),
            {?WF_ERROR_JOB_NOT_FOUND, undefined, []};
        ?ERROR_NOT_FOUND ->
            ?debug("Result for job ~p of ended execution ~p", [JobIdentifier, ExecutionId]),
            {?WF_ERROR_JOB_NOT_FOUND, undefined, []}
    end,

    case UpdatedDocOrError of
        ?WF_ERROR_JOB_NOT_FOUND ->
            case UpdateType of
                ?ASYNC_CALL_ENDED ->
                    workflow_cached_async_result:delete(CachedAns);
                _ ->
                    ok
            end,
            ?WF_ERROR_JOB_NOT_FOUND; % Error occurred - no task can be connected to result
        #document{value = #workflow_execution_state{engine_id = EngineId, execution_status = Status}} = UpdatedDoc ->
            maybe_report_item_error(UpdatedDoc, ItemIdToReportError),
            % TODO VFS-8456 - maybe execute notification handlers also on pool?
            lists:foreach(fun({Identifier, TaskStatus}) ->
                maybe_notify_task_execution_ended(UpdatedDoc, Identifier, TaskStatus)
            end, TaskStatusesToReport),

            #document{value = #workflow_execution_state{
                current_lane = #current_lane{parallel_box_specs = BoxSpecs}
            }} = UpdatedDoc,
            {_TaskId, TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxSpecs),

            case {UpdateType, Status} of
                {?ASYNC_CALL_ENDED, #execution_cancelled{is_interrupted = true}} ->
                    workflow_cached_async_result:delete(CachedAns);
                _ ->
                    ok
            end,

            {ok, EngineId, TaskSpec}
    end.

-spec report_lane_execution_prepared(
    workflow_handler:handler(),
    workflow_engine:execution_id(),
    workflow_engine:lane_id(),
    workflow_engine:preparation_mode(),
    init_type(),
    workflow_handler:prepare_lane_result()
) -> ok.
report_lane_execution_prepared(Handler, ExecutionId, _LaneId, ?PREPARE_CURRENT, prepare, {ok, LaneSpec}) ->
    case finish_lane_preparation(Handler, ExecutionId, LaneSpec, prepare) of
        {ok, #current_lane{index = LaneIndex, id = LaneId}, IteratorToSave, NextLaneId} ->
            workflow_iterator_snapshot:save(ExecutionId, LaneIndex, LaneId, 0, IteratorToSave, NextLaneId);
        ?WF_ERROR_LANE_ALREADY_PREPARED -> ok;
        ?WF_ERROR_PREPARATION_FAILED -> ok
    end;
report_lane_execution_prepared(Handler, ExecutionId, _LaneId, ?PREPARE_CURRENT, ?RESUMING(_, Iterator, _) = InitType, {ok, LaneSpec}) ->
    case finish_lane_preparation(Handler, ExecutionId, LaneSpec#{iterator => Iterator}, InitType) of
        {ok, #current_lane{index = LaneIndex, id = LaneId}, IteratorToSave, NextLaneId} ->
            workflow_iterator_snapshot:save(ExecutionId, LaneIndex, LaneId, 0, IteratorToSave, NextLaneId);
        ?WF_ERROR_LANE_ALREADY_PREPARED -> ok;
        ?WF_ERROR_PREPARATION_FAILED -> ok
    end;
report_lane_execution_prepared(Handler, ExecutionId, LaneId, ?PREPARE_IN_ADVANCE, prepare, {ok, LaneSpec} = Ans) ->
    case update(ExecutionId, fun(State) -> finish_lane_preparation_in_advance(State, LaneId, LaneSpec) end) of
        {ok, _} -> ok;
        ?WF_ERROR_UNKNOWN_LANE -> ok;
        ?WF_ERROR_CURRENT_LANE -> report_lane_execution_prepared(Handler, ExecutionId, LaneId, ?PREPARE_CURRENT, prepare, Ans)
    end;
report_lane_execution_prepared(_Handler, ExecutionId, LaneId, ?PREPARE_CURRENT, _, error) ->
    case update(ExecutionId, fun(State) ->
        handle_lane_preparation_failure(reset_state_fields_for_next_lane(State), LaneId, ?PREPARE_CURRENT)
    end) of
        {ok, _} -> ok;
        ?WF_ERROR_UNKNOWN_LANE -> ok
    end;
report_lane_execution_prepared(_Handler, ExecutionId, LaneId, ?PREPARE_IN_ADVANCE, _, error) ->
    case update(ExecutionId, fun(State) -> handle_lane_preparation_failure(State, LaneId, ?PREPARE_IN_ADVANCE) end) of
        {ok, _} -> ok;
        ?WF_ERROR_UNKNOWN_LANE -> ok
    end.

-spec pause_job(workflow_engine:execution_id(), workflow_jobs:job_identifier()) ->
    {ok, workflow_engine:id(), workflow_engine:task_spec()}.
pause_job(ExecutionId, JobIdentifier) ->
    {ok, #document{value = #workflow_execution_state{
        engine_id = EngineId,
        current_lane = #current_lane{parallel_box_specs = BoxSpecs}
    }}} = update(ExecutionId, fun(State) -> pause_job_internal(State, JobIdentifier) end),
    {_TaskId, PausedTaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxSpecs),
    {ok, EngineId, PausedTaskSpec}.


-spec report_new_streamed_task_data(workflow_engine:execution_id(), workflow_engine:task_id(),
    workflow_engine:streamed_task_data()) -> {ok, workflow_engine:id()}.
report_new_streamed_task_data(ExecutionId, TaskId, TaskData) ->
    CachedTaskDataId = workflow_cached_task_data:put(TaskData),
    UpdateAns = update(ExecutionId, fun
        (#workflow_execution_state{
            execution_status = #execution_cancelled{is_interrupted = true},
            engine_id = EngineId
        }) ->
            ?WF_ERROR_EXCEPTION_APPEARED(EngineId);
        (State = #workflow_execution_state{tasks_data_registry = TasksDataRegistry}) ->
            {ok, State#workflow_execution_state{
                tasks_data_registry = workflow_tasks_data_registry:put(TaskId, CachedTaskDataId, TasksDataRegistry)
            }}
    end),

    case UpdateAns of
        {ok, #document{value = #workflow_execution_state{engine_id = EngineId}}} ->
            {ok, EngineId};
        ?WF_ERROR_EXCEPTION_APPEARED(EngineId) ->
            workflow_cached_task_data:delete(CachedTaskDataId),
            {ok, EngineId}
    end.


-spec report_streamed_task_data_processed(workflow_engine:execution_id(), workflow_engine:task_id(),
    workflow_cached_task_data:id(), workflow_handler:handler_execution_result()) -> ok.
report_streamed_task_data_processed(ExecutionId, TaskId, CachedTaskDataId, _HandlerExecutionResult) ->
    {ok, #document{value = #workflow_execution_state{
        execution_status = Status,
        update_report = ?TASK_PROCESSED_REPORT(TaskStatus)
    }} = Doc} =
        update(ExecutionId, fun(State) ->
            % Engine does not analyse result. If stream processing fails, upper layer
            % is responsible for cancelling workflow.
            {ok, mark_task_data_processed(TaskId, CachedTaskDataId, State)}
        end),

    case {Status, TaskStatus} of
        {?EXECUTION_CANCELLED(Counter), _} when Counter > 0 ->
            ok;
        {_, ended} ->
            handle_task_execution_stopped(Doc, TaskId, TaskId);
        {_, ongoing} ->
            ok
    end.

-spec mark_all_streamed_task_data_received(workflow_engine:execution_id(), workflow_engine:task_id(),
    workflow_engine:stream_closing_result()) -> {ok, workflow_engine:id()}.
mark_all_streamed_task_data_received(ExecutionId, TaskId, success) ->
    {ok, #document{value = #workflow_execution_state{
        execution_status = Status,
        engine_id = EngineId,
        update_report = ?TASK_PROCESSED_REPORT(TaskStatus)
    }} = Doc} =
        update(ExecutionId, fun(State) -> {ok, mark_all_streamed_task_data_received_internal(TaskId, State)} end),

    case {Status, TaskStatus} of
        {?EXECUTION_CANCELLED(Counter), _} when Counter > 0 ->
            ok;
        {_, ended} ->
            handle_task_execution_stopped(Doc, TaskId, TaskId);
        {_, waiting_for_data_stream_finalization} ->
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
    case update(ExecutionId, fun(State) -> reset_keepalive_timer_internal(State, JobIdentifier) end) of
        {ok, _} -> ok;
        ?ERROR_NOT_FOUND -> ok
    end.

-spec get_result_processing_data(workflow_engine:execution_id(), workflow_jobs:job_identifier()) ->
    {workflow_handler:handler(), workflow_engine:execution_context(), workflow_engine:task_id()}.
get_result_processing_data(ExecutionId, JobIdentifier) ->
    {ok, #workflow_execution_state{
        handler = Handler,
        current_lane = #current_lane{parallel_box_specs = BoxSpecs, execution_context = Context}
    }} = get(ExecutionId),
    {TaskId, _TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxSpecs),
    {Handler, Context, TaskId}.


-spec get(workflow_engine:execution_id()) -> {ok, state()} | ?ERROR_NOT_FOUND.
get(ExecutionId) ->
    case datastore_model:get(?CTX, ExecutionId) of
        {ok, #document{value = State}} ->
            {ok, State};
        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND
    end.

-spec save(doc()) -> ok.
save(Doc) ->
    {ok, _} = datastore_model:save(?CTX, Doc),
    ok.


-spec get_boxes_map(state()) -> boxes_map().
get_boxes_map(#workflow_execution_state{current_lane = #current_lane{parallel_box_specs = undefined}}) ->
    #{};
get_boxes_map(#workflow_execution_state{current_lane = #current_lane{parallel_box_specs = BoxSpecs}}) ->
    BoxSpecs.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec finish_lane_preparation(
    workflow_handler:handler(),
    workflow_engine:execution_id(),
    workflow_engine:lane_spec(),
    init_type()
) ->
    {ok, current_lane(), iterator:iterator(), workflow_engine:lane_id() | undefined} |
    ?WF_ERROR_PREPARATION_FAILED | ?WF_ERROR_LANE_ALREADY_PREPARED.
finish_lane_preparation(Handler, ExecutionId,
    #{
        parallel_boxes := Boxes,
        iterator := Iterator,
        execution_context := LaneExecutionContext
    } = LaneSpec,
    InitType
) ->
    case Boxes of
        [] when InitType =:= prepare ->
            % workflow_jobs require at least one parallel_boxes in lane
            ?error("No parallel boxes for lane ~p of execution id: ~p", [LaneSpec, ExecutionId]),
            {ok, _} = update(ExecutionId, fun(State) ->
                handle_lane_preparation_failure(reset_state_fields_for_next_lane(State), undefined, ?PREPARE_CURRENT)
            end),
            ?WF_ERROR_PREPARATION_FAILED;
        _ ->
            BoxesMap = lists:foldl(fun({BoxIndex, BoxSpec}, BoxesAcc) ->
                Tasks = lists:foldl(fun({TaskIndex, {TaskId, TaskSpec}}, TaskAcc) ->
                    TaskAcc#{TaskIndex => {TaskId, TaskSpec}}
                end, #{}, lists_utils:enumerate(maps:to_list(BoxSpec))),
                BoxesAcc#{BoxIndex => Tasks}
            end, #{}, lists_utils:enumerate(Boxes)),

            NextIterationStep = get_next_iterator(Handler, LaneExecutionContext, Iterator, ExecutionId),
            FailureCountToCancel = maps:get(failure_count_to_cancel, LaneSpec, undefined),
            case update(ExecutionId, fun(#workflow_execution_state{jobs = Jobs} = State) ->
                State2 = case InitType of
                    ?RESUMING_FROM_DUMP(_, OriginalIndexMap) ->
                        State#workflow_execution_state{
                            jobs = workflow_jobs:fix_resumed_indexes(Jobs, BoxesMap, OriginalIndexMap)
                        };
                    _ ->
                        reset_state_fields_for_next_lane(State)
                end,
                finish_lane_preparation_internal(
                    State2, BoxesMap, LaneExecutionContext, NextIterationStep, FailureCountToCancel
                )
            end) of
                {ok, #document{value = #workflow_execution_state{
                    execution_status = ?EXECUTION_CANCELLED(_), current_lane = CurrentLane,
                    next_lane = #next_lane{id = NextLaneId}
                }}} ->
                    {ok, CurrentLane, Iterator, NextLaneId};
                {ok, #document{value = #workflow_execution_state{
                    current_lane = CurrentLane,
                    next_lane = #next_lane{id = NextLaneId}
                }} = UpdatedDoc} when NextIterationStep =:= undefined ->
                    case InitType of
                        ?RESUMING_FROM_DUMP(_, _) ->
                            {ok, _} = update(ExecutionId, fun(StateToUpdate) ->
                                remove_pending_callback(StateToUpdate, ?CALLBACKS_ON_EMPTY_LANE_SELECTOR)
                            end);
                        _ ->
                            call_callbacks_for_empty_lane(UpdatedDoc, ?CALLBACKS_ON_EMPTY_LANE_SELECTOR)
                    end,
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

-spec call_callbacks_for_empty_lane(doc(), callback_selector()) -> ok.
call_callbacks_for_empty_lane(#document{value = #workflow_execution_state{
    current_lane = #current_lane{parallel_box_specs = BoxesMap}
}} = Doc, CallbackSelector) ->
    call_callbacks_for_tasks(Doc, BoxesMap, CallbackSelector).

-spec call_callbacks_for_partially_ended_lane(doc(), callback_selector(),
    #{workflow_jobs:job_identifier() => [workflow_jobs:job_identifier()]}) -> ok.
call_callbacks_for_partially_ended_lane(#document{value = #workflow_execution_state{
    current_lane = #current_lane{parallel_box_specs = BoxesMap, parallel_box_count = BoxesCount}
}} = Doc, CallbackSelector, WaitingOrOngoingTasksIndexes) ->
    case maps:size(WaitingOrOngoingTasksIndexes) of
        0 ->
            call_callbacks_for_tasks(Doc, BoxesMap, CallbackSelector);
        _ ->
            MinBoxIndex = lists:min(maps:keys(WaitingOrOngoingTasksIndexes)),
            FilteredBoxesMap = maps:map(fun(BoxIndex, BoxSpec) ->
                TaskIndexes = maps:get(BoxIndex, WaitingOrOngoingTasksIndexes, []),
                maps:without(TaskIndexes, BoxSpec)
            end, maps:without(lists:seq(MinBoxIndex + 1, BoxesCount), BoxesMap)),
            call_callbacks_for_tasks(Doc, FilteredBoxesMap, CallbackSelector)
    end.


-spec call_callbacks_for_tasks(doc(), boxes_map(), callback_selector()) -> ok.
call_callbacks_for_tasks(#document{key = ExecutionId, value = #workflow_execution_state{
    handler = Handler,
    current_lane = #current_lane{execution_context = Context}
}}, FilteredBoxesMap, CallbackSelector) ->
    TaskIds = get_task_ids(FilteredBoxesMap),
    TaskIdsWithStreams = get_task_ids_with_streams(FilteredBoxesMap),
    workflow_engine:call_handle_task_results_processed_for_all_items_for_all_tasks(
        ExecutionId, Handler, Context, TaskIdsWithStreams),
    workflow_engine:call_handle_task_execution_stopped_for_all_tasks(ExecutionId, Handler, Context,
        TaskIds -- TaskIdsWithStreams),

    {ok, _} = update(ExecutionId, fun(StateToUpdate) ->
        remove_pending_callback(StateToUpdate, CallbackSelector)
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
        workflow_engine:lane_id(), workflow_engine:preparation_mode(), init_type()
    ) | ?WF_ERROR_LANE_EXECUTION_CANCELLED(
        workflow_handler:handler(), workflow_engine:lane_id(),
        workflow_engine:execution_context(), [workflow_engine:task_id()]
    ) | ?EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT(
        workflow_handler:handler(), workflow_engine:execution_context(), [workflow_engine:task_id()]
    ) | ?ERROR_NOT_FOUND.
prepare_next_job_for_current_lane(ExecutionId) ->
    case update(ExecutionId, fun prepare_next_waiting_job/1) of
        {ok, Doc} ->
            handle_state_update_after_job_preparation(ExecutionId, Doc);
        ?WF_ERROR_NO_CACHED_ITEMS(LaneIndex, ItemIndex, IterationStep, Handler, Context) ->
            prepare_next_job_using_iterator(ExecutionId, ItemIndex, IterationStep, LaneIndex, Handler, Context);
        {error, _} = Error ->
            Error
    end.

-spec prepare_next_job_using_iterator(workflow_engine:execution_id(), index(), iteration_status(),
    index(), workflow_handler:handler(), workflow_engine:execution_context()) ->
    {ok, workflow_engine:execution_spec()} | no_items_error() |
    ?WF_ERROR_LANE_EXECUTION_CANCELLED(
        workflow_handler:handler(), workflow_engine:lane_id(),
        workflow_engine:execution_context(), [workflow_engine:task_id()]
    ) | ?EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT(
        workflow_handler:handler(), workflow_engine:execution_context(), [workflow_engine:task_id()]
    ) | ?ERROR_NOT_FOUND.
prepare_next_job_using_iterator(ExecutionId, ItemIndex, CurrentIterationStep, LaneIndex, Handler, Context) ->
    NextIterationStep = case CurrentIterationStep of
        undefined ->
            undefined;
        {_, CurrentIterator} ->
            % TODO VFS-7787 return (to engine) item instead of item_id in this case (engine must read from cache when we have item here)
            % Maybe generate item_id using index (there will be no need to translate job to datastore key)?
            get_next_iterator(Handler, Context, CurrentIterator, ExecutionId)
    end,

    ParallelBoxToStart = 1, % TODO VFS-7788 - get ParallelBoxToStart from iterator
    case update(ExecutionId, fun(State) ->
        handle_next_iteration_step(State, LaneIndex, ItemIndex, NextIterationStep, ParallelBoxToStart)
    end) of
        {ok, #document{value = #workflow_execution_state{
            update_report = ?DELETE_ITEM(ItemId)
        }}} ->
            workflow_cached_item:delete(ItemId),
            prepare_next_job_for_current_lane(ExecutionId);
        {ok, #document{value = #workflow_execution_state{
            update_report = ?EXECUTE_CALLBACKS_ON_PARTIALLY_ENDED_LANE(WaitingOrOngoingTasksIndexes)
        }} = UpdatedDoc} ->
            call_callbacks_for_partially_ended_lane(
                UpdatedDoc, ?CALLBACKS_ON_PARTIALLY_ENDED_LANE_SELECTOR, WaitingOrOngoingTasksIndexes),
            prepare_next_job_for_current_lane(ExecutionId);
        {ok, Doc} ->
            handle_state_update_after_job_preparation(ExecutionId, Doc);
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


-spec handle_state_update_after_job_preparation(workflow_engine:execution_id(), doc()) ->
    {ok, workflow_engine:execution_spec()} | no_items_error() |
    ?PREPARE_LANE_EXECUTION(
        workflow_handler:handler(), workflow_engine:execution_context(),
        workflow_engine:lane_id(), workflow_engine:preparation_mode(), init_type()
    ) | ?WF_ERROR_LANE_EXECUTION_CANCELLED(
        workflow_handler:handler(), workflow_engine:lane_id(),
        workflow_engine:execution_context(), [workflow_engine:task_id()]
    ) | ?EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT(
        workflow_handler:handler(), workflow_engine:execution_context(), [workflow_engine:task_id()]
    ) | ?ERROR_NOT_FOUND.
handle_state_update_after_job_preparation(_ExecutionId, #document{value = #workflow_execution_state{
    update_report = #job_prepared_report{
        job_identifier = JobIdentifier,
        task_id = TaskId,
        task_spec = TaskSpec,
        subject_id = SubjectId
    },
    handler = Handler,
    current_lane = #current_lane{execution_context = LaneContext}
}}) ->
    {ok, #execution_spec{
        handler = Handler,
        context = LaneContext,
        task_id = TaskId,
        task_spec = TaskSpec,
        subject_id = SubjectId,
        job_identifier = JobIdentifier
    }};
handle_state_update_after_job_preparation(_ExecutionId, #document{value = #workflow_execution_state{
    update_report = ?LANE_DESIGNATED_FOR_INIT(InitType),
    handler = Handler,
    initial_context = ExecutionContext,
    current_lane = #current_lane{id = LaneId}
}}) ->
    ?PREPARE_LANE_EXECUTION(Handler, ExecutionContext, LaneId, ?PREPARE_CURRENT, InitType);
handle_state_update_after_job_preparation(_ExecutionId, #document{value = #workflow_execution_state{
    update_report = ?LANE_DESIGNATED_FOR_PREPARATION_IN_ADVANCE,
    handler = Handler,
    initial_context = ExecutionContext,
    next_lane = #next_lane{id = LaneId}
}}) ->
    ?PREPARE_LANE_EXECUTION(Handler, ExecutionContext, LaneId, ?PREPARE_IN_ADVANCE, prepare);
handle_state_update_after_job_preparation(ExecutionId, #document{value = #workflow_execution_state{
    update_report = ?LANE_READY_TO_BE_FINISHED_REPORT(FinishedLaneId, LaneContext),
    handler = Handler,
    initial_context = ExecutionContext,
    next_lane_preparation_status = NextLaneStatus
}} = Doc) ->
    case workflow_engine:call_handler(ExecutionId, LaneContext, Handler, handle_lane_execution_stopped, [FinishedLaneId]) of
        ?CONTINUE(NextLaneId, LaneIdToBePreparedInAdvance) ->
            case update(ExecutionId, fun(State) -> set_current_lane(State, NextLaneId, LaneIdToBePreparedInAdvance) end) of
                {ok, #document{value = #workflow_execution_state{
                    execution_status = ?PREPARED_IN_ADVANCE,
                    update_report = ?LANE_PREPARED_REPORT(#next_lane{
                        id = LaneId,
                        lane_spec = LaneSpec
                    })
                }}} ->
                    report_lane_execution_prepared(Handler, ExecutionId, LaneId, ?PREPARE_CURRENT, prepare, {ok, LaneSpec}),
                    prepare_next_job_for_current_lane(ExecutionId);
                {ok, #document{value = #workflow_execution_state{
                    execution_status = ?PREPARING,
                    update_report = ?LANE_DESIGNATED_FOR_INIT(InitType),
                    current_lane = #current_lane{index = IndexToSnapshot, id = IdToSnapshot},
                    next_lane = #next_lane{id = NextLaneIdToSnapshot}
                }}} ->
                    workflow_iterator_snapshot:save(
                        ExecutionId, IndexToSnapshot, IdToSnapshot, 0, undefined, NextLaneIdToSnapshot),
                    ?PREPARE_LANE_EXECUTION(Handler, ExecutionContext, NextLaneId, ?PREPARE_CURRENT, InitType);
                {ok, #document{value = #workflow_execution_state{
                    execution_status = ?PREPARATION_FAILED,
                    current_lane = #current_lane{index = IndexToSnapshot, id = IdToSnapshot},
                    next_lane = #next_lane{id = NextLaneIdToSnapshot}
                }}} ->
                    workflow_iterator_snapshot:save(
                        ExecutionId, IndexToSnapshot, IdToSnapshot, 0, undefined, NextLaneIdToSnapshot),
                    ?WF_ERROR_EXECUTION_ENDED(#execution_ended{handler = Handler, context = ExecutionContext});
                {ok, #document{value = #workflow_execution_state{
                    current_lane = #current_lane{index = IndexToSnapshot, id = IdToSnapshot},
                    next_lane = #next_lane{id = NextLaneIdToSnapshot}
                }}} ->
                    workflow_iterator_snapshot:save(
                        ExecutionId, IndexToSnapshot, IdToSnapshot, 0, undefined, NextLaneIdToSnapshot),
                    ?WF_ERROR_NO_WAITING_ITEMS
            end;
        % Other possible answers are ?END_EXECUTION or error - error is logged by workflow_engine:call_handler/5 function
        HandlerAns when NextLaneStatus =:= ?PREPARING ->
            maybe_save_iterator_on_workflow_end_or_error(HandlerAns, Doc),
            case update(ExecutionId, fun maybe_wait_for_preparation_in_advance/1) of
                ?WF_ERROR_LANE_ALREADY_PREPARED ->
                    % TODO VFS-9993 - test if exception handler is called when exception appears here
                    ?WF_ERROR_EXECUTION_ENDED(#execution_ended{handler = Handler, context = ExecutionContext});
                {ok, _} ->
                    % TODO VFS-9993 - test if exception handler is called when exception appears here
                    ?WF_ERROR_NO_WAITING_ITEMS
            end;
        HandlerAns ->
            % TODO VFS-9993 - test if exception handler is called when exception appears here
            maybe_save_iterator_on_workflow_end_or_error(HandlerAns, Doc),
            ?WF_ERROR_EXECUTION_ENDED(#execution_ended{handler = Handler, context = ExecutionContext})
    end;
handle_state_update_after_job_preparation(_ExecutionId, #document{value = #workflow_execution_state{
    update_report = ?EXECUTION_CANCELLED_REPORT(ItemIdsToDelete, TaskIdsToFinish, ResultIdsToDel, TaskDataIds),
    current_lane = #current_lane{id = LaneId, execution_context = LaneContext},
    execution_status = ExecutionStatus,
    handler = Handler,
    initial_context = ExecutionContext,
    prefetched_iteration_step = PrefetchedIterationStep
}}) ->
    lists:foreach(fun workflow_cached_item:delete/1, ItemIdsToDelete),
    lists:foreach(fun workflow_cached_async_result:delete/1, ResultIdsToDel),
    lists:foreach(fun workflow_cached_task_data:delete/1, TaskDataIds),

    case PrefetchedIterationStep of
        undefined -> ok;
        {PrefetchedItemId, _} -> workflow_cached_item:delete(PrefetchedItemId)
    end,
    % TODO VFS-7787 - test cancel during lane_execution_finished callback execution
    case ExecutionStatus of
        ?WAITING_FOR_ERROR_HANDLING_OR_NEXT_LANE_PREPARATION_END(false) ->
            ?WF_ERROR_LANE_EXECUTION_CANCELLED(Handler, LaneId, LaneContext, TaskIdsToFinish);
        ?WAITING_FOR_ERROR_HANDLING_OR_NEXT_LANE_PREPARATION_END(true) ->
            ?WF_ERROR_NO_WAITING_ITEMS;
        #execution_cancelled{is_interrupted = true, abrupt_stop_reason = Reason} ->
            ?WF_ERROR_EXECUTION_ENDED(#execution_ended{handler = Handler, context = ExecutionContext,
                final_callback = {handle_workflow_abruptly_stopped, Reason}});
        _ ->
            ?WF_ERROR_EXECUTION_ENDED(#execution_ended{handler = Handler, context = ExecutionContext,
                lane_callbacks = {true, LaneId, LaneContext, TaskIdsToFinish}})
    end;
handle_state_update_after_job_preparation(_ExecutionId, #document{value = #workflow_execution_state{
    update_report = ?EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT(TaskIdsWithStreamsToFinish),
    current_lane = #current_lane{execution_context = LaneContext},
    handler = Handler
}}) ->
    ?EXECUTION_CANCELLED_WITH_OPEN_STREAMS_REPORT(LaneContext, Handler, TaskIdsWithStreamsToFinish);
handle_state_update_after_job_preparation(ExecutionId, Doc = #document{value = #workflow_execution_state{
    update_report = ?EXECUTE_DELAYED_CALLBACKS(Callbacks)
}}) ->
    lists:foreach(fun({CallbackFun, CallbackSelector, CallbackArgs}) ->
        apply(CallbackFun, [Doc, CallbackSelector | CallbackArgs])
    end, Callbacks),
    prepare_next_job_for_current_lane(ExecutionId);
handle_state_update_after_job_preparation(ExecutionId, #document{value = #workflow_execution_state{
    update_report = ?ITEM_RESUMED
}}) ->
    prepare_next_job_for_current_lane(ExecutionId);
handle_state_update_after_job_preparation(_ExecutionId, #document{value = #workflow_execution_state{
    update_report = {error, _} = UpdateReport
}}) ->
    UpdateReport.


-spec maybe_save_iterator_on_workflow_end_or_error(?END_EXECUTION | error, doc()) -> ok.
maybe_save_iterator_on_workflow_end_or_error(HandlerAns, #document{key = ExecutionId, value = #workflow_execution_state{
    current_lane = #current_lane{index = LaneIndex},
    execution_status = Status
}}) ->
    case {HandlerAns, Status} of
        {error, _} ->
            ok;
        {_, #execution_cancelled{}} ->
            ok;
        _ ->
            % Double cancel check as cancel could be called during handler execution
            case get(ExecutionId) of
                {ok, #workflow_execution_state{execution_status = #execution_cancelled{}}} -> ok;
                _ -> workflow_iterator_snapshot:save(ExecutionId, LaneIndex, undefined, 0, undefined, undefined)
            end
    end.


-spec maybe_report_item_error(doc(), workflow_cached_item:id() | undefined) -> ok.
maybe_report_item_error(_Doc, undefined = _ItemIdToReportError) ->
    ok;
maybe_report_item_error(
    #document{value = #workflow_execution_state{execution_status = ?EXECUTION_CANCELLED(Counter)}}, _
) when Counter > 0 ->
    ok;
maybe_report_item_error(#document{key = ExecutionId, value = #workflow_execution_state{
    handler = Handler,
    current_lane = #current_lane{execution_context = Context}
}} = Doc, ItemIdToReportError) ->
    Item = workflow_cached_item:get_item(ItemIdToReportError),
    workflow_engine:call_handler(ExecutionId, Context, Handler, report_item_error, [Item]),

    case update(ExecutionId, fun(#workflow_execution_state{items_to_process = ItemsToProcess} = State) ->
        case maps:get(ItemIdToReportError, ItemsToProcess, undefined) of
            [report] ->
                {ok, State#workflow_execution_state{items_to_process = maps:remove(ItemIdToReportError, ItemsToProcess)}};
            [report, snapshot] ->
                {ok, State#workflow_execution_state{items_to_process = maps:remove(ItemIdToReportError, ItemsToProcess)}};
            [report, delete] ->
                {ok, State#workflow_execution_state{
                    items_to_process = maps:remove(ItemIdToReportError, ItemsToProcess),
                    update_report = ?DELETE_ITEM
                }};
            undefined ->
                ?ERROR_NOT_FOUND
        end
    end) of
        {ok, #document{value = #workflow_execution_state{update_report = ?DELETE_ITEM}} = UpdatedDoc} ->
            delete_item_callback(UpdatedDoc, ItemIdToReportError);
        ?ERROR_NOT_FOUND ->
            delete_item_callback(Doc, ItemIdToReportError);
        _ ->
            {ok, _} = update(ExecutionId, fun(State) -> remove_pending_callback(State, ItemIdToReportError) end),
            ok
    end.

-spec delete_item_callback(doc(), workflow_cached_item:id()) -> ok.
delete_item_callback(#document{key = ExecutionId}, ItemIdToReportError) ->
    workflow_cached_item:delete(ItemIdToReportError),
    {ok, _} = update(ExecutionId, fun(State) -> remove_pending_callback(State, ItemIdToReportError) end),
    ok.

-spec maybe_delete_item(workflow_engine:execution_id(), workflow_cached_item:id()) -> ok.
maybe_delete_item(ExecutionId, ItemId) ->
    case update(ExecutionId, fun(#workflow_execution_state{items_to_process = ItemsToProcess} = State) ->
        case maps:get(ItemId, ItemsToProcess, undefined) of
            [report, snapshot] ->
                {ok, State#workflow_execution_state{items_to_process = maps:remove(ItemId, ItemsToProcess)}};
            [report, delete] ->
                ?WF_ERROR_RACE_CONDITION;
            undefined ->
                ?ERROR_NOT_FOUND
        end
    end) of
        {ok, _} -> ok;
        ?WF_ERROR_RACE_CONDITION -> ok;
        ?ERROR_NOT_FOUND -> workflow_cached_item:delete(ItemId)
    end.

-spec maybe_notify_task_execution_ended(doc(), workflow_jobs:job_identifier(), task_status()) -> ok.
maybe_notify_task_execution_ended(_Doc, _JobIdentifier, ongoing) ->
    ok;
maybe_notify_task_execution_ended(
    #document{value = #workflow_execution_state{execution_status = ?EXECUTION_CANCELLED(Counter)}}, _, _
) when Counter > 0 ->
    ok;
maybe_notify_task_execution_ended(#document{key = ExecutionId, value = #workflow_execution_state{
    handler = Handler,
    execution_status = Status,
    current_lane = #current_lane{parallel_box_specs = BoxSpecs, execution_context = Context}
}}, JobIdentifier, waiting_for_data_stream_finalization) ->
    {TaskId, _TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxSpecs),
    case Status of
        #execution_cancelled{is_interrupted = true} ->
            ok;
        _ ->
            workflow_engine:call_handler(
                ExecutionId, Context, Handler, handle_task_results_processed_for_all_items, [TaskId])
    end,
    {ok, _} = update(ExecutionId, fun(State) -> remove_pending_callback(State, JobIdentifier) end),
    ok;
maybe_notify_task_execution_ended(#document{value = #workflow_execution_state{
    current_lane = #current_lane{parallel_box_specs = BoxSpecs}
}} = Doc, JobIdentifier, ended) ->
    {TaskId, _TaskSpec} = workflow_jobs:get_task_details(JobIdentifier, BoxSpecs),
    handle_task_execution_stopped(Doc, JobIdentifier, TaskId).

-spec handle_task_execution_stopped(doc(), callback_selector(), workflow_engine:task_id()) -> ok.
handle_task_execution_stopped(#document{key = ExecutionId, value = #workflow_execution_state{
    handler = Handler,
    execution_status = Status,
    current_lane = #current_lane{execution_context = Context}
}}, CallbackSelector, TaskId) ->
    case Status of
        #execution_cancelled{is_interrupted = true} ->
            ok;
        _ ->
            workflow_engine:call_handler(ExecutionId, Context, Handler, handle_task_execution_stopped, [TaskId])
    end,
    {ok, _} = update(ExecutionId, fun(State) -> remove_pending_callback(State, CallbackSelector) end),
    ok.

-spec update(workflow_engine:execution_id(), update_fun()) -> {ok, doc()} | {error, term()}.
update(ExecutionId, UpdateFun) ->
    % TODO VFS-7787 - should we add try/catch or allow functions fail?
    datastore_model:update(?CTX, ExecutionId, fun(State) ->
        UpdateFun(State#workflow_execution_state{update_report = undefined})
    end).

-spec get_next_iterator(workflow_handler:handler(), workflow_engine:execution_context(),
    iterator:iterator(), workflow_engine:execution_id()) -> iteration_status().
get_next_iterator(Handler, Context, Iterator, ExecutionId) ->
    try
        case iterator:get_next(Context, Iterator) of
            {ok, NextItem, NextIterator} ->
                {workflow_cached_item:put(NextItem, NextIterator), NextIterator};
            stop ->
                undefined
        end
    catch
        Error:Reason:Stacktrace ->
            workflow_engine:handle_exception(
                ExecutionId, Handler, Context,
                "Unexpected error getting next iterator", [],
                Error, Reason, Stacktrace
            ),
            undefined
    end.


-spec maybe_delete_prefetched_iteration_step(iteration_status()) -> ok.
maybe_delete_prefetched_iteration_step(undefined) ->
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
            {?PREPARING, ?LANE_DESIGNATED_FOR_INIT(prepare)}
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

-spec handle_execution_cancel_init(state(), workflow_engine:cancel_pred()) -> {ok, state()} | ?WF_ERROR_PRED_NOT_MEET.
handle_execution_cancel_init(
    #workflow_execution_state{current_lane = #current_lane{id = LaneId}},
    #{lane_id := ExpectedLaneId}
) when LaneId =/= ExpectedLaneId ->
    ?WF_ERROR_PRED_NOT_MEET;
handle_execution_cancel_init(#workflow_execution_state{execution_status = ?PREPARING} = State, _) ->
    {ok, State#workflow_execution_state{execution_status = ?PREPARATION_CANCELLED(1)}};
handle_execution_cancel_init(#workflow_execution_state{execution_status = ?PREPARATION_FAILED} = State, _) ->
    % TODO VFS-7787 Return error to prevent document update
    {ok, State};
handle_execution_cancel_init(#workflow_execution_state{execution_status = #execution_cancelled{is_interrupted = true}} = State, _) ->
    % TODO VFS-7787 Return error to prevent document update
    {ok, State};
handle_execution_cancel_init(#workflow_execution_state{execution_status = ?EXECUTION_CANCELLED(Counter) = Status} = State, _) ->
    {ok, State#workflow_execution_state{execution_status = Status#execution_cancelled{call_count = Counter + 1}}};
handle_execution_cancel_init(State, _) ->
    {ok, State#workflow_execution_state{execution_status = ?EXECUTION_CANCELLED(1)}}.

-spec handle_execution_cancel_finish(state()) -> {ok, state()} | ?WF_ERROR_CANCEL_NOT_INITIALIZED.
handle_execution_cancel_finish(#workflow_execution_state{execution_status = ?EXECUTION_CANCELLED(Counter) = Status} = State)
    when Counter > 0 ->
    {ok, State#workflow_execution_state{execution_status = Status#execution_cancelled{call_count = Counter - 1}}};
handle_execution_cancel_finish(_State) ->
    ?WF_ERROR_CANCEL_NOT_INITIALIZED.

-spec mark_exception_appeared(state(), workflow_engine:execution_context(),
    throw | error | exit, term(), list()) -> {ok, state()}.
mark_exception_appeared(
    #workflow_execution_state{execution_status = #execution_cancelled{is_interrupted = true}} = State,
    _Context, _ErrorType, _Reason, _Stacktrace
) ->
    {ok, State};
mark_exception_appeared(
    #workflow_execution_state{
        execution_status = #execution_cancelled{callbacks_to_execute = Callbacks} = Status
    } = State,
    Context, ErrorType, Reason, Stacktrace
) ->
    {ok, State#workflow_execution_state{execution_status = Status#execution_cancelled{
        call_count = 0,
        is_interrupted = true,
        callbacks_to_execute = [
            {fun execute_exception_handler/6, ?CALLBACK_ON_EXCEPTION, [Context, ErrorType, Reason, Stacktrace]} | Callbacks
        ]
    }}};
mark_exception_appeared(State, Context, ErrorType, Reason, Stacktrace) ->
    {ok, State#workflow_execution_state{execution_status = #execution_cancelled{
        call_count = 0,
        is_interrupted = true,
        callbacks_to_execute = [
            {fun execute_exception_handler/6, ?CALLBACK_ON_EXCEPTION, [Context, ErrorType, Reason, Stacktrace]}
        ]
    }}}.

-spec check_waiting_exception_handler(state()) -> {ok, state()} | ?ERROR_NOT_FOUND.
check_waiting_exception_handler(#workflow_execution_state{
    execution_status = #execution_cancelled{
        call_count = 0,
        is_interrupted = true,
        callbacks_to_execute = [{_, ?CALLBACK_ON_EXCEPTION, _} = ExceptionCallback | _]
    } = Status
} = State) ->
    {ok, State#workflow_execution_state{
        execution_status = Status#execution_cancelled{callbacks_to_execute = []},
        update_report = ?EXECUTE_DELAYED_CALLBACKS([ExceptionCallback])
    }};
check_waiting_exception_handler(_) ->
    ?ERROR_NOT_FOUND.

-spec mark_workflow_abandoned(state(), workflow_handler:abrupt_stop_reason()) -> {ok, state()}.
mark_workflow_abandoned(
    #workflow_execution_state{execution_status = #execution_cancelled{is_interrupted = true}} = State, _InterruptReason
) ->
    {ok, State};
mark_workflow_abandoned(State, InterruptReason) ->
    {ok, State#workflow_execution_state{execution_status = #execution_cancelled{
        call_count = 0,
        is_interrupted = true,
        abrupt_stop_reason = InterruptReason
    }}}.


-spec execute_exception_handler(doc(), callback_selector(), workflow_engine:execution_context(),
    throw | error | exit, term(), list()) -> ok.
execute_exception_handler(#document{key = ExecutionId, value = #workflow_execution_state{
    handler = Handler
}}, CallbackSelector, Context, ErrorType, Reason, Stacktrace) ->
    InterruptReason = workflow_engine:execute_exception_handler(
        ExecutionId, Context, Handler, ErrorType, Reason, Stacktrace),
    {ok, _} = update(ExecutionId, fun(#workflow_execution_state{execution_status = Status} = State) ->
        remove_pending_callback(State#workflow_execution_state{
            execution_status = Status#execution_cancelled{abrupt_stop_reason = InterruptReason}
        }, CallbackSelector)
    end),
    ok.


-spec reset_state_fields_for_next_lane(state()) -> state().
reset_state_fields_for_next_lane(#workflow_execution_state{execution_status = ?RESUMING_FROM_DUMP(_, _)} = State) ->
    State;
reset_state_fields_for_next_lane(State) ->
    State#workflow_execution_state{
        iteration_state = workflow_iteration_state:init(),
        jobs = workflow_jobs:init(),
        tasks_data_registry = workflow_tasks_data_registry:empty()
    }.


-spec handle_lane_preparation_failure(state(), workflow_engine:lane_id() | undefined,
    workflow_engine:preparation_mode()) -> {ok, state()} | ?WF_ERROR_UNKNOWN_LANE.
handle_lane_preparation_failure(
    #workflow_execution_state{
        execution_status = #execution_cancelled{is_interrupted = true} = Status,
        next_lane_preparation_status = ?PREPARING
    } = State, _LaneId, ?PREPARE_CURRENT) ->
    {ok, State#workflow_execution_state{
        execution_status = Status#execution_cancelled{execution_step = waiting_on_next_lane_prepare, has_lane_preparation_failed = true}
    }};
handle_lane_preparation_failure(
    #workflow_execution_state{
        next_lane_preparation_status = ?PREPARING
    } = State, _LaneId, ?PREPARE_CURRENT) ->
    {ok, State#workflow_execution_state{execution_status = #execution_cancelled{has_lane_preparation_failed = true,
        execution_step = waiting_on_next_lane_prepare, call_count = 0, is_interrupted = false}}};
handle_lane_preparation_failure(
    #workflow_execution_state{
        execution_status = ?EXECUTION_CANCELLED(_) = Status
    } = State, _LaneId, ?PREPARE_CURRENT) ->
    {ok, State#workflow_execution_state{
        execution_status = Status#execution_cancelled{execution_step = finishing_execution, has_lane_preparation_failed = true}
    }};
handle_lane_preparation_failure(State, _LaneId, ?PREPARE_CURRENT) ->
    {ok, State#workflow_execution_state{execution_status = ?PREPARATION_FAILED}};
handle_lane_preparation_failure(
    #workflow_execution_state{
        execution_status = #execution_cancelled{execution_step = waiting_on_next_lane_prepare} = Status,
        next_lane = #next_lane{
            id = LaneId
        }
    } = State, LaneId, _LaneType) ->
    {ok, State#workflow_execution_state{
        execution_status = Status#execution_cancelled{execution_step = finishing_execution},
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
        execution_status = #execution_cancelled{} = Status,
        current_lane = #current_lane{parallel_box_specs = undefined} = CurrentLane
    } = State, BoxesMap, LaneExecutionContext, PrefetchedIterationStep, _FailureCountToCancel) ->
    {ok, State#workflow_execution_state{
        execution_status = Status#execution_cancelled{execution_step = lane_execution},
        current_lane = CurrentLane#current_lane{execution_context = LaneExecutionContext, parallel_box_specs = BoxesMap},
        prefetched_iteration_step = PrefetchedIterationStep,
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
    State2 = case PrefetchedIterationStep of
        undefined ->
            register_if_callback_is_pending(State,
                fun call_callbacks_for_empty_lane/2, ?CALLBACKS_ON_EMPTY_LANE_SELECTOR, [], true);
        _ ->
            State
    end,

    {ok, State2#workflow_execution_state{
        execution_status = ?EXECUTING,
        current_lane = UpdatedCurrentLane,
        prefetched_iteration_step = PrefetchedIterationStep,
        tasks_data_registry = workflow_tasks_data_registry:empty()
    }};
finish_lane_preparation_internal(_State, _BoxesMap, _LaneExecutionContext,
    _PrefetchedIterationStep, _FailureCountToCancel) ->
    ?WF_ERROR_LANE_ALREADY_PREPARED.

-spec finish_lane_preparation_in_advance(state(), workflow_engine:lane_id(), workflow_engine:lane_spec()) ->
    {ok, state()} | ?WF_ERROR_CURRENT_LANE | ?WF_ERROR_UNKNOWN_LANE.
finish_lane_preparation_in_advance(
    #workflow_execution_state{
        execution_status = #execution_cancelled{execution_step = waiting_on_next_lane_prepare} = Status,
        next_lane = #next_lane{
            id = LaneId
        } = Lane
    } = State, LaneId, LaneSpec) ->
    {ok, State#workflow_execution_state{
        execution_status = Status#execution_cancelled{execution_step = finishing_execution},
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

-spec maybe_wait_for_preparation_in_advance(state()) -> {ok, state()} | ?WF_ERROR_LANE_ALREADY_PREPARED.
maybe_wait_for_preparation_in_advance(#workflow_execution_state{
    next_lane_preparation_status = ?PREPARING
} = State) ->
    {ok, State#workflow_execution_state{execution_status = ?WAITING_FOR_ERROR_HANDLING_OR_NEXT_LANE_PREPARATION_END(false)}};
maybe_wait_for_preparation_in_advance(_State) ->
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
    execution_status = ?EXECUTION_CANCELLED(_)
}, _LaneIndex, _PrevItemIndex, _NextIterationStep, _ParallelBoxToStart) ->
    ?WF_ERROR_RACE_CONDITION;
handle_next_iteration_step(State = #workflow_execution_state{
    incarnation_tag = IncarnationTag,
    jobs = Jobs,
    iteration_state = IterationState,
    prefetched_iteration_step = PrefetchedIterationStep,
    current_lane = #current_lane{index = LaneIndex, parallel_box_specs = BoxSpecs}
}, LaneIndex, PrevItemIndex, NextIterationStep, ParallelBoxToStart) ->
    CanUsePrefetchedIterationStep = case PrefetchedIterationStep of
        undefined -> false;
        _ -> true
    end,

    case {CanUsePrefetchedIterationStep, NextIterationStep} of
        {false, undefined} ->
            case resume_iteration(State) of
                {ok, State2} ->
                    {ok, State2#workflow_execution_state{
                        iteration_state = workflow_iteration_state:handle_iteration_finished(IterationState)}};
                already_resumed ->
                    State2 = State#workflow_execution_state{
                        iteration_state = workflow_iteration_state:handle_iteration_finished(IterationState)},
                    case prepare_next_waiting_job(State2) of
                        {ok, _} = OkAns -> OkAns;
                        Error -> {ok, State2#workflow_execution_state{update_report = Error}}
                    end
            end;
        {false, _} ->
            % TODO VFS-7787 - maybe call handle_iteration_finished/1 when NextIterationStep
            % is undefined first time (next case) to prevent this race
            ?WF_ERROR_RACE_CONDITION;
        {true, _} ->
            {PrefetchedItemId, _PrefetchedIterator} = PrefetchedIterationStep,
            % TODO VFS-7789 - it may be needed to allow registration of waiting items as a result of async call processing finish
            case workflow_iteration_state:register_item(
                IterationState, PrevItemIndex, PrefetchedItemId) of
                ?WF_ERROR_RACE_CONDITION = Error ->
                    Error;
                {already_processed_item, _, NewIterationState} ->
                    FinalJobs = case NextIterationStep of
                        undefined -> workflow_jobs:build_tasks_tree(Jobs);
                        _ -> Jobs
                    end,
                    {ok, State#workflow_execution_state{
                        update_report = ?DELETE_ITEM(PrefetchedItemId),
                        iteration_state = NewIterationState,
                        prefetched_iteration_step = NextIterationStep,
                        jobs = FinalJobs
                    }};
                {new_item, NewItemIndex, NewIterationState} ->
                    {NewJobs, ToStart} = workflow_jobs:populate_with_jobs_for_item(
                        Jobs, NewItemIndex, ParallelBoxToStart, BoxSpecs, IncarnationTag),
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
                    }};
                {resumed_item, _NewItemIndex, NewIterationState} ->
                    State2 = State#workflow_execution_state{
                        update_report = ?ITEM_RESUMED,
                        iteration_state = NewIterationState,
                        prefetched_iteration_step = NextIterationStep
                    },
                    case resume_iteration(State2) of
                        {ok, State3} ->
                            {ok, State3};
                        already_resumed ->
                            {ok, State2}
                    end
            end
    end;
handle_next_iteration_step(_State, _LaneIndex, _PrevItemIndex, _NextIterationStep, _ParallelBoxToStart) ->
    ?WF_ERROR_LANE_CHANGED.

-spec resume_iteration(state()) -> {ok, state()} | already_resumed.
resume_iteration(State = #workflow_execution_state{
    jobs = Jobs,
    iteration_state = IterationState
}) ->
    case workflow_iteration_state:resume_iteration(IterationState) of
        {ok, UpdatedIterationState} ->
            WaitingOrOngoingTasksIndexes = workflow_jobs:get_waiting_or_ongoing_tasks_indexes(Jobs),
            State2 = register_if_callback_is_pending(State, fun call_callbacks_for_partially_ended_lane/3,
                ?CALLBACKS_ON_PARTIALLY_ENDED_LANE_SELECTOR, [WaitingOrOngoingTasksIndexes], true),
            {ok, State2#workflow_execution_state{
                jobs = workflow_jobs:build_tasks_tree(Jobs),
                iteration_state = UpdatedIterationState,
                update_report = ?EXECUTE_CALLBACKS_ON_PARTIALLY_ENDED_LANE(WaitingOrOngoingTasksIndexes)
            }};
        already_resumed ->
            already_resumed
    end.

-spec pause_job_internal(state(), workflow_jobs:job_identifier()) -> {ok, state()}.
pause_job_internal(State = #workflow_execution_state{jobs = Jobs}, JobIdentifier) ->
    {ok, State#workflow_execution_state{
        jobs = workflow_jobs:schedule_resume_of_job(Jobs, JobIdentifier)
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
    ?WF_ERROR_NO_CACHED_ITEMS(index(), index(), iteration_status(),
        workflow_handler:handler(), workflow_engine:execution_context()).
prepare_next_waiting_job(State = #workflow_execution_state{
    execution_status = ?NOT_PREPARED
}) ->
    {ok, State#workflow_execution_state{execution_status = ?PREPARING, update_report = ?LANE_DESIGNATED_FOR_INIT(prepare)}};
prepare_next_waiting_job(State = #workflow_execution_state{
    execution_status = ?RESUMING(_, _, _) = Status
}) ->
    {ok, State#workflow_execution_state{
        execution_status = ?PREPARING,
        update_report = ?LANE_DESIGNATED_FOR_INIT(Status)
    }};
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
    prepare_next_waiting_job(State#workflow_execution_state{execution_status = ?EXECUTION_CANCELLED(0)});
prepare_next_waiting_job(State = #workflow_execution_state{
    handler = Handler,
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
            case workflow_iteration_state:can_process_items(IterationState) of
                true ->
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
                                    ?WF_ERROR_NO_CACHED_ITEMS(LaneIndex, ItemIndex, PrefetchedIterationStep, Handler, Context)
                            end
                    end;
                false ->
                    case workflow_iteration_state:get_last_registered_item_index(IterationState) of
                        undefined -> % iteration has finished
                            % Iteration should not be finished when workflow_iteration_state:can_process_items returns false
                            ?warning("Unexpected workflow_iteration_state"),
                            handle_no_waiting_items_error(State, ?ERROR_NOT_FOUND);
                        ItemIndex ->
                            ?WF_ERROR_NO_CACHED_ITEMS(LaneIndex, ItemIndex, PrefetchedIterationStep, Handler, Context)
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
    execution_status = ?PREPARATION_CANCELLED(_)
}) ->
    ?WF_ERROR_NO_WAITING_ITEMS;
prepare_next_waiting_job(#workflow_execution_state{
    execution_status = ?WAITING_FOR_NEXT_LANE_PREPARATION_END(_)
}) ->
    ?WF_ERROR_NO_WAITING_ITEMS;
prepare_next_waiting_job(#workflow_execution_state{
    execution_status = ?EXECUTION_ENDED_AFTER_CANCEL(true, Reason),
    pending_callbacks = [],
    handler = Handler,
    initial_context = ExecutionContext
}) ->
    ?WF_ERROR_EXECUTION_ENDED(#execution_ended{handler = Handler, context = ExecutionContext,
        final_callback = {handle_workflow_abruptly_stopped, Reason}});
prepare_next_waiting_job(#workflow_execution_state{
    execution_status = ?EXECUTION_ENDED_AFTER_CANCEL(_, _),
    pending_callbacks = [],
    current_lane = #current_lane{id = undefined},
    handler = Handler,
    initial_context = ExecutionContext
}) ->
    ?WF_ERROR_EXECUTION_ENDED(#execution_ended{handler = Handler, context = ExecutionContext});
prepare_next_waiting_job(#workflow_execution_state{
    execution_status = ?EXECUTION_ENDED_AFTER_CANCEL(_, _),
    pending_callbacks = [],
    handler = Handler,
    initial_context = ExecutionContext
}) ->
    ?WF_ERROR_EXECUTION_ENDED(#execution_ended{handler = Handler, context = ExecutionContext});
prepare_next_waiting_job(#workflow_execution_state{
    execution_status = ?EXECUTION_ENDED_AFTER_CANCEL(_, _)
}) ->
    ?WF_ERROR_NO_WAITING_ITEMS;
prepare_next_waiting_job(#workflow_execution_state{
    execution_status = ?PREPARATION_FAILED,
    handler = Handler,
    initial_context = Context
}) ->
    ?WF_ERROR_EXECUTION_ENDED(#execution_ended{handler = Handler, context = Context});
prepare_next_waiting_job(State = #workflow_execution_state{
    execution_status = #execution_cancelled{call_count = 0, is_interrupted = false, callbacks_to_execute = []},
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
    end;
prepare_next_waiting_job(State = #workflow_execution_state{
    execution_status = #execution_cancelled{
        call_count = 0, is_interrupted = true, callbacks_to_execute = []
    } = Status,
    jobs = Jobs,
    iteration_state = IterationState,
    next_lane_preparation_status = NextLaneStatus,
    pending_callbacks = PendingCallbacks,
    tasks_data_registry = TaskDataRegistry
}) ->
    case workflow_jobs:has_ongoing_jobs(Jobs)
        orelse PendingCallbacks =/= []
        orelse workflow_tasks_data_registry:has_ongoing(TaskDataRegistry)
    of
        true ->
            ?WF_ERROR_NO_WAITING_ITEMS;
        false ->
            ResultIds = workflow_jobs:get_all_async_cached_result_ids(Jobs),
            {TaskDataIds, UpdatedTaskDataRegistry} = workflow_tasks_data_registry:finalize(TaskDataRegistry),
            {ItemIds, UpdatedIterationState} = workflow_iteration_state:finalize(IterationState),
            case NextLaneStatus of
                ?PREPARING ->
                    {ok, State#workflow_execution_state{
                        iteration_state = UpdatedIterationState,
                        tasks_data_registry = UpdatedTaskDataRegistry,
                        execution_status = Status#execution_cancelled{execution_step = waiting_on_next_lane_prepare},
                        update_report = ?EXECUTION_CANCELLED_REPORT(ItemIds, [], ResultIds, TaskDataIds)
                    }};
                _ ->
                    {ok, State#workflow_execution_state{
                        iteration_state = UpdatedIterationState,
                        tasks_data_registry = UpdatedTaskDataRegistry,
                        update_report = ?EXECUTION_CANCELLED_REPORT(ItemIds, [], ResultIds, TaskDataIds)
                    }}
            end
    end;
prepare_next_waiting_job(State = #workflow_execution_state{
    execution_status = #execution_cancelled{
        call_count = 0,
        is_interrupted = true,
        callbacks_to_execute = Callbacks
    } = Status,
    pending_callbacks = []
}) ->
    ReportItemErrorFun = fun maybe_report_item_error/2,
    MappedCallbacks = lists:filtermap(fun
        ({CallbackFun, CallbackSelector, CallbackArgs}) when CallbackFun =:= ReportItemErrorFun ->
            {true, {fun delete_item_callback/2, CallbackSelector, CallbackArgs}};
        ({_, ?CALLBACK_ON_EXCEPTION, _} = Callback) ->
            {true, Callback};
        (_) ->
            false
    end, Callbacks),
    {ok, State#workflow_execution_state{
        execution_status = Status#execution_cancelled{callbacks_to_execute = []},
        pending_callbacks = lists:map(fun({_, CallbackSelector, _}) -> CallbackSelector end, Callbacks),
        update_report = ?EXECUTE_DELAYED_CALLBACKS(lists:reverse(MappedCallbacks))
    }};
prepare_next_waiting_job(State = #workflow_execution_state{
    execution_status = #execution_cancelled{call_count = 0, callbacks_to_execute = Callbacks} = Status,
    pending_callbacks = []
}) ->
    {ok, State#workflow_execution_state{
        execution_status = Status#execution_cancelled{callbacks_to_execute = []},
        pending_callbacks = lists:map(fun({_, CallbackSelector, _}) -> CallbackSelector end, Callbacks),
        update_report = ?EXECUTE_DELAYED_CALLBACKS(lists:reverse(Callbacks))
    }};
prepare_next_waiting_job(#workflow_execution_state{
    execution_status = ?EXECUTION_CANCELLED(_)
}) ->
    ?WF_ERROR_NO_WAITING_ITEMS.

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
                    {ItemIds, UpdatedIterationState} = workflow_iteration_state:finalize(IterationState),
                    {ok, State#workflow_execution_state{
                        iteration_state = UpdatedIterationState,
                        execution_status = ?WAITING_FOR_ERROR_HANDLING_OR_NEXT_LANE_PREPARATION_END(false),
                        update_report = ?EXECUTION_CANCELLED_REPORT(ItemIds, TaskIdsToNotify),
                        pending_callbacks = [?CALLBACKS_ON_CANCEL_SELECTOR | PendingCallbacks]
                    }};
                true ->
                    {ItemIds, UpdatedIterationState} = workflow_iteration_state:finalize(IterationState),
                    {ok, State#workflow_execution_state{
                        iteration_state = UpdatedIterationState,
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
get_task_ids_to_notify_on_cancel(#workflow_execution_state{
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

    TaskIdsWithStreams = get_task_ids_with_streams(BoxSpecs),
    TaskIdsWithStreamsToNotify = lists_utils:intersect(TaskIdsToNotify, TaskIdsWithStreams),
    TaskIdsWithoutStreamsToNotify = TaskIdsToNotify -- TaskIdsWithStreamsToNotify,
    {TaskIdsWithStreamsToNotify, TaskIdsWithoutStreamsToNotify}.


-spec get_task_ids_with_streams(boxes_map()) -> [workflow_engine:task_id()].
get_task_ids_with_streams(BoxSpecs) ->
    lists:filtermap(fun
        ({TaskId, #{data_stream_enabled := true}}) -> {true, TaskId};
        (_) -> false
    end, lists:flatmap(fun(BoxSpec) -> maps:values(BoxSpec) end, maps:values(BoxSpecs))).

-spec are_all_data_streams_finalized(state()) -> boolean().
are_all_data_streams_finalized(#workflow_execution_state{
    tasks_data_registry = TasksDataRegistry,
    current_lane = #current_lane{
        parallel_box_specs = BoxSpecs
    }
}) ->
    lists:all(fun(TaskId) ->
        workflow_tasks_data_registry:is_stream_finalized(TaskId, TasksDataRegistry)
    end, get_task_ids_with_streams(BoxSpecs)).


-spec prepare_next_parallel_box(state(), workflow_jobs:job_identifier()) -> {ok, state()}.
prepare_next_parallel_box(State = #workflow_execution_state{
    snapshot_mode = SnapshotMode,
    current_lane = #current_lane{
        parallel_box_count = BoxCount,
        parallel_box_specs = BoxSpecs,
        index = LaneIndex,
        id = LaneId
    },
    jobs = Jobs,
    iteration_state = IterationState,
    items_to_process = ItemsToProcess
}, JobIdentifier) ->
    case workflow_jobs:prepare_next_parallel_box(Jobs, JobIdentifier, BoxSpecs, BoxCount) of
        {ok, NewJobs} ->
            State2 = State#workflow_execution_state{jobs = NewJobs},
            TaskStatus = get_task_status(JobIdentifier, State2),
            {ok, register_if_callback_is_pending(State2#workflow_execution_state{
                update_report = ?TASK_PROCESSED_REPORT(TaskStatus)
            }, fun maybe_notify_task_execution_ended/3, JobIdentifier, [TaskStatus], TaskStatus =/= ongoing)};
        {?WF_ERROR_ITEM_PROCESSING_ENDED(ItemIndex, SuccessOrFailure), NewJobs} ->
            {NewIterationState, FinishedItemId, ItemIdToSnapshot, ItemIdsToDelete} =
                workflow_iteration_state:handle_item_processed(
                    IterationState, ItemIndex, SuccessOrFailure =:= ?SUCCESS orelse SnapshotMode =:= ?ALL_ITEMS),
            State2 = State#workflow_execution_state{jobs = NewJobs, iteration_state = NewIterationState},
            State3 = register_if_callback_is_pending(State2, fun maybe_report_item_error/2,
                FinishedItemId, [], SuccessOrFailure =:= ?FAILURE),

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
                register_if_callback_is_pending(StateAcc, fun maybe_notify_task_execution_ended/3,
                    Identifier, [TaskStatus], TaskStatus =/= ongoing)
            end, State3, TaskStatusesToReport),

            {ItemIdToReportError, UpdatedItemIdsToDelete, UpdatedItemsToProcess} = case {
                SuccessOrFailure,
                lists:member(FinishedItemId, ItemIdsToDelete)
            } of
                {?SUCCESS, _} ->
                    {undefined, ItemIdsToDelete, ItemsToProcess};
                {?FAILURE, false} ->
                    {FinishedItemId, ItemIdsToDelete -- [FinishedItemId], ItemsToProcess#{FinishedItemId => [report]}};
                {?FAILURE, true} when FinishedItemId =:= ItemIdToSnapshot ->
                    {FinishedItemId, ItemIdsToDelete -- [FinishedItemId], ItemsToProcess#{FinishedItemId => [report, snapshot]}};
                {?FAILURE, true} ->
                    {FinishedItemId, ItemIdsToDelete -- [FinishedItemId], ItemsToProcess}
            end,

            FinalItemsToProcess = lists:foldl(fun(ItemId, Acc) ->
                case maps:get(ItemId, Acc, undefined) of
                    [report] when ItemId =:= ItemIdToSnapshot -> Acc#{ItemId => [report, snapshot]};
                    [report] -> Acc#{ItemId => [report, delete]};
                    _ -> Acc
                end
            end, UpdatedItemsToProcess, UpdatedItemIdsToDelete),

            {ok, State4#workflow_execution_state{
                items_to_process = FinalItemsToProcess,
                update_report = #items_processed_report{lane_index = LaneIndex, lane_id = LaneId,
                    last_finished_item_index = ItemIndex, item_id_to_report_error = ItemIdToReportError,
                    item_id_to_snapshot = ItemIdToSnapshot, task_statuses_to_report = TaskStatusesToReport,
                    item_ids_to_delete = UpdatedItemIdsToDelete -- [ItemIdToSnapshot]}
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

    register_if_callback_is_pending(State#workflow_execution_state{
        tasks_data_registry = UpdatedTasksDataRegistry,
        update_report = ?TASK_PROCESSED_REPORT(TaskStatus)
    }, fun handle_task_execution_stopped/3, TaskId, [TaskId], TaskStatus =:= ended).

mark_all_streamed_task_data_received_internal(TaskId, State = #workflow_execution_state{tasks_data_registry = TasksDataRegistry}) ->
    UpdatedTasksDataRegistry = workflow_tasks_data_registry:mark_all_task_data_received(TaskId, TasksDataRegistry),
    TaskStatus = case  workflow_tasks_data_registry:is_stream_finalized(TaskId, UpdatedTasksDataRegistry) of
        true -> ended;
        false -> waiting_for_data_stream_finalization
    end,

    register_if_callback_is_pending(State#workflow_execution_state{
        tasks_data_registry = UpdatedTasksDataRegistry,
        update_report = ?TASK_PROCESSED_REPORT(TaskStatus)
    }, fun handle_task_execution_stopped/3, TaskId, [TaskId], TaskStatus =:= ended).

-spec register_if_callback_is_pending(state(), function(), callback_selector(), list(), boolean()) -> state().
register_if_callback_is_pending(
    #workflow_execution_state{
        execution_status = #execution_cancelled{call_count = Counter, callbacks_to_execute = CallbacksList} = Status
    } = State,
    CallbackFun,
    CallbackSelector,
    CallbackArgs,
    true = _IsCallbackPending
) when Counter > 0 ->
    State#workflow_execution_state{
        execution_status = Status#execution_cancelled{
            callbacks_to_execute = [{CallbackFun, CallbackSelector, CallbackArgs} | CallbacksList]
        }
    };
register_if_callback_is_pending(
    #workflow_execution_state{pending_callbacks = PendingCallbacks} = State,
    _CallbackFun, CallbackSelector, _CallbackArgs, true = _IsCallbackPending
) ->
    State#workflow_execution_state{pending_callbacks = [CallbackSelector | PendingCallbacks]};
register_if_callback_is_pending(State, _CallbackFun, _CallbackSelector, _CallbackArgs, false = _IsCallbackPending) ->
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
            {ok, register_if_callback_is_pending(State2#workflow_execution_state{
                update_report = ?TASK_PROCESSED_REPORT(TaskStatus)
            }, fun maybe_notify_task_execution_ended/3, JobIdentifier, [TaskStatus], TaskStatus =/= ongoing)};
        ?NO_JOBS_LEFT_FOR_PARALLEL_BOX ->
            prepare_next_parallel_box(State#workflow_execution_state{jobs = NewJobs2}, JobIdentifier)
    end;
report_job_finish(State = #workflow_execution_state{
    execution_status = #execution_cancelled{is_interrupted = true},
    jobs = Jobs
}, JobIdentifier, error) ->
    % Schedule resume of job to allow job resume after resume of workflow
    UpdatedJobs = workflow_jobs:schedule_resume_of_job(Jobs, JobIdentifier),
    % TODO VFS-7787 - should we increment failed_job_count?
    {ok, State#workflow_execution_state{jobs = UpdatedJobs}};
report_job_finish(State = #workflow_execution_state{
    jobs = Jobs,
    failed_job_count = FailedJobCount
}, JobIdentifier, error) ->
    {FinalJobs, RemainingForBox} = workflow_jobs:register_failure(Jobs, JobIdentifier),
    State2 = State#workflow_execution_state{jobs = FinalJobs, failed_job_count = FailedJobCount + 1},
    case RemainingForBox of
        ?AT_LEAST_ONE_JOB_LEFT_FOR_PARALLEL_BOX ->
            TaskStatus = get_task_status(JobIdentifier, State2),
            {ok, register_if_callback_is_pending(State2#workflow_execution_state{
                update_report = ?TASK_PROCESSED_REPORT(TaskStatus)
            }, fun maybe_notify_task_execution_ended/3, JobIdentifier, [TaskStatus], TaskStatus =/= ongoing)};
        ?NO_JOBS_LEFT_FOR_PARALLEL_BOX ->
            % Call prepare_next_parallel_box/2 to delete metadata for failed item
            prepare_next_parallel_box(State2, JobIdentifier)
    end.

-spec handle_no_waiting_items_error(state(), ?WF_ERROR_NO_WAITING_ITEMS | ?ERROR_NOT_FOUND) -> no_items_error().
handle_no_waiting_items_error(#workflow_execution_state{current_lane = #current_lane{id = undefined}}, _Error) ->
    ?WF_ERROR_NO_WAITING_ITEMS;
handle_no_waiting_items_error(#workflow_execution_state{pending_callbacks = Waiting}, _Error) when Waiting =/= [] ->
    ?WF_ERROR_NO_WAITING_ITEMS;
handle_no_waiting_items_error(_State, ?WF_ERROR_NO_WAITING_ITEMS) ->
    ?WF_ERROR_NO_WAITING_ITEMS;
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
    % TODO VFS-9993 - check if items_to_process are empty
    case get(ExecutionId) of
        {ok, #workflow_execution_state{
            current_lane = #current_lane{index = Index, id = undefined},
            jobs = Jobs,
            iteration_state = IterationState,
            tasks_data_registry = TasksDataRegistry
        } = Record} when Index > LaneIndex ->
            case workflow_jobs:is_empty(Jobs) andalso
                workflow_iteration_state:is_finished_and_cleaned(IterationState) andalso
                workflow_tasks_data_registry:is_empty(TasksDataRegistry)
            of
                true -> true;
                false -> {false, Record}
            end;
        {ok, #workflow_execution_state{
            current_lane = #current_lane{index = LaneIndex},
            execution_status = ?EXECUTION_CANCELLED(0)
        } = Record} ->
            is_canceled_execution_finished_and_cleaned(Record);
        {ok, #workflow_execution_state{
            current_lane = #current_lane{index = LaneIndex},
            execution_status = ?WAITING_FOR_NEXT_LANE_PREPARATION_END(_)
        } = Record} ->
            is_canceled_execution_finished_and_cleaned(Record);
        {ok, #workflow_execution_state{
            current_lane = #current_lane{index = LaneIndex},
            execution_status = ?EXECUTION_ENDED_AFTER_CANCEL(_, _),
            next_lane_preparation_status = ?PREPARED_IN_ADVANCE,
            next_lane = #next_lane{}
        } = Record} ->
            % ?EXECUTION_ENDED with next line ?PREPARED_IN_ADVANCE is possible only after cancelation
            is_canceled_execution_finished_and_cleaned(Record);
        {ok, Record} ->
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
    {ok, #workflow_execution_state{iteration_state = IterationState}} = get(ExecutionId),
    workflow_jobs:get_item_id(JobIdentifier, IterationState).

-spec get_current_lane_context(workflow_engine:execution_id()) -> workflow_engine:execution_context().
get_current_lane_context(ExecutionId) ->
    {ok, #workflow_execution_state{current_lane = #current_lane{execution_context = Context}}} =
        get(ExecutionId),
    Context.

%% @private
-spec gen_incarnation_tag() -> incarnation_tag().
gen_incarnation_tag() ->
    generator:gen_name().