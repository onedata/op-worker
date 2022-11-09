%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021-2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions that handle atm task execution status
%%% transitions according (with some exceptions described below) to following
%%% state machine:
%%%                                         |
%%%                                         v
%%%    ------------------------------ +-----------+ <---- no item ever scheduled
%%%  /                                |  PENDING  |                      \
%%% |             ------------------- +-----------+                       \
%%% |           /                           |                              \                  +----------+
%%% |  task execution stopped               |                              resumed with ----- | RESUMING | --------
%%% |     with no item ever            first item                          /                  +----------+          \
%%% |    scheduled to process      scheduled to process                   /                           ^              |
%%% |          |                   /                                     /                            |              |
%%% |          |                  |      ----------------------------- else                           |              |
%%% |          |                  |    /                                                              |              |
%%% |          |                  |   |                                          ____    overriding   |              |
%%% |          |                  v   v                                        /      \  ^stopping    |              |
%%% |          |            +----------+                           +------------+     /  reason       |              |
%%% |          |            |  ACTIVE  | ------ ^stopping -------> |  STOPPING  | <--                 |              |
%%% |          |            +----------+                           +------------+                     |              |
%%% |          |                  |                                 /                                 |              |
%%% |          |        task execution stopped            ---------                                  /               |
%%% |          |            with all items              /                  resuming execution       /                |
%%% |          |               processed               |              -----------------o-----------                  |
%%% |          |           /              \            |            /                  |                             |
%%% |          |      successfully       else          |           /      -------------|------- 4* ----              |
%%% |          |           |               |           |          /     /              |                \            |
%%% |          v           v               v           |         /     /               |                 v           |
%%% |    +-----------+   +----------+     +--------+   |   +-------------+         +--------+         +-----------+  |
%%% |    |  SKIPPED  |   | FINISHED |     | FAILED |   |   | INTERRUPTED | <- 2* - | PAUSED | - 4* -> | CANCELLED |  |
%%% |    +-----------+   +----------+     +--------+   |   +-------------+         +--------+         +-----------+  |
%%% |                                             ^    |       ^                       ^              ^              |
%%% |                                             |    |       |                       |              |              |
%%% |                                             1*   |       2*                      3*            4*              |
%%% |                                              \   v       |                       |            /                |
%%% |                                              [ task execution stopped due to ^stopping reason ]                |
%%% |                                                 ^                                           ^                  |
%%%  \                                               /                                             \                /
%%%    ---------------------------------------------                                                 --------------
%%%
%%% Task transition to STOPPING status when execution is halted and not all items were processed.
%%% It is necessary as results for already scheduled ones must be awaited even if no more items are scheduled.
%%% In case when all items were already processed, such intermediate transition is omitted (transition
%%% from ACTIVE to stopped status can be done during one update operation) - although
%%% logically such transition occurs.
%%% Possible reasons for ^stopping task execution when not all items were processed are as follows:
%%% 1* - failure severe enough to cause stopping of entire automation workflow execution
%%%      (e.g. error when processing uncorrelated results) or interruption of active task
%%%      if it has any uncorrelated results (some of them may have been lost).
%%% 2* - abrupt interruption when some other component (e.g task or external service like OpenFaaS)
%%%      has failed and entire automation workflow execution is being stopped.
%%% 3* - user pausing entire automation workflow execution.
%%% 4* - user cancelling entire automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_status).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([is_transition_allowed/2, is_running/1]).

-export([
    handle_items_in_processing/2,
    handle_item_processed/1,
    handle_items_withdrawn/2,
    handle_items_failed/2,

    handle_stopping/3,
    handle_stopped/1,

    handle_resuming/2,
    handle_resumed/1
]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec is_transition_allowed(atm_task_execution:status(), atm_task_execution:status()) ->
    boolean().
is_transition_allowed(?PENDING_STATUS, ?SKIPPED_STATUS) -> true;
is_transition_allowed(?PENDING_STATUS, ?INTERRUPTED_STATUS) -> true;
is_transition_allowed(?PENDING_STATUS, ?PAUSED_STATUS) -> true;
is_transition_allowed(?PENDING_STATUS, ?CANCELLED_STATUS) -> true;
is_transition_allowed(?PENDING_STATUS, ?ACTIVE_STATUS) -> true;

is_transition_allowed(?ACTIVE_STATUS, ?FINISHED_STATUS) -> true;
is_transition_allowed(?ACTIVE_STATUS, ?FAILED_STATUS) -> true;
is_transition_allowed(?ACTIVE_STATUS, ?STOPPING_STATUS) -> true;
is_transition_allowed(?STOPPING_STATUS, ?FAILED_STATUS) -> true;
is_transition_allowed(?STOPPING_STATUS, ?INTERRUPTED_STATUS) -> true;
is_transition_allowed(?STOPPING_STATUS, ?PAUSED_STATUS) -> true;
is_transition_allowed(?STOPPING_STATUS, ?CANCELLED_STATUS) -> true;

is_transition_allowed(?PAUSED_STATUS, ?INTERRUPTED_STATUS) -> true;
is_transition_allowed(?PAUSED_STATUS, ?CANCELLED_STATUS) -> true;
% Transition possible in case of race between resuming interrupted task and pausing it
% (normally task should first transition to RESUMING status and only then to PAUSED)
is_transition_allowed(?INTERRUPTED_STATUS, ?PAUSED_STATUS) -> true;
is_transition_allowed(?INTERRUPTED_STATUS, ?CANCELLED_STATUS) -> true;

is_transition_allowed(?INTERRUPTED_STATUS, ?RESUMING_STATUS) -> true;
is_transition_allowed(?PAUSED_STATUS, ?RESUMING_STATUS) -> true;

is_transition_allowed(?RESUMING_STATUS, ?INTERRUPTED_STATUS) -> true;
is_transition_allowed(?RESUMING_STATUS, ?PAUSED_STATUS) -> true;
is_transition_allowed(?RESUMING_STATUS, ?CANCELLED_STATUS) -> true;
is_transition_allowed(?RESUMING_STATUS, ?PENDING_STATUS) -> true;
is_transition_allowed(?RESUMING_STATUS, ?ACTIVE_STATUS) -> true;

is_transition_allowed(_, _) -> false.


-spec is_running(atm_task_execution:status()) -> boolean().
is_running(?RESUMING_STATUS) -> true;
is_running(?PENDING_STATUS) -> true;
is_running(?ACTIVE_STATUS) -> true;
is_running(?STOPPING_STATUS) -> true;
is_running(_) -> false.


-spec handle_items_in_processing(atm_task_execution:id(), pos_integer()) ->
    {ok, atm_task_execution:doc()} | {error, task_already_stopping} | {error, task_already_stopped}.
handle_items_in_processing(AtmTaskExecutionId, ItemCount) ->
    apply_diff(AtmTaskExecutionId, fun
        (AtmTaskExecution = #atm_task_execution{
            status = ?PENDING_STATUS,
            items_in_processing = 0
        }) ->
            {ok, AtmTaskExecution#atm_task_execution{
                status = ?ACTIVE_STATUS,
                items_in_processing = ItemCount
            }};

        (AtmTaskExecution = #atm_task_execution{
            status = ?ACTIVE_STATUS,
            items_in_processing = CurrentItemsInProcessingNum
        }) ->
            {ok, AtmTaskExecution#atm_task_execution{
                items_in_processing = CurrentItemsInProcessingNum + ItemCount
            }};

        (#atm_task_execution{status = ?STOPPING_STATUS}) ->
            {error, task_already_stopping};

        (#atm_task_execution{status = Status}) when
            Status =:= ?SKIPPED_STATUS;
            Status =:= ?INTERRUPTED_STATUS;
            Status =:= ?PAUSED_STATUS;
            Status =:= ?FINISHED_STATUS;
            Status =:= ?FAILED_STATUS;
            Status =:= ?CANCELLED_STATUS
        ->
            {error, task_already_stopped}
    end).


-spec handle_item_processed(atm_task_execution:id()) -> ok.
handle_item_processed(AtmTaskExecutionId) ->
    {ok, _} = apply_diff(AtmTaskExecutionId, fun(#atm_task_execution{
        items_in_processing = ItemsInProcessing,
        items_processed = ItemsProcessed
    } = AtmTaskExecution) ->
        {ok, AtmTaskExecution#atm_task_execution{
            items_in_processing = ItemsInProcessing - 1,
            items_processed = ItemsProcessed + 1
        }}
    end),
    ok.


-spec handle_items_withdrawn(atm_task_execution:id(), pos_integer()) ->
    {ok, atm_task_execution:doc()} | {error, task_not_stopping}.
handle_items_withdrawn(AtmTaskExecutionId, ItemCount) ->
    apply_diff(AtmTaskExecutionId, fun
        (AtmTaskExecution = #atm_task_execution{
            status = ?STOPPING_STATUS,
            items_in_processing = ItemsInProcessing
        }) ->
            {ok, AtmTaskExecution#atm_task_execution{
                items_in_processing = ItemsInProcessing - ItemCount
            }};

        (_) ->
            {error, task_not_stopping}
    end).


-spec handle_items_failed(atm_task_execution:id(), pos_integer()) -> ok.
handle_items_failed(AtmTaskExecutionId, ItemCount) ->
    {ok, _} = apply_diff(AtmTaskExecutionId, fun(#atm_task_execution{
        items_in_processing = ItemsInProcessing,
        items_processed = ItemsProcessed,
        items_failed = ItemsFailed
    } = AtmTaskExecution) ->
        {ok, AtmTaskExecution#atm_task_execution{
            items_in_processing = ItemsInProcessing - ItemCount,
            items_processed = ItemsProcessed + ItemCount,
            items_failed = ItemsFailed + ItemCount
        }}
    end),
    ok.


-spec handle_stopping(
    atm_task_execution:id(),
    atm_task_execution:stopping_reason(),
    atm_workflow_execution:incarnation()
) ->
    {ok, atm_task_execution:doc()} | {error, task_already_stopping} | {error, task_already_stopped}.
handle_stopping(AtmTaskExecutionId, Reason, CurrentIncarnation) ->
    apply_diff(AtmTaskExecutionId, fun
        (AtmTaskExecution = #atm_task_execution{status = Status}) when
            Status =:= ?PENDING_STATUS;
            Status =:= ?RESUMING_STATUS
        ->
            {ok, AtmTaskExecution#atm_task_execution{
                status = infer_stopped_status(Reason),
                stopping_incarnation = CurrentIncarnation
            }};

        (AtmTaskExecution = #atm_task_execution{status = ?ACTIVE_STATUS}) ->
            {ok, AtmTaskExecution#atm_task_execution{
                status = ?STOPPING_STATUS,
                stopping_reason = Reason,
                stopping_incarnation = CurrentIncarnation
            }};

        (AtmTaskExecution = #atm_task_execution{
            status = ?STOPPING_STATUS,
            stopping_reason = PrevReason
        }) ->
            case should_overwrite_stopping_reason(PrevReason, Reason) of
                true -> {ok, AtmTaskExecution#atm_task_execution{stopping_reason = Reason}};
                false -> {error, task_already_stopping}
            end;

        (AtmTaskExecution = #atm_task_execution{status = ?PAUSED_STATUS}) when
            Reason =:= cancel;
            Reason =:= interrupt
        ->
            {ok, AtmTaskExecution#atm_task_execution{
                status = infer_stopped_status(Reason),
                stopping_reason = Reason,
                stopping_incarnation = CurrentIncarnation
            }};

        (AtmTaskExecution = #atm_task_execution{
            status = ?INTERRUPTED_STATUS,
            stopping_incarnation = PrevStoppingIncarnation
        }) when
            Reason =:= cancel;
            % Possible race condition when resuming and immediately pausing interrupted task (resuming
            % traverse hasn't yet transition task to RESUMING status while pausing traverse pauses it)
            (Reason =:= pause andalso PrevStoppingIncarnation < CurrentIncarnation)
        ->
            {ok, AtmTaskExecution#atm_task_execution{
                status = infer_stopped_status(Reason),
                stopping_reason = Reason,
                stopping_incarnation = CurrentIncarnation
            }};

        (#atm_task_execution{status = Status}) when
            Status =:= ?SKIPPED_STATUS;
            Status =:= ?INTERRUPTED_STATUS;
            Status =:= ?PAUSED_STATUS;
            Status =:= ?FINISHED_STATUS;
            Status =:= ?FAILED_STATUS;
            Status =:= ?CANCELLED_STATUS
        ->
            {error, task_already_stopped}
    end).


-spec handle_stopped(atm_task_execution:id()) ->
    {ok, atm_task_execution:doc()} | {error, task_already_stopped}.
handle_stopped(AtmTaskExecutionId) ->
    apply_diff(AtmTaskExecutionId, fun
        (AtmTaskExecution = #atm_task_execution{status = ?PENDING_STATUS}) ->
            {ok, AtmTaskExecution#atm_task_execution{status = ?SKIPPED_STATUS}};

        (AtmTaskExecution = #atm_task_execution{
            status = ?ACTIVE_STATUS,
            items_in_processing = 0,
            items_failed = 0
        }) ->
            {ok, AtmTaskExecution#atm_task_execution{status = ?FINISHED_STATUS}};

        (AtmTaskExecution = #atm_task_execution{
            status = ?ACTIVE_STATUS,
            items_in_processing = 0
        }) ->
            % All jobs were executed but some must have failed
            {ok, AtmTaskExecution#atm_task_execution{status = ?FAILED_STATUS}};

        (AtmTaskExecution = #atm_task_execution{
            status = ?STOPPING_STATUS,
            stopping_reason = StoppingReason,
            items_in_processing = ItemsInProcessing,
            items_processed = ItemsProcessed,
            items_failed = ItemsFailed,
            uncorrelated_result_specs = UncorrelatedResultSpecs
        }) ->
            % atm workflow execution may have been abruptly interrupted by e.g.
            % provider restart which resulted in stale `items_in_processing`
            UpdatedProcessedItems = ItemsProcessed + ItemsInProcessing,
            UpdatedFailedItems = ItemsFailed + ItemsInProcessing,

            {ok, AtmTaskExecution#atm_task_execution{
                status = case {StoppingReason, lists_utils:is_empty(UncorrelatedResultSpecs)} of
                    {interrupt, false} ->
                        % If task having uncorrelated results is interrupted not all of those
                        % results may have been received and as such it is treated as failure
                        % rather then interruption
                        ?FAILED_STATUS;
                    _ ->
                        infer_stopped_status(StoppingReason)
                end,
                items_in_processing = 0,
                items_processed = UpdatedProcessedItems,
                items_failed = UpdatedFailedItems
            }};

        (#atm_task_execution{status = Status}) when
            Status =:= ?SKIPPED_STATUS;
            Status =:= ?INTERRUPTED_STATUS;
            Status =:= ?PAUSED_STATUS;
            Status =:= ?FINISHED_STATUS;
            Status =:= ?FAILED_STATUS;
            Status =:= ?CANCELLED_STATUS
        ->
            {error, task_already_stopped}
    end).


-spec handle_resuming(atm_task_execution:id(), atm_workflow_execution:incarnation()) ->
    {ok, atm_task_execution:doc()} | {error, task_already_stopped}.
handle_resuming(AtmTaskExecutionId, CurrentIncarnation) ->
    apply_diff(AtmTaskExecutionId, fun
        (AtmTaskExecution = #atm_task_execution{
            status = Status,
            stopping_incarnation = PrevStoppingIncarnation
        }) when
            (Status =:= ?INTERRUPTED_STATUS orelse Status =:= ?PAUSED_STATUS),
            % Ensure status wasn't updated by parallel pause/interrupt (possible race)
            % in current incarnation
            PrevStoppingIncarnation < CurrentIncarnation
        ->
            {ok, AtmTaskExecution#atm_task_execution{
                status = ?RESUMING_STATUS,
                stopping_reason = undefined
            }};

        (#atm_task_execution{status = Status}) when
            Status =:= ?SKIPPED_STATUS;
            Status =:= ?INTERRUPTED_STATUS;
            Status =:= ?PAUSED_STATUS;
            Status =:= ?FINISHED_STATUS;
            Status =:= ?FAILED_STATUS;
            Status =:= ?CANCELLED_STATUS
        ->
            {error, task_already_stopped}
    end).


-spec handle_resumed(atm_task_execution:id()) ->
    {ok, atm_task_execution:doc()} | {error, task_already_stopped}.
handle_resumed(AtmTaskExecutionId) ->
    apply_diff(AtmTaskExecutionId, fun
        (AtmTaskExecution = #atm_task_execution{
            status = ?RESUMING_STATUS,
            items_in_processing = 0,
            items_processed = 0,
            items_failed = 0
        }) ->
            {ok, AtmTaskExecution#atm_task_execution{status = ?PENDING_STATUS}};

        (AtmTaskExecution = #atm_task_execution{status = ?RESUMING_STATUS}) ->
            {ok, AtmTaskExecution#atm_task_execution{status = ?ACTIVE_STATUS}};

        (#atm_task_execution{status = Status}) when
            Status =:= ?SKIPPED_STATUS;
            Status =:= ?INTERRUPTED_STATUS;
            Status =:= ?PAUSED_STATUS;
            Status =:= ?FINISHED_STATUS;
            Status =:= ?FAILED_STATUS;
            Status =:= ?CANCELLED_STATUS
        ->
            {error, task_already_stopped}
    end).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec infer_stopped_status(atm_task_execution:stopping_reason()) -> atm_task_execution:status().
infer_stopped_status(pause) -> ?PAUSED_STATUS;
infer_stopped_status(interrupt) -> ?INTERRUPTED_STATUS;
infer_stopped_status(failure) -> ?FAILED_STATUS;
infer_stopped_status(cancel) -> ?CANCELLED_STATUS.


%% @private
-spec should_overwrite_stopping_reason(
    atm_task_execution:stopping_reason(),
    atm_task_execution:stopping_reason()
) ->
    boolean().
should_overwrite_stopping_reason(PrevReason, NewReason) ->
    stopping_reason_priority(NewReason) > stopping_reason_priority(PrevReason).


%% @private
-spec stopping_reason_priority(atm_task_execution:stopping_reason()) ->
    non_neg_integer().
stopping_reason_priority(pause) -> 0;
stopping_reason_priority(interrupt) -> 1;
stopping_reason_priority(failure) -> 2;
stopping_reason_priority(cancel) -> 3.


%% @private
-spec apply_diff(atm_task_execution:id(), atm_task_execution:diff()) ->
    {ok, atm_task_execution:doc()} | {error, term()}.
apply_diff(AtmTaskExecutionId, Diff) ->
    case atm_task_execution:update(AtmTaskExecutionId, Diff) of
        {ok, AtmTaskExecutionDoc} = Result ->
            handle_status_change(AtmTaskExecutionDoc),
            Result;
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates atm task execution status stored in atm parallel box execution of
%% corresponding atm lane run if it was changed in task execution doc.
%%
%% NOTE: normally this should happen only after lane run processing has started
%% and concrete 'run_num' was set for all its tasks (it is not possible to
%% foresee what it will be beforehand as previous lane run may retried numerous
%% times). However, in case of failure/interruption during lane run preparation
%% after task execution documents have been created, this function will also
%% be called. Despite not having 'run_num' set there is no ambiguity to which
%% lane run it belongs as it can only happen to the newest run of given lane.
%% @end
%%--------------------------------------------------------------------
-spec handle_status_change(atm_task_execution:doc()) -> ok.
handle_status_change(#document{value = #atm_task_execution{status_changed = false}}) ->
    ok;

handle_status_change(#document{
    key = AtmTaskExecutionId,
    value = #atm_task_execution{
        workflow_execution_id = AtmWorkflowExecutionId,
        lane_index = AtmLaneIndex,
        run_num = RunNumOrUndefined,
        parallel_box_index = AtmParallelBoxIndex,
        status = NewStatus,
        status_changed = true
    }
}) ->
    RunSelector = utils:ensure_defined(RunNumOrUndefined, current),

    ok = atm_lane_execution_status:handle_task_status_change(
        AtmWorkflowExecutionId, {AtmLaneIndex, RunSelector}, AtmParallelBoxIndex,
        AtmTaskExecutionId, NewStatus
    ).
