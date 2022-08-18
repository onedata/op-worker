%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions that handle atm lane execution run status
%%% transitions according (with some exceptions described below) to following
%%% state machine:
%%%                                      |
%%%                                      v
%%%                               +-------------+       ^stopping
%%%                               |  SCHEDULED  |------------------------
%%%                               +-------------+                         \
%%% W  +------------+                    |              ^stopping          \
%%% A  |  RESUMING  | -------------------|----------------------------------o
%%% I  +------------+                    v                                  |
%%% T    ^        |               +-------------+       ^stopping           |
%%% I    |        |               |  PREPARING  |---------------------------o
%%% N    |        |               +-------------+                           |
%%% G    |        |                      |                                  |
%%%      |        |        ready to execute (all associated                 |
%%% P    |        |        documents created and initiated)                 |
%%% H    |        |                      |                                  |
%%% A    |        |                      v                                  |
%%% S    |         \              +-------------+         ^stopping         |
%%% E    |           -----------> |   ENQUEUED  |---------------------------o
%%%      |                        +-------------+                           |
%%%      |                               |                                  |
%%%      |                 first task within atm lane run                   |
%%%      |                            started                               |
%%% =====|===============================|==================================|=================================
%%%      |                               v                                  |
%%%  O   |                         +-------------+        ^stopping         |
%%%  N   |                ---------|    ACTIVE   |--------------------------o
%%%  G   |              /          +-------------+                          |       ____
%%%  O   |             |                                                    v     /      \ overriding ^stopping
%%%  I   |             |                                             +-------------+     /        reason
%%%  N   |   ending lane execution                                   |   STOPPING  | <--
%%%  G   |         run with                                          +-------------+
%%%      |          /    \                                                |
%%%  P   |         /      \                                               |
%%%  H   |        /        \                 ending lane run execution with ^stopping reason
%%%  A   |       /          \              /           /              |         \            \
%%%  S   |    else    any parallel box    1*          2*              3*         4*           5*
%%%  E   |      |        ended with      /           /                |           \            \
%%%      |      |         failure       /           /                 |            \            \
%%% =====|======|============|=========/===========/==================|=============\============\==========
%%%      |      |            |        /           /                   |              \            \
%%%  E   |      |            |       /           /                    |               \            \
%%%  N   |      v            v      v           v                     v                v            v
%%%  D   |  +----------+    +--------+    +-----------+          +----------+    +-------------+    +-----------+
%%%  E   |  | FINISHED |    | FAILED |    | CANCELLED |          |  PAUSED  |    | INTERRUPTED |    |  CRUSHED  |   %% TODO pause -> cancel/stopping?
%%%  D   |  +----------+    +--------+    +-----------+          +----------+    +-------------+    +-----------+
%%%      |                                      |                     |                 |
%%%  P    \                                     |                     |                /
%%%  H      ------------------------------------o---------------------o---------------
%%%  A              resuming execution
%%%  S
%%%  E
%%%
%%% ^stopping - common step when halting execution due to:
%%% 1* - failure severe enough to cause stopping of entire automation workflow execution
%%%      (e.g. error when processing uncorrelated results).
%%% 2* - user cancelling entire automation workflow execution.
%%% 3* - user pausing entire automation workflow execution.
%%% 4* - abrupt interruption when some other component (e.g user offline session expired)
%%%      has failed and entire automation workflow execution is being stopped.
%%% 5* - unhandled exception occurred.
%%%
%%% There are some exceptions to above diagram:
%%% 1) atm lane execution run prepared in advance is always marked as INTERRUPTED
%%%    if workflow execution ends before reaching it.
%%% 2) interrupted lane run when preparing in advance if previous lane run finished
%%%    successfully - INTERRUPTED status is changed to FAILED as this run effectively
%%%    becomes the point of workflow execution failure at which it can be rerun.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution_status).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    status_to_phase/1,
    can_manual_lane_run_repeat_be_scheduled/2
]).
-export([
    handle_preparing/2,
    handle_enqueued/2,
    handle_stopping/3,
    handle_task_status_change/5,
    handle_ended/2,

    handle_manual_repeat/3
]).


-define(extract_doc(__CALL),
    case __CALL of
        {ok, __DOC} -> __DOC;
        {error, _} = _ERROR -> throw(_ERROR)
    end
).


%%%===================================================================
%%% API
%%%===================================================================


-spec status_to_phase(atm_lane_execution:run_status()) ->
    atm_workflow_execution:phase().
status_to_phase(?SCHEDULED_STATUS) -> ?WAITING_PHASE;
status_to_phase(?PREPARING_STATUS) -> ?WAITING_PHASE;
status_to_phase(?ENQUEUED_STATUS) -> ?WAITING_PHASE;
status_to_phase(?ACTIVE_STATUS) -> ?ONGOING_PHASE;
status_to_phase(?STOPPING_STATUS) -> ?ONGOING_PHASE;
status_to_phase(?FINISHED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?CRUSHED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?CANCELLED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?FAILED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?INTERRUPTED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?PAUSED_STATUS) -> ?ENDED_PHASE.


%% TODO check overall status of workflow - in case of crush no repeat can be made?
-spec can_manual_lane_run_repeat_be_scheduled(
    atm_workflow_execution:repeat_type(),
    atm_lane_execution:run()
) ->
    boolean().
can_manual_lane_run_repeat_be_scheduled(retry, #atm_lane_execution_run{
    status = ?FAILED_STATUS,
    stopping_reason = undefined
}) ->
    % lane run can be retried only if all items finished execution but some
    % of them failed (direct transition from ?ACTIVE_STATUS to ?FAILED_STATUS)
    true;
can_manual_lane_run_repeat_be_scheduled(rerun, #atm_lane_execution_run{status = Status}) when
    Status =:= ?FINISHED_STATUS;
    Status =:= ?CANCELLED_STATUS;
    Status =:= ?FAILED_STATUS;
    Status =:= ?INTERRUPTED_STATUS;
    Status =:= ?PAUSED_STATUS
->
    true;
can_manual_lane_run_repeat_be_scheduled(_, _) ->
    false.


-spec handle_preparing(atm_lane_execution:lane_run_selector(), atm_workflow_execution:id()) ->
    atm_workflow_execution:doc() | no_return().
handle_preparing(AtmLaneRunSelector, AtmWorkflowExecutionId) ->
    % transition to ?PREPARING_STATUS from ?SCHEDULED_STATUS
    AtmLaneRunDiff = fun
        (#atm_lane_execution_run{status = ?SCHEDULED_STATUS} = Run) ->
            {ok, Run#atm_lane_execution_run{status = ?PREPARING_STATUS}};

        (#atm_lane_execution_run{status = Status}) ->
            ?ERROR_ATM_INVALID_STATUS_TRANSITION(Status, ?PREPARING_STATUS)
    end,
    % preparing in advance
    Default = #atm_lane_execution_run{run_num = undefined, status = ?PREPARING_STATUS},

    ?extract_doc(atm_workflow_execution_status:handle_lane_preparing(
        AtmLaneRunSelector, AtmWorkflowExecutionId, fun(AtmWorkflowExecution) ->
            atm_lane_execution:update_run(
                AtmLaneRunSelector, AtmLaneRunDiff, Default, AtmWorkflowExecution
            )
        end
    )).


-spec handle_enqueued(atm_lane_execution:lane_run_selector(), atm_workflow_execution:id()) ->
    atm_workflow_execution:doc() | no_return().
handle_enqueued(AtmLaneRunSelector, AtmWorkflowExecutionId) ->
    Diff = fun(AtmWorkflowExecution) ->
        atm_lane_execution:update_run(AtmLaneRunSelector, fun
            (#atm_lane_execution_run{status = ?PREPARING_STATUS} = Run) ->
                {ok, Run#atm_lane_execution_run{status = ?ENQUEUED_STATUS}};
            (#atm_lane_execution_run{status = Status}) ->
                ?ERROR_ATM_INVALID_STATUS_TRANSITION(Status, ?PREPARING_STATUS)
        end, AtmWorkflowExecution)
    end,
    ?extract_doc(atm_workflow_execution_status:handle_lane_enqueued(AtmWorkflowExecutionId, Diff)).


-spec handle_stopping(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id(),
    atm_lane_execution:run_stopping_reason()
) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_stopping(AtmLaneRunSelector, AtmWorkflowExecutionId, Reason) ->
    Diff = fun(AtmWorkflowExecution) ->
        atm_lane_execution:update_run(AtmLaneRunSelector, fun
            (#atm_lane_execution_run{status = Status} = Run) when
                Status =:= ?SCHEDULED_STATUS;
                Status =:= ?PREPARING_STATUS;
                Status =:= ?ENQUEUED_STATUS;
                Status =:= ?ACTIVE_STATUS
            ->
                {ok, Run#atm_lane_execution_run{status = ?STOPPING_STATUS, stopping_reason = Reason}};

            (#atm_lane_execution_run{status = ?STOPPING_STATUS, stopping_reason = PrevReason} = Run) ->
                FinalReason = case should_overwrite_stopping_reason(PrevReason, Reason) of
                    true -> Reason;
                    false -> PrevReason
                end,
                {ok, Run#atm_lane_execution_run{stopping_reason = FinalReason}};

            (#atm_lane_execution_run{status = EndedStatus}) ->
                ?ERROR_ATM_INVALID_STATUS_TRANSITION(EndedStatus, ?STOPPING_STATUS)
        end, AtmWorkflowExecution)
    end,
    atm_workflow_execution_status:handle_lane_stopping(AtmLaneRunSelector, AtmWorkflowExecutionId, Diff).


-spec handle_task_status_change(
    atm_workflow_execution:id(),
    atm_lane_execution:lane_run_selector(),
    pos_integer(),
    atm_task_execution:id(),
    atm_task_execution:status()
) ->
    ok | errors:error().
handle_task_status_change(
    AtmWorkflowExecutionId,
    AtmLaneRunSelector,
    AtmParallelBoxIndex,
    AtmTaskExecutionId,
    NewAtmTaskExecutionStatus
) ->
    HasTaskStarted = lists:member(NewAtmTaskExecutionStatus, [?ACTIVE_STATUS, ?SKIPPED_STATUS]),

    Diff = fun(AtmWorkflowExecution) ->
        atm_lane_execution:update_run(AtmLaneRunSelector, fun(Run) ->
            AtmParallelBoxExecutions = Run#atm_lane_execution_run.parallel_boxes,
            AtmParallelBoxExecution = lists:nth(AtmParallelBoxIndex, AtmParallelBoxExecutions),

            case atm_parallel_box_execution:update_task_status(
                AtmTaskExecutionId, NewAtmTaskExecutionStatus, AtmParallelBoxExecution
            ) of
                {ok, NewParallelBoxExecution} ->
                    {ok, Run#atm_lane_execution_run{
                        status = case {Run#atm_lane_execution_run.status, HasTaskStarted} of
                            {?ENQUEUED_STATUS, true} ->
                                ?ACTIVE_STATUS;
                            {CurrentStatus, _} ->
                                CurrentStatus
                        end,
                        parallel_boxes = lists_utils:replace_at(
                            NewParallelBoxExecution, AtmParallelBoxIndex, AtmParallelBoxExecutions
                        )
                    }};
                {error, _} = Error ->
                    Error
            end
        end, AtmWorkflowExecution)
    end,
    atm_workflow_execution_status:handle_lane_task_status_change(AtmWorkflowExecutionId, Diff).


-spec handle_ended(atm_lane_execution:lane_run_selector(), atm_workflow_execution:id()) ->
    atm_workflow_execution:doc() | no_return().
handle_ended(AtmLaneRunSelector, AtmWorkflowExecutionId) ->
    Diff = fun(AtmWorkflowExecution) ->
        case atm_lane_execution:is_current_lane_run(AtmLaneRunSelector, AtmWorkflowExecution) of
            true ->
                handle_currently_executed_lane_run_ended(AtmWorkflowExecution);
            false ->
                handle_prepared_in_advance_lane_run_ended(AtmLaneRunSelector, AtmWorkflowExecution)
        end
    end,
    ?extract_doc(atm_workflow_execution:update(AtmWorkflowExecutionId, Diff)).


-spec handle_manual_repeat(
    atm_workflow_execution:repeat_type(),
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id()
) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_manual_repeat(RepeatType, {AtmLaneSelector, _} = AtmLaneRunSelector, AtmWorkflowExecutionId) ->
    Diff = fun(AtmWorkflowExecution) ->
        case atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution) of
            {ok, Run} ->
                try_to_schedule_manual_lane_run_repeat(
                    RepeatType, AtmLaneSelector, Run, AtmWorkflowExecution
                );
            {error, _} = Error ->
                Error
        end
    end,
    % TODO in case of repeat when latest run was paused - transit it to cancelled?
    atm_workflow_execution_status:handle_manual_lane_repeat(AtmWorkflowExecutionId, Diff).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec should_overwrite_stopping_reason(
    atm_lane_execution:run_stopping_reason(),
    atm_lane_execution:run_stopping_reason()
) ->
    boolean().
should_overwrite_stopping_reason(PrevReason, NewReason) ->
    stopping_reason_priority(NewReason) > stopping_reason_priority(PrevReason).


%% @private
-spec stopping_reason_priority(atm_lane_execution:run_stopping_reason()) ->
    non_neg_integer().
stopping_reason_priority(pause) -> 0;
stopping_reason_priority(interrupt) -> 1;
stopping_reason_priority(failure) -> 2;
stopping_reason_priority(cancel) -> 3;
stopping_reason_priority(crush) -> 4.


%% @private
-spec handle_currently_executed_lane_run_ended(atm_workflow_execution:record()) ->
    {ok, atm_workflow_execution:record()} | errors:error().
handle_currently_executed_lane_run_ended(AtmWorkflowExecution1 = #atm_workflow_execution{
    lanes_count = AtmLanesCount,
    current_lane_index = CurrentAtmLaneIndex
}) ->
    {ok, AtmWorkflowExecution2} = end_currently_executed_lane_run(AtmWorkflowExecution1),

    {ok, CurrentRun = #atm_lane_execution_run{
        stopping_reason = StoppingReason,
        status = Status
    }} = atm_lane_execution:get_run({current, current}, AtmWorkflowExecution2),

    case Status of
        ?FAILED_STATUS when StoppingReason == undefined ->
            % lane run can be automatically retried only if all items finished execution but
            % some of them failed (direct transition from ?ACTIVE_STATUS to ?FAILED_STATUS)
            try_to_schedule_automatic_current_lane_run_retry(CurrentRun, AtmWorkflowExecution2);
        ?FINISHED_STATUS when CurrentAtmLaneIndex < AtmLanesCount ->
            schedule_next_lane_run(AtmWorkflowExecution2);
        _ ->
            {ok, AtmWorkflowExecution2}
    end.


%% @private
-spec end_currently_executed_lane_run(atm_workflow_execution:record()) ->
    {ok, atm_workflow_execution:record()} | errors:error().
end_currently_executed_lane_run(AtmWorkflowExecution) ->
    atm_lane_execution:update_run({current, current}, fun
        (#atm_lane_execution_run{status = Status} = Run) when
            Status =:= ?SCHEDULED_STATUS;
            Status =:= ?PREPARING_STATUS;
            Status =:= ?ENQUEUED_STATUS
        ->
            % Provider must have been restarted as otherwise it is not possible to
            % transition from waiting phase to ended phase directly
            {ok, Run#atm_lane_execution_run{status = ?INTERRUPTED_STATUS}};

        (#atm_lane_execution_run{status = ?ACTIVE_STATUS} = Run) ->
            AtmParallelBoxExecutionStatuses = atm_parallel_box_execution:gather_statuses(
                Run#atm_lane_execution_run.parallel_boxes
            ),
            EndedStatus = case lists:member(?FAILED_STATUS, AtmParallelBoxExecutionStatuses) of
                true -> ?FAILED_STATUS;
                false -> ?FINISHED_STATUS
            end,
            {ok, Run#atm_lane_execution_run{status = EndedStatus}};

        (#atm_lane_execution_run{status = ?STOPPING_STATUS} = Run) ->
            EndedStatus = case Run#atm_lane_execution_run.stopping_reason of
                crush -> ?CRUSHED_STATUS;
                cancel -> ?CANCELLED_STATUS;
                failure -> ?FAILED_STATUS;
                interrupt -> ?INTERRUPTED_STATUS;
                pause -> ?PAUSED_STATUS
            end,
            {ok, Run#atm_lane_execution_run{status = EndedStatus}};

        (#atm_lane_execution_run{status = EndedStatus}) ->
            EndedStatus
    end, AtmWorkflowExecution).


%% @private
-spec try_to_schedule_automatic_current_lane_run_retry(
    atm_lane_execution:run(),
    atm_workflow_execution:record()
) ->
    {ok, atm_workflow_execution:record()} | errors:error().
try_to_schedule_automatic_current_lane_run_retry(CurrentRun, AtmWorkflowExecution) ->
    Diff = fun
        (#atm_lane_execution{retries_left = 0}) ->
            {error, no_retries_left};
        (#atm_lane_execution{retries_left = RetriesLeft} = AtmLaneExecution) ->
            {ok, AtmLaneExecution#atm_lane_execution{retries_left = RetriesLeft - 1}}
    end,
    CurrentAtmLaneIndex = AtmWorkflowExecution#atm_workflow_execution.current_lane_index,

    case atm_lane_execution:update(CurrentAtmLaneIndex, Diff, AtmWorkflowExecution) of
        {ok, NewAtmWorkflowExecution} ->
            schedule_lane_run_repeat(retry, current, CurrentRun, NewAtmWorkflowExecution);
        {error, no_retries_left} ->
            {ok, AtmWorkflowExecution}
    end.


%% @private
-spec schedule_next_lane_run(atm_workflow_execution:record()) ->
    {ok, atm_workflow_execution:record()}.
schedule_next_lane_run(AtmWorkflowExecution = #atm_workflow_execution{
    current_lane_index = CurrentAtmLaneIndex,
    current_run_num = CurrentRunNum
}) ->
    NextAtmLaneIndex = CurrentAtmLaneIndex + 1,

    % set run_no for lane run that is already preparing in advance
    Diff = fun
        (#atm_lane_execution_run{run_num = undefined, status = ?INTERRUPTED_STATUS} = Run) ->
            % interrupted lane run previously preparing in advance - change to ?FAILED_STATUS
            % as it will become the point of workflow execution failure at which it can be rerun
            {ok, Run#atm_lane_execution_run{run_num = CurrentRunNum, status = ?FAILED_STATUS}};

        (#atm_lane_execution_run{run_num = undefined} = Run) ->
            % lane run already preparing in advance
            {ok, Run#atm_lane_execution_run{run_num = CurrentRunNum}}
    end,
    % schedule new lane run
    Default = #atm_lane_execution_run{run_num = CurrentRunNum, status = ?SCHEDULED_STATUS},

    {ok, NewAtmWorkflowExecution} = atm_lane_execution:update_run(
        {NextAtmLaneIndex, CurrentRunNum}, Diff, Default, AtmWorkflowExecution
    ),
    {ok, NewAtmWorkflowExecution#atm_workflow_execution{current_lane_index = NextAtmLaneIndex}}.


%% @private
-spec handle_prepared_in_advance_lane_run_ended(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:record()
) ->
    {ok, atm_workflow_execution:record()}.
handle_prepared_in_advance_lane_run_ended(AtmLaneRunSelector, AtmWorkflowExecution) ->
    atm_lane_execution:update_run(AtmLaneRunSelector, fun(Run) ->
        {ok, Run#atm_lane_execution_run{status = ?INTERRUPTED_STATUS}}
    end, AtmWorkflowExecution).


%% @private
-spec try_to_schedule_manual_lane_run_repeat(
    atm_workflow_execution:repeat_type(),
    atm_lane_execution:selector(),
    atm_lane_execution:run(),
    atm_workflow_execution:record()
) ->
    {ok, atm_workflow_execution:record()} | errors:error().
try_to_schedule_manual_lane_run_repeat(RepeatType, AtmLaneSelector, Run, AtmWorkflowExecution) ->
    case can_manual_lane_run_repeat_be_scheduled(RepeatType, Run) of
        true ->
            schedule_manual_lane_run_repeat(RepeatType, AtmLaneSelector, Run, AtmWorkflowExecution);
        false when RepeatType == rerun ->
            ?ERROR_ATM_LANE_EXECUTION_RERUN_FAILED;
        false when RepeatType == retry ->
            ?ERROR_ATM_LANE_EXECUTION_RETRY_FAILED
    end.


%% @private
-spec schedule_manual_lane_run_repeat(
    atm_workflow_execution:repeat_type(),
    atm_lane_execution:selector(),
    atm_lane_execution:run(),
    atm_workflow_execution:record()
) ->
    {ok, atm_workflow_execution:record()} | errors:error().
schedule_manual_lane_run_repeat(RepeatType, AtmLaneSelector, Run, AtmWorkflowExecution) ->
    % Manual lane run repetitions must not be automatically retried
    Diff = fun(AtmLaneExecution) -> {ok, AtmLaneExecution#atm_lane_execution{retries_left = 0}} end,

    {ok, NewAtmWorkflowExecution} = atm_lane_execution:update(
        AtmWorkflowExecution#atm_workflow_execution.current_lane_index, Diff, AtmWorkflowExecution
    ),
    schedule_lane_run_repeat(RepeatType, AtmLaneSelector, Run, NewAtmWorkflowExecution).


%% @private
-spec schedule_lane_run_repeat(
    atm_workflow_execution:repeat_type(),
    atm_lane_execution:selector(),
    atm_lane_execution:run(),
    atm_workflow_execution:record()
) ->
    {ok, atm_workflow_execution:record()} | errors:error().
schedule_lane_run_repeat(RepeatType, AtmLaneSelector, Run, AtmWorkflowExecution = #atm_workflow_execution{
    current_run_num = CurrentRunNum
}) ->
    AtmLaneIndex = atm_lane_execution:resolve_selector(AtmLaneSelector, AtmWorkflowExecution),
    NextRunNum = CurrentRunNum + 1,

    NextRun = #atm_lane_execution_run{
        run_num = NextRunNum,
        origin_run_num = Run#atm_lane_execution_run.run_num,
        status = ?SCHEDULED_STATUS,
        iterated_store_id = case RepeatType of
            retry -> Run#atm_lane_execution_run.exception_store_id;
            rerun -> Run#atm_lane_execution_run.iterated_store_id
        end
    },
    Diff = fun(_Run) -> ?ERROR_ALREADY_EXISTS end,

    NewAtmWorkflowExecution = AtmWorkflowExecution#atm_workflow_execution{
        current_lane_index = AtmLaneIndex,
        current_run_num = NextRunNum
    },
    atm_lane_execution:update_run({AtmLaneIndex, NextRunNum}, Diff, NextRun, NewAtmWorkflowExecution).
