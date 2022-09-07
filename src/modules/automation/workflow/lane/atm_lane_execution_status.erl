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
%%%    +------------+                    |              ^stopping          \
%%%    |  RESUMING  | -------------------|----------------------------------o
%%%    +------------+                    v                                  |
%%% W    ^        |               +-------------+       ^stopping           |
%%% A    |        |               |  PREPARING  |---------------------------o
%%% I    |        |               +-------------+                           |
%%% T    |        |                      |                                  |
%%% I    |        |        ready to execute (all associated                 |
%%% N    |        |        documents created and initiated)                 |
%%% G    |        |                      |                                  |
%%%      |        |                      v                                  |
%%%      |         \              +-------------+         ^stopping         |
%%%      |           -----------> |   ENQUEUED  |---------------------------o
%%%      |                        +-------------+                           |
%%%      |                               |                                  |
%%%      |                 first task within atm lane run                   |
%%%      |                            started                               |
%%% =====|===============================|==================================|================================
%%%      |                               v                                  |
%%%      |                         +-------------+        ^stopping         |
%%%      |                ---------|    ACTIVE   |--------------------------o
%%%      |              /          +-------------+                          |       ____
%%%  O   |             |                                                    v     /      \ overriding ^stopping
%%%  N   |             |                                             +-------------+     /        reason
%%%  G   |  lane execution stopped                                   |   STOPPING  | <--
%%%  O   |         run with                                          +-------------+ <-------------------
%%%  I   |          /    \                                                |                               \
%%%  N   |         /      \                                               |                               |
%%%  G   |        /        \                  lane run execution stopped with ^stopping reason            |
%%%      |       /          \                 /        |           |          |            |              |
%%%      |    else    any parallel box       1*       2*          3*         4*           5*              |
%%%      |      |       stopped with        /          |           |          |            |              |
%%%      |      |         failure          /           |           |          |            |              |
%%% =====|======|============|============/============|===========|==========|============|==============|==
%%%      |      |            |           /             |           |          |            |              |
%%%  S   |      |            |          /              |           |          |            |              2*
%%%  U   |      |            |         /               |           |          |            v              |
%%%  S   |      |            |        /                |           |          |           +----------+    |
%%%  P   |      |            |       /                 |           |          |           |  PAUSED  | ---o
%%%  E   |      |            |      |                  |           |          |           +----------+    |
%%%  N   |      |            |      |                  |           |          v                 |         |
%%%  D   |      |            |      |                  |           |    +-------------+         |        /
%%%  E   |      |            |      |                  |           |    | INTERRUPTED | --------|-------
%%%  D   |      |            |      |                  |           |    +-------------+         |
%%%      |      |            |      |                  |           |          |                 |
%%%  ====|======|============|======|==================|===========|==========|=================|============
%%%      |      |            |      |                  |           |          |                 |
%%%      |      |            |      |                  |           |          |                 |
%%%  E   |      v            v      v                  v           v          |                 |
%%%  N   |  +----------+    +--------+       +-----------+    +-----------+   |                 |
%%%  D   |  | FINISHED |    | FAILED |       | CANCELLED |    |  CRASHED  |   |                 |
%%%  E   |  +----------+    +--------+       +-----------+    +-----------+   |                 |
%%%  D   |                                             |                      |                 |
%%%       \                                            |                      |                /
%%%         -------------------------------------------o----------------------o---------------
%%%                                         resuming execution
%%%
%%% Lane run transition to STOPPING status when execution is halted and not all items were processed.
%%% It is necessary as results for already scheduled ones must be awaited even if no more items are scheduled.
%%% In case when all items were already processed, such intermediate transition is not needed and lane run can
%%% be immediately stopped.
%%% Possible reasons for ^stopping lane run execution when not all items were processed are as follows:
%%% 1* - failure severe enough to cause stopping of entire automation workflow execution
%%%      (e.g. error when processing uncorrelated results).
%%% 2* - user cancelling entire automation workflow execution.
%%% 3* - unhandled exception occurred.
%%% 4* - abrupt interruption when some other component (e.g user offline session expired)
%%%      has failed and entire automation workflow execution is being stopped.
%%% 5* - user pausing entire automation workflow execution.
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
    can_manual_lane_run_repeat_be_scheduled/3
]).
-export([
    handle_preparing/2,
    handle_enqueued/2,
    handle_stopping/3,
    handle_task_status_change/5,
    handle_stopped/2,

    handle_manual_repeat/3,
    handle_resume/1
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
status_to_phase(?RESUMING_STATUS) -> ?WAITING_PHASE;
status_to_phase(?SCHEDULED_STATUS) -> ?WAITING_PHASE;
status_to_phase(?PREPARING_STATUS) -> ?WAITING_PHASE;
status_to_phase(?ENQUEUED_STATUS) -> ?WAITING_PHASE;

status_to_phase(?ACTIVE_STATUS) -> ?ONGOING_PHASE;
status_to_phase(?STOPPING_STATUS) -> ?ONGOING_PHASE;

status_to_phase(?INTERRUPTED_STATUS) -> ?SUSPENDED_PHASE;
status_to_phase(?PAUSED_STATUS) -> ?SUSPENDED_PHASE;

status_to_phase(?FINISHED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?CRASHED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?CANCELLED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?FAILED_STATUS) -> ?ENDED_PHASE.


-spec can_manual_lane_run_repeat_be_scheduled(
    atm_workflow_execution:repeat_type(),
    atm_lane_execution:run(),
    atm_workflow_execution:record()
) ->
    boolean().
can_manual_lane_run_repeat_be_scheduled(_, _, #atm_workflow_execution{status = ?CRASHED_STATUS}) ->
    false;

can_manual_lane_run_repeat_be_scheduled(RepeatType, Run, AtmWorkflowExecution) ->
    case atm_workflow_execution_status:infer_phase(AtmWorkflowExecution) of
        ?ENDED_PHASE -> is_lane_run_repeatable(RepeatType, Run);
        _ -> false
    end.


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
            (#atm_lane_execution_run{status = Status} = Run) when
                Status =:= ?PREPARING_STATUS;
                Status =:= ?RESUMING_STATUS
            ->
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
    {ok, atm_workflow_execution:doc()} | {error, already_stopping} | errors:error().
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
                case should_overwrite_stopping_reason(PrevReason, Reason) of
                    true -> {ok, Run#atm_lane_execution_run{stopping_reason = Reason}};
                    false -> {error, already_stopping}
                end;

            (#atm_lane_execution_run{status = Status} = Run) when
                (Status =:= ?INTERRUPTED_STATUS orelse Status =:= ?PAUSED_STATUS),
                Reason =:= cancel
            ->
                {ok, Run#atm_lane_execution_run{status = ?STOPPING_STATUS, stopping_reason = Reason}};

            (#atm_lane_execution_run{status = StoppedStatus}) ->
                ?ERROR_ATM_INVALID_STATUS_TRANSITION(StoppedStatus, ?STOPPING_STATUS)
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


-spec handle_stopped(atm_lane_execution:lane_run_selector(), atm_workflow_execution:id()) ->
    atm_workflow_execution:doc() | no_return().
handle_stopped(AtmLaneRunSelector, AtmWorkflowExecutionId) ->
    Diff = fun(AtmWorkflowExecution) ->
        case atm_lane_execution:is_current_lane_run(AtmLaneRunSelector, AtmWorkflowExecution) of
            true ->
                handle_currently_executed_lane_run_stopped(AtmWorkflowExecution);
            false ->
                handle_prepared_in_advance_lane_run_stopped(AtmLaneRunSelector, AtmWorkflowExecution)
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
    atm_workflow_execution_status:handle_manual_lane_repeat(AtmWorkflowExecutionId, Diff).


-spec handle_resume(atm_workflow_execution:id()) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_resume(AtmWorkflowExecutionId) ->
    Diff = fun(AtmWorkflowExecution) ->
        atm_lane_execution:update_run({current, current}, fun
            (#atm_lane_execution_run{status = Status} = Run) when
                Status =:= ?INTERRUPTED_STATUS;
                Status =:= ?PAUSED_STATUS;
                Status =:= ?CANCELLED_STATUS
            ->
                {ok, Run#atm_lane_execution_run{
                    status = ?RESUMING_STATUS,
                    stopping_reason = undefined
                }};

            (#atm_lane_execution_run{status = Status}) ->
                ?ERROR_ATM_INVALID_STATUS_TRANSITION(Status, ?RESUMING_STATUS)
        end, AtmWorkflowExecution)
    end,
    atm_workflow_execution_status:handle_resume(AtmWorkflowExecutionId, Diff).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec is_lane_run_repeatable(
    atm_workflow_execution:repeat_type(),
    atm_lane_execution:run()
) ->
    boolean().
is_lane_run_repeatable(retry, #atm_lane_execution_run{
    status = ?FAILED_STATUS,
    stopping_reason = undefined
}) ->
    % lane run can be retried only if all items finished execution but some
    % of them failed (direct transition from ?ACTIVE_STATUS to ?FAILED_STATUS)
    true;
is_lane_run_repeatable(rerun, #atm_lane_execution_run{status = Status}) when
    Status =:= ?FINISHED_STATUS;
    Status =:= ?CANCELLED_STATUS;
    Status =:= ?FAILED_STATUS
->
    true;
is_lane_run_repeatable(_, _) ->
    false.


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
stopping_reason_priority(crash) -> 4.


%% @private
-spec handle_currently_executed_lane_run_stopped(atm_workflow_execution:record()) ->
    {ok, atm_workflow_execution:record()} | errors:error().
handle_currently_executed_lane_run_stopped(AtmWorkflowExecution1 = #atm_workflow_execution{
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
        (#atm_lane_execution_run{status = Status}) when
            Status =:= ?RESUMING_STATUS;
            Status =:= ?SCHEDULED_STATUS;
            Status =:= ?PREPARING_STATUS;
            Status =:= ?ENQUEUED_STATUS
        ->
            % it is not possible to transition directly to ended/suspended phase
            ?ERROR_ATM_INVALID_STATUS_TRANSITION(Status, ?INTERRUPTED_STATUS);

        (#atm_lane_execution_run{status = ?ACTIVE_STATUS} = Run) ->
            AtmParallelBoxExecutionStatuses = atm_parallel_box_execution:gather_statuses(
                Run#atm_lane_execution_run.parallel_boxes
            ),
            StoppedStatus = case lists:member(?FAILED_STATUS, AtmParallelBoxExecutionStatuses) of
                true -> ?FAILED_STATUS;
                false -> ?FINISHED_STATUS
            end,
            {ok, Run#atm_lane_execution_run{status = StoppedStatus}};

        (#atm_lane_execution_run{status = ?STOPPING_STATUS} = Run) ->
            StoppedStatus = case Run#atm_lane_execution_run.stopping_reason of
                crash -> ?CRASHED_STATUS;
                cancel -> ?CANCELLED_STATUS;
                failure -> ?FAILED_STATUS;
                interrupt -> ?INTERRUPTED_STATUS;
                pause -> ?PAUSED_STATUS
            end,
            {ok, Run#atm_lane_execution_run{status = StoppedStatus}};

        (#atm_lane_execution_run{status = StoppedStatus}) ->
            ?ERROR_ATM_INVALID_STATUS_TRANSITION(StoppedStatus, ?INTERRUPTED_STATUS)
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
-spec handle_prepared_in_advance_lane_run_stopped(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:record()
) ->
    {ok, atm_workflow_execution:record()}.
handle_prepared_in_advance_lane_run_stopped(AtmLaneRunSelector, AtmWorkflowExecution) ->
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
    case can_manual_lane_run_repeat_be_scheduled(RepeatType, Run, AtmWorkflowExecution) of
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
