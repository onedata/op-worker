%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utility functions for management of automation
%%% workflow execution status and phase.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_status).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([infer_phase/1]).
-export([
    handle_lane_preparing/3,
    handle_lane_enqueued/2,
    handle_lane_aborting/3
]).
-export([
    handle_ended/1,
    report_task_status_change/5
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec infer_phase(atm_workflow_execution:record()) -> atm_workflow_execution:phase().
infer_phase(#atm_workflow_execution{status = Status}) ->
    status_to_phase(Status).


-spec handle_lane_preparing(
    pos_integer(),
    atm_workflow_execution:id(),
    fun((atm_workflow_execution:record()) -> atm_workflow_execution:record() | errors:error())
) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_lane_preparing(AtmLaneIndex, AtmWorkflowExecutionId, AtmLaneExecutionDiff) ->
    Diff = fun
        (Record = #atm_workflow_execution{status = ?SCHEDULED_STATUS, curr_lane_index = CurrIndex})
            when CurrIndex =:= AtmLaneIndex
        ->
            case AtmLaneExecutionDiff(Record) of
                {ok, NewRecord} ->
                    {ok, set_times_on_phase_transition(NewRecord#atm_workflow_execution{
                        status = ?ACTIVE_STATUS
                    })};
                {error, _} = Error ->
                    Error
            end;

        (Record = #atm_workflow_execution{status = Status}) when
            Status == ?SCHEDULED_STATUS;
            Status == ?ACTIVE_STATUS
        ->
            AtmLaneExecutionDiff(Record);

        (#atm_workflow_execution{status = ?ABORTING_STATUS}) ->
            ?ERROR_ATM_TASK_EXECUTION_ENDED;  %% TODO new error atm_workflow_execution_aborting

        (#atm_workflow_execution{status = _EndedStatus}) ->
            ?ERROR_ATM_TASK_EXECUTION_ENDED   %% TODO new error atm_workflow_execution_ended
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} = Result ->
            ensure_in_proper_phase_tree(AtmWorkflowExecutionDoc),
            Result;
        {error, _} = Error ->
            Error
    end.


-spec handle_lane_enqueued(
    atm_workflow_execution:id(),
    fun((atm_workflow_execution:record()) -> atm_workflow_execution:record() | errors:error())
) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_lane_enqueued(AtmWorkflowExecutionId, AtmLaneExecutionDiff) ->
    atm_workflow_execution:update(AtmWorkflowExecutionId, fun
        (Record = #atm_workflow_execution{status = Status}) when
            Status == ?SCHEDULED_STATUS;
            Status == ?ACTIVE_STATUS
        ->
            AtmLaneExecutionDiff(Record);

        (#atm_workflow_execution{status = ?ABORTING_STATUS}) ->
            ?ERROR_ATM_TASK_EXECUTION_ENDED;  %% TODO new error atm_workflow_execution_aborting

        (#atm_workflow_execution{status = _EndedStatus}) ->
            ?ERROR_ATM_TASK_EXECUTION_ENDED   %% TODO new error atm_workflow_execution_ended
    end).


-spec handle_lane_aborting(
    undefined | pos_integer(),
    atm_workflow_execution:id(),
    fun((atm_workflow_execution:record()) -> atm_workflow_execution:record() | errors:error())
) ->
    ok | errors:error().
handle_lane_aborting(AtmLaneIndex, AtmWorkflowExecutionId, AtmLaneExecutionDiff) ->
    Diff = fun
        (Record = #atm_workflow_execution{status = Status, curr_lane_index = CurrIndex}) when
            Status == ?SCHEDULED_STATUS;
            Status == ?ACTIVE_STATUS;
            Status == ?ABORTING_STATUS
        ->
            IsCurrentlyExecutedLane = AtmLaneIndex == undefined orelse AtmLaneIndex == CurrIndex,

            case {IsCurrentlyExecutedLane, AtmLaneExecutionDiff(Record)} of
                {true, {ok, NewRecord}} ->
                    {ok, set_times_on_phase_transition(NewRecord#atm_workflow_execution{
                        status = ?ABORTING_STATUS
                    })};
                {false, {ok, NewRecord}} ->
                    {ok, NewRecord};
                {_, {error, _} = Error} ->
                    Error
            end;

        (#atm_workflow_execution{status = _EndedStatus}) ->
            ?ERROR_ATM_TASK_EXECUTION_ENDED   %% TODO new error atm_workflow_execution_ended
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} ->
            ensure_in_proper_phase_tree(AtmWorkflowExecutionDoc);
        {error, _} = Error ->
            Error
    end.





















-spec handle_ended(atm_workflow_execution:id()) ->
    {ok, atm_workflow_execution:doc()} | no_return().
handle_ended(AtmWorkflowExecutionId) ->
    Diff = fun(#atm_workflow_execution{status = CurrStatus} = AtmWorkflowExecution) ->
        EndedStatus = case status_to_phase(CurrStatus) of
            ?WAITING_PHASE ->
                % Workflow preparation must have failed or provider was restarted
                % as otherwise it is not possible to transition from waiting phase
                % to ended phase directly
                ?FAILED_STATUS;
            ?ONGOING_PHASE when CurrStatus == ?ACTIVE_STATUS ->
                AtmLaneExecutionStatuses = lists:usort(atm_lane_execution:gather_statuses(
                    AtmWorkflowExecution#atm_workflow_execution.lanes
                )),
                case lists:member(?FAILED_STATUS, AtmLaneExecutionStatuses) of
                    true ->
                        % Workflow may not have been aborted as maximum failed items threshold
                        % has not been breached but still after execution end it is considered
                        % as failed
                        ?FAILED_STATUS;
                    false ->
                        ?FINISHED_STATUS
                end;
            ?ONGOING_PHASE when CurrStatus == ?ABORTING_STATUS ->
                case AtmWorkflowExecution#atm_workflow_execution.aborting_reason of
                    cancel -> ?CANCELLED_STATUS;
                    failure -> ?FAILED_STATUS
                end;
            ?ENDED_PHASE ->
                CurrStatus
        end,

        {ok, set_times_on_phase_transition(AtmWorkflowExecution#atm_workflow_execution{
            status = EndedStatus
        })}
    end,

    Result = {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:update(
        AtmWorkflowExecutionId, Diff
    ),
    ensure_in_proper_phase_tree(AtmWorkflowExecutionDoc),

    Result.


-spec report_task_status_change(
    atm_workflow_execution:id(),
    non_neg_integer(),
    non_neg_integer(),
    atm_task_execution:id(),
    atm_task_execution:status()
) ->
    ok.
report_task_status_change(
    AtmWorkflowExecutionId,
    AtmLaneExecutionIndex,
    AtmParallelBoxExecutionIndex,
    AtmTaskExecutionId,
    NewAtmTaskExecutionStatus
) ->
    HasTaskStarted = lists:member(NewAtmTaskExecutionStatus, [?ACTIVE_STATUS, ?SKIPPED_STATUS]),

    Diff = fun(AtmWorkflowExecution = #atm_workflow_execution{
        status = CurrStatus,
        lanes = AtmLaneExecutions
    }) ->
        AtmLanExecution = lists:nth(AtmLaneExecutionIndex, AtmLaneExecutions),

        case atm_lane_execution:update_task_status(
            AtmParallelBoxExecutionIndex, AtmTaskExecutionId,
            NewAtmTaskExecutionStatus, AtmLanExecution
        ) of
            {ok, NewLaneExecution} ->
                NewAtmLaneExecutions = lists_utils:replace_at(
                    NewLaneExecution, AtmLaneExecutionIndex, AtmLaneExecutions
                ),
                NewAtmWorkflowExecution = AtmWorkflowExecution#atm_workflow_execution{
                    status = case {CurrStatus, HasTaskStarted} of
                        {?ENQUEUED_STATUS, true} ->
                            % Workflow transition to ?ACTIVE_STATUS when first task has started
                            ?ACTIVE_STATUS;
                        _ ->
                            CurrStatus
                    end,
                    lanes = NewAtmLaneExecutions
                },
                {ok, set_times_on_phase_transition(NewAtmWorkflowExecution)};
            {error, _} = Error ->
                Error
        end
    end,
    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} ->
            ensure_in_proper_phase_tree(AtmWorkflowExecutionDoc);
        {error, _} ->
            % Race with other process which must have already updated task status
            ok
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec status_to_phase(atm_workflow_execution:status()) ->
    atm_workflow_execution:phase().
status_to_phase(?SCHEDULED_STATUS) -> ?WAITING_PHASE;
status_to_phase(?ACTIVE_STATUS) -> ?ONGOING_PHASE;
status_to_phase(?ABORTING_STATUS) -> ?ONGOING_PHASE;
status_to_phase(?FINISHED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?CANCELLED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?FAILED_STATUS) -> ?ENDED_PHASE.


%% @private
-spec set_times_on_phase_transition(atm_workflow_execution:record()) ->
    atm_workflow_execution:record().
set_times_on_phase_transition(AtmWorkflowExecution = #atm_workflow_execution{
    schedule_time = ScheduleTime,
    start_time = StartTime
}) ->
    case has_phase_transition_occurred(AtmWorkflowExecution) of
        {true, ?WAITING_PHASE, ?ENDED_PHASE} ->
            AtmWorkflowExecution#atm_workflow_execution{
                finish_time = global_clock:monotonic_timestamp_seconds(ScheduleTime)
            };
        {true, ?WAITING_PHASE, ?ONGOING_PHASE} ->
            AtmWorkflowExecution#atm_workflow_execution{
                start_time = global_clock:monotonic_timestamp_seconds(ScheduleTime)
            };
        {true, ?ONGOING_PHASE, ?ENDED_PHASE} ->
            AtmWorkflowExecution#atm_workflow_execution{
                finish_time = global_clock:monotonic_timestamp_seconds(StartTime)
            };
        false ->
            AtmWorkflowExecution
    end.


%% @private
-spec ensure_in_proper_phase_tree(atm_workflow_execution:doc()) -> ok.
ensure_in_proper_phase_tree(#document{value = AtmWorkflowExecution} = AtmWorkflowExecutionDoc) ->
    case has_phase_transition_occurred(AtmWorkflowExecution) of
        {true, ?WAITING_PHASE, ?ENDED_PHASE} ->
            atm_ended_workflow_executions:add(AtmWorkflowExecutionDoc),
            atm_waiting_workflow_executions:delete(AtmWorkflowExecutionDoc);
        {true, ?WAITING_PHASE, ?ONGOING_PHASE} ->
            atm_ongoing_workflow_executions:add(AtmWorkflowExecutionDoc),
            atm_waiting_workflow_executions:delete(AtmWorkflowExecutionDoc);
        {true, ?ONGOING_PHASE, ?ENDED_PHASE} ->
            atm_ended_workflow_executions:add(AtmWorkflowExecutionDoc),
            atm_ongoing_workflow_executions:delete(AtmWorkflowExecutionDoc);
        false ->
            ok
    end.


-spec has_phase_transition_occurred(atm_workflow_execution:record()) ->
    false | {true, atm_workflow_execution:phase(), atm_workflow_execution:phase()}.
has_phase_transition_occurred(#atm_workflow_execution{
    status = CurrStatus,
    prev_status = PrevStatus
}) ->
    case {status_to_phase(PrevStatus), status_to_phase(CurrStatus)} of
        {SamePhase, SamePhase} -> false;
        {PrevPhase, CurrPhase} -> {true, PrevPhase, CurrPhase}
    end.
