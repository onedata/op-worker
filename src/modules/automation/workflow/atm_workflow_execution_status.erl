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
-export([
    handle_preparing/1,
    handle_enqueued/1,
    handle_aborting/2,
    handle_ended/1
]).
-export([
    infer_phase/1,
    report_task_status_change/5
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec handle_preparing(atm_workflow_execution:id()) ->
    {ok, atm_workflow_execution:doc()} | no_return().
handle_preparing(AtmWorkflowExecutionId) ->
    handle_transition_within_waiting_phase(AtmWorkflowExecutionId, ?PREPARING_STATUS).


-spec handle_enqueued(atm_workflow_execution:id()) ->
    {ok, atm_workflow_execution:doc()} | no_return().
handle_enqueued(AtmWorkflowExecutionId) ->
    handle_transition_within_waiting_phase(AtmWorkflowExecutionId, ?ENQUEUED_STATUS).


-spec handle_aborting(atm_workflow_execution:id(), cancel | failure) ->
    ok | {error, already_ended}.
handle_aborting(AtmWorkflowExecutionId, Reason) ->
    Diff = fun(AtmWorkflowExecution) ->
        case infer_phase(AtmWorkflowExecution) of
            ?ENDED_PHASE ->
                {error, already_ended};
            _ ->
                {ok, set_times_on_phase_transition(AtmWorkflowExecution#atm_workflow_execution{
                    status = ?ABORTING_STATUS,
                    aborting_reason = Reason
                })}
        end
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} ->
            ensure_in_proper_phase_tree(AtmWorkflowExecutionDoc);
        {error, already_ended} = Error ->
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


-spec infer_phase(atm_workflow_execution:record()) -> atm_workflow_execution:phase().
infer_phase(#atm_workflow_execution{status = Status}) ->
    status_to_phase(Status).


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
status_to_phase(?PREPARING_STATUS) -> ?WAITING_PHASE;
status_to_phase(?ENQUEUED_STATUS) -> ?WAITING_PHASE;
status_to_phase(?ACTIVE_STATUS) -> ?ONGOING_PHASE;
status_to_phase(?ABORTING_STATUS) -> ?ONGOING_PHASE;
status_to_phase(?FINISHED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?CANCELLED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?FAILED_STATUS) -> ?ENDED_PHASE.


%% @private
-spec handle_transition_within_waiting_phase(
    atm_workflow_execution:id(),
    ?PREPARING_STATUS | ?ENQUEUED_STATUS
) ->
    {ok, atm_workflow_execution:doc()} | no_return().
handle_transition_within_waiting_phase(AtmWorkflowExecutionId, NextStatus) ->
    Diff = fun(#atm_workflow_execution{status = Status} = AtmWorkflowExecution) ->
        IsTransitionAllowed = case {Status, NextStatus} of
            {?SCHEDULED_STATUS, ?PREPARING_STATUS} -> true;
            {?PREPARING_STATUS, ?ENQUEUED_STATUS} -> true;
            _ -> false
        end,
        case IsTransitionAllowed of
            true ->
                {ok, AtmWorkflowExecution#atm_workflow_execution{status = NextStatus}};
            false ->
                {error, Status}
        end
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, _} = Result ->
            Result;
        {error, CurrStatus} ->
            throw(?ERROR_ATM_INVALID_STATUS_TRANSITION(CurrStatus, NextStatus))
    end.


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
