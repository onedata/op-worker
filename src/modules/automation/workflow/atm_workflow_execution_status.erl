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
    infer_phase/1,
    handle_transition_in_waiting_phase/2,
    report_task_status_change/5
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec infer_phase(atm_workflow_execution:record()) -> atm_workflow_execution:phase().
infer_phase(#atm_workflow_execution{status = ?SCHEDULED_STATUS}) -> ?WAITING_PHASE;
infer_phase(#atm_workflow_execution{status = ?PREPARING_STATUS}) -> ?WAITING_PHASE;
infer_phase(#atm_workflow_execution{status = ?ENQUEUED_STATUS}) -> ?WAITING_PHASE;
infer_phase(#atm_workflow_execution{status = ?ACTIVE_STATUS}) -> ?ONGOING_PHASE;
infer_phase(#atm_workflow_execution{status = ?FINISHED_STATUS}) -> ?ENDED_PHASE;
infer_phase(#atm_workflow_execution{status = ?FAILED_STATUS}) -> ?ENDED_PHASE.


-spec handle_transition_in_waiting_phase(
    atm_workflow_execution:id(),
    ?PREPARING_STATUS | ?ENQUEUED_STATUS | ?FAILED_STATUS
) ->
    {ok, atm_workflow_execution:doc()} | no_return().
handle_transition_in_waiting_phase(AtmWorkflowExecutionId, WaitingStatus) when
    WaitingStatus == ?PREPARING_STATUS;
    WaitingStatus == ?ENQUEUED_STATUS
->
    Diff = fun(#atm_workflow_execution{status = Status} = AtmWorkflowExecution) ->
        case atm_status_utils:is_transition_allowed(Status, WaitingStatus) of
            true ->
                {ok, AtmWorkflowExecution#atm_workflow_execution{status = WaitingStatus}};
            false ->
                {error, Status}
        end
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, _} = Result ->
            Result;
        {error, CurrStatus} ->
            throw(?ERROR_ATM_INVALID_STATUS_TRANSITION(CurrStatus, WaitingStatus))
    end;

handle_transition_in_waiting_phase(AtmWorkflowExecutionId, ?FAILED_STATUS) ->
    % TODO VFS-7674 should fail here change status of all lanes, pboxes and tasks ??
    Diff = fun
        (AtmWorkflowExecution = #atm_workflow_execution{
            status = WaitingStatus,
            schedule_time = ScheduleTime
        }) when WaitingStatus == ?PREPARING_STATUS; WaitingStatus == ?ENQUEUED_STATUS ->
            {ok, AtmWorkflowExecution#atm_workflow_execution{
                status = ?FAILED_STATUS,
                finish_time = global_clock:monotonic_timestamp_seconds(ScheduleTime)
            }};
        (#atm_workflow_execution{status = Status}) ->
            {error, Status}
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} = Result ->
            move_from_waiting_to_ended_phase(AtmWorkflowExecutionDoc),
            Result;
        {error, CurrStatus} ->
            throw(?ERROR_ATM_INVALID_STATUS_TRANSITION(CurrStatus, ?FAILED_STATUS))
    end.


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
    Diff = fun(AtmWorkflowExecution = #atm_workflow_execution{lanes = AtmLaneExecutions}) ->
        AtmLanExecution = lists:nth(AtmLaneExecutionIndex, AtmLaneExecutions),

        case atm_lane_execution:update_task_status(
            AtmParallelBoxExecutionIndex, AtmTaskExecutionId,
            NewAtmTaskExecutionStatus, AtmLanExecution
        ) of
            {ok, NewLaneExecution} ->
                NewAtmLaneExecutions = atm_status_utils:replace_at(
                    NewLaneExecution, AtmLaneExecutionIndex, AtmLaneExecutions
                ),
                NewAtmWorkflowExecutionStatus = atm_status_utils:converge(
                    atm_lane_execution:gather_statuses(NewAtmLaneExecutions)
                ),
                NewAtmWorkflowExecution = AtmWorkflowExecution#atm_workflow_execution{
                    status = NewAtmWorkflowExecutionStatus,
                    lanes = NewAtmLaneExecutions
                },
                {ok, set_times_on_phase_transition(NewAtmWorkflowExecution)};
            {error, _} = Error ->
                Error
        end
    end,
    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} ->
            ensure_proper_phase(AtmWorkflowExecutionDoc);
        {error, _} ->
            % Race with other process which must have already updated task status
            ok
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec set_times_on_phase_transition(atm_workflow_execution:record()) ->
    atm_workflow_execution:record().
set_times_on_phase_transition(AtmWorkflowExecution = #atm_workflow_execution{
    schedule_time = ScheduleTime,
    start_time = StartTime
}) ->
    case has_phase_transition_occurred(AtmWorkflowExecution) of
        {true, ?ONGOING_PHASE} ->
            AtmWorkflowExecution#atm_workflow_execution{
                start_time = global_clock:monotonic_timestamp_seconds(ScheduleTime)
            };
        {true, ?ENDED_PHASE} ->
            AtmWorkflowExecution#atm_workflow_execution{
                finish_time = global_clock:monotonic_timestamp_seconds(StartTime)
            };
        false ->
            AtmWorkflowExecution
    end.


%% @private
-spec ensure_proper_phase(atm_workflow_execution:doc()) -> ok.
ensure_proper_phase(Doc = #document{value = AtmWorkflowExecution}) ->
    case has_phase_transition_occurred(AtmWorkflowExecution) of
        {true, ?ONGOING_PHASE} ->
            move_from_waiting_to_ongoing_phase(Doc);
        {true, ?ENDED_PHASE} ->
            move_from_ongoing_to_ended_phase(Doc);
        false ->
            ok
    end.


-spec has_phase_transition_occurred(atm_workflow_execution:record()) ->
    {true, atm_workflow_execution:phase()} | false.
has_phase_transition_occurred(#atm_workflow_execution{status_changed = false}) ->
    % no phase transition
    false;

has_phase_transition_occurred(#atm_workflow_execution{status = ?ACTIVE_STATUS}) ->
    {true, ?ONGOING_PHASE};

has_phase_transition_occurred(#atm_workflow_execution{status = EndedStatus}) when
    EndedStatus == ?FINISHED_STATUS;
    EndedStatus == ?FAILED_STATUS
->
    {true, ?ENDED_PHASE};

has_phase_transition_occurred(#atm_workflow_execution{}) ->
    % status transition within one phase
    false.


%% @private
-spec move_from_waiting_to_ended_phase(atm_workflow_execution:doc()) -> ok.
move_from_waiting_to_ended_phase(AtmWorkflowExecutionDoc) ->
    atm_ended_workflow_executions:add(AtmWorkflowExecutionDoc),
    atm_waiting_workflow_executions:delete(AtmWorkflowExecutionDoc).


%% @private
-spec move_from_waiting_to_ongoing_phase(atm_workflow_execution:doc()) -> ok.
move_from_waiting_to_ongoing_phase(AtmWorkflowExecutionDoc) ->
    atm_ongoing_workflow_executions:add(AtmWorkflowExecutionDoc),
    atm_waiting_workflow_executions:delete(AtmWorkflowExecutionDoc).


%% @private
-spec move_from_ongoing_to_ended_phase(atm_workflow_execution:doc()) -> ok.
move_from_ongoing_to_ended_phase(AtmWorkflowExecutionDoc) ->
    atm_ended_workflow_executions:add(AtmWorkflowExecutionDoc),
    atm_ongoing_workflow_executions:delete(AtmWorkflowExecutionDoc).
