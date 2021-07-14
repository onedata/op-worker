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
    handle_failed_in_waiting_phase/1
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
    handle_transition_in_waiting_phase(AtmWorkflowExecutionId, ?PREPARING_STATUS).


-spec handle_enqueued(atm_workflow_execution:id()) ->
    {ok, atm_workflow_execution:doc()} | no_return().
handle_enqueued(AtmWorkflowExecutionId) ->
    handle_transition_in_waiting_phase(AtmWorkflowExecutionId, ?ENQUEUED_STATUS).


-spec handle_failed_in_waiting_phase(atm_workflow_execution:id()) ->
    ok | no_return().
handle_failed_in_waiting_phase(AtmWorkflowExecutionId) ->
    Diff = fun(AtmWorkflowExecution = #atm_workflow_execution{
        status = Status,
        schedule_time = ScheduleTime
    }) ->
        case status_to_phase(Status) of
            ?WAITING_PHASE ->
                {ok, AtmWorkflowExecution#atm_workflow_execution{
                    status = ?FAILED_STATUS,
                    finish_time = global_clock:monotonic_timestamp_seconds(ScheduleTime)
                }};
            _ ->
                {error, Status}
        end
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} ->
            move_from_waiting_to_ended_phase(AtmWorkflowExecutionDoc),
            ok;
        {error, CurrStatus} ->
            throw(?ERROR_ATM_INVALID_STATUS_TRANSITION(CurrStatus, ?FAILED_STATUS))
    end.


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
    Diff = fun(AtmWorkflowExecution = #atm_workflow_execution{
        status = AtmWorkflowExecutionStatus,
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
                NewAtmWorkflowExecutionStatus = infer_workflow_execution_status(lists:usort(
                    atm_lane_execution:gather_statuses(NewAtmLaneExecutions)
                )),
                NewAtmWorkflowExecution = AtmWorkflowExecution#atm_workflow_execution{
                    status = NewAtmWorkflowExecutionStatus,
                    % 'status_changed' field must be changed manually here as it's value
                    % is checked right below when still in update fun (automatic update
                    % happens after returning from Diff fun)
                    status_changed = NewAtmWorkflowExecutionStatus /= AtmWorkflowExecutionStatus,
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
-spec status_to_phase(atm_workflow_execution:status()) ->
    atm_workflow_execution:phase().
status_to_phase(?SCHEDULED_STATUS) -> ?WAITING_PHASE;
status_to_phase(?PREPARING_STATUS) -> ?WAITING_PHASE;
status_to_phase(?ENQUEUED_STATUS) -> ?WAITING_PHASE;
status_to_phase(?ACTIVE_STATUS) -> ?ONGOING_PHASE;
status_to_phase(?FINISHED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?FAILED_STATUS) -> ?ENDED_PHASE.


%% @private
-spec handle_transition_in_waiting_phase(
    atm_workflow_execution:id(),
    ?PREPARING_STATUS | ?ENQUEUED_STATUS
) ->
    {ok, atm_workflow_execution:doc()} | no_return().
handle_transition_in_waiting_phase(AtmWorkflowExecutionId, NextStatus) ->
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
-spec infer_workflow_execution_status(UniqueAtmLaneExecutionStatuses :: [atm_task_execution:status()]) ->
    atm_workflow_execution:status().
infer_workflow_execution_status([?PENDING_STATUS]) ->
    ?ENQUEUED_STATUS;
infer_workflow_execution_status([Status]) ->
    Status;
infer_workflow_execution_status(Statuses) ->
    [LowestStatusPresent | _] = lists:dropwhile(
        fun(Status) -> not lists:member(Status, Statuses) end,
        [?FAILED_STATUS, ?ACTIVE_STATUS, ?PENDING_STATUS, ?FINISHED_STATUS]
    ),

    case LowestStatusPresent of
        ?PENDING_STATUS ->
            % Some lanes must have ended execution while others are still
            % pending - overall workflow execution status is active
            ?ACTIVE_STATUS;
        Status ->
            Status
    end.


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
