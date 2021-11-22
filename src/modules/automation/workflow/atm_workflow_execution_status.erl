%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions that handle atm workflow execution status
%%% transitions according to following state machine:
%%%
%%%  W
%%%  A         +-----------+
%%%  I         | SCHEDULED |------------------------------
%%%  T         +-----------+                               \
%%%  I               |                                      |
%%%  N    first lane run transitions                        |
%%%  G        to preparing status                           |
%%%                  |                                      |
%%% =================|======================================|=================
%%%                  v                                      |
%%%            +-----------+                                |
%%%            |   ACTIVE  |--------------------------------o
%%%            +-----------+                                |
%%%                       \                    currently executed lane run
%%%                        \                     failed or was cancelled
%%%  O                      \                               |
%%%  N                       \                              v
%%%  G                        \                       +-----------+
%%%  O                         \                      |  ABORTING |
%%%  I                          \                     +-----------+
%%%  N                           \                  /
%%%  G                        last executed lane run
%%%                          /          |           \
%%%                         /           |            \
%%%                        /            |             \
%%%         successfully finished     failed      was cancelled
%%%                      /              |                \
%%% ====================/===============|=================\===================
%%%                    /                |                  \
%%%  E                v                 v                   v
%%%  N     +-----------+           +-----------+           +-----------+
%%%  D     |  FINISHED |           |   FAILED  |           | CANCELLED |
%%%  E     +-----------+           +-----------+           +-----------+
%%%  D
%%%
%%% Note: When transition to ended phase occurs the workflow execution status is
%%% copied from last executed lane run which works because:
%%% 1) if last lane run successfully finished than all previous lane runs must
%%%    have also successfully finished.
%%% 2) if last lane run aborted (due to cancel or failure) than entire workflow
%%%    execution is aborted with the same reason.
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
    handle_lane_aborting/3,
    handle_lane_task_status_change/2,
    handle_manual_lane_repeat/2
]).
-export([handle_ended/1]).


% Function taking as argument entire atm_workflow_execution record, modifying lane run
% and returning updated atm_workflow_execution record (or record)
-type lane_run_diff() :: fun((atm_workflow_execution:record()) ->
    atm_workflow_execution:record() | errors:error()
).


%%%===================================================================
%%% API
%%%===================================================================


-spec infer_phase(atm_workflow_execution:record()) -> atm_workflow_execution:phase().
infer_phase(#atm_workflow_execution{status = Status}) ->
    status_to_phase(Status).


-spec handle_lane_preparing(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id(),
    lane_run_diff()
) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_lane_preparing(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmLaneRunDiff) ->
    Diff = fun
        (Record = #atm_workflow_execution{status = ?SCHEDULED_STATUS}) ->
            UpdateResult = AtmLaneRunDiff(Record),

            case {atm_lane_execution:is_current_lane_run(AtmLaneRunSelector, Record), UpdateResult} of
                {true, {ok, NewRecord}} ->
                    {ok, set_times_on_phase_transition(NewRecord#atm_workflow_execution{
                        status = ?ACTIVE_STATUS
                    })};
                _ ->
                    UpdateResult
            end;

        (Record = #atm_workflow_execution{status = ?ACTIVE_STATUS}) ->
            AtmLaneRunDiff(Record);

        (#atm_workflow_execution{status = ?ABORTING_STATUS}) ->
            ?ERROR_ATM_WORKFLOW_EXECUTION_ABORTING;

        (#atm_workflow_execution{status = _EndedStatus}) ->
            ?ERROR_ATM_WORKFLOW_EXECUTION_ENDED
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} = Result ->
            ensure_in_proper_phase_tree(AtmWorkflowExecutionDoc),
            Result;
        {error, _} = Error ->
            Error
    end.


-spec handle_lane_enqueued(atm_workflow_execution:id(), lane_run_diff()) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_lane_enqueued(AtmWorkflowExecutionId, AtmLaneRunDiff) ->
    atm_workflow_execution:update(AtmWorkflowExecutionId, fun
        (Record = #atm_workflow_execution{status = Status}) when
            Status == ?SCHEDULED_STATUS;
            Status == ?ACTIVE_STATUS
        ->
            AtmLaneRunDiff(Record);

        (#atm_workflow_execution{status = ?ABORTING_STATUS}) ->
            ?ERROR_ATM_WORKFLOW_EXECUTION_ABORTING;

        (#atm_workflow_execution{status = _EndedStatus}) ->
            ?ERROR_ATM_WORKFLOW_EXECUTION_ENDED
    end).


-spec handle_lane_aborting(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id(),
    lane_run_diff()
) ->
    ok | errors:error().
handle_lane_aborting(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmLaneRunDiff) ->
    Diff = fun
        (Record = #atm_workflow_execution{status = Status}) when
            Status == ?SCHEDULED_STATUS;
            Status == ?ACTIVE_STATUS;
            Status == ?ABORTING_STATUS
        ->
            UpdateResult = AtmLaneRunDiff(Record),

            case {atm_lane_execution:is_current_lane_run(AtmLaneRunSelector, Record), UpdateResult} of
                {true, {ok, NewRecord}} ->
                    {ok, set_times_on_phase_transition(NewRecord#atm_workflow_execution{
                        status = ?ABORTING_STATUS
                    })};
                _ ->
                    UpdateResult
            end;

        (#atm_workflow_execution{status = _EndedStatus}) ->
            ?ERROR_ATM_WORKFLOW_EXECUTION_ENDED
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} ->
            ensure_in_proper_phase_tree(AtmWorkflowExecutionDoc);
        {error, _} = Error ->
            Error
    end.


-spec handle_lane_task_status_change(atm_workflow_execution:id(), lane_run_diff()) ->
    ok | errors:error().
handle_lane_task_status_change(AtmWorkflowExecutionId, AtmLaneRunDiff) ->
    Diff = fun(Record) ->
        case infer_phase(Record) of
            ?ENDED_PHASE ->
                ?ERROR_ATM_WORKFLOW_EXECUTION_ENDED;
            _ ->
                AtmLaneRunDiff(Record)
        end
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, _} ->
            ok;
        ?ERROR_ATM_INVALID_STATUS_TRANSITION(_, _) ->
            % Race with other process which must have already updated task status
            ok;
        {error, _} = Error ->
            Error
    end.


-spec handle_ended(atm_workflow_execution:id()) ->
    {ok, atm_workflow_execution:doc()} | no_return().
handle_ended(AtmWorkflowExecutionId) ->
    Diff = fun(Record) ->
        {ok, #atm_lane_execution_run{status = Status}} = atm_lane_execution:get_run(
            {current, current}, Record
        ),
        {ok, set_times_on_phase_transition(Record#atm_workflow_execution{status = Status})}
    end,

    Result = {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:update(
        AtmWorkflowExecutionId, Diff
    ),
    ensure_in_proper_phase_tree(AtmWorkflowExecutionDoc),

    Result.


-spec handle_manual_lane_repeat(atm_workflow_execution:id(), lane_run_diff()) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_manual_lane_repeat(AtmWorkflowExecutionId, AtmLaneRunDiff) ->
    Diff = fun(Record) ->
        case infer_phase(Record) of
            ?ENDED_PHASE ->
                AtmLaneRunDiff(Record#atm_workflow_execution{
                    status = ?SCHEDULED_STATUS,
                    schedule_time = global_clock:timestamp_seconds()
                });
            _ ->
                ?ERROR_ATM_WORKFLOW_EXECUTION_ENDED  %% TODO NOT_ENDED ?
        end
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} = Result ->
            ensure_in_proper_phase_tree(AtmWorkflowExecutionDoc),
            Result;
        {error, _} = Error ->
            Error
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
        {true, ?ENDED_PHASE, ?WAITING_PHASE} ->
            atm_waiting_workflow_executions:add(AtmWorkflowExecutionDoc),
            atm_ended_workflow_executions:delete(AtmWorkflowExecutionDoc);
        false ->
            ok
    end.


-spec has_phase_transition_occurred(atm_workflow_execution:record()) ->
    false | {true, atm_workflow_execution:phase(), atm_workflow_execution:phase()}.
has_phase_transition_occurred(#atm_workflow_execution{
    status = CurrentStatus,
    prev_status = PrevStatus
}) ->
    case {status_to_phase(PrevStatus), status_to_phase(CurrentStatus)} of
        {SamePhase, SamePhase} -> false;
        {PrevPhase, CurrentPhase} -> {true, PrevPhase, CurrentPhase}
    end.
