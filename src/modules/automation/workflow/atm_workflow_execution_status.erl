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
%%%                                      *
%%%                                      |
%%%                                      v
%%% W                             +-------------+
%%% A                             |  SCHEDULED  |-------- ^stopping ------
%%% I                             +-------------+                          \
%%% T  +------------+                    |                                  |
%%% I  |  RESUMING  |       first lane run transitioned                     |
%%% N  +------------+            to preparing status                        |
%%% G    ^    |   |                      |                                  |
%%%      |    |   |                      |                                  |
%%%      |    |   |                      |                                  |
%%% =====|====|===|======================|==================================|================================
%%%      |    |    \                     v                                  |
%%%      |    |      -- resumed -> +-------------+                          |
%%%      |    |                    |    ACTIVE   |------- ^stopping --------o
%%%      |    |                    +-------------+                          |
%%%      |    |                           |                                 |       ____
%%%  O   |    |                           |                                 v     /      \ overriding ^stopping
%%%  N   |    |             last lane run execution ended  --------> +-------------+     /        reason
%%%  G   |     \                                                     |   STOPPING  | <--
%%%  O   |       ------------------- ^stopping --------------------> +-------------+ <-------------------
%%%  I   |                                                                |                               \
%%%  N   |                                                                |                                |
%%%  G   |                 ------------------------------------ last lane run execution                    |
%%%      |               /                                                |                                |
%%%      |      ended with all items                                      |                                |
%%%      |          /         \                          stopped due to ^stopping reason                   |
%%%      |         /           \                         /       /     |      |        \                   |
%%%      |   successfully       |                       /       /      |      |         \                  |
%%%      |    processed         |                      1*      2*      3*     4*         5*                |
%%%      |       |            else                    /       /        |      |           \                |
%%%      |       |              |                    /       /         |      |            \               |
%%% =====|=======|==============|===================/=======/==========|======|=============\==============|====
%%%      |       |              |                  /       /           |      |              \             |
%%%  S   |       |              |                 /       /            |      |               \            2*
%%%  U   |       |              |                /       /             |      |                v           |
%%%  S   |       |              |               /       /              |      |           +----------+     |
%%%  P   |       |              |              /       /               |      |           |  PAUSED  |-----o
%%%  E   |       |              |             /       /                |      |           +----------+     |
%%%  N   |       |              |            /       /                 |      v                 |          |
%%%  D   |       |              |           /       /                  |    +-------------+     |         /
%%%  E   |       |              |          /       /                   |    | INTERRUPTED | -------------
%%%  D   |       |              |         /       /                    |    +-------------+     |
%%%      |       |              |        /       /                     |      |                 |
%%%  ====|=======|==============|=======/=======/======================|======|=================|===============
%%%      |       |              |      /       /                       |      |                 |
%%%      |       |              |     /       /                        |      |                 |
%%%  E   |       v              v    v       v                         v      |                 |
%%%  N   |  +----------+     +--------+    +-----------+      +-----------+   |                 |
%%%  D   |  | FINISHED |     | FAILED |    | CANCELLED |      |  CRASHED  |   |                 |
%%%  E   |  +----------+     +--------+    +-----------+      +-----------+   |                 |
%%%  D   |                       |                                            |                 |
%%%      |                 force continue                                  resume            resume
%%%       \                      |                                            |                /
%%%         ---------------------o--------------------------------------------o---------------
%%%
%%% Workflow transition to STOPPING status when last lane run ended execution and all items were
%%% processed but also when execution is halted (^stopping) and not all items were processed
%%% (results for already scheduled items must be awaited although no more items are scheduled).
%%% Possible reasons for ^stopping workflow execution when not all items were processed are as follows:
%%% 1* - failure stopping entire automation workflow execution (e.g. error when processing task
%%%      uncorrelated results or failure of last possible automatic lane run retry).
%%% 2* - user cancelling entire automation workflow execution.
%%% 3* - unhandled exception occurred.
%%% 4* - abrupt interruption when some other component (e.g user offline session expired)
%%%      has failed and entire automation workflow execution is being stopped.
%%% 5* - user pausing entire automation workflow execution.
%%%
%%% NOTE: When atm workflow execution is stopped the workflow execution status is
%%% copied from last executed lane run which works because:
%%% 1) if last lane run successfully finished than all previous lane runs must
%%%    have also successfully finished.
%%% 2) if last lane run stopped then entire workflow execution is stopped with
%%%    the same reason.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_status).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    infer_phase/1,
    status_to_phase/1
]).
-export([
    handle_lane_run_preparing/3,
    handle_lane_run_created/2,
    handle_lane_run_enqueued/2,
    handle_lane_run_resumed/2,
    handle_lane_run_stopping/3,
    handle_lane_run_stopped/2,
    handle_lane_run_task_status_change/2,
    handle_stopped/1,
    handle_crashed/1,
    handle_discard/1,
    handle_manual_lane_run_repeat/2,
    handle_resume/2,
    handle_forced_continue/1
]).


% Function taking as argument entire atm_workflow_execution record, modifying lane run
% and returning updated atm_workflow_execution record (or record)
-type lane_run_diff() :: fun((atm_workflow_execution:record()) ->
    {ok, atm_workflow_execution:record()} | errors:error()
).


%%%===================================================================
%%% API
%%%===================================================================


-spec infer_phase(atm_workflow_execution:record()) -> atm_workflow_execution:phase().
infer_phase(#atm_workflow_execution{status = Status}) ->
    status_to_phase(Status).


-spec status_to_phase(atm_workflow_execution:status()) ->
    atm_workflow_execution:phase().
status_to_phase(?RESUMING_STATUS) -> ?WAITING_PHASE;
status_to_phase(?SCHEDULED_STATUS) -> ?WAITING_PHASE;

status_to_phase(?ACTIVE_STATUS) -> ?ONGOING_PHASE;
status_to_phase(?STOPPING_STATUS) -> ?ONGOING_PHASE;

status_to_phase(?INTERRUPTED_STATUS) -> ?SUSPENDED_PHASE;
status_to_phase(?PAUSED_STATUS) -> ?SUSPENDED_PHASE;

status_to_phase(?FINISHED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?CRASHED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?CANCELLED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?FAILED_STATUS) -> ?ENDED_PHASE.


-spec handle_lane_run_preparing(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id(),
    lane_run_diff()
) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_lane_run_preparing(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmLaneRunDiff) ->
    Diff = fun
        (Record = #atm_workflow_execution{status = Status}) when
            Status =:= ?SCHEDULED_STATUS;
            Status =:= ?RESUMING_STATUS
        ->
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

        (#atm_workflow_execution{status = ?STOPPING_STATUS}) ->
            ?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING;

        (#atm_workflow_execution{status = Status}) when
            Status =:= ?FINISHED_STATUS;
            Status =:= ?FAILED_STATUS;
            Status =:= ?CANCELLED_STATUS;
            Status =:= ?CRASHED_STATUS;
            Status =:= ?INTERRUPTED_STATUS;
            Status =:= ?PAUSED_STATUS
        ->
            ?ERROR_ATM_WORKFLOW_EXECUTION_STOPPED
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} = Result ->
            ensure_in_proper_phase_tree(AtmWorkflowExecutionDoc),
            Result;
        {error, _} = Error ->
            Error
    end.


-spec handle_lane_run_created(atm_workflow_execution:id(), lane_run_diff()) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_lane_run_created(AtmWorkflowExecutionId, AtmLaneRunDiff) ->
    atm_workflow_execution:update(AtmWorkflowExecutionId, fun
        (Record = #atm_workflow_execution{status = Status}) when
            % Account for lane run preparing in advance (it may be created when
            % current lane run still hasn't started preparing or finished resuming)
            Status =:= ?RESUMING_STATUS;
            Status =:= ?SCHEDULED_STATUS;

            Status =:= ?ACTIVE_STATUS
        ->
            AtmLaneRunDiff(Record);

        (#atm_workflow_execution{status = ?STOPPING_STATUS}) ->
            ?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING;

        (#atm_workflow_execution{status = Status}) when
            Status =:= ?FINISHED_STATUS;
            Status =:= ?FAILED_STATUS;
            Status =:= ?CANCELLED_STATUS;
            Status =:= ?CRASHED_STATUS;
            Status =:= ?INTERRUPTED_STATUS;
            Status =:= ?PAUSED_STATUS
        ->
            ?ERROR_ATM_WORKFLOW_EXECUTION_STOPPED
    end).


-spec handle_lane_run_enqueued(atm_workflow_execution:id(), lane_run_diff()) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_lane_run_enqueued(AtmWorkflowExecutionId, AtmLaneRunDiff) ->
    atm_workflow_execution:update(AtmWorkflowExecutionId, fun
        (Record = #atm_workflow_execution{status = Status}) when
            % Account for lane run preparing in advance (it may be enqueued when
            % current lane run still hasn't started preparing or finished resuming)
            Status == ?RESUMING_STATUS;
            Status == ?SCHEDULED_STATUS;

            Status == ?ACTIVE_STATUS
        ->
            AtmLaneRunDiff(Record);

        (#atm_workflow_execution{status = ?STOPPING_STATUS}) ->
            ?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING;

        (#atm_workflow_execution{status = Status}) when
            Status =:= ?FINISHED_STATUS;
            Status =:= ?FAILED_STATUS;
            Status =:= ?CANCELLED_STATUS;
            Status =:= ?CRASHED_STATUS;
            Status =:= ?INTERRUPTED_STATUS;
            Status =:= ?PAUSED_STATUS
        ->
            ?ERROR_ATM_WORKFLOW_EXECUTION_STOPPED
    end).


-spec handle_lane_run_resumed(atm_workflow_execution:id(), lane_run_diff()) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_lane_run_resumed(AtmWorkflowExecutionId, AtmLaneRunDiff) ->
    Diff = fun
        (Record = #atm_workflow_execution{status = ?RESUMING_STATUS}) ->
            case AtmLaneRunDiff(Record) of
                {ok, NewRecord} ->
                    {ok, set_times_on_phase_transition(NewRecord#atm_workflow_execution{
                        status = ?ACTIVE_STATUS
                    })};
                {error, _} = Error ->
                    Error
            end;

        (#atm_workflow_execution{status = ?STOPPING_STATUS}) ->
            ?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING;

        (#atm_workflow_execution{status = Status}) when
            Status =:= ?FINISHED_STATUS;
            Status =:= ?FAILED_STATUS;
            Status =:= ?CANCELLED_STATUS;
            Status =:= ?CRASHED_STATUS;
            Status =:= ?INTERRUPTED_STATUS;
            Status =:= ?PAUSED_STATUS
        ->
            ?ERROR_ATM_WORKFLOW_EXECUTION_STOPPED
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} = Result ->
            ensure_in_proper_phase_tree(AtmWorkflowExecutionDoc),
            Result;
        {error, _} = Error ->
            Error
    end.


-spec handle_lane_run_stopping(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:id(),
    lane_run_diff()
) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_lane_run_stopping(AtmLaneRunSelector, AtmWorkflowExecutionId, AtmLaneRunDiff) ->
    Diff = fun
        (Record = #atm_workflow_execution{status = Status}) when
            Status == ?RESUMING_STATUS;
            Status == ?SCHEDULED_STATUS;
            Status == ?ACTIVE_STATUS;
            Status == ?STOPPING_STATUS;

            % Suspended execution can be cancelled
            Status == ?INTERRUPTED_STATUS;
            Status == ?PAUSED_STATUS
        ->
            UpdateResult = AtmLaneRunDiff(Record),

            case {atm_lane_execution:is_current_lane_run(AtmLaneRunSelector, Record), UpdateResult} of
                {true, {ok, NewRecord}} ->
                    {ok, set_times_on_phase_transition(NewRecord#atm_workflow_execution{
                        status = ?STOPPING_STATUS
                    })};
                _ ->
                    UpdateResult
            end;

        (#atm_workflow_execution{status = Status}) when
            Status =:= ?FINISHED_STATUS;
            Status =:= ?FAILED_STATUS;
            Status =:= ?CANCELLED_STATUS;
            Status =:= ?CRASHED_STATUS
        ->
            ?ERROR_ATM_WORKFLOW_EXECUTION_ENDED
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} = Result ->
            ensure_in_proper_phase_tree(AtmWorkflowExecutionDoc),
            Result;
        {error, _} = Error ->
            Error
    end.


-spec handle_lane_run_stopped(atm_workflow_execution:id(), lane_run_diff()) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_lane_run_stopped(AtmWorkflowExecutionId, AtmLaneRunDiff) ->
    atm_workflow_execution:update(AtmWorkflowExecutionId, fun
        (Record = #atm_workflow_execution{status = ?ACTIVE_STATUS}) ->
            case AtmLaneRunDiff(Record) of
                {ok, NewRecord} ->
                    case atm_lane_execution:get_run({current, current}, NewRecord) of
                        {ok, _LastLaneRun = #atm_lane_execution_run{status = RunStatus}} when
                            RunStatus =:= ?FINISHED_STATUS;
                            RunStatus =:= ?FAILED_STATUS
                        ->
                            {ok, NewRecord#atm_workflow_execution{status = ?STOPPING_STATUS}};
                        _ ->
                            {ok, NewRecord}
                    end;
                {error, _} = Error ->
                    Error
            end;

        (Record) ->
            AtmLaneRunDiff(Record)
    end).


-spec handle_lane_run_task_status_change(atm_workflow_execution:id(), lane_run_diff()) ->
    ok | errors:error().
handle_lane_run_task_status_change(AtmWorkflowExecutionId, AtmLaneRunDiff) ->
    Diff = fun(Record) ->
        case infer_phase(Record) of
            ?SUSPENDED_PHASE -> ?ERROR_ATM_WORKFLOW_EXECUTION_STOPPED;
            ?ENDED_PHASE -> ?ERROR_ATM_WORKFLOW_EXECUTION_STOPPED;
            _ -> AtmLaneRunDiff(Record)
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


-spec handle_stopped(atm_workflow_execution:id()) ->
    {ok, atm_workflow_execution:doc()} | no_return().
handle_stopped(AtmWorkflowExecutionId) ->
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


-spec handle_crashed(atm_workflow_execution:id()) ->
    {ok, atm_workflow_execution:doc()} | no_return().
handle_crashed(AtmWorkflowExecutionId) ->
    Diff = fun(Record) ->
        {ok, set_times_on_phase_transition(Record#atm_workflow_execution{
            status = ?CRASHED_STATUS
        })}
    end,

    Result = {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:update(
        AtmWorkflowExecutionId, Diff
    ),
    ensure_in_proper_phase_tree(AtmWorkflowExecutionDoc),

    Result.


-spec handle_discard(atm_workflow_execution:id()) ->
    ok | errors:error().
handle_discard(AtmWorkflowExecutionId) ->
    Diff = fun(Record) ->
        case infer_phase(Record) of
            ?SUSPENDED_PHASE -> {ok, Record#atm_workflow_execution{discarded = true}};
            ?ENDED_PHASE -> {ok, Record#atm_workflow_execution{discarded = true}};
            _ -> ?ERROR_ATM_WORKFLOW_EXECUTION_NOT_STOPPED
        end
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff, include_discarded) of
        {ok, #document{value = AtmWorkflowExecution} = AtmWorkflowExecutionDoc} ->
            case infer_phase(AtmWorkflowExecution) of
                ?SUSPENDED_PHASE ->
                    workflow_engine:cleanup_execution(AtmWorkflowExecutionId),
                    atm_discarded_workflow_executions:add(AtmWorkflowExecutionId),
                    atm_suspended_workflow_executions:delete(AtmWorkflowExecutionDoc);
                ?ENDED_PHASE ->
                    atm_discarded_workflow_executions:add(AtmWorkflowExecutionId),
                    atm_ended_workflow_executions:delete(AtmWorkflowExecutionDoc)
            end;
        {error, _} = Error ->
            Error
    end.


-spec handle_manual_lane_run_repeat(atm_workflow_execution:id(), lane_run_diff()) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_manual_lane_run_repeat(AtmWorkflowExecutionId, AtmLaneRunDiff) ->
    Diff = fun(Record) ->
        case infer_phase(Record) of
            ?ENDED_PHASE ->
                case AtmLaneRunDiff(Record) of
                    {ok, NewRecord} ->
                        {ok, NewRecord#atm_workflow_execution{
                            status = ?SCHEDULED_STATUS,
                            incarnation = Record#atm_workflow_execution.incarnation + 1,
                            schedule_time = global_clock:timestamp_seconds()
                        }};
                    {error, _} = Error ->
                        Error
                end;
            _ ->
                ?ERROR_ATM_WORKFLOW_EXECUTION_NOT_ENDED
        end
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} = Result ->
            ensure_in_proper_phase_tree(AtmWorkflowExecutionDoc),
            Result;
        {error, _} = Error ->
            Error
    end.


-spec handle_resume(atm_workflow_execution:id(), lane_run_diff()) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_resume(AtmWorkflowExecutionId, AtmLaneRunDiff) ->
    Diff = fun
        (Record = #atm_workflow_execution{status = Status}) when
            Status =:= ?INTERRUPTED_STATUS;
            Status =:= ?PAUSED_STATUS
        ->
            AtmLaneRunDiff(Record#atm_workflow_execution{
                status = ?RESUMING_STATUS,
                schedule_time = global_clock:timestamp_seconds(),
                incarnation = Record#atm_workflow_execution.incarnation + 1
            });

        (_) ->
            ?ERROR_ATM_WORKFLOW_EXECUTION_NOT_RESUMABLE
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} = Result ->
            ensure_in_proper_phase_tree(AtmWorkflowExecutionDoc),
            Result;
        {error, _} = Error ->
            Error
    end.


-spec handle_forced_continue(atm_workflow_execution:id()) ->
    {ok, atm_workflow_execution:doc()} | errors:error().
handle_forced_continue(AtmWorkflowExecutionId) ->
    Diff = fun
        (Record = #atm_workflow_execution{
            status = ?FAILED_STATUS,
            current_lane_index = CurrentAtmLaneIndex,
            current_run_num = CurrentAtmRunNum,
            lanes_count = AtmLanesCount
        }) when CurrentAtmLaneIndex < AtmLanesCount ->

            NextAtmLaneIndex = CurrentAtmLaneIndex + 1,
            NextRun = #atm_lane_execution_run{
                run_num = CurrentAtmRunNum,
                status = ?SCHEDULED_STATUS
            },
            Diff = fun(_Run) -> ?ERROR_ALREADY_EXISTS end,

            NewAtmWorkflowExecution = Record#atm_workflow_execution{
                status = ?RESUMING_STATUS,
                current_lane_index = NextAtmLaneIndex,
                schedule_time = global_clock:timestamp_seconds(),
                incarnation = Record#atm_workflow_execution.incarnation + 1
            },
            atm_lane_execution:update_run(
                {NextAtmLaneIndex, CurrentAtmRunNum}, Diff, NextRun, NewAtmWorkflowExecution
            );

        (_) ->
            ?ERROR_ATM_WORKFLOW_EXECUTION_NOT_RESUMABLE
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
-spec set_times_on_phase_transition(atm_workflow_execution:record()) ->
    atm_workflow_execution:record().
set_times_on_phase_transition(AtmWorkflowExecution = #atm_workflow_execution{
    schedule_time = ScheduleTime,
    start_time = StartTime,
    suspend_time = SuspendTime
}) ->
    case has_phase_transition_occurred(AtmWorkflowExecution) of
        {true, ?WAITING_PHASE, ?ONGOING_PHASE} ->
            AtmWorkflowExecution#atm_workflow_execution{
                start_time = global_clock:monotonic_timestamp_seconds(ScheduleTime)
            };

        {true, ?ONGOING_PHASE, ?SUSPENDED_PHASE} ->
            AtmWorkflowExecution#atm_workflow_execution{
                suspend_time = global_clock:monotonic_timestamp_seconds(StartTime)
            };

        {true, ?ONGOING_PHASE, ?ENDED_PHASE} ->
            AtmWorkflowExecution#atm_workflow_execution{
                finish_time = global_clock:monotonic_timestamp_seconds(StartTime)
            };

        % Cancelling suspended execution
        {true, ?SUSPENDED_PHASE, ?ONGOING_PHASE} ->
            AtmWorkflowExecution#atm_workflow_execution{
                start_time = global_clock:monotonic_timestamp_seconds(SuspendTime)
            };

        % Resuming execution
        {true, ?SUSPENDED_PHASE, ?WAITING_PHASE} ->
            AtmWorkflowExecution#atm_workflow_execution{
                schedule_time = global_clock:monotonic_timestamp_seconds(SuspendTime)
            };

        % Manual lane run repeat
        {true, ?ENDED_PHASE, ?WAITING_PHASE} ->
            AtmWorkflowExecution#atm_workflow_execution{
                schedule_time = global_clock:monotonic_timestamp_seconds(SuspendTime)
            };

        false ->
            AtmWorkflowExecution
    end.


%% @private
-spec ensure_in_proper_phase_tree(atm_workflow_execution:doc()) -> ok.
ensure_in_proper_phase_tree(#document{value = AtmWorkflowExecution} = AtmWorkflowExecutionDoc) ->
    case has_phase_transition_occurred(AtmWorkflowExecution) of
        {true, ?WAITING_PHASE, ?ONGOING_PHASE} ->
            atm_ongoing_workflow_executions:add(AtmWorkflowExecutionDoc),
            atm_waiting_workflow_executions:delete(AtmWorkflowExecutionDoc);

        {true, ?ONGOING_PHASE, ?SUSPENDED_PHASE} ->
            atm_suspended_workflow_executions:add(AtmWorkflowExecutionDoc),
            atm_ongoing_workflow_executions:delete(AtmWorkflowExecutionDoc);

        {true, ?ONGOING_PHASE, ?ENDED_PHASE} ->
            atm_ended_workflow_executions:add(AtmWorkflowExecutionDoc),
            atm_ongoing_workflow_executions:delete(AtmWorkflowExecutionDoc);

        % Cancelling suspended execution
        {true, ?SUSPENDED_PHASE, ?ONGOING_PHASE} ->
            atm_ongoing_workflow_executions:add(AtmWorkflowExecutionDoc),
            atm_suspended_workflow_executions:delete(AtmWorkflowExecutionDoc);

        % Resuming execution
        {true, ?SUSPENDED_PHASE, ?WAITING_PHASE} ->
            atm_waiting_workflow_executions:add(AtmWorkflowExecutionDoc),
            atm_suspended_workflow_executions:delete(AtmWorkflowExecutionDoc);

        % Manual lane run repeat
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
