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
%%%
%%%               +-------------+   cancel or failure
%%%               |  SCHEDULED  |------------------------
%%%  W            +-------------+                         \
%%%  A                   |                                 \
%%%  I                   v                                  |
%%%  T            +-------------+    cancel or failure      |
%%%  I            |  PREPARING  |-------------------------  |
%%%  N            +-------------+                          \|
%%%  G                   |                                  o
%%%      ready to execute (all associated                   |
%%%  P     documents created and setup)                     |
%%%  H                   v                                  |
%%%  A            +-------------+          cancel           |
%%%  S            |   ENQUEUED  |---------------------------o
%%%  E            +-------------+                           |
%%%                      |    \_____________________________|__________________________
%%%                      |                                  |                           \
%%%        first task within atm lane run                   |                   workflow aborting when
%%%                   started                               |                    preparing in advance
%%% =====================|==================================|============================|========
%%%                      v                                  |                            |
%%%  O             +-------------+         cancel           |                            |
%%%  N             |    ACTIVE   |--------------------------o                            |
%%%  G             +-------------+              ____        |                            |
%%%  O                   |             cancel /      \      v                            |
%%%  I                   |          overrides \     +-------------+     failure when     |
%%%  N                  ---           failure   --->|   ABORTING  |----- preparing ------o
%%%  G                /     \                       +-------------+      in advance      |
%%%                  /       \                         |                                 |
%%%  P              /         \         aborting when currently executed                 |
%%%  H             /           \                       |                                 |
%%%  A            /             \                 -----------                            |
%%%  S         else     any parallel box        /             \                          |
%%%  E           |        finished with      failure        cancel                       |
%%%              |           failure          /                |                         |
%%% =============|==============|============/=================|=========================|========
%%%              |              |           /                  |                         |
%%%  E           |              |          /                   |                         |
%%%  N           v              v         v                    v                         |
%%%  D    +-------------+     +-------------+          +-------------+                   |
%%%  E    |   FINISHED  |     |   FAILED    |          |  CANCELLED  |                   |
%%%  D    +-------------+     +-------------+          +-------------+                   |
%%%                                                                                      |
%%%  P                                                                                   v
%%%  H                                                                            +-------------+
%%%  A                                                                            | INTERRUPTED |
%%%  S                                                                            +-------------+
%%%  E
%%%
%%% There are 2 exceptions to above diagram:
%%% 1) provider restart - atm lane execution run is forcefully ended as failed (or interrupted
%%%    in case of lanes preparing in advance) disregarding status it had before restart.
%%% 2) interrupted atm lane execution run when preparing in advance if previous lane run
%%%    finished successfully - INTERRUPTED status is changed to FAILED as this run effectively
%%%    becomes the point of workflow execution failure at which it can be rerun.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution_status).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([status_to_phase/1]).
-export([
    handle_preparing/2,
    handle_enqueued/2,
    handle_aborting/3,
    handle_task_status_change/5,
    handle_ended/2
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


-spec status_to_phase(atm_lane_execution:status()) ->
    atm_workflow_execution:phase().
status_to_phase(?SCHEDULED_STATUS) -> ?WAITING_PHASE;
status_to_phase(?PREPARING_STATUS) -> ?WAITING_PHASE;
status_to_phase(?ENQUEUED_STATUS) -> ?WAITING_PHASE;
status_to_phase(?ACTIVE_STATUS) -> ?ONGOING_PHASE;
status_to_phase(?ABORTING_STATUS) -> ?ONGOING_PHASE;
status_to_phase(?FINISHED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?CANCELLED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?FAILED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?INTERRUPTED_STATUS) -> ?ENDED_PHASE.


-spec handle_preparing(pos_integer(), atm_workflow_execution:id()) ->
    atm_workflow_execution:doc() | no_return().
handle_preparing(LaneIndex, AtmWorkflowExecutionId) ->
    % transition to ?PREPARING_STATUS from ?SCHEDULED_STATUS
    LaneRunDiff = fun
        (#atm_lane_execution_run{status = ?SCHEDULED_STATUS} = Run) ->
            {ok, Run#atm_lane_execution_run{status = ?PREPARING_STATUS}};

        (#atm_lane_execution_run{status = Status}) ->
            ?ERROR_ATM_INVALID_STATUS_TRANSITION(Status, ?PREPARING_STATUS)
    end,
    % preparing in advance
    Default = #atm_lane_execution_run{run_no = undefined, status = ?PREPARING_STATUS},

    ?extract_doc(atm_workflow_execution_status:handle_lane_preparing(
        LaneIndex, AtmWorkflowExecutionId, fun(AtmWorkflowExecution) ->
            atm_lane_execution:update_curr_run(LaneIndex, LaneRunDiff, Default, AtmWorkflowExecution)
        end
    )).


-spec handle_enqueued(pos_integer(), atm_workflow_execution:id()) ->
    atm_workflow_execution:doc() | no_return().
handle_enqueued(AtmLaneIndex, AtmWorkflowExecutionId) ->
    Diff = fun(AtmWorkflowExecution) ->
        atm_lane_execution:update_curr_run(AtmLaneIndex, fun
            (#atm_lane_execution_run{status = ?PREPARING_STATUS} = Run) ->
                {ok, Run#atm_lane_execution_run{status = ?ENQUEUED_STATUS}};
            (#atm_lane_execution_run{status = Status}) ->
                ?ERROR_ATM_INVALID_STATUS_TRANSITION(Status, ?PREPARING_STATUS)
        end, AtmWorkflowExecution)
    end,
    ?extract_doc(atm_workflow_execution_status:handle_lane_enqueued(AtmWorkflowExecutionId, Diff)).


-spec handle_aborting(undefined | pos_integer(), atm_workflow_execution:id(), cancel | failure) ->
    ok | errors:error().
handle_aborting(AtmLaneIndex, AtmWorkflowExecutionId, Reason) ->
    Diff = fun(AtmWorkflowExecution) ->
        atm_lane_execution:update_curr_run(AtmLaneIndex, fun
            (#atm_lane_execution_run{status = Status} = Run) when
                Status =:= ?SCHEDULED_STATUS;
                Status =:= ?PREPARING_STATUS;
                Status =:= ?ENQUEUED_STATUS;
                Status =:= ?ACTIVE_STATUS
            ->
                {ok, Run#atm_lane_execution_run{status = ?ABORTING_STATUS, aborting_reason = Reason}};

            (#atm_lane_execution_run{status = ?ABORTING_STATUS, aborting_reason = PrevReason} = Run) ->
                FinalReason = case {PrevReason, Reason} of
                    {failure, cancel} -> cancel;
                    _ -> PrevReason
                end,
                {ok, Run#atm_lane_execution_run{aborting_reason = FinalReason}};

            (#atm_lane_execution_run{status = EndedStatus}) ->
                ?ERROR_ATM_INVALID_STATUS_TRANSITION(EndedStatus, ?ABORTING_STATUS)
        end, AtmWorkflowExecution)
    end,
    atm_workflow_execution_status:handle_lane_aborting(AtmLaneIndex, AtmWorkflowExecutionId, Diff).


-spec handle_task_status_change(
    atm_workflow_execution:id(),
    pos_integer(),
    pos_integer(),
    atm_task_execution:id(),
    atm_task_execution:status()
) ->
    ok | errors:error().
handle_task_status_change(
    AtmWorkflowExecutionId,
    AtmLaneIndex,
    AtmParallelBoxIndex,
    AtmTaskExecutionId,
    NewAtmTaskExecutionStatus
) ->
    HasTaskStarted = lists:member(NewAtmTaskExecutionStatus, [?ACTIVE_STATUS, ?SKIPPED_STATUS]),

    Diff = fun(AtmWorkflowExecution) ->
        atm_lane_execution:update_curr_run(AtmLaneIndex, fun(Run) ->
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


-spec handle_ended(pos_integer(), atm_workflow_execution:id()) ->
    atm_workflow_execution:doc() | no_return().
handle_ended(AtmLaneIndex, AtmWorkflowExecutionId) ->
    Diff = fun(AtmWorkflowExecution = #atm_workflow_execution{curr_lane_index = CurrLaneIndex}) ->
        case AtmLaneIndex > CurrLaneIndex of
            true ->
                handle_prepared_in_advance_lane_run_ended(AtmLaneIndex, AtmWorkflowExecution);
            false ->
                handle_currently_executed_lane_run_ended(AtmWorkflowExecution)
        end
    end,
    ?extract_doc(atm_workflow_execution:update(AtmWorkflowExecutionId, Diff)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec handle_prepared_in_advance_lane_run_ended(pos_integer(), atm_workflow_execution:record()) ->
    {ok, atm_workflow_execution:record()}.
handle_prepared_in_advance_lane_run_ended(AtmLaneIndex, AtmWorkflowExecution) ->
    atm_lane_execution:update_curr_run(AtmLaneIndex, fun(Run) ->
        {ok, Run#atm_lane_execution_run{status = ?INTERRUPTED_STATUS}}
    end, AtmWorkflowExecution).


%% @private
-spec handle_currently_executed_lane_run_ended(atm_workflow_execution:record()) ->
    {ok, atm_workflow_execution:record()}.
handle_currently_executed_lane_run_ended(AtmWorkflowExecution = #atm_workflow_execution{
    lanes_num = AtmLanesNum,
    curr_lane_index = CurrAtmLaneIndex
}) ->
    {ok, NewAtmWorkflowExecution} = end_currently_executed_lane_run(AtmWorkflowExecution),
    {ok, EndedRun} = atm_lane_execution:get_curr_run(CurrAtmLaneIndex, NewAtmWorkflowExecution),

    case EndedRun#atm_lane_execution_run.status of
        ?FAILED_STATUS when EndedRun#atm_lane_execution_run.src_run_no == undefined ->
            %% TODO proper retry with max_retries ?
            ExceptionStoreId = EndedRun#atm_lane_execution_run.exception_store_id,
            schedule_lane_run_retry(ExceptionStoreId, NewAtmWorkflowExecution);
        ?FINISHED_STATUS when CurrAtmLaneIndex < AtmLanesNum ->
            schedule_next_lane_run(NewAtmWorkflowExecution);
        _ ->
            {ok, NewAtmWorkflowExecution}
    end.


%% @private
-spec end_currently_executed_lane_run(atm_workflow_execution:record()) ->
    {ok, atm_workflow_execution:record()} | errors:error().
end_currently_executed_lane_run(AtmWorkflowExecution = #atm_workflow_execution{
    curr_lane_index = CurrAtmLaneIndex
}) ->
    atm_lane_execution:update_curr_run(CurrAtmLaneIndex, fun
        (#atm_lane_execution_run{status = Status} = Run) when
            Status =:= ?SCHEDULED_STATUS;
            Status =:= ?PREPARING_STATUS;
            Status =:= ?ENQUEUED_STATUS
        ->
            % Provider must have been restarted as otherwise it is not possible to
            % transition from waiting phase to ended phase directly
            {ok, Run#atm_lane_execution_run{status = ?FAILED_STATUS}};

        (#atm_lane_execution_run{status = ?ACTIVE_STATUS} = Run) ->
            AtmParallelBoxExecutionStatuses = atm_parallel_box_execution:gather_statuses(
                Run#atm_lane_execution_run.parallel_boxes
            ),
            EndedStatus = case lists:member(?FAILED_STATUS, AtmParallelBoxExecutionStatuses) of
                true -> ?FAILED_STATUS;
                false -> ?FINISHED_STATUS
            end,
            {ok, Run#atm_lane_execution_run{status = EndedStatus}};

        (#atm_lane_execution_run{status = ?ABORTING_STATUS} = Run) ->
            EndedStatus = case Run#atm_lane_execution_run.aborting_reason of
                cancel -> ?CANCELLED_STATUS;
                failure -> ?FAILED_STATUS
            end,
            {ok, Run#atm_lane_execution_run{status = EndedStatus}};

        (#atm_lane_execution_run{status = EndedStatus}) ->
            EndedStatus
    end, AtmWorkflowExecution).


%% @private
-spec schedule_lane_run_retry(atm_store:id(), atm_workflow_execution:record()) ->
    {ok, atm_workflow_execution:record()}.
schedule_lane_run_retry(AtmExceptionStoreId, AtmWorkflowExecution = #atm_workflow_execution{
    curr_lane_index = CurrLaneIndex,
    curr_run_no = CurrRunNo
}) ->
    NextRunNo = CurrRunNo + 1,

    Diff = fun(_Run) -> ?ERROR_ALREADY_EXISTS end,
    Default = #atm_lane_execution_run{
        run_no = NextRunNo,
        src_run_no = CurrRunNo,
        status = ?SCHEDULED_STATUS,
        iterated_store_id = AtmExceptionStoreId
    },
    NewAtmWorkflowExecution = AtmWorkflowExecution#atm_workflow_execution{curr_run_no = NextRunNo},

    atm_lane_execution:update_curr_run(CurrLaneIndex, Diff, Default, NewAtmWorkflowExecution).


%% @private
-spec schedule_next_lane_run(atm_workflow_execution:record()) ->
    {ok, atm_workflow_execution:record()}.
schedule_next_lane_run(AtmWorkflowExecution = #atm_workflow_execution{
    curr_lane_index = CurrLaneIndex,
    curr_run_no = CurrRunNo
}) ->
    % set run_no for lane run that is already preparing in advance
    Diff = fun
        (#atm_lane_execution_run{run_no = undefined, status = ?INTERRUPTED_STATUS} = Run) ->
            % interrupted lane run previously preparing in advance - change to ?FAILED_STATUS
            % as it will become the point of workflow execution failure at which it can be rerun
            {ok, Run#atm_lane_execution_run{run_no = CurrRunNo, status = ?FAILED_STATUS}};

        (#atm_lane_execution_run{run_no = undefined} = Run) ->
            % lane run already preparing in advance
            {ok, Run#atm_lane_execution_run{run_no = CurrRunNo}}
    end,
    % schedule new lane run
    Default = #atm_lane_execution_run{run_no = CurrRunNo, status = ?SCHEDULED_STATUS},

    NextLaneIndex = CurrLaneIndex + 1,
    {ok, NewAtmWorkflowExecution} = atm_lane_execution:update_curr_run(
        NextLaneIndex, Diff, Default, AtmWorkflowExecution
    ),
    {ok, NewAtmWorkflowExecution#atm_workflow_execution{curr_lane_index = NextLaneIndex}}.
