%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME.
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


-define(extract_atm_workflow_execution_doc(__CALL),
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
status_to_phase(?SKIPPED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?CANCELLED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?FAILED_STATUS) -> ?ENDED_PHASE.


-spec handle_preparing(pos_integer(), atm_workflow_execution:id()) ->
    atm_workflow_execution:doc() | no_return().
handle_preparing(AtmLaneIndex, AtmWorkflowExecutionId) ->
    Diff = fun(AtmWorkflowExecution) ->
        update_runs_at_lane(AtmLaneIndex, fun mark_current_run_as_preparing/1, AtmWorkflowExecution)
    end,
    ?extract_atm_workflow_execution_doc(atm_workflow_execution_status:handle_lane_preparing(
        AtmLaneIndex, AtmWorkflowExecutionId, Diff
    )).


-spec handle_enqueued(pos_integer(), atm_workflow_execution:id()) ->
    atm_workflow_execution:doc() | no_return().
handle_enqueued(AtmLaneIndex, AtmWorkflowExecutionId) ->
    Diff = fun(AtmWorkflowExecution) ->
        update_runs_at_lane(AtmLaneIndex, fun mark_current_run_as_enqueued/1, AtmWorkflowExecution)
    end,
    ?extract_atm_workflow_execution_doc(atm_workflow_execution_status:handle_lane_enqueued(
        AtmWorkflowExecutionId, Diff
    )).


-spec handle_aborting(undefined | pos_integer(), atm_workflow_execution:id(), cancel | failure) ->
    ok | errors:error().
handle_aborting(AtmLaneIndex, AtmWorkflowExecutionId, Reason) ->
    Diff = fun(AtmWorkflowExecution) -> update_runs_at_lane(
        AtmLaneIndex, fun mark_current_run_as_aborting/2, [Reason], AtmWorkflowExecution
    ) end,
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
    Diff = fun(AtmWorkflowExecution) ->
        update_runs_at_lane(
            AtmLaneIndex,
            fun handle_task_status_change_in_current_run/4,
            [AtmParallelBoxIndex, AtmTaskExecutionId, NewAtmTaskExecutionStatus],
            AtmWorkflowExecution
        )
    end,
    atm_workflow_execution_status:handle_lane_task_status_change(AtmWorkflowExecutionId, Diff).


-spec handle_ended(pos_integer(), atm_workflow_execution:id()) ->
    atm_workflow_execution:doc() | no_return().
handle_ended(AtmLaneIndex, AtmWorkflowExecutionId) ->
    Diff = fun(AtmWorkflowExecution = #atm_workflow_execution{curr_lane_index = CurrLaneIndex}) ->
        case CurrLaneIndex == AtmLaneIndex of
            true ->
                end_currently_executed_lane_run(AtmWorkflowExecution);
            false ->
                update_runs_at_lane(
                    AtmLaneIndex,
                    fun mark_lane_run_prepared_in_advance_as_interrupted/1,
                    AtmWorkflowExecution
                )
        end
    end,

    ?extract_atm_workflow_execution_doc(atm_workflow_execution:update(AtmWorkflowExecutionId, Diff)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec update_runs_at_lane(undefined | pos_integer(), fun(), atm_workflow_execution:record()) ->
    {ok, atm_workflow_execution:record()} | errors:error().
update_runs_at_lane(AtmLaneIndex, UpdateFun, AtmWorkflowExecution) ->
    update_runs_at_lane(AtmLaneIndex, UpdateFun, [], AtmWorkflowExecution).


%% @private
-spec update_runs_at_lane(
    undefined | pos_integer(),
    fun(),
    [term()],
    atm_workflow_execution:record()
) ->
    {ok, atm_workflow_execution:record()} | errors:error().
update_runs_at_lane(SpecificLaneIndex, UpdateFun, UpdateArgs, #atm_workflow_execution{
    lanes = AtmLaneExecutions,
    curr_lane_index = CurrLaneIndex
} = AtmWorkflowExecution) ->
    AtmLaneIndex = case SpecificLaneIndex of
        undefined -> CurrLaneIndex;
        _ -> SpecificLaneIndex
    end,
    AtmLaneExecution = lists:nth(AtmLaneIndex, AtmLaneExecutions),

    case erlang:apply(UpdateFun, UpdateArgs ++ [AtmLaneExecution#atm_lane_execution.runs]) of
        {ok, UpdatedRuns} ->
            NewAtmLaneExecution = AtmLaneExecution#atm_lane_execution{runs = UpdatedRuns},
            NewAtmLaneExecutions = lists_utils:replace_at(
                NewAtmLaneExecution, AtmLaneIndex, AtmLaneExecutions
            ),
            {ok, AtmWorkflowExecution#atm_workflow_execution{lanes = NewAtmLaneExecutions}};
        {error, _} = Error ->
            Error
    end.


%% @private
-spec mark_current_run_as_preparing([atm_lane_execution:run()]) ->
    {ok, [atm_lane_execution:run()]} | errors:error().
mark_current_run_as_preparing([]) ->
    % preparing in advance
    {ok, [#atm_lane_execution_run{run_no = undefined, status = ?PREPARING_STATUS}]};

mark_current_run_as_preparing([
    #atm_lane_execution_run{status = ?SCHEDULED_STATUS} = CurrRun
    | RestRuns
]) ->
    {ok, [CurrRun#atm_lane_execution_run{status = ?PREPARING_STATUS} | RestRuns]};

mark_current_run_as_preparing([#atm_lane_execution_run{status = Status} | _] = PreviousRuns) ->
    case status_to_phase(Status) of
        ?ENDED_PHASE ->
            % preparing in advance
            NewRun = #atm_lane_execution_run{run_no = undefined, status = ?PREPARING_STATUS},
            {ok, [NewRun | PreviousRuns]};
        _ ->
            ?ERROR_ATM_INVALID_STATUS_TRANSITION(Status, ?PREPARING_STATUS)
    end.


%% @private
-spec mark_current_run_as_enqueued([atm_lane_execution:run()]) ->
    {ok, [atm_lane_execution:run()]} | errors:error().
mark_current_run_as_enqueued([
    #atm_lane_execution_run{status = ?PREPARING_STATUS} = CurrRun
    | RestRuns
]) ->
    {ok, [CurrRun#atm_lane_execution_run{status = ?ENQUEUED_STATUS} | RestRuns]};

mark_current_run_as_enqueued([#atm_lane_execution_run{status = Status} | _]) ->
    ?ERROR_ATM_INVALID_STATUS_TRANSITION(Status, ?PREPARING_STATUS).


%% @private
-spec mark_current_run_as_aborting(cancel | failure, [atm_lane_execution:run()]) ->
    {ok, [atm_lane_execution:run()]} | errors:error().
mark_current_run_as_aborting(CurrAbortingReason, [
    #atm_lane_execution_run{status = ?ABORTING_STATUS, aborting_reason = PrevAbortingReason} = Run
    | RestRuns
]) ->
    NewAbortingReason = case {PrevAbortingReason, CurrAbortingReason} of
        {failure, cancel} -> cancel;
        _ -> PrevAbortingReason
    end,
    {ok, [Run#atm_lane_execution_run{aborting_reason = NewAbortingReason} | RestRuns]};

mark_current_run_as_aborting(
    AbortingReason,
    [#atm_lane_execution_run{status = Status} = Run | RestRuns]
) ->
    case status_to_phase(Status) of
        ?ENDED_PHASE ->
            ?ERROR_ATM_INVALID_STATUS_TRANSITION(Status, ?ABORTING_STATUS);
        _ ->
            NewRun = Run#atm_lane_execution_run{
                status = ?ABORTING_STATUS,
                aborting_reason = AbortingReason
            },
            {ok, [NewRun | RestRuns]}
    end.


%% @private
-spec mark_lane_run_prepared_in_advance_as_interrupted([atm_lane_execution:run()]) ->
    {ok, [atm_lane_execution:run()]}.
mark_lane_run_prepared_in_advance_as_interrupted([
    #atm_lane_execution_run{run_no = undefined} = CurrRun
    | RestRuns
]) ->
    {ok, [CurrRun#atm_lane_execution_run{status = ?INTERRUPTED_STATUS} | RestRuns]}.


%% @private
-spec handle_task_status_change_in_current_run(
    pos_integer(),
    atm_task_execution:id(),
    atm_task_execution:status(),
    [atm_lane_execution:run()]
) ->
    {ok, [atm_lane_execution:run()]} | errors:error().
handle_task_status_change_in_current_run(
    AtmParallelBoxIndex,
    AtmTaskExecutionId,
    NewAtmTaskExecutionStatus,
    [#atm_lane_execution_run{parallel_boxes = AtmParallelBoxExecutions} = Run | RestRuns]
) ->
    HasTaskStarted = lists:member(NewAtmTaskExecutionStatus, [?ACTIVE_STATUS, ?SKIPPED_STATUS]),
    AtmParallelBoxExecution = lists:nth(AtmParallelBoxIndex, AtmParallelBoxExecutions),

    case atm_parallel_box_execution:update_task_status(
        AtmTaskExecutionId, NewAtmTaskExecutionStatus, AtmParallelBoxExecution
    ) of
        {ok, NewParallelBoxExecution} ->
            UpdatedRun = Run#atm_lane_execution_run{
                status = case {Run#atm_lane_execution_run.status, HasTaskStarted} of
                    {?ENQUEUED_STATUS, true} ->
                        % lane transition to ?ACTIVE_STATUS when first task has started
                        ?ACTIVE_STATUS;
                    {CurrentStatus, _} ->
                        CurrentStatus
                end,
                parallel_boxes = lists_utils:replace_at(
                    NewParallelBoxExecution, AtmParallelBoxIndex, AtmParallelBoxExecutions
                )
            },
            {ok, [UpdatedRun | RestRuns]};
        {error, _} = Error ->
            Error
    end.


%% @private
-spec end_currently_executed_lane_run(atm_workflow_execution:record()) ->
    {ok, atm_workflow_execution:record()}.
end_currently_executed_lane_run(AtmWorkflowExecution = #atm_workflow_execution{
    lanes = AtmLaneExecutions,
    lanes_num = AtmLanesNum,
    curr_lane_index = CurrAtmLaneIndex,
    curr_run_no = CurrRunNo
}) ->
    NextAtmLaneIndex = CurrAtmLaneIndex + 1,
    IsNotLastLane = CurrAtmLaneIndex < AtmLanesNum,
    {EndedStatus, NewAtmLaneExecutions} = end_lane_run(CurrAtmLaneIndex, AtmLaneExecutions),

    case {EndedStatus, IsNotLastLane} of
        {?FINISHED_STATUS, true} ->
            {ok, AtmWorkflowExecution#atm_workflow_execution{
                lanes = schedule_lane_run(NextAtmLaneIndex, CurrRunNo, NewAtmLaneExecutions),
                curr_lane_index = NextAtmLaneIndex
            }};
        {_, true} ->
            % current lane execution run hasn't ended successfully meaning entire workflow
            % execution will abort - interrupt next lane run preparation if any
            {ok, AtmWorkflowExecution#atm_workflow_execution{
                lanes = interrupt_lane_run_prepared_in_advance(
                    NextAtmLaneIndex, CurrRunNo, NewAtmLaneExecutions
                )
            }};
        {_, false} ->
            {ok, AtmWorkflowExecution#atm_workflow_execution{lanes = NewAtmLaneExecutions}}
    end.


%% @private
-spec end_lane_run(pos_integer(), [atm_lane_execution:record()]) ->
    {atm_lane_execution:status(), [atm_lane_execution:record()]}.
end_lane_run(AtmLaneIndex, AtmLaneExecutions) ->
    AtmLaneExecution = #atm_lane_execution{runs = [Run = #atm_lane_execution_run{
        status = CurrStatus,
        parallel_boxes = AtmParallelBoxExecutions
    } | RestRuns]} = lists:nth(AtmLaneIndex, AtmLaneExecutions),

    EndedStatus = case atm_lane_execution_status:status_to_phase(CurrStatus) of
        ?WAITING_PHASE ->
            % Lane preparation must have failed or provider was restarted
            % as otherwise it is not possible to transition from waiting phase
            % to ended phase directly
            ?FAILED_STATUS;
        ?ONGOING_PHASE when CurrStatus == ?ACTIVE_STATUS ->
            AtmParallelBoxExecutionStatuses = atm_parallel_box_execution:gather_statuses(
                AtmParallelBoxExecutions
            ),
            case lists:member(?FAILED_STATUS, AtmParallelBoxExecutionStatuses) of
                true -> ?FAILED_STATUS;
                false -> ?FINISHED_STATUS
            end;
        ?ONGOING_PHASE when CurrStatus == ?ABORTING_STATUS ->
            case Run#atm_lane_execution_run.aborting_reason of
                cancel -> ?CANCELLED_STATUS;
                failure -> ?FAILED_STATUS
            end;
        ?ENDED_PHASE ->
            CurrStatus
    end,
    EndedAtmLaneExecution = AtmLaneExecution#atm_lane_execution{
        runs = [Run#atm_lane_execution_run{status = EndedStatus} | RestRuns]
    },

    {EndedStatus, lists_utils:replace_at(EndedAtmLaneExecution, AtmLaneIndex, AtmLaneExecutions)}.


%% @private
-spec schedule_lane_run(pos_integer(), pos_integer(), [atm_lane_execution:record()]) ->
    [atm_lane_execution:record()].
schedule_lane_run(AtmLaneIndex, CurrRunNo, AtmLaneExecutions) ->
    AtmLaneExecution = lists:nth(AtmLaneIndex, AtmLaneExecutions),

    NewRuns = case AtmLaneExecution#atm_lane_execution.runs of
        [#atm_lane_execution_run{run_no = undefined, status = ?INTERRUPTED_STATUS} = Run | RestRuns] ->
            % interrupted lane preparing in advance - change to ?FAILED_STATUS as it will
            % become the point of workflow execution failure at which it can be rerun
            [Run#atm_lane_execution_run{run_no = CurrRunNo, status = ?FAILED_STATUS} | RestRuns];
        [#atm_lane_execution_run{run_no = undefined} = Run | RestRuns] ->
            % lane preparing in advance
            [Run#atm_lane_execution_run{run_no = CurrRunNo} | RestRuns];
        Runs ->
            [#atm_lane_execution_run{run_no = CurrRunNo, status = ?SCHEDULED_STATUS} | Runs]
    end,
    NewAtmLaneExecution = AtmLaneExecution#atm_lane_execution{runs = NewRuns},

    lists_utils:replace_at(NewAtmLaneExecution, AtmLaneIndex, AtmLaneExecutions).


%% @private
-spec interrupt_lane_run_prepared_in_advance(
    pos_integer(),
    pos_integer(),
    [atm_lane_execution:record()]
) ->
    [atm_lane_execution:record()].
interrupt_lane_run_prepared_in_advance(AtmLaneIndex, CurrRunNo, AtmLaneExecutions) ->
    AtmLaneExecution = lists:nth(AtmLaneIndex, AtmLaneExecutions),

    case AtmLaneExecution#atm_lane_execution.runs of
        [#atm_lane_execution_run{run_no = undefined} = Run | RestRuns] ->
            NewAtmLaneExecution = AtmLaneExecution#atm_lane_execution{runs = [
                Run#atm_lane_execution_run{run_no = CurrRunNo, status = ?INTERRUPTED_STATUS}
                | RestRuns
            ]},
            lists_utils:replace_at(NewAtmLaneExecution, AtmLaneIndex, AtmLaneExecutions);
        _ ->
            AtmLaneExecutions
    end.
