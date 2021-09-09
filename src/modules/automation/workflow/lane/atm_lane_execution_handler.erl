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
-module(atm_lane_execution_handler).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([prepare/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec prepare(pos_integer(), atm_workflow_execution:id(), atm_workflow_execution_ctx:record()) ->
    ok.
prepare(AtmLaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    AtmWorkflowExecutionDoc = mark_as_preparing(AtmLaneIndex, AtmWorkflowExecutionId),

    try
        atm_lane_execution_factory:create(
            AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
        ),
        ok
    catch throw:{error, _} ->
        % TODO log error
        ok
    end,

    ok.


%%-spec prepare_all(atm_workflow_execution_ctx:record(), [record()]) -> ok | no_return().
%%prepare_all(AtmWorkflowExecutionCtx, AtmLaneExecutions) ->
%%    atm_parallel_runner:foreach(fun(#atm_lane_execution{schema_id = AtmLaneSchemaId} = AtmLaneExecution) ->
%%        try
%%            prepare(AtmWorkflowExecutionCtx, AtmLaneExecution)
%%        catch _:Reason ->
%%            throw(?ERROR_ATM_LANE_EXECUTION_PREPARATION_FAILED(AtmLaneSchemaId, Reason))
%%        end
%%    end, AtmLaneExecutions).
%%
%%
%%-spec prepare(atm_workflow_execution_ctx:record(), record()) -> ok | no_return().
%%prepare(AtmWorkflowExecutionCtx, #atm_lane_execution{parallel_boxes = AtmParallelBoxExecutions}) ->
%%    atm_parallel_box_execution:setup_all(AtmWorkflowExecutionCtx, AtmParallelBoxExecutions),
%%    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec mark_as_preparing(pos_integer(), atm_workflow_execution:id()) ->
    atm_workflow_execution:doc() | no_return().
mark_as_preparing(AtmLaneIndex, AtmWorkflowExecutionId) ->
    Diff = fun(#atm_workflow_execution{lanes = AtmLaneExecutions} = AtmWorkflowExecution) ->
        % TODO check that workflow in ongoing/is not aborting; maybe change workflow status to active?
        AtmLaneExecution = lists:nth(AtmLaneIndex, AtmLaneExecutions),

        case mark_current_run_as_preparing(AtmLaneExecution#atm_lane_execution_rec.runs) of
            {ok, UpdatedRuns} ->
                NewAtmLaneExecution = AtmLaneExecution#atm_lane_execution_rec{runs = UpdatedRuns},
                NewAtmLaneExecutions = lists_utils:replace_at(
                    NewAtmLaneExecution, AtmLaneIndex, AtmLaneExecutions
                ),
                {ok, AtmWorkflowExecution#atm_workflow_execution{lanes = NewAtmLaneExecutions}};
            {error, _} = Error ->
                Error
        end
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} ->
            AtmWorkflowExecutionDoc;
        ?ERROR_ATM_INVALID_STATUS_TRANSITION(_, _) = Error ->
            throw(Error)
    end.


%% @private
-spec mark_current_run_as_preparing([atm_lane_execution:run()]) ->
    {ok, [atm_lane_execution:run()]} | errors:error().
mark_current_run_as_preparing([]) ->
    % preparing in advance
    {ok, [#atm_lane_execution_run{run_no = undefined, status = ?PREPARING_STATUS}]};

mark_current_run_as_preparing([
    #atm_lane_execution_run{status = ?SCHEDULED_STATUS} = CurrentRun
    | RestRuns
]) ->
    {ok, [CurrentRun#atm_lane_execution_run{status = ?PREPARING_STATUS} | RestRuns]};

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
