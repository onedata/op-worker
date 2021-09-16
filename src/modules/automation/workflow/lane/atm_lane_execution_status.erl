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
-export([
    handle_preparing/2,
    handle_enqueued/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec handle_preparing(pos_integer(), atm_workflow_execution:id()) ->
    atm_workflow_execution:doc() | no_return().
handle_preparing(AtmLaneIndex, AtmWorkflowExecutionId) ->
    Diff = fun(#atm_workflow_execution{lanes = AtmLaneExecutions} = AtmWorkflowExecution) ->
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

    case atm_workflow_execution_status:handle_lane_preparing(
        AtmLaneIndex, AtmWorkflowExecutionId, Diff
    ) of
        {ok, AtmWorkflowExecutionDoc} ->
            AtmWorkflowExecutionDoc;
        {error, _} = Error ->
            throw(Error)
    end.


-spec handle_enqueued(pos_integer(), atm_workflow_execution:id()) ->
    atm_workflow_execution:doc() | no_return().
handle_enqueued(AtmLaneIndex, AtmWorkflowExecutionId) ->
    Diff = fun(#atm_workflow_execution{lanes = AtmLaneExecutions} = AtmWorkflowExecution) ->
        #atm_lane_execution_rec{runs = [CurrRun | RestRuns]} = AtmLaneExecution = lists:nth(
            AtmLaneIndex, AtmLaneExecutions
        ),
        case CurrRun of
            #atm_lane_execution_run{status = ?PREPARING_STATUS} ->
                NewAtmLaneExecution = AtmLaneExecution#atm_lane_execution_rec{
                    runs = [CurrRun#atm_lane_execution_run{status = ?ENQUEUED_STATUS} | RestRuns]
                },
                NewAtmLaneExecutions = lists_utils:replace_at(
                    NewAtmLaneExecution, AtmLaneIndex, AtmLaneExecutions
                ),
                {ok, AtmWorkflowExecution#atm_workflow_execution{lanes = NewAtmLaneExecutions}};
            #atm_lane_execution_run{status = Status} ->
                ?ERROR_ATM_INVALID_STATUS_TRANSITION(Status, ?ENQUEUED_STATUS)
        end
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, AtmWorkflowExecutionDoc} ->
            AtmWorkflowExecutionDoc;
        {error, _} = Error ->
            throw(Error)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


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
-spec status_to_phase(atm_lane_execution:status()) ->
    atm_workflow_execution:phase().
status_to_phase(?SCHEDULED_STATUS) -> ?WAITING_PHASE;
status_to_phase(?PREPARING_STATUS) -> ?WAITING_PHASE;
status_to_phase(?ENQUEUED_STATUS) -> ?WAITING_PHASE;
status_to_phase(?ACTIVE_STATUS) -> ?ONGOING_PHASE;
status_to_phase(?ABORTING_STATUS) -> ?ONGOING_PHASE;
status_to_phase(?FINISHED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?CANCELLED_STATUS) -> ?ENDED_PHASE;
status_to_phase(?FAILED_STATUS) -> ?ENDED_PHASE.
