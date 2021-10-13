%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles the lane execution process (for information about
%%% state machine consult 'atm_lane_execution_status.erl').
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution_handler).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("workflow_engine.hrl").

%% API
-export([
    prepare/3,
    handle_ended/3
]).


-type teardown_ctx() :: #atm_lane_execution_run_teardown_ctx{}.

-export_type([teardown_ctx/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec prepare(pos_integer(), atm_workflow_execution:id(), atm_workflow_execution_ctx:record()) ->
    workflow_engine:lane_spec() | no_return().
prepare(LaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    try
        AtmWorkflowExecutionDoc = atm_lane_execution_status:handle_preparing(
            LaneIndex, AtmWorkflowExecutionId
        ),

        NewAtmWorkflowExecutionDoc = atm_lane_execution_factory:create_run(
            LaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
        ),
        AtmLaneExecutionSpec = setup_lane_run(
            LaneIndex, NewAtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
        ),

        #document{value = AtmWorkflowExecution} = atm_lane_execution_status:handle_enqueued(
            LaneIndex, AtmWorkflowExecutionId
        ),
        freeze_curr_lane_run_iterated_store_if_ready_to_execute(AtmWorkflowExecution),

        AtmLaneExecutionSpec
    catch Type:Reason ->
        atm_lane_execution_status:handle_aborting(LaneIndex, AtmWorkflowExecutionId, failure),
        handle_ended(LaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),
        erlang:Type(Reason)
    end.


-spec handle_ended(
    pos_integer(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_handler:lane_ended_callback_result() | no_return().
handle_ended(LaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    NewAtmWorkflowExecution = #atm_workflow_execution{
        curr_lane_index = NextLaneIndex,
        lanes_count = AtmLanesCount
    } = end_lane_run(LaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),

    freeze_curr_lane_run_iterated_store_if_ready_to_execute(NewAtmWorkflowExecution),
    {ok, NextLaneRun} = atm_lane_execution:get_curr_run(NextLaneIndex, NewAtmWorkflowExecution),

    case atm_lane_execution_status:status_to_phase(NextLaneRun#atm_lane_execution_run.status) of
        ?ENDED_PHASE ->
            ?FINISH_EXECUTION;
        _ ->
            LaneToPrepareInAdvanceIndex = case NextLaneIndex < AtmLanesCount of
                true -> NextLaneIndex + 1;
                false -> undefined
            end,
            ?CONTINUE(NextLaneIndex, LaneToPrepareInAdvanceIndex)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec setup_lane_run(
    pos_integer(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_engine:lane_spec() | no_return().
setup_lane_run(LaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    AtmWorkflowExecution = AtmWorkflowExecutionDoc#document.value,
    AtmWorkflowExecutionEnv = atm_workflow_execution_ctx:get_env(AtmWorkflowExecutionCtx),

    try
        {ok, #atm_lane_execution_run{
            iterated_store_id = AtmIteratedStoreId,
            exception_store_id = ExceptionStoreId,
            parallel_boxes = AtmParallelBoxExecutions
        }} = atm_lane_execution:get_curr_run(LaneIndex, AtmWorkflowExecution),

        {AtmParallelBoxExecutionSpecs, AtmWorkflowExecutionEnvDiff} = atm_parallel_box_execution:setup_all(
            AtmWorkflowExecutionCtx, AtmParallelBoxExecutions
        ),
        {ok, #atm_store{container = AtmLaneRunExceptionStoreContainer}} = atm_store_api:get(ExceptionStoreId),
        NewAtmWorkflowExecutionEnv = atm_workflow_execution_env:set_lane_run_exception_store_container(
            AtmLaneRunExceptionStoreContainer, AtmWorkflowExecutionEnv
        ),
        #atm_lane_schema{store_iterator_spec = AtmStoreIteratorSpec} = atm_lane_execution:get_schema(
            LaneIndex, AtmWorkflowExecution
        ),

        #{
            execution_context => AtmWorkflowExecutionEnvDiff(NewAtmWorkflowExecutionEnv),
            parallel_boxes => AtmParallelBoxExecutionSpecs,
            iterator => atm_store_api:acquire_iterator(AtmIteratedStoreId, AtmStoreIteratorSpec)
        }
    catch _:Reason ->
        AtmLaneSchemaId = atm_lane_execution:get_schema_id(LaneIndex, AtmWorkflowExecution),
        throw(?ERROR_ATM_LANE_EXECUTION_SETUP_FAILED(AtmLaneSchemaId, Reason))
    end.


%% @private
-spec end_lane_run(
    pos_integer(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    atm_workflow_execution:record().
end_lane_run(LaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    {ok, #document{value = AtmWorkflowExecution = #atm_workflow_execution{
        curr_lane_index = PrevLaneIndex,
        curr_run_no = PrevRunNo
    }}} = atm_workflow_execution:get(AtmWorkflowExecutionId),

    {ok, PrevRun} = atm_lane_execution:get_curr_run(LaneIndex, AtmWorkflowExecution),

    unfreeze_iterated_store_in_case_of_workflow_store(PrevRun, AtmWorkflowExecutionCtx),
    freeze_exception_store(PrevRun),

    AtmParallelBoxExecutions = PrevRun#atm_lane_execution_run.parallel_boxes,
    atm_parallel_box_execution:ensure_all_ended(AtmParallelBoxExecutions),

    #document{value = NewAtmWorkflowExecution = #atm_workflow_execution{
        curr_lane_index = NextLaneIndex,
        curr_run_no = NextRunNo
    }} = atm_lane_execution_status:handle_ended(LaneIndex, AtmWorkflowExecutionId),

    AtmLaneExecutionRunTeardownCtx = #atm_lane_execution_run_teardown_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        is_retried = LaneIndex == PrevLaneIndex andalso
            NextLaneIndex == PrevLaneIndex andalso
            NextRunNo == PrevRunNo + 1
    },
    atm_parallel_box_execution:teardown_all(AtmLaneExecutionRunTeardownCtx, AtmParallelBoxExecutions),

    NewAtmWorkflowExecution.


%% @private
-spec unfreeze_iterated_store_in_case_of_workflow_store(
    atm_lane_execution:run(),
    atm_workflow_execution_ctx:record()
) ->
    ok.
unfreeze_iterated_store_in_case_of_workflow_store(
    #atm_lane_execution_run{exception_store_id = undefined},
    _AtmWorkflowExecutionCtx
) ->
    ok;
unfreeze_iterated_store_in_case_of_workflow_store(
    #atm_lane_execution_run{iterated_store_id = AtmIteratedStoreId},
    AtmWorkflowExecutionCtx
) ->
    case atm_workflow_execution_ctx:is_workflow_store(AtmIteratedStoreId, AtmWorkflowExecutionCtx) of
        true -> atm_store_api:unfreeze(AtmIteratedStoreId);
        false -> ok
    end.


%% @private
-spec freeze_exception_store(atm_lane_execution:run()) -> ok.
freeze_exception_store(#atm_lane_execution_run{exception_store_id = undefined}) ->
    ok;
freeze_exception_store(#atm_lane_execution_run{exception_store_id = AtmExceptionStoreId}) ->
    atm_store_api:freeze(AtmExceptionStoreId).


%% @private
-spec freeze_curr_lane_run_iterated_store_if_ready_to_execute(atm_workflow_execution:record()) ->
    ok.
freeze_curr_lane_run_iterated_store_if_ready_to_execute(AtmWorkflowExecution = #atm_workflow_execution{
    curr_lane_index = LaneIndex,
    curr_run_no = CurrRunNo
}) ->
    case atm_lane_execution:get_curr_run(LaneIndex, AtmWorkflowExecution) of
        {ok, #atm_lane_execution_run{status = ?ENQUEUED_STATUS, run_no = CurrRunNo} = Run} ->
            atm_store_api:freeze(Run#atm_lane_execution_run.iterated_store_id);
        _ ->
            % execution must have ended or lane run is still not ready
            % (either not fully prepared or previous run is still ongoing)
            ok
    end.
