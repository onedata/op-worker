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


-spec prepare(
    atm_lane_execution:index(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_engine:lane_spec() | no_return().
prepare(AtmLaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    try
        AtmWorkflowExecutionDoc = atm_lane_execution_status:handle_preparing(
            AtmLaneIndex, AtmWorkflowExecutionId
        ),

        NewAtmWorkflowExecutionDoc = atm_lane_execution_factory:create_run(
            AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
        ),
        AtmLaneExecutionSpec = initiate_lane_run(
            AtmLaneIndex, NewAtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
        ),

        #document{value = AtmWorkflowExecution} = atm_lane_execution_status:handle_enqueued(
            AtmLaneIndex, AtmWorkflowExecutionId
        ),
        freeze_current_lane_run_iterated_store_if_ready_to_execute(AtmWorkflowExecution),

        AtmLaneExecutionSpec
    catch Type:Reason:Stacktrace ->
        atm_lane_execution_status:handle_aborting(AtmLaneIndex, AtmWorkflowExecutionId, failure),
        handle_ended(AtmLaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),
        throw(?atm_examine_error(Type, Reason, Stacktrace))
    end.


-spec handle_ended(
    atm_lane_execution:index(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_handler:lane_ended_callback_result() | no_return().
handle_ended(AtmLaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    NewAtmWorkflowExecution = #atm_workflow_execution{
        current_lane_index = NextAtmLaneIndex,
        lanes_count = AtmLanesCount
    } = end_lane_run(AtmLaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),

    freeze_current_lane_run_iterated_store_if_ready_to_execute(NewAtmWorkflowExecution),
    {ok, NextLaneRun} = atm_lane_execution:get_current_run(NextAtmLaneIndex, NewAtmWorkflowExecution),

    case atm_lane_execution_status:status_to_phase(NextLaneRun#atm_lane_execution_run.status) of
        ?ENDED_PHASE ->
            ?FINISH_EXECUTION;
        _ ->
            AtmLaneToPrepareInAdvanceIndex = case NextAtmLaneIndex < AtmLanesCount of
                true -> NextAtmLaneIndex + 1;
                false -> undefined
            end,
            ?CONTINUE(NextAtmLaneIndex, AtmLaneToPrepareInAdvanceIndex)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec initiate_lane_run(
    atm_lane_execution:index(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_engine:lane_spec() | no_return().
initiate_lane_run(AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    AtmWorkflowExecution = AtmWorkflowExecutionDoc#document.value,
    AtmWorkflowExecutionEnv = atm_workflow_execution_ctx:get_env(AtmWorkflowExecutionCtx),

    try
        {ok, #atm_lane_execution_run{
            iterated_store_id = AtmIteratedStoreId,
            exception_store_id = ExceptionStoreId,
            parallel_boxes = AtmParallelBoxExecutions
        }} = atm_lane_execution:get_current_run(AtmLaneIndex, AtmWorkflowExecution),

        {AtmParallelBoxExecutionSpecs, AtmWorkflowExecutionEnvDiff} = atm_parallel_box_execution:initiate_all(
            AtmWorkflowExecutionCtx, AtmParallelBoxExecutions
        ),
        {ok, #atm_store{container = AtmLaneRunExceptionStoreContainer}} = atm_store_api:get(ExceptionStoreId),
        NewAtmWorkflowExecutionEnv = atm_workflow_execution_env:set_lane_run_exception_store_container(
            AtmLaneRunExceptionStoreContainer, AtmWorkflowExecutionEnv
        ),
        #atm_lane_schema{store_iterator_spec = AtmStoreIteratorSpec} = atm_lane_execution:get_schema(
            AtmLaneIndex, AtmWorkflowExecution
        ),

        #{
            execution_context => AtmWorkflowExecutionEnvDiff(NewAtmWorkflowExecutionEnv),
            parallel_boxes => AtmParallelBoxExecutionSpecs,
            iterator => atm_store_api:acquire_iterator(AtmIteratedStoreId, AtmStoreIteratorSpec)
        }
    catch Type:Reason:Stacktrace ->
        throw(?ERROR_ATM_LANE_EXECUTION_INITIATION_FAILED(
            atm_lane_execution:get_schema_id(AtmLaneIndex, AtmWorkflowExecution),
            ?atm_examine_error(Type, Reason, Stacktrace)
        ))
    end.


%% @private
-spec end_lane_run(
    atm_lane_execution:index(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    atm_workflow_execution:record().
end_lane_run(AtmLaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    {ok, #document{value = AtmWorkflowExecution = #atm_workflow_execution{
        current_lane_index = PrevAtmLaneIndex,
        current_run_num = PrevRunNum
    }}} = atm_workflow_execution:get(AtmWorkflowExecutionId),

    {ok, PrevRun} = atm_lane_execution:get_current_run(AtmLaneIndex, AtmWorkflowExecution),

    unfreeze_iterated_store_in_case_of_global_store(PrevRun, AtmWorkflowExecutionCtx),
    freeze_exception_store(PrevRun),

    AtmParallelBoxExecutions = PrevRun#atm_lane_execution_run.parallel_boxes,
    atm_parallel_box_execution:ensure_all_ended(AtmParallelBoxExecutions),

    #document{value = NewAtmWorkflowExecution = #atm_workflow_execution{
        current_lane_index = NextAtmLaneIndex,
        current_run_num = NextRunNum
    }} = atm_lane_execution_status:handle_ended(AtmLaneIndex, AtmWorkflowExecutionId),

    AtmLaneExecutionRunTeardownCtx = #atm_lane_execution_run_teardown_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        is_retried_scheduled = AtmLaneIndex == PrevAtmLaneIndex andalso
            NextAtmLaneIndex == PrevAtmLaneIndex andalso
            NextRunNum == PrevRunNum + 1
    },
    atm_parallel_box_execution:teardown_all(AtmLaneExecutionRunTeardownCtx, AtmParallelBoxExecutions),

    NewAtmWorkflowExecution.


%% @private
-spec unfreeze_iterated_store_in_case_of_global_store(
    atm_lane_execution:run(),
    atm_workflow_execution_ctx:record()
) ->
    ok.
unfreeze_iterated_store_in_case_of_global_store(
    #atm_lane_execution_run{exception_store_id = undefined},
    _AtmWorkflowExecutionCtx
) ->
    ok;
unfreeze_iterated_store_in_case_of_global_store(
    #atm_lane_execution_run{iterated_store_id = AtmIteratedStoreId},
    AtmWorkflowExecutionCtx
) ->
    case atm_workflow_execution_ctx:is_global_store(AtmIteratedStoreId, AtmWorkflowExecutionCtx) of
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
-spec freeze_current_lane_run_iterated_store_if_ready_to_execute(atm_workflow_execution:record()) ->
    ok.
freeze_current_lane_run_iterated_store_if_ready_to_execute(#atm_workflow_execution{
    current_lane_index = CurrentAtmLaneIndex,
    current_run_num = CurrentRunNum
} = AtmWorkflowExecution) ->
    case atm_lane_execution:get_current_run(CurrentAtmLaneIndex, AtmWorkflowExecution) of
        {ok, #atm_lane_execution_run{status = ?ENQUEUED_STATUS, run_num = CurrentRunNum} = Run} ->
            atm_store_api:freeze(Run#atm_lane_execution_run.iterated_store_id);
        _ ->
            % execution must have ended or lane run is still not ready
            % (either not fully prepared or previous run is still ongoing)
            ok
    end.
