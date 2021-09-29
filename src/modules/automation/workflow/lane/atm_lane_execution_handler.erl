%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles the lane execution process.
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


%%%===================================================================
%%% API
%%%===================================================================


-spec prepare(pos_integer(), atm_workflow_execution:id(), atm_workflow_execution_ctx:record()) ->
    workflow_engine:lane_spec() | no_return().
prepare(AtmLaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    AtmWorkflowExecutionDoc = atm_lane_execution_status:handle_preparing(
        AtmLaneIndex, AtmWorkflowExecutionId
    ),

    try
        NewAtmWorkflowExecutionDoc0 = atm_lane_execution_factory:create_run(
            AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
        ),
        AtmLaneExecutionSpec = setup_lane(
            AtmLaneIndex, NewAtmWorkflowExecutionDoc0, AtmWorkflowExecutionCtx
        ),

        NewAtmWorkflowExecutionDoc1 = atm_lane_execution_status:handle_enqueued(
            AtmLaneIndex, AtmWorkflowExecutionId
        ),
        freeze_iterated_store_if_ready_to_execute(AtmLaneIndex, NewAtmWorkflowExecutionDoc1),

        AtmLaneExecutionSpec
    catch Type:Reason ->
        atm_lane_execution_status:handle_aborting(AtmLaneIndex, AtmWorkflowExecutionId, failure),
        handle_ended(AtmLaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),
        erlang:Type(Reason)
    end.


-spec handle_ended(
    pos_integer(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_handler:lane_ended_callback_result() | no_return().
handle_ended(AtmLaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    teardown_lane(AtmLaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx),

    NewAtmWorkflowExecutionDoc = #document{value = #atm_workflow_execution{
        curr_lane_index = NextLaneIndex,
        lanes = AtmLaneExecutions,
        lanes_num = LanesNum
    }} = atm_lane_execution_status:handle_ended(AtmLaneIndex, AtmWorkflowExecutionId),

    freeze_iterated_store_if_ready_to_execute(NextLaneIndex, NewAtmWorkflowExecutionDoc),

    #atm_lane_execution{runs = [Run |_]} = lists:nth(NextLaneIndex, AtmLaneExecutions),
    case atm_lane_execution_status:status_to_phase(Run#atm_lane_execution_run.status) of
        ?ENDED_PHASE ->
            ?FINISH_EXECUTION;
        _ ->
            LaneToPrepareInAdvanceIndex = case NextLaneIndex < LanesNum of
                true -> NextLaneIndex + 1;
                false -> undefined
            end,
            ?CONTINUE(NextLaneIndex, LaneToPrepareInAdvanceIndex)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec setup_lane(
    pos_integer(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_engine:lane_spec() | no_return().
setup_lane(AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    AtmWorkflowExecutionEnv0 = atm_workflow_execution_ctx:get_env(AtmWorkflowExecutionCtx),

    #atm_lane_execution{
        schema_id = AtmLaneSchemaId,
        runs = [#atm_lane_execution_run{
            iterated_store_id = AtmIteratedStoreId,
            exception_store_id = ExceptionStoreId,
            parallel_boxes = AtmParallelBoxExecutions
        } | _]
    } = get_lane_execution(AtmLaneIndex, AtmWorkflowExecutionDoc),

    try
        {ok, #atm_store{container = ExceptionStoreContainer}} = atm_store_api:get(ExceptionStoreId),
        AtmWorkflowExecutionEnv1 = atm_workflow_execution_env:set_lane_run_exception_store_container(
            ExceptionStoreContainer, AtmWorkflowExecutionEnv0
        ),

        {AtmParallelBoxExecutionSpecs, AtmWorkflowExecutionEnvDiff} = atm_parallel_box_execution:setup_all(
            AtmWorkflowExecutionCtx, AtmParallelBoxExecutions
        ),
        #atm_lane_schema{store_iterator_spec = AtmStoreIteratorSpec} = get_lane_schema(
            AtmLaneIndex, AtmWorkflowExecutionDoc
        ),

        #{
            execution_context => AtmWorkflowExecutionEnvDiff(AtmWorkflowExecutionEnv1),
            parallel_boxes => AtmParallelBoxExecutionSpecs,
            iterator => atm_store_api:acquire_iterator(AtmIteratedStoreId, AtmStoreIteratorSpec)
        }
    catch _:Reason ->
        throw(?ERROR_ATM_LANE_EXECUTION_SETUP_FAILED(AtmLaneSchemaId, Reason))
    end.


%% @private
-spec teardown_lane(
    pos_integer(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    ok.
teardown_lane(AtmLaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),

    AtmLaneExecution = #atm_lane_execution{runs = [#atm_lane_execution_run{
        parallel_boxes = AtmParallelBoxExecutions
    } | _]} = get_lane_execution(AtmLaneIndex, AtmWorkflowExecutionDoc),

    unfreeze_iterated_store_in_case_of_workflow_store(AtmLaneExecution, AtmWorkflowExecutionCtx),
    freeze_exception_store(AtmLaneExecution),

    atm_parallel_box_execution:ensure_all_ended(AtmParallelBoxExecutions),
    atm_parallel_box_execution:teardown_all(AtmParallelBoxExecutions).


%% @private
-spec unfreeze_iterated_store_in_case_of_workflow_store(
    atm_lane_execution:record(),
    atm_workflow_execution_ctx:record()
) ->
    ok.
unfreeze_iterated_store_in_case_of_workflow_store(
    #atm_lane_execution{runs = [#atm_lane_execution_run{exception_store_id = undefined} | _]},
    _AtmWorkflowExecutionCtx
) ->
    ok;
unfreeze_iterated_store_in_case_of_workflow_store(
    #atm_lane_execution{runs = [#atm_lane_execution_run{iterated_store_id = AtmIteratedStoreId} | _]},
    AtmWorkflowExecutionCtx
) ->
    case atm_workflow_execution_ctx:is_workflow_store(AtmIteratedStoreId, AtmWorkflowExecutionCtx) of
        true -> atm_store_api:unfreeze(AtmIteratedStoreId);
        false -> ok
    end.


%% @private
-spec freeze_exception_store(atm_lane_execution:record()) -> ok.
freeze_exception_store(#atm_lane_execution{runs = [
    #atm_lane_execution_run{exception_store_id = undefined} | _
]}) ->
    ok;
freeze_exception_store(#atm_lane_execution{runs = [
    #atm_lane_execution_run{exception_store_id = AtmExceptionStoreId} | _
]}) ->
    atm_store_api:freeze(AtmExceptionStoreId).


%% @private
-spec freeze_iterated_store_if_ready_to_execute(
    pos_integer(),
    atm_workflow_execution:doc()
) ->
    ok.
freeze_iterated_store_if_ready_to_execute(AtmLaneIndex, #document{
    value = #atm_workflow_execution{
        curr_run_no = CurrRunNo,
        lanes = AtmLaneExecutions
    }
}) ->
    #atm_lane_execution{runs = [Run |_]} = lists:nth(AtmLaneIndex, AtmLaneExecutions),

    case Run of
        #atm_lane_execution_run{status = ?ENQUEUED_STATUS, run_no = CurrRunNo} ->
            atm_store_api:freeze(Run#atm_lane_execution_run.iterated_store_id);
        _ ->
            % execution must have ended or lane run is still not ready
            % (either not fully prepared or previous run is still ongoing)
            ok
    end.


%% @private
-spec get_lane_schema(non_neg_integer(), atm_workflow_execution:doc()) ->
    atm_lane_schema:record().
get_lane_schema(AtmLaneIndex, #document{value = #atm_workflow_execution{
    schema_snapshot_id = AtmWorkflowSchemaSnapshotId
}}) ->
    {ok, #document{value = #atm_workflow_schema_snapshot{
        lanes = AtmLaneSchemas
    }}} = atm_workflow_schema_snapshot:get(AtmWorkflowSchemaSnapshotId),

    lists:nth(AtmLaneIndex, AtmLaneSchemas).


%% @private
-spec get_lane_execution(pos_integer(), atm_workflow_execution:doc()) ->
    atm_lane_execution:record().
get_lane_execution(AtmLaneIndex, #document{value = #atm_workflow_execution{
    lanes = AtmLaneExecutions
}}) ->
    lists:nth(AtmLaneIndex, AtmLaneExecutions).
