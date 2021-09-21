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
        {NewAtmWorkflowExecutionDoc0, AtmWorkflowExecutionEnv} = atm_lane_execution_factory:create(
            AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
        ),
        AtmLaneExecutionSpec = setup_lane(
            AtmLaneIndex, NewAtmWorkflowExecutionDoc0,
            AtmWorkflowExecutionEnv, AtmWorkflowExecutionCtx
        ),

        NewAtmWorkflowExecutionDoc1 = atm_lane_execution_status:handle_enqueued(
            AtmLaneIndex, AtmWorkflowExecutionId
        ),
        freeze_lane_iterated_store_if_ready_to_execute(AtmLaneIndex, NewAtmWorkflowExecutionDoc1),

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
    stop | {ok, pos_integer()} | no_return().
handle_ended(AtmLaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),

    AtmLaneExecution = #atm_lane_execution{runs = [#atm_lane_execution_run{
        parallel_boxes = AtmParallelBoxExecutions
    } | _]} = get_lane_execution(AtmLaneIndex, AtmWorkflowExecutionDoc),

    unfreeze_iterated_store_in_case_of_workflow_store(AtmLaneExecution, AtmWorkflowExecutionCtx),
    freeze_exception_store(AtmLaneExecution),

    atm_parallel_box_execution:ensure_all_ended(AtmParallelBoxExecutions),
    atm_parallel_box_execution:teardown_all(AtmParallelBoxExecutions),

    NewAtmWorkflowExecutionDoc = atm_lane_execution_status:handle_ended(
        AtmLaneIndex, AtmWorkflowExecutionId
    ),
    freeze_curr_lane_iterated_store_if_ready_to_execute(NewAtmWorkflowExecutionDoc),
    get_curr_lane_to_execute(NewAtmWorkflowExecutionDoc).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec setup_lane(
    pos_integer(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_env:record(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_engine:lane_spec() | no_return().
setup_lane(AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionEnv, AtmWorkflowExecutionCtx) ->
    try
        setup_lane_insecure(
            AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionEnv, AtmWorkflowExecutionCtx
        )
    catch _:Reason ->
        #atm_lane_execution{schema_id = AtmLaneSchemaId} = get_lane_execution(
            AtmLaneIndex, AtmWorkflowExecutionDoc
        ),
        throw(?ERROR_ATM_LANE_EXECUTION_PREPARATION_FAILED(AtmLaneSchemaId, Reason))  %% TODO preparation -> setup
    end.


%% @private
-spec setup_lane_insecure(
    pos_integer(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_env:record(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_engine:lane_spec() | no_return().
setup_lane_insecure(
    AtmLaneIndex,
    AtmWorkflowExecutionDoc,
    AtmWorkflowExecutionEnv,
    AtmWorkflowExecutionCtx
) ->
    #atm_lane_schema{store_iterator_spec = AtmStoreIteratorSpec} = get_lane_schema(
        AtmLaneIndex, AtmWorkflowExecutionDoc
    ),
    #atm_lane_execution{runs = [#atm_lane_execution_run{
        iterated_store_id = AtmIteratedStoreId,
        parallel_boxes = AtmParallelBoxExecutions
    } | _]} = get_lane_execution(AtmLaneIndex, AtmWorkflowExecutionDoc),

    % TODO add next lane index to spec? one or more?
    #{
%%        execution_context => AtmWorkflowExecutionEnv,
        parallel_boxes => atm_parallel_box_execution:setup_all(
            AtmWorkflowExecutionCtx, AtmParallelBoxExecutions
        ),
        iterator => atm_store_api:acquire_iterator(AtmIteratedStoreId, AtmStoreIteratorSpec)
    }.


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
-spec freeze_curr_lane_iterated_store_if_ready_to_execute(atm_workflow_execution:doc()) ->
    ok.
freeze_curr_lane_iterated_store_if_ready_to_execute(AtmWorkflowExecutionDoc = #document{
    value = #atm_workflow_execution{curr_lane_index = CurrLaneIndex}
}) ->
    freeze_lane_iterated_store_if_ready_to_execute(CurrLaneIndex, AtmWorkflowExecutionDoc).


%% @private
-spec freeze_lane_iterated_store_if_ready_to_execute(pos_integer(), atm_workflow_execution:doc()) ->
    ok.
freeze_lane_iterated_store_if_ready_to_execute(AtmLaneIndex, #document{value = #atm_workflow_execution{
    curr_run_no = CurrRunNo,
    lanes = AtmLaneExecutions
}}) ->
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
-spec get_curr_lane_to_execute(atm_workflow_execution:doc()) ->
    stop | {ok, pos_integer()}.
get_curr_lane_to_execute(#document{value = #atm_workflow_execution{
    curr_lane_index = CurrLaneIndex,
    lanes = AtmLaneExecutions
}}) ->
    #atm_lane_execution{runs = [Run |_]} = lists:nth(CurrLaneIndex, AtmLaneExecutions),

    case atm_lane_execution_status:status_to_phase(Run#atm_lane_execution_run.status) of
        ?ENDED_PHASE ->
            stop;
        _ ->
            {ok, CurrLaneIndex}
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
