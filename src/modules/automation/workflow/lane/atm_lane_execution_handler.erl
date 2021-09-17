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
-export([
    prepare/3,
    handle_ended/3
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec prepare(pos_integer(), atm_workflow_execution:id(), atm_workflow_execution_ctx:record()) ->
    {workflow_engine:lane_spec() , atm_workflow_execution_env:record()} | no_return().
prepare(AtmLaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    AtmWorkflowExecutionDoc = atm_lane_execution_status:handle_preparing(
        AtmLaneIndex, AtmWorkflowExecutionId
    ),

    try
        {NewAtmWorkflowExecutionDoc, AtmWorkflowExecutionEnv} = atm_lane_execution_factory:create(
            AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
        ),
        AtmLaneExecutionSpec = setup_lane(
            AtmLaneIndex, NewAtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
        ),
        atm_lane_execution_status:handle_enqueued(AtmLaneIndex, AtmWorkflowExecutionId),

        {AtmLaneExecutionSpec, AtmWorkflowExecutionEnv}
    catch Type:Reason ->
        atm_lane_execution_status:handle_aborting(AtmLaneIndex, AtmWorkflowExecutionId, failure),
        erlang:Type(Reason)
    end.


-spec handle_ended(
    pos_integer(),
    atm_workflow_execution:id(),
    atm_workflow_execution_ctx:record()
) ->
    ok | no_return().
handle_ended(AtmLaneIndex, AtmWorkflowExecutionId, _AtmWorkflowExecutionCtx) ->
    {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),

    teardown_lane(AtmLaneIndex, AtmWorkflowExecutionDoc),


    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec setup_lane(pos_integer(), atm_workflow_execution:doc(), atm_workflow_execution_ctx:record()) ->
    workflow_engine:lane_spec() | no_return().
setup_lane(AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    try
        setup_lane_insecure(AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx)
    catch _:Reason ->
        #atm_lane_execution_rec{schema_id = AtmLaneSchemaId} = get_lane_execution(
            AtmLaneIndex, AtmWorkflowExecutionDoc
        ),
        throw(?ERROR_ATM_LANE_EXECUTION_PREPARATION_FAILED(AtmLaneSchemaId, Reason))  %% TODO preparation -> setup
    end.


%% @private
-spec setup_lane_insecure(
    pos_integer(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    workflow_engine:lane_spec() | no_return().
setup_lane_insecure(AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    #atm_lane_schema{store_iterator_spec = AtmStoreIteratorSpec} = get_lane_schema(
        AtmLaneIndex, AtmWorkflowExecutionDoc
    ),

    #atm_lane_execution_rec{runs = [
        _CurrRun = #atm_lane_execution_run{
            iterated_store_id = AtmIteratedStoreId,
            parallel_boxes = AtmParallelBoxExecutions
        }
        | _
    ]} = get_lane_execution(AtmLaneIndex, AtmWorkflowExecutionDoc),

    #{
        parallel_boxes => atm_parallel_box_execution:setup_all(
            AtmWorkflowExecutionCtx, AtmParallelBoxExecutions
        ),
        iterator => atm_store_api:acquire_iterator(AtmIteratedStoreId, AtmStoreIteratorSpec)
    }.


%% @private
-spec get_lane_execution(pos_integer(), atm_workflow_execution:doc()) ->
    atm_lane_execution:record2().
get_lane_execution(AtmLaneIndex, #document{value = #atm_workflow_execution{
    lanes = AtmLaneExecutions
}}) ->
    lists:nth(AtmLaneIndex, AtmLaneExecutions).


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
-spec teardown_lane(pos_integer(), atm_workflow_execution:doc()) -> ok.
teardown_lane(AtmLaneIndex, AtmWorkflowExecutionDoc) ->
    #atm_lane_execution_rec{runs = [#atm_lane_execution_run{
        iterated_store_id = AtmIteratedStoreId,
        parallel_boxes = AtmParallelBoxExecutions
    } | _]} = get_lane_execution(AtmLaneIndex, AtmWorkflowExecutionDoc),

    atm_store_api:unfreeze(AtmIteratedStoreId),
    atm_parallel_box_execution:teardown_all(AtmParallelBoxExecutions).
