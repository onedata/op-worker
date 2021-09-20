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
    workflow_engine:lane_spec() | no_return().
prepare(AtmLaneIndex, AtmWorkflowExecutionId, AtmWorkflowExecutionCtx) ->
    AtmWorkflowExecutionDoc = atm_lane_execution_status:handle_preparing(
        AtmLaneIndex, AtmWorkflowExecutionId
    ),

    try
        % TODO add env to lane spec
        % TODO add next lane index to spec
        {NewAtmWorkflowExecutionDoc, _AtmWorkflowExecutionEnv} = atm_lane_execution_factory:create(
            AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
        ),
        AtmLaneExecutionSpec = setup_lane(
            AtmLaneIndex, NewAtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
        ),
        atm_lane_execution_status:handle_enqueued(AtmLaneIndex, AtmWorkflowExecutionId),

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
    ok | no_return().
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
    %% TODO freeze next lane iterated store ??

    %% TODO return is_last
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
        #atm_lane_execution{schema_id = AtmLaneSchemaId} = get_lane_execution(
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
    #atm_lane_execution{runs = [#atm_lane_execution_run{
        iterated_store_id = AtmIteratedStoreId,
        parallel_boxes = AtmParallelBoxExecutions
    } | _]} = get_lane_execution(AtmLaneIndex, AtmWorkflowExecutionDoc),

    #{
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
