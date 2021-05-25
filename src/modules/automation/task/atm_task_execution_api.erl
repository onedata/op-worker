%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for performing operations on automation task execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_api).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([
    create_all/4, create/4,
    prepare_all/1, prepare/1,
    delete_all/1, delete/1,
    run/2
]).


-type task_id() :: binary().

-export_type([task_id/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_all(
    atm_api:creation_ctx(),
    non_neg_integer(),
    non_neg_integer(),
    [atm_task_schema:record()]
) ->
    [atm_task_execution:doc()] | no_return().
create_all(AtmExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, AtmTaskSchemas) ->
    lists:reverse(lists:foldl(fun(#atm_task_schema{id = AtmTaskSchemaId} = AtmTaskSchema, Acc) ->
        try
            {ok, AtmTaskExecutionDoc} = create(
                AtmExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, AtmTaskSchema
            ),
            [AtmTaskExecutionDoc | Acc]
        catch _:Reason ->
            catch delete_all([Doc#document.key || Doc <- Acc]),
            throw(?ERROR_ATM_TASK_EXECUTION_CREATION_FAILED(AtmTaskSchemaId, Reason))
        end
    end, [], AtmTaskSchemas)).


-spec create(
    atm_api:creation_ctx(),
    non_neg_integer(),
    non_neg_integer(),
    atm_task_schema:record()
) ->
    {ok, atm_task_execution:doc()} | no_return().
create(AtmExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, #atm_task_schema{
    id = AtmTaskSchemaId,
    lambda_id = AtmLambdaId,
    argument_mappings = AtmTaskArgMappers
}) ->
    #atm_execution_creation_ctx{
        workflow_execution_id = AtmWorkflowExecutionId
    } = AtmExecutionCreationCtx,

    %% TODO VFS-7671 use lambda snapshots stored in atm_execution:creation_ctx()
    {ok, #document{value = #od_atm_lambda{
        operation_spec = AtmLambdaOperationSpec,
        argument_specs = AtmLambdaArgSpecs
    }}} = atm_lambda_logic:get(?ROOT_SESS_ID, AtmLambdaId),

    {ok, _} = atm_task_execution:create(#atm_task_execution{
        workflow_execution_id = AtmWorkflowExecutionId,
        lane_index = AtmLaneIndex,
        parallel_box_index = AtmParallelBoxIndex,

        schema_id = AtmTaskSchemaId,

        executor = atm_task_executor:create(AtmWorkflowExecutionId, AtmLambdaOperationSpec),
        argument_specs = atm_task_execution_args:build_specs(AtmLambdaArgSpecs, AtmTaskArgMappers),

        status = ?PENDING_STATUS,

        items_in_processing = 0,
        items_processed = 0,
        items_failed = 0
    }).


-spec prepare_all([atm_task_execution:id()]) -> ok | no_return().
prepare_all(AtmTaskExecutionIds) ->
    lists:foreach(fun(AtmTaskExecutionId) ->
        {ok, #document{value = AtmTaskExecutionRecord = #atm_task_execution{
            schema_id = AtmTaskSchemaId
        }}} = atm_task_execution:get(AtmTaskExecutionId),

        try
            prepare(AtmTaskExecutionRecord)
        catch _:Reason ->
            throw(?ERROR_ATM_TASK_EXECUTION_PREPARATION_FAILED(AtmTaskSchemaId, Reason))
        end
    end, AtmTaskExecutionIds).


-spec prepare(atm_task_execution:id() | atm_task_execution:record()) -> ok | no_return().
prepare(AtmTaskExecutionIdOrRecord) ->
    #atm_task_execution{executor = AtmTaskExecutor} = ensure_atm_task_execution_record(
        AtmTaskExecutionIdOrRecord
    ),
    atm_task_executor:prepare(AtmTaskExecutor).


-spec delete_all([atm_task_execution:id()]) -> ok.
delete_all(AtmTaskExecutionIds) ->
    lists:foreach(fun delete/1, AtmTaskExecutionIds).


-spec delete(atm_task_execution:id()) -> ok | {error, term()}.
delete(AtmTaskExecutionId) ->
    atm_task_execution:delete(AtmTaskExecutionId).


-spec run(atm_task_execution:id(), json_utils:json_term()) ->
    {ok, task_id()} | no_return().
run(AtmTaskExecutionId, Item) ->
    #atm_task_execution{
        executor = AtmTaskExecutor,
        argument_specs = AtmTaskArgSpecs
    } = update_items_in_processing(AtmTaskExecutionId),

    AtmTaskExecutionCtx = #atm_task_execution_ctx{item = Item},
    Args = atm_task_execution_args:build_args(AtmTaskExecutionCtx, AtmTaskArgSpecs),

    atm_task_executor:run(Args, AtmTaskExecutor).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec update_items_in_processing(atm_task_execution:id()) ->
    atm_task_execution:record().
update_items_in_processing(AtmTaskExecutionId) ->
    {ok, #document{value = AtmTaskExecutionRecord}} = atm_task_execution:update(
        AtmTaskExecutionId, fun
            (#atm_task_execution{
                status = ?PENDING_STATUS,
                status_changed = false,
                items_in_processing = 0
            } = AtmTaskExecution) ->
                {ok, AtmTaskExecution#atm_task_execution{
                    status = ?ACTIVE_STATUS,
                    status_changed = true,
                    items_in_processing = 1
                }};
            (#atm_task_execution{items_in_processing = ItemsInProcessing} = AtmTaskExecution) ->
                {ok, AtmTaskExecution#atm_task_execution{
                    status_changed = false,
                    items_in_processing = ItemsInProcessing + 1
                }}
        end
    ),
    case AtmTaskExecutionRecord of
        #atm_task_execution{status_changed = true} ->
            report_status_change(AtmTaskExecutionId, AtmTaskExecutionRecord);
        _ ->
            ok
    end,
    AtmTaskExecutionRecord.


%% @private
-spec report_status_change(atm_task_execution:id(), atm_task_execution:record()) -> ok.
report_status_change(AtmTaskExecutionId, #atm_task_execution{
    workflow_execution_id = AtmWorkflowExecutionId,
    lane_index = AtmLaneIndex,
    parallel_box_index = AtmParallelBoxIndex,
    status = NewStatus
}) ->
    atm_workflow_execution_api:report_task_status_change(
        AtmWorkflowExecutionId, AtmLaneIndex, AtmParallelBoxIndex,
        AtmTaskExecutionId, NewStatus
    ).


%% @private
-spec ensure_atm_task_execution_record(atm_task_execution:id() | atm_task_execution:record()) ->
    atm_task_execution:record().
ensure_atm_task_execution_record(#atm_task_execution{} = AtmTaskExecution) ->
    AtmTaskExecution;
ensure_atm_task_execution_record(AtmTaskExecutionId) ->
    {ok, #document{value = AtmTaskExecutionRecord}} = atm_task_execution:get(AtmTaskExecutionId),
    AtmTaskExecutionRecord.
