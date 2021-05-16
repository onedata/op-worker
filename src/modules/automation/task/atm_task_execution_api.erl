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

-include("modules/automation/atm_task_execution.hrl").
-include("modules/automation/atm_wokflow_execution.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([
    create_all/4, create/4,
    init_all/1, init/1,
    delete_all/1, delete/1,
    run/2
]).


-type registry() :: #{atm_task_execution:id() => atm_task_execution:status()}.
-type task_id() :: binary().

-export_type([registry/0, task_id/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_all(
    atm_workflow_execution:id(),
    non_neg_integer(),
    non_neg_integer(),
    [atm_task_schema()]
) ->
    registry() | no_return().
create_all(AtmWorkflowExecutionId, AtmLaneNo, AtmParallelBoxNo, AtmTaskSchemas) ->
    lists:foldl(fun(#atm_task_schema{
        id = AtmTaskSchemaId
    } = AtmTaskSchema, Acc) ->
        try
            {ok, AtmTaskExecutionId} = create(
                AtmWorkflowExecutionId, AtmLaneNo, AtmParallelBoxNo, AtmTaskSchema
            ),
            #{AtmTaskExecutionId => ?PENDING_STATUS}
        catch _:Reason ->
            delete_all(maps:keys(Acc)),
            throw(?ERROR_ATM_TASK_EXECUTION_CREATION_FAILED(AtmTaskSchemaId, Reason))
        end
    end, #{}, AtmTaskSchemas).


-spec create(
    atm_workflow_execution:id(),
    non_neg_integer(),
    non_neg_integer(),
    atm_task_schema()
) ->
    {ok, atm_task_execution:id()} | no_return().
create(AtmWorkflowExecutionId, AtmLaneNo, AtmParallelBoxNo, #atm_task_schema{
    id = AtmTaskSchemaId,
    name = AtmTaskName,
    lambda_id = AtmLambdaId,
    argument_mappings = AtmTaskArgMappers
}) ->
    #od_atm_lambda{
        operation_spec = AtmLambdaOperationSpec,
        argument_specs = AtmLambdaArgSpecs
    } = od_atm_lambda:get(AtmLambdaId),

    {ok, _} = atm_task_execution:create(#atm_task_execution{
        schema_id = AtmTaskSchemaId,
        name = AtmTaskName,
        lambda_id = AtmLambdaId,

        workflow_execution_id = AtmWorkflowExecutionId,
        lane_no = AtmLaneNo,
        parallel_box_no = AtmParallelBoxNo,

        executor = atm_task_executor:create(AtmWorkflowExecutionId, AtmLambdaOperationSpec),
        argument_specs = atm_task_execution_args:build_specs(AtmLambdaArgSpecs, AtmTaskArgMappers),

        status = ?PENDING_STATUS,
        handled_items = 0,
        processed_items = 0,
        failed_items = 0
    }).


-spec init_all([atm_task_execution:id()]) -> ok | no_return().
init_all(AtmTaskExecutionIds) ->
    lists:foreach(fun(AtmTaskExecutionId) ->
        {ok, AtmTaskExecutionRecord = #atm_task_execution{
            schema_id = AtmTaskSchemaId
        }} = atm_task_execution:get(AtmTaskExecutionId),

        try
            init(AtmTaskExecutionRecord)
        catch _:Reason ->
            throw(?ERROR_ATM_TASK_EXECUTION_INIT_FAILED(AtmTaskSchemaId, Reason))
        end
    end, AtmTaskExecutionIds).


-spec init(atm_task_execution:id() | atm_task_execution:record()) -> ok | no_return().
init(AtmTaskExecutionIdOrRecord) ->
    #atm_task_execution{executor = AtmTaskExecutor} = ensure_atm_task_execution_record(
        AtmTaskExecutionIdOrRecord
    ),
    atm_task_executor:init(AtmTaskExecutor).


-spec delete_all([atm_task_execution:id()]) -> ok.
delete_all(AtmTaskExecutionIds) ->
    lists:foreach(fun delete/1, AtmTaskExecutionIds).


-spec delete(atm_task_execution:id()) -> ok | {error, term()}.
delete(AtmTaskExecutionId) ->
    atm_task_execution:delete(AtmTaskExecutionId).


-spec run(json_utils:json_term(), atm_task_execution:id()) ->
    {ok, task_id()} | no_return().
run(Item, AtmTaskExecutionId) ->
    #atm_task_execution{
        executor = AtmTaskExecutor,
        argument_specs = AtmTaskArgSpecs
    } = ensure_atm_task_execution_record(AtmTaskExecutionId),

    AtmTaskExecutionCtx = #atm_task_execution_ctx{
        item = Item,
        stores = #{}  %% TODO VFS-7638 get stores from workflow execution ctx
    },
    Args = atm_task_execution_args:build_args(AtmTaskExecutionCtx, AtmTaskArgSpecs),
    atm_task_executor:run(Args, AtmTaskExecutor).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec ensure_atm_task_execution_record(atm_task_execution:id() | atm_task_execution:record()) ->
    atm_task_execution:record().
ensure_atm_task_execution_record(#atm_task_execution{} = AtmTaskExecution) ->
    AtmTaskExecution;
ensure_atm_task_execution_record(AtmTaskExecutionId) ->
    {ok, AtmTaskExecutionRecord} = atm_task_execution:get(AtmTaskExecutionId),
    AtmTaskExecutionRecord.
