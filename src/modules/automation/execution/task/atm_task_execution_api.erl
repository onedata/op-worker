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
-include("modules/automation/atm_tmp.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([create/3, init/1, run/2, delete/1]).


-type task_id() :: binary().

-export_type([task_id/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%-------------------------------------------------------------------
%% @doc
%% Creates automation task execution related documents in db. No further
%% initialization is performed (e.g. registering functions in Openfaas
%% service) - this must be done with call to 'init/1'.
%% @end
%%-------------------------------------------------------------------
-spec create(atm_workflow_execution:id(), atm_task_schema(), od_atm_lambda()) ->
    {ok, atm_task_execution:id()} | no_return().
create(AtmWorkflowExecutionId, #atm_task_schema{
    id = AtmTaskSchemaId,
    name = AtmTaskName,
    lambda_id = AtmLambdaId,
    argument_mappings = AtmTaskArgMappers
}, AtmLambda) ->
    %% TODO rm lambda arg after integration with lo
    #od_atm_lambda{
        operation_spec = AtmLambdaOperationSpec = #atm_lambda_operation_spec{
            spec = Spec
        },
        argument_specs = AtmLambdaArgSpecs
    } = AtmLambda,

    ExecutorModel = get_executor_model(Spec),

    {ok, _} = atm_task_execution:create(#atm_task_execution{
        name = AtmTaskName,
        schema_id = AtmTaskSchemaId,
        lambda_id = AtmLambdaId,
        executor = atm_task_executor:create(
            ExecutorModel, AtmWorkflowExecutionId, AtmLambdaOperationSpec
        ),
        argument_specs = atm_task_execution_args:build_specs(
            AtmLambdaArgSpecs, AtmTaskArgMappers
        )
    }).


-spec init(atm_task_execution:id()) -> ok | no_return().
init(AtmTaskExecutionId) ->
    #atm_task_execution{executor = AtmTaskExecutor} = ensure_atm_task_execution_record(
        AtmTaskExecutionId
    ),
    atm_task_executor:init(AtmTaskExecutor).


-spec run(json_utils:json_term(), atm_task_execution:id()) ->
    {ok, atm_task_execution_api:task_id()} | no_return().
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


-spec delete(atm_task_execution:id()) -> ok | {error, term()}.
delete(AtmTaskExecutionId) ->
    atm_task_execution:delete(AtmTaskExecutionId).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_executor_model(atm_openfaas_operation_spec()) ->
    atom().
get_executor_model(#atm_openfaas_operation_spec{}) -> openfaas.


%% @private
-spec ensure_atm_task_execution_record(atm_task_execution:id() | atm_task_execution:record()) ->
    atm_task_execution:record().
ensure_atm_task_execution_record(#atm_task_execution{} = AtmTaskExecution) ->
    AtmTaskExecution;
ensure_atm_task_execution_record(AtmTaskExecutionId) ->
    {ok, AtmTaskExecutionRecord} = atm_task_execution:get(AtmTaskExecutionId),
    AtmTaskExecutionRecord.
