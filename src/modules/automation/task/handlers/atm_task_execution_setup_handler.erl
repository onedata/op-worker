%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021-2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles the task execution setup process (for information about
%%% state machine @see 'atm_task_execution_status.erl').
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_setup_handler).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    start/2,
    resume/2,

    set_run_num/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec start(atm_workflow_execution_ctx:record(), atm_task_execution:id()) ->
    {workflow_engine:task_spec(), atm_workflow_execution_env:diff()} | no_return().
start(AtmWorkflowExecutionCtx, AtmTaskExecutionId) ->
    {ok, AtmTaskExecutionDoc} = atm_task_execution:get(AtmTaskExecutionId),
    Result = initiate(AtmWorkflowExecutionCtx, AtmTaskExecutionDoc),

    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    ?atm_task_info(Logger, <<"Task started.">>),
    ?atm_workflow_info(Logger, ?ATM_WORKFLOW_TASK_LOG(AtmTaskExecutionId, <<"Started.">>)),

    Result.


-spec resume(atm_workflow_execution_ctx:record(), atm_task_execution:id()) ->
    ignored | {ok, {workflow_engine:task_spec(), atm_workflow_execution_env:diff()}} | no_return().
resume(AtmWorkflowExecutionCtx, AtmTaskExecutionId) ->
    case atm_task_execution_status:handle_resuming(
        AtmTaskExecutionId,
        atm_workflow_execution_ctx:get_workflow_execution_incarnation(AtmWorkflowExecutionCtx)
    ) of
        {ok, AtmTaskExecutionDoc} ->
            handle_resuming(AtmWorkflowExecutionCtx, AtmTaskExecutionDoc);

        {error, task_already_stopped} ->
            ignored
    end.


-spec set_run_num(atm_lane_execution:run_num(), atm_task_execution:id()) ->
    ok.
set_run_num(RunNum, AtmTaskExecutionId) ->
    {ok, _} = atm_task_execution:update(AtmTaskExecutionId, fun(AtmTaskExecution) ->
        {ok, AtmTaskExecution#atm_task_execution{run_num = RunNum}}
    end),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec handle_resuming(atm_workflow_execution_ctx:record(), atm_task_execution:doc()) ->
    {ok, {workflow_engine:task_spec(), atm_workflow_execution_env:diff()}} | no_return().
handle_resuming(AtmWorkflowExecutionCtx, AtmTaskExecutionDoc = #document{
    key = AtmTaskExecutionId,
    value = AtmTaskExecution
}) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),

    ?atm_task_debug(Logger, <<"Task resuming...">>),
    ?atm_workflow_debug(Logger, ?ATM_WORKFLOW_TASK_LOG(AtmTaskExecutionId, <<"Resuming...">>)),

    unfreeze_stores(AtmTaskExecution),
    InitiationResult = initiate(AtmWorkflowExecutionCtx, AtmTaskExecutionDoc),

    case atm_task_execution_status:handle_resumed(AtmTaskExecutionId) of
        {ok, _} ->
            ?atm_task_info(Logger, <<"Task resumed.">>),
            ?atm_workflow_info(Logger, ?ATM_WORKFLOW_TASK_LOG(AtmTaskExecutionId, <<"Resumed.">>)),

            {ok, InitiationResult};
        {error, task_already_stopped} ->
            atm_task_execution_stop_handler:teardown(AtmWorkflowExecutionCtx, AtmTaskExecutionDoc),
            throw(?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING)
    end.


%% @private
-spec unfreeze_stores(atm_task_execution:record()) -> ok.
unfreeze_stores(#atm_task_execution{
    system_audit_log_store_id = AtmSystemAuditLogStoreId,
    time_series_store_id = AtmTSStoreId
}) ->
    AtmTSStoreId /= undefined andalso atm_store_api:unfreeze(AtmTSStoreId),
    atm_store_api:unfreeze(AtmSystemAuditLogStoreId).


%% @private
-spec initiate(atm_workflow_execution_ctx:record(), atm_task_execution:doc()) ->
    {workflow_engine:task_spec(), atm_workflow_execution_env:diff()} | no_return().
initiate(AtmWorkflowExecutionCtx, AtmTaskExecutionDoc = #document{value = #atm_task_execution{
    executor = AtmTaskExecutor
}}) ->
    AtmTaskExecutionSpec = atm_task_executor:initiate(
        build_task_executor_initiation_ctx(AtmWorkflowExecutionCtx, AtmTaskExecutionDoc),
        AtmTaskExecutor
    ),
    AtmWorkflowExecutionEnvDiff = gen_atm_workflow_execution_env_diff(AtmTaskExecutionDoc),

    {AtmTaskExecutionSpec, AtmWorkflowExecutionEnvDiff}.


%% @private
-spec build_task_executor_initiation_ctx(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id() | atm_task_execution:doc()
) ->
    atm_task_executor:initiation_ctx().
build_task_executor_initiation_ctx(AtmWorkflowExecutionCtx, #document{
    key = AtmTaskExecutionId,
    value = AtmTaskExecution = #atm_task_execution{
        workflow_execution_id = AtmWorkflowExecutionId,
        uncorrelated_result_specs = AtmTaskExecutionUncorrelatedResultSpecs
    }
}) ->
    {ok, #document{value = AtmWorkflowExecution}} = atm_workflow_execution:get(
        AtmWorkflowExecutionId
    ),
    AtmTaskSchema = get_task_schema(AtmTaskExecution, AtmWorkflowExecution),

    #atm_task_executor_initiation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        task_execution_id = AtmTaskExecutionId,
        task_schema = AtmTaskSchema,
        lambda_revision = get_lambda_revision(AtmTaskSchema, AtmWorkflowExecution),
        uncorrelated_results = lists:map(
            fun atm_task_execution_result_spec:get_name/1,
            AtmTaskExecutionUncorrelatedResultSpecs
        )
    }.


%% @private
-spec get_task_schema(atm_task_execution:record(), atm_workflow_execution:record()) ->
    atm_task_schema:record().
get_task_schema(
    #atm_task_execution{
        lane_index = AtmLaneIndex,
        parallel_box_index = AtmParallelBoxIndex,
        schema_id = AtmTaskSchemaId
    },
    #atm_workflow_execution{schema_snapshot_id = AtmWorkflowSchemaSnapshotId}
) ->
    {ok, AtmWorkflowSchemaSnapshotDoc} = atm_workflow_schema_snapshot:get(AtmWorkflowSchemaSnapshotId),

    AtmLaneSchemas = atm_workflow_schema_snapshot:get_lane_schemas(AtmWorkflowSchemaSnapshotDoc),
    AtmLaneSchema = lists:nth(AtmLaneIndex, AtmLaneSchemas),
    AtmParallelBoxSchema = lists:nth(AtmParallelBoxIndex, AtmLaneSchema#atm_lane_schema.parallel_boxes),

    {value, AtmTaskSchema} = lists:search(
        fun(#atm_task_schema{id = Id}) -> Id == AtmTaskSchemaId end,
        AtmParallelBoxSchema#atm_parallel_box_schema.tasks
    ),
    AtmTaskSchema.


%% @private
-spec get_lambda_revision(atm_task_schema:record(), atm_workflow_execution:record()) ->
    atm_lambda_revision:record().
get_lambda_revision(
    #atm_task_schema{lambda_id = AtmLambdaId, lambda_revision_number = AtmLambdaRevisionNum},
    #atm_workflow_execution{lambda_snapshot_registry = AtmLambdaSnapshotRegistry}
) ->
    {ok, #document{value = AtmLambdaSnapshot}} = atm_lambda_snapshot:get(
        maps:get(AtmLambdaId, AtmLambdaSnapshotRegistry)
    ),
    atm_lambda_snapshot:get_revision(AtmLambdaRevisionNum, AtmLambdaSnapshot).


%% @private
-spec gen_atm_workflow_execution_env_diff(atm_task_execution:doc()) ->
    atm_workflow_execution_env:diff().
gen_atm_workflow_execution_env_diff(#document{
    key = AtmTaskExecutionId,
    value = #atm_task_execution{
        system_audit_log_store_id = AtmSystemAuditLogStoreId,
        time_series_store_id = AtmTaskTSStoreId
    }
}) ->
    {ok, #atm_store{container = AtmTaskAuditLogStoreContainer}} = atm_store_api:get(
        AtmSystemAuditLogStoreId
    ),

    fun(Env0) ->
        Env1 = atm_workflow_execution_env:add_task_audit_log_store_container(
            AtmTaskExecutionId, AtmTaskAuditLogStoreContainer, Env0
        ),
        atm_workflow_execution_env:add_task_time_series_store_id(
            AtmTaskExecutionId, AtmTaskTSStoreId, Env1
        )
    end.
