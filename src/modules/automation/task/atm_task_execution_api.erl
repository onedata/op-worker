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
    prepare_all/2, prepare/2,
    delete_all/1, delete/1,

    get_spec/1,
    run/5, handle_results/3,
    mark_ended/1
]).


-type task_id() :: binary().

-export_type([task_id/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_all(
    atm_workflow_execution:creation_ctx(),
    non_neg_integer(),
    non_neg_integer(),
    [atm_task_schema:record()]
) ->
    [atm_task_execution:doc()] | no_return().
create_all(AtmWorkflowExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, AtmTaskSchemas) ->
    lists:reverse(lists:foldl(fun(#atm_task_schema{id = AtmTaskSchemaId} = AtmTaskSchema, Acc) ->
        try
            {ok, AtmTaskExecutionDoc} = create(
                AtmWorkflowExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, AtmTaskSchema
            ),
            [AtmTaskExecutionDoc | Acc]
        catch _:Reason ->
            catch delete_all([Doc#document.key || Doc <- Acc]),
            throw(?ERROR_ATM_TASK_EXECUTION_CREATION_FAILED(AtmTaskSchemaId, Reason))
        end
    end, [], AtmTaskSchemas)).


-spec create(
    atm_workflow_execution:creation_ctx(),
    non_neg_integer(),
    non_neg_integer(),
    atm_task_schema:record()
) ->
    {ok, atm_task_execution:doc()} | no_return().
create(AtmWorkflowExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, #atm_task_schema{
    id = AtmTaskSchemaId,
    lambda_id = AtmLambdaId,
    argument_mappings = AtmTaskSchemaArgMappers,
    result_mappings = AtmTaskSchemaResultMappers
}) ->
    #atm_workflow_execution_creation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        lambda_docs = AtmLambdaDocs
    } = AtmWorkflowExecutionCreationCtx,

    AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
        AtmWorkflowExecutionCtx
    ),

    #document{value = #od_atm_lambda{
        operation_spec = AtmLambdaOperationSpec,
        argument_specs = AtmLambdaArgSpecs,
        result_specs = AtmLambdaResultSpecs
    }} = maps:get(AtmLambdaId, AtmLambdaDocs),

    {ok, _} = atm_task_execution:create(#atm_task_execution{
        workflow_execution_id = AtmWorkflowExecutionId,
        lane_index = AtmLaneIndex,
        parallel_box_index = AtmParallelBoxIndex,

        schema_id = AtmTaskSchemaId,

        executor = atm_task_executor:create(AtmWorkflowExecutionId, AtmLambdaOperationSpec),
        argument_specs = atm_task_execution_arguments:build_specs(
            AtmLambdaArgSpecs, AtmTaskSchemaArgMappers
        ),
        result_specs = atm_task_execution_results:build_specs(
            AtmLambdaResultSpecs, AtmTaskSchemaResultMappers
        ),

        status = ?PENDING_STATUS,

        items_in_processing = 0,
        items_processed = 0,
        items_failed = 0
    }).


-spec prepare_all(atm_workflow_execution_ctx:record(), [atm_task_execution:id()]) ->
    ok | no_return().
prepare_all(AtmWorkflowExecutionCtx, AtmTaskExecutionIds) ->
    lists:foreach(fun(AtmTaskExecutionId) ->
        {ok, AtmTaskExecutionDoc = #document{value = #atm_task_execution{
            schema_id = AtmTaskSchemaId
        }}} = atm_task_execution:get(AtmTaskExecutionId),

        try
            prepare(AtmWorkflowExecutionCtx, AtmTaskExecutionDoc)
        catch _:Reason ->
            throw(?ERROR_ATM_TASK_EXECUTION_PREPARATION_FAILED(AtmTaskSchemaId, Reason))
        end
    end, AtmTaskExecutionIds).


-spec prepare(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id() | atm_task_execution:doc()
) ->
    ok | no_return().
prepare(AtmWorkflowExecutionCtx, AtmTaskExecutionIdOrDoc) ->
    #document{value = #atm_task_execution{executor = AtmTaskExecutor}} = ensure_atm_task_execution_doc(
        AtmTaskExecutionIdOrDoc
    ),
    atm_task_executor:prepare(AtmWorkflowExecutionCtx, AtmTaskExecutor).


-spec delete_all([atm_task_execution:id()]) -> ok.
delete_all(AtmTaskExecutionIds) ->
    lists:foreach(fun delete/1, AtmTaskExecutionIds).


-spec delete(atm_task_execution:id()) -> ok | {error, term()}.
delete(AtmTaskExecutionId) ->
    atm_task_execution:delete(AtmTaskExecutionId).


-spec get_spec(atm_task_execution:id()) -> workflow_engine:task_spec().
get_spec(AtmTaskExecutionId) ->
    #document{value = #atm_task_execution{executor = AtmTaskExecutor}} = ensure_atm_task_execution_doc(
        AtmTaskExecutionId
    ),
    atm_task_executor:get_spec(AtmTaskExecutor).


-spec run(
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    json_utils:json_term(),
    binary(),
    binary()
) ->
    ok | no_return().
run(AtmWorkflowExecutionEnv, AtmTaskExecutionId, Item, ReportResultUrl, HeartbeatUrl) ->
    #document{value = #atm_task_execution{
        executor = AtmTaskExecutor,
        argument_specs = AtmTaskExecutionArgSpecs
    }} = update_items_in_processing(AtmTaskExecutionId),

    AtmJobExecutionCtx = atm_job_execution_ctx:build(
        AtmWorkflowExecutionEnv, atm_task_executor:in_readonly_mode(AtmTaskExecutor),
        Item, ReportResultUrl, HeartbeatUrl
    ),
    Args = atm_task_execution_arguments:construct_args(AtmJobExecutionCtx, AtmTaskExecutionArgSpecs),

    atm_task_executor:run(AtmJobExecutionCtx, Args, AtmTaskExecutor).


-spec handle_results(
    atm_workflow_execution_env:record(),
    atm_task_execution:id(),
    error | json_utils:json_map()
) ->
    ok | no_return().
handle_results(_AtmWorkflowExecutionEnv, AtmTaskExecutionId, error) ->
    update_items_failed_and_processed(AtmTaskExecutionId);

handle_results(AtmWorkflowExecutionEnv, AtmTaskExecutionId, Results) when is_map(Results) ->
    #document{value = #atm_task_execution{
        result_specs = AtmTaskExecutionResultSpecs
    }} = ensure_atm_task_execution_doc(AtmTaskExecutionId),

    atm_task_execution_results:apply(AtmWorkflowExecutionEnv, AtmTaskExecutionResultSpecs, Results),
    update_items_processed(AtmTaskExecutionId);

handle_results(_AtmWorkflowExecutionEnv, _AtmTaskExecutionId, _Results) ->
    throw(?ERROR_ATM_BAD_DATA(<<"results">>, <<"not an object">>)).


-spec mark_ended(atm_task_execution:id()) -> ok.
mark_ended(AtmTaskExecutionId) ->
    {ok, AtmTaskExecutionDoc} = atm_task_execution:update(AtmTaskExecutionId, fun
        (#atm_task_execution{items_failed = 0} = AtmTaskExecution) ->
            {ok, AtmTaskExecution#atm_task_execution{status = ?FINISHED_STATUS}};
        (#atm_task_execution{} = AtmTaskExecution) ->
            {ok, AtmTaskExecution#atm_task_execution{status = ?FAILED_STATUS}}
    end),
    handle_status_change(AtmTaskExecutionDoc).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec update_items_in_processing(atm_task_execution:id()) ->
    atm_task_execution:doc().
update_items_in_processing(AtmTaskExecutionId) ->
    {ok, AtmTaskExecutionDoc} = atm_task_execution:update(AtmTaskExecutionId, fun
        (#atm_task_execution{status = ?PENDING_STATUS, items_in_processing = 0} = AtmTaskExecution) ->
            {ok, AtmTaskExecution#atm_task_execution{
                status = ?ACTIVE_STATUS,
                items_in_processing = 1
            }};
        (#atm_task_execution{items_in_processing = ItemsInProcessing} = AtmTaskExecution) ->
            {ok, AtmTaskExecution#atm_task_execution{items_in_processing = ItemsInProcessing + 1}}
    end),
    handle_status_change(AtmTaskExecutionDoc),
    AtmTaskExecutionDoc.


%% @private
-spec update_items_processed(atm_task_execution:id()) -> ok.
update_items_processed(AtmTaskExecutionId) ->
    {ok, _} = atm_task_execution:update(AtmTaskExecutionId, fun(#atm_task_execution{
        items_in_processing = ItemsInProcessing,
        items_processed = ItemsProcessed
    } = AtmTaskExecution) ->
        {ok, AtmTaskExecution#atm_task_execution{
            items_in_processing = ItemsInProcessing - 1,
            items_processed = ItemsProcessed + 1
        }}
    end),
    ok.


%% @private
-spec update_items_failed_and_processed(atm_task_execution:id()) -> ok.
update_items_failed_and_processed(AtmTaskExecutionId) ->
    {ok, _} = atm_task_execution:update(AtmTaskExecutionId, fun(#atm_task_execution{
        items_in_processing = ItemsInProcessing,
        items_processed = ItemsProcessed,
        items_failed = ItemsFailed
    } = AtmTaskExecution) ->
        {ok, AtmTaskExecution#atm_task_execution{
            items_in_processing = ItemsInProcessing - 1,
            items_processed = ItemsProcessed + 1,
            items_failed = ItemsFailed + 1
        }}
    end),
    ok.


%% @private
-spec handle_status_change(atm_task_execution:doc()) -> ok.
handle_status_change(#document{value = #atm_task_execution{status_changed = false}}) ->
    ok;
handle_status_change(#document{
    key = AtmTaskExecutionId,
    value = #atm_task_execution{
        workflow_execution_id = AtmWorkflowExecutionId,
        lane_index = AtmLaneIndex,
        parallel_box_index = AtmParallelBoxIndex,
        status = NewStatus,
        status_changed = true
    }
}) ->
    atm_workflow_execution_status:report_task_status_change(
        AtmWorkflowExecutionId, AtmLaneIndex, AtmParallelBoxIndex,
        AtmTaskExecutionId, NewStatus
    ).


%% @private
-spec ensure_atm_task_execution_doc(atm_task_execution:id() | atm_task_execution:doc()) ->
    atm_task_execution:doc().
ensure_atm_task_execution_doc(#document{value = #atm_task_execution{}} = AtmTaskExecutionDoc) ->
    AtmTaskExecutionDoc;
ensure_atm_task_execution_doc(AtmTaskExecutionId) ->
    {ok, AtmTaskExecutionDoc = #document{}} = atm_task_execution:get(AtmTaskExecutionId),
    AtmTaskExecutionDoc.
