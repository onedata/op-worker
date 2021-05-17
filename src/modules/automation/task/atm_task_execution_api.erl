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

-include("modules/fslogic/fslogic_common.hrl").
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
    {ok, #document{value = #od_atm_lambda{
        operation_spec = AtmLambdaOperationSpec,
        argument_specs = AtmLambdaArgSpecs
    }}} = atm_lambda_logic:get(?ROOT_SESS_ID, AtmLambdaId),  %% TODO VFS-7671 use user session

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
        {ok, #document{value = AtmTaskExecutionRecord = #atm_task_execution{
            schema_id = AtmTaskSchemaId
        }}} = atm_task_execution:get(AtmTaskExecutionId),

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


-spec run(atm_task_execution:id(), json_utils:json_term()) ->
    {ok, task_id()} | no_return().
run(AtmTaskExecutionId, Item) ->
    #atm_task_execution{
        executor = AtmTaskExecutor,
        argument_specs = AtmTaskArgSpecs
    } = update_handled_items(AtmTaskExecutionId),

    AtmTaskExecutionCtx = #atm_task_execution_ctx{item = Item},
    Args = atm_task_execution_args:build_args(AtmTaskExecutionCtx, AtmTaskArgSpecs),

    atm_task_executor:run(Args, AtmTaskExecutor).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec update_handled_items(atm_task_execution:id()) ->
    atm_task_execution:record().
update_handled_items(AtmTaskExecutionId) ->
    {ok, #document{value = AtmTaskExecutionRecord}} = atm_task_execution:update(
        AtmTaskExecutionId, fun
            (#atm_task_execution{
                status = ?PENDING_STATUS,
                status_changed = false,
                handled_items = 0
            } = AtmTaskExecution) ->
                {ok, AtmTaskExecution#atm_task_execution{
                    status = ?ACTIVE_STATUS,
                    status_changed = true,
                    handled_items = 1
                }};
            (#atm_task_execution{handled_items = HandledItems} = AtmTaskExecution) ->
                {ok, AtmTaskExecution#atm_task_execution{
                    status_changed = false,
                    handled_items = HandledItems + 1
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
    lane_no = AtmLaneNo,
    parallel_box_no = AtmParallelBoxNo,
    status = NewStatus
}) ->
    atm_workflow_execution_api:report_task_status_change(
        AtmWorkflowExecutionId, AtmLaneNo, AtmParallelBoxNo,
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
