%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles creation of all documents associated with automation
%%% lane execution (e.g. exception store, etc.). If creation of any element
%%% fails then ones created before are deleted.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution_factory).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([create_run/3, delete_run/1]).

-type create_run_ctx() :: #atm_lane_execution_run_create_ctx{}.

-export_type([create_run_ctx/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_run(pos_integer(), atm_workflow_execution:doc(), atm_workflow_execution_ctx:record()) ->
    atm_workflow_execution:doc() | no_return().
create_run(AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    try
        create_run_internal(AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx)
    catch _:Reason ->
        AtmWorkflowExecution = AtmWorkflowExecutionDoc#document.value,
        AtmLaneSchemaId = atm_lane_execution:get_schema_id(AtmLaneIndex, AtmWorkflowExecution),
        throw(?ERROR_ATM_LANE_EXECUTION_CREATION_FAILED(AtmLaneSchemaId, Reason))
    end.


-spec delete_run(atm_lane_execution:run()) -> ok.
delete_run(#atm_lane_execution_run{
    exception_store_id = ExceptionStoreId,
    parallel_boxes = AtmParallelBoxExecutions
}) ->
    delete_execution_elements(#atm_lane_execution_run_elements{
        exception_store_id = ExceptionStoreId,
        parallel_boxes = AtmParallelBoxExecutions
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_run_internal(
    pos_integer(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    atm_workflow_execution:doc() | no_return().
create_run_internal(AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    #atm_lane_execution_run_create_ctx{
        iterated_store_id = IteratedStoreId,
        elements = ExecutionElements = #atm_lane_execution_run_elements{
            exception_store_id = ExceptionStoreId,
            parallel_boxes = AtmParallelBoxExecutions
        }
    } = create_execution_elements(build_lane_execution_create_ctx(
        AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
    )),

    Diff = fun(AtmWorkflowExecution) ->
        atm_lane_execution:update_curr_run(AtmLaneIndex, fun
            (Run = #atm_lane_execution_run{
                status = ?PREPARING_STATUS,
                exception_store_id = undefined,
                parallel_boxes = []
            }) ->
                {ok, Run#atm_lane_execution_run{
                    iterated_store_id = IteratedStoreId,
                    exception_store_id = ExceptionStoreId,
                    parallel_boxes = AtmParallelBoxExecutions
                }};
            (_) ->
                ?ERROR_ALREADY_EXISTS
        end, AtmWorkflowExecution)
    end,
    case atm_workflow_execution:update(AtmWorkflowExecutionDoc#document.key, Diff) of
        {ok, NewAtmWorkflowExecutionDoc} ->
            NewAtmWorkflowExecutionDoc;
        {error, _} = Error ->
            delete_execution_elements(ExecutionElements),
            throw(Error)
    end.


%% @private
-spec build_lane_execution_create_ctx(
    pos_integer(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    create_run_ctx().
build_lane_execution_create_ctx(AtmLaneIndex, AtmWorkflowExecutionDoc = #document{
    value = AtmWorkflowExecution = #atm_workflow_execution{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId
    }
}, AtmWorkflowExecutionCtx) ->
    {ok, LaneRun = #atm_lane_execution_run{status = ?PREPARING_STATUS}} = atm_lane_execution:get_curr_run(
        AtmLaneIndex, AtmWorkflowExecution
    ),

    {ok, AtmWorkflowSchemaSnapshotDoc = #document{value = #atm_workflow_schema_snapshot{
        lanes = AtmLaneSchemas
    }}} = atm_workflow_schema_snapshot:get(AtmWorkflowSchemaSnapshotId),

    AtmLaneSchema = #atm_lane_schema{store_iterator_spec = #atm_store_iterator_spec{
        store_schema_id = AtmStoreSchemaId
    }} = lists:nth(AtmLaneIndex, AtmLaneSchemas),

    #atm_lane_execution_run_create_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,

        workflow_schema_snapshot_doc = AtmWorkflowSchemaSnapshotDoc,
        workflow_execution_doc = AtmWorkflowExecutionDoc,

        lane_index = AtmLaneIndex,
        lane_schema = AtmLaneSchema,

        iterated_store_id = case LaneRun#atm_lane_execution_run.iterated_store_id of
            undefined ->
                % If not explicitly set then take designated by schema store
                atm_workflow_execution_ctx:get_workflow_store_id(
                    AtmStoreSchemaId, AtmWorkflowExecutionCtx
                );
            IteratedStoreId ->
                IteratedStoreId
        end,

        elements = #atm_lane_execution_run_elements{
            exception_store_id = undefined,
            parallel_boxes = undefined
        }
    }.


%% @private
-spec create_execution_elements(create_run_ctx()) -> create_run_ctx() | no_return().
create_execution_elements(AtmLaneExecutionRunCreateCtx) ->
    lists:foldl(fun(CreateExecutionElementFun, NewAtmLaneExecutionRunCreateCtx) ->
        try
            CreateExecutionElementFun(NewAtmLaneExecutionRunCreateCtx)
        catch Type:Reason ->
            delete_execution_elements(
                NewAtmLaneExecutionRunCreateCtx#atm_lane_execution_run_create_ctx.elements
            ),
            erlang:Type(Reason)
        end
    end, AtmLaneExecutionRunCreateCtx, [
        fun create_exception_store/1,
        fun create_parallel_box_executions/1
    ]).


%% @private
-spec create_exception_store(create_run_ctx()) -> create_run_ctx().
create_exception_store(AtmLaneExecutionRunCreateCtx = #atm_lane_execution_run_create_ctx{
    workflow_execution_ctx = AtmWorkflowExecutionCtx,
    iterated_store_id = AtmIteratedStoreId,
    elements = ExecutionElements
}) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx),
    {ok, #atm_store{container = AtmStoreContainer}} = atm_store_api:get(AtmIteratedStoreId),

    {ok, #document{key = AtmLaneExceptionStoreId}} = atm_store_api:create(
        AtmWorkflowExecutionAuth, undefined, #atm_store_schema{
            id = ?CURRENT_LANE_EXCEPTION_STORE_SCHEMA_ID,
            name = ?CURRENT_LANE_EXCEPTION_STORE_SCHEMA_ID,
            description = <<>>,
            type = list,
            data_spec = atm_store_container:get_data_spec(AtmStoreContainer),
            requires_initial_value = false
        }
    ),

    AtmLaneExecutionRunCreateCtx#atm_lane_execution_run_create_ctx{
        elements = ExecutionElements#atm_lane_execution_run_elements{
            exception_store_id = AtmLaneExceptionStoreId
        }
    }.


%% @private
-spec create_parallel_box_executions(create_run_ctx()) -> create_run_ctx().
create_parallel_box_executions(AtmLaneExecutionRunCreateCtx = #atm_lane_execution_run_create_ctx{
    elements = ExecutionElements
}) ->
    AtmLaneExecutionRunCreateCtx#atm_lane_execution_run_create_ctx{
        elements = ExecutionElements#atm_lane_execution_run_elements{
            parallel_boxes = atm_parallel_box_execution:create_all(AtmLaneExecutionRunCreateCtx)
        }
    }.


%% @private
-spec delete_execution_elements(atm_lane_execution:run_elements()) -> ok.
delete_execution_elements(ExecutionElements = #atm_lane_execution_run_elements{
    exception_store_id = AtmLaneExceptionStoreId
}) when AtmLaneExceptionStoreId /= undefined ->
    catch atm_store_api:delete(AtmLaneExceptionStoreId),

    delete_execution_elements(ExecutionElements#atm_lane_execution_run_elements{
        exception_store_id = undefined
    });

delete_execution_elements(ExecutionElements = #atm_lane_execution_run_elements{
    parallel_boxes = AtmParallelBoxExecutions
}) when AtmParallelBoxExecutions /= undefined ->
    catch atm_parallel_box_execution:delete_all(AtmParallelBoxExecutions),

    delete_execution_elements(ExecutionElements#atm_lane_execution_run_elements{
        parallel_boxes = undefined
    });

delete_execution_elements(_) ->
    ok.
