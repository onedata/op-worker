%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles creation of all documents associated with automation
%%% workflow (e.g. task execution docs, store docs, etc.). If creation of any
%%% element fails then ones created before are deleted.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_factory).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([create/1]).


-record(execution_elements, {
    schema_snapshot_id = undefined :: undefined | atm_workflow_schema_snapshot:id(),
    lambda_snapshot_registry = undefined :: undefined | atm_workflow_execution:lambda_snapshot_registry(),
    store_registry = undefined :: undefined | atm_workflow_execution:store_registry(),
    lanes = undefined :: undefined | [atm_lane_execution:record()]
}).
-type execution_elements() :: #execution_elements{}.


%%%===================================================================
%%% API
%%%===================================================================


-spec create(atm_workflow_execution:creation_ctx()) ->
    {ok, atm_workflow_execution:doc()} | no_return().
create(AtmWorkflowExecutionCreationCtx) ->
    AtmWorkflowExecutionDoc = create_workflow_execution_doc(
        AtmWorkflowExecutionCreationCtx,
        create_execution_elements(AtmWorkflowExecutionCreationCtx)
    ),
    atm_waiting_workflow_executions:add(AtmWorkflowExecutionDoc),

    {ok, AtmWorkflowExecutionDoc}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_execution_elements(atm_workflow_execution:creation_ctx()) ->
    execution_elements() | no_return().
create_execution_elements(AtmWorkflowExecutionCreationCtx) ->
    lists:foldl(fun(CreateExecutionElementFun, ExecutionElements) ->
        try
            CreateExecutionElementFun(AtmWorkflowExecutionCreationCtx, ExecutionElements)
        catch Type:Reason ->
            delete_execution_elements(ExecutionElements),
            erlang:Type(Reason)
        end
    end, #execution_elements{}, [
        fun create_schema_snapshot/2,
        fun create_lambda_snapshots/2,
        fun create_stores/2,
        fun create_lane_executions/2
    ]).


%% @private
-spec create_schema_snapshot(atm_workflow_execution:creation_ctx(), execution_elements()) ->
    execution_elements().
create_schema_snapshot(#atm_workflow_execution_creation_ctx{
    workflow_execution_ctx = AtmWorkflowExecutionCtx,
    workflow_schema_doc = AtmWorkflowSchemaDoc
}, ExecutionElements) ->
    {ok, AtmWorkflowSchemaSnapshotId} = atm_workflow_schema_snapshot:create(
        atm_workflow_execution_ctx:get_workflow_execution_id(AtmWorkflowExecutionCtx),
        AtmWorkflowSchemaDoc
    ),
    ExecutionElements#execution_elements{schema_snapshot_id = AtmWorkflowSchemaSnapshotId}.


%% @private
-spec create_lambda_snapshots(atm_workflow_execution:creation_ctx(), execution_elements()) ->
    execution_elements().
create_lambda_snapshots(#atm_workflow_execution_creation_ctx{
    workflow_execution_ctx = AtmWorkflowExecutionCtx,
    lambda_docs = AtmLambdaDocs
}, ExecutionElements) ->
    AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
        AtmWorkflowExecutionCtx
    ),
    AtmLambdaSnapshotRegistry = lists:foldl(fun(#document{key = AtmLambdaId} = AtmLambdaDoc, Acc) ->
        {ok, AtmLambdaSnapshotId} = atm_lambda_snapshot:create(AtmWorkflowExecutionId, AtmLambdaDoc),
        Acc#{AtmLambdaId => AtmLambdaSnapshotId}
    end, #{}, maps:values(AtmLambdaDocs)),

    ExecutionElements#execution_elements{lambda_snapshot_registry = AtmLambdaSnapshotRegistry}.


%% @private
-spec create_stores(atm_workflow_execution:creation_ctx(), execution_elements()) ->
    execution_elements().
create_stores(AtmWorkflowExecutionCreationCtx, ExecutionElements) ->
    AtmStoreDocs = atm_store_api:create_all(AtmWorkflowExecutionCreationCtx),
    AtmStoreRegistry = lists:foldl(fun(#document{key = AtmStoreId, value = #atm_store{
        schema_id = AtmStoreSchemaId
    }}, Acc) ->
        Acc#{AtmStoreSchemaId => AtmStoreId}
    end, #{}, AtmStoreDocs),

    ExecutionElements#execution_elements{store_registry = AtmStoreRegistry}.


%% @private
-spec create_lane_executions(atm_workflow_execution:creation_ctx(), execution_elements()) ->
    execution_elements().
create_lane_executions(AtmWorkflowExecutionCreationCtx, ExecutionElements) ->
    ExecutionElements#execution_elements{
        lanes = atm_lane_execution:create_all(AtmWorkflowExecutionCreationCtx)
    }.


%% @private
-spec create_workflow_execution_doc(atm_workflow_execution:creation_ctx(), execution_elements()) ->
    atm_workflow_execution:doc() | no_return().
create_workflow_execution_doc(
    #atm_workflow_execution_creation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        workflow_schema_doc = #document{value = #od_atm_workflow_schema{
            name = AtmWorkflowSchemaName,
            atm_inventory = AtmInventoryId
        }}
    },
    ExecutionElements = #execution_elements{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
        lambda_snapshot_registry = AtmLambdaSnapshotRegistry,
        store_registry = AtmStoreRegistry,
        lanes = AtmLaneExecutions
    }
) ->
    try
        {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:create(#document{
            key = atm_workflow_execution_ctx:get_workflow_execution_id(
                AtmWorkflowExecutionCtx
            ),
            value = #atm_workflow_execution{
                space_id = atm_workflow_execution_ctx:get_space_id(AtmWorkflowExecutionCtx),
                atm_inventory_id = AtmInventoryId,

                name = AtmWorkflowSchemaName,
                schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
                lambda_snapshot_registry = AtmLambdaSnapshotRegistry,

                store_registry = AtmStoreRegistry,
                lanes = AtmLaneExecutions,

                status = ?SCHEDULED_STATUS,

                schedule_time = global_clock:timestamp_seconds(),
                start_time = 0,
                finish_time = 0
            }
        }),
        AtmWorkflowExecutionDoc
    catch Type:Reason ->
        delete_execution_elements(ExecutionElements),
        erlang:Type(Reason)
    end.


%% @private
-spec delete_execution_elements(execution_elements()) -> ok.
delete_execution_elements(#execution_elements{
    schema_snapshot_id = AtmWorkflowSchemaSnapshotId
} = ExecutionElements) when AtmWorkflowSchemaSnapshotId /= undefined ->
    catch atm_workflow_schema_snapshot:delete(AtmWorkflowSchemaSnapshotId),

    delete_execution_elements(ExecutionElements#execution_elements{
        schema_snapshot_id = undefined
    });

delete_execution_elements(#execution_elements{
    lambda_snapshot_registry = AtmLambdaSnapshotRegistry
} = ExecutionElements) when AtmLambdaSnapshotRegistry /= undefined ->
    lists:foreach(fun atm_lambda_snapshot:delete/1, maps:values(AtmLambdaSnapshotRegistry)),

    delete_execution_elements(ExecutionElements#execution_elements{
        lambda_snapshot_registry = undefined
    });

delete_execution_elements(#execution_elements{
    store_registry = AtmStoreRegistry
} = ExecutionElements) when AtmStoreRegistry /= undefined ->
    catch atm_store_api:delete_all(maps:values(AtmStoreRegistry)),

    delete_execution_elements(ExecutionElements#execution_elements{
        store_registry = undefined
    });

delete_execution_elements(#execution_elements{
    lanes = AtmLaneExecutions
} = ExecutionElements) when AtmLaneExecutions /= undefined ->
    catch atm_lane_execution:delete_all(AtmLaneExecutions),
    delete_execution_elements(ExecutionElements#execution_elements{lanes = undefined});

delete_execution_elements(_) ->
    ok.
