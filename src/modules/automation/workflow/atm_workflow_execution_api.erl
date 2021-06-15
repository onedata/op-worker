%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for performing operations on automation workflow executions.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_api).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([list/4]).
-export([create/1, delete/1]).
-export([get_summary/1, get_summary/2]).
-export([report_task_status_change/5]).


-type listing_mode() :: basic | summary.

-type basic_entries() :: atm_workflow_executions_forest:entries().
-type summary_entries() :: [{atm_workflow_executions_forest:index(), atm_workflow_execution:summary()}].
-type entries() :: basic_entries() | summary_entries().

-export_type([listing_mode/0, basic_entries/0, summary_entries/0, entries/0]).


-record(execution_elements, {
    schema_snapshot_id = undefined :: undefined | atm_workflow_schema_snapshot:id(),
    lambda_snapshot_registry = undefined :: undefined | #{
        od_atm_lambda:id() => atm_lambda_snapshot:id()
    },
    store_registry = undefined :: undefined | atm_workflow_execution:store_registry(),
    lanes = undefined :: undefined | [atm_lane_execution:record()]
}).
-type execution_elements() :: #execution_elements{}.


%%%===================================================================
%%% API
%%%===================================================================


-spec list(
    od_space:id(),
    atm_workflow_execution:phase(),
    listing_mode(),
    atm_workflow_executions_forest:listing_opts()
) ->
    {ok, entries(), IsLast :: boolean()}.
list(SpaceId, Phase, ListingMode, ListingOpts) ->
    AtmWorkflowExecutionEntries = list(SpaceId, Phase, ListingOpts),
    IsLast = maps:get(limit, ListingOpts) > length(AtmWorkflowExecutionEntries),

    case ListingMode of
        basic ->
            {ok, AtmWorkflowExecutionEntries, IsLast};
        summary ->
            AtmWorkflowExecutionSummaries = lists_utils:pmap(fun({Index, AtmWorkflowExecutionId}) ->
                {Index, get_summary(AtmWorkflowExecutionId)}
            end, AtmWorkflowExecutionEntries),

            {ok, AtmWorkflowExecutionSummaries, IsLast}
    end.


-spec create(atm_workflow_execution:creation_ctx()) ->
    {ok, atm_workflow_execution:doc()} | no_return().
create(AtmWorkflowExecutionCreationCtx) ->
    AtmWorkflowExecutionDoc = create_doc(
        AtmWorkflowExecutionCreationCtx,
        create_execution_elements(AtmWorkflowExecutionCreationCtx)
    ),
    % TODO VFS-7672 move to atm_workflow_execution_status
    atm_waiting_workflow_executions:add(AtmWorkflowExecutionDoc),

    {ok, AtmWorkflowExecutionDoc}.


-spec delete(atm_workflow_execution:id()) -> ok | no_return().
delete(AtmWorkflowExecutionId) ->
    {ok, #document{value = #atm_workflow_execution{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
        lambda_snapshot_registry = AtmLambdaSnapshotRegistry,
        store_registry = AtmStoreRegistry,
        lanes = AtmLaneExecutions
    }}} = atm_workflow_execution:get(AtmWorkflowExecutionId),

    delete_execution_elements(#execution_elements{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
        lambda_snapshot_registry = AtmLambdaSnapshotRegistry,
        store_registry = AtmStoreRegistry,
        lanes = AtmLaneExecutions
    }),

    % TODO VFS-7672 remove from links tree
    atm_workflow_execution:delete(AtmWorkflowExecutionId).


-spec get_summary(atm_workflow_execution:id() | atm_workflow_execution:doc()) ->
    atm_workflow_execution:summary().
get_summary(#document{key = AtmWorkflowExecutionId, value = AtmWorkflowExecution}) ->
    get_summary(AtmWorkflowExecutionId, AtmWorkflowExecution);
get_summary(AtmWorkflowExecutionId) ->
    {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
    get_summary(AtmWorkflowExecutionDoc).


-spec get_summary(atm_workflow_execution:id(), atm_workflow_execution:record()) ->
    atm_workflow_execution:summary().
get_summary(AtmWorkflowExecutionId, #atm_workflow_execution{
    name = Name,
    atm_inventory_id = AtmInventoryId,
    status = AtmWorkflowExecutionStatus,
    schedule_time = ScheduleTime,
    start_time = StartTime,
    finish_time = FinishTime
}) ->
    #atm_workflow_execution_summary{
        atm_workflow_execution_id = AtmWorkflowExecutionId,
        name = Name,
        atm_inventory_id = AtmInventoryId,
        status = AtmWorkflowExecutionStatus,
        schedule_time = ScheduleTime,
        start_time = StartTime,
        finish_time = FinishTime
    }.


-spec report_task_status_change(
    atm_workflow_execution:id(),
    non_neg_integer(),
    non_neg_integer(),
    atm_task_execution:id(),
    atm_task_execution:status()
) ->
    ok.
report_task_status_change(
    AtmWorkflowExecutionId,
    AtmLaneExecutionIndex,
    AtmParallelBoxExecutionIndex,
    AtmTaskExecutionId,
    NewStatus
) ->
    atm_workflow_execution_status:report_task_status_change(
        AtmWorkflowExecutionId, AtmLaneExecutionIndex, AtmParallelBoxExecutionIndex,
        AtmTaskExecutionId, NewStatus
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec list(
    od_space:id(),
    atm_workflow_execution:phase(),
    atm_workflow_executions_forest:listing_opts()
) ->
    atm_workflow_executions_forest:entries().
list(SpaceId, ?WAITING_PHASE, ListingOpts) ->
    atm_waiting_workflow_executions:list(SpaceId, ListingOpts);
list(SpaceId, ?ONGOING_PHASE, ListingOpts) ->
    atm_ongoing_workflow_executions:list(SpaceId, ListingOpts);
list(SpaceId, ?ENDED_PHASE, ListingOpts) ->
    atm_ended_workflow_executions:list(SpaceId, ListingOpts).


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
-spec create_doc(atm_workflow_execution:creation_ctx(), execution_elements()) ->
    atm_workflow_execution:doc() | no_return().
create_doc(
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
