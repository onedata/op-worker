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
-export([create/1, prepare/1, delete/1]).
-export([report_task_status_change/5]).


-record(components, {
    schema_snapshot_id = undefined :: undefined | atm_workflow_schema_snapshot:id(),
    store_registry = undefined :: undefined | atm_execution:store_registry(),
    lanes = undefined :: undefined | [atm_lane_execution:record()]
}).
-type components() :: #components{}.


%%%===================================================================
%%% API
%%%===================================================================


-spec create(atm_execution:creation_ctx()) ->
    {ok, atm_workflow_execution:doc()} | no_return().
create(AtmExecutionCreationCtx) ->
    AtmWorkflowExecutionComponents = create_components(AtmExecutionCreationCtx),
    AtmWorkflowExecutionDoc = create_doc(
        AtmExecutionCreationCtx, AtmWorkflowExecutionComponents
    ),
    atm_waiting_workflow_executions:add(AtmWorkflowExecutionDoc),

    {ok, AtmWorkflowExecutionDoc}.


-spec prepare(atm_workflow_execution:id()) -> ok | no_return().
prepare(AtmWorkflowExecutionId) ->
    {ok, #atm_workflow_execution{lanes = AtmLaneExecutions}} = transition_to_status(
        AtmWorkflowExecutionId, ?PREPARING_STATUS
    ),

    try
        atm_lane_execution:prepare_all(AtmLaneExecutions)
    catch Type:Reason ->
        transition_to_status(AtmWorkflowExecutionId, ?FAILED_STATUS),
        erlang:Type(Reason)
    end,
    transition_to_status(AtmWorkflowExecutionId, ?ENQUEUED_STATUS),

    ok.


-spec delete(atm_workflow_execution:id()) -> ok | no_return().
delete(AtmWorkflowExecutionId) ->
    {ok, #document{value = #atm_workflow_execution{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
        store_registry = AtmStoreRegistry,
        lanes = AtmLaneExecutions
    }}} = atm_workflow_execution:get(AtmWorkflowExecutionId),

    delete_components(#components{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
        store_registry = AtmStoreRegistry,
        lanes = AtmLaneExecutions
    }),

    atm_workflow_execution:delete(AtmWorkflowExecutionId).


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
    Diff = fun(#atm_workflow_execution{} = AtmWorkflowExecution) ->
        update_task_status(
            AtmLaneExecutionIndex, AtmParallelBoxExecutionIndex,
            AtmTaskExecutionId, NewStatus, AtmWorkflowExecution
        )
    end,
    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, #document{value = #atm_workflow_execution{status_changed = true}}} ->
            % TODO VFS-7672 change link tree
            ok;
        _ ->
            ok
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_components(atm_execution:creation_ctx()) -> components() | no_return().
create_components(AtmExecutionCreationCtx) ->
    lists:foldl(fun(CreateComponentFun, Components) ->
        try
            CreateComponentFun(AtmExecutionCreationCtx, Components)
        catch Type:Reason ->
            delete_components(Components),
            erlang:Type(Reason)
        end
    end, #components{}, [
        fun create_schema_snapshot/2,
        fun create_stores/2,
        fun create_lane_executions/2
    ]).


%% @private
-spec create_schema_snapshot(atm_execution:creation_ctx(), components()) -> components().
create_schema_snapshot(#atm_execution_creation_ctx{
    workflow_execution_id = AtmWorkflowExecutionId,
    workflow_schema_doc = AtmWorkflowSchemaDoc
}, Components) ->
    {ok, AtmWorkflowSchemaSnapshotId} = atm_workflow_schema_snapshot:create(
        AtmWorkflowExecutionId, AtmWorkflowSchemaDoc
    ),
    Components#components{schema_snapshot_id = AtmWorkflowSchemaSnapshotId}.


%% @private
-spec create_stores(atm_execution:creation_ctx(), components()) -> components().
create_stores(AtmExecutionCreationCtx, Components) ->
    AtmStoreDocs = atm_store_api:create_all(AtmExecutionCreationCtx),
    AtmStoreRegistry = lists:foldl(fun(#document{key = AtmStoreId, value = #atm_store{
        schema_id = AtmStoreSchemaId
    }}, Acc) ->
        Acc#{AtmStoreSchemaId => AtmStoreId}
    end, #{}, AtmStoreDocs),

    Components#components{store_registry = AtmStoreRegistry}.


%% @private
-spec create_lane_executions(atm_execution:creation_ctx(), components()) -> components().
create_lane_executions(AtmExecutionCreationCtx, Components) ->
    Components#components{lanes = atm_lane_execution:create_all(AtmExecutionCreationCtx)}.


%% @private
-spec create_doc(atm_execution:creation_ctx(), components()) ->
    atm_workflow_execution:doc() | no_return().
create_doc(
    #atm_execution_creation_ctx{
        space_id = SpaceId,
        workflow_execution_id = AtmWorkflowExecutionId
    },
    Components = #components{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
        store_registry = AtmStoreRegistry,
        lanes = AtmLaneExecutions
    }
) ->
    try
        {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:create(#document{
            key = AtmWorkflowExecutionId,
            value = #atm_workflow_execution{
                space_id = SpaceId,
                schema_snapshot_id = AtmWorkflowSchemaSnapshotId,

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
        delete_components(Components),
        erlang:Type(Reason)
    end.


%% @private
-spec delete_components(components()) -> ok.
delete_components(#components{schema_snapshot_id = AtmWorkflowSchemaSnapshotId} = Components) when
    AtmWorkflowSchemaSnapshotId /= undefined
->
    catch atm_workflow_schema_snapshot:delete(AtmWorkflowSchemaSnapshotId),
    delete_components(Components#components{schema_snapshot_id = undefined});

delete_components(#components{store_registry = AtmStoreRegistry} = Components) when
    AtmStoreRegistry /= undefined
->
    catch atm_store_api:delete_all(maps:values(AtmStoreRegistry)),
    delete_components(Components#components{store_registry = undefined});

delete_components(#components{lanes = AtmLaneExecutions} = Components) when
    AtmLaneExecutions /= undefined
->
    catch atm_lane_execution:delete_all(AtmLaneExecutions),
    delete_components(Components#components{lanes = undefined});

delete_components(_) ->
    ok.


%% @private
-spec transition_to_status(atm_workflow_execution:id(), atm_workflow_execution:status()) ->
    {ok, atm_workflow_execution:record()} | no_return().
transition_to_status(AtmWorkflowExecutionId, ?FAILED_STATUS) ->
    % TODO should fail here change status of all lanes, pboxes and tasks ??
    TransitFun = fun(#atm_workflow_execution{} = AtmWorkflowExecution) ->
        {ok, AtmWorkflowExecution#atm_workflow_execution{status = ?FAILED_STATUS}}
    end,
    {ok, #document{value = AtmWorkflowExecutionRecord}} = atm_workflow_execution:update(
        AtmWorkflowExecutionId, TransitFun
    ),
    {ok, AtmWorkflowExecutionRecord};

transition_to_status(AtmWorkflowExecutionId, NewStatus) ->
    Diff = fun(#atm_workflow_execution{status = Status} = AtmWorkflowExecution) ->
        case atm_execution_status:is_transition_allowed(Status, NewStatus) of
            true ->
                {ok, AtmWorkflowExecution#atm_workflow_execution{status = NewStatus}};
            false ->
                {error, Status}
        end
    end,

    case atm_workflow_execution:update(AtmWorkflowExecutionId, Diff) of
        {ok, #document{value = AtmWorkflowExecution}} ->
            {ok, AtmWorkflowExecution};
        {error, CurrStatus} ->
            throw(?ERROR_ATM_INVALID_STATUS_TRANSITION(CurrStatus, NewStatus))
    end.


%% @private
-spec update_task_status(
    non_neg_integer(),
    non_neg_integer(),
    atm_task_execution:id(),
    atm_task_execution:status(),
    atm_workflow_execution:record()
) ->
    {ok, atm_workflow_execution:record()} | {error, term()}.
update_task_status(
    AtmLaneIndex,
    AtmParallelBoxIndex,
    AtmTaskExecutionId,
    NewStatus,
    AtmWorkflowExecution = #atm_workflow_execution{
        status = CurrentStatus,
        lanes = AtmLaneExecutions
    }
) ->
    AtmLanExecution = lists:nth(AtmLaneIndex, AtmLaneExecutions),

    case atm_lane_execution:update_task_status(
        AtmParallelBoxIndex, AtmTaskExecutionId, NewStatus, AtmLanExecution
    ) of
        {ok, NewLaneExecution} ->
            NewAtmLaneExecutions = atm_execution_status:replace_at(
                NewLaneExecution, AtmLaneIndex, AtmLaneExecutions
            ),
            NewAtmWorkflowStatus = atm_execution_status:converge(
                atm_lane_execution:gather_statuses(NewAtmLaneExecutions)
            ),
            {ok, AtmWorkflowExecution#atm_workflow_execution{
                status = NewAtmWorkflowStatus,
                status_changed = NewAtmWorkflowStatus /= CurrentStatus,
                lanes = NewAtmLaneExecutions
            }};
        {error, _} = Error ->
            Error
    end.
