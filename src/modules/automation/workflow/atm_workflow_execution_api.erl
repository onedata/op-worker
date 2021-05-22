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
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    create/3, prepare/1, delete/1,
    report_task_status_change/5
]).

-record(components, {
    schema_snapshot_id = undefined :: undefined | atm_workflow_schema_snapshot:id(),
    stores = undefined :: undefined | atm_workflow_execution:store_registry(),
    lanes = undefined :: undefined | [atm_lane_execution:record()]
}).
-type components() :: #components{}.

-record(create_ctx, {
    space_id :: od_space:id(),
    workflow_execution_id :: atm_workflow_execution:id(),
    workflow_schema_doc :: od_atm_workflow_schema:doc(),
    initial_values :: atm_store_api:initial_values()
}).
-type create_ctx() :: #create_ctx{}.


%%%===================================================================
%%% API
%%%===================================================================


-spec create(od_space:id(), od_atm_workflow_schema:id(), atm_store_api:initial_values()) ->
    {ok, atm_workflow_execution:id()} | no_return().
create(SpaceId, AtmWorkflowSchemaId, InitialValues) ->
    %% TODO VFS-7671 use user session
    {ok, AtmWorkflowSchemaDoc} = atm_workflow_schema_logic:get(?ROOT_SESS_ID, AtmWorkflowSchemaId),

    AtmWorkflowExecutionId = datastore_key:new(),

    CreateCtx = #create_ctx{
        space_id = SpaceId,
        workflow_execution_id = AtmWorkflowExecutionId,
        workflow_schema_doc = AtmWorkflowSchemaDoc,
        initial_values = InitialValues
    },
    Components = create_components(CreateCtx),
    AtmWorkflowExecutionDoc = create_doc(CreateCtx, Components),

    atm_waiting_workflow_executions:add(AtmWorkflowExecutionDoc),

    {ok, AtmWorkflowExecutionId}.


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
        stores = AtmStoreRegistry,
        lanes = AtmLaneExecutions
    }}} = atm_workflow_execution:get(AtmWorkflowExecutionId),

    delete_components(#components{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
        stores = AtmStoreRegistry,
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
    AtmLaneExecutionNo,
    AtmParallelBoxExecutionNo,
    AtmTaskExecutionId,
    NewStatus
) ->
    Diff = fun(#atm_workflow_execution{} = AtmWorkflowExecution) ->
        update_task_status(
            AtmLaneExecutionNo, AtmParallelBoxExecutionNo,
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
-spec create_components(create_ctx()) -> components() | no_return().
create_components(CreateCtx) ->
    lists:foldl(fun(CreateComponentFun, Components) ->
        try
            CreateComponentFun(Components, CreateCtx)
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
-spec create_schema_snapshot(create_ctx(), components()) -> components().
create_schema_snapshot(#create_ctx{
    workflow_execution_id = AtmWorkflowExecutionId,
    workflow_schema_doc = AtmWorkflowSchemaDoc
}, Components) ->
    {ok, AtmWorkflowSchemaSnapshotId} = atm_workflow_schema_snapshot:create(
        AtmWorkflowExecutionId, AtmWorkflowSchemaDoc
    ),
    Components#components{schema_snapshot_id = AtmWorkflowSchemaSnapshotId}.


%% @private
-spec create_stores(create_ctx(), components()) -> components().
create_stores(#create_ctx{
    workflow_execution_id = AtmWorkflowExecutionId,
    workflow_schema_doc = #document{value = #od_atm_workflow_schema{
        stores = AtmStoreSchemas
    }},
    initial_values = InitialValues
}, Components) ->
    AtmStoreDocs = atm_store_api:create_all(
        AtmWorkflowExecutionId, InitialValues, AtmStoreSchemas
    ),
    AtmStoreRegistry = lists:foldl(fun(#document{key = AtmStoreId, value = #atm_store{
        schema_id = AtmStoreSchemaId
    }}, Acc) ->
        Acc#{AtmStoreSchemaId => AtmStoreId}
    end, #{}, AtmStoreDocs),

    Components#components{stores = AtmStoreRegistry}.


%% @private
-spec create_lane_executions(create_ctx(), components()) -> components().
create_lane_executions(#create_ctx{
    workflow_execution_id = AtmWorkflowExecutionId,
    workflow_schema_doc = #document{value = #od_atm_workflow_schema{lanes = AtmLaneSchemas}}
}, Components) ->
    Components#components{lanes = atm_lane_execution:create_all(
        AtmWorkflowExecutionId, AtmLaneSchemas
    )}.


%% @private
-spec create_doc(create_ctx(), components()) ->
    atm_workflow_execution:doc() | no_return().
create_doc(
    #create_ctx{
        space_id = SpaceId,
        workflow_execution_id = AtmWorkflowExecutionId
    },
    Components = #components{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
        stores = AtmStoreRegistry,
        lanes = AtmLaneExecutions
    }
) ->
    try
        {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:create(#document{
            key = AtmWorkflowExecutionId,
            value = #atm_workflow_execution{
                space_id = SpaceId,
                schema_snapshot_id = AtmWorkflowSchemaSnapshotId,

                stores = AtmStoreRegistry,
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
delete_components(#components{
    schema_snapshot_id = undefined,
    stores = undefined,
    lanes = undefined
}) ->
    ok;

delete_components(#components{schema_snapshot_id = AtmWorkflowSchemaSnapshotId} = Components) ->
    catch atm_workflow_schema_snapshot:delete(AtmWorkflowSchemaSnapshotId),
    delete_components(Components#components{schema_snapshot_id = undefined});

delete_components(#components{stores = StoreRegistry} = Components) ->
    catch atm_store_api:delete_all(maps:values(StoreRegistry)),
    delete_components(Components#components{stores = undefined});

delete_components(#components{lanes = AtmLaneExecutions} = Components) ->
    catch atm_lane_execution:delete_all(AtmLaneExecutions),
    delete_components(Components#components{lanes = undefined}).


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
        case atm_status_utils:is_transition_allowed(Status, NewStatus) of
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
    AtmLaneExecutionNo,
    AtmParallelBoxExecutionNo,
    AtmTaskExecutionId,
    NewStatus,
    AtmWorkflowExecution = #atm_workflow_execution{
        status = CurrentStatus,
        lanes = AtmLaneExecutions
    }
) ->
    AtmLanExecution = lists:nth(AtmLaneExecutionNo, AtmLaneExecutions),

    case atm_lane_execution:update_task_status(
        AtmParallelBoxExecutionNo, AtmTaskExecutionId, NewStatus, AtmLanExecution
    ) of
        {ok, NewLaneExecution} ->
            NewAtmLaneExecutions = atm_status_utils:replace_at(
                NewLaneExecution, AtmLaneExecutionNo, AtmLaneExecutions
            ),
            NewAtmWorkflowStatus = atm_status_utils:converge(
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
