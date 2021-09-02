%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles creation of all documents associated with automation
%%% workflow execution (e.g. task execution docs, store docs, etc.). If creation
%%% of any element fails then ones created before are deleted.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_factory).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([create/5]).


-type task_execution_registry() :: #{
    AtmTaskSchemaId :: automation:id() => atm_task_execution:doc()
}.

-record(execution_elements, {
    schema_snapshot_id = undefined :: undefined | atm_workflow_schema_snapshot:id(),
    lambda_snapshot_registry = undefined :: undefined | atm_workflow_execution:lambda_snapshot_registry(),
    workflow_audit_log = undefined :: undefined | atm_store:doc(),
    workflow_store_registry = undefined :: undefined | atm_workflow_execution:store_registry(),
    lanes = undefined :: undefined | [atm_lane_execution:record()],

    % this element is not created directly but during creation of lanes and as such
    % doesn't need to be deleted directly (it will be done when deleting lanes)
    task_store_registry :: undefined | atm_task_execution_factory:task_store_registry()
}).
-type execution_elements() :: #execution_elements{}.

-type creation_ctx() :: #atm_workflow_execution_creation_ctx{}.

-export_type([task_execution_registry/0, creation_ctx/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(
    user_ctx:ctx(),
    od_space:id(),
    od_atm_workflow_schema:id(),
    atm_workflow_execution_api:store_initial_values(),
    undefined | http_client:url()
) ->
    {atm_workflow_execution:doc(), atm_workflow_execution_env:record()} | no_return().
create(UserCtx, SpaceId, AtmWorkflowSchemaId, StoreInitialValues, CallbackUrl) ->
    AtmWorkflowExecutionCreationCtx = build_creation_ctx(
        UserCtx, SpaceId, AtmWorkflowSchemaId, StoreInitialValues, CallbackUrl
    ),

    {AtmWorkflowExecutionDoc, _} = Result = create_workflow_execution_doc(
        AtmWorkflowExecutionCreationCtx,
        create_execution_elements(AtmWorkflowExecutionCreationCtx)
    ),
    atm_waiting_workflow_executions:add(AtmWorkflowExecutionDoc),

    Result.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_creation_ctx(
    user_ctx:ctx(),
    od_space:id(),
    od_atm_workflow_schema:id(),
    atm_workflow_execution_api:store_initial_values(),
    undefined | http_client:url()
) ->
    creation_ctx() | no_return().
build_creation_ctx(UserCtx, SpaceId, AtmWorkflowSchemaId, StoreInitialValues, CallbackUrl) ->
    AtmWorkflowExecutionId = datastore_key:new(),

    SessionId = user_ctx:get_session_id(UserCtx),

    {ok, AtmWorkflowSchemaDoc = #document{value = #od_atm_workflow_schema{
        atm_lambdas = AtmLambdaIds
    }}} = atm_workflow_schema_logic:get(SessionId, AtmWorkflowSchemaId),

    AtmLambdaDocs = lists:foldl(fun(AtmLambdaId, Acc) ->
        {ok, AtmLambdaDoc} = atm_lambda_logic:get(SessionId, AtmLambdaId),
        Acc#{AtmLambdaId => AtmLambdaDoc}
    end, #{}, AtmLambdaIds),

    #atm_workflow_execution_creation_ctx{
        workflow_execution_id = AtmWorkflowExecutionId,
        workflow_execution_auth = atm_workflow_execution_auth:build(
            SpaceId, AtmWorkflowExecutionId, UserCtx
        ),
        workflow_schema_doc = AtmWorkflowSchemaDoc,
        lambda_docs = AtmLambdaDocs,
        system_audit_log_schema = #atm_store_schema{
            id = <<"system_audit_log">>,
            name = <<"system_audit_log">>,
            description = <<>>,
            type = audit_log,
            data_spec = #atm_data_spec{type = atm_object_type},
            requires_initial_value = false
        },
        store_initial_values = StoreInitialValues,
        callback_url = CallbackUrl
    }.


%% @private
-spec create_execution_elements(creation_ctx()) ->
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
        fun create_audit_log/2,
        fun create_lane_executions/2
    ]).


%% @private
-spec create_schema_snapshot(creation_ctx(), execution_elements()) ->
    execution_elements().
create_schema_snapshot(#atm_workflow_execution_creation_ctx{
    workflow_execution_id = AtmWorkflowExecutionId,
    workflow_schema_doc = AtmWorkflowSchemaDoc
}, ExecutionElements) ->
    {ok, AtmWorkflowSchemaSnapshotId} = atm_workflow_schema_snapshot:create(
        AtmWorkflowExecutionId, AtmWorkflowSchemaDoc
    ),
    ExecutionElements#execution_elements{schema_snapshot_id = AtmWorkflowSchemaSnapshotId}.


%% @private
-spec create_lambda_snapshots(creation_ctx(), execution_elements()) ->
    execution_elements().
create_lambda_snapshots(#atm_workflow_execution_creation_ctx{
    workflow_execution_id = AtmWorkflowExecutionId,
    lambda_docs = AtmLambdaDocs
}, ExecutionElements) ->
    AtmLambdaSnapshotRegistry = lists:foldl(fun(#document{key = AtmLambdaId} = AtmLambdaDoc, Acc) ->
        try
            {ok, AtmLambdaSnapshotId} = atm_lambda_snapshot:create(
                AtmWorkflowExecutionId, AtmLambdaDoc
            ),
            Acc#{AtmLambdaId => AtmLambdaSnapshotId}
        catch Type:Reason ->
            catch delete_lambda_snapshots(Acc),
            erlang:Type(Reason)
        end
    end, #{}, maps:values(AtmLambdaDocs)),

    ExecutionElements#execution_elements{lambda_snapshot_registry = AtmLambdaSnapshotRegistry}.


%% @private
-spec create_stores(creation_ctx(), execution_elements()) ->
    execution_elements().
create_stores(AtmWorkflowExecutionCreationCtx, ExecutionElements) ->
    AtmWorkflowStoreDocs = atm_store_api:create_all(AtmWorkflowExecutionCreationCtx),

    AtmWorkflowStoreRegistry = lists:foldl(fun(#document{key = AtmStoreId, value = #atm_store{
        schema_id = AtmStoreSchemaId
    }}, Acc) ->
        Acc#{AtmStoreSchemaId => AtmStoreId}
    end, #{}, AtmWorkflowStoreDocs),

    ExecutionElements#execution_elements{workflow_store_registry = AtmWorkflowStoreRegistry}.


%% @private
-spec create_audit_log(creation_ctx(), execution_elements()) ->
    execution_elements().
create_audit_log(#atm_workflow_execution_creation_ctx{
    workflow_execution_auth = AtmWorkflowExecutionAuth,
    system_audit_log_schema = AtmAuditLogSchema
}, ExecutionElements) ->
    {ok, AtmWorkflowAuditLogDoc} = atm_store_api:create(
        AtmWorkflowExecutionAuth, undefined, AtmAuditLogSchema#atm_store_schema{
            id = ?WORKFLOW_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID
        }
    ),

    ExecutionElements#execution_elements{workflow_audit_log = AtmWorkflowAuditLogDoc}.


%% @private
-spec create_lane_executions(creation_ctx(), execution_elements()) ->
    execution_elements().
create_lane_executions(#atm_workflow_execution_creation_ctx{
    workflow_schema_doc = #document{value = #od_atm_workflow_schema{
        lanes = []
    }}
}, _ExecutionElements) ->
    throw(?ERROR_ATM_WORKFLOW_EMPTY);

create_lane_executions(AtmWorkflowExecutionCreationCtx, ExecutionElements) ->
    AtmLaneExecutionsAndTaskStoreRegistries = atm_lane_execution:create_all(
        AtmWorkflowExecutionCreationCtx
    ),

    {AtmLaneExecutions, AtmWorkflowTaskStoreRegistry} = lists:foldr(fun(
        {AtmLaneExecution, AtmLaneTaskStoreRegistry},
        {AtmLaneExecutionsAcc, AtmWorkflowTaskStoreRegistryAcc}
    ) ->
        {
            [AtmLaneExecution | AtmLaneExecutionsAcc],
            maps:merge(AtmLaneTaskStoreRegistry, AtmWorkflowTaskStoreRegistryAcc)
        }
    end, {[], #{}}, AtmLaneExecutionsAndTaskStoreRegistries),

    ExecutionElements#execution_elements{
        lanes = AtmLaneExecutions,
        task_store_registry = AtmWorkflowTaskStoreRegistry
    }.


%% @private
-spec create_workflow_execution_doc(creation_ctx(), execution_elements()) ->
    {atm_workflow_execution:doc(), atm_workflow_execution_env:record()} | no_return().
create_workflow_execution_doc(
    #atm_workflow_execution_creation_ctx{
        workflow_execution_id = AtmWorkflowExecutionId,
        workflow_execution_auth = AtmWorkflowExecutionAuth,
        workflow_schema_doc = #document{value = #od_atm_workflow_schema{
            name = AtmWorkflowSchemaName,
            atm_inventory = AtmInventoryId
        }},
        callback_url = CallbackUrl
    },
    ExecutionElements = #execution_elements{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
        lambda_snapshot_registry = AtmLambdaSnapshotRegistry,
        workflow_store_registry = AtmWorkflowStoreRegistry,
        workflow_audit_log = #document{
            key = AtmWorkflowAuditLogId,
            value = #atm_store{container = AtmWorkflowAuditLogStoreContainer}
        },
        lanes = AtmLaneExecutions,
        task_store_registry = AtmTaskStoreRegistry
    }
) ->
    SpaceId = atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),

    try
        {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:create(#document{
            key = AtmWorkflowExecutionId,
            value = #atm_workflow_execution{
                user_id = atm_workflow_execution_auth:get_user_id(AtmWorkflowExecutionAuth),
                space_id = SpaceId,
                atm_inventory_id = AtmInventoryId,

                name = AtmWorkflowSchemaName,
                schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
                lambda_snapshot_registry = AtmLambdaSnapshotRegistry,

                store_registry = AtmWorkflowStoreRegistry,
                system_audit_log_id = AtmWorkflowAuditLogId,
                lanes = AtmLaneExecutions,

                status = ?SCHEDULED_STATUS,
                prev_status = ?SCHEDULED_STATUS,

                callback = CallbackUrl,

                schedule_time = global_clock:timestamp_seconds(),
                start_time = 0,
                finish_time = 0
            }
        }),
        AtmWorkflowExecutionEnv = atm_workflow_execution_env:build(
            SpaceId, AtmWorkflowExecutionId,
            AtmWorkflowStoreRegistry, AtmWorkflowAuditLogStoreContainer,
            AtmTaskStoreRegistry
        ),
        {AtmWorkflowExecutionDoc, AtmWorkflowExecutionEnv}
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
    catch delete_lambda_snapshots(AtmLambdaSnapshotRegistry),

    delete_execution_elements(ExecutionElements#execution_elements{
        lambda_snapshot_registry = undefined
    });

delete_execution_elements(#execution_elements{
    workflow_store_registry = AtmWorkflowStoreRegistry
} = ExecutionElements) when AtmWorkflowStoreRegistry /= undefined ->
    catch atm_store_api:delete_all(maps:values(AtmWorkflowStoreRegistry)),

    delete_execution_elements(ExecutionElements#execution_elements{
        workflow_store_registry = undefined
    });

delete_execution_elements(#execution_elements{
    workflow_audit_log = AtmWorkflowAuditLogDoc
} = ExecutionElements) when AtmWorkflowAuditLogDoc /= undefined ->
    catch atm_store_api:delete(AtmWorkflowAuditLogDoc#document.key),

    delete_execution_elements(ExecutionElements#execution_elements{
        workflow_audit_log = undefined
    });

delete_execution_elements(#execution_elements{
    lanes = AtmLaneExecutions
} = ExecutionElements) when AtmLaneExecutions /= undefined ->
    catch atm_lane_execution:delete_all(AtmLaneExecutions),

    delete_execution_elements(ExecutionElements#execution_elements{lanes = undefined});

delete_execution_elements(_) ->
    ok.


%% @private
-spec delete_lambda_snapshots(atm_workflow_execution:lambda_snapshot_registry()) -> ok.
delete_lambda_snapshots(AtmLambdaSnapshotRegistry) ->
    lists:foreach(fun atm_lambda_snapshot:delete/1, maps:values(AtmLambdaSnapshotRegistry)).
