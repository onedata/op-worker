%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles creation of all documents associated with automation
%%% task execution (e.g. system audit log, task execution doc, etc.).
%%% If creation of any element fails then ones created before are deleted.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_factory).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    create_all/1, create/2,
    delete_all/1, delete/1
]).


-record(atm_task_execution_create_ctx, {
    parallel_box_execution_create_ctx :: atm_parallel_box_execution:create_ctx(),
    task_schema :: atm_task_schema:record(),
    lambda_snapshot :: atm_lambda_snapshot:record(),

    % execution elements having their own documents
    audit_log_store_doc = undefined :: undefined | atm_store:doc()
}).
-type create_ctx() :: #atm_task_execution_create_ctx{}.


%%%===================================================================
%%% API
%%%===================================================================


-spec create_all(atm_parallel_box_execution:create_ctx()) ->
    atm_parallel_box_execution:create_ctx() | no_return().
create_all(AtmParallelBoxExecutionCreateCtx = #atm_parallel_box_execution_create_ctx{
    parallel_box_schema = #atm_parallel_box_schema{tasks = AtmTaskSchemas}
}) ->
    lists:foldl(fun(
        AtmTaskSchema = #atm_task_schema{id = AtmTaskSchemaId},
        NewAtmParallelBoxExecutionCreateCtx = #atm_parallel_box_execution_create_ctx{
            record = #atm_parallel_box_execution{task_registry = AtmTaskRegistry}
        }
    ) ->
        try
            create(NewAtmParallelBoxExecutionCreateCtx, AtmTaskSchema)
        catch _:Reason ->
            catch delete_all(maps:values(AtmTaskRegistry)),
            throw(?ERROR_ATM_TASK_EXECUTION_CREATION_FAILED(AtmTaskSchemaId, Reason))
        end
    end, AtmParallelBoxExecutionCreateCtx, AtmTaskSchemas).


-spec create(atm_parallel_box_execution:create_ctx(), atm_task_schema:record()) ->
    atm_parallel_box_execution:create_ctx().
create(AtmParallelBoxExecutionCreateCtx, AtmTaskSchema = #atm_task_schema{
    lambda_id = AtmLambdaId
}) ->
    AtmTaskExecutionCreateCtx = create_execution_elements(#atm_task_execution_create_ctx{
        parallel_box_execution_create_ctx = AtmParallelBoxExecutionCreateCtx,
        task_schema = AtmTaskSchema,
        lambda_snapshot = get_lambda_snapshot(AtmLambdaId, AtmParallelBoxExecutionCreateCtx),
        audit_log_store_doc = undefined
    }),

    try
        AtmTaskExecutionDoc = create_task_execution_doc(AtmTaskExecutionCreateCtx),
        update_parallel_box_execution_create_ctx(AtmTaskExecutionCreateCtx, AtmTaskExecutionDoc)
    catch Type:Reason ->
        delete_execution_elements(AtmTaskExecutionCreateCtx),
        erlang:Type(Reason)
    end.


-spec delete_all([atm_task_execution:id()]) -> ok.
delete_all(AtmTaskExecutionIds) ->
    lists:foreach(fun delete/1, AtmTaskExecutionIds).


-spec delete(atm_task_execution:id()) -> ok.
delete(AtmTaskExecutionId) ->
    case atm_task_execution:get(AtmTaskExecutionId) of
        {ok, #document{value = #atm_task_execution{system_audit_log_id = AtmSystemAuditLogId}}} ->
            atm_store_api:delete(AtmSystemAuditLogId),
            atm_task_execution:delete(AtmTaskExecutionId);
        ?ERROR_NOT_FOUND ->
            ok
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_lambda_snapshot(automation:id(), atm_parallel_box_execution:create_ctx()) ->
    atm_lambda_snapshot:record().
get_lambda_snapshot(AtmLambdaId, #atm_parallel_box_execution_create_ctx{
    lane_execution_create_ctx = #atm_lane_execution_create_ctx{
        workflow_execution_doc = #document{value = #atm_workflow_execution{
            lambda_snapshot_registry = AtmLambdaSnapshotRegistry
        }}
    }
}) ->
    {ok, #document{value = AtmLambdaSnapshot}} = atm_lambda_snapshot:get(
        maps:get(AtmLambdaId, AtmLambdaSnapshotRegistry)
    ),
    AtmLambdaSnapshot.


%% @private
-spec create_execution_elements(create_ctx()) -> create_ctx() | no_return().
create_execution_elements(AtmTaskExecutionCreateCtx) ->
    lists:foldl(fun(CreateExecutionElementFun, NewAtmTaskExecutionCreateCtx) ->
        try
            CreateExecutionElementFun(NewAtmTaskExecutionCreateCtx)
        catch Type:Reason ->
            delete_execution_elements(NewAtmTaskExecutionCreateCtx),
            erlang:Type(Reason)
        end
    end, AtmTaskExecutionCreateCtx, [
        fun create_audit_log/1
    ]).


%% @private
-spec create_audit_log(create_ctx()) -> create_ctx().
create_audit_log(AtmTaskExecutionCreateCtx = #atm_task_execution_create_ctx{
    parallel_box_execution_create_ctx = #atm_parallel_box_execution_create_ctx{
        lane_execution_create_ctx = #atm_lane_execution_create_ctx{
            workflow_execution_ctx = AtmWorkflowExecutionCtx
        }
    }
}) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx),

    {ok, AtmSystemAuditLogDoc} = atm_store_api:create(
        AtmWorkflowExecutionAuth, undefined, #atm_store_schema{
            id = ?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID,
            name = ?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID,
            description = <<>>,
            type = audit_log,
            data_spec = #atm_data_spec{type = atm_object_type},
            requires_initial_value = false
        }
    ),

    AtmTaskExecutionCreateCtx#atm_task_execution_create_ctx{
        audit_log_store_doc = AtmSystemAuditLogDoc
    }.


%% @private
-spec delete_execution_elements(create_ctx()) -> ok.
delete_execution_elements(AtmTaskExecutionCreateCtx = #atm_task_execution_create_ctx{
    audit_log_store_doc = #document{key = AtmTaskAuditLogId}
}) ->
    catch atm_store_api:delete(AtmTaskAuditLogId),

    delete_execution_elements(AtmTaskExecutionCreateCtx#atm_task_execution_create_ctx{
        audit_log_store_doc = undefined
    });

delete_execution_elements(_) ->
    ok.


%% @private
-spec create_task_execution_doc(create_ctx()) -> atm_task_execution:doc().
create_task_execution_doc(#atm_task_execution_create_ctx{
    parallel_box_execution_create_ctx = #atm_parallel_box_execution_create_ctx{
        parallel_box_index = AtmParallelBoxIndex,
        lane_execution_create_ctx = #atm_lane_execution_create_ctx{
            workflow_execution_env = AtmWorkflowExecutionEnv,
            lane_index = AtmLaneIndex
        }
    },
    lambda_snapshot = AtmLambdaSnapshot,
    task_schema = #atm_task_schema{id = AtmTaskSchemaId} = AtmTaskSchema,

    audit_log_store_doc = #document{key = AtmTaskAuditLogId}
}) ->
    AtmWorkflowExecutionId = atm_workflow_execution_env:get_workflow_execution_id(
        AtmWorkflowExecutionEnv
    ),

    {ok, AtmTaskExecutionDoc} = atm_task_execution:create(#atm_task_execution{
        workflow_execution_id = AtmWorkflowExecutionId,
        lane_index = AtmLaneIndex,
        parallel_box_index = AtmParallelBoxIndex,

        schema_id = AtmTaskSchemaId,

        executor = atm_task_executor:build(AtmWorkflowExecutionId, AtmLaneIndex, AtmLambdaSnapshot),
        argument_specs = build_argument_specs(AtmLambdaSnapshot, AtmTaskSchema),
        result_specs = build_result_specs(AtmLambdaSnapshot, AtmTaskSchema),

        system_audit_log_id = AtmTaskAuditLogId,

        status = ?PENDING_STATUS,

        items_in_processing = 0,
        items_processed = 0,
        items_failed = 0
    }),
    AtmTaskExecutionDoc.


%% @private
-spec build_argument_specs(atm_lambda_snapshot:record(), atm_task_schema:record()) ->
    [atm_task_execution_argument_spec:record()] | no_return().
build_argument_specs(
    #atm_lambda_snapshot{argument_specs = AtmLambdaArgSpecs},
    #atm_task_schema{argument_mappings = AtmTaskSchemaArgMappers}
) ->
    atm_task_execution_arguments:build_specs(AtmLambdaArgSpecs, AtmTaskSchemaArgMappers).


%% @private
-spec build_result_specs(atm_lambda_snapshot:record(), atm_task_schema:record()) ->
    [atm_task_execution_result_spec:record()] | no_return().
build_result_specs(
    #atm_lambda_snapshot{result_specs = AtmLambdaResultSpecs},
    #atm_task_schema{result_mappings = AtmTaskSchemaResultMappers}
) ->
    atm_task_execution_results:build_specs(AtmLambdaResultSpecs, AtmTaskSchemaResultMappers).


%% @private
-spec update_parallel_box_execution_create_ctx(create_ctx(), atm_task_execution:doc()) ->
    atm_parallel_box_execution:create_ctx().
update_parallel_box_execution_create_ctx(
    #atm_task_execution_create_ctx{
        audit_log_store_doc = #document{value = #atm_store{
            container = AtmTaskStoreAuditLogContainer
        }},
        parallel_box_execution_create_ctx = #atm_parallel_box_execution_create_ctx{
            lane_execution_create_ctx = AtmLaneExecutionCreateCtx = #atm_lane_execution_create_ctx{
                workflow_execution_env = AtmWorkflowExecutionEnv
            },
            record = AtmParallelBoxExecutionRecord = #atm_parallel_box_execution{
                task_registry = AtmTaskRegistry,
                task_statuses = AtmTaskStatuses
            }
        } = AtmParallelBoxExecutionCreateCtx
    },
    #document{key = AtmTaskExecutionId, value = #atm_task_execution{
        schema_id = AtmTaskSchemaId,
        status = AtmTaskExecutionStatus
    }}
) ->
    AtmParallelBoxExecutionCreateCtx#atm_parallel_box_execution_create_ctx{
        lane_execution_create_ctx = AtmLaneExecutionCreateCtx#atm_lane_execution_create_ctx{
            workflow_execution_env = atm_workflow_execution_env:set_task_audit_log_store_container(
                AtmTaskExecutionId, AtmTaskStoreAuditLogContainer, AtmWorkflowExecutionEnv
            )
        },
        record = AtmParallelBoxExecutionRecord#atm_parallel_box_execution{
            task_registry = AtmTaskRegistry#{AtmTaskSchemaId => AtmTaskExecutionId},
            task_statuses = AtmTaskStatuses#{AtmTaskExecutionId => AtmTaskExecutionStatus}
        }
    }.
