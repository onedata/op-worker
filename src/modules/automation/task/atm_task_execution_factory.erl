%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles creation of all documents associated with automation
%%% task execution (e.g. system audit log, task execution doc, etc.). If creation
%%% of any element fails then ones created before are deleted.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_factory).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    create_all/4, create/4,
    delete_all/1, delete/1
]).

-type task_store_registry() :: #{atm_task_execution:id() => atm_store_container:record()}.

-export_type([task_store_registry/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_all(
    atm_workflow_execution_factory:creation_ctx(),
    non_neg_integer(),
    non_neg_integer(),
    [atm_task_schema:record()]
) ->
    [{atm_task_execution:doc(), atm_store_container:record()}] | no_return().
create_all(AtmWorkflowExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, AtmTaskSchemas) ->
    lists:reverse(lists:foldl(fun(#atm_task_schema{id = AtmTaskSchemaId} = AtmTaskSchema, Acc) ->
        try
            Result = create(
                AtmWorkflowExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, AtmTaskSchema
            ),
            [Result | Acc]
        catch _:Reason ->
            catch delete_all([Doc#document.key || Doc <- Acc]),
            throw(?ERROR_ATM_TASK_EXECUTION_CREATION_FAILED(AtmTaskSchemaId, Reason))
        end
    end, [], AtmTaskSchemas)).


-spec create(
    atm_workflow_execution_factory:creation_ctx(),
    non_neg_integer(),
    non_neg_integer(),
    atm_task_schema:record()
) ->
    {atm_task_execution:doc(), atm_store_container:record()} | no_return().
create(AtmWorkflowExecutionCreationCtx, AtmLaneIndex, AtmParallelBoxIndex, AtmTaskSchema) ->
    AtmWorkflowExecutionId = get_workflow_execution_id(AtmWorkflowExecutionCreationCtx),

    #document{
        key = AtmTaskAuditLogId,
        value = #atm_store{
            container = AtmTaskAuditLogStoreContainer
        }
    } = create_audit_log(AtmWorkflowExecutionCreationCtx),

    try
        AtmLambdaDoc = #document{value = AtmLambda} = get_lambda_doc(
            AtmWorkflowExecutionCreationCtx, AtmTaskSchema
        ),
        {ok, AtmTaskExecutionDoc} = atm_task_execution:create(#atm_task_execution{
            workflow_execution_id = AtmWorkflowExecutionId,
            lane_index = AtmLaneIndex,
            parallel_box_index = AtmParallelBoxIndex,

            schema_id = AtmTaskSchema#atm_task_schema.id,

            executor = atm_task_executor:create(AtmWorkflowExecutionId, AtmLambdaDoc),
            argument_specs = build_argument_specs(AtmLambda, AtmTaskSchema),
            result_specs = build_result_specs(AtmLambda, AtmTaskSchema),

            system_audit_log_id = AtmTaskAuditLogId,

            status = ?PENDING_STATUS,

            items_in_processing = 0,
            items_processed = 0,
            items_failed = 0
        }),
        {AtmTaskExecutionDoc, AtmTaskAuditLogStoreContainer}
    catch Type:Reason ->
        delete_audit_log(AtmTaskAuditLogId),
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
-spec create_audit_log(atm_workflow_execution_factory:creation_ctx()) ->
    atm_store:doc() | no_return().
create_audit_log(#atm_workflow_execution_creation_ctx{
    workflow_execution_auth = AtmWorkflowExecutionAuth,
    system_audit_log_schema = AtmSystemAuditLogSchema
}) ->
    {ok, AtmSystemAuditLogDoc} = atm_store_api:create(
        AtmWorkflowExecutionAuth, undefined, AtmSystemAuditLogSchema#atm_store_schema{
            id = ?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID
        }
    ),
    AtmSystemAuditLogDoc.


%% @private
-spec delete_audit_log(atm_store:id()) -> ok.
delete_audit_log(AtmSystemAuditLogId) ->
    catch atm_store_api:delete(AtmSystemAuditLogId),
    ok.


%% @private
-spec get_workflow_execution_id(atm_workflow_execution_factory:creation_ctx()) ->
    atm_workflow_execution:id().
get_workflow_execution_id(#atm_workflow_execution_creation_ctx{
    workflow_execution_auth = AtmWorkflowExecutionAuth
}) ->
    atm_workflow_execution_auth:get_workflow_execution_id(AtmWorkflowExecutionAuth).


%% @private
-spec get_lambda_doc(atm_workflow_execution_factory:creation_ctx(), atm_task_schema:record()) ->
    od_atm_lambda:doc().
get_lambda_doc(
    #atm_workflow_execution_creation_ctx{lambda_docs = AtmLambdaDocs},
    #atm_task_schema{lambda_id = AtmLambdaId}
) ->
    maps:get(AtmLambdaId, AtmLambdaDocs).


%% @private
-spec build_argument_specs(od_atm_lambda:record(), atm_task_schema:record()) ->
    [atm_task_execution_argument_spec:record()] | no_return().
build_argument_specs(
    #od_atm_lambda{argument_specs = AtmLambdaArgSpecs},
    #atm_task_schema{argument_mappings = AtmTaskSchemaArgMappers}
) ->
    atm_task_execution_arguments:build_specs(AtmLambdaArgSpecs, AtmTaskSchemaArgMappers).


%% @private
-spec build_result_specs(od_atm_lambda:record(), atm_task_schema:record()) ->
    [atm_task_execution_result_spec:record()] | no_return().
build_result_specs(
    #od_atm_lambda{result_specs = AtmLambdaResultSpecs},
    #atm_task_schema{result_mappings = AtmTaskSchemaResultMappers}
) ->
    atm_task_execution_results:build_specs(AtmLambdaResultSpecs, AtmTaskSchemaResultMappers).
