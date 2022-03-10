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
%%% If creation of any component fails then ones created before are deleted.
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


-record(creation_args, {
    parallel_box_execution_creation_args :: atm_parallel_box_execution:creation_args(),
    task_schema :: atm_task_schema:record(),
    lambda_revision :: atm_lambda_revision:record()
}).
-type creation_args() :: #creation_args{}.

-record(execution_components, {
    executor = undefined :: undefined | atm_task_executor:record(),
    audit_log_store_id = undefined :: undefined | atm_store:id()
}).
-type execution_components() :: #execution_components{}.

-record(creation_ctx, {
    creation_args :: creation_args(),
    execution_components :: execution_components()
}).
-type creation_ctx() :: #creation_ctx{}.


%%%===================================================================
%%% API
%%%===================================================================


-spec create_all(atm_parallel_box_execution:creation_args()) ->
    [atm_task_execution:doc()] | no_return().
create_all(AtmParallelBoxExecutionCreationArgs = #atm_parallel_box_execution_creation_args{
    parallel_box_schema = #atm_parallel_box_schema{tasks = AtmTaskSchemas}
}) ->
    lists:foldl(fun(#atm_task_schema{id = AtmTaskSchemaId} = AtmTaskSchema, AtmTaskExecutionDocs) ->
        try
            [create(AtmParallelBoxExecutionCreationArgs, AtmTaskSchema) | AtmTaskExecutionDocs]
        catch Type:Reason:Stacktrace ->
            catch delete_all([Doc#document.key || Doc <- AtmTaskExecutionDocs]),

            Error = ?atm_examine_error(Type, Reason, Stacktrace),
            throw(?ERROR_ATM_TASK_EXECUTION_CREATION_FAILED(AtmTaskSchemaId, Error))
        end
    end, [], AtmTaskSchemas).


-spec create(atm_parallel_box_execution:creation_args(), atm_task_schema:record()) ->
    atm_task_execution:doc().
create(AtmParallelBoxExecutionCreationArgs, AtmTaskSchema) ->
    CreationCtx = create_execution_components(#creation_ctx{
        creation_args = #creation_args{
            parallel_box_execution_creation_args = AtmParallelBoxExecutionCreationArgs,
            task_schema = AtmTaskSchema,
            lambda_revision = get_lambda_revision(AtmTaskSchema, AtmParallelBoxExecutionCreationArgs)
        },
        execution_components = #execution_components{}
    }),

    try
        create_task_execution_doc(CreationCtx)
    catch Type:Reason:Stacktrace ->
        delete_execution_components(CreationCtx#creation_ctx.execution_components),
        throw(?atm_examine_error(Type, Reason, Stacktrace))
    end.


-spec delete_all([atm_task_execution:id()]) -> ok.
delete_all(AtmTaskExecutionIds) ->
    lists:foreach(fun delete/1, AtmTaskExecutionIds).


-spec delete(atm_task_execution:id()) -> ok.
delete(AtmTaskExecutionId) ->
    case atm_task_execution:get(AtmTaskExecutionId) of
        {ok, #document{value = #atm_task_execution{
            executor = Executor,
            system_audit_log_id = AtmSystemAuditLogId
        }}} ->
            delete_execution_components(#execution_components{
                executor = Executor,
                audit_log_store_id = AtmSystemAuditLogId
            }),
            atm_task_execution:delete(AtmTaskExecutionId);
        ?ERROR_NOT_FOUND ->
            ok
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_lambda_revision(atm_task_schema:record(), atm_parallel_box_execution:creation_args()) ->
    atm_lambda_revision:record().
get_lambda_revision(
    #atm_task_schema{
        lambda_id = AtmLambdaId,
        lambda_revision_number = AtmLambdaRevisionNum
    },
    #atm_parallel_box_execution_creation_args{
        lane_execution_run_creation_args = #atm_lane_execution_run_creation_args{
            workflow_execution_doc = #document{value = #atm_workflow_execution{
                lambda_snapshot_registry = AtmLambdaSnapshotRegistry
            }}
        }
    }
) ->
    {ok, #document{value = AtmLambdaSnapshot}} = atm_lambda_snapshot:get(
        maps:get(AtmLambdaId, AtmLambdaSnapshotRegistry)
    ),
    atm_lambda_snapshot:get_revision(AtmLambdaRevisionNum, AtmLambdaSnapshot).


%% @private
-spec create_execution_components(creation_ctx()) -> creation_ctx() | no_return().
create_execution_components(CreationCtx) ->
    lists:foldl(fun(CreateExecutionComponentFun, NewCreationCtx) ->
        try
            CreateExecutionComponentFun(NewCreationCtx)
        catch Type:Reason:Stacktrace ->
            delete_execution_components(NewCreationCtx#creation_ctx.execution_components),
            throw(?atm_examine_error(Type, Reason, Stacktrace))
        end
    end, CreationCtx, [
        fun create_executor/1,
        fun create_audit_log/1
    ]).


%% @private
-spec create_executor(creation_ctx()) -> creation_ctx().
create_executor(CreationCtx = #creation_ctx{
    creation_args = #creation_args{
        parallel_box_execution_creation_args = #atm_parallel_box_execution_creation_args{
            lane_execution_run_creation_args = #atm_lane_execution_run_creation_args{
                workflow_execution_ctx = AtmWorkflowExecutionCtx,
                lane_index = AtmLaneIndex
            }
        },
        lambda_revision = AtmLambdaRevision,
        task_schema = AtmTaskSchema
    },
    execution_components = ExecutionComponents
}) ->
    CreationCtx#creation_ctx{execution_components = ExecutionComponents#execution_components{
        executor = atm_task_executor:create(
            AtmWorkflowExecutionCtx, AtmLaneIndex, AtmTaskSchema, AtmLambdaRevision
        )
    }}.


%% @private
-spec create_audit_log(creation_ctx()) -> creation_ctx().
create_audit_log(CreationCtx = #creation_ctx{
    creation_args = #creation_args{
        parallel_box_execution_creation_args = #atm_parallel_box_execution_creation_args{
            lane_execution_run_creation_args = #atm_lane_execution_run_creation_args{
                workflow_execution_ctx = AtmWorkflowExecutionCtx
            }
        }
    },
    execution_components = ExecutionComponents
}) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx),

    {ok, #document{key = AtmSystemAuditLogId}} = atm_store_api:create(
        AtmWorkflowExecutionAuth,
        undefined,
        ?ATM_SYSTEM_AUDIT_LOG_SCHEMA(?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID)
    ),

    CreationCtx#creation_ctx{execution_components = ExecutionComponents#execution_components{
        audit_log_store_id = AtmSystemAuditLogId
    }}.


%% @private
-spec delete_execution_components(execution_components()) -> ok.
delete_execution_components(ExecutionComponents = #execution_components{
    executor = Executor
}) when Executor /= undefined ->
    catch atm_task_executor:delete(Executor),

    delete_execution_components(ExecutionComponents#execution_components{
        executor = undefined
    });

delete_execution_components(ExecutionComponents = #execution_components{
    audit_log_store_id = AtmTaskAuditLogId
}) when AtmTaskAuditLogId /= undefined ->
    catch atm_store_api:delete(AtmTaskAuditLogId),

    delete_execution_components(ExecutionComponents#execution_components{
        audit_log_store_id = undefined
    });

delete_execution_components(_) ->
    ok.


%% @private
-spec create_task_execution_doc(creation_ctx()) -> atm_task_execution:doc().
create_task_execution_doc(#creation_ctx{
    creation_args = #creation_args{
        parallel_box_execution_creation_args = #atm_parallel_box_execution_creation_args{
            parallel_box_index = AtmParallelBoxIndex,
            lane_execution_run_creation_args = #atm_lane_execution_run_creation_args{
                workflow_execution_ctx = AtmWorkflowExecutionCtx,
                lane_index = AtmLaneIndex
            }
        },
        lambda_revision = AtmLambdaRevision,
        task_schema = #atm_task_schema{id = AtmTaskSchemaId} = AtmTaskSchema
    },
    execution_components = #execution_components{
        executor = Executor,
        audit_log_store_id = AtmTaskAuditLogId
    }
}) ->
    AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
        AtmWorkflowExecutionCtx
    ),

    {ok, AtmTaskExecutionDoc} = atm_task_execution:create(#atm_task_execution{
        workflow_execution_id = AtmWorkflowExecutionId,
        lane_index = AtmLaneIndex,
        run_num = undefined,
        parallel_box_index = AtmParallelBoxIndex,

        schema_id = AtmTaskSchemaId,

        executor = Executor,
        argument_specs = build_argument_specs(AtmLambdaRevision, AtmTaskSchema),
        result_specs = build_result_specs(AtmLambdaRevision, AtmTaskSchema),

        system_audit_log_id = AtmTaskAuditLogId,

        status = ?PENDING_STATUS,

        items_in_processing = 0,
        items_processed = 0,
        items_failed = 0
    }),
    AtmTaskExecutionDoc.


%% @private
-spec build_argument_specs(atm_lambda_revision:record(), atm_task_schema:record()) ->
    [atm_task_execution_argument_spec:record()] | no_return().
build_argument_specs(
    #atm_lambda_revision{argument_specs = AtmLambdaArgSpecs},
    #atm_task_schema{argument_mappings = AtmTaskSchemaArgMappers}
) ->
    atm_task_execution_arguments:build_specs(AtmLambdaArgSpecs, AtmTaskSchemaArgMappers).


%% @private
-spec build_result_specs(atm_lambda_revision:record(), atm_task_schema:record()) ->
    [atm_task_execution_result_spec:record()] | no_return().
build_result_specs(
    #atm_lambda_revision{result_specs = AtmLambdaResultSpecs},
    #atm_task_schema{result_mappings = AtmTaskSchemaResultMappers}
) ->
    atm_task_execution_results:build_specs(AtmLambdaResultSpecs, AtmTaskSchemaResultMappers).
