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
    create_all/1, create/3,
    delete_all/1, delete/1
]).


-record(creation_args, {
    parallel_box_execution_creation_args :: atm_parallel_box_execution:creation_args(),
    task_index :: non_neg_integer(),
    task_schema :: atm_task_schema:record(),
    lambda_revision :: atm_lambda_revision:record()
}).
-type creation_args() :: #creation_args{}.

-record(execution_components, {
    executor = undefined :: undefined | atm_task_executor:record(),
    system_audit_log_store_id = undefined :: undefined | atm_store:id(),
    time_series_store_id = undefined :: undefined | atm_store:id()
}).
-type execution_components() :: #execution_components{}.

-record(creation_ctx, {
    task_id :: atm_task_execution:id(),
    logger :: atm_workflow_execution_logger:record(),
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
    lists:foldl(fun({AtmTaskSchemaIndex, AtmTaskSchema}, AtmTaskExecutionDocs) ->
        try
            AtmTaskExecutionDoc = create(
                AtmParallelBoxExecutionCreationArgs, AtmTaskSchemaIndex, AtmTaskSchema
            ),
            [AtmTaskExecutionDoc | AtmTaskExecutionDocs]
        catch Type:Reason:Stacktrace ->
            catch delete_all([Doc#document.key || Doc <- AtmTaskExecutionDocs]),

            AtmTaskSchemaId = AtmTaskSchema#atm_task_schema.id,
            Error = ?examine_exception(Type, Reason, Stacktrace),
            throw(?ERROR_ATM_TASK_EXECUTION_CREATION_FAILED(AtmTaskSchemaId, Error))
        end
    end, [], lists:enumerate(1, AtmTaskSchemas)).


-spec create(atm_parallel_box_execution:creation_args(), non_neg_integer(), atm_task_schema:record()) ->
    atm_task_execution:doc().
create(AtmParallelBoxExecutionCreationArgs, AtmTaskSchemaIndex, AtmTaskSchema) ->
    AtmTaskExecutionId = datastore_key:new(),

    #atm_parallel_box_execution_creation_args{
        lane_execution_run_creation_args = #atm_lane_execution_run_creation_args{
            workflow_execution_ctx = AtmWorkflowExecutionCtx
        }
    } = AtmParallelBoxExecutionCreationArgs,

    CreationCtx = create_execution_components(#creation_ctx{
        task_id = AtmTaskExecutionId,
        logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
        creation_args = #creation_args{
            parallel_box_execution_creation_args = AtmParallelBoxExecutionCreationArgs,
            task_index = AtmTaskSchemaIndex,
            task_schema = AtmTaskSchema,
            lambda_revision = get_lambda_revision(AtmTaskSchema, AtmParallelBoxExecutionCreationArgs)
        },
        execution_components = #execution_components{}
    }),

    try
        create_task_execution_doc(CreationCtx)
    catch Type:Reason:Stacktrace ->
        delete_execution_components(CreationCtx#creation_ctx.execution_components),
        throw(?examine_exception(Type, Reason, Stacktrace))
    end.


-spec delete_all([atm_task_execution:id()]) -> ok.
delete_all(AtmTaskExecutionIds) ->
    lists:foreach(fun delete/1, AtmTaskExecutionIds).


-spec delete(atm_task_execution:id()) -> ok.
delete(AtmTaskExecutionId) ->
    case atm_task_execution:get(AtmTaskExecutionId) of
        {ok, #document{value = #atm_task_execution{
            executor = Executor,
            system_audit_log_store_id = AtmSystemAuditLogStoreId,
            time_series_store_id = AtmTaskTSStoreId
        }}} ->
            delete_execution_components(#execution_components{
                executor = Executor,
                system_audit_log_store_id = AtmSystemAuditLogStoreId,
                time_series_store_id = AtmTaskTSStoreId
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
            throw(?examine_exception(Type, Reason, Stacktrace))
        end
    end, CreationCtx, [
        fun create_executor/1,
        fun create_audit_log/1,
        fun create_time_series_store/1
    ]).


%% @private
-spec create_executor(creation_ctx()) -> creation_ctx().
create_executor(CreationCtx = #creation_ctx{
    task_id = AtmTaskExecutionId,
    logger = Logger,
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
    AtmTaskExecutor = atm_task_executor:create(#atm_task_executor_creation_args{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        lane_execution_index = AtmLaneIndex,
        task_id = AtmTaskExecutionId,
        task_schema = AtmTaskSchema,
        lambda_revision = AtmLambdaRevision
    }),

    ?atm_workflow_debug(Logger, #atm_workflow_log_schema{
        selector = get_task_selector(CreationCtx),
        description = <<"Executor created.">>
    }),

    CreationCtx#creation_ctx{execution_components = ExecutionComponents#execution_components{
        executor = AtmTaskExecutor
    }}.


%% @private
-spec create_audit_log(creation_ctx()) -> creation_ctx().
create_audit_log(CreationCtx = #creation_ctx{
    logger = Logger,
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

    {ok, #document{key = AtmSystemAuditLogStoreId}} = atm_store_api:create(
        AtmWorkflowExecutionAuth,
        atm_workflow_execution_ctx:get_log_level_int(AtmWorkflowExecutionCtx),
        undefined,
        ?ATM_SYSTEM_AUDIT_LOG_STORE_SCHEMA(?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID)
    ),

    ?atm_workflow_debug(Logger, #atm_workflow_log_schema{
        selector = get_task_selector(CreationCtx),
        description = <<"Audit log created.">>,
        details = #{<<"auditLogStoreId">> => AtmSystemAuditLogStoreId}
    }),

    CreationCtx#creation_ctx{execution_components = ExecutionComponents#execution_components{
        system_audit_log_store_id = AtmSystemAuditLogStoreId
    }}.


%% @private
-spec create_time_series_store(creation_ctx()) -> creation_ctx().
create_time_series_store(CreationCtx = #creation_ctx{creation_args = #creation_args{
    task_schema = #atm_task_schema{time_series_store_config = undefined}
}}) ->
    CreationCtx;

%% Copy ts store in case of retry
create_time_series_store(CreationCtx = #creation_ctx{
    logger = Logger,
    creation_args = #creation_args{
        task_schema = #atm_task_schema{id = AtmTaskSchemaId},
        parallel_box_execution_creation_args = #atm_parallel_box_execution_creation_args{
            lane_execution_run_creation_args = #atm_lane_execution_run_creation_args{
                type = retry,
                origin_run = #atm_lane_execution_run{parallel_boxes = OriginAtmParallelBoxExecutions}
            },
            parallel_box_index = AtmParallelBoxIndex
        }
    },
    execution_components = ExecutionComponents
}) ->
    OriginAtmTaskExecutionId = atm_parallel_box_execution:get_task_id(
        AtmTaskSchemaId,
        lists:nth(AtmParallelBoxIndex, OriginAtmParallelBoxExecutions)
    ),
    {ok, #document{value = OriginAtmTaskExecution}} = atm_task_execution:get(OriginAtmTaskExecutionId),

    OriginAtmTaskTSStoreId = OriginAtmTaskExecution#atm_task_execution.time_series_store_id,
    #document{key = AtmTaskTSStoreId} = atm_store_api:copy(OriginAtmTaskTSStoreId, false),

    ?atm_workflow_debug(Logger, #atm_workflow_log_schema{
        selector = get_task_selector(CreationCtx),
        description = <<"Time series store copied from origin run.">>,
        details = #{<<"timeSeriesStoreId">> => AtmTaskTSStoreId}
    }),

    CreationCtx#creation_ctx{execution_components = ExecutionComponents#execution_components{
        time_series_store_id = AtmTaskTSStoreId
    }};

%% Otherwise create new ts store instance
create_time_series_store(CreationCtx = #creation_ctx{
    logger = Logger,
    creation_args = #creation_args{
        task_schema = #atm_task_schema{
            time_series_store_config = AtmTaskTSStoreConfig
        },
        parallel_box_execution_creation_args = #atm_parallel_box_execution_creation_args{
            lane_execution_run_creation_args = #atm_lane_execution_run_creation_args{
                workflow_execution_ctx = AtmWorkflowExecutionCtx
            }
        }
    },
    execution_components = ExecutionComponents
}) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx),

    {ok, #document{key = AtmTaskTSStoreId}} = atm_store_api:create(
        AtmWorkflowExecutionAuth,
        atm_workflow_execution_ctx:get_log_level_int(AtmWorkflowExecutionCtx),
        undefined,
        ?ATM_TASK_TIME_SERIES_STORE_SCHEMA(AtmTaskTSStoreConfig)
    ),

    ?atm_workflow_debug(Logger, #atm_workflow_log_schema{
        selector = get_task_selector(CreationCtx),
        description = <<"Time series store created.">>,
        details = #{<<"timeSeriesStoreId">> => AtmTaskTSStoreId}
    }),

    CreationCtx#creation_ctx{execution_components = ExecutionComponents#execution_components{
        time_series_store_id = AtmTaskTSStoreId
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
    system_audit_log_store_id = AtmTaskAuditLogStoreId
}) when AtmTaskAuditLogStoreId /= undefined ->
    catch atm_store_api:delete(AtmTaskAuditLogStoreId),

    delete_execution_components(ExecutionComponents#execution_components{
        system_audit_log_store_id = undefined
    });

delete_execution_components(ExecutionComponents = #execution_components{
    time_series_store_id = AtmTaskTSStoreId
}) when AtmTaskTSStoreId /= undefined ->
    catch atm_store_api:delete(AtmTaskTSStoreId),

    delete_execution_components(ExecutionComponents#execution_components{
        time_series_store_id = undefined
    });

delete_execution_components(_) ->
    ok.


%% @private
-spec create_task_execution_doc(creation_ctx()) -> atm_task_execution:doc().
create_task_execution_doc(CreationCtx = #creation_ctx{
    logger = Logger,
    task_id = AtmTaskExecutionId,
    creation_args = #creation_args{
        parallel_box_execution_creation_args = #atm_parallel_box_execution_creation_args{
            parallel_box_index = AtmParallelBoxIndex,
            lane_execution_run_creation_args = #atm_lane_execution_run_creation_args{
                workflow_execution_ctx = AtmWorkflowExecutionCtx,
                lane_index = AtmLaneIndex
            }
        },
        lambda_revision = #atm_lambda_revision{
            config_parameter_specs = AtmLambdaConfigParameterSpecs,
            argument_specs = AtmLambdaArgSpecs,
            result_specs = AtmLambdaResultSpecs
        },
        task_schema = #atm_task_schema{
            id = AtmTaskSchemaId,
            lambda_config = AtmLambdaConfigValues,
            argument_mappings = AtmTaskSchemaArgMappers,
            result_mappings = AtmTaskSchemaResultMappers
        }
    },
    execution_components = #execution_components{
        executor = Executor,
        system_audit_log_store_id = AtmTaskAuditLogStoreId,
        time_series_store_id = AtmTaskTSStoreIdOrUndefined
    }
}) ->
    AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
        AtmWorkflowExecutionCtx
    ),
    AtmWorkflowExecutionAuth = atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx),

    {ItemRelatedResultSpecs, UncorrelatedResultSpecs} = atm_task_execution_results:build_specs(
        AtmLambdaResultSpecs,
        AtmTaskSchemaResultMappers
    ),

    {ok, AtmTaskExecutionDoc} = atm_task_execution:create(AtmTaskExecutionId, #atm_task_execution{
        workflow_execution_id = AtmWorkflowExecutionId,
        lane_index = AtmLaneIndex,
        run_num = undefined,
        parallel_box_index = AtmParallelBoxIndex,

        schema_id = AtmTaskSchemaId,

        executor = Executor,
        lambda_execution_config_entries = atm_lambda_execution_config_entries:build_entries(
            AtmWorkflowExecutionAuth, AtmLambdaConfigParameterSpecs, AtmLambdaConfigValues
        ),
        argument_specs = atm_task_execution_arguments:build_specs(
            AtmLambdaArgSpecs,
            AtmTaskSchemaArgMappers
        ),
        item_related_result_specs = ItemRelatedResultSpecs,
        uncorrelated_result_specs = UncorrelatedResultSpecs,

        system_audit_log_store_id = AtmTaskAuditLogStoreId,
        time_series_store_id = AtmTaskTSStoreIdOrUndefined,

        status = ?PENDING_STATUS,

        items_in_processing = 0,
        items_processed = 0,
        items_failed = 0
    }),

    AtmTaskExecutionId = AtmTaskExecutionDoc#document.key,
    ?atm_workflow_debug(Logger, #atm_workflow_log_schema{
        selector = get_task_selector(CreationCtx),
        description = <<"Created.">>,
        details = #{<<"taskId">> => AtmTaskExecutionId}
    }),

    AtmTaskExecutionDoc.


%% @private
-spec get_task_selector(creation_ctx()) -> atm_workflow_execution_logger:component_selector().
get_task_selector(#creation_ctx{
    creation_args = #creation_args{
        task_index = AtmTaskSchemaIndex,
        parallel_box_execution_creation_args = #atm_parallel_box_execution_creation_args{
            parallel_box_index = AtmParallelBoxIndex,
            lane_execution_run_creation_args = #atm_lane_execution_run_creation_args{
                lane_run_selector = AtmLaneRunSelector
            }
        }
    }
}) ->
    {task, {AtmLaneRunSelector, AtmParallelBoxIndex, AtmTaskSchemaIndex}}.
