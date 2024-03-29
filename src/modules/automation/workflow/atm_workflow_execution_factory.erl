%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles creation of all documents associated with automation
%%% workflow execution (e.g. store docs, etc.) with exception to lane execution
%%% related ones (they are created dynamically before specific lane execution
%%% starts). If creation of any component fails then ones created before are
%%% deleted.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_factory).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([create/7, delete_insecure/1]).


-record(creation_args, {
    workflow_execution_id :: atm_workflow_execution:id(),
    workflow_execution_auth :: atm_workflow_execution_auth:record(),

    workflow_schema_doc :: od_atm_workflow_schema:doc(),
    workflow_schema_revision_num :: atm_workflow_schema_revision:revision_number(),
    workflow_schema_revision :: atm_workflow_schema_revision:record(),
    lambda_docs :: [od_atm_lambda:doc()],
    store_initial_content_overlay :: atm_workflow_execution_api:store_initial_content_overlay(),
    log_level :: audit_log:entry_severity_int(),
    callback_url :: undefined | http_client:url()
}).
-type creation_args() :: #creation_args{}.

-record(execution_components, {
    schema_snapshot_id = undefined :: undefined | atm_workflow_schema_snapshot:id(),
    lambda_snapshot_registry = undefined :: undefined | atm_workflow_execution:lambda_snapshot_registry(),
    global_store_registry = undefined :: undefined | atm_workflow_execution:store_registry(),
    workflow_audit_log_store_id = undefined :: undefined | atm_store:id(),
    lanes = undefined :: undefined | #{atm_lane_execution:index() => atm_lane_execution:record()}
}).
-type execution_components() :: #execution_components{}.

-record(creation_ctx, {
    workflow_execution_env :: atm_workflow_execution_env:record(),
    creation_args :: creation_args(),
    execution_components :: execution_components()
}).
-type creation_ctx() :: #creation_ctx{}.


%%%===================================================================
%%% API
%%%===================================================================


-spec create(
    user_ctx:ctx(),
    od_space:id(),
    od_atm_workflow_schema:id(),
    atm_workflow_schema_revision:revision_number(),
    atm_workflow_execution_api:store_initial_content_overlay(),
    audit_log:entry_severity_int(),
    undefined | http_client:url()
) ->
    {atm_workflow_execution:doc(), atm_workflow_execution_env:record()} | no_return().
create(
    UserCtx,
    SpaceId,
    AtmWorkflowSchemaId,
    AtmWorkflowSchemaRevisionNum,
    AtmStoreInitialContentOverlay,
    LogLevel,
    CallbackUrl
) ->
    SessionId = user_ctx:get_session_id(UserCtx),

    {ok, #document{value = AtmWorkflowSchema} = AtmWorkflowSchemaDoc} = atm_workflow_schema_logic:get(
        SessionId, AtmWorkflowSchemaId
    ),
    AtmWorkflowSchema#od_atm_workflow_schema.compatible orelse throw(?ERROR_NOT_SUPPORTED),

    {ok, AtmWorkflowSchemaRevision} = atm_workflow_schema_logic:get_revision(
        AtmWorkflowSchemaRevisionNum, AtmWorkflowSchemaDoc
    ),
    atm_workflow_schema_logic:assert_executable_revision(AtmWorkflowSchemaRevision),

    AtmLambdaDocs = fetch_executable_lambdas_with_referenced_revisions(
        SessionId, AtmWorkflowSchemaRevision
    ),

    AtmWorkflowExecutionId = datastore_key:new(),

    CreationCtx = #creation_ctx{
        workflow_execution_env = AtmWorkflowExecutionEnv,
        execution_components = ExecutionComponents
    } = create_execution_components(#creation_ctx{
        workflow_execution_env = atm_workflow_execution_env:build(
            SpaceId, AtmWorkflowExecutionId, 0, LogLevel
        ),
        creation_args = #creation_args{
            workflow_execution_id = AtmWorkflowExecutionId,
            workflow_execution_auth = atm_workflow_execution_auth:build(
                SpaceId, AtmWorkflowExecutionId, UserCtx
            ),
            workflow_schema_doc = AtmWorkflowSchemaDoc,
            workflow_schema_revision_num = AtmWorkflowSchemaRevisionNum,
            workflow_schema_revision = AtmWorkflowSchemaRevision,
            lambda_docs = AtmLambdaDocs,
            store_initial_content_overlay = AtmStoreInitialContentOverlay,
            log_level = LogLevel,
            callback_url = CallbackUrl
        },
        execution_components = #execution_components{global_store_registry = #{}}
    }),

    AtmWorkflowExecutionDoc = try
        create_workflow_execution_doc(CreationCtx)
    catch Type:Reason:Stacktrace ->
        delete_execution_components(ExecutionComponents),
        throw(?examine_exception(Type, Reason, Stacktrace))
    end,
    atm_waiting_workflow_executions:add(AtmWorkflowExecutionDoc),

    {AtmWorkflowExecutionDoc, AtmWorkflowExecutionEnv}.


%%--------------------------------------------------------------------
%% @doc
%% Deletes atm workflow execution doc and all docs associated with it.
%%
%%                              !!! Caution !!!
%% This operation simply deletes workflow execution and all it's components
%% regardless of atm workflow execution status and without blocking other
%% operations. Such protections should be enforced (if necessary) by higher
%% layers before calling this function.
%% @end
%%--------------------------------------------------------------------
-spec delete_insecure(atm_workflow_execution:id()) -> ok.
delete_insecure(AtmWorkflowExecutionId) ->
    {ok, AtmWorkflowExecutionDoc = #document{
        value = AtmWorkflowExecution = #atm_workflow_execution{
            discarded = IsDiscarded,
            schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
            lambda_snapshot_registry = AtmLambdaSnapshotRegistry,
            store_registry = AtmGlobalStoreRegistry,
            system_audit_log_store_id = AtmWorkflowAuditLogStoreId,
            lanes = AtmLaneExecutions
        }
    }} = atm_workflow_execution:get(AtmWorkflowExecutionId, include_discarded),

    delete_execution_components(#execution_components{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
        lambda_snapshot_registry = AtmLambdaSnapshotRegistry,
        global_store_registry = AtmGlobalStoreRegistry,
        workflow_audit_log_store_id = AtmWorkflowAuditLogStoreId,
        lanes = AtmLaneExecutions
    }),
    atm_workflow_execution:delete(AtmWorkflowExecutionId),

    IsDiscarded andalso atm_discarded_workflow_executions:delete(AtmWorkflowExecutionId),

    case atm_workflow_execution_status:infer_phase(AtmWorkflowExecution) of
        ?WAITING_PHASE -> atm_waiting_workflow_executions:delete(AtmWorkflowExecutionDoc);
        ?ONGOING_PHASE -> atm_ongoing_workflow_executions:delete(AtmWorkflowExecutionDoc);
        ?SUSPENDED_PHASE -> atm_suspended_workflow_executions:delete(AtmWorkflowExecutionDoc);
        ?ENDED_PHASE -> atm_ended_workflow_executions:delete(AtmWorkflowExecutionDoc)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec fetch_executable_lambdas_with_referenced_revisions(
    session:id(),
    atm_workflow_schema_revision:record()
) ->
    [od_atm_lambda:doc()] | no_return().
fetch_executable_lambdas_with_referenced_revisions(SessionId, AtmWorkflowSchemaRevision) ->
    AtmLambdaReferences = atm_workflow_schema_revision:extract_atm_lambda_references(
        AtmWorkflowSchemaRevision
    ),

    lists:map(fun({AtmLambdaId, AtmLambdaRevisionNums}) ->
        {ok, AtmLambdaDoc = #document{
            value = AtmLambda = #od_atm_lambda{
                revision_registry = AtmLambdaRevisionRegistry,
                compatible = IsCompatible
            }
        }} = atm_lambda_logic:get(SessionId, AtmLambdaId),

        IsCompatible orelse throw(?ERROR_NOT_SUPPORTED),

        AtmLambdaWithReferencedRevisions = AtmLambda#od_atm_lambda{
            revision_registry = atm_lambda_revision_registry:with(
                AtmLambdaRevisionNums, AtmLambdaRevisionRegistry
            )
        },
        atm_lambda_logic:assert_executable_revisions(
            AtmLambdaRevisionNums, AtmLambdaWithReferencedRevisions
        ),
        AtmLambdaDoc#document{value = AtmLambdaWithReferencedRevisions}
    end, maps:to_list(AtmLambdaReferences)).


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
        fun create_workflow_schema_snapshot/1,
        fun create_lambda_snapshots/1,
        fun create_global_stores/1,
        fun create_workflow_audit_log/1,
        fun create_lane_executions/1
    ]).


%% @private
-spec create_workflow_schema_snapshot(creation_ctx()) -> creation_ctx().
create_workflow_schema_snapshot(CreationCtx = #creation_ctx{
    creation_args = #creation_args{
        workflow_execution_id = AtmWorkflowExecutionId,
        workflow_schema_doc = AtmWorkflowSchemaDoc,
        workflow_schema_revision_num = AtmWorkflowSchemaRevisionNum
    },
    execution_components = ExecutionComponents
}) ->
    {ok, AtmWorkflowSchemaSnapshotId} = atm_workflow_schema_snapshot:create(
        AtmWorkflowExecutionId, AtmWorkflowSchemaRevisionNum, AtmWorkflowSchemaDoc
    ),

    CreationCtx#creation_ctx{execution_components = ExecutionComponents#execution_components{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId
    }}.


%% @private
-spec create_lambda_snapshots(creation_ctx()) -> creation_ctx().
create_lambda_snapshots(CreationCtx = #creation_ctx{
    creation_args = #creation_args{
        workflow_execution_id = AtmWorkflowExecutionId,
        lambda_docs = AtmLambdaDocs
    },
    execution_components = ExecutionComponents
}) ->
    AtmLambdaSnapshotRegistry = lists:foldl(fun(#document{key = AtmLambdaId} = AtmLambdaDoc, Acc) ->
        try
            {ok, AtmLambdaSnapshotId} = atm_lambda_snapshot:create(
                AtmWorkflowExecutionId, AtmLambdaDoc
            ),
            Acc#{AtmLambdaId => AtmLambdaSnapshotId}
        catch Type:Reason:Stacktrace ->
            catch delete_lambda_snapshots(Acc),
            throw(?examine_exception(Type, Reason, Stacktrace))
        end
    end, #{}, AtmLambdaDocs),

    CreationCtx#creation_ctx{execution_components = ExecutionComponents#execution_components{
        lambda_snapshot_registry = AtmLambdaSnapshotRegistry
    }}.


%% @private
-spec create_global_stores(creation_ctx()) -> creation_ctx() | no_return().
create_global_stores(CreationCtx = #creation_ctx{
    creation_args = #creation_args{
        workflow_execution_auth = AtmWorkflowExecutionAuth,
        workflow_schema_revision = #atm_workflow_schema_revision{
            stores = AtmStoreSchemas
        },
        store_initial_content_overlay = AtmStoreInitialContentOverlay,
        log_level = LogLevel
    }
}) ->
    lists:foldl(fun(
        AtmStoreSchema = #atm_store_schema{id = AtmStoreSchemaId},
        NewCreationCtx = #creation_ctx{
            workflow_execution_env = AtmWorkflowExecutionEnv,
            execution_components = ExecutionComponents = #execution_components{
                global_store_registry = AtmGlobalStoreRegistry
            }
        }
    ) ->
        StoreInitialContent = utils:null_to_undefined(maps:get(
            AtmStoreSchemaId, AtmStoreInitialContentOverlay, undefined
        )),
        try
            {ok, #document{key = AtmStoreId}} = atm_store_api:create(
                AtmWorkflowExecutionAuth, LogLevel, StoreInitialContent, AtmStoreSchema
            ),
            NewCreationCtx#creation_ctx{
                workflow_execution_env = atm_workflow_execution_env:add_global_store_mapping(
                    AtmStoreSchemaId, AtmStoreId, AtmWorkflowExecutionEnv
                ),
                execution_components = ExecutionComponents#execution_components{
                    global_store_registry = AtmGlobalStoreRegistry#{
                        AtmStoreSchemaId => AtmStoreId
                    }
                }
            }
        catch Type:Reason:Stacktrace ->
            catch delete_stores(maps:values(AtmGlobalStoreRegistry)),

            Error = ?examine_exception(Type, Reason, Stacktrace),
            throw(?ERROR_ATM_STORE_CREATION_FAILED(AtmStoreSchemaId, Error))
        end
    end, CreationCtx, AtmStoreSchemas).


%% @private
-spec create_workflow_audit_log(creation_ctx()) -> creation_ctx().
create_workflow_audit_log(CreationCtx = #creation_ctx{
    workflow_execution_env = AtmWorkflowExecutionEnv,
    creation_args = #creation_args{
        workflow_execution_auth = AtmWorkflowExecutionAuth,
        log_level = LogLevel
    },
    execution_components = ExecutionComponents
}) ->
    {ok, #document{
        key = AtmWorkflowAuditLogStoreId,
        value = #atm_store{
            container = AtmWorkflowAuditLogStoreContainer
        }
    }} = atm_store_api:create(
        AtmWorkflowExecutionAuth,
        LogLevel,
        undefined,
        ?ATM_SYSTEM_AUDIT_LOG_STORE_SCHEMA(?WORKFLOW_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID)
    ),

    CreationCtx#creation_ctx{
        workflow_execution_env = atm_workflow_execution_env:set_workflow_audit_log_store_container(
            AtmWorkflowAuditLogStoreContainer, AtmWorkflowExecutionEnv
        ),
        execution_components = ExecutionComponents#execution_components{
            workflow_audit_log_store_id = AtmWorkflowAuditLogStoreId
        }
    }.


%% @private
-spec create_lane_executions(creation_ctx()) -> creation_ctx().
create_lane_executions(CreationCtx = #creation_ctx{
    creation_args = #creation_args{workflow_schema_revision = #atm_workflow_schema_revision{
        lanes = AtmLaneSchemas
    }},
    execution_components = ExecutionComponents
}) ->
    CreationCtx#creation_ctx{execution_components = ExecutionComponents#execution_components{
        lanes = lists:foldl(fun({AtmLaneIndex, AtmLaneSchema}, Acc) ->
            Acc#{AtmLaneIndex => create_lane_execution(AtmLaneIndex, AtmLaneSchema)}
        end, #{}, lists_utils:enumerate(AtmLaneSchemas))
    }}.


%% @private
-spec create_lane_execution(atm_lane_execution:index(), atm_lane_schema:record()) ->
    atm_lane_execution:record().
create_lane_execution(1, #atm_lane_schema{id = AtmLaneSchemaId, max_retries = MaxRetries}) ->
    #atm_lane_execution{
        schema_id = AtmLaneSchemaId,
        retries_left = MaxRetries,
        runs = [#atm_lane_execution_run{run_num = 1, status = ?SCHEDULED_STATUS}]
    };
create_lane_execution(_, #atm_lane_schema{id = AtmLaneSchemaId, max_retries = MaxRetries}) ->
    #atm_lane_execution{
        schema_id = AtmLaneSchemaId,
        retries_left = MaxRetries,
        runs = []
    }.


%% @private
-spec create_workflow_execution_doc(creation_ctx()) ->
    atm_workflow_execution:doc().
create_workflow_execution_doc(#creation_ctx{
    creation_args = #creation_args{
        workflow_execution_id = AtmWorkflowExecutionId,
        workflow_execution_auth = AtmWorkflowExecutionAuth,
        workflow_schema_doc = #document{value = #od_atm_workflow_schema{
            name = AtmWorkflowSchemaName,
            atm_inventory = AtmInventoryId
        }},
        log_level = LogLevel,
        callback_url = CallbackUrl
    },
    execution_components = #execution_components{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
        lambda_snapshot_registry = AtmLambdaSnapshotRegistry,
        global_store_registry = AtmGlobalStoreRegistry,
        workflow_audit_log_store_id = AtmWorkflowAuditLogStoreId,
        lanes = AtmLaneExecutions
    }
}) ->
    {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:create(#document{
        key = AtmWorkflowExecutionId,
        value = #atm_workflow_execution{
            user_id = atm_workflow_execution_auth:get_user_id(AtmWorkflowExecutionAuth),
            space_id = atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
            atm_inventory_id = AtmInventoryId,

            name = AtmWorkflowSchemaName,
            schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
            lambda_snapshot_registry = AtmLambdaSnapshotRegistry,

            store_registry = AtmGlobalStoreRegistry,
            system_audit_log_store_id = AtmWorkflowAuditLogStoreId,

            lanes = AtmLaneExecutions,
            lanes_count = map_size(AtmLaneExecutions),

            incarnation = 1,
            current_lane_index = 1,
            current_run_num = 1,

            status = ?SCHEDULED_STATUS,
            prev_status = ?SCHEDULED_STATUS,

            log_level = LogLevel,

            callback = CallbackUrl,

            schedule_time = global_clock:timestamp_seconds(),
            start_time = 0,
            suspend_time = 0,
            finish_time = 0
        }
    }),
    AtmWorkflowExecutionDoc.


%% @private
-spec delete_execution_components(execution_components()) -> ok.
delete_execution_components(ExecutionComponents = #execution_components{
    schema_snapshot_id = AtmWorkflowSchemaSnapshotId
}) when AtmWorkflowSchemaSnapshotId /= undefined ->
    catch atm_workflow_schema_snapshot:delete(AtmWorkflowSchemaSnapshotId),

    delete_execution_components(ExecutionComponents#execution_components{
        schema_snapshot_id = undefined
    });

delete_execution_components(ExecutionComponents = #execution_components{
    lambda_snapshot_registry = AtmLambdaSnapshotRegistry
}) when AtmLambdaSnapshotRegistry /= undefined ->
    catch delete_lambda_snapshots(AtmLambdaSnapshotRegistry),

    delete_execution_components(ExecutionComponents#execution_components{
        lambda_snapshot_registry = undefined
    });

delete_execution_components(ExecutionComponents = #execution_components{
    global_store_registry = AtmGlobalStoreRegistry
}) when AtmGlobalStoreRegistry /= undefined ->
    catch delete_stores(maps:values(AtmGlobalStoreRegistry)),

    delete_execution_components(ExecutionComponents#execution_components{
        global_store_registry = undefined
    });

delete_execution_components(ExecutionComponents = #execution_components{
    workflow_audit_log_store_id = AtmWorkflowAuditLogStoreId
}) when AtmWorkflowAuditLogStoreId /= undefined ->
    catch atm_store_api:delete(AtmWorkflowAuditLogStoreId),

    delete_execution_components(ExecutionComponents#execution_components{
        workflow_audit_log_store_id = undefined
    });

delete_execution_components(ExecutionComponents = #execution_components{
    lanes = AtmLaneExecutions
}) when AtmLaneExecutions /= undefined ->
    catch delete_lane_executions(maps:values(AtmLaneExecutions)),

    delete_execution_components(ExecutionComponents#execution_components{
        lanes = undefined
    });

delete_execution_components(_) ->
    ok.


%% @private
-spec delete_lambda_snapshots(atm_workflow_execution:lambda_snapshot_registry()) -> ok.
delete_lambda_snapshots(AtmLambdaSnapshotRegistry) ->
    lists:foreach(fun atm_lambda_snapshot:delete/1, maps:values(AtmLambdaSnapshotRegistry)).


%% @private
-spec delete_stores([atm_store:id()]) -> ok.
delete_stores(AtmStoreIds) ->
    lists:foreach(fun atm_store_api:delete/1, AtmStoreIds).


%% @private
-spec delete_lane_executions([atm_lane_execution:record()]) -> ok.
delete_lane_executions(AtmLaneExecutions) ->
    lists:foreach(fun(#atm_lane_execution{runs = Runs}) ->
        lists:foreach(fun atm_lane_execution_factory:delete_run/1, Runs)
    end, AtmLaneExecutions).
