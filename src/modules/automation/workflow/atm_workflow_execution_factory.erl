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
%%% starts). If creation of any element fails then ones created before are
%%% deleted.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_factory).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([create/5, delete_insecure/1]).


-record(atm_workflow_execution_create_params, {
    workflow_execution_id :: atm_workflow_execution:id(),
    workflow_execution_auth :: atm_workflow_execution_auth:record(),

    workflow_schema_doc :: od_atm_workflow_schema:doc(),
    lambda_docs :: [od_atm_lambda:doc()],
    store_initial_values :: atm_workflow_execution_api:store_initial_values(),
    callback_url :: undefined | http_client:url()
}).
-type create_params() :: #atm_workflow_execution_create_params{}.

-record(atm_workflow_execution_elements, {
    schema_snapshot_id = undefined :: undefined | atm_workflow_schema_snapshot:id(),
    lambda_snapshot_registry = undefined :: undefined | atm_workflow_execution:lambda_snapshot_registry(),
    workflow_store_registry = undefined :: undefined | atm_workflow_execution:store_registry(),
    workflow_audit_log_id = undefined :: undefined | atm_store:id(),
    lanes = undefined :: undefined | [atm_lane_execution:record()]
}).
-type elements() :: #atm_workflow_execution_elements{}.

-record(atm_workflow_execution_create_ctx, {
    env :: atm_workflow_execution_env:record(),
    params :: create_params(),
    elements :: elements()
}).
-type create_ctx() :: #atm_workflow_execution_create_ctx{}.


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
    SessionId = user_ctx:get_session_id(UserCtx),

    AtmWorkflowSchemaDoc = fetch_workflow_schema(SessionId, AtmWorkflowSchemaId),
    AtmLambdaDocs = fetch_lambdas(SessionId, AtmWorkflowSchemaDoc),

    AtmWorkflowExecutionId = datastore_key:new(),

    AtmWorkflowExecutionCreateCtx = #atm_workflow_execution_create_ctx{
        env = AtmWorkflowExecutionEnv,
        elements = Elements
    } = create_execution_elements(#atm_workflow_execution_create_ctx{
        env = atm_workflow_execution_env:build(SpaceId, AtmWorkflowExecutionId),
        params = #atm_workflow_execution_create_params{
            workflow_execution_id = AtmWorkflowExecutionId,
            workflow_execution_auth = atm_workflow_execution_auth:build(
                SpaceId, AtmWorkflowExecutionId, UserCtx
            ),
            workflow_schema_doc = AtmWorkflowSchemaDoc,
            lambda_docs = AtmLambdaDocs,
            store_initial_values = StoreInitialValues,
            callback_url = CallbackUrl
        },
        elements = #atm_workflow_execution_elements{
            workflow_store_registry = #{}
        }
    }),

    AtmWorkflowExecutionDoc = try
        create_workflow_execution_doc(AtmWorkflowExecutionCreateCtx)
    catch Type:Reason ->
        delete_execution_elements(Elements),
        erlang:Type(Reason)
    end,
    atm_waiting_workflow_executions:add(AtmWorkflowExecutionDoc),

    {AtmWorkflowExecutionDoc, AtmWorkflowExecutionEnv}.


%%--------------------------------------------------------------------
%% @doc
%% Deletes atm workflow execution doc and all docs associated with it.
%%
%%                              !!! Caution !!!
%% This operation simply deletes workflow execution and all it's elements
%% regardless of atm workflow execution status and without blocking other
%% operations. Such protections should be enforced (if necessary) by higher
%% layers before calling this function.
%% @end
%%--------------------------------------------------------------------
-spec delete_insecure(atm_workflow_execution:id()) -> ok.
delete_insecure(AtmWorkflowExecutionId) ->
    {ok, AtmWorkflowExecutionDoc = #document{
        value = AtmWorkflowExecution = #atm_workflow_execution{
            schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
            lambda_snapshot_registry = AtmLambdaSnapshotRegistry,
            store_registry = AtmWorkflowStoreRegistry,
            system_audit_log_id = AtmWorkflowAuditLogId,
            lanes = AtmLaneExecutions
        }
    }} = atm_workflow_execution:get(AtmWorkflowExecutionId),

    delete_execution_elements(#atm_workflow_execution_elements{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
        lambda_snapshot_registry = AtmLambdaSnapshotRegistry,
        workflow_store_registry = AtmWorkflowStoreRegistry,
        workflow_audit_log_id = AtmWorkflowAuditLogId,
        lanes = AtmLaneExecutions
    }),
    atm_workflow_execution:delete(AtmWorkflowExecutionId),

    case atm_workflow_execution_status:infer_phase(AtmWorkflowExecution) of
        ?WAITING_PHASE -> atm_waiting_workflow_executions:delete(AtmWorkflowExecutionDoc);
        ?ONGOING_PHASE -> atm_ongoing_workflow_executions:delete(AtmWorkflowExecutionDoc);
        ?ENDED_PHASE -> atm_ended_workflow_executions:delete(AtmWorkflowExecutionDoc)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec fetch_workflow_schema(session:id(), od_atm_workflow_schema:id()) ->
    od_atm_workflow_schema:doc() | no_return().
fetch_workflow_schema(SessionId, AtmWorkflowSchemaId) ->
    {ok, AtmWorkflowSchemaDoc} = atm_workflow_schema_logic:get(SessionId, AtmWorkflowSchemaId),
    atm_workflow_schema_logic:assert_executable(AtmWorkflowSchemaDoc),

    AtmWorkflowSchemaDoc.


%% @private
-spec fetch_lambdas(session:id(), od_atm_workflow_schema:doc()) ->
    [od_atm_lambda:doc()] | no_return().
fetch_lambdas(SessionId, #document{value = #od_atm_workflow_schema{
    atm_lambdas = AtmLambdaIds
}}) ->
    lists:map(fun(AtmLambdaId) ->
        {ok, AtmLambdaDoc} = atm_lambda_logic:get(SessionId, AtmLambdaId),
        atm_lambda_logic:assert_executable(AtmLambdaDoc),

        AtmLambdaDoc
    end, AtmLambdaIds).


%% @private
-spec create_execution_elements(create_ctx()) -> create_ctx() | no_return().
create_execution_elements(AtmWorkflowExecutionCreateCtx) ->
    lists:foldl(fun(CreateExecutionElementFun, NewAtmWorkflowExecutionCreateCtx) ->
        try
            CreateExecutionElementFun(NewAtmWorkflowExecutionCreateCtx)
        catch Type:Reason ->
            delete_execution_elements(
                NewAtmWorkflowExecutionCreateCtx#atm_workflow_execution_create_ctx.elements
            ),
            erlang:Type(Reason)
        end
    end, AtmWorkflowExecutionCreateCtx, [
        fun create_workflow_schema_snapshot/1,
        fun create_lambda_snapshots/1,
        fun create_workflow_stores/1,
        fun create_workflow_audit_log/1,
        fun create_lane_executions/1
    ]).


%% @private
-spec create_workflow_schema_snapshot(create_ctx()) -> create_ctx().
create_workflow_schema_snapshot(AtmWorkflowExecutionCreateCtx = #atm_workflow_execution_create_ctx{
    params = #atm_workflow_execution_create_params{
        workflow_execution_id = AtmWorkflowExecutionId,
        workflow_schema_doc = AtmWorkflowSchemaDoc
    },
    elements = Elements
}) ->
    {ok, AtmWorkflowSchemaSnapshotId} = atm_workflow_schema_snapshot:create(
        AtmWorkflowExecutionId, AtmWorkflowSchemaDoc
    ),

    AtmWorkflowExecutionCreateCtx#atm_workflow_execution_create_ctx{
        elements = Elements#atm_workflow_execution_elements{
            schema_snapshot_id = AtmWorkflowSchemaSnapshotId
        }
    }.


%% @private
-spec create_lambda_snapshots(create_ctx()) -> create_ctx().
create_lambda_snapshots(AtmWorkflowExecutionCreateCtx = #atm_workflow_execution_create_ctx{
    params = #atm_workflow_execution_create_params{
        workflow_execution_id = AtmWorkflowExecutionId,
        lambda_docs = AtmLambdaDocs
    },
    elements = Elements
}) ->
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
    end, #{}, AtmLambdaDocs),

    AtmWorkflowExecutionCreateCtx#atm_workflow_execution_create_ctx{
        elements = Elements#atm_workflow_execution_elements{
            lambda_snapshot_registry = AtmLambdaSnapshotRegistry
        }
    }.


%% @private
-spec create_workflow_stores(create_ctx()) -> create_ctx() | no_return().
create_workflow_stores(AtmWorkflowExecutionCreateCtx = #atm_workflow_execution_create_ctx{
    params = #atm_workflow_execution_create_params{
        workflow_execution_auth = AtmWorkflowExecutionAuth,
        workflow_schema_doc = #document{value = #od_atm_workflow_schema{
            stores = AtmStoreSchemas
        }},
        store_initial_values = AtmStoreInitialValues
    }
}) ->
    lists:foldl(fun(
        AtmStoreSchema = #atm_store_schema{id = AtmStoreSchemaId},
        NewAtmWorkflowExecutionCreateCtx = #atm_workflow_execution_create_ctx{
            env = AtmWorkflowExecutionEnv,
            elements = Elements = #atm_workflow_execution_elements{
                workflow_store_registry = AtmWorkflowStoreRegistry
            }
        }
    ) ->
        StoreInitialValue = utils:null_to_undefined(maps:get(
            AtmStoreSchemaId, AtmStoreInitialValues, undefined
        )),
        try
            {ok, #document{key = AtmStoreId}} = atm_store_api:create(
                AtmWorkflowExecutionAuth, StoreInitialValue, AtmStoreSchema
            ),
            NewAtmWorkflowExecutionCreateCtx#atm_workflow_execution_create_ctx{
                env = atm_workflow_execution_env:add_workflow_store_mapping(
                    AtmStoreSchemaId, AtmStoreId, AtmWorkflowExecutionEnv
                ),
                elements = Elements#atm_workflow_execution_elements{
                    workflow_store_registry = AtmWorkflowStoreRegistry#{
                        AtmStoreSchemaId => AtmStoreId
                    }
                }
            }
        catch _:Reason ->
            catch delete_workflow_stores(maps:values(AtmWorkflowStoreRegistry)),
            throw(?ERROR_ATM_STORE_CREATION_FAILED(AtmStoreSchemaId, Reason))
        end
    end, AtmWorkflowExecutionCreateCtx, AtmStoreSchemas).


%% @private
-spec create_workflow_audit_log(create_ctx()) -> create_ctx().
create_workflow_audit_log(AtmWorkflowExecutionCreateCtx = #atm_workflow_execution_create_ctx{
    env = AtmWorkflowExecutionEnv,
    params = #atm_workflow_execution_create_params{
        workflow_execution_auth = AtmWorkflowExecutionAuth
    },
    elements = Elements
}) ->
    {ok, #document{
        key = AtmWorkflowAuditLogId,
        value = #atm_store{container = AtmWorkflowAuditLogStoreContainer}
    }} = atm_store_api:create(
        AtmWorkflowExecutionAuth, undefined, #atm_store_schema{
            id = ?WORKFLOW_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID,
            name = ?WORKFLOW_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID,
            description = <<>>,
            type = audit_log,
            data_spec = #atm_data_spec{type = atm_object_type},
            requires_initial_value = false
        }
    ),

    AtmWorkflowExecutionCreateCtx#atm_workflow_execution_create_ctx{
        env = atm_workflow_execution_env:set_workflow_audit_log_store_container(
            AtmWorkflowAuditLogStoreContainer, AtmWorkflowExecutionEnv
        ),
        elements = Elements#atm_workflow_execution_elements{
            workflow_audit_log_id = AtmWorkflowAuditLogId
        }
    }.


%% @private
-spec create_lane_executions(create_ctx()) -> create_ctx().
create_lane_executions(AtmWorkflowExecutionCreateCtx = #atm_workflow_execution_create_ctx{
    params = #atm_workflow_execution_create_params{
        workflow_schema_doc = #document{value = #od_atm_workflow_schema{
            lanes = [FirstAtmLaneSchema | RestAtmLaneSchemas]
        }}
    },
    elements = Elements
}) ->
    FirstAtmLaneExecution = #atm_lane_execution{
        schema_id = FirstAtmLaneSchema#atm_lane_schema.id,
        runs = [#atm_lane_execution_run{run_no = 1, status = ?SCHEDULED_STATUS}]
    },
    RestAtmLaneExecutions = lists:map(fun(AtmLaneSchema) ->
        #atm_lane_execution{
            schema_id = AtmLaneSchema#atm_lane_schema.id,
            runs = []
        }
    end, RestAtmLaneSchemas),

    AtmWorkflowExecutionCreateCtx#atm_workflow_execution_create_ctx{
        elements = Elements#atm_workflow_execution_elements{
            lanes = [FirstAtmLaneExecution | RestAtmLaneExecutions]
        }
    }.


%% @private
-spec create_workflow_execution_doc(create_ctx()) ->
    atm_workflow_execution:doc().
create_workflow_execution_doc(#atm_workflow_execution_create_ctx{
    params = #atm_workflow_execution_create_params{
        workflow_execution_id = AtmWorkflowExecutionId,
        workflow_execution_auth = AtmWorkflowExecutionAuth,
        workflow_schema_doc = #document{value = #od_atm_workflow_schema{
            name = AtmWorkflowSchemaName,
            atm_inventory = AtmInventoryId
        }},
        callback_url = CallbackUrl
    },
    elements = #atm_workflow_execution_elements{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId,
        lambda_snapshot_registry = AtmLambdaSnapshotRegistry,
        workflow_store_registry = AtmWorkflowStoreRegistry,
        workflow_audit_log_id = AtmWorkflowAuditLogId,
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

            store_registry = AtmWorkflowStoreRegistry,
            system_audit_log_id = AtmWorkflowAuditLogId,

            lanes = AtmLaneExecutions,
            lanes_num = length(AtmLaneExecutions),

            curr_lane_index = 1,
            curr_run_no = 1,

            status = ?SCHEDULED_STATUS,
            prev_status = ?SCHEDULED_STATUS,

            callback = CallbackUrl,

            schedule_time = global_clock:timestamp_seconds(),
            start_time = 0,
            finish_time = 0
        }
    }),
    AtmWorkflowExecutionDoc.


%% @private
-spec delete_execution_elements(elements()) -> ok.
delete_execution_elements(Elements = #atm_workflow_execution_elements{
    schema_snapshot_id = AtmWorkflowSchemaSnapshotId
}) when AtmWorkflowSchemaSnapshotId /= undefined ->
    catch atm_workflow_schema_snapshot:delete(AtmWorkflowSchemaSnapshotId),

    delete_execution_elements(Elements#atm_workflow_execution_elements{
        schema_snapshot_id = undefined
    });

delete_execution_elements(Elements = #atm_workflow_execution_elements{
    lambda_snapshot_registry = AtmLambdaSnapshotRegistry
}) when AtmLambdaSnapshotRegistry /= undefined ->
    catch delete_lambda_snapshots(AtmLambdaSnapshotRegistry),

    delete_execution_elements(Elements#atm_workflow_execution_elements{
        lambda_snapshot_registry = undefined
    });

delete_execution_elements(Elements = #atm_workflow_execution_elements{
    workflow_store_registry = AtmWorkflowStoreRegistry
}) when AtmWorkflowStoreRegistry /= undefined ->
    catch delete_workflow_stores(maps:values(AtmWorkflowStoreRegistry)),

    delete_execution_elements(Elements#atm_workflow_execution_elements{
        workflow_store_registry = undefined
    });

delete_execution_elements(Elements = #atm_workflow_execution_elements{
    workflow_audit_log_id = AtmWorkflowAuditLogId
}) when AtmWorkflowAuditLogId /= undefined ->
    catch atm_store_api:delete(AtmWorkflowAuditLogId),

    delete_execution_elements(Elements#atm_workflow_execution_elements{
        workflow_audit_log_id = undefined
    });

delete_execution_elements(Elements = #atm_workflow_execution_elements{
    lanes = AtmLaneExecutions
}) when AtmLaneExecutions /= undefined ->
    catch delete_lane_executions(AtmLaneExecutions),

    delete_execution_elements(Elements#atm_workflow_execution_elements{
        lanes = undefined
    });

delete_execution_elements(_) ->
    ok.


%% @private
-spec delete_lambda_snapshots(atm_workflow_execution:lambda_snapshot_registry()) -> ok.
delete_lambda_snapshots(AtmLambdaSnapshotRegistry) ->
    lists:foreach(fun atm_lambda_snapshot:delete/1, maps:values(AtmLambdaSnapshotRegistry)).


%% @private
-spec delete_workflow_stores([atm_store:id()]) -> ok.
delete_workflow_stores(AtmStoreIds) ->
    lists:foreach(fun atm_store_api:delete/1, AtmStoreIds).


%% @private
-spec delete_lane_executions([atm_lane_execution:record()]) -> ok.
delete_lane_executions(AtmLaneExecutions) ->
    lists:foreach(fun(#atm_lane_execution{runs = Runs}) ->
        lists:foreach(fun atm_lane_execution_factory:delete_run/1, Runs)
    end, AtmLaneExecutions).
