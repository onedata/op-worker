%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles creation of all documents associated with automation
%%% lane execution (e.g. exception store, etc.). If creation of any component
%%% fails then ones created before are deleted.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lane_execution_factory).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([create_run/3, delete_run/1]).


-type run_creation_args() :: #atm_lane_execution_run_creation_args{}.

-record(run_execution_components, {
    exception_store_id = undefined :: undefined | atm_store:id(),
    parallel_boxes = undefined :: undefined | [atm_parallel_box_execution:record()]
}).
-type run_execution_components() :: #run_execution_components{}.

-record(run_creation_ctx, {
    creation_args :: run_creation_args(),
    execution_components :: run_execution_components()
}).
-type run_creation_ctx() :: #run_creation_ctx{}.

-export_type([run_creation_args/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_run(
    atm_lane_execution:index(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    atm_workflow_execution:doc() | no_return().
create_run(AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    try
        create_run_internal(AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx)
    catch Type:Reason:Stacktrace ->
        throw(?ERROR_ATM_LANE_EXECUTION_CREATION_FAILED(
            atm_lane_execution:get_schema_id(AtmLaneIndex, AtmWorkflowExecutionDoc#document.value),
            ?atm_examine_error(Type, Reason, Stacktrace)
        ))
    end.


-spec delete_run(atm_lane_execution:run()) -> ok.
delete_run(#atm_lane_execution_run{
    exception_store_id = ExceptionStoreId,
    parallel_boxes = AtmParallelBoxExecutions
}) ->
    delete_run_execution_components(#run_execution_components{
        exception_store_id = ExceptionStoreId,
        parallel_boxes = AtmParallelBoxExecutions
    }).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_run_internal(
    atm_lane_execution:index(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    atm_workflow_execution:doc() | no_return().
create_run_internal(AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    #run_creation_ctx{
        creation_args = #atm_lane_execution_run_creation_args{
            iterated_store_id = IteratedStoreId
        },
        execution_components = RunExecutionComponents = #run_execution_components{
            exception_store_id = ExceptionStoreId,
            parallel_boxes = AtmParallelBoxExecutions
        }
    } = create_run_execution_components(build_run_creation_ctx(
        AtmLaneIndex, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
    )),

    Diff = fun(AtmWorkflowExecution) ->
        atm_lane_execution:update_current_run(AtmLaneIndex, fun
            (Run = #atm_lane_execution_run{
                status = ?PREPARING_STATUS,
                exception_store_id = undefined,
                parallel_boxes = []
            }) ->
                {ok, Run#atm_lane_execution_run{
                    iterated_store_id = IteratedStoreId,
                    exception_store_id = ExceptionStoreId,
                    parallel_boxes = AtmParallelBoxExecutions
                }};
            (_) ->
                ?ERROR_ALREADY_EXISTS
        end, AtmWorkflowExecution)
    end,
    case atm_workflow_execution:update(AtmWorkflowExecutionDoc#document.key, Diff) of
        {ok, NewAtmWorkflowExecutionDoc} ->
            NewAtmWorkflowExecutionDoc;
        {error, _} = Error ->
            delete_run_execution_components(RunExecutionComponents),
            throw(Error)
    end.


%% @private
-spec build_run_creation_ctx(
    atm_lane_execution:index(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    run_creation_ctx().
build_run_creation_ctx(AtmLaneIndex, AtmWorkflowExecutionDoc = #document{
    value = AtmWorkflowExecution = #atm_workflow_execution{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId
    }
}, AtmWorkflowExecutionCtx) ->
    {ok, Run = #atm_lane_execution_run{
        status = ?PREPARING_STATUS,
        iterated_store_id = IteratedStoreId
    }} = atm_lane_execution:get_current_run(AtmLaneIndex, AtmWorkflowExecution),

    {ok, #document{value = #atm_workflow_schema_snapshot{
        lanes = AtmLaneSchemas
    }}} = atm_workflow_schema_snapshot:get(AtmWorkflowSchemaSnapshotId),

    AtmLaneSchema = #atm_lane_schema{store_iterator_spec = #atm_store_iterator_spec{
        store_schema_id = AtmStoreSchemaId
    }} = lists:nth(AtmLaneIndex, AtmLaneSchemas),

    #run_creation_ctx{
        creation_args = #atm_lane_execution_run_creation_args{
            workflow_execution_ctx = AtmWorkflowExecutionCtx,
            workflow_execution_doc = AtmWorkflowExecutionDoc,

            lane_index = AtmLaneIndex,
            lane_schema = AtmLaneSchema,

            iterated_store_id = case IteratedStoreId of
                undefined ->
                    % If not explicitly set then take designated by schema store
                    atm_workflow_execution_ctx:get_global_store_id(
                        AtmStoreSchemaId, AtmWorkflowExecutionCtx
                    );
                _ ->
                    IteratedStoreId
            end
        },
        execution_components = #run_execution_components{}
    }.


%% @private
-spec create_run_execution_components(run_creation_ctx()) ->
    run_creation_ctx() | no_return().
create_run_execution_components(RunCreationCtx) ->
    lists:foldl(fun(CreateExecutionComponentFun, NewRunCreationCtx) ->
        try
            CreateExecutionComponentFun(NewRunCreationCtx)
        catch Type:Reason:Stacktrace ->
            delete_run_execution_components(NewRunCreationCtx#run_creation_ctx.execution_components),
            throw(?atm_examine_error(Type, Reason, Stacktrace))
        end
    end, RunCreationCtx, [
        fun create_exception_store/1,
        fun create_parallel_box_executions/1
    ]).


%% @private
-spec create_exception_store(run_creation_ctx()) -> run_creation_ctx().
create_exception_store(RunCreationCtx = #run_creation_ctx{
    creation_args = #atm_lane_execution_run_creation_args{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        iterated_store_id = AtmIteratedStoreId
    },
    execution_components = RunExecutionComponents
}) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx),
    {ok, #atm_store{container = AtmStoreContainer}} = atm_store_api:get(AtmIteratedStoreId),

    {ok, #document{key = AtmLaneExceptionStoreId}} = atm_store_api:create(
        AtmWorkflowExecutionAuth, undefined, #atm_store_schema{
            id = ?CURRENT_LANE_RUN_EXCEPTION_STORE_SCHEMA_ID,
            name = ?CURRENT_LANE_RUN_EXCEPTION_STORE_SCHEMA_ID,
            description = <<>>,
            type = list,
            data_spec = atm_store_container:get_data_spec(AtmStoreContainer),
            requires_initial_value = false
        }
    ),

    RunCreationCtx#run_creation_ctx{
        execution_components = RunExecutionComponents#run_execution_components{
            exception_store_id = AtmLaneExceptionStoreId
        }
    }.


%% @private
-spec create_parallel_box_executions(run_creation_ctx()) -> run_creation_ctx().
create_parallel_box_executions(RunCreationCtx = #run_creation_ctx{
    creation_args = RunCreationArgs,
    execution_components = RunExecutionComponents
}) ->
    RunCreationCtx#run_creation_ctx{
        execution_components = RunExecutionComponents#run_execution_components{
            parallel_boxes = atm_parallel_box_execution:create_all(RunCreationArgs)
        }
    }.


%% @private
-spec delete_run_execution_components(run_execution_components()) -> ok.
delete_run_execution_components(RunExecutionComponents = #run_execution_components{
    exception_store_id = AtmLaneExceptionStoreId
}) when AtmLaneExceptionStoreId /= undefined ->
    catch atm_store_api:delete(AtmLaneExceptionStoreId),

    delete_run_execution_components(RunExecutionComponents#run_execution_components{
        exception_store_id = undefined
    });

delete_run_execution_components(RunExecutionComponents = #run_execution_components{
    parallel_boxes = AtmParallelBoxExecutions
}) when AtmParallelBoxExecutions /= undefined ->
    catch atm_parallel_box_execution:delete_all(AtmParallelBoxExecutions),

    delete_run_execution_components(RunExecutionComponents#run_execution_components{
        parallel_boxes = undefined
    });

delete_run_execution_components(_) ->
    ok.