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
    reset_lane_retries_num :: boolean(),
    execution_components :: run_execution_components()
}).
-type run_creation_ctx() :: #run_creation_ctx{}.

-export_type([run_creation_args/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_run(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    atm_workflow_execution:doc() | no_return().
create_run(AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    try
        create_run_internal(AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx)
    catch
        throw:?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING ->
            throw(?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING);

        Type:Reason:Stacktrace ->
            AtmWorkflowExecution = AtmWorkflowExecutionDoc#document.value,

            throw(?ERROR_ATM_LANE_EXECUTION_CREATION_FAILED(
                atm_lane_execution:get_schema_id(AtmLaneRunSelector, AtmWorkflowExecution),
                ?examine_exception(Type, Reason, Stacktrace)
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
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    atm_workflow_execution:doc() | no_return().
create_run_internal(AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    RunCreationCtx = create_run_execution_components(build_run_creation_ctx(
        AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx
    )),
    Diff = fun(AtmWorkflowExecution) ->
        case reset_lane_retries_num_if_needed(AtmWorkflowExecution, RunCreationCtx) of
            {ok, NewAtmWorkflowExecution} ->
                complement_run(AtmLaneRunSelector, NewAtmWorkflowExecution, RunCreationCtx);
            {error, _} = Error ->
                Error
        end
    end,
    AtmWorkflowExecutionId = AtmWorkflowExecutionDoc#document.key,

    case atm_workflow_execution_status:handle_lane_run_created(AtmWorkflowExecutionId, Diff) of
        {ok, NewAtmWorkflowExecutionDoc} ->
            NewAtmWorkflowExecutionDoc;
        {error, _} = Error ->
            delete_run_execution_components(RunCreationCtx#run_creation_ctx.execution_components),
            throw(Error)
    end.


%% @private
-spec build_run_creation_ctx(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:doc(),
    atm_workflow_execution_ctx:record()
) ->
    run_creation_ctx().
build_run_creation_ctx(AtmLaneRunSelector, AtmWorkflowExecutionDoc, AtmWorkflowExecutionCtx) ->
    {AtmLaneSelector, _} = AtmLaneRunSelector,
    AtmWorkflowExecution = AtmWorkflowExecutionDoc#document.value,

    {ok, Run = #atm_lane_execution_run{status = ?PREPARING_STATUS}} = atm_lane_execution:get_run(
        AtmLaneRunSelector, AtmWorkflowExecution
    ),
    AtmLaneSchema = #atm_lane_schema{
        store_iterator_spec = #atm_store_iterator_spec{
            store_schema_id = AtmStoreSchemaId
        }
    } = atm_lane_execution:get_schema(AtmLaneRunSelector, AtmWorkflowExecution),

    IteratedStoreId = case Run#atm_lane_execution_run.iterated_store_id of
        undefined ->
            % If not explicitly set then take designated by schema store
            atm_workflow_execution_ctx:get_global_store_id(
                AtmStoreSchemaId, AtmWorkflowExecutionCtx
            );
        Id ->
            Id
    end,

    {RunType, OriginAtmLaneRun} = case Run#atm_lane_execution_run.origin_run_num of
        undefined ->
            {regular, undefined};
        OriginRunNum ->
            OriginLaneRunSelector = {AtmLaneSelector, OriginRunNum},
            {ok, OriginRun} = atm_lane_execution:get_run(OriginLaneRunSelector, AtmWorkflowExecution),
            case OriginRun#atm_lane_execution_run.iterated_store_id of
                IteratedStoreId -> {rerun, OriginRun};
                _ -> {retry, OriginRun}
            end
    end,

    #run_creation_ctx{
        creation_args = #atm_lane_execution_run_creation_args{
            type = RunType,
            workflow_execution_ctx = AtmWorkflowExecutionCtx,
            workflow_execution_doc = AtmWorkflowExecutionDoc,

            lane_index = atm_lane_execution:resolve_selector(AtmLaneSelector, AtmWorkflowExecution),
            lane_schema = AtmLaneSchema,
            origin_run = OriginAtmLaneRun,

            iterated_store_id = IteratedStoreId
        },
        reset_lane_retries_num = case RunType of
            regular -> true;
            _ -> false
        end,
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
            throw(?examine_exception(Type, Reason, Stacktrace))
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
        AtmWorkflowExecutionAuth, ?LOGGER_DEBUG_LEVEL, undefined, ?ATM_LANE_RUN_EXCEPTION_STORE_SCHEMA(
            atm_store_container:get_iterated_item_data_spec(AtmStoreContainer)
        )
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


%% @private
-spec reset_lane_retries_num_if_needed(atm_workflow_execution:record(), run_creation_ctx()) ->
    {ok, atm_workflow_execution:record()} | errors:error().
reset_lane_retries_num_if_needed(AtmWorkflowExecution, #run_creation_ctx{
    creation_args = #atm_lane_execution_run_creation_args{
        lane_index = AtmLaneIndex,
        lane_schema = #atm_lane_schema{max_retries = MaxRetries}
    },
    reset_lane_retries_num = true
}) ->
    Diff = fun(AtmLaneExecution) ->
        {ok, AtmLaneExecution#atm_lane_execution{retries_left = MaxRetries}}
    end,
    atm_lane_execution:update(AtmLaneIndex, Diff, AtmWorkflowExecution);

reset_lane_retries_num_if_needed(AtmWorkflowExecution, _) ->
    {ok, AtmWorkflowExecution}.


%% @private
-spec complement_run(
    atm_lane_execution:lane_run_selector(),
    atm_workflow_execution:record(),
    run_creation_ctx()
) ->
    {ok, atm_workflow_execution:record()} | errors:error().
complement_run(AtmLaneRunSelector, AtmWorkflowExecution, #run_creation_ctx{
    creation_args = #atm_lane_execution_run_creation_args{
        iterated_store_id = IteratedStoreId
    },
    execution_components = #run_execution_components{
        exception_store_id = ExceptionStoreId,
        parallel_boxes = AtmParallelBoxExecutions
    }
}) ->
    atm_lane_execution:update_run(AtmLaneRunSelector, fun
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
    end, AtmWorkflowExecution).
