%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles the task execution process according to following
%%% state machine:
%%%
%%%                      <WAITING PHASE (initiation)>
%%%     +-----------+
%%%     |  PENDING  |---------------------------------------
%%%     +-----------+                                        \
%%%           |                               ending task execution with no item
%%%  first item scheduled to process               ever scheduled to process
%%%           |                                               |
%%% ==========|===============================================|===============
%%%           v               <ONGOING PHASE>                 |
%%%         +-----------+                                     |
%%%         |   ACTIVE  |                                     |
%%%         +-----------+                                     |
%%%                   |                                       |
%%%                   |                                       |
%%%        ending task execution with                         |
%%%             /                \                            |
%%%            /                  \                           |
%%%  all items successfully      else                         |
%%%        processed               |                          |
%%%           |                    |                          |
%%% ==========|====================|==========================|===============
%%%           |           <ENDED PHASE (teardown)>            |
%%%           |                    |                          |
%%%           v                    v                          v
%%%     +-----------+        +-----------+              +-----------+
%%%     |  FINISHED |        |   FAILED  |              |  SKIPPED  |
%%%     +-----------+        +-----------+              +-----------+
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_handler).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    initiate/2,
    abort/3,
    teardown/2,
    set_run_num/2,

    run_job_batch/5,
    process_job_batch_output/4,
    process_streamed_data/3,

    handle_ended/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec initiate(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id() | atm_task_execution:doc()
) ->
    {workflow_engine:task_spec(), atm_workflow_execution_env:diff()} | no_return().
initiate(AtmWorkflowExecutionCtx, AtmTaskExecutionIdOrDoc) ->
    AtmTaskExecutionDoc = ensure_atm_task_execution_doc(AtmTaskExecutionIdOrDoc),

    AtmTaskExecutionSpec = atm_task_executor:initiate(
        build_atm_task_executor_initiation_ctx(AtmWorkflowExecutionCtx, AtmTaskExecutionDoc),
        AtmTaskExecutionDoc#document.value#atm_task_execution.executor
    ),
    AtmWorkflowExecutionEnvDiff = gen_atm_workflow_execution_env_diff(AtmTaskExecutionDoc),

    {AtmTaskExecutionSpec, AtmWorkflowExecutionEnvDiff}.


-spec abort(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    atm_task_execution:aborting_reason()
) ->
    ok | no_return().
abort(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Reason) ->
    case atm_task_execution_status:handle_aborting(AtmTaskExecutionId, Reason) of
        {ok, #document{value = #atm_task_execution{executor = AtmTaskExecutor}}} ->
            atm_task_executor:abort(AtmWorkflowExecutionCtx, AtmTaskExecutor);
        {error, already_ended} ->
            ok
    end.


-spec teardown(atm_workflow_execution_ctx:record(), atm_task_execution:id()) ->
    ok | no_return().
teardown(AtmWorkflowExecutionCtx, AtmTaskExecutionId) ->
    AtmTaskExecutionDoc = ensure_atm_task_execution_doc(AtmTaskExecutionId),

    atm_task_executor:teardown(
        AtmWorkflowExecutionCtx,
        AtmTaskExecutionDoc#document.value#atm_task_execution.executor
    ).


-spec set_run_num(atm_lane_execution:run_num(), atm_task_execution:id()) ->
    ok.
set_run_num(RunNum, AtmTaskExecutionId) ->
    {ok, _} = atm_task_execution:update(AtmTaskExecutionId, fun(AtmTaskExecution) ->
        {ok, AtmTaskExecution#atm_task_execution{run_num = RunNum}}
    end),
    ok.


-spec run_job_batch(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    [automation:item()],
    binary(),
    binary()
) ->
    ok | error.
run_job_batch(
    AtmWorkflowExecutionCtx,
    AtmTaskExecutionId,
    ItemBatch,
    ForwardOutputUrl,
    HeartbeatUrl
) ->
    ItemsNum = length(ItemBatch),

    case atm_task_execution_status:handle_items_in_processing(AtmTaskExecutionId, ItemsNum) of
        {ok, #document{value = AtmTaskExecution = #atm_task_execution{
            executor = AtmTaskExecutor
        }}} ->
            AtmRunJobBatchCtx = atm_run_job_batch_ctx:build(
                AtmWorkflowExecutionCtx, ForwardOutputUrl, HeartbeatUrl, AtmTaskExecution
            ),
            LambdaInput = build_lambda_input(AtmRunJobBatchCtx, ItemBatch, AtmTaskExecution),

            try
                atm_task_executor:run(AtmRunJobBatchCtx, LambdaInput, AtmTaskExecutor)
            catch Type:Reason:Stacktrace ->
                handle_job_batch_processing_error(
                    AtmWorkflowExecutionCtx, AtmTaskExecutionId, ItemBatch,
                    ?atm_examine_error(Type, Reason, Stacktrace)
                ),
                error
            end;

        {error, aborting} ->
            error;  %% TODO

        {error, already_ended} ->
            %% TODO return ok | error | aborted instead of throw ?
            throw(?ERROR_ATM_TASK_EXECUTION_ENDED)
    end.


-spec process_job_batch_output(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    [automation:item()],
    atm_task_executor:lambda_output()
) ->
    ok | error.
process_job_batch_output(AtmWorkflowExecutionCtx, AtmTaskExecutionId, ItemBatch, LambdaOutput) ->
    case parse_lambda_output(ItemBatch, LambdaOutput) of
        {ok, ResultsPerJob} ->
            AnyErrorOccurred = lists:member(error, atm_parallel_runner:map(fun({Item, JobResults}) ->
                process_job_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, JobResults)
            end, ResultsPerJob)),

            case AnyErrorOccurred of
                true -> error;
                false -> ok
            end;
        Error = {error, _} ->
            handle_job_batch_processing_error(
                AtmWorkflowExecutionCtx, AtmTaskExecutionId, ItemBatch, Error
            ),
            error
    end.


-spec process_streamed_data(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    atm_task_executor:streamed_data()
) ->
    ok | error.
process_streamed_data(AtmWorkflowExecutionCtx, AtmTaskExecutionId, {chunk, UncorrelatedResults}) ->
    {ok, #document{value = AtmTaskExecution}} = atm_task_execution:get(AtmTaskExecutionId),

    try
        maps:foreach(fun(ResultName, ResultArray) ->
            lists:foreach(fun(ResultValue) ->
                atm_task_execution_results:consume_results(
                    AtmWorkflowExecutionCtx,
                    uncorrelated,
                    AtmTaskExecution#atm_task_execution.uncorrelated_result_specs,
                    #{ResultName => ResultValue}
                )
            end, ResultArray)
        end, UncorrelatedResults)
    catch Type:Reason:Stacktrace ->
        handle_uncorrelated_results_processing_error(
            AtmWorkflowExecutionCtx,
            AtmTaskExecutionId,
            ?atm_examine_error(Type, Reason, Stacktrace)
        ),
        error
    end;

process_streamed_data(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Error = {error, _}) ->
    handle_uncorrelated_results_processing_error(
        AtmWorkflowExecutionCtx, AtmTaskExecutionId, Error
    ),
    error.


-spec handle_ended(atm_task_execution:id()) -> ok.
handle_ended(AtmTaskExecutionId) ->
    case atm_task_execution_status:handle_ended(AtmTaskExecutionId) of
        {ok, #document{value = AtmTaskExecution}} ->
            freeze_stores(AtmTaskExecution);
        {error, already_ended} ->
            ok
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec ensure_atm_task_execution_doc(atm_task_execution:id() | atm_task_execution:doc()) ->
    atm_task_execution:doc().
ensure_atm_task_execution_doc(#document{value = #atm_task_execution{}} = AtmTaskExecutionDoc) ->
    AtmTaskExecutionDoc;
ensure_atm_task_execution_doc(AtmTaskExecutionId) ->
    {ok, AtmTaskExecutionDoc = #document{}} = atm_task_execution:get(AtmTaskExecutionId),
    AtmTaskExecutionDoc.


%% @private
-spec build_atm_task_executor_initiation_ctx(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id() | atm_task_execution:doc()
) ->
    atm_task_executor:initiation_ctx().
build_atm_task_executor_initiation_ctx(AtmWorkflowExecutionCtx, #document{
    key = AtmTaskExecutionId,
    value = AtmTaskExecution = #atm_task_execution{
        workflow_execution_id = AtmWorkflowExecutionId,
        uncorrelated_result_specs = AtmTaskExecutionUncorrelatedResultSpecs
    }
}) ->
    {ok, #document{value = AtmWorkflowExecution}} = atm_workflow_execution:get(
        AtmWorkflowExecutionId
    ),
    AtmTaskSchema = get_task_schema(AtmTaskExecution, AtmWorkflowExecution),

    #atm_task_executor_initiation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        task_execution_id = AtmTaskExecutionId,
        task_schema = AtmTaskSchema,
        lambda_revision = get_lambda_revision(AtmTaskSchema, AtmWorkflowExecution),
        uncorrelated_results = lists:map(
            fun atm_task_execution_result_spec:get_name/1,
            AtmTaskExecutionUncorrelatedResultSpecs
        )
    }.


%% @private
-spec get_task_schema(atm_task_execution:record(), atm_workflow_execution:record()) ->
    atm_task_schema:record().
get_task_schema(
    #atm_task_execution{
        lane_index = AtmLaneIndex,
        parallel_box_index = AtmParallelBoxIndex,
        schema_id = AtmTaskSchemaId
    },
    #atm_workflow_execution{schema_snapshot_id = AtmWorkflowSchemaSnapshotId}
) ->
    {ok, AtmWorkflowSchemaSnapshotDoc} = atm_workflow_schema_snapshot:get(AtmWorkflowSchemaSnapshotId),

    AtmLaneSchemas = atm_workflow_schema_snapshot:get_lane_schemas(AtmWorkflowSchemaSnapshotDoc),
    AtmLaneSchema = lists:nth(AtmLaneIndex, AtmLaneSchemas),
    AtmParallelBoxSchema = lists:nth(AtmParallelBoxIndex, AtmLaneSchema#atm_lane_schema.parallel_boxes),

    {value, AtmTaskSchema} = lists:search(
        fun(#atm_task_schema{id = Id}) -> Id == AtmTaskSchemaId end,
        AtmParallelBoxSchema#atm_parallel_box_schema.tasks
    ),
    AtmTaskSchema.


%% @private
-spec get_lambda_revision(atm_task_schema:record(), atm_workflow_execution:record()) ->
    atm_lambda_revision:record().
get_lambda_revision(
    #atm_task_schema{lambda_id = AtmLambdaId, lambda_revision_number = AtmLambdaRevisionNum},
    #atm_workflow_execution{lambda_snapshot_registry = AtmLambdaSnapshotRegistry}
) ->
    {ok, #document{value = AtmLambdaSnapshot}} = atm_lambda_snapshot:get(
        maps:get(AtmLambdaId, AtmLambdaSnapshotRegistry)
    ),
    atm_lambda_snapshot:get_revision(AtmLambdaRevisionNum, AtmLambdaSnapshot).


%% @private
-spec gen_atm_workflow_execution_env_diff(atm_task_execution:doc()) ->
    atm_workflow_execution_env:diff().
gen_atm_workflow_execution_env_diff(#document{
    key = AtmTaskExecutionId,
    value = #atm_task_execution{
        system_audit_log_store_id = AtmSystemAuditLogStoreId,
        time_series_store_id = AtmTaskTSStoreId
    }
}) ->
    {ok, #atm_store{container = AtmTaskAuditLogStoreContainer}} = atm_store_api:get(
        AtmSystemAuditLogStoreId
    ),

    fun(Env0) ->
        Env1 = atm_workflow_execution_env:add_task_audit_log_store_container(
            AtmTaskExecutionId, AtmTaskAuditLogStoreContainer, Env0
        ),
        atm_workflow_execution_env:add_task_time_series_store_id(
            AtmTaskExecutionId, AtmTaskTSStoreId, Env1
        )
    end.


%% @private
-spec build_lambda_input(
    atm_run_job_batch_ctx:record(),
    [automation:item()],
    atm_task_execution:record()
) ->
    json_utils:json_map().
build_lambda_input(AtmRunJobBatchCtx, ItemBatch, #atm_task_execution{
    argument_specs = AtmTaskExecutionArgSpecs
}) ->
    %% TODO VFS-8668 optimize argsBatch creation
    ArgsBatch = atm_parallel_runner:map(fun(Item) ->
        atm_task_execution_arguments:construct_args(
            Item, AtmRunJobBatchCtx, AtmTaskExecutionArgSpecs
        )
    end, ItemBatch),

    #{
        <<"ctx">> => #{
            <<"heartbeatUrl">> => atm_run_job_batch_ctx:get_heartbeat_url(AtmRunJobBatchCtx)
        },
        <<"argsBatch">> => ArgsBatch
    }.


%% @private
-spec parse_lambda_output([automation:item()], atm_task_executor:lambda_output()) ->
    {ok, [{automation:item(), atm_task_executor:job_results()}]} | errors:error().
parse_lambda_output(_ItemBatch, Error = {error, _}) ->
    % Entire batch processing failed (e.g. timeout or malformed lambda response)
    Error;

parse_lambda_output(ItemBatch, #{<<"resultsBatch">> := ResultsBatch}) when
    is_list(ResultsBatch),
    length(ItemBatch) == length(ResultsBatch)
->
    {ok, lists:zipwith(fun
        (Item, Result) when is_map(Result) ->
            {Item, Result};
        (Item, Result) ->
            {Item, ?ERROR_BAD_DATA(<<"lambdaResultsForItem">>, str_utils:format_bin(
                "Expected object with result names matching those defined in task schema. "
                "Instead got: ~s", [json_utils:encode(Result)]
            ))}
    end, ItemBatch, ResultsBatch)};

parse_lambda_output(_ItemBatch, MalformedLambdaOutput) ->
    ?ERROR_BAD_DATA(<<"lambdaOutput">>, str_utils:format_bin(
        "Expected '{\"resultsBatch\": [$LAMBDA_RESULTS_FOR_ITEM, ...]}' with "
        "$LAMBDA_RESULTS_FOR_ITEM object for each item in 'argsBatch' "
        "provided to lambda. Instead got: ~s",
        [json_utils:encode(MalformedLambdaOutput)]
    )).


%% @private
-spec handle_job_batch_processing_error(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    [automation:item()],
    errors:error()
) ->
    ok.
handle_job_batch_processing_error(
    AtmWorkflowExecutionCtx,
    AtmTaskExecutionId,
    ItemBatch,
    Error
) ->
    atm_task_execution_status:handle_items_failed(AtmTaskExecutionId, length(ItemBatch)),

    ErrorLog = #{
        <<"description">> => <<"Failed to process batch of items.">>,
        <<"itemBatch">> => ItemBatch,
        <<"reason">> => errors:to_json(Error)
    },
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    atm_workflow_execution_logger:task_error(ErrorLog, Logger).


-spec process_job_results(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    automation:item(),
    atm_task_executor:job_results()
) ->
    ok | error | no_return().
process_job_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, {error, _} = Error) ->
    handle_job_processing_error(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Error),
    error;

process_job_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Exception = #{
    <<"exception">> := _
}) ->
    handle_job_processing_error(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Exception),
    error;

process_job_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, JobResults) ->
    {ok, #document{value = AtmTaskExecution}} = atm_task_execution:get(AtmTaskExecutionId),

    try
        atm_task_execution_results:consume_results(
            AtmWorkflowExecutionCtx,
            item_related,
            AtmTaskExecution#atm_task_execution.item_related_result_specs,
            JobResults
        ),
        atm_task_execution_status:handle_item_processed(AtmTaskExecutionId)
    catch Type:Reason:Stacktrace ->
        Error = ?atm_examine_error(Type, Reason, Stacktrace),
        handle_job_processing_error(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Error),
        error
    end.


%% @private
-spec handle_job_processing_error(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    automation:item(),
    errors:error() | json_utils:json_map()
) ->
    ok.
handle_job_processing_error(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Error) ->
    atm_task_execution_status:handle_items_failed(AtmTaskExecutionId, 1),

    ErrorLog = case Error of
        _LambdaException = #{<<"exception">> := Reason} ->
            #{
                <<"description">> => <<"Lambda exception occurred during item processing.">>,
                <<"reason">> => Reason
            };
        _SystemError = {error, _} ->
            #{
                <<"description">> => <<"Failed to process item.">>,
                <<"reason">> => errors:to_json(Error)
            }
    end,
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    atm_workflow_execution_logger:task_error(ErrorLog#{<<"item">> => Item}, Logger).


%% @private
-spec handle_uncorrelated_results_processing_error(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    errors:error()
) ->
    ok.
handle_uncorrelated_results_processing_error(
    AtmWorkflowExecutionCtx,
    AtmTaskExecutionId,
    Error
) ->
    case atm_task_execution_status:handle_aborting(AtmTaskExecutionId, failure) of
        {ok, #document{value = #atm_task_execution{lane_index = AtmLaneIndex, run_num = RunNum}}} ->
            log_uncorrelated_results_processing_error(
                AtmWorkflowExecutionCtx, AtmTaskExecutionId, Error
            ),
            atm_lane_execution_handler:abort(
                {AtmLaneIndex, RunNum}, failure, AtmWorkflowExecutionCtx
            );

        {error, already_ended} ->
            ok
    end.


%% @private
-spec log_uncorrelated_results_processing_error(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    errors:error()
) ->
    ok.
log_uncorrelated_results_processing_error(
    AtmWorkflowExecutionCtx,
    AtmTaskExecutionId,
    Error
) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),

    TaskLog = #{
        <<"description">> => <<"Failed to process uncorrelated task results.">>,
        <<"reason">> => errors:to_json(Error)
    },
    atm_workflow_execution_logger:task_critical(TaskLog, Logger),

    WorkflowLog = #{
        <<"description">> => str_utils:format_bin(
            "Failed to process uncorrelated results for task '~s'.",
            [AtmTaskExecutionId]
        ),
        <<"referencedComponents">> => #{
            <<"tasks">> => [AtmTaskExecutionId]
        }
    },
    atm_workflow_execution_logger:workflow_critical(WorkflowLog, Logger).


%% @private
-spec freeze_stores(atm_task_execution:record()) -> ok.
freeze_stores(#atm_task_execution{
    system_audit_log_store_id = AtmSystemAuditLogStoreId,
    time_series_store_id = undefined
}) ->
    atm_store_api:freeze(AtmSystemAuditLogStoreId);

freeze_stores(#atm_task_execution{
    system_audit_log_store_id = AtmSystemAuditLogStoreId,
    time_series_store_id = AtmTSStoreId
}) ->
    atm_store_api:freeze(AtmSystemAuditLogStoreId),
    atm_store_api:freeze(AtmTSStoreId).
