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
    teardown/2,
    set_run_num/2,

    process_items_batch/5,
    process_lambda_output/4,
    process_supplementary_results/3,

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
    #document{
        key = AtmTaskExecutionId,
        value = AtmTaskExecution = #atm_task_execution{
            workflow_execution_id = AtmWorkflowExecutionId,
            executor = AtmTaskExecutor,
            supplementary_result_specs = AtmTaskExecutionSupplementaryResultSpecs,
            system_audit_log_id = AtmSystemAuditLogId,
            time_series_store_id = AtmTaskTSStoreId
        }
    } = ensure_atm_task_execution_doc(AtmTaskExecutionIdOrDoc),

    {ok, #document{value = AtmWorkflowExecution}} = atm_workflow_execution:get(
        AtmWorkflowExecutionId
    ),
    AtmTaskSchema = get_task_schema(AtmTaskExecution, AtmWorkflowExecution),

    AtmTaskExecutorInitiationCtx = #atm_task_executor_initiation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        task_execution_id = AtmTaskExecutionId,
        task_schema = AtmTaskSchema,
        lambda_revision = get_lambda_revision(AtmTaskSchema, AtmWorkflowExecution),
        supplementary_results = lists:map(
            fun atm_task_execution_result_spec:get_name/1,
            AtmTaskExecutionSupplementaryResultSpecs
        )
    },
    AtmTaskExecutionSpec = atm_task_executor:initiate(
        AtmTaskExecutorInitiationCtx,
        AtmTaskExecutor
    ),

    {ok, #atm_store{container = AtmTaskAuditLogStoreContainer}} = atm_store_api:get(
        AtmSystemAuditLogId
    ),
    AtmWorkflowExecutionEnvDiff = fun(Env0) ->
        Env1 = atm_workflow_execution_env:add_task_audit_log_store_container(
            AtmTaskExecutionId, AtmTaskAuditLogStoreContainer, Env0
        ),
        atm_workflow_execution_env:add_task_time_series_store_id(
            AtmTaskExecutionId, AtmTaskTSStoreId, Env1
        )
    end,

    {AtmTaskExecutionSpec, AtmWorkflowExecutionEnvDiff}.


-spec teardown(atm_workflow_execution_ctx:record(), atm_task_execution:id()) ->
    ok | no_return().
teardown(AtmWorkflowExecutionCtx, AtmTaskExecutionId) ->
    #document{value = #atm_task_execution{
        executor = AtmTaskExecutor
    }} = ensure_atm_task_execution_doc(AtmTaskExecutionId),

    atm_task_executor:teardown(AtmWorkflowExecutionCtx, AtmTaskExecutor).


-spec set_run_num(atm_lane_execution:run_num(), atm_task_execution:id()) ->
    ok.
set_run_num(RunNum, AtmTaskExecutionId) ->
    {ok, _} = atm_task_execution:update(AtmTaskExecutionId, fun(AtmTaskExecution) ->
        {ok, AtmTaskExecution#atm_task_execution{run_num = RunNum}}
    end),
    ok.


-spec process_items_batch(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    [automation:item()],
    binary(),
    binary()
) ->
    ok | error.
process_items_batch(
    AtmWorkflowExecutionCtx,
    AtmTaskExecutionId,
    ItemsBatch,
    ReportResultUrl,
    HeartbeatUrl
) ->
    #document{
        value = #atm_task_execution{
            executor = AtmTaskExecutor,
            argument_specs = AtmTaskExecutionArgSpecs
        }
    } = update_items_in_processing(AtmTaskExecutionId, length(ItemsBatch)),

    AtmJobCtx = atm_job_ctx:build(
        AtmWorkflowExecutionCtx,
        atm_task_executor:is_in_readonly_mode(AtmTaskExecutor),
        ReportResultUrl
    ),

    try
        LambdaInput = #{
            <<"ctx">> => #{<<"heartbeatUrl">> => HeartbeatUrl},
            %% TODO VFS-8668 optimize argsBatch creation
            <<"argsBatch">> => atm_parallel_runner:map(fun(Item) ->
                atm_task_execution_arguments:construct_args(Item, AtmJobCtx, AtmTaskExecutionArgSpecs)
            end, ItemsBatch)
        },
        atm_task_executor:run(AtmJobCtx, LambdaInput, AtmTaskExecutor)
    catch Type:Reason:Stacktrace ->
        Error = ?atm_examine_error(Type, Reason, Stacktrace),
        handle_items_batch_processing_error(
            AtmWorkflowExecutionCtx, AtmTaskExecutionId, ItemsBatch, Error
        ),
        error
    end.


-spec process_lambda_output(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    [automation:item()],
    errors:error() | atm_task_executor:lambda_output()
) ->
    ok | error.
process_lambda_output(AtmWorkflowExecutionCtx, AtmTaskExecutionId, ItemsBatch, LambdaOutput) ->
    case resolve_lambda_results(ItemsBatch, LambdaOutput) of
        {ok, ResultsPerItem} ->
            AnyErrorOccurred = lists:member(error, atm_parallel_runner:map(fun({Item, Results}) ->
                process_elementary_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Results)
            end, ResultsPerItem)),

            case AnyErrorOccurred of
                true -> error;
                false -> ok
            end;
        Error = {error, _} ->
            handle_items_batch_processing_error(
                AtmWorkflowExecutionCtx, AtmTaskExecutionId, ItemsBatch, Error
            ),
            error
    end.


-spec process_supplementary_results(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    json_utils:json_map() | errors:error()
) ->
    ok | error.
process_supplementary_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Error = {error, _}) ->
    handle_supplementary_results_processing_error(
        AtmWorkflowExecutionCtx, AtmTaskExecutionId, Error
    ),
    error;

process_supplementary_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Results) ->
    {ok, #document{value = #atm_task_execution{
        supplementary_result_specs = AtmTaskExecutionSupplementaryResultSpecs
    }}} = atm_task_execution:get(AtmTaskExecutionId),

    try
        atm_task_execution_results:consume_supplementary_results(
            AtmWorkflowExecutionCtx,
            AtmTaskExecutionSupplementaryResultSpecs,
            Results
        )
    catch Type:Reason:Stacktrace ->
        handle_supplementary_results_processing_error(
            AtmWorkflowExecutionCtx,
            AtmTaskExecutionId,
            ?atm_examine_error(Type, Reason, Stacktrace)
        ),
        error
    end.


-spec handle_ended(atm_task_execution:id()) -> ok.
handle_ended(AtmTaskExecutionId) ->
    Diff = fun
        (AtmTaskExecution = #atm_task_execution{status = ?PENDING_STATUS}) ->
            {ok, AtmTaskExecution#atm_task_execution{status = ?SKIPPED_STATUS}};

        (AtmTaskExecution = #atm_task_execution{
            status = ?ACTIVE_STATUS,
            aborting_reason = AbortingReason,
            items_in_processing = ItemsInProcessing,
            items_processed = ItemsProcessed,
            items_failed = ItemsFailed
        }) ->
            % atm workflow execution may have been abruptly interrupted by e.g.
            % provider restart which resulted in stale `items_in_processing`
            AllProcessedItems = ItemsProcessed + ItemsInProcessing,
            AllFailedItems = ItemsFailed + ItemsInProcessing,

            {ok, AtmTaskExecution#atm_task_execution{
                status = if
                    AbortingReason == failure -> ?FAILED_STATUS;
                    AllFailedItems == 0 -> ?FINISHED_STATUS;
                    true -> ?FAILED_STATUS
                end,
                items_in_processing = 0,
                items_processed = AllProcessedItems,
                items_failed = AllFailedItems
            }};

        (_) ->
            {error, already_ended}
    end,
    case atm_task_execution:update(AtmTaskExecutionId, Diff) of
        {ok, #document{value = AtmTaskExecution} = AtmTaskExecutionDoc} ->
            atm_store_api:freeze(AtmTaskExecution#atm_task_execution.system_audit_log_id),
            handle_status_change(AtmTaskExecutionDoc);
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
-spec resolve_lambda_results(
    [automation:item()],
    errors:error() | atm_task_executor:lambda_output()
) ->
    {ok, [{automation:item(), atm_task_executor:results()}]} | errors:error().
resolve_lambda_results(_ItemsBatch, Error = {error, _}) ->
    % Entire batch processing failed (e.g. timeout or malformed lambda response)
    Error;

resolve_lambda_results(ItemsBatch, #{<<"resultsBatch">> := ResultsBatch}) when
    is_list(ResultsBatch),
    length(ItemsBatch) == length(ResultsBatch)
->
    {ok, lists:zipwith(fun
        (Item, Result) when is_map(Result) ->
            {Item, Result};
        (Item, Result) ->
            {Item, ?ERROR_BAD_DATA(<<"lambdaResultsForItem">>, str_utils:format_bin(
                "Expected object with result names matching those defined in task schema. "
                "Instead got: ~s", [json_utils:encode(Result)]
            ))}
    end, ItemsBatch, ResultsBatch)};

resolve_lambda_results(_ItemsBatch, MalformedLambdaOutput) ->
    ?ERROR_BAD_DATA(<<"lambdaOutput">>, str_utils:format_bin(
        "Expected '{\"resultsBatch\": [$LAMBDA_RESULTS_FOR_ITEM, ...]}' with "
        "$LAMBDA_RESULTS_FOR_ITEM object for each item in 'argsBatch' "
        "provided to lambda. Instead got: ~s",
        [json_utils:encode(MalformedLambdaOutput)]
    )).


%% @private
-spec handle_items_batch_processing_error(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    [automation:item()],
    errors:error()
) ->
    ok.
handle_items_batch_processing_error(
    AtmWorkflowExecutionCtx,
    AtmTaskExecutionId,
    ItemsBatch,
    Error
) ->
    update_items_failed_and_processed(AtmTaskExecutionId, length(ItemsBatch)),

    ErrorLog = #{
        <<"description">> => <<"Failed to process batch of items.">>,
        <<"itemsBatch">> => ItemsBatch,
        <<"reason">> => errors:to_json(Error)
    },
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    atm_workflow_execution_logger:task_error(ErrorLog, Logger).


-spec process_elementary_results(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    automation:item(),
    atm_task_executor:results()
) ->
    ok | error | no_return().
process_elementary_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, {error, _} = Error) ->
    handle_item_processing_error(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Error),
    error;

process_elementary_results(
    AtmWorkflowExecutionCtx,
    AtmTaskExecutionId,
    Item,
    Exception = #{<<"exception">> := _}
) ->
    handle_item_processing_error(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Exception),
    error;

process_elementary_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Results) ->
    {ok, #document{value = #atm_task_execution{
        elementary_result_specs = AtmTaskExecutionElementaryResultSpecs
    }}} = atm_task_execution:get(AtmTaskExecutionId),

    try
        atm_task_execution_results:consume_elementary_results(
            AtmWorkflowExecutionCtx,
            AtmTaskExecutionElementaryResultSpecs,
            Results
        ),
        update_items_processed(AtmTaskExecutionId)
    catch Type:Reason:Stacktrace ->
        Error = ?atm_examine_error(Type, Reason, Stacktrace),
        handle_item_processing_error(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Error),
        error
    end.


%% @private
-spec handle_item_processing_error(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    automation:item(),
    errors:error() | json_utils:json_map()
) ->
    ok.
handle_item_processing_error(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Error) ->
    update_items_failed_and_processed(AtmTaskExecutionId),

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
-spec update_items_in_processing(atm_task_execution:id(), pos_integer()) ->
    atm_task_execution:doc().
update_items_in_processing(AtmTaskExecutionId, Num) ->
    Diff = fun
        (#atm_task_execution{status = ?PENDING_STATUS, items_in_processing = 0} = AtmTaskExecution) ->
            {ok, AtmTaskExecution#atm_task_execution{
                status = ?ACTIVE_STATUS,
                items_in_processing = Num
            }};
        (#atm_task_execution{status = ?ACTIVE_STATUS, items_in_processing = Current} = AtmTaskExecution) ->
            {ok, AtmTaskExecution#atm_task_execution{items_in_processing = Current + Num}};
        (_) ->
            {error, already_ended}
    end,
    case atm_task_execution:update(AtmTaskExecutionId, Diff) of
        {ok, AtmTaskExecutionDoc} ->
            handle_status_change(AtmTaskExecutionDoc),
            AtmTaskExecutionDoc;
        {error, already_ended} ->
            throw(?ERROR_ATM_TASK_EXECUTION_ENDED)
    end.


%% @private
-spec update_items_processed(atm_task_execution:id()) -> ok.
update_items_processed(AtmTaskExecutionId) ->
    {ok, _} = atm_task_execution:update(AtmTaskExecutionId, fun(#atm_task_execution{
        items_in_processing = ItemsInProcessing,
        items_processed = ItemsProcessed
    } = AtmTaskExecution) ->
        {ok, AtmTaskExecution#atm_task_execution{
            items_in_processing = ItemsInProcessing - 1,
            items_processed = ItemsProcessed + 1
        }}
    end),
    ok.


%% @private
-spec update_items_failed_and_processed(atm_task_execution:id()) -> ok.
update_items_failed_and_processed(AtmTaskExecutionId) ->
    update_items_failed_and_processed(AtmTaskExecutionId, 1).


%% @private
-spec update_items_failed_and_processed(atm_task_execution:id(), pos_integer()) ->
    ok.
update_items_failed_and_processed(AtmTaskExecutionId, Inc) ->
    {ok, _} = atm_task_execution:update(AtmTaskExecutionId, fun(#atm_task_execution{
        items_in_processing = ItemsInProcessing,
        items_processed = ItemsProcessed,
        items_failed = ItemsFailed
    } = AtmTaskExecution) ->
        {ok, AtmTaskExecution#atm_task_execution{
            items_in_processing = ItemsInProcessing - Inc,
            items_processed = ItemsProcessed + Inc,
            items_failed = ItemsFailed + Inc
        }}
    end),
    ok.


%% @private
-spec handle_supplementary_results_processing_error(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    errors:error()
) ->
    ok.
handle_supplementary_results_processing_error(
    AtmWorkflowExecutionCtx,
    AtmTaskExecutionId,
    Error
) ->
    UpdateResult = atm_task_execution:update(AtmTaskExecutionId, fun
        (AtmTaskExecution = #atm_task_execution{aborting_reason = undefined}) ->
            {ok, AtmTaskExecution#atm_task_execution{aborting_reason = failure}};
        (_) ->
            {error, already_aborting}
    end),

    case UpdateResult of
        {ok, #document{value = #atm_task_execution{
            workflow_execution_id = AtmWorkflowExecutionId,
            lane_index = AtmLaneIndex,
            run_num = RunNum
        }}} ->
            log_supplementary_results_processing_error(
                AtmWorkflowExecutionCtx,
                AtmTaskExecutionId,
                Error
            ),
            atm_lane_execution_status:handle_aborting(
                {AtmLaneIndex, RunNum},
                AtmWorkflowExecutionId,
                failure
            ),
            ok;

        {error, already_aborting} ->
            ok
    end.


%% @private
-spec log_supplementary_results_processing_error(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    errors:error()
) ->
    ok.
log_supplementary_results_processing_error(
    AtmWorkflowExecutionCtx,
    AtmTaskExecutionId,
    Error
) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),

    TaskLog = #{
        <<"description">> => <<"Failed to process supplementary results.">>,
        <<"reason">> => errors:to_json(Error)
    },
    atm_workflow_execution_logger:task_critical(TaskLog, Logger),

    atm_workflow_execution_logger:workflow_critical(
        "Failed to process supplementary results for task '~s'.",
        [AtmTaskExecutionId],
        Logger
    ).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates atm task execution status stored in atm parallel box execution of
%% corresponding atm lane run if it was changed in task execution doc.
%%
%% NOTE: normally this should happen only after lane run processing has started
%% and concrete 'run_num' was set for all its tasks (it is not possible to
%% foresee what it will be beforehand as previous lane run may retried numerous
%% times). However, in case of failure/interruption during lane run preparation
%% after task execution documents have been created, this function will also
%% be called. Despite not having 'run_num' set there is no ambiguity to which
%% lane run it belongs as it can only happen to the newest run of given lane.
%% @end
%%--------------------------------------------------------------------
-spec handle_status_change(atm_task_execution:doc()) -> ok.
handle_status_change(#document{value = #atm_task_execution{status_changed = false}}) ->
    ok;
handle_status_change(#document{
    key = AtmTaskExecutionId,
    value = #atm_task_execution{
        workflow_execution_id = AtmWorkflowExecutionId,
        lane_index = AtmLaneIndex,
        run_num = RunNumOrUndefined,
        parallel_box_index = AtmParallelBoxIndex,
        status = NewStatus,
        status_changed = true
    }
}) ->
    RunSelector = utils:ensure_defined(RunNumOrUndefined, current),

    ok = atm_lane_execution_status:handle_task_status_change(
        AtmWorkflowExecutionId, {AtmLaneIndex, RunSelector}, AtmParallelBoxIndex,
        AtmTaskExecutionId, NewStatus
    ).
