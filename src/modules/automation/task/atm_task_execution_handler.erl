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
    process_outcome/4,

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
            system_audit_log_id = AtmSystemAuditLogId,
            time_series_store_id = AtmTaskTSStoreId
        }
    } = ensure_atm_task_execution_doc(AtmTaskExecutionIdOrDoc),

    {ok, #document{value = AtmWorkflowExecution}} = atm_workflow_execution:get(AtmWorkflowExecutionId),
    AtmTaskSchema = get_task_schema(AtmTaskExecution, AtmWorkflowExecution),
    AtmLambdaRevision = get_lambda_revision(AtmTaskSchema, AtmWorkflowExecution),

    AtmTaskExecutionSpec = atm_task_executor:initiate(
        AtmWorkflowExecutionCtx, AtmTaskSchema, AtmLambdaRevision, AtmTaskExecutor
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


-spec teardown(atm_lane_execution_handler:teardown_ctx(), atm_task_execution:id()) ->
    ok | no_return().
teardown(AtmLaneExecutionRunTeardownCtx, AtmTaskExecutionId) ->
    #document{value = #atm_task_execution{
        executor = AtmTaskExecutor
    }} = ensure_atm_task_execution_doc(AtmTaskExecutionId),

    atm_task_executor:teardown(AtmLaneExecutionRunTeardownCtx, AtmTaskExecutor).


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
    ok | error | no_return().
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
        Input = #{
            <<"ctx">> => #{<<"heartbeatUrl">> => HeartbeatUrl},
            %% TODO VFS-8668 optimize argsBatch creation
            <<"argsBatch">> => atm_parallel_runner:map(fun(Item) ->
                atm_task_execution_arguments:construct_args(Item, AtmJobCtx, AtmTaskExecutionArgSpecs)
            end, ItemsBatch)
        },
        atm_task_executor:run(AtmJobCtx, Input, AtmTaskExecutor)
    catch Type:Reason:Stacktrace ->
        Error = ?atm_examine_error(Type, Reason, Stacktrace),
        process_outcome(AtmWorkflowExecutionCtx, AtmTaskExecutionId, ItemsBatch, Error)
    end.


-spec process_outcome(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    [automation:item()],
    atm_task_executor:outcome()
) ->
    ok | error | no_return().
process_outcome(AtmWorkflowExecutionCtx, AtmTaskExecutionId, ItemsBatch, Outcome) ->
    AnyExceptionOccurred = lists:member(error, atm_parallel_runner:map(fun({Item, Result}) ->
        process_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Result)
    end, zip_items_with_results(ItemsBatch, Outcome))),

    case AnyExceptionOccurred of
        true -> error;
        false -> ok
    end.


-spec handle_ended(atm_task_execution:id()) -> ok.
handle_ended(AtmTaskExecutionId) ->
    Diff = fun
        (#atm_task_execution{status = ?PENDING_STATUS} = AtmTaskExecution) ->
            {ok, AtmTaskExecution#atm_task_execution{status = ?SKIPPED_STATUS}};

        (#atm_task_execution{
            status = ?ACTIVE_STATUS,
            items_in_processing = ItemsInProcessing,
            items_processed = ItemsProcessed,
            items_failed = ItemsFailed
        } = AtmTaskExecution) ->
            % atm workflow execution may have been abruptly interrupted by e.g.
            % provider restart which resulted in stale `items_in_processing`
            NewAtmTaskExecution = case ItemsFailed + ItemsInProcessing of
                0 ->
                    AtmTaskExecution#atm_task_execution{status = ?FINISHED_STATUS};
                AllFailedItems ->
                    AtmTaskExecution#atm_task_execution{
                        status = ?FAILED_STATUS,
                        items_in_processing = 0,
                        items_processed = ItemsProcessed + ItemsInProcessing,
                        items_failed = AllFailedItems
                    }
            end,
            {ok, NewAtmTaskExecution};

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
-spec zip_items_with_results([automation:item()], atm_task_executor:outcome()) ->
    [{automation:item(), atm_task_executor:results()}].
zip_items_with_results(ItemsBatch, #{<<"resultsBatch">> := ResultsBatch}) when
    is_list(ResultsBatch),
    length(ItemsBatch) == length(ResultsBatch)
->
    lists:zipwith(fun
        (Item, Result) when is_map(Result) ->
            {Item, Result};
        (Item, Result) ->
            {Item, ?ERROR_BAD_DATA(<<"lambdaResult">>, str_utils:format_bin(
                "Expected object with keys and values matching those defined in task schema. "
                "Instead got: ~s", [json_utils:encode(Result)]
            ))}
    end, ItemsBatch, ResultsBatch);

zip_items_with_results(ItemsBatch, {error, _} = Error) ->
    % Entire batch processing failed (e.g. timeout or malformed lambda response)
    [{Item, Error} || Item <- ItemsBatch];

zip_items_with_results(ItemsBatch, MalformedOutcome) ->
    Error = ?ERROR_BAD_DATA(<<"lambdaOutcome">>, str_utils:format_bin(
        "Expected '{\"resultsBatch\": [$LAMBDA_RESULT]}' object with $LAMBDA_RESULT "
        "for each item (~B) in 'argsBatch' provided to lambda. Instead got: ~s",
        [length(ItemsBatch), json_utils:encode(MalformedOutcome)]
    )),
    [{Item, Error} || Item <- ItemsBatch].


-spec process_results(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    automation:item(),
    atm_task_executor:results()
) ->
    ok | error | no_return().
process_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, {error, _} = Error) ->
    handle_exception(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Error);

process_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, #{<<"exception">> := _} = Exception) ->
    handle_exception(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Exception);

process_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Results) ->
    {ok, #document{value = #atm_task_execution{
        result_specs = AtmTaskExecutionResultSpecs
    }}} = atm_task_execution:get(AtmTaskExecutionId),

    try
        atm_task_execution_results:consume_results(AtmWorkflowExecutionCtx, AtmTaskExecutionResultSpecs, Results),
        update_items_processed(AtmTaskExecutionId)
    catch Type:Reason:Stacktrace ->
        Error = ?atm_examine_error(Type, Reason, Stacktrace),
        handle_exception(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Error)
    end.


%% @private
-spec handle_exception(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    automation:item(),
    errors:error() | json_utils:json_map()
) ->
    error.
handle_exception(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, ItemProcessingError) ->
    update_items_failed_and_processed(AtmTaskExecutionId),

    ErrorLog = case ItemProcessingError of
        _LambdaException = #{<<"exception">> := Reason} ->
            #{
                <<"description">> => <<"Lambda exception occurred during item processing.">>,
                <<"reason">> => Reason
            };
        _SystemError = {error, _} ->
            #{
                <<"description">> => <<"Failed to process item.">>,
                <<"reason">> => errors:to_json(ItemProcessingError)
            }
    end,
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    atm_workflow_execution_logger:task_error(ErrorLog#{<<"item">> => Item}, Logger),

    error.


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
    {ok, _} = atm_task_execution:update(AtmTaskExecutionId, fun(#atm_task_execution{
        items_in_processing = ItemsInProcessing,
        items_processed = ItemsProcessed,
        items_failed = ItemsFailed
    } = AtmTaskExecution) ->
        {ok, AtmTaskExecution#atm_task_execution{
            items_in_processing = ItemsInProcessing - 1,
            items_processed = ItemsProcessed + 1,
            items_failed = ItemsFailed + 1
        }}
    end),
    ok.


%% @private
-spec handle_status_change(atm_task_execution:doc()) -> ok.
handle_status_change(#document{value = #atm_task_execution{status_changed = false}}) ->
    ok;
handle_status_change(#document{
    key = AtmTaskExecutionId,
    value = #atm_task_execution{
        workflow_execution_id = AtmWorkflowExecutionId,
        lane_index = AtmLaneIndex,
        run_num = RunNum,
        parallel_box_index = AtmParallelBoxIndex,
        status = NewStatus,
        status_changed = true
    }
}) ->
    ok = atm_lane_execution_status:handle_task_status_change(
        AtmWorkflowExecutionId, {AtmLaneIndex, RunNum}, AtmParallelBoxIndex,
        AtmTaskExecutionId, NewStatus
    ).
