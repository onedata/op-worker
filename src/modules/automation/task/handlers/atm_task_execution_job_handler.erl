%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021-2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles the task execution job (for information about
%%% state machine @see 'atm_task_execution_status.erl').
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_job_handler).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    run_job_batch/4,
    process_job_batch_result/4
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec run_job_batch(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    atm_task_executor:job_batch_id(),
    [atm_workflow_execution_handler:item()]
) ->
    ok | {error, running_item_failed} | {error, task_already_stopping} | {error, task_already_stopped}.
run_job_batch(
    AtmWorkflowExecutionCtx,
    AtmTaskExecutionId,
    AtmJobBatchId,
    ItemBatch
) ->
    ItemCount = length(ItemBatch),
    case atm_task_execution_status:handle_items_in_processing(AtmTaskExecutionId, ItemCount) of
        {ok, AtmTaskExecutionDoc} ->
            try
                run_job_batch_insecure(
                    AtmWorkflowExecutionCtx, AtmTaskExecutionDoc, AtmJobBatchId, ItemBatch
                )
            catch Type:Reason:Stacktrace ->
                handle_job_batch_processing_error(
                    AtmWorkflowExecutionCtx, AtmTaskExecutionId, ItemBatch,
                    ?examine_exception(Type, Reason, Stacktrace)
                ),
                {error, running_item_failed}
            end;

        {error, _} = Error ->
            Error
    end.


-spec process_job_batch_result(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    [atm_workflow_execution_handler:item()],
    atm_task_executor:job_batch_result()
) ->
    ok | error.
process_job_batch_result(AtmWorkflowExecutionCtx, AtmTaskExecutionId, ItemBatch, JobBatchResult) ->
    case parse_job_batch_result(ItemBatch, JobBatchResult) of
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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec run_job_batch_insecure(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:doc(),
    atm_task_executor:job_batch_id(),
    [atm_workflow_execution_handler:item()]
) ->
    ok | {error, running_item_failed} | {error, task_already_stopping} | {error, task_already_stopped}.
run_job_batch_insecure(
    AtmWorkflowExecutionCtx,
    #document{key = AtmTaskExecutionId, value = AtmTaskExecution = #atm_task_execution{
        executor = AtmTaskExecutor
    }},
    AtmJobBatchId,
    ItemBatch
) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    ?atm_task_debug(Logger, #{
        <<"description">> => <<"Running task for items...">>,
        <<"details">> => #{
            <<"itemBatch">> => ?ensure_log_term_size_not_exceeded(lists:map(
                fun item_execution_to_json/1, ItemBatch
            ))
        }
    }),

    AtmRunJobBatchCtx = atm_run_job_batch_ctx:build(AtmWorkflowExecutionCtx, AtmTaskExecution),
    AtmLambdaInput = build_lambda_input(AtmJobBatchId, AtmRunJobBatchCtx, ItemBatch, AtmTaskExecution),

    case atm_task_executor:run(AtmRunJobBatchCtx, AtmLambdaInput, AtmTaskExecutor) of
        ok ->
            ok;
        #atm_lambda_output{} = AtmLambdaOutput ->
            case process_job_batch_result(
                AtmWorkflowExecutionCtx, AtmTaskExecutionId, ItemBatch, {ok, AtmLambdaOutput}
            ) of
                ok -> ok;
                error -> {error, running_item_failed}
            end
    end.


%% @private
-spec build_lambda_input(
    atm_task_executor:job_batch_id(),
    atm_run_job_batch_ctx:record(),
    [atm_workflow_execution_handler:item()],
    atm_task_execution:record()
) ->
    atm_task_executor:lambda_input().
build_lambda_input(AtmJobBatchId, AtmRunJobBatchCtx, ItemBatch, #atm_task_execution{
    lambda_execution_config_entries = AtmLambdaExecutionConfigEntries,
    argument_specs = AtmTaskExecutionArgSpecs
}) ->
    AtmWorkflowExecutionCtx = atm_run_job_batch_ctx:get_workflow_execution_ctx(AtmRunJobBatchCtx),

    AtmLambdaExecutionConfig = atm_lambda_execution_config_entries:acquire_config(
        AtmRunJobBatchCtx, AtmLambdaExecutionConfigEntries
    ),

    %% TODO VFS-8668 optimize argsBatch creation
    ArgsBatch = atm_parallel_runner:map(fun(Item) ->
        atm_task_execution_arguments:acquire_args(
            Item, AtmRunJobBatchCtx, AtmTaskExecutionArgSpecs
        )
    end, ItemBatch),

    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    ?atm_task_debug(Logger, #{
        <<"description">> => <<"Lambda's input argsBatch created.">>,
        <<"details">> => #{<<"argsBatch">> => ?ensure_log_term_size_not_exceeded(ArgsBatch)}
    }),

    #atm_lambda_input{
        workflow_execution_id = atm_run_job_batch_ctx:get_workflow_execution_id(AtmRunJobBatchCtx),
        log_level = atm_workflow_execution_ctx:get_log_level_int(AtmWorkflowExecutionCtx),
        job_batch_id = AtmJobBatchId,
        config = AtmLambdaExecutionConfig,
        args_batch = ArgsBatch
    }.


%% @private
-spec parse_job_batch_result(
    [atm_workflow_execution_handler:item()],
    atm_task_executor:job_batch_result()
) ->
    {ok, [{atm_workflow_execution_handler:item(), atm_task_executor:job_results()}]} | errors:error().
parse_job_batch_result(_ItemBatch, Error = {error, _}) ->
    % Entire batch processing failed (e.g. timeout or malformed lambda response)
    Error;

parse_job_batch_result(ItemBatch, {ok, #atm_lambda_output{results_batch = undefined}}) ->
    {ok, lists:map(fun(Item) -> {Item, #{}} end, ItemBatch)};

parse_job_batch_result(ItemBatch, {ok, #atm_lambda_output{results_batch = ResultsBatch}}) when
    length(ItemBatch) == length(ResultsBatch)
->
    {ok, lists:zipwith(fun
        (Item, undefined) ->
            {Item, #{}};
        (Item, Result) when is_map(Result) ->
            {Item, Result};
        (Item, Result) ->
            {Item, ?ERROR_BAD_DATA(<<"lambdaResultsForItem">>, str_utils:format_bin(
                "Expected object with result names matching those defined in task schema. "
                "Instead got: ~s", [json_utils:encode(Result)]
            ))}
    end, ItemBatch, ResultsBatch)};

parse_job_batch_result(_ItemBatch, {ok, #atm_lambda_output{results_batch = ResultsBatch}}) ->
    ?ERROR_BAD_DATA(<<"lambdaOutput.resultsBatch">>, str_utils:format_bin(
        "Expected array with object for each item in 'argsBatch' provided to lambda. "
        "Instead got: ~s", [json_utils:encode(ResultsBatch)]
    )).


%% @private
-spec handle_job_batch_processing_error(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    [atm_workflow_execution_handler:item()],
    errors:error()
) ->
    ok.
handle_job_batch_processing_error(
    AtmWorkflowExecutionCtx,
    AtmTaskExecutionId,
    ItemBatch,
    ?ERROR_ATM_JOB_BATCH_WITHDRAWN(Reason)
) ->
    case atm_task_execution_status:handle_items_withdrawn(AtmTaskExecutionId, length(ItemBatch)) of
        {ok, _} ->
            ok;
        {error, task_not_stopping} ->
            % items withdrawal caused not by stopping execution is treated as error
            handle_job_batch_processing_error(
                AtmWorkflowExecutionCtx, AtmTaskExecutionId, ItemBatch,
                ?ERROR_ATM_JOB_BATCH_CRASHED(Reason)
            )
    end;

handle_job_batch_processing_error(
    AtmWorkflowExecutionCtx,
    AtmTaskExecutionId,
    ItemBatch,
    Error
) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),

    ?atm_task_error(Logger, #{
        <<"description">> => <<"Failed to process batch of items.">>,
        <<"details">> => #{
            <<"reason">> => case Error of
                ?ERROR_ATM_JOB_BATCH_CRASHED(Reason) -> Reason;
                _ -> errors:to_json(Error)
            end
        },
        <<"referencedElements">> => #{
            <<"itemTraceIds">> => lists:map(fun(Item) ->
                Item#atm_item_execution.trace_id
            end, ItemBatch)
        }
    }),

    handle_items_failed(AtmWorkflowExecutionCtx, AtmTaskExecutionId, length(ItemBatch)).


%% @private
-spec process_job_results(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    atm_workflow_execution_handler:item(),
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

    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),

    ?atm_task_debug(Logger, #{
        <<"description">> => <<"Processing results for item...">>,
        <<"details">> => #{
            <<"item">> => ?ensure_log_term_size_not_exceeded(item_execution_to_json(Item)),
            <<"results">> => JobResults
        }
    }),

    try
        consume_job_results(AtmWorkflowExecutionCtx, AtmTaskExecution, JobResults),
        atm_task_execution_status:handle_item_processed(AtmTaskExecutionId)
    catch Type:Reason:Stacktrace ->
        Error = ?examine_exception(Type, Reason, Stacktrace),

        handle_job_processing_error(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Error),
        error
    end.


%% @private
-spec consume_job_results(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:record(),
    atm_task_executor:job_results()
) ->
    ok | error | no_return().
consume_job_results(AtmWorkflowExecutionCtx, AtmTaskExecution, JobResults) ->
    atm_task_execution_results:consume_results(
        AtmWorkflowExecutionCtx,
        item_related,
        AtmTaskExecution#atm_task_execution.item_related_result_specs,
        JobResults
    ).


%% @private
-spec handle_job_processing_error(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    atm_workflow_execution_handler:item(),
    errors:error() | json_utils:json_map()
) ->
    ok.
handle_job_processing_error(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Error) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),

    ?atm_task_error(Logger, case Error of
        _LambdaException = #{<<"exception">> := Reason} ->
            #{
                <<"description">> => <<"Lambda exception occurred during item processing.">>,
                <<"details">> => #{
                    <<"reason">> => Reason
                },
                <<"referencedElements">> => #{
                    <<"itemTraceIds">> => [Item#atm_item_execution.trace_id]
                }
            };
        _SystemError = {error, _} ->
            #{
                <<"description">> => <<"Failed to process item.">>,
                <<"details">> => #{
                    <<"reason">> => errors:to_json(Error)
                },
                <<"referencedElements">> => #{
                    <<"itemTraceIds">> => [Item#atm_item_execution.trace_id]
                }
            }
    end),

    handle_items_failed(AtmWorkflowExecutionCtx, AtmTaskExecutionId, 1).


%% @private
-spec handle_items_failed(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    pos_integer()
) ->
    ok.
handle_items_failed(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Count) ->
    AtmTaskExecutionDoc = atm_task_execution_status:handle_items_failed(AtmTaskExecutionId, Count),

    case atm_workflow_execution_ctx:is_lane_run_instant_failure_exception_threshold_breached(
        calc_task_exception_ratio(AtmTaskExecutionDoc), AtmWorkflowExecutionCtx
    ) of
        true ->
            handle_exceeded_lane_run_fail_for_exceptions_ratio(
                AtmWorkflowExecutionCtx, AtmTaskExecutionId
            );
        false ->
            ok
    end.


%% @private
-spec calc_task_exception_ratio(atm_task_execution:doc()) -> float().
calc_task_exception_ratio(#document{value = #atm_task_execution{
    items_in_processing = ItemsInProcessing,
    items_processed = ItemsProcessed,
    items_failed = ItemsFailed
}}) ->
    ItemsFailed / (ItemsInProcessing + ItemsProcessed).


%% @private
-spec handle_exceeded_lane_run_fail_for_exceptions_ratio(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id()
) ->
    ok.
handle_exceeded_lane_run_fail_for_exceptions_ratio(AtmWorkflowExecutionCtx, AtmTaskExecutionId) ->
    case atm_task_execution_stop_handler:init_stop(
        AtmWorkflowExecutionCtx, AtmTaskExecutionId, failure, false
    ) of
        {ok, #document{value = AtmTaskExecution}} ->
            Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
            ?atm_task_critical(Logger, <<"Exceeeded allowed failed jobs threshold.">>),
            ?atm_workflow_critical(Logger, ?ATM_WORKFLOW_TASK_LOG(AtmTaskExecutionId, <<
                "Exceeeded allowed failed jobs threshold."  % TODO desc
            >>)),

            init_lane_run_stop(AtmWorkflowExecutionCtx, AtmTaskExecution, failure);

        {error, Reason} when Reason =:= task_already_stopping; Reason =:= task_already_stopped ->
            ok
    end.


%% @private
-spec init_lane_run_stop(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:record(),
    atm_lane_execution:run_stopping_reason()
) ->
    ok.
init_lane_run_stop(AtmWorkflowExecutionCtx, #atm_task_execution{
    lane_index = AtmLaneIndex,
    run_num = RunNum
}, Reason) ->
    {ok, _} = atm_lane_execution_stop_handler:init_stop(
        {AtmLaneIndex, RunNum}, Reason, AtmWorkflowExecutionCtx
    ),
    ok.


%% @private
-spec item_execution_to_json(atm_workflow_execution_handler:item()) ->
    json_utils:json_term().
item_execution_to_json(#atm_item_execution{trace_id = TraceId, value = Value}) ->
    #{<<"traceId">> => TraceId, <<"value">> => Value}.
