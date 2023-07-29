%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles the task execution process (for information about
%%% state machine @see 'atm_task_execution_status.erl').
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_handler).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    run_job_batch/4,
    process_job_batch_result/4,
    process_streamed_data/3,
    trigger_stream_conclusion/2
]).


-define(LOG_TERM_SIZE_LIMIT, 1000).

-define(workflow_log(__DESCRIPTION, __ATM_TASK_EXECUTION_ID),
    ?workflow_log(__DESCRIPTION, undefined, __ATM_TASK_EXECUTION_ID)
).
-define(workflow_log(__DESCRIPTION, __DETAILS, __ATM_TASK_EXECUTION_ID), #atm_workflow_log_schema{
    selector = {task, __ATM_TASK_EXECUTION_ID},
    description = __DESCRIPTION,
    details = __DETAILS,
    referenced_tasks = [__ATM_TASK_EXECUTION_ID]
}).


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


-spec process_streamed_data(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    atm_task_executor:streamed_data()
) ->
    ok | error.
process_streamed_data(AtmWorkflowExecutionCtx, AtmTaskExecutionId, {chunk, UncorrelatedResults}) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    ?atm_task_debug(Logger, #{
        <<"description">> => <<"Processing streamed results...">>,
        <<"details">> => #{<<"results">> => ensure_log_term_size_not_exceeded(UncorrelatedResults)}
    }),

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
            ?examine_exception(Type, Reason, Stacktrace)
        ),
        error
    end;

process_streamed_data(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Error = {error, _}) ->
    handle_uncorrelated_results_processing_error(
        AtmWorkflowExecutionCtx, AtmTaskExecutionId, Error
    ),
    error.


-spec trigger_stream_conclusion(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id()
) ->
    ok | error.
trigger_stream_conclusion(AtmWorkflowExecutionCtx, AtmTaskExecutionId) ->
    {ok, #document{value = AtmTaskExecution}} = atm_task_execution:get(AtmTaskExecutionId),
    AtmTaskExecutor = AtmTaskExecution#atm_task_execution.executor,

    atm_task_executor:trigger_stream_conclusion(AtmWorkflowExecutionCtx, AtmTaskExecutor).


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
            <<"itemBatch">> => ensure_log_term_size_not_exceeded(lists:map(
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
        <<"details">> => #{<<"argsBatch">> => ensure_log_term_size_not_exceeded(ArgsBatch)}
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
    atm_task_execution_status:handle_items_failed(AtmTaskExecutionId, length(ItemBatch)),

    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),

    ?atm_task_error(Logger, #{
        <<"description">> => <<"Failed to process batch of items.">>,
        <<"details">> => #{
            <<"reason">> => case Error of
                ?ERROR_ATM_JOB_BATCH_CRASHED(Reason) -> Reason;
                _ -> errors:to_json(Error)
            end,
            <<"itemBatch">> => ensure_log_term_size_not_exceeded(lists:map(
                fun item_execution_to_json/1, ItemBatch
            ))
        }
    }).


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
            <<"item">> => ensure_log_term_size_not_exceeded(item_execution_to_json(Item)),
            <<"results">> => JobResults
        }
    }),

    try
        atm_task_execution_results:consume_results(
            AtmWorkflowExecutionCtx,
            item_related,
            AtmTaskExecution#atm_task_execution.item_related_result_specs,
            JobResults
        ),
        atm_task_execution_status:handle_item_processed(AtmTaskExecutionId)
    catch Type:Reason:Stacktrace ->
        Error = ?examine_exception(Type, Reason, Stacktrace),
        handle_job_processing_error(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Error),
        error
    end.


%% @private
-spec handle_job_processing_error(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    atm_workflow_execution_handler:item(),
    errors:error() | json_utils:json_map()
) ->
    ok.
handle_job_processing_error(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Item, Error) ->
    atm_task_execution_status:handle_items_failed(AtmTaskExecutionId, 1),

    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),

    ?atm_task_error(Logger, case Error of
        _LambdaException = #{<<"exception">> := Reason} ->
            #{
                <<"description">> => <<"Lambda exception occurred during item processing.">>,
                <<"details">> => #{
                    <<"reason">> => Reason,
                    <<"item">> => ensure_log_term_size_not_exceeded(item_execution_to_json(Item))
                }
            };
        _SystemError = {error, _} ->
            #{
                <<"description">> => <<"Failed to process item.">>,
                <<"details">> => #{
                    <<"reason">> => errors:to_json(Error),
                    <<"item">> => ensure_log_term_size_not_exceeded(item_execution_to_json(Item))
                }
            }
    end).


%% @private
-spec item_execution_to_json(atm_workflow_execution_handler:item()) ->
    json_utils:json_term().
item_execution_to_json(#atm_item_execution{trace_id = TraceId, value = Value}) ->
    #{<<"traceId">> => TraceId, <<"value">> => Value}.


%% @private
-spec ensure_log_term_size_not_exceeded(term()) -> term() | binary().
ensure_log_term_size_not_exceeded(Bin) when is_binary(Bin), byte_size(Bin) =< ?LOG_TERM_SIZE_LIMIT ->
    Bin;
ensure_log_term_size_not_exceeded(TooLongBinary) when is_binary(TooLongBinary) ->
    trim_binary(TooLongBinary);
ensure_log_term_size_not_exceeded(Term) ->
    case json_utils:encode(Term) of
        TooLongBinary when byte_size(TooLongBinary) > ?LOG_TERM_SIZE_LIMIT ->
            trim_binary(TooLongBinary);
        _ ->
            Term
    end.


%% @private
-spec trim_binary(binary()) -> binary().
trim_binary(TooLongBinary) ->
    TrimmedBinary = binary:part(TooLongBinary, 0, ?LOG_TERM_SIZE_LIMIT),
    <<TrimmedBinary/binary, "...">>.


%% @private
-spec handle_uncorrelated_results_processing_error(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    errors:error()
) ->
    ok.
handle_uncorrelated_results_processing_error(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Error) ->
    case atm_task_execution_status:handle_stopping(
        AtmTaskExecutionId, failure, get_incarnation(AtmWorkflowExecutionCtx)
    ) of
        {ok, #document{value = #atm_task_execution{
            lane_index = AtmLaneIndex,
            run_num = RunNum,
            executor = AtmTaskExecutor
        }}} ->
            atm_task_executor:abort(AtmWorkflowExecutionCtx, AtmTaskExecutor),

            log_uncorrelated_results_processing_error(
                AtmWorkflowExecutionCtx, AtmTaskExecutionId, Error
            ),
            {ok, _} = atm_lane_execution_handler:init_stop(
                {AtmLaneIndex, RunNum}, failure, AtmWorkflowExecutionCtx
            ),
            ok;

        {error, task_already_stopping} ->
            ok;

        {error, task_already_stopped} ->
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

    ?atm_task_critical(Logger, #{
        <<"description">> => <<"Failed to process streamed results.">>,
        <<"details">> => #{<<"reason">> => errors:to_json(Error)}
    }),
    ?atm_workflow_critical(Logger, ?workflow_log(
        <<"Failed to process streamed results.">>, AtmTaskExecutionId
    )).


%% @private
-spec get_incarnation(atm_workflow_execution_ctx:record()) -> atm_workflow_execution:incarnation().
get_incarnation(AtmWorkflowExecutionCtx) ->
    atm_workflow_execution_ctx:get_workflow_execution_incarnation(AtmWorkflowExecutionCtx).
