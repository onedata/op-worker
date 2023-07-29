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
