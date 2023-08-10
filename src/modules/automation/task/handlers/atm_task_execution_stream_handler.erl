%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021-2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles the task execution streamed data (for information about
%%%%% state machine @see 'atm_task_execution_status.erl').
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_stream_handler).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    process_streamed_data/3,
    trigger_stream_conclusion/2
]).


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
    process_uncorrelated_results(
        AtmWorkflowExecutionCtx, AtmTaskExecutionId, UncorrelatedResults
    );

process_streamed_data(AtmWorkflowExecutionCtx, AtmTaskExecutionId, {error, _} = Error) ->
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
-spec process_uncorrelated_results(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    json_utils:json_map()
) ->
    ok | error.
process_uncorrelated_results(AtmWorkflowExecutionCtx, AtmTaskExecutionId, UncorrelatedResults) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    ?atm_task_debug(Logger, #{
        <<"description">> => <<"Processing streamed results...">>,
        <<"details">> => #{<<"results">> => ?ensure_log_term_size_not_exceeded(UncorrelatedResults)}
    }),

    {ok, #document{value = AtmTaskExecution}} = atm_task_execution:get(AtmTaskExecutionId),

    try
        consume_uncorrelated_results(
            AtmWorkflowExecutionCtx, AtmTaskExecution, UncorrelatedResults
        )
    catch Type:Reason:Stacktrace ->
        Error = ?examine_exception(Type, Reason, Stacktrace),

        handle_uncorrelated_results_processing_error(
            AtmWorkflowExecutionCtx, AtmTaskExecutionId, Error
        ),
        error
    end.


%% @private
-spec consume_uncorrelated_results(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    json_utils:json_map()
) ->
    ok | no_return().
consume_uncorrelated_results(AtmWorkflowExecutionCtx, AtmTaskExecution, UncorrelatedResults) ->
    maps:foreach(fun(ResultName, ResultArray) ->
        lists:foreach(fun(ResultValue) ->
            atm_task_execution_results:consume_results(
                AtmWorkflowExecutionCtx,
                uncorrelated,
                AtmTaskExecution#atm_task_execution.uncorrelated_result_specs,
                #{ResultName => ResultValue}
            )
        end, ResultArray)
    end, UncorrelatedResults).


%% @private
-spec handle_uncorrelated_results_processing_error(
    atm_workflow_execution_ctx:record(),
    atm_task_execution:id(),
    errors:error()
) ->
    ok.
handle_uncorrelated_results_processing_error(AtmWorkflowExecutionCtx, AtmTaskExecutionId, Error) ->
    case atm_task_execution_stop_handler:init_stop(
        AtmWorkflowExecutionCtx, AtmTaskExecutionId, failure, false
    ) of
        {ok, #document{value = AtmTaskExecution}} ->
            Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
            ?atm_task_critical(Logger, #{
                <<"description">> => <<"Failed to process streamed results.">>,
                <<"details">> => #{<<"reason">> => errors:to_json(Error)}
            }),
            ?atm_workflow_critical(Logger, ?ATM_WORKFLOW_TASK_LOG(AtmTaskExecutionId, <<
                "Failed to process streamed results."
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
