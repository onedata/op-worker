%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module that handles the lifecycle of a result stream, by processing
%%% incoming result stream reports and coordinating streaming conclusion.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_result_stream_handler).
-author("Lukasz Opiola").

-include("modules/automation/atm_execution.hrl").


%% API
-export([consume_report/3]).
-export([handle_reporting_error_if_related/2]).
-export([trigger_conclusion/2]).


% state related with a single activity feed server connection,
% holding information about the context of connected result streamer client
% specialization of the atm_openfaas_activity_feed_ws_handler:handler_state() type
-record(result_streamer_context, {
    workflow_execution_id :: atm_workflow_execution:id(),
    task_execution_id :: atm_task_execution:id(),
    result_streamer_id :: atm_openfaas_result_streamer_registry:result_streamer_id()
}).


%%%===================================================================
%%% API functions
%%%===================================================================

-spec consume_report(
    atm_openfaas_activity_feed_ws_handler:connection_ref(),
    atm_openfaas_activity_feed_ws_handler:handler_state(),
    atm_openfaas_result_streamer_report:record()
) -> atm_openfaas_activity_feed_ws_handler:handler_state().
consume_report(ConnRef, _HandlerState, #atm_openfaas_result_streamer_registration_report{
    workflow_execution_id = WorkflowExecutionId,
    task_execution_id = TaskExecutionId,
    result_streamer_id = ResultStreamerId
}) ->
    atm_openfaas_result_streamer_registry:register(
        WorkflowExecutionId, TaskExecutionId, ResultStreamerId, ConnRef
    ),
    #result_streamer_context{
        workflow_execution_id = WorkflowExecutionId,
        task_execution_id = TaskExecutionId,
        result_streamer_id = ResultStreamerId
    };
consume_report(ConnRef, HandlerState, #atm_openfaas_result_streamer_chunk_report{chunk = Chunk}) ->
    #result_streamer_context{
        workflow_execution_id = WorkflowExecutionId,
        task_execution_id = TaskExecutionId,
        result_streamer_id = ResultStreamerId
    } = HandlerState,
    case atm_openfaas_result_streamer_registry:has(WorkflowExecutionId, TaskExecutionId, ResultStreamerId, ConnRef) of
        false ->
            % reports that are late (after deregistration) or from a previous incarnation of the streamer are
            % ignored - such situation can only happen when there has been an anomaly and the stream will
            % anyway conclude with failure, so no special handling of this situation is required
            ?warning(
                "Ignoring a stale report by result streamer ~s for workflow execution ~s and task execution ~s",
                [ResultStreamerId, WorkflowExecutionId, TaskExecutionId]
            );
        true ->
            ok = workflow_engine:stream_task_data(WorkflowExecutionId, TaskExecutionId, {chunk, Chunk})
    end,
    HandlerState;
consume_report(ConnRef, HandlerState, #atm_openfaas_result_streamer_deregistration_report{}) ->
    #result_streamer_context{
        workflow_execution_id = WorkflowExecutionId,
        task_execution_id = TaskExecutionId,
        result_streamer_id = ResultStreamerId
    } = HandlerState,
    atm_openfaas_result_streamer_registry:deregister(
        WorkflowExecutionId, TaskExecutionId, ResultStreamerId, ConnRef
    ),
    HandlerState.


-spec handle_reporting_error_if_related(atm_openfaas_activity_feed_ws_handler:handler_state(), errors:error()) ->
    ok.
handle_reporting_error_if_related(#result_streamer_context{
    workflow_execution_id = WorkflowExecutionId,
    task_execution_id = TaskExecutionId
}, Error) ->
    ok = workflow_engine:stream_task_data(WorkflowExecutionId, TaskExecutionId, Error);
handle_reporting_error_if_related(_, _) ->
    ok.


-spec trigger_conclusion(atm_workflow_execution:id(), atm_task_execution:id()) -> ok.
trigger_conclusion(WorkflowExecutionId, TaskExecutionId) ->
    spawn(fun() ->
        StreamClosingResult = try
            conclude(WorkflowExecutionId, TaskExecutionId)
        catch Class:Reason:Stacktrace ->
            ?error_stacktrace(
                "Unexpected error while concluding task data stream for workflow execution ~s and task execution ~s~n"
                "Error was: ~w:~p",
                [WorkflowExecutionId, TaskExecutionId, Class, Reason],
                Stacktrace
            ),
            {failure, ?ERROR_INTERNAL_SERVER_ERROR}
        end,
        catch atm_openfaas_result_streamer_registry:clear(WorkflowExecutionId, TaskExecutionId),
        workflow_engine:report_task_data_streaming_concluded(WorkflowExecutionId, TaskExecutionId, StreamClosingResult)
    end),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec conclude(atm_workflow_execution:id(), atm_task_execution:id()) -> workflow_engine:stream_closing_result().
conclude(WorkflowExecutionId, TaskExecutionId) ->
    case atm_openfaas_result_streamer_registry:claim_conclusion_orchestration(WorkflowExecutionId, TaskExecutionId, self()) of
        no_streamers_ever_registered ->
            ?error(
                "Conclusion of result stream was triggered for workflow execution ~s and task execution ~s, "
                "but no streamers were ever registered",
                [WorkflowExecutionId, TaskExecutionId]
            ),
            {failure, ?ERROR_INTERNAL_SERVER_ERROR};
        all_streamers_deregistered ->
            success;
        {active_result_streamers, ConnRefs} ->
            EncodedFinalizationSignalJson = jsonable_record:to_json(
                #atm_openfaas_result_streamer_finalization_signal{},
                atm_openfaas_result_streamer_finalization_signal
            ),
            lists:foreach(fun(ConnRef) ->
                atm_openfaas_activity_feed_ws_connection:push_json_to_client(ConnRef, EncodedFinalizationSignalJson)
            end, ConnRefs),
            atm_openfaas_result_streamer_registry:await_deregistration_of_all_streamers()
    end.
