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


%% atm_openfaas_activity_report_handler callbacks
-export([consume_report/3]).
-export([handle_error/3]).

%% API
-export([trigger_conclusion/2]).


% state related with a single activity feed server connection,
% holding information about the context of connected result streamer client
% specialization of the atm_openfaas_activity_feed_ws_handler:handler_state() type
-record(result_streamer_context, {
    workflow_execution_id :: atm_workflow_execution:id(),
    task_execution_id :: atm_task_execution:id(),
    result_streamer_id :: atm_openfaas_result_streamer_registry:result_streamer_id()
}).

-define(FINALIZATION_SIGNAL_JSON, jsonable_record:to_json(
    #atm_openfaas_result_streamer_finalization_signal{},
    atm_openfaas_result_streamer_push_message
)).


%%%===================================================================
%%% atm_openfaas_activity_report_handler callbacks
%%%===================================================================

-spec consume_report(
    atm_openfaas_activity_feed_ws_handler:connection_ref(),
    atm_openfaas_activity_report:body(),
    atm_openfaas_activity_feed_ws_handler:handler_state()
) ->
    {no_reply | {reply_json, json_utils:json_term()}, atm_openfaas_activity_feed_ws_handler:handler_state()}.
consume_report(ConnRef, #atm_openfaas_result_streamer_report{
    id = ReportId,
    body = ReportBody
}, HandlerState) ->
    NewHandlerState = consume_result_streamer_report(ConnRef, ReportId, ReportBody, HandlerState),
    ReportAck = #atm_openfaas_result_streamer_report_ack{id = ReportId},
    ReplyJson = jsonable_record:to_json(ReportAck, atm_openfaas_result_streamer_push_message),
    {{reply_json, ReplyJson}, NewHandlerState}.


-spec handle_error(
    atm_openfaas_activity_feed_ws_handler:connection_ref(),
    errors:error(),
    atm_openfaas_activity_feed_ws_handler:handler_state()
) ->
    ok.
handle_error(ConnRef, Error, ResultStreamerContext) ->
    % this is an internal report, use a random report ID
    handle_streamed_task_data(ConnRef, str_utils:rand_hex(16), Error, ResultStreamerContext).

%%%===================================================================
%%% API functions
%%%===================================================================

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
-spec consume_result_streamer_report(
    atm_openfaas_activity_feed_ws_handler:connection_ref(),
    atm_openfaas_result_streamer_report:id(),
    atm_openfaas_result_streamer_report:body(),
    atm_openfaas_activity_feed_ws_handler:handler_state()
) ->
    atm_openfaas_activity_feed_ws_handler:handler_state().
consume_result_streamer_report(ConnRef, ReportId, #atm_openfaas_result_streamer_registration_report{
    workflow_execution_id = WorkflowExecutionId,
    task_execution_id = TaskExecutionId,
    result_streamer_id = ResultStreamerId
}, _HandlerState) ->
    case atm_openfaas_result_streamer_registry:register(
        WorkflowExecutionId, TaskExecutionId, ResultStreamerId, ConnRef, ReportId
    ) of
        stream_open ->
            ok;
        conclusion_ongoing ->
            atm_openfaas_activity_feed_ws_connection:push_json_to_client(ConnRef, ?FINALIZATION_SIGNAL_JSON)
    end,
    #result_streamer_context{
        workflow_execution_id = WorkflowExecutionId,
        task_execution_id = TaskExecutionId,
        result_streamer_id = ResultStreamerId
    };

consume_result_streamer_report(ConnRef, ReportId, #atm_openfaas_result_streamer_chunk_report{chunk = Chunk}, HandlerState) ->
    handle_streamed_task_data(ConnRef, ReportId, {chunk, Chunk}, HandlerState),
    HandlerState;

consume_result_streamer_report(ConnRef, ReportId, #atm_openfaas_result_streamer_invalid_data_report{
    result_name = ResultName,
    base_64_encoded_data = Base64EncodedData
}, HandlerState) ->
    Error = ?ERROR_BAD_DATA(
        <<"filePipeResult.", ResultName/binary>>,
        str_utils:format_bin(
            "Received invalid data for filePipe result with name '~s'.~n"
            "Base64 encoded data: ~s",
            [ResultName, Base64EncodedData]
        )
    ),
    handle_streamed_task_data(ConnRef, ReportId, Error, HandlerState),
    HandlerState;

consume_result_streamer_report(ConnRef, _ReportId, #atm_openfaas_result_streamer_deregistration_report{}, HandlerState) ->
    #result_streamer_context{
        workflow_execution_id = WorkflowExecutionId,
        task_execution_id = TaskExecutionId,
        result_streamer_id = ResultStreamerId
    } = HandlerState,
    atm_openfaas_result_streamer_registry:deregister(
        WorkflowExecutionId, TaskExecutionId, ResultStreamerId, ConnRef
    ),
    HandlerState.


%% @private
-spec handle_streamed_task_data(
    atm_openfaas_activity_feed_ws_handler:connection_ref(),
    atm_openfaas_result_streamer_report:id(),
    workflow_engine:streamed_task_data(),
    atm_openfaas_activity_feed_ws_handler:handler_state()
) ->
    ok.
handle_streamed_task_data(ConnRef, ReportId, StreamedTaskData, #result_streamer_context{
    workflow_execution_id = WorkflowExecutionId,
    task_execution_id = TaskExecutionId,
    result_streamer_id = ResultStreamerId
}) ->
    case atm_openfaas_result_streamer_registry:qualify_chunk_report(
        WorkflowExecutionId, TaskExecutionId, ResultStreamerId, ConnRef, ReportId
    ) of
        stale ->
            % reports that are late (after deregistration) or from a previous incarnation of the streamer are
            % ignored - such situation can only happen when there has been an anomaly and the stream will
            % anyway conclude with failure, so no special handling of this situation is required
            ?warning(
                "Ignoring a stale data from result streamer ~s for workflow execution ~s and task execution ~s~n"
                "Data: ~p",
                [ResultStreamerId, WorkflowExecutionId, TaskExecutionId, StreamedTaskData]
            );
        duplicate ->
            ok;
        to_process ->
            ok = workflow_engine:stream_task_data(WorkflowExecutionId, TaskExecutionId, StreamedTaskData),
            ok = atm_openfaas_result_streamer_registry:mark_chunk_report_processed(
                WorkflowExecutionId, TaskExecutionId, ResultStreamerId, ReportId
            )
    end.


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
            EncodedFinalizationSignalJson = ?FINALIZATION_SIGNAL_JSON,
            lists:foreach(fun(ConnRef) ->
                atm_openfaas_activity_feed_ws_connection:push_json_to_client(ConnRef, EncodedFinalizationSignalJson)
            end, ConnRefs),
            atm_openfaas_result_streamer_registry:await_deregistration_of_all_streamers()
    end.
