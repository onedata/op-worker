%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Simulates an external openfaas-lambda-result-streamer client that
%%% connects via the OpenFaaS activity feed to send reports with lambda
%%% results relayed via a file pipe, using a WebSocket connection underneath.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_result_streamer_mock).
-author("Lukasz Opiola").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


%% API
-export([connect_to_provider_node/2]).
-export([connect_to_url/3]).
-export([send_text/2]).
-export([send_report/2, deliver_reports/2, deliver_reports_with_bodies/2]).
-export([deliver_registration_report/4]).
-export([deliver_chunk_report/2]).
-export([deliver_deregistration_report/1]).
-export([simulate_conclusion_failure/2]).
-export([has_received_internal_server_error_push_message/1]).


%%%===================================================================
%%% API
%%%===================================================================

-spec connect_to_provider_node(oct_background:node_selector(), undefined | binary()) ->
    {ok, test_websocket_client:client_ref()} | {error, term()}.
connect_to_provider_node(NodeSelector, BasicAuthorization) ->
    atm_openfaas_activity_feed_client_mock:connect_to_provider_node(
        NodeSelector, result_streamer, BasicAuthorization, fun handle_push_message/2
    ).


-spec connect_to_url(binary(), undefined | binary(), proplists:proplist()) ->
    {ok, test_websocket_client:client_ref()} | {error, term()}.
connect_to_url(Url, BasicAuthorization, TransportOpts) ->
    atm_openfaas_activity_feed_client_mock:connect_to_url(
        Url, BasicAuthorization, TransportOpts, fun handle_push_message/2
    ).


-spec send_text(test_websocket_client:client_ref(), binary()) -> ok.
send_text(ClientRef, Message) ->
    atm_openfaas_activity_feed_client_mock:send_text(ClientRef, Message).


%% @doc sends a report and does not wait for ACK
-spec send_report(test_websocket_client:client_ref(), atm_openfaas_result_streamer_report:body()) ->
    ok.
send_report(ClientRef, Report) ->
    ActivityReportBatch = [
        #atm_openfaas_result_streamer_report{
            id = ?RAND_STR(),
            body = Report
        }
    ],
    atm_openfaas_activity_feed_client_mock:send_report(
        ClientRef, atm_openfaas_result_streamer_report, ActivityReportBatch
    ).


%% @doc sends a batch of result streamer reports and waits for ACK
-spec deliver_reports(test_websocket_client:client_ref(), [atm_openfaas_result_streamer_report:record()]) ->
    ok.
deliver_reports(ClientRef, Reports) ->
    ReportIds = [R#atm_openfaas_result_streamer_report.id || R <- Reports],
    lists:foreach(fun(ReportId) ->
        store_reporting_process(ReportId, self())
    end, ReportIds),

    atm_openfaas_activity_feed_client_mock:send_report(ClientRef, atm_openfaas_result_streamer_report, Reports),
    lists:foreach(fun(ReportId) ->
        receive
            {report_ack, ReportId} -> ok
        after
            60000 -> error(timeout)
        end
    end, ReportIds).


%% @doc sends a batch of reports with given bodies (wraps them in the report record) and waits for ACK
-spec deliver_reports_with_bodies(test_websocket_client:client_ref(), [atm_openfaas_result_streamer_report:body()]) ->
    ok.
deliver_reports_with_bodies(ClientRef, ReportBodies) ->
    Reports = lists:map(fun(ReportBody) ->
        #atm_openfaas_result_streamer_report{
            id = ?RAND_STR(),
            body = ReportBody
        }
    end, ReportBodies),
    deliver_reports(ClientRef, Reports).


-spec deliver_registration_report(
    test_websocket_client:client_ref(),
    atm_workflow_execution:id(),
    atm_task_execution:id(),
    atm_openfaas_result_streamer_registry:result_streamer_id()
) ->
    ok.
deliver_registration_report(ClientRef, WorkflowExecutionId, TaskExecutionId, ResultStreamerId) ->
    deliver_reports_with_bodies(ClientRef, [
        #atm_openfaas_result_streamer_registration_report{
            workflow_execution_id = WorkflowExecutionId,
            task_execution_id = TaskExecutionId,
            result_streamer_id = ResultStreamerId
        }
    ]).


-spec deliver_chunk_report(test_websocket_client:client_ref(), atm_openfaas_result_streamer_chunk_report:chunk()) ->
    ok.
deliver_chunk_report(Client, Chunk) ->
    deliver_reports_with_bodies(Client, [
        #atm_openfaas_result_streamer_chunk_report{chunk = Chunk}
    ]).


-spec deliver_deregistration_report(test_websocket_client:client_ref()) -> ok.
deliver_deregistration_report(Client) ->
    deliver_reports_with_bodies(Client, [
        #atm_openfaas_result_streamer_deregistration_report{}
    ]).


-spec simulate_conclusion_failure(test_websocket_client:client_ref(), boolean()) -> ok.
simulate_conclusion_failure(ClientRef, Flag) ->
    node_cache:put({should_simulate_conclusion_failure, ClientRef}, Flag).


-spec has_received_internal_server_error_push_message(test_websocket_client:client_ref()) -> boolean().
has_received_internal_server_error_push_message(ClientRef) ->
    node_cache:get({internal_server_error_received, ClientRef}, false).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%% @doc Callback handling push messages received by the test_websocket_client.
-spec handle_push_message(test_websocket_client:client_ref(), binary()) ->
    no_reply | {reply_text, binary()}.
handle_push_message(_ClientRef, <<"Bad request: ", _/binary>>) ->
    % this push message is received when a bad request is performed
    no_reply;
handle_push_message(ClientRef, <<"Internal server error while processing the request">>) ->
    % this push message is received when an error occurs during report processing
    node_cache:put({internal_server_error_received, ClientRef}, true),
    no_reply;
handle_push_message(ClientRef, Payload) ->
    try
        PushMessage = jsonable_record:from_json(
            json_utils:decode(Payload),
            atm_openfaas_result_streamer_push_message
        ),
        case PushMessage of
            #atm_openfaas_result_streamer_report_ack{id = ReportId} ->
                case lookup_reporting_process(ReportId) of
                    none ->
                        ok;
                    {ok, Pid} ->
                        Pid ! {report_ack, ReportId}
                end,
                no_reply;
            #atm_openfaas_result_streamer_finalization_signal{} ->
                handle_finalization_signal(ClientRef)
        end
    catch
        _:_:_ ->
            ct:print("Unexpected message in ~tp: ~ts", [?MODULE, Payload]),
            error(unexpected_message)
    end.


%% @private
-spec handle_finalization_signal(test_websocket_client:client_ref()) -> no_reply | {reply_text, binary()}.
handle_finalization_signal(ClientRef) ->
    case node_cache:get({should_simulate_conclusion_failure, ClientRef}, false) of
        true ->
            % lack of response to finalization signal will cause a timeout of stream conclusion
            no_reply;
        false ->
            timer:sleep(?RAND_INT(0, 500)),
            {reply_text, json_utils:encode(jsonable_record:to_json(
                #atm_openfaas_activity_report{
                    type = atm_openfaas_result_streamer_report,
                    batch = [#atm_openfaas_result_streamer_report{
                        id = ?RAND_STR(),
                        body = #atm_openfaas_result_streamer_deregistration_report{}
                    }]
                },
                atm_openfaas_activity_report
            ))}
    end.


%% @private
-spec store_reporting_process(atm_openfaas_result_streamer_report:id(), pid()) -> ok.
store_reporting_process(ReportId, Pid) ->
    node_cache:put({reporting_process, ReportId}, Pid),
    ok.


%% @private
-spec lookup_reporting_process(atm_openfaas_result_streamer_report:id()) -> {ok, pid()} | none.
lookup_reporting_process(ReportId) ->
    case node_cache:get({reporting_process, ReportId}, none) of
        none -> none;
        Pid -> {ok, Pid}
    end.
