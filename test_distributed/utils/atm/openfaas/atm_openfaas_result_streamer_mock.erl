%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Simulates an external k8s-events-monitor client that connects via the OpenFaaS activity
%%% feed to send pod status reports, using a WebSocket connection underneath.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_result_streamer_mock).
-author("Lukasz Opiola").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


%% API
-export([start/2]).
-export([send_text/2]).
-export([send_report/2]).
-export([send_registration_report/4]).
-export([send_chunk_report/2]).
-export([send_deregistration_report/1]).
-export([simulate_deregistration_failure/1]).


%%%===================================================================
%%% API
%%%===================================================================

-spec start(oct_background:node_selector(), undefined | binary()) ->
    {ok, test_websocket_client:client_ref()} | {error, term()}.
start(NodeSelector, BasicAuthorization) ->
    atm_openfaas_activity_feed_client_mock:start(NodeSelector, BasicAuthorization, fun handle_push_message/2).


-spec send_text(test_websocket_client:client_ref(), binary()) -> ok.
send_text(ClientRef, Message) ->
    atm_openfaas_activity_feed_client_mock:send_text(ClientRef, Message).


-spec send_report(test_websocket_client:client_ref(), [atm_openfaas_result_streamer_report:record()]) ->
    ok.
send_report(ClientRef, Batch) ->
    atm_openfaas_activity_feed_client_mock:send_report(ClientRef, atm_openfaas_result_streamer_report, Batch).


-spec send_registration_report(
    test_websocket_client:client_ref(),
    atm_workflow_execution:id(),
    atm_task_execution:id(),
    atm_openfaas_result_streamer_registry:result_streamer_id()
) ->
    ok.
send_registration_report(ClientRef, WorkflowExecutionId, TaskExecutionId, ResultStreamerId) ->
    send_report(ClientRef, [
        #atm_openfaas_result_streamer_registration_report{
            workflow_execution_id = WorkflowExecutionId,
            task_execution_id = TaskExecutionId,
            result_streamer_id = ResultStreamerId
        }
    ]).


-spec send_chunk_report(test_websocket_client:client_ref(), atm_openfaas_result_streamer_chunk_report:chunk()) ->
    ok.
send_chunk_report(Client, Chunk) ->
    send_report(Client, [
        #atm_openfaas_result_streamer_chunk_report{chunk = Chunk}
    ]).


-spec send_deregistration_report(test_websocket_client:client_ref()) -> ok.
send_deregistration_report(Client) ->
    send_report(Client, [
        #atm_openfaas_result_streamer_deregistration_report{}
    ]).


-spec simulate_deregistration_failure(test_websocket_client:client_ref()) -> ok.
simulate_deregistration_failure(ClientRef) ->
    node_cache:put({should_simulate_deregistration_failure, ClientRef}, true).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%% @doc Callback handling push messages received by the test_websocket_client. Currently, only one
%% type of message is expected (finalization signal sent to result streamers).
%% This callback is common for connections simulating k8s-event-monitor and openfaas-lambda-result-streamer,
%% though the k8s-event-monitor does not expect any push messages.
-spec handle_push_message(test_websocket_client:client_ref(), binary()) ->
    no_reply | {reply, binary()}.
handle_push_message(_ClientRef, <<"Bad request: ", _/binary>>) ->
    % this push message is received when a bad request is performed (result_streamer_error_handling_test)
    no_reply;
handle_push_message(_ClientRef, <<"Internal server error while processing the request">>) ->
    % this push message is received when an error occurs during report processing (result_streamer_error_handling_test)
    no_reply;
handle_push_message(ClientRef, Payload) ->
    try
        #atm_openfaas_result_streamer_finalization_signal{} = jsonable_record:from_json(
            json_utils:decode(Payload),
            atm_openfaas_result_streamer_finalization_signal
        ),
        handle_finalization_signal(ClientRef)
    catch
        _:_ ->
            ct:print("Unexpected message in ~p: ~s", [?MODULE, Payload]),
            error(unexpected_message)
    end.


%% @private
-spec handle_finalization_signal(test_websocket_client:client_ref()) -> no_reply | {reply, binary()}.
handle_finalization_signal(ClientRef) ->
    case node_cache:get({should_simulate_deregistration_failure, ClientRef}, false) of
        true ->
            % lack of response to finalization signal will cause a timeout of stream closing
            no_reply;
        false ->
            timer:sleep(?RAND_INT(0, 500)),
            {reply, json_utils:encode(jsonable_record:to_json(
                #atm_openfaas_activity_report{
                    type = atm_openfaas_result_streamer_report,
                    batch = [#atm_openfaas_result_streamer_deregistration_report{}]
                },
                atm_openfaas_activity_report
            ))}
    end.
