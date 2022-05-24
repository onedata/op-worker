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
-module(atm_openfaas_k8s_events_monitor_mock).
-author("Lukasz Opiola").


%% API
-export([start/2]).
-export([send_pod_status_report/2]).


%%%===================================================================
%%% API
%%%===================================================================

-spec start(oct_background:node_selector(), undefined | binary()) ->
    {ok, test_websocket_client:client_ref()} | {error, term()}.
start(NodeSelector, BasicAuthorization) ->
    atm_openfaas_activity_feed_client_mock:start(NodeSelector, BasicAuthorization, fun handle_push_message/2).


-spec send_pod_status_report(test_websocket_client:client_ref(), [atm_openfaas_function_pod_status_report:record()]) ->
    ok.
send_pod_status_report(ClientRef, Batch) ->
    atm_openfaas_activity_feed_client_mock:send_report(ClientRef, atm_openfaas_function_pod_status_report, Batch).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%% @doc Callback handling push messages received by the test_websocket_client.
-spec handle_push_message(test_websocket_client:client_ref(), binary()) ->
    no_reply | {reply, binary()}.
handle_push_message(_ClientRef, Message) ->
    ct:print("Unexpected message in ~p: ~s", [?MODULE, Message]),
    error(unexpected_message).
