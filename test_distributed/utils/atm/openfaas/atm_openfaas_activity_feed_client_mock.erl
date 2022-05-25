%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Simulates an external client that connects via the OpenFaaS activity
%%% feed to send reports, using a WebSocket connection underneath.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_activity_feed_client_mock).
-author("Lukasz Opiola").

-include("http/gui_paths.hrl").
-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/http/headers.hrl").


%% API
-export([set_secret_on_provider/2]).
-export([start/4]).
-export([send_text/2]).
-export([send_report/3]).


%%%===================================================================
%%% API
%%%===================================================================

-spec set_secret_on_provider(oct_background:node_selector(), string() | binary()) -> ok.
set_secret_on_provider(NodeSelector, Secret) ->
    opw_test_rpc:set_env(NodeSelector, openfaas_activity_feed_secret, Secret).


-spec start(
    oct_background:node_selector(),
    atm_openfaas_activity_feed_ws_handler:client_type(),
    undefined | binary(),
    test_websocket_client:push_message_handler()
) ->
    {ok, test_websocket_client:client_ref()} | {error, term()}.
start(NodeSelector, ClientType, BasicAuthorization, PushMessageHandler) ->
    Path = string:replace(?OPENFAAS_ACTIVITY_FEED_WS_COWBOY_ROUTE, ":client_type", atom_to_list(ClientType)),
    Headers = case BasicAuthorization of
        undefined -> [];
        _ -> [{?HDR_AUTHORIZATION, <<"Basic ", BasicAuthorization/binary>>}]
    end,
    test_websocket_client:start(NodeSelector, Path, Headers, PushMessageHandler).


-spec send_text(test_websocket_client:client_ref(), binary()) -> ok.
send_text(ClientRef, Message) ->
    test_websocket_client:send(ClientRef, Message).


-spec send_report(
    test_websocket_client:client_ref(),
    atm_openfaas_activity_report:type(),
    atm_openfaas_result_streamer_report:batch()
) ->
    ok.
send_report(ClientRef, Type, Batch) ->
    send_text(ClientRef, json_utils:encode(jsonable_record:to_json(
        #atm_openfaas_activity_report{type = Type, batch = Batch},
        atm_openfaas_activity_report
    ))).