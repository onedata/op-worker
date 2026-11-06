%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module for interacting with the websocket connection process of the
%%% OpenFaaS activity feed.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_activity_feed_ws_connection).
-author("Lukasz Opiola").

-include_lib("ctool/include/logging.hrl").

-export([push_json_to_client/2]).
-export([interpret_info_message/1]).


%%%===================================================================
%%% API
%%%===================================================================


%% @doc called to request sending an asynchronous push message by a connection process
-spec push_json_to_client(atm_openfaas_activity_feed_ws_handler:connection_ref(), json_utils:json_term()) -> ok.
push_json_to_client(ConnRef, JsonMessage) ->
    ConnRef ! {push_json_to_client, JsonMessage},
    ok.


%% @doc called from within a websocket connection process to identify received messages
-spec interpret_info_message(term()) -> no_reply | {reply, binary()}.
interpret_info_message({push_json_to_client, JsonMessage}) ->
    {reply, json_utils:encode(JsonMessage)};
interpret_info_message(Message) ->
    ?warning("Unexpected message in ~tp: ~tp", [?MODULE, Message]),
    no_reply.
