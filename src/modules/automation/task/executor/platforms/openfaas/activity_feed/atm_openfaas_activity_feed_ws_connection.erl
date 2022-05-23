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

-export([handle_info/1]).
-export([push_message/2]).


%%%===================================================================
%%% API
%%%===================================================================


%% @doc called from withing a connection process to handle received messages
-spec handle_info(term()) -> ok | {send_message, json_utils:json_term()}.
handle_info({send_message, JsonMessage}) ->
    {send_message, json_utils:encode(JsonMessage)};
handle_info(Message) ->
    ?warning("Unexpected message in ~p: ~p", [?MODULE, Message]).


%% @doc called to request sending an asynchronous push message by a connection process
-spec push_message(atm_openfaas_activity_feed_ws_handler:connection_ref(), json_utils:json_term()) -> ok.
push_message(ConnRef, JsonMessage) ->
    ConnRef ! {send_message, JsonMessage},
    ok.
