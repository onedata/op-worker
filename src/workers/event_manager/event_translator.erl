
%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(event_translator).
-author("Krzysztof Trzepla").

-include("proto/oneclient/event_messages.hrl").
-include("workers/event_manager/events.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([translate_client_message/1, translate_server_message/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Translates messages from the client to the server.
%% @end
%%--------------------------------------------------------------------
-spec translate_client_message(Message :: client_message()) ->
    {ok, Event :: event()} | {error, Reason :: term()}.
translate_client_message(#'ReadEvent'{} = Message) ->
    {ok, #write_event{
        counter = Message#read_event.counter,
        file_id = Message#read_event.file_id,
        size = Message#read_event.size,
        blocks = Message#read_event.blocks
    }};

translate_client_message(#write_event{} = Message) ->
    {ok, #'WriteEvent'{
        counter = Message#write_event.counter,
        file_id = Message#write_event.file_id,
        size = Message#write_event.size,
        blocks = Message#write_event.blocks
    }};

translate_client_message(Message) ->
    {error, {unknown_message, Message}}.

%%--------------------------------------------------------------------
%% @doc
%% Translates messages from the server to the client.
%% @end
%%--------------------------------------------------------------------
-spec translate_server_message(Subscription :: event_subscription()) ->
    {ok, Message :: server_message()} | {error, Reason :: term()}.
translate_server_message(#read_event_subscription{} = Message) ->
    {ok, #'ReadEventSubscription'{
        id = Message#read_event_subscription.id,
        counter_threshold = Message#read_event_subscription.counter_threshold,
        time_threshold = Message#read_event_subscription.time_threshold,
        size_threshold = Message#read_event_subscription.size_threshold
    }};

translate_server_message(#write_event_subscription{} = Message) ->
    {ok, #'WriteEventSubscription'{
        id = Message#write_event_subscription.id,
        counter_threshold = Message#write_event_subscription.counter_threshold,
        time_threshold = Message#write_event_subscription.time_threshold,
        size_threshold = Message#write_event_subscription.size_threshold
    }};

translate_server_message(Message) ->
    {error, {unknown_message, Message}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================