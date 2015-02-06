
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
-export([translate/1]).

%%%===================================================================
%%% API
%%%===================================================================

translate(#read_event{} = Record) ->
    {ok, #'ReadEvent'{
        counter = Record#read_event.counter,
        file_id = Record#read_event.file_id,
        size = Record#read_event.size,
        blocks = Record#read_event.blocks
    }};

translate(#'ReadEvent'{} = Record) ->
    {ok, #write_event{
        counter = Record#read_event.counter,
        file_id = Record#read_event.file_id,
        size = Record#read_event.size,
        blocks = Record#read_event.blocks
    }};

translate(#read_event_subscription{} = Record) ->
    {ok, #'ReadEventSubscription'{
        id = Record#read_event_subscription.id,
        counter_threshold = Record#read_event_subscription.counter_threshold,
        time_threshold = Record#read_event_subscription.time_threshold,
        size_threshold = Record#read_event_subscription.size_threshold
    }};

translate(#write_event{} = Record) ->
    {ok, #'WriteEvent'{
        counter = Record#write_event.counter,
        file_id = Record#write_event.file_id,
        size = Record#write_event.size,
        blocks = Record#write_event.blocks
    }};

translate(#'WriteEvent'{} = Record) ->
    {ok, #write_event{
        counter = Record#write_event.counter,
        file_id = Record#write_event.file_id,
        size = Record#write_event.size,
        blocks = Record#write_event.blocks
    }};

translate(#write_event_subscription{} = Record) ->
    {ok, #'WriteEventSubscription'{
        id = Record#write_event_subscription.id,
        counter_threshold = Record#write_event_subscription.counter_threshold,
        time_threshold = Record#write_event_subscription.time_threshold,
        size_threshold = Record#write_event_subscription.size_threshold
    }};

translate(Record) ->
    ?warning("~p:~p - bad record ~p", [?MODULE, ?LINE, Record]),
    {error, {unknown_record, Record}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================