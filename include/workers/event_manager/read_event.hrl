%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains definition of read event and read event subscription.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(READ_EVENT_HRL).
-define(READ_EVENT_HRL, 1).

-record(read_event, {
    counter :: non_neg_integer(),
    file_id :: binary(),
    size :: non_neg_integer(),
    blocks :: [{non_neg_integer(), non_neg_integer()}]
}).

-record(read_event_subscription, {
    subscription_id :: binary(),
    producer = fuse :: event_manager:event_producer(),
    producer_counter_threshold :: non_neg_integer(),
    producer_time_threshold ::  non_neg_integer(),
    producer_size_threshold :: non_neg_integer(),
    subscriber_counter_threshold :: non_neg_integer(),
    subscriber_time_threshold :: non_neg_integer(),
    subscriber_size_threshold :: non_neg_integer(),
    admission_rule = fun
        (#read_event{}) -> true;
        (_) -> false
    end :: event_stream:admission_rule(),
    aggregation_rule = fun(Event1, Event2) ->
        {ok, #read_event{
            counter = Event1#read_event.counter + Event2#read_event.counter,
            size = Event1#read_event.size + Event2#read_event.size
        }}
    end :: event_stream:aggregation_rule(),
    handlers = [] :: [event_stream:event_handler()]
}).

-endif.