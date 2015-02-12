%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains definition of write event and write event subscription.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(WRITE_EVENT_HRL).
-define(WRITE_EVENT_HRL, 1).

-record(write_event, {
    counter :: non_neg_integer(),
    file_id :: binary(),
    file_size :: non_neg_integer(),
    size :: non_neg_integer(),
    blocks :: [{non_neg_integer(), non_neg_integer()}]
}).

-record(write_event_subscription, {
    subscription_id :: binary(),
    producer = fuse :: event_manager:event_producer(),
    producer_counter_threshold :: non_neg_integer(),
    producer_time_threshold ::  non_neg_integer(),
    producer_size_threshold :: non_neg_integer(),
    subscriber_counter_threshold :: non_neg_integer(),
    subscriber_time_threshold :: non_neg_integer(),
    subscriber_size_threshold :: non_neg_integer(),
    admission_rule = fun
        (#write_event{}) -> true;
        (_) -> false
    end :: event_stream:admission_rule(),
    aggregation_rule = fun(Event1, Event2) ->
        {ok, #write_event{
            counter = Event1#write_event.counter + Event2#write_event.counter,
            size = Event1#write_event.size + Event2#write_event.size
        }}
    end :: event_stream:aggregation_rule(),
    handlers = [] :: [event_stream:event_handler()]
}).

-endif.