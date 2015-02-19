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
    blocks = [] :: [{non_neg_integer(), non_neg_integer()}]
}).

-record(write_event_subscription, {
    id :: event_manager:subscription_id(),
    producer :: event_manager:producer(),
    producer_counter_threshold :: non_neg_integer(),
    producer_time_threshold :: non_neg_integer(),
    producer_size_threshold :: non_neg_integer(),
    event_stream :: event_stream:event_stream()
}).

-endif.