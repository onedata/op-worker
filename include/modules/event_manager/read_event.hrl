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

-include("event_stream.hrl").

-record(read_event, {
    counter :: non_neg_integer(),
    file_uuid :: binary(),
    size :: non_neg_integer(),
    blocks = [] :: [event_utils:file_block()]
}).

%% Default read event stream specification
-define(READ_EVENT_STREAM, #event_stream{
    metadata = 0,
    admission_rule = fun(#read_event{}) -> true; (_) -> false end,
    aggregation_rule = fun
        (#read_event{file_uuid = Id} = Evt1, #read_event{file_uuid = Id} = Evt2) ->
            {ok, #read_event{
                file_uuid = Evt1#read_event.file_uuid,
                counter = Evt1#read_event.counter + Evt2#read_event.counter,
                size = Evt1#read_event.size + Evt2#read_event.size,
                blocks = event_utils:aggregate_blocks(
                    Evt1#read_event.blocks,
                    Evt2#read_event.blocks
                )
            }};
        (_, _) -> {error, different}
    end,
    transition_rule = fun(Meta, #read_event{counter = Counter}) ->
        Meta + Counter
    end,
    emission_rule = fun(_) -> false end,
    handlers = []
}).

-record(read_event_subscription, {
    id :: event_manager:subscription_id(),
    producer :: event_manager:producer(),
    producer_counter_threshold :: non_neg_integer(),
    producer_time_threshold :: non_neg_integer(),
    producer_size_threshold :: non_neg_integer(),
    event_stream = ?READ_EVENT_STREAM :: event_stream:event_stream()
}).

-endif.