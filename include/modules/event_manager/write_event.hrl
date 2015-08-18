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

-include("event_stream.hrl").

-record(write_event, {
    counter :: non_neg_integer(),
    file_uuid :: binary(),
    file_size :: non_neg_integer(),
    size :: non_neg_integer(),
    blocks = [] :: [event_utils:file_block()]
}).

%% Default write event stream specification
-define(WRITE_EVENT_STREAM, #event_stream{
    metadata = 0,
    admission_rule = fun(#write_event{}) -> true; (_) -> false end,
    aggregation_rule = fun
        (#write_event{file_uuid = Id} = Evt1, #write_event{file_uuid = Id} = Evt2) ->
            {ok, #write_event{
                file_uuid = Evt1#write_event.file_uuid,
                counter = Evt1#write_event.counter + Evt2#write_event.counter,
                size = Evt1#write_event.size + Evt2#write_event.size,
                file_size = Evt2#write_event.file_size,
                blocks = event_utils:aggregate_blocks(
                    Evt1#write_event.blocks,
                    Evt2#write_event.blocks
                )
            }};
        (_, _) -> {error, different}
    end,
    transition_rule = fun(Meta, #write_event{counter = Counter}) ->
        Meta + Counter
    end,
    emission_rule = fun(_) -> false end,
    handlers = []
}).

-record(write_event_subscription, {
    id :: event_manager:subscription_id(),
    producer = all :: event_manager:producer(),
    producer_counter_threshold :: non_neg_integer(),
    producer_time_threshold :: non_neg_integer(),
    producer_size_threshold :: non_neg_integer(),
    event_stream = ?WRITE_EVENT_STREAM :: event_stream:event_stream()
}).

-endif.