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
-ifndef(READ_EVENT_HRL).
-define(READ_EVENT_HRL, 1).

-record(read_event, {
    counter :: non_neg_integer(),
    file_id :: binary(),
    size :: non_neg_integer(),
    blocks :: [{non_neg_integer(), non_neg_integer()}]
}).

-record(read_event_subscription, {
    id :: non_neg_integer(),
    counter_threshold = 1 :: undefined | non_neg_integer(),
    time_threshold :: undefined | non_neg_integer(),
    size_threshold :: undefined | non_neg_integer(),
    admission_rule = fun
        (#read_event{}) -> true;
        (_) -> false
    end :: event_stream:admission_rule(),
    aggregation_rule = fun(_, _) ->
        {error, disparate}
    end :: event_stream:aggregation_rule(),
    emission_rule = fun(_) -> true end :: event_stream:emission_rule(),
    callbacks = [] :: [event_stream:event_callback()]
}).

-endif.