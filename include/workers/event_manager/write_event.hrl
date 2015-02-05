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
    counter_threshold = 1 :: undefined | non_neg_integer(),
    time_threshold :: undefined | non_neg_integer(),
    size_threshold :: undefined | non_neg_integer(),
    admission_rule = fun
        (#write_event{}) -> true;
        (_) -> false
    end :: event_stream:admission_rule(),
    aggregation_rule = fun(_, _) ->
        {error, disparate}
    end :: event_stream:aggregation_rule(),
    emission_rule = fun(_) -> true end :: event_stream:emission_rule(),
    callbacks = [] :: [event_stream:event_callback()]
}).

-endif.