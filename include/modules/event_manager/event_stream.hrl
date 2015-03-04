%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains definition of event stream.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(EVENT_STREAM_HRL).
-define(EVENT_STREAM_HRL, 1).

-record(event_stream, {
    metadata :: event_stream:metadata(),
    admission_rule :: event_stream:admission_rule(),
    aggregation_rule :: event_stream:aggregation_rule(),
    transition_rule :: event_stream:transition_rule(),
    emission_rule :: event_stream:emission_rule(),
    emission_time :: timeout(),
    handlers :: [event_stream:event_handler()]
}).

-endif.