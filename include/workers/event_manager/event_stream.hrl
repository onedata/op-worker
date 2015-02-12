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
    admission_rule :: admission_rule(),
    aggregation_rule :: aggregation_rule(),
    emission_rule :: emission_rule(),
    handlers :: [event_handler()]
}).

-endif.