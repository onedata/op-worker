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
-ifndef(EVENT_STREAM_HRL).
-define(EVENT_STREAM_HRL, 1).

-export_type([event_stream/0, admission_rule/0, aggregation_rule/0,
    emission_rule/0, event_handler/0]).

-record(event_stream, {
    admission_rule :: admission_rule(),
    aggregation_rule :: aggregation_rule(),
    emission_rule :: emission_rule(),
    handlers :: [event_handler()]
}).

-type event_stream() :: #event_stream{}.
-type admission_rule() :: fun((event_manager:event()) -> true | false).
-type aggregation_rule() :: fun((event_manager:event(), event_manager:event()) ->
    {ok, event_manager:event()} | {error, disparate}).
-type emission_rule() :: fun((event_stream()) -> true | false).
-type event_handler() :: {fun(([event_manager:event()]) -> term()),
    reliable | unreliable}.

-endif.