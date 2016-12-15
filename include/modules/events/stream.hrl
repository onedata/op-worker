%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains definition of an event stream.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(OP_WORKER_MODULES_EVENTS_STREAM_HRL).
-define(OP_WORKER_MODULES_EVENTS_STREAM_HRL, 1).

%% definition of an event stream
%% key               - unique stream ID
%% metadata          - arbitrary term
%% init_handler      - function that is called when event stream starts
%% terminate_handler - function that is called when event stream terminates
%% event_handler     - function that takes an input list of events and may return
%%                     arbitrary value, which will be later ignored by the stream
%% admission_rule    - function that takes as an input an event and returns
%%                     boolean value describing whether this event can be
%%                     processed by the stream
%% aggregation_rule  - function that takes as an input two events and returns
%%                     an event that is a merge result
%% transition_rule   - function that takes as an input stream metadata
%%                     and an event and returns new stream metadata
%% emission_rule     - function that takes as an input stream metadata
%%                     and decides whether event handler should be executed
%% emission_time     - maximal delay between successive event handler executions
-record(event_stream, {
    metadata = 0 :: event_stream:metadata(),
    init_handler = fun(SubId, SessId) ->
        #{subscription_id => SubId, session_id => SessId}
    end :: event_stream:init_handler(),
    terminate_handler = fun(_) -> ok end :: event_stream:terminate_handler(),
    event_handler = fun(_, _) -> ok end :: event_stream:event_handler(),
    aggregation_rule = fun(_OldEvt, Evt) ->
        Evt
    end :: event_stream:aggregation_rule(),
    transition_rule = fun(Meta, _NewEvt) ->
        Meta
    end :: event_stream:transition_rule(),
    emission_rule = fun(_Meta) -> true end :: event_stream:emission_rule(),
    emission_time = infinity :: event_stream:emission_time()
}).

-endif.
