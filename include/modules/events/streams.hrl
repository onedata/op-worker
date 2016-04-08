%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains definition of an event stream along with default event
%%% stream specializations.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(OP_WORKER_MODULES_EVENTS_STREAMS_HRL).
-define(OP_WORKER_MODULES_EVENTS_STREAMS_HRL, 1).

-include("types.hrl").
-include("proto/oneclient/fuse_messages.hrl").

%% definition of an event stream
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
-record(event_stream_definition, {
    metadata = 0 :: event_stream:metadata(),
    init_handler = fun(_, _, _) -> #{} end :: event_stream:init_handler(),
    terminate_handler = fun(_) -> ok end :: event_stream:terminate_handler(),
    event_handler = fun(_, _) -> ok end :: event_stream:event_handler(),
    admission_rule :: event_stream:admission_rule(),
    aggregation_rule = fun(#event{} = E1, #event{} = E2) ->
        E2#event{counter = E1#event.counter + E2#event.counter}
    end :: event_stream:aggregation_rule(),
    transition_rule = fun(Meta, #event{counter = Counter}) ->
        Meta + Counter
    end :: event_stream:transition_rule(),
    emission_rule = fun(_) -> true end :: event_stream:emission_rule(),
    emission_time = infinity :: event_stream:emission_time()
}).

%% Default read event stream specialization
-define(READ_EVENT_STREAM, #event_stream_definition{
    admission_rule = fun
        (#event{object = #read_event{}}) -> true;
        (_) -> false
    end,
    aggregation_rule = fun(#event{object = O1} = E1, #event{object = O2} = E2) ->
        E1#event{
            counter = E1#event.counter + E2#event.counter,
            object = O2#read_event{
                size = O1#read_event.size + O2#read_event.size,
                blocks = fslogic_blocks:aggregate(
                    O1#read_event.blocks,
                    O2#read_event.blocks
                )
            }
        }
    end
}).

%% Default write event stream specialization
-define(WRITE_EVENT_STREAM, #event_stream_definition{
    admission_rule = fun
        (#event{object = #write_event{}}) -> true;
        (_) -> false
    end,
    aggregation_rule = fun(#event{object = O1} = E1, #event{object = O2} = E2) ->
        E1#event{
            counter = E1#event.counter + E2#event.counter,
            % Use new write event (O2), because it has up-to-date file size.
            object = O2#write_event{
                size = O1#write_event.size + O2#write_event.size,
                blocks = fslogic_blocks:aggregate(
                    O1#write_event.blocks,
                    O2#write_event.blocks
                )
            }
        }
    end
}).

%% Default file attr event stream specialization
-define(FILE_ATTR_EVENT_STREAM, #event_stream_definition{
    admission_rule = fun
        (#event{object = #update_event{object = #file_attr{}}}) -> true;
        (_) -> false
    end
}).

%% Default file location event stream specialization
-define(FILE_LOCATION_EVENT_STREAM, #event_stream_definition{
    admission_rule = fun
        (#event{object = #update_event{object = #file_location{}}}) -> true;
        (_) -> false
    end
}).


%% Default permission_changed event stream specialization
-define(PERMISSION_CHANGED_EVENT_STREAM, #event_stream_definition{
    admission_rule = fun
        (#event{object = #permission_changed_event{}}) -> true;
        (_) -> false
    end
}).

%% Default file removal stream specialization
-define(FILE_REMOVAL_EVENT_STREAM, #event_stream_definition{
    admission_rule = fun
        (#event{object = #file_removal_event{}}) -> true;
        (_) -> false
    end
}).

-endif.