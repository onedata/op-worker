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
%% id                - unique stream ID
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
    id :: undefined | event_stream:id(),
    metadata = 0 :: event_stream:metadata(),
    init_handler = fun(#subscription{id = SubId}, SessId, _) ->
        #{subscription_id => SubId, session_id => SessId}
    end :: event_stream:init_handler(),
    terminate_handler = fun(_) -> ok end :: event_stream:terminate_handler(),
    event_handler = fun(_, _) -> ok end :: event_stream:event_handler(),
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
    id = read_event_stream,
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
    id = write_event_stream,
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
    id = file_attr_event_stream,
    event_handler = event_utils:send_events_handler(),
    aggregation_rule = fun(#event{object = O1} = E1, #event{object = O2} = E2) ->
        OldAttr = O1#update_event.object,
        NewAttr = O2#update_event.object,
        AggregatedAttr = NewAttr#file_attr{
            % Use the new attrs, but preserve the size update if new attrs don't
            % change the size
            size = case NewAttr#file_attr.size of
                undefined -> OldAttr#file_attr.size;
                X -> X
            end
        },
        E1#event{
            counter = E1#event.counter + E2#event.counter,
            object = O2#update_event{object = AggregatedAttr}
        }
    end
}).

%% Default file location event stream specialization
-define(FILE_LOCATION_EVENT_STREAM, #event_stream_definition{
    id = file_location_event_stream,
    event_handler = event_utils:send_events_handler()
}).


%% Default permission_changed event stream specialization
-define(PERMISSION_CHANGED_EVENT_STREAM, #event_stream_definition{
    id = permission_changed_event_stream,
    event_handler = event_utils:send_events_handler()
}).

%% Default file removal stream specialization
-define(FILE_REMOVAL_EVENT_STREAM, #event_stream_definition{
    id = file_removal_event_stream,
    event_handler = event_utils:send_events_handler()
}).

%% Default file renamed event stream specialization
-define(FILE_RENAMED_EVENT_STREAM, #event_stream_definition{
    id = file_renamed_event_stream,
    event_handler = event_utils:send_events_handler()
}).

%% Default file accessed event stream specialization
-define(FILE_ACCESSED_EVENT_STREAM, #event_stream_definition{
    id = file_accessed_event_stream,
    event_handler = event_utils:send_events_handler()
}).

%% Default quota exceeded event stream specialization
-define(QUOTA_EXCEEDED_EVENT_STREAM, #event_stream_definition{
    id = quota_exceeded_event_stream,
    event_handler = event_utils:send_events_handler()
}).

-endif.
