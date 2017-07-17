%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains definition of a top level event wrapper and subsequent
%%% event types.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(OP_WORKER_MODULES_EVENTS_TYPES_HRL).
-define(OP_WORKER_MODULES_EVENTS_TYPES_HRL, 1).

-include("proto/oneclient/fuse_messages.hrl").

%% definition of a top level event wrapper
%% type - specific event
-record(event, {
    type :: event:type()
}).

%% definition of a events container
-record(events, {
    events = [] :: [#event{}]
}).

%% definition of a events container
-record(flush_events, {
    provider_id :: oneprovider:id(),
    subscription_id :: subscription:id(),
    context :: term(),
    notify :: undefined | fun((term()) -> ok)
}).

%% definition of an event associated with a read operation in the file system
%% counter   - number of events aggregated in this event
%% file_guid - GUID of a file associated with the read operation
%% size      - number of bytes read
%% blocks    - list of offset, size pairs that describes bytes segments read
-record(file_read_event, {
    counter = 1 :: non_neg_integer(),
    file_guid :: fslogic_worker:file_guid(),
    size = 0 :: file_meta:size(),
    blocks = [] :: fslogic_blocks:blocks()
}).

%% definition of an event associated with a write operation in the file system
%% counter    - number of events aggregated in this event
%% file_guid - GUID of a file associated with the write operation
%% file_size - size of a file after the write operation
%% size      - number of bytes written
%% blocks    - list of offset, size pairs that describes bytes segments written
-record(file_written_event, {
    counter = 1 :: non_neg_integer(),
    file_guid :: fslogic_worker:file_guid(),
    file_size :: undefined | file_meta:size(),
    size = 0 :: file_meta:size(),
    blocks = [] :: fslogic_blocks:blocks()
}).

%% definition of an event triggered when file attributes are changed
%% file_attr - updated file attributes
-record(file_attr_changed_event, {
    file_attr :: #file_attr{}
}).

%% definition of an event triggered when file location is changed
%% file_location - updated file location
-record(file_location_changed_event, {
    file_location :: #file_location{}
}).

%% definition of an event triggered when file permission is changed
%% file_guid - GUID of a file
-record(file_perm_changed_event, {
    file_guid :: fslogic_worker:file_guid()
}).

%% definition of an event triggered when file is removed
%% file_guid - GUID of a file
-record(file_removed_event, {
    file_guid :: fslogic_worker:file_guid()
}).

%% definition of an event triggered when file is renamed
%% top_entry     - file renamed entry
%% child_entries - list of file renamed entries for children
-record(file_renamed_event, {
    top_entry :: #file_renamed_entry{},
    child_entries = [] :: [#file_renamed_entry{}]
}).

%% definition of an event triggered when space exceeds the storage quota
%% spaces - list of spaces for which storage quota is exceeded
-record(quota_exceeded_event, {
    spaces = [] :: [od_space:id()]
}).

%% definition of a monitoring event
-record(monitoring_event, {
    type :: monitoring_event:type()
}).

-endif.
