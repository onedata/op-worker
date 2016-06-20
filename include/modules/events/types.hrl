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

-include("proto/oneclient/common_messages.hrl").

%% definition of a top level event wrapper
%% key     - arbitrary value that distinguish events, i.e. events with the same
%%           key can be aggregated
%% counter - number of events aggregated in this event
%% object  - wrapped event
-record(event, {
    key :: event:key(),
    counter = 1 :: event:counter(),
    object :: event:object()
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
    notify :: fun((term()) -> ok)
}).

%% definition of an event associated with a read operation in the file system
%% file_uuid - UUID of a file associated with the read operation
%% size      - number of bytes read
%% blocks    - list of offset, size pairs that describes bytes segments read
-record(read_event, {
    file_uuid :: file_meta:uuid(),
    size = 0 :: file_meta:size(),
    blocks = [] :: fslogic_blocks:blocks()
}).

%% definition of an event associated with an update operation in the file system
%% object - wrapped structure that has been modified
-record(update_event, {
    object :: event:update_object()
}).

%% definition of an event associated with a write operation in the file system
%% file_uuid - UUID of a file associated with the write operation
%% file_size - size of a file after the write operation
%% size      - number of bytes written
%% blocks    - list of offset, size pairs that describes bytes segments written
-record(write_event, {
    file_uuid :: file_meta:uuid(),
    file_size :: file_meta:size(),
    size = 0 :: file_meta:size(),
    blocks = [] :: fslogic_blocks:blocks()
}).

%% definition of an event triggered when file permission gets changed
%% file_uuid - UUID of a file
-record(permission_changed_event, {
    file_uuid :: file_meta:uuid()
}).

%% definition of an event triggered when file is removed
%% file_uuid - UUID of a file
-record(file_removal_event, {
    file_uuid :: file_meta:uuid()
}).

%% definition of an event triggered when any of spaces becomes (un)available
-record(quota_exeeded_event, {
   spaces = [] :: [space_info:id()]
}).

%% definition of an event triggered when file is renamed
%% old_uuid - old UUID of renamed file
%% new_uuid - new UUID of renamed file
-record(file_renamed_event, {
    top_entry :: #file_renamed_entry{},
    child_entries = [] :: [#file_renamed_entry{}]
}).

-endif.