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
%% key     - arbitrary value that distinguish events, i.e. events with the same
%%           key can be aggregated
%% counter - number of events aggregated in this event
%% type    - wrapped event
-record(event, {
    key :: event:key(),
    counter = 1 :: event:counter(),
    type :: event:type()
}).

%% definition of an event associated with a read operation in the file system
%% file_uuid - UUID of a file associated with the read operation
%% size      - number of bytes read
%% blocks    - list of offset, size pairs that describes bytes segments read
-record(read_event, {
    file_uuid :: file_meta:uuid(),
    size :: file_meta:size(),
    blocks = [] :: [event_utils:file_block()]
}).

%% definition of an event associated with an update operation in the file system
%% type - wrapped structure that has been modified
-record(update_event, {
    type :: #file_attr{} | #file_location{}
}).

%% definition of an event associated with a write operation in the file system
%% file_uuid - UUID of a file associated with the write operation
%% file_size - size of a file after the write operation
%% size      - number of bytes written
%% blocks    - list of offset, size pairs that describes bytes segments written
-record(write_event, {
    file_uuid :: file_meta:uuid(),
    file_size :: file_meta:size(),
    size :: file_meta:size(),
    blocks = [] :: [event_utils:file_block()]
}).

-endif.