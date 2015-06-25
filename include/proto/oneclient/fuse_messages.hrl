%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Communication protocol between FUSE client and the server.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(FUSE_MESSAGES_HRL).
-define(FUSE_MESSAGES_HRL, 1).

-include("common_messages.hrl").

-record(child_link, {
    uuid :: binary(),
    name :: binary()
}).

-record(get_file_attr, {
    entry :: file_meta:entry()
}).

-record(get_file_children, {
    uuid :: file_meta:uuid(),
    offset :: file_meta:offset(),
    size :: file_meta:size()
}).

-record(create_dir, {
    name :: file_meta:name(),
    parent_uuid :: file_meta:uuid(),
    mode :: file_meta:mode()
}).

-record(delete_file, {
    uuid :: file_meta:uuid()
}).

-record(update_times, {
    uuid :: file_meta:uuid(),
    atime :: file_meta:time(),
    mtime :: file_meta:time(),
    ctime :: file_meta:time()
}).

-record(file_attr, {
    uuid :: file_meta:uuid(),
    name :: file_meta:name(),
    mode :: file_meta:mode(),
    uid :: file_meta:uuid(),
    gid :: file_meta:uuid(),
    atime :: file_meta:time(),
    mtime :: file_meta:time(),
    ctime :: file_meta:time(),
    type :: file_meta:type(),
    size :: file_meta:size()
}).

-record(file_children, {
    child_links :: [#child_link{}]
}).

-type fuse_request() :: #get_file_attr{} | #get_file_children{} | #create_dir{} |
                        #delete_file{} | #update_times{}.

-type fuse_response() :: #file_attr{} | #file_children{}.

-record(fuse_request, {
    fuse_request :: fuse_request()
}).

-record(fuse_response, {
    status :: #status{},
    fuse_response :: fuse_response()
}).

-endif.