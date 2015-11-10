%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal protocol layer between FUSE client and the server.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(FUSE_MESSAGES_HRL).
-define(FUSE_MESSAGES_HRL, 1).

-include("common_messages.hrl").
-include("cluster/worker/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").

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

-record(change_mode, {
    uuid :: file_meta:uuid(),
    mode :: file_meta:mode()
}).

-record(rename, {
    uuid :: file_meta:uuid(),
    target_path :: file_meta:path()
}).

-record(file_attr, {
    uuid :: file_meta:uuid(),
    name :: file_meta:name(),
    mode :: file_meta:mode(),
    uid = 0 :: file_meta:uuid(),
    gid = 0 :: file_meta:uuid(),
    atime = 0 :: file_meta:time(),
    mtime = 0 :: file_meta:time(),
    ctime = 0 :: file_meta:time(),
    type :: file_meta:type(),
    size = 0 :: file_meta:size()
}).

-record(file_children, {
    child_links :: [#child_link{}]
}).

-record(get_new_file_location, {
    name :: file_meta:name(),
    parent_uuid :: file_meta:uuid(),
    flags :: atom(),
    mode = 8#644 :: file_meta:posix_permissions()
}).

-record(get_file_location, {
    uuid :: file_meta:uuid(),
    flags :: fslogic_worker:open_flags()
}).

-record(unlink, {
    uuid :: file_meta:uuid()
}).


-record(get_helper_params, {
    storage_id :: storage:id(),
    force_cluster_proxy = false :: boolean()
}).

-record(truncate, {
    uuid :: file_meta:uuid(),
    size :: non_neg_integer()
}).


-record(close, {
    uuid :: file_meta:uuid()
}).


-record(helper_arg, {
    key :: binary(),
    value :: binary()
}).


-record(helper_params, {
    helper_name :: binary(),
    helper_args :: [#helper_arg{}]
}).


-type fuse_request() :: #get_file_attr{} | #get_file_children{} | #create_dir{} |
                        #delete_file{} | #update_times{} | #change_mode{} | #rename{} |
                        #close{} | #truncate{} | #get_helper_params{} | #get_new_file_location{} |
                        #get_file_location{}.

-type fuse_response() :: #file_attr{} | #file_children{} | #helper_params{} | #file_location{}.

-record(fuse_request, {
    fuse_request :: fuse_request()
}).

-record(fuse_response, {
    status :: #status{},
    fuse_response :: fuse_response()
}).

-endif.