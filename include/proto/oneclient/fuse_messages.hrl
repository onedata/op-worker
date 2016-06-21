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
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

-record(child_link, {
    uuid :: binary() | fslogic_worker:file_guid(),
    name :: binary()
}).

-record(get_file_attr, {
    entry :: fslogic_worker:ext_file()
}).

-record(get_file_children, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    offset :: file_meta:offset(),
    size :: file_meta:size()
}).

-record(create_dir, {
    name :: file_meta:name(),
    parent_uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    mode :: file_meta:mode()
}).

-record(delete_file, {
    uuid :: file_meta:uuid()
}).

-record(update_times, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    atime :: file_meta:time(),
    mtime :: file_meta:time(),
    ctime :: file_meta:time()
}).

-record(change_mode, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    mode :: file_meta:mode()
}).

-record(rename, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    target_path :: file_meta:path()
}).

-record(get_new_file_location, {
    name :: file_meta:name(),
    parent_uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    flags :: atom(),
    mode = 8#644 :: file_meta:posix_permissions()
}).

-record(get_file_location, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    flags :: fslogic_worker:open_flags()
}).

-record(unlink, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid()
}).

-record(get_helper_params, {
    storage_id :: storage:id(),
    force_proxy_io = false :: boolean()
}).

-record(truncate, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    size :: non_neg_integer()
}).

-record(release, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    handle_id :: binary()
}).

-record(synchronize_block, {
    uuid :: file_meta:uuid() | fslogic_worker:file_guid(),
    block :: #file_block{},
    prefetch :: boolean()
}).

-record(synchronize_block_and_compute_checksum, {
    uuid :: file_meta:uuid(),
    block :: #file_block{}
}).

-record(create_storage_test_file, {
    storage_id :: storage:id(),
    file_uuid :: file_meta:uuid() | fslogic_worker:file_guid()
}).

-record(verify_storage_test_file, {
    storage_id :: storage:id(),
    space_uuid :: file_meta:uuid(),
    file_id :: helpers:file(),
    file_content :: binary()
}).

-type fuse_request() ::
    #get_file_attr{} | #get_file_children{} | #create_dir{} | #delete_file{} |
    #update_times{} | #change_mode{} | #rename{} | #release{} | #truncate{} |
    #get_helper_params{} | #get_new_file_location{} | #get_file_location{} |
    #synchronize_block{} | #create_storage_test_file{} |
    #verify_storage_test_file{} | #synchronize_block_and_compute_checksum{}.


-record(file_children, {
    child_links :: [#child_link{}]
}).

-record(helper_arg, {
    key :: binary(),
    value :: binary()
}).

-record(helper_params, {
    helper_name :: binary(),
    helper_args :: [#helper_arg{}]
}).

-record(storage_test_file, {
    helper_params :: #helper_params{},
    space_uuid :: file_meta:uuid(),
    file_id :: helpers:file(),
    file_content :: binary()
}).

-record(checksum, {
    value :: binary()
}).

-record(file_renamed, {
    new_uuid :: fslogic_worker:file_guid(),
    child_entries :: [#file_renamed_entry{}]
}).

-type fuse_response() ::
    #file_attr{} | #file_children{} | #helper_params{} | #file_location{} |
    #storage_test_file{} | #dir{} | #checksum{} | #file_renamed{}.

-record(fuse_request, {
    fuse_request :: fuse_request()
}).

-record(fuse_response, {
    status :: #status{},
    fuse_response :: fuse_response()
}).

-endif.
