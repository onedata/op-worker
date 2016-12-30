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
}).

-record(get_child_attr, {
    name :: file_meta:name()
}).

-record(get_file_children, {
    offset :: file_meta:offset(),
    size :: file_meta:size()
}).

-record(create_dir, {
    name :: file_meta:name(),
    mode :: file_meta:mode()
}).

-record(delete_file, {
    silent = false :: boolean()
}).

-record(update_times, {
    atime :: file_meta:time(),
    mtime :: file_meta:time(),
    ctime :: file_meta:time()
}).

-record(change_mode, {
    mode :: file_meta:mode()
}).

-record(rename, {
    target_parent_uuid :: fslogic_worker:file_guid(),
    target_name :: file_meta:name()
}).

-record(create_file, {
    name :: file_meta:name(),
    mode = 8#644 :: file_meta:posix_permissions(),
    flag = rdwr :: fslogic_worker:open_flag()
}).

-record(make_file, {
    name :: file_meta:name(),
    mode = 8#644 :: file_meta:posix_permissions()
}).

-record(open_file, {
    flag :: fslogic_worker:open_flag()
}).

-record(get_file_location, {
}).

-record(release, {
    handle_id :: binary()
}).

-record(truncate, {
    size :: non_neg_integer()
}).

-record(synchronize_block, {
    block :: #file_block{},
    prefetch = false :: boolean()
}).

-record(synchronize_block_and_compute_checksum, {
    block :: #file_block{}
}).

-type file_request_type() ::
    #get_file_attr{} | #get_file_children{} | #create_dir{} | #delete_file{} |
    #update_times{} | #change_mode{} | #rename{} | #create_file{} | #make_file{} |
    #open_file{} | #get_file_location{} | #release{} | #truncate{} |
    #synchronize_block{} | #synchronize_block_and_compute_checksum{} |
    #get_child_attr{}.

-record(file_request, {
    context_guid :: fslogic_worker:file_guid(),
    file_request :: file_request_type()
}).

-record(resolve_guid, {
    path :: file_meta:path()
}).

-record(get_helper_params, {
    storage_id :: storage:id(),
    force_proxy_io = false :: boolean()
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

-type fuse_request_value() ::
    #resolve_guid{} | #get_helper_params{} | #create_storage_test_file{} |
    #verify_storage_test_file{} | #file_request{}.

-record(fuse_request, {
    fuse_request :: fuse_request_value()
}).

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

-record(sync_response, {
    checksum :: binary(),
    file_location :: #file_location{}
}).

-record(file_created, {
    handle_id :: binary(),
    file_attr :: #file_attr{},
    file_location :: #file_location{}
}).

-record(file_opened, {
    handle_id :: binary()
}).

-record(file_renamed, {
    new_uuid :: fslogic_worker:file_guid(),
    child_entries :: undefined | [#file_renamed_entry{}]
}).

-record(uuid, {
    uuid :: fslogic_worker:file_guid()
}).

-type fuse_response_type() ::
    #file_attr{} | #file_children{} | #file_location{} | #helper_params{} |
    #storage_test_file{} | #dir{} | #sync_response{} | #file_created{} |
    #file_opened{} | #file_renamed{} | #uuid{} | undefined.

-record(fuse_response, {
    status :: undefined | #status{},
    fuse_response :: fuse_response_type()
}).

-endif.
