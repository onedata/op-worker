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
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/file_details.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

-define(AUTO_HELPER_MODE, 'AUTO').
-define(FORCE_PROXY_HELPER_MODE, 'FORCE_PROXY').
-define(FORCE_DIRECT_HELPER_MODE, 'FORCE_DIRECT').

-record(child_link_uuid, {
    uuid :: file_meta:uuid(),
    name :: binary()
}).

-record(child_link, {
    guid :: fslogic_worker:file_guid(),
    name :: binary()
}).

-record(get_file_attr, {
    include_replication_status :: undefined | boolean()
}).

-record(get_file_details, {
}).

-record(get_child_attr, {
    name :: file_meta:name(),
    include_replication_status :: undefined | boolean()
}).

-record(get_file_children, {
    offset :: file_meta:offset(),
    size :: file_meta:size(),
    index_token = undefined :: undefined | binary(),
    index_startid = undefined :: undefined | binary()
}).

-record(get_file_children_attrs, {
    offset :: file_meta:offset(),
    size :: file_meta:size(),
    index_token :: undefined | binary(),
    include_replication_status :: undefined | boolean()
}).

-record(get_file_children_details, {
    offset :: file_meta:offset(),
    size :: file_meta:size(),
    index_startid = undefined :: undefined | binary()
}).

-record(create_dir, {
    name :: file_meta:name(),
    mode = ?DEFAULT_DIR_PERMS :: file_meta:mode()
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
    target_parent_guid :: fslogic_worker:file_guid(),
    target_name :: file_meta:name()
}).

-record(create_file, {
    name :: file_meta:name(),
    mode = ?DEFAULT_FILE_PERMS :: file_meta:posix_permissions(),
    flag = rdwr :: fslogic_worker:open_flag()
}).

-record(make_file, {
    name :: file_meta:name(),
    mode = ?DEFAULT_FILE_PERMS :: file_meta:posix_permissions()
}).

-record(open_file, {
    flag :: fslogic_worker:open_flag()
}).

-record(open_file_with_extended_info, {
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
    block :: fslogic_blocks:block(),
    prefetch = false :: boolean(),
    priority :: non_neg_integer()
}).

-record(synchronize_block_and_compute_checksum, {
    block :: fslogic_blocks:block(),
    prefetch = false :: boolean(),
    priority :: non_neg_integer()
}).

-record(block_synchronization_request, {
    block :: fslogic_blocks:block(),
    prefetch = false :: boolean(),
    priority :: non_neg_integer()
}).

-record(get_xattr, {
    name :: custom_metadata:name(),
    inherited = false :: boolean()
}).

-record(set_xattr, {
    xattr :: #xattr{},
    create :: boolean(),
    replace :: boolean()
}).

-record(remove_xattr, {
    name :: custom_metadata:name()
}).

-record(list_xattr, {
    inherited = false :: boolean(),
    show_internal = true :: boolean()
}).

-record(fsync, {
    data_only :: boolean(),
    handle_id :: undefined | binary()
}).

-record(storage_file_created, {
}).

-type file_request_type() ::
    #get_file_attr{} | #get_file_children{} | #get_file_children_attrs{} |
    #get_file_details{} | #get_file_children_details{} |
    #create_dir{} | #delete_file{} |
    #update_times{} | #change_mode{} | #rename{} | #create_file{} | #make_file{} |
    #open_file{} | #get_file_location{} | #release{} | #truncate{} |
    #synchronize_block{} | #synchronize_block_and_compute_checksum{} |
    #block_synchronization_request{} |
    #get_child_attr{} | #get_xattr{} | #set_xattr{} | #remove_xattr{} |
    #list_xattr{} | #fsync{} |
    #storage_file_created{} | #open_file_with_extended_info{}.

-record(file_request, {
    context_guid :: fslogic_worker:file_guid(),
    file_request :: file_request_type()
}).

-record(resolve_guid, {
    path :: file_meta:path()
}).

-record(resolve_guid_by_canonical_path, {
    path :: file_meta:path()
}).

-record(get_helper_params, {
    storage_id :: storage:id(),
    space_id :: od_space:id(),
    helper_mode :: ?AUTO_HELPER_MODE | ?FORCE_PROXY_HELPER_MODE | ?FORCE_DIRECT_HELPER_MODE
}).

-record(create_storage_test_file, {
    storage_id :: storage:id(),
    file_guid :: fslogic_worker:file_guid()
}).

-record(verify_storage_test_file, {
    storage_id :: storage:id(),
    space_id :: od_space:id(),
    file_id :: helpers:file_id(),
    file_content :: binary()
}).

-record(get_fs_stats, {
    file_id :: file_id:file_guid()
}).

-type fuse_request_type() ::
    #resolve_guid{} | #resolve_guid_by_canonical_path{} |
    #get_helper_params{} | #create_storage_test_file{} |
    #verify_storage_test_file{} | #file_request{} | #get_fs_stats{}.

-record(fuse_request, {
    fuse_request :: fuse_request_type()
}).

-record(file_children, {
    child_links :: [#child_link{}],
    index_token :: binary(),
    is_last :: boolean()
}).

-record(file_children_attrs, {
    child_attrs :: [#file_attr{}],
    index_token :: binary(),
    is_last :: boolean()
}).

-record(file_children_details, {
    child_details :: [#file_details{}],
    is_last :: boolean()
}).

-record(helper_arg, {
    key :: binary(),
    value :: binary()
}).

-record(helper_params, {
    helper_name :: helper:name(),
    helper_args :: [#helper_arg{}]
}).

-record(storage_test_file, {
    helper_params :: #helper_params{},
    space_id :: od_space:id(),
    file_id :: helpers:file_id(),
    file_content :: binary()
}).

-record(file_location_changed, {
    file_location :: #file_location{},
    change_beg_offset :: undefined | non_neg_integer(),
    change_end_offset :: undefined | non_neg_integer()
}).

-record(sync_response, {
    checksum :: binary(),
    file_location_changed :: #file_location_changed{}
}).

-record(file_created, {
    handle_id :: binary(),
    file_attr :: #file_attr{},
    file_location :: #file_location{}
}).

-record(file_opened, {
    handle_id :: binary()
}).

-record(file_opened_extended, {
    handle_id :: binary(),
    provider_id,
    file_id,
    storage_id
}).

-record(file_renamed, {
    new_guid :: fslogic_worker:file_guid(),
    child_entries :: undefined | [#file_renamed_entry{}]
}).

-record(guid, {
    guid :: fslogic_worker:file_guid()
}).

-record(xattr_list, {
    names :: [custom_metadata:name()]
}).

-record(storage_stats, {
    storage_id :: storage:id(),
    size :: non_neg_integer(),
    occupied :: non_neg_integer()
}).

-record(fs_stats, {
    space_id :: od_space:id(),
    storage_stats :: [#storage_stats{}]
}).

-type fuse_response_type() ::
    #file_attr{} | #file_children{} | #file_location{} | #helper_params{} |
    #storage_test_file{} | #dir{} | #sync_response{} | #file_created{} |
    #file_opened{} | #file_renamed{} | #guid{} | #xattr_list{} | #xattr{} |
    #file_children_attrs{} | #file_location_changed{} | #file_opened_extended{} |
    #file_details{} | #file_children_details{} | #fs_stats{} |
    undefined.

-record(fuse_response, {
    status :: undefined | #status{},
    fuse_response :: fuse_response_type()
}).

-define(FUSE_OK_RESP(__RESPONSE), #fuse_response{
    status = #status{code = ?OK},
    fuse_response = __RESPONSE
}).

-endif.
