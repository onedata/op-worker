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
-include("modules/fslogic/file_attr.hrl").
-include("modules/datastore/datastore_models.hrl").

-define(AUTO_HELPER_MODE, 'AUTO').
-define(FORCE_PROXY_HELPER_MODE, 'FORCE_PROXY').
-define(FORCE_DIRECT_HELPER_MODE, 'FORCE_DIRECT').

%% @TODO VFS-11299 deprecated, left for compatibility with oneclient
-record(child_link, {
    guid :: fslogic_worker:file_guid(),
    name :: binary()
}).

-record(get_file_attr, {
    attributes = [] :: [file_attr:attribute()]
}).

-record(get_file_references, {
}).

-record(get_child_attr, {
    name :: file_meta:name(),
    attributes = [] :: [file_attr:attribute()]
}).

%% @TODO VFS-11299 deprecated, left for compatibility with oneclient
-record(get_file_children, {
    listing_options :: file_listing:options()
}).

-record(get_file_children_attrs, {
    listing_options :: file_listing:options(),
    attributes = [] :: [file_attr:attribute()]
}).

-record(create_dir, {
    name :: file_meta:name(),
    mode = ?DEFAULT_DIR_PERMS :: file_meta:mode()
}).

-record(delete_file, {
    silent = false :: boolean()
}).

-record(move_to_trash, {
    emit_events = true :: boolean()
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

-record(make_link, {
    target_parent_guid :: fslogic_worker:file_guid(),
    target_name :: file_meta:name()
}).

-record(make_symlink, {
    target_name :: file_meta:name(),
    link :: file_meta_symlinks:symlink()
}).

-record(open_file, {
    flag :: fslogic_worker:open_flag()
}).

-record(open_file_with_extended_info, {
    flag :: fslogic_worker:open_flag()
}).

-record(get_file_location, {
}).

% Operation that returns symlink value as it is
-record(read_symlink, {
}).

% Operation that returns target file (file pointed by path stored in symlink) guid
-record(resolve_symlink, {}).

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

-record(xattr, {
    name :: binary(),
    value :: term()
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

-record(get_file_attr_by_path, {
    path :: file_meta:path(),
    attributes = [] :: [file_attr:attribute()]
}).

-record(create_path, {
    path :: file_meta:path()
}).

-record(report_file_written, {
    offset :: non_neg_integer(),
    size :: integer()
}).

-record(report_file_read, {
    offset :: non_neg_integer(),
    size :: integer()
}).

-record(get_recursive_file_list, {
    listing_options :: dir_req:recursive_listing_opts(),
    attributes = [] :: [file_attr:attribute()]
}).

-type file_request_type() ::
    #get_file_attr{} | #get_file_references{} |
    #get_file_children{} | #get_file_children_attrs{} |
    #create_dir{} | #delete_file{} | #move_to_trash{} |
    #update_times{} | #change_mode{} |
    #rename{} | #create_file{} | #make_file{} |
    #make_link{} | #make_symlink{} | #open_file{} | #get_file_location{} |
    #read_symlink{} | #resolve_symlink{} |
    #release{} | #truncate{} | #synchronize_block{} |
    #synchronize_block_and_compute_checksum{} | #block_synchronization_request{} |
    #get_child_attr{} | #get_xattr{} | #set_xattr{} | #remove_xattr{} |
    #list_xattr{} | #fsync{} |
    #storage_file_created{} | #open_file_with_extended_info{} | 
    #get_file_attr_by_path{} | #create_path{} | #report_file_written{} | #report_file_read{} | 
    #get_recursive_file_list{}.

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

-record(resolve_guid_by_relative_path, {
    % file, relative to which the path remainder will be resolved
    root_file :: fslogic_worker:file_guid(),
    path :: file_meta:path()
}).

-record(ensure_dir, {
    % file, relative to which the path remainder will be resolved
    root_file :: fslogic_worker:file_guid(),
    path :: file_meta:path(),
    mode :: file_meta:mode()
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

-record(create_multipart_upload, {
    space_id :: od_space:id(),
    path :: multipart_upload:path()
}).

-record(upload_multipart_part, {
    multipart_upload_id :: multipart_upload:id(),
    part :: multipart_upload_part:record()
}).

-record(list_multipart_parts, {
    multipart_upload_id :: multipart_upload:id(),
    limit :: undefined | non_neg_integer(),
    part_marker :: undefined | non_neg_integer()
}).

-record(abort_multipart_upload, {
    multipart_upload_id :: multipart_upload:id()
}).

-record(complete_multipart_upload, {
    multipart_upload_id :: multipart_upload:id()
}).

-record(list_multipart_uploads, {
    space_id :: od_space:id(),
    limit :: non_neg_integer(),
    index_token :: multipart_upload:pagination_token() | undefined
}).

-type multipart_request_type() :: #create_multipart_upload{} | #upload_multipart_part{} |
    #list_multipart_parts{} | #abort_multipart_upload{} | #complete_multipart_upload{} |
    #list_multipart_uploads{}.

-record(multipart_upload_request, {
    multipart_request :: multipart_request_type()
}).


-type fuse_request_type() ::
    #resolve_guid{} | #resolve_guid_by_canonical_path{} | #resolve_guid_by_relative_path{} | #ensure_dir{} |
    #get_helper_params{} | #create_storage_test_file{} | #verify_storage_test_file{} | #file_request{} | 
    #get_fs_stats{} |  #multipart_upload_request{}.

-record(fuse_request, {
    fuse_request :: fuse_request_type()
}).

-record(file_children, {
    child_links :: [#child_link{}],
    pagination_token :: file_listing:pagination_token()
}).

-record(file_children_attrs, {
    child_attrs :: [#file_attr{}],
    pagination_token :: file_listing:pagination_token()
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

-record(symlink, {
    link :: file_meta_symlinks:symlink()
}).

-record(recursive_listing_result, {
    entries :: [any()], % [recursive_listing:result_entry()] but dialyzer does not accept it
    inaccessible_paths :: [any()], % [recursive_listing:node_path()] but dialyzer does not accept it
    pagination_token :: undefined | recursive_listing:pagination_token()
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

-record(file_references, {
    references :: [file_id:file_guid()]
}).

-record(multipart_parts, {
    parts :: [multipart_upload_part:record()],
    is_last :: boolean()
}).

-record(multipart_uploads, {
    uploads :: [multipart_upload:record()],
    is_last :: boolean(),
    next_page_token :: multipart_upload:pagination_token() | undefined
}).

-type fuse_response_type() ::
    #file_attr{} | #file_references{} | #file_children{} | #file_location{} | #helper_params{} |
    #storage_test_file{} | #dir{} | #sync_response{} | #file_created{} |
    #file_opened{} | #file_renamed{} | #guid{} | #xattr_list{} | #xattr{} |
    #file_children_attrs{} | #file_location_changed{} | #recursive_listing_result{} | 
    #file_opened_extended{} | #fs_stats{} | #symlink{} |
    #multipart_uploads{} | #multipart_upload{} | #multipart_parts{} |
    undefined.

-record(fuse_response, {
    status :: undefined | #status{},
    fuse_response :: fuse_response_type()
}).

-define(FUSE_OK_RESP, #fuse_response{
    status = #status{code = ?OK}
}).

-define(FUSE_OK_RESP(__RESPONSE), (?FUSE_OK_RESP)#fuse_response{
    fuse_response = __RESPONSE
}).

-endif.
