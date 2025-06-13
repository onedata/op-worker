%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc List of fslogic file operations.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(FSLOGIC_OPERATIONS_HRL).
-define(FSLOGIC_OPERATIONS_HRL, 1).

-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").


%% NOTE: Any operation added here will be automatically allowed for space dir.
%% For more details see space_dir:allowed_operations/0.
-define(FSLOGIC_ALL_OPERATIONS, [
    %% @TODO VFS-12502 - Untangle fslogic operations types
    % FUSE REQUEST
    #resolve_guid{}, #resolve_guid_by_canonical_path{}, #resolve_guid_by_relative_path{}, #ensure_dir{},
    #get_helper_params{}, #create_storage_test_file{}, #verify_storage_test_file{}, #file_request{},
    #get_fs_stats{},  #multipart_upload_request{},

    % FILE REQUEST
    #get_file_attr{}, #get_file_references{}, #get_file_children{}, #get_file_children_attrs{},
    #create_dir{}, #delete_file{}, #move_to_trash{}, #update_times{}, #change_mode{},
    #rename{}, #create_file{}, #make_file{}, #make_link{}, #make_symlink{}, #open_file{}, #get_file_location{},
    #read_symlink{}, #resolve_symlink{}, #release{}, #truncate{}, #synchronize_block{},
    #synchronize_block_and_compute_checksum{}, #block_synchronization_request{}, #get_child_attr{}, #get_xattr{},
    #set_xattr{}, #remove_xattr{}, #list_xattr{}, #fsync{}, #storage_file_created{}, #open_file_with_extended_info{},
    #get_file_attr_by_path{}, #create_path{}, #report_file_written{}, #report_file_read{}, #get_recursive_file_list{},

    % PROXYIO REQUEST
    #remote_read{}, #remote_write{}
]).

-endif.
