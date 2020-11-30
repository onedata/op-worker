%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test suite verifies correct behaviour of posix and acl permissions
%%% with corresponding lfm (logical_file_manager) functions and posix storage.
%%% @end
%%%-------------------------------------------------------------------
-module(permissions_posix_test_SUITE).
-author("Bartosz Walkowicz").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    data_access_caveats_test/1,
    data_access_caveats_ancestors_test/1,
    data_access_caveats_ancestors_test2/1,
    data_access_caveats_cache_test/1,

    mkdir_test/1,
    get_children_test/1,
    get_children_attrs_test/1,
    get_children_details_test/1,
    get_child_attr_test/1,
    mv_dir_test/1,
    rm_dir_test/1,

    create_file_test/1,
    open_for_read_test/1,
    open_for_write_test/1,
    open_for_rdwr_test/1,
    create_and_open_test/1,
    truncate_test/1,
    mv_file_test/1,
    rm_file_test/1,

    get_parent_test/1,
    get_file_path_test/1,
    get_file_guid_test/1,
    get_file_attr_test/1,
    get_file_details_test/1,
    get_file_distribution_test/1,

    set_perms_test/1,
    check_read_perms_test/1,
    check_write_perms_test/1,
    check_rdwr_perms_test/1,

    create_share_test/1,
    remove_share_test/1,
    share_perms_test/1,

    get_acl_test/1,
    set_acl_test/1,
    remove_acl_test/1,

    get_transfer_encoding_test/1,
    set_transfer_encoding_test/1,
    get_cdmi_completion_status_test/1,
    set_cdmi_completion_status_test/1,
    get_mimetype_test/1,
    set_mimetype_test/1,

    get_metadata_test/1,
    set_metadata_test/1,
    remove_metadata_test/1,
    get_xattr_test/1,
    list_xattr_test/1,
    set_xattr_test/1,
    remove_xattr_test/1,

    add_qos_entry_test/1,
    get_qos_entry_test/1,
    remove_qos_entry_test/1,
    get_effective_file_qos_test/1,
    check_qos_fulfillment_test/1,

    permission_cache_test/1,
    multi_provider_permission_cache_test/1,
    expired_session_test/1
]).

all() -> [
    data_access_caveats_test,
    data_access_caveats_ancestors_test,
    data_access_caveats_ancestors_test2,
    data_access_caveats_cache_test,

    mkdir_test,
    get_children_test,
    get_children_attrs_test,
    get_children_details_test,
    get_child_attr_test,
    mv_dir_test,
    rm_dir_test,

    create_file_test,
    open_for_read_test,
    open_for_write_test,
    open_for_rdwr_test,
    create_and_open_test,
    truncate_test,
    mv_file_test,
    rm_file_test,

    get_parent_test,
    get_file_path_test,
    get_file_guid_test,
    get_file_attr_test,
    get_file_details_test,
    get_file_distribution_test,

    set_perms_test,
    check_read_perms_test,
    check_write_perms_test,
    check_rdwr_perms_test,

    create_share_test,
    remove_share_test,
    share_perms_test,

    get_acl_test,
    set_acl_test,
    remove_acl_test,

    get_transfer_encoding_test,
    set_transfer_encoding_test,
    get_cdmi_completion_status_test,
    set_cdmi_completion_status_test,
    get_mimetype_test,
    set_mimetype_test,

    get_metadata_test,
    set_metadata_test,
    remove_metadata_test,
    get_xattr_test,
    list_xattr_test,
    set_xattr_test,
    remove_xattr_test,

    add_qos_entry_test,
    get_qos_entry_test,
    remove_qos_entry_test,
    get_effective_file_qos_test,
    check_qos_fulfillment_test,

    permission_cache_test,
    multi_provider_permission_cache_test,
    expired_session_test
].


%%%===================================================================
%%% Test functions
%%%===================================================================


data_access_caveats_test(Config) ->
    permissions_test_base:data_access_caveats_test(Config).


data_access_caveats_ancestors_test(Config) ->
    permissions_test_base:data_access_caveats_ancestors_test(Config).


data_access_caveats_ancestors_test2(Config) ->
    permissions_test_base:data_access_caveats_ancestors_test2(Config).


data_access_caveats_cache_test(Config) ->
    permissions_test_base:data_access_caveats_cache_test(Config).


mkdir_test(Config) ->
    permissions_test_base:mkdir_test(Config).


get_children_test(Config) ->
    permissions_test_base:get_children_test(Config).


get_children_attrs_test(Config) ->
    permissions_test_base:get_children_attrs_test(Config).


get_children_details_test(Config) ->
    permissions_test_base:get_children_details_test(Config).


get_child_attr_test(Config) ->
    permissions_test_base:get_child_attr_test(Config).


mv_dir_test(Config) ->
    permissions_test_base:mv_dir_test(Config).


rm_dir_test(Config) ->
    permissions_test_base:rm_dir_test(Config).


create_file_test(Config) ->
    permissions_test_base:create_file_test(Config).


open_for_read_test(Config) ->
    permissions_test_base:open_for_read_test(Config).


open_for_write_test(Config) ->
    permissions_test_base:open_for_write_test(Config).


open_for_rdwr_test(Config) ->
    permissions_test_base:open_for_rdwr_test(Config).


create_and_open_test(Config) ->
    permissions_test_base:create_and_open_test(Config).


truncate_test(Config) ->
    permissions_test_base:truncate_test(Config).


mv_file_test(Config) ->
    permissions_test_base:mv_file_test(Config).


rm_file_test(Config) ->
    permissions_test_base:rm_file_test(Config).


get_parent_test(Config) ->
    permissions_test_base:get_parent_test(Config).


get_file_path_test(Config) ->
    permissions_test_base:get_file_path_test(Config).


get_file_guid_test(Config) ->
    permissions_test_base:get_file_guid_test(Config).


get_file_attr_test(Config) ->
    permissions_test_base:get_file_attr_test(Config).


get_file_details_test(Config) ->
    permissions_test_base:get_file_details_test(Config).


get_file_distribution_test(Config) ->
    permissions_test_base:get_file_distribution_test(Config).


set_perms_test(Config) ->
    permissions_test_base:set_perms_test(Config).


check_read_perms_test(Config) ->
    permissions_test_base:check_read_perms_test(Config).


check_write_perms_test(Config) ->
    permissions_test_base:check_write_perms_test(Config).


check_rdwr_perms_test(Config) ->
    permissions_test_base:check_rdwr_perms_test(Config).


create_share_test(Config) ->
    permissions_test_base:create_share_test(Config).


remove_share_test(Config) ->
    permissions_test_base:remove_share_test(Config).


share_perms_test(Config) ->
    permissions_test_base:share_perms_test(Config).


get_acl_test(Config) ->
    permissions_test_base:get_acl_test(Config).


set_acl_test(Config) ->
    permissions_test_base:set_acl_test(Config).


remove_acl_test(Config) ->
    permissions_test_base:remove_acl_test(Config).


get_transfer_encoding_test(Config) ->
    permissions_test_base:get_transfer_encoding_test(Config).


set_transfer_encoding_test(Config) ->
    permissions_test_base:set_transfer_encoding_test(Config).


get_cdmi_completion_status_test(Config) ->
    permissions_test_base:get_cdmi_completion_status_test(Config).


set_cdmi_completion_status_test(Config) ->
    permissions_test_base:set_cdmi_completion_status_test(Config).


get_mimetype_test(Config) ->
    permissions_test_base:get_mimetype_test(Config).


set_mimetype_test(Config) ->
    permissions_test_base:set_mimetype_test(Config).


get_metadata_test(Config) ->
    permissions_test_base:get_metadata_test(Config).


set_metadata_test(Config) ->
    permissions_test_base:set_metadata_test(Config).


remove_metadata_test(Config) ->
    permissions_test_base:remove_metadata_test(Config).


get_xattr_test(Config) ->
    permissions_test_base:get_xattr_test(Config).


list_xattr_test(Config) ->
    permissions_test_base:list_xattr_test(Config).


set_xattr_test(Config) ->
    permissions_test_base:set_xattr_test(Config).


remove_xattr_test(Config) ->
    permissions_test_base:remove_xattr_test(Config).


add_qos_entry_test(Config) ->
    permissions_test_base:add_qos_entry_test(Config).


get_qos_entry_test(Config) ->
    permissions_test_base:get_qos_entry_test(Config).


remove_qos_entry_test(Config) ->
    permissions_test_base:remove_qos_entry_test(Config).


get_effective_file_qos_test(Config) ->
    permissions_test_base:get_effective_file_qos_test(Config).


check_qos_fulfillment_test(Config) ->
    permissions_test_base:check_qos_fulfillment_test(Config).


permission_cache_test(Config) ->
    permissions_test_base:permission_cache_test(Config).


multi_provider_permission_cache_test(Config) ->
    permissions_test_base:multi_provider_permission_cache_test(Config).


expired_session_test(Config) ->
    permissions_test_base:expired_session_test(Config).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    permissions_test_base:init_per_suite(Config).


end_per_suite(Config) ->
    permissions_test_base:end_per_suite(Config).


init_per_testcase(Case, Config) ->
    permissions_test_base:init_per_testcase(Case, Config).


end_per_testcase(Case, Config) ->
    permissions_test_base:end_per_testcase(Case, Config).
