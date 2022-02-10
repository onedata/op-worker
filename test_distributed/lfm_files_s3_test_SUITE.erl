%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%% @doc
%%% This file contains tests of lfm API on s3 storage.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_files_s3_test_SUITE).
-author("Michal Cwiertnia").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    fslogic_new_file_test/1,
    lfm_create_and_unlink_test/1,
    lfm_create_and_access_test/1,
    lfm_basic_rename_test/1,
    lfm_basic_rdwr_test/1,
    lfm_basic_rdwr_opens_file_once_test/1,
    lfm_basic_rdwr_after_file_delete_test/1,
    lfm_write_test/1,
    lfm_stat_test/1,
    lfm_get_details_test/1,
    lfm_synch_stat_test/1,
    lfm_cp_file/1,
    lfm_cp_empty_dir/1,
    lfm_cp_dir_to_itself_should_fail/1,
    lfm_cp_dir_to_its_child_should_fail/1,
    lfm_cp_dir/1,
    lfm_truncate_test/1,
    lfm_truncate_and_write/1,
    lfm_acl_test/1,
    lfm_rmdir_test/1,
    lfm_rmdir_fails_with_eperm_on_space_directory_test/1,
    rm_recursive_test/1,
    rm_recursive_fails_with_eperm_on_space_directory_test/1,
    file_gap_test/1,
    ls_test/1,
    ls_with_stats_test/1,
    create_share_dir_test/1,
    create_share_file_test/1,
    remove_share_test/1,
    share_getattr_test/1,
    share_get_parent_test/1,
    share_list_test/1,
    share_read_test/1,
    share_child_getattr_test/1,
    share_child_list_test/1,
    share_child_read_test/1,
    share_permission_denied_test/1,
    echo_loop_test/1,
    deferred_creation_should_not_prevent_mv/1,
    deferred_creation_should_not_prevent_truncate/1,
    new_file_should_not_have_popularity_doc/1,
    new_file_should_have_zero_popularity/1,
    opening_file_should_increase_file_popularity/1,
    file_popularity_should_have_correct_file_size/1,
    readdir_plus_should_return_empty_result_for_empty_dir/1,
    readdir_plus_should_return_empty_result_zero_size/1,
    readdir_plus_should_work_with_zero_offset/1,
    readdir_plus_should_work_with_non_zero_offset/1,
    readdir_plus_should_work_with_size_greater_than_dir_size/1,
    readdir_plus_should_work_with_token/1,
    readdir_plus_should_work_with_token2/1,
    readdir_should_work_with_token/1,
    readdir_should_work_with_token2/1,
    readdir_should_work_with_startid/1,
    get_children_details_should_return_empty_result_for_empty_dir/1,
    get_children_details_should_return_empty_result_zero_size/1,
    get_children_details_should_work_with_zero_offset/1,
    get_children_details_should_work_with_non_zero_offset/1,
    get_children_details_should_work_with_size_greater_than_dir_size/1,
    get_children_details_should_work_with_startid/1,
    lfm_recreate_handle_test/1,
    lfm_write_after_create_no_perms_test/1,
    lfm_recreate_handle_after_delete_test/1,
    lfm_open_failure_test/1,
    lfm_create_and_open_failure_test/1,
    lfm_open_in_direct_mode_test/1,
    lfm_mv_failure_test/1,
    lfm_open_multiple_times_failure_test/1,
    lfm_open_failure_multiple_users_test/1,
    lfm_open_and_create_open_failure_test/1,
    lfm_mv_failure_multiple_users_test/1,
    sparse_files_should_be_created/1,
    lfm_close_deleted_open_files/1
]).

-define(TEST_CASES, [
    fslogic_new_file_test,
    lfm_create_and_unlink_test,
    lfm_create_and_access_test,
    lfm_basic_rename_test,
    lfm_basic_rdwr_test,
    lfm_basic_rdwr_opens_file_once_test,
    lfm_basic_rdwr_after_file_delete_test,
    lfm_write_test,
    lfm_stat_test,
    lfm_get_details_test,
    lfm_synch_stat_test,
    lfm_cp_file,
    lfm_cp_empty_dir,
    lfm_cp_dir_to_itself_should_fail,
    lfm_cp_dir_to_its_child_should_fail,
    lfm_cp_dir,
    lfm_truncate_test,
    lfm_truncate_and_write,
    lfm_acl_test,
    lfm_rmdir_test,
    lfm_rmdir_fails_with_eperm_on_space_directory_test,
    rm_recursive_test,
    rm_recursive_fails_with_eperm_on_space_directory_test,
    file_gap_test,
    ls_test,
    ls_with_stats_test,
    create_share_dir_test,
    create_share_file_test,
    remove_share_test,
    share_getattr_test,
    share_get_parent_test,
    share_list_test,
    share_read_test,
    share_child_getattr_test,
    share_child_list_test,
    share_child_read_test,
    share_permission_denied_test,
    echo_loop_test,
    deferred_creation_should_not_prevent_mv,
    deferred_creation_should_not_prevent_truncate,
    new_file_should_not_have_popularity_doc,
    new_file_should_have_zero_popularity,
    opening_file_should_increase_file_popularity,
    file_popularity_should_have_correct_file_size,
    deferred_creation_should_not_prevent_truncate,
    readdir_plus_should_return_empty_result_for_empty_dir,
    readdir_plus_should_return_empty_result_zero_size,
    readdir_plus_should_work_with_zero_offset,
    readdir_plus_should_work_with_non_zero_offset,
    readdir_plus_should_work_with_size_greater_than_dir_size,
    readdir_plus_should_work_with_token,
    readdir_plus_should_work_with_token2,
    readdir_should_work_with_token,
    readdir_should_work_with_token2,
    readdir_should_work_with_startid,
    get_children_details_should_return_empty_result_for_empty_dir,
    get_children_details_should_return_empty_result_zero_size,
    get_children_details_should_work_with_zero_offset,
    get_children_details_should_work_with_non_zero_offset,
    get_children_details_should_work_with_size_greater_than_dir_size,
    get_children_details_should_work_with_startid,
    lfm_recreate_handle_test,
    lfm_write_after_create_no_perms_test,
    lfm_recreate_handle_after_delete_test,
    lfm_open_failure_test,
    lfm_create_and_open_failure_test,
    lfm_open_in_direct_mode_test,
    lfm_mv_failure_test,
    lfm_open_multiple_times_failure_test,
    lfm_open_failure_multiple_users_test,
    lfm_open_and_create_open_failure_test,
    lfm_mv_failure_multiple_users_test,
    sparse_files_should_be_created,
    lfm_close_deleted_open_files
]).

-define(SPACE_ID, <<"space1">>).

-define(PERFORMANCE_TEST_CASES, [
    ls_test, ls_with_stats_test, echo_loop_test
]).

all() ->
    ?ALL(?TEST_CASES, ?PERFORMANCE_TEST_CASES).


%%%====================================================================
%%% Test function
%%%====================================================================

lfm_recreate_handle_test(Config) ->
    lfm_files_test_base:lfm_recreate_handle(Config, ?DEFAULT_FILE_PERMS, dont_delete_file).

lfm_write_after_create_no_perms_test(Config) ->
    lfm_files_test_base:lfm_recreate_handle(Config, 8#444, dont_delete_file).

lfm_recreate_handle_after_delete_test(Config) ->
    lfm_files_test_base:lfm_recreate_handle(Config, ?DEFAULT_FILE_PERMS, delete_after_open).

lfm_open_failure_test(Config) ->
    lfm_files_test_base:lfm_open_failure(Config).

lfm_create_and_open_failure_test(Config) ->
    lfm_files_test_base:lfm_create_and_open_failure(Config).

lfm_open_and_create_open_failure_test(Config) ->
    lfm_files_test_base:lfm_open_and_create_open_failure(Config).

lfm_open_multiple_times_failure_test(Config) ->
    lfm_files_test_base:lfm_open_multiple_times_failure(Config).

lfm_open_failure_multiple_users_test(Config) ->
    lfm_files_test_base:lfm_open_failure_multiple_users(Config).

lfm_open_in_direct_mode_test(Config) ->
    lfm_files_test_base:lfm_open_in_direct_mode(Config).

lfm_mv_failure_test(Config) ->
    lfm_files_test_base:lfm_mv_failure(Config).

lfm_mv_failure_multiple_users_test(Config) ->
    lfm_files_test_base:lfm_mv_failure_multiple_users(Config).

readdir_plus_should_return_empty_result_for_empty_dir(Config) ->
    lfm_files_test_base:readdir_plus_should_return_empty_result_for_empty_dir(Config).

readdir_plus_should_return_empty_result_zero_size(Config) ->
    lfm_files_test_base:readdir_plus_should_return_empty_result_zero_size(Config).

readdir_plus_should_work_with_zero_offset(Config) ->
    lfm_files_test_base:readdir_plus_should_work_with_zero_offset(Config).

readdir_plus_should_work_with_non_zero_offset(Config) ->
    lfm_files_test_base:readdir_plus_should_work_with_non_zero_offset(Config).

readdir_plus_should_work_with_size_greater_than_dir_size(Config) ->
    lfm_files_test_base:readdir_plus_should_work_with_size_greater_than_dir_size(Config).

readdir_plus_should_work_with_token(Config) ->
    lfm_files_test_base:readdir_plus_should_work_with_token(Config).

readdir_plus_should_work_with_token2(Config) ->
    lfm_files_test_base:readdir_plus_should_work_with_token2(Config).

readdir_should_work_with_token(Config) ->
    lfm_files_test_base:readdir_should_work_with_token(Config).

readdir_should_work_with_token2(Config) ->
    lfm_files_test_base:readdir_should_work_with_token2(Config).

readdir_should_work_with_startid(Config) ->
    lfm_files_test_base:readdir_should_work_with_startid(Config).

get_children_details_should_return_empty_result_for_empty_dir(Config) ->
    lfm_files_test_base:get_children_details_should_return_empty_result_for_empty_dir(Config).

get_children_details_should_return_empty_result_zero_size(Config) ->
    lfm_files_test_base:get_children_details_should_return_empty_result_zero_size(Config).

get_children_details_should_work_with_zero_offset(Config) ->
    lfm_files_test_base:get_children_details_should_work_with_zero_offset(Config).

get_children_details_should_work_with_non_zero_offset(Config) ->
    lfm_files_test_base:get_children_details_should_work_with_non_zero_offset(Config).

get_children_details_should_work_with_size_greater_than_dir_size(Config) ->
    lfm_files_test_base:get_children_details_should_work_with_size_greater_than_dir_size(Config).

get_children_details_should_work_with_startid(Config) ->
    lfm_files_test_base:get_children_details_should_work_with_startid(Config).

echo_loop_test(Config) ->
    lfm_files_test_base:echo_loop(Config).

ls_with_stats_test(Config) ->
    lfm_files_test_base:ls_with_stats(Config).

ls_test(Config) ->
    lfm_files_test_base:ls(Config).

fslogic_new_file_test(Config) ->
    lfm_files_test_base:fslogic_new_file(Config).

lfm_create_and_access_test(Config) ->
    lfm_files_test_base:lfm_create_and_access(Config).

lfm_create_and_unlink_test(Config) ->
    lfm_files_test_base:lfm_create_and_unlink(Config).

lfm_basic_rename_test(Config) ->
    lfm_files_test_base:lfm_basic_rename(Config).

lfm_basic_rdwr_test(Config) ->
    lfm_files_test_base:lfm_basic_rdwr(Config).

lfm_basic_rdwr_opens_file_once_test(Config) ->
    lfm_files_test_base:lfm_basic_rdwr_opens_file_once(Config).

lfm_basic_rdwr_after_file_delete_test(Config) ->
    lfm_files_test_base:lfm_basic_rdwr_after_file_delete(Config).

lfm_write_test(Config) ->
    lfm_files_test_base:lfm_write(Config).

lfm_stat_test(Config) ->
    lfm_files_test_base:lfm_stat(Config).

lfm_get_details_test(Config) ->
    lfm_files_test_base:lfm_get_details(Config).

lfm_synch_stat_test(Config) ->
    lfm_files_test_base:lfm_synch_stat(Config).

lfm_cp_file(Config) ->
    lfm_files_test_base:lfm_cp_file(Config).

lfm_cp_empty_dir(Config) ->
    lfm_files_test_base:lfm_cp_empty_dir(Config).

lfm_cp_dir_to_itself_should_fail(Config) ->
    lfm_files_test_base:lfm_cp_dir_to_itself_should_fail(Config).

lfm_cp_dir_to_its_child_should_fail(Config) ->
    lfm_files_test_base:lfm_cp_dir_to_its_child_should_fail(Config).

lfm_cp_dir(Config) ->
    lfm_files_test_base:lfm_cp_dir(Config).

lfm_truncate_test(Config) ->
    lfm_files_test_base:lfm_truncate(Config).

lfm_truncate_and_write(Config) ->
    lfm_files_test_base:lfm_truncate_and_write(Config).

lfm_acl_test(Config) ->
    lfm_files_test_base:lfm_acl(Config).

lfm_rmdir_test(Config) ->
    lfm_files_test_base:lfm_rmdir(Config).

lfm_rmdir_fails_with_eperm_on_space_directory_test(Config) ->
    lfm_files_test_base:lfm_rmdir_fails_with_eperm_on_space_directory(Config).

rm_recursive_test(Config) ->
    lfm_files_test_base:rm_recursive(Config).

rm_recursive_fails_with_eperm_on_space_directory_test(Config) ->
    lfm_files_test_base:rm_recursive_fails_with_eperm_on_space_directory(Config).

file_gap_test(Config) ->
    lfm_files_test_base:file_gap(Config).

create_share_dir_test(Config) ->
    lfm_files_test_base:create_share_dir(Config).

create_share_file_test(Config) ->
    lfm_files_test_base:create_share_file(Config).

remove_share_test(Config) ->
    lfm_files_test_base:remove_share(Config).

share_getattr_test(Config) ->
    lfm_files_test_base:share_getattr(Config).

share_get_parent_test(Config) ->
    lfm_files_test_base:share_get_parent(Config).

share_list_test(Config) ->
    lfm_files_test_base:share_list(Config).

share_read_test(Config) ->
    lfm_files_test_base:share_read(Config).

share_child_getattr_test(Config) ->
    lfm_files_test_base:share_child_getattr(Config).

share_child_list_test(Config) ->
    lfm_files_test_base:share_child_list(Config).

share_child_read_test(Config) ->
    lfm_files_test_base:share_child_read(Config).

share_permission_denied_test(Config) ->
    lfm_files_test_base:share_permission_denied(Config).

deferred_creation_should_not_prevent_mv(Config) ->
    lfm_files_test_base:deferred_creation_should_not_prevent_mv(Config).

deferred_creation_should_not_prevent_truncate(Config) ->
    lfm_files_test_base:deferred_creation_should_not_prevent_truncate(Config).

new_file_should_not_have_popularity_doc(Config) ->
    lfm_files_test_base:new_file_should_not_have_popularity_doc(Config).

new_file_should_have_zero_popularity(Config) ->
    lfm_files_test_base:new_file_should_have_zero_popularity(Config).

opening_file_should_increase_file_popularity(Config) ->
    lfm_files_test_base:opening_file_should_increase_file_popularity(Config).

file_popularity_should_have_correct_file_size(Config) ->
    lfm_files_test_base:file_popularity_should_have_correct_file_size(Config).

sparse_files_should_be_created(Config) ->
    lfm_files_test_base:sparse_files_should_be_created(Config, check_size_and_read).

lfm_close_deleted_open_files(Config) ->
    lfm_files_test_base:lfm_close_deleted_open_files(Config).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    lfm_files_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    lfm_files_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    lfm_files_test_base:init_per_testcase(Case, Config).

end_per_testcase(Case, Config) ->
    lfm_files_test_base:end_per_testcase(Case, Config).
