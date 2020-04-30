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
    lfm_truncate_test/1,
    lfm_acl_test/1,
    rm_recursive_test/1,
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
    delayed_creation_should_not_prevent_mv/1,
    delayed_creation_should_not_prevent_truncate/1,
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
    lfm_copy_failure_test/1,
    lfm_open_multiple_times_failure_test/1,
    lfm_open_failure_multiple_users_test/1,
    lfm_open_and_create_open_failure_test/1,
    lfm_copy_failure_multiple_users_test/1,
    lfm_rmdir_test/1
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
    lfm_truncate_test,
    lfm_acl_test,
    rm_recursive_test,
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
    delayed_creation_should_not_prevent_mv,
    delayed_creation_should_not_prevent_truncate,
    new_file_should_not_have_popularity_doc,
    new_file_should_have_zero_popularity,
    opening_file_should_increase_file_popularity,
    file_popularity_should_have_correct_file_size,
    delayed_creation_should_not_prevent_truncate,
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
    lfm_copy_failure_test,
    lfm_open_multiple_times_failure_test,
    lfm_open_failure_multiple_users_test,
    lfm_open_and_create_open_failure_test,
    lfm_copy_failure_multiple_users_test,
    lfm_rmdir_test
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

lfm_rmdir_test(Config) ->
    lfm_files_test_base:lfm_rmdir(Config).

lfm_recreate_handle_test(Config) ->
    lfm_files_test_base:lfm_recreate_handle(Config, 8#755, dont_delete_file).

lfm_write_after_create_no_perms_test(Config) ->
    lfm_files_test_base:lfm_recreate_handle(Config, 8#444, dont_delete_file).

lfm_recreate_handle_after_delete_test(Config) ->
    lfm_files_test_base:lfm_recreate_handle(Config, 8#755, delete_after_open).

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

lfm_copy_failure_test(Config) ->
    lfm_files_test_base:lfm_copy_failure(Config).

lfm_copy_failure_multiple_users_test(Config) ->
    lfm_files_test_base:lfm_copy_failure_multiple_users(Config).

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

lfm_truncate_test(Config) ->
    lfm_files_test_base:lfm_truncate(Config).

lfm_acl_test(Config) ->
    lfm_files_test_base:lfm_acl(Config).

rm_recursive_test(Config) ->
    lfm_files_test_base:rm_recursive(Config).

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

delayed_creation_should_not_prevent_mv(Config) ->
    lfm_files_test_base:delayed_creation_should_not_prevent_mv(Config).

delayed_creation_should_not_prevent_truncate(Config) ->
    lfm_files_test_base:delayed_creation_should_not_prevent_truncate(Config).

new_file_should_not_have_popularity_doc(Config) ->
    lfm_files_test_base:new_file_should_not_have_popularity_doc(Config).

new_file_should_have_zero_popularity(Config) ->
    lfm_files_test_base:new_file_should_have_zero_popularity(Config).

opening_file_should_increase_file_popularity(Config) ->
    lfm_files_test_base:opening_file_should_increase_file_popularity(Config).

file_popularity_should_have_correct_file_size(Config) ->
    lfm_files_test_base:file_popularity_should_have_correct_file_size(Config).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{storage_type, s3}, {?LOAD_MODULES, [initializer, pool_utils]} | Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(Case, Config) when
    Case =:= lfm_open_in_direct_mode_test orelse
    Case =:= lfm_recreate_handle_test orelse
    Case =:= lfm_write_after_create_no_perms_test orelse
    Case =:= lfm_recreate_handle_after_delete_test
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, user_ctx, [passthrough]),
    test_utils:mock_expect(Workers, user_ctx, is_direct_io,
        fun(_, _) ->
            true
        end),
    init_per_testcase(default, Config);


init_per_testcase(Case, Config) when
    Case =:= lfm_open_failure_test orelse
    Case =:= lfm_create_and_open_failure_test orelse
    Case =:= lfm_copy_failure_test orelse
    Case =:= lfm_open_multiple_times_failure_test orelse
    Case =:= lfm_open_failure_multiple_users_test orelse
    Case =:= lfm_open_and_create_open_failure_test orelse
    Case =:= lfm_copy_failure_multiple_users_test
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, storage_driver, [passthrough]),
    init_per_testcase(default, Config);

init_per_testcase(ShareTest, Config) when
    ShareTest =:= create_share_dir_test orelse
    ShareTest =:= create_share_file_test orelse
    ShareTest =:= remove_share_test orelse
    ShareTest =:= share_getattr_test orelse
    ShareTest =:= share_get_parent_test orelse
    ShareTest =:= share_list_test orelse
    ShareTest =:= share_read_test orelse
    ShareTest =:= share_child_getattr_test orelse
    ShareTest =:= share_child_list_test orelse
    ShareTest =:= share_child_read_test orelse
    ShareTest =:= share_permission_denied_test
->
    initializer:mock_share_logic(Config),
    init_per_testcase(default, Config);

init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(Case, Config) when
    Case =:= lfm_open_in_direct_mode_test orelse
    Case =:= lfm_recreate_handle_test orelse
    Case =:= lfm_write_after_create_no_perms_test orelse
    Case =:= lfm_recreate_handle_after_delete_test
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [user_ctx]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case, Config) when
    Case =:= lfm_open_failure_test orelse
    Case =:= lfm_create_and_open_failure_test orelse
    Case =:= lfm_copy_failure_test orelse
    Case =:= lfm_open_multiple_times_failure_test orelse
    Case =:= lfm_open_failure_multiple_users_test orelse
    Case =:= lfm_open_and_create_open_failure_test orelse
    Case =:= lfm_copy_failure_multiple_users_test
->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [storage_driver]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(ShareTest, Config) when
    ShareTest =:= create_share_dir_test orelse
    ShareTest =:= create_share_file_test orelse
    ShareTest =:= remove_share_test orelse
    ShareTest =:= share_getattr_test orelse
    ShareTest =:= share_get_parent_test orelse
    ShareTest =:= share_list_test orelse
    ShareTest =:= share_read_test orelse
    ShareTest =:= share_child_getattr_test orelse
    ShareTest =:= share_child_list_test orelse
    ShareTest =:= share_child_read_test orelse
    ShareTest =:= share_permission_denied_test
->
    initializer:unmock_share_logic(Config),

    end_per_testcase(?DEFAULT_CASE(ShareTest), Config);

end_per_testcase(Case, Config) when
    Case =:= opening_file_should_increase_file_popularity;
    Case =:= file_popularity_should_have_correct_file_size
    ->
    [W | _] = ?config(op_worker_nodes, Config),
    rpc:call(W, file_popularity_api, disable, [?SPACE_ID]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).
