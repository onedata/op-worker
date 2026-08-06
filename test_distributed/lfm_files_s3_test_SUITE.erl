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
    lfm_acl_test/1,
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
    new_file_should_not_have_popularity_doc/1,
    new_file_should_have_zero_popularity/1,
    opening_file_should_increase_file_popularity/1,
    file_popularity_should_have_correct_file_size/1,
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
    lfm_mv_failure_multiple_users_test/1
]).

-define(TEST_CASES, [
    fslogic_new_file_test,
    lfm_acl_test,
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
    new_file_should_not_have_popularity_doc,
    new_file_should_have_zero_popularity,
    opening_file_should_increase_file_popularity,
    file_popularity_should_have_correct_file_size,
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
    lfm_mv_failure_multiple_users_test
]).

-define(SPACE_ID, <<"space1">>).

all() ->
    ?ALL(?TEST_CASES).


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

fslogic_new_file_test(Config) ->
    lfm_files_test_base:fslogic_new_file(Config).

lfm_acl_test(Config) ->
    lfm_files_test_base:lfm_acl(Config).

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
    lfm_files_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    lfm_files_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    lfm_files_test_base:init_per_testcase(Case, Config).

end_per_testcase(Case, Config) ->
    lfm_files_test_base:end_per_testcase(Case, Config).
