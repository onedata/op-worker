%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage import
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_delete_and_links_test_SUITE).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/performance.hrl").

% TODO VFS-6161 divide to smaller test suites

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    delete_empty_directory_update_test/1,
    delete_non_empty_directory_update_test/1,
    sync_works_properly_after_delete_test/1,
    delete_and_update_files_simultaneously_update_test/1,
    delete_file_update_test/1,
    delete_file_in_dir_update_test/1,
    delete_many_subfiles_test/1,
    create_delete_race_test/1,
    create_list_race_test/1,
    properly_handle_hardlink_when_file_and_hardlink_are_not_deleted/1,
    properly_handle_hardlink_when_file_is_deleted_when_opened_and_hardlink_is_not_deleted/1,
    properly_handle_hardlink_when_file_is_deleted_and_hardlink_is_not_deleted/1,
    properly_handle_hardlink_when_file_is_not_deleted_and_hardlink_is_deleted_when_opened/1,
    properly_handle_hardlink_when_file_and_hardlink_are_deleted_when_opened/1,
    properly_handle_hardlink_when_file_is_deleted_and_hardlink_is_deleted_when_opened/1,
    properly_handle_hardlink_when_file_is_not_deleted_and_hardlink_is_deleted/1,
    properly_handle_hardlink_when_file_is_deleted_when_opened_and_hardlink_is_deleted/1,
    properly_handle_hardlink_when_file_and_hardlink_are_deleted/1,
    symlink_is_ignored_by_initial_scan/1,
    symlink_is_ignored_by_continuous_scan/1
]).

-define(TEST_CASES, [
    delete_empty_directory_update_test,
    delete_non_empty_directory_update_test,
    sync_works_properly_after_delete_test,
    delete_and_update_files_simultaneously_update_test,
    delete_file_update_test,
    delete_file_in_dir_update_test,
    delete_many_subfiles_test,
    create_delete_race_test,
    create_list_race_test,
    properly_handle_hardlink_when_file_and_hardlink_are_not_deleted,
    properly_handle_hardlink_when_file_is_deleted_when_opened_and_hardlink_is_not_deleted,
    properly_handle_hardlink_when_file_is_deleted_and_hardlink_is_not_deleted,
    properly_handle_hardlink_when_file_is_not_deleted_and_hardlink_is_deleted_when_opened,
    properly_handle_hardlink_when_file_and_hardlink_are_deleted_when_opened,
    properly_handle_hardlink_when_file_is_deleted_and_hardlink_is_deleted_when_opened,
    properly_handle_hardlink_when_file_is_not_deleted_and_hardlink_is_deleted,
    properly_handle_hardlink_when_file_is_deleted_when_opened_and_hardlink_is_deleted,
    properly_handle_hardlink_when_file_and_hardlink_are_deleted,
    symlink_is_ignored_by_initial_scan,
    symlink_is_ignored_by_continuous_scan
]).

all() -> ?ALL(?TEST_CASES).

%%%==================================================================
%%% Test functions
%%%===================================================================

delete_empty_directory_update_test(Config) ->
    storage_import_test_base:delete_empty_directory_update_test(Config).

delete_non_empty_directory_update_test(Config) ->
    storage_import_test_base:delete_non_empty_directory_update_test(Config).

sync_works_properly_after_delete_test(Config) ->
    storage_import_test_base:sync_works_properly_after_delete_test(Config).

delete_and_update_files_simultaneously_update_test(Config) ->
    storage_import_test_base:delete_and_update_files_simultaneously_update_test(Config).

delete_file_update_test(Config) ->
    storage_import_test_base:delete_file_update_test(Config).

delete_file_in_dir_update_test(Config) ->
    storage_import_test_base:delete_file_in_dir_update_test(Config).

delete_many_subfiles_test(Config) ->
    storage_import_test_base:delete_many_subfiles_test(Config).

create_delete_race_test(Config) ->
    storage_import_test_base:create_delete_race_test(Config, ?POSIX_HELPER_NAME).

create_list_race_test(Config) ->
    storage_import_test_base:create_list_race_test(Config).

properly_handle_hardlink_when_file_and_hardlink_are_not_deleted(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_and_hardlink_are_not_deleted(Config, ?POSIX_HELPER_NAME).

properly_handle_hardlink_when_file_is_deleted_when_opened_and_hardlink_is_not_deleted(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_is_deleted_when_opened_and_hardlink_is_not_deleted(Config, ?POSIX_HELPER_NAME).

properly_handle_hardlink_when_file_is_deleted_and_hardlink_is_not_deleted(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_is_deleted_and_hardlink_is_not_deleted(Config, ?POSIX_HELPER_NAME).

properly_handle_hardlink_when_file_is_not_deleted_and_hardlink_is_deleted_when_opened(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_is_not_deleted_and_hardlink_is_deleted_when_opened(Config, ?POSIX_HELPER_NAME).

properly_handle_hardlink_when_file_and_hardlink_are_deleted_when_opened(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_and_hardlink_are_deleted_when_opened(Config, ?POSIX_HELPER_NAME).

properly_handle_hardlink_when_file_is_deleted_and_hardlink_is_deleted_when_opened(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_is_deleted_and_hardlink_is_deleted_when_opened(Config, ?POSIX_HELPER_NAME).

properly_handle_hardlink_when_file_is_not_deleted_and_hardlink_is_deleted(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_is_not_deleted_and_hardlink_is_deleted(Config, ?POSIX_HELPER_NAME).

properly_handle_hardlink_when_file_is_deleted_when_opened_and_hardlink_is_deleted(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_is_deleted_when_opened_and_hardlink_is_deleted(Config, ?POSIX_HELPER_NAME).

properly_handle_hardlink_when_file_and_hardlink_are_deleted(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_and_hardlink_are_deleted(Config, ?POSIX_HELPER_NAME).

symlink_is_ignored_by_initial_scan(Config) ->
    storage_import_test_base:symlink_is_ignored_by_initial_scan(Config).

symlink_is_ignored_by_continuous_scan(Config) ->
    storage_import_test_base:symlink_is_ignored_by_continuous_scan(Config, ?POSIX_HELPER_NAME).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    storage_import_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    storage_import_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    storage_import_test_base:init_per_testcase(Case, Config).

end_per_testcase(_Case, Config) ->
    storage_import_test_base:end_per_testcase(_Case, Config).