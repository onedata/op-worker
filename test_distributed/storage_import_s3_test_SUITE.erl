%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage import on s3 storage.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_s3_test_SUITE).
-author("Jakub Kudzia").

-include("storage_import_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("kernel/include/file.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    % tests of import
    create_empty_file_import_test/1,
    create_file_import_test/1,
    create_delete_import_test/1,
    create_file_in_dir_import_test/1,
    create_subfiles_import_many_test/1,
    create_subfiles_import_many2_test/1,
    create_remote_file_import_conflict_test/1,
    create_remote_file_import_race_test/1,
    create_file_import_race_test/1,
    close_file_import_race_test/1,
    delete_file_reimport_race_test/1,
    remote_delete_file_reimport_race_test/1,
    remote_delete_file_reimport_race2_test/1,
    delete_opened_file_reimport_race_test/1,

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
    symlink_is_ignored_by_continuous_scan/1,

    append_file_update_test/1,
    append_empty_file_update_test/1,
    truncate_file_update_test/1,
    change_file_content_constant_size_test/1,
    change_file_content_update_test/1,
    change_file_type_test/1,
    change_file_type3_test/1,
    change_file_type4_test/1,
    recreate_file_deleted_by_sync_test/1,
    sync_should_not_delete_not_replicated_file_created_in_remote_provider/1,
    sync_should_not_delete_dir_created_in_remote_provider/1,
    sync_should_not_delete_not_replicated_files_created_in_remote_provider2/1,
    should_not_sync_file_during_replication/1,
    sync_should_not_invalidate_file_after_replication/1,
    time_warp_between_scans_test/1,
    time_warp_during_scan_test/1
]).

-define(TEST_CASES, [
    % tests of import
    create_empty_file_import_test,
    create_file_import_test,
    create_delete_import_test,
    create_file_in_dir_import_test,
    create_subfiles_import_many_test,
    create_subfiles_import_many2_test,
    create_remote_file_import_conflict_test,
    create_remote_file_import_race_test,
    create_file_import_race_test,
    close_file_import_race_test,
    delete_file_reimport_race_test,
    remote_delete_file_reimport_race_test,
    remote_delete_file_reimport_race2_test,
    delete_opened_file_reimport_race_test,

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
    symlink_is_ignored_by_continuous_scan,

    append_file_update_test,
    append_empty_file_update_test,
    truncate_file_update_test,
    change_file_content_constant_size_test,
    change_file_content_update_test,
    change_file_type_test,
    change_file_type3_test,
    change_file_type4_test,
    recreate_file_deleted_by_sync_test,
    sync_should_not_delete_not_replicated_file_created_in_remote_provider,
    sync_should_not_delete_dir_created_in_remote_provider,
    sync_should_not_delete_not_replicated_files_created_in_remote_provider2,
    should_not_sync_file_during_replication,
    sync_should_not_invalidate_file_after_replication,
    time_warp_between_scans_test,
    time_warp_during_scan_test
]).

all() -> ?ALL(?TEST_CASES).

%%%==================================================================
%%% Test functions
%%%===================================================================

create_empty_file_import_test(Config) ->
    storage_import_test_base:create_empty_file_import_test(Config).

create_file_import_test(Config) ->
    storage_import_test_base:create_file_import_test(Config).

create_delete_import_test(Config) ->
    storage_import_test_base:create_delete_import_test(Config).

create_file_in_dir_import_test(Config) ->
    storage_import_s3_test_base:create_file_in_dir_import_test(Config).

create_subfiles_import_many_test(Config) ->
    storage_import_s3_test_base:create_subfiles_import_many_test(Config).

create_subfiles_import_many2_test(Config) ->
    storage_import_s3_test_base:create_subfiles_import_many2_test(Config).

create_remote_file_import_conflict_test(Config) ->
    storage_import_test_base:create_remote_file_import_conflict_test(Config).

create_remote_file_import_race_test(Config) ->
    storage_import_test_base:create_remote_file_import_race_test(Config).

create_file_import_race_test(Config) ->
    storage_import_test_base:create_file_import_race_test(Config).

close_file_import_race_test(Config) ->
    storage_import_test_base:close_file_import_race_test(Config, ?S3_HELPER_NAME).

delete_file_reimport_race_test(Config) ->
    storage_import_test_base:delete_file_reimport_race_test(Config, ?S3_HELPER_NAME).

remote_delete_file_reimport_race_test(Config) ->
    storage_import_test_base:remote_delete_file_reimport_race_test(Config, ?S3_HELPER_NAME).

remote_delete_file_reimport_race2_test(Config) ->
    storage_import_test_base:remote_delete_file_reimport_race2_test(Config, ?S3_HELPER_NAME).

delete_opened_file_reimport_race_test(Config) ->
    storage_import_test_base:delete_opened_file_reimport_race_test(Config, ?S3_HELPER_NAME).


delete_non_empty_directory_update_test(Config) ->
    storage_import_s3_test_base:delete_non_empty_directory_update_test(Config).

sync_works_properly_after_delete_test(Config) ->
    storage_import_s3_test_base:sync_works_properly_after_delete_test(Config).

delete_and_update_files_simultaneously_update_test(Config) ->
    storage_import_s3_test_base:delete_and_update_files_simultaneously_update_test(Config).

delete_file_update_test(Config) ->
    storage_import_test_base:delete_file_update_test(Config).

delete_file_in_dir_update_test(Config) ->
    storage_import_s3_test_base:delete_file_in_dir_update_test(Config).

delete_many_subfiles_test(Config) ->
    storage_import_s3_test_base:delete_many_subfiles_test(Config).

create_delete_race_test(Config) ->
    storage_import_test_base:create_delete_race_test(Config, ?S3_HELPER_NAME).

create_list_race_test(Config) ->
    storage_import_s3_test_base:create_list_race_test(Config).

properly_handle_hardlink_when_file_and_hardlink_are_not_deleted(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_and_hardlink_are_not_deleted(Config, ?S3_HELPER_NAME).

properly_handle_hardlink_when_file_is_deleted_when_opened_and_hardlink_is_not_deleted(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_is_deleted_when_opened_and_hardlink_is_not_deleted(Config, ?S3_HELPER_NAME).

properly_handle_hardlink_when_file_is_deleted_and_hardlink_is_not_deleted(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_is_deleted_and_hardlink_is_not_deleted(Config, ?S3_HELPER_NAME).

properly_handle_hardlink_when_file_is_not_deleted_and_hardlink_is_deleted_when_opened(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_is_not_deleted_and_hardlink_is_deleted_when_opened(Config, ?S3_HELPER_NAME).

properly_handle_hardlink_when_file_and_hardlink_are_deleted_when_opened(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_and_hardlink_are_deleted_when_opened(Config, ?S3_HELPER_NAME).

properly_handle_hardlink_when_file_is_deleted_and_hardlink_is_deleted_when_opened(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_is_deleted_and_hardlink_is_deleted_when_opened(Config, ?S3_HELPER_NAME).

properly_handle_hardlink_when_file_is_not_deleted_and_hardlink_is_deleted(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_is_not_deleted_and_hardlink_is_deleted(Config, ?S3_HELPER_NAME).

properly_handle_hardlink_when_file_is_deleted_when_opened_and_hardlink_is_deleted(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_is_deleted_when_opened_and_hardlink_is_deleted(Config, ?S3_HELPER_NAME).

properly_handle_hardlink_when_file_and_hardlink_are_deleted(Config) ->
    storage_import_test_base:properly_handle_hardlink_when_file_and_hardlink_are_deleted(Config, ?S3_HELPER_NAME).

symlink_is_ignored_by_initial_scan(Config) ->
    storage_import_test_base:symlink_is_ignored_by_initial_scan(Config).

symlink_is_ignored_by_continuous_scan(Config) ->
    storage_import_test_base:symlink_is_ignored_by_continuous_scan(Config, ?S3_HELPER_NAME).


append_file_update_test(Config) ->
    storage_import_test_base:append_file_update_test(Config).

append_empty_file_update_test(Config) ->
    storage_import_test_base:append_empty_file_update_test(Config).

truncate_file_update_test(Config) ->
    storage_import_test_base:truncate_file_update_test(Config).

change_file_content_constant_size_test(Config) ->
    storage_import_test_base:change_file_content_constant_size_test(Config).

change_file_content_update_test(Config) ->
    storage_import_test_base:change_file_content_update_test(Config).

change_file_type_test(Config) ->
    storage_import_s3_test_base:change_file_type_test(Config).

change_file_type3_test(Config) ->
    storage_import_s3_test_base:change_file_type3_test(Config).

change_file_type4_test(Config) ->
    storage_import_test_base:change_file_type4_test(Config).

recreate_file_deleted_by_sync_test(Config) ->
    storage_import_test_base:recreate_file_deleted_by_sync_test(Config).

sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config) ->
    storage_import_test_base:sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config).

sync_should_not_delete_dir_created_in_remote_provider(Config) ->
    storage_import_test_base:sync_should_not_delete_dir_created_in_remote_provider(Config).

sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config) ->
    storage_import_test_base:sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config).

should_not_sync_file_during_replication(Config) ->
    % file size limit that can be created via lfm on s3
    storage_import_test_base:should_not_sync_file_during_replication(Config).

sync_should_not_invalidate_file_after_replication(Config) ->
    storage_import_test_base:sync_should_not_invalidate_file_after_replication(Config).

time_warp_between_scans_test(Config) ->
    storage_import_test_base:time_warp_between_scans_test(Config).

time_warp_during_scan_test(Config) ->
    storage_import_test_base:time_warp_during_scan_test(Config).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    storage_import_s3_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    storage_import_s3_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    storage_import_s3_test_base:init_per_testcase(Case, Config).

end_per_testcase(Case, Config) ->
    storage_import_s3_test_base:end_per_testcase(Case, Config).