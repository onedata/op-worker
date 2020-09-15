%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage import
%%% @end
%%%--------------------------------------------------------------------
-module(storage_sync_test_SUITE).
-author("Rafal Slota").
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/performance.hrl").

% TODO VFS-6161 divide to smaller test suites, maybe get rid of readonly test SUITES as all imported storages are mounted in root

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    % tests of import
    empty_import_test/1,
    create_directory_import_test/1,
    create_directory_import_error_test/1,
    create_directory_import_check_user_id_test/1,
    create_directory_import_check_user_id_error_test/1,
    create_directory_import_without_read_permission_test/1,
    create_directory_import_many_test/1,
    create_empty_file_import_test/1,
    create_file_import_test/1,
    create_delete_import_test_read_both/1,
    create_delete_import_test_read_remote_only/1,
    create_file_import_check_user_id_test/1,
    create_file_import_check_user_id_error_test/1,
    create_file_in_dir_import_test/1,
    create_subfiles_import_many_test/1,
    create_subfiles_import_many2_test/1,
    create_remote_file_import_conflict_test/1,
    create_remote_dir_import_race_test/1,
    create_remote_file_import_race_test/1,
    import_nfs_acl_test/1,
    import_nfs_acl_with_disabled_luma_should_fail_test/1,
    create_file_import_race_test/1,
    close_file_import_race_test/1,
    delete_file_reimport_race_test/1,
    delete_opened_file_reimport_race_test/1,

    % tests of update
    update_syncs_files_after_import_failed_test/1,
    update_syncs_files_after_previous_update_failed_test/1,
    sync_should_not_reimport_deleted_but_still_opened_file/1,
    sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage/1,
    sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage/1,
    sync_should_not_import_recreated_file_with_suffix_on_storage/1,
    sync_should_update_blocks_of_recreated_file_with_suffix_on_storage/1,
    sync_should_not_import_replicated_file_with_suffix_on_storage/1,
    sync_should_update_replicated_file_with_suffix_on_storage/1,
    sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed/1,
    create_delete_import2_test/1,
    create_subfiles_and_delete_before_import_is_finished_test/1,
    create_file_in_dir_update_test/1,
    changing_max_depth_test/1,
    create_file_in_dir_exceed_batch_update_test/1,
    force_start_test/1,
    force_stop_test/1,

    delete_empty_directory_update_test/1,
    delete_non_empty_directory_update_test/1,
    sync_works_properly_after_delete_test/1,
    delete_and_update_files_simultaneously_update_test/1,
    delete_file_update_test/1,
    delete_file_in_dir_update_test/1,
    delete_many_subfiles_test/1,
    create_delete_race_test/1,
    create_list_race_test/1,

    append_file_update_test/1,
    append_file_not_changing_mtime_update_test/1,
    append_empty_file_update_test/1,
    copy_file_update_test/1,
    move_file_update_test/1,
    truncate_file_update_test/1,
    change_file_content_constant_size_test/1,
    change_file_content_update_test/1,
    change_file_content_the_same_moment_when_sync_performs_stat_on_file_test/1,
    chmod_file_update_test/1,
    chmod_file_update2_test/1,
    change_file_type_test/1,
    change_file_type2_test/1,
    change_file_type3_test/1,
    change_file_type4_test/1,
    update_timestamps_file_import_test/1,
    should_not_detect_timestamp_update_test/1,
    update_nfs_acl_test/1,
    recreate_file_deleted_by_sync_test/1,
    sync_should_not_delete_not_replicated_file_created_in_remote_provider/1,
    sync_should_not_delete_dir_created_in_remote_provider/1,
    sync_should_not_delete_not_replicated_files_created_in_remote_provider2/1,
    should_not_sync_file_during_replication/1,
    sync_should_not_invalidate_file_after_replication/1
]).

-define(TEST_CASES, [
    % tests of import
    empty_import_test,
    create_directory_import_test,
    create_directory_import_error_test,
    create_directory_import_check_user_id_test,
    create_directory_import_check_user_id_error_test,
    create_directory_import_without_read_permission_test,
    create_directory_import_many_test,
    create_empty_file_import_test,
    create_file_import_test,
    create_delete_import_test_read_both,
    create_delete_import_test_read_remote_only,
    create_file_import_check_user_id_test,
    create_file_import_check_user_id_error_test,
    create_file_in_dir_import_test,
    create_subfiles_import_many_test,
    create_subfiles_import_many2_test,
    create_remote_file_import_conflict_test,
    create_remote_dir_import_race_test,
    create_remote_file_import_race_test,
    import_nfs_acl_test,
    import_nfs_acl_with_disabled_luma_should_fail_test,
    create_file_import_race_test,
    close_file_import_race_test,
    delete_file_reimport_race_test,
    delete_opened_file_reimport_race_test,

    % tests of update
    update_syncs_files_after_import_failed_test,
    update_syncs_files_after_previous_update_failed_test,
    % todo VFS-5203 add more tests of sync handling suffixed files
    sync_should_not_reimport_deleted_but_still_opened_file,
    sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage,
    sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage,
    sync_should_not_import_recreated_file_with_suffix_on_storage,
    sync_should_update_blocks_of_recreated_file_with_suffix_on_storage,
    sync_should_not_import_replicated_file_with_suffix_on_storage,
    sync_should_update_replicated_file_with_suffix_on_storage,
    sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed,
    create_delete_import2_test,
    create_subfiles_and_delete_before_import_is_finished_test,
    create_file_in_dir_update_test,
    changing_max_depth_test,
    create_file_in_dir_exceed_batch_update_test,
    force_start_test,
    force_stop_test,

    delete_empty_directory_update_test,
    delete_non_empty_directory_update_test,
    sync_works_properly_after_delete_test,
    delete_and_update_files_simultaneously_update_test,
    delete_file_update_test,
    delete_file_in_dir_update_test,
    delete_many_subfiles_test,
    create_delete_race_test,
    create_list_race_test,

    append_file_update_test,
    append_file_not_changing_mtime_update_test,
    append_empty_file_update_test,
    copy_file_update_test,
    move_file_update_test,
    truncate_file_update_test,
    change_file_content_constant_size_test,
    change_file_content_update_test,
    change_file_content_the_same_moment_when_sync_performs_stat_on_file_test,
    chmod_file_update_test,
    chmod_file_update2_test,
    change_file_type_test,
    change_file_type2_test,
    change_file_type3_test,
    change_file_type4_test,
    update_timestamps_file_import_test,
    should_not_detect_timestamp_update_test,
    update_nfs_acl_test,
    recreate_file_deleted_by_sync_test,
    sync_should_not_delete_not_replicated_file_created_in_remote_provider,
    sync_should_not_delete_dir_created_in_remote_provider,
    sync_should_not_delete_not_replicated_files_created_in_remote_provider2,
    should_not_sync_file_during_replication,
    sync_should_not_invalidate_file_after_replication
]).

all() -> ?ALL(?TEST_CASES).

%%%==================================================================
%%% Test functions
%%%===================================================================

empty_import_test(Config) ->
    storage_import_test_base:empty_import_test(Config).

create_directory_import_test(Config) ->
    storage_import_test_base:create_directory_import_test(Config).

create_directory_import_error_test(Config) ->
    storage_import_test_base:create_directory_import_error_test(Config).

create_directory_import_check_user_id_test(Config) ->
    storage_import_test_base:create_directory_import_check_user_id_test(Config).

create_directory_import_check_user_id_error_test(Config) ->
    storage_import_test_base:create_directory_import_check_user_id_error_test(Config).

create_directory_import_without_read_permission_test(Config) ->
    storage_import_test_base:create_directory_import_without_read_permission_test(Config).

create_directory_import_many_test(Config) ->
    storage_import_test_base:create_directory_import_many_test(Config).

create_empty_file_import_test(Config) ->
    storage_import_test_base:create_empty_file_import_test(Config).

create_file_import_test(Config) ->
    storage_import_test_base:create_file_import_test(Config).

create_delete_import_test_read_both(Config) ->
    storage_import_test_base:create_delete_import_test_read_both(Config).

create_delete_import_test_read_remote_only(Config) ->
    storage_import_test_base:create_delete_import_test_read_remote_only(Config).

create_file_import_check_user_id_test(Config) ->
    storage_import_test_base:create_file_import_check_user_id_test(Config).

create_file_import_check_user_id_error_test(Config) ->
    storage_import_test_base:create_file_import_check_user_id_error_test(Config).

create_file_in_dir_import_test(Config) ->
    storage_import_test_base:create_file_in_dir_import_test(Config).

create_subfiles_import_many_test(Config) ->
    storage_import_test_base:create_subfiles_import_many_test(Config).

create_subfiles_import_many2_test(Config) ->
    storage_import_test_base:create_subfiles_import_many2_test(Config).

create_remote_file_import_conflict_test(Config) ->
    storage_import_test_base:create_remote_file_import_conflict_test(Config).

create_remote_dir_import_race_test(Config) ->
    storage_import_test_base:create_remote_dir_import_race_test(Config).

create_remote_file_import_race_test(Config) ->
    storage_import_test_base:create_remote_file_import_race_test(Config).

force_stop_test(Config) ->
    storage_import_test_base:force_stop_test(Config).

import_nfs_acl_test(Config) ->
    storage_import_test_base:import_nfs_acl_test(Config).

import_nfs_acl_with_disabled_luma_should_fail_test(Config) ->
    storage_import_test_base:import_nfs_acl_with_disabled_luma_should_fail_test(Config).

create_file_import_race_test(Config) ->
    storage_import_test_base:create_file_import_race_test(Config).

close_file_import_race_test(Config) ->
    storage_import_test_base:close_file_import_race_test(Config, ?POSIX_HELPER_NAME).

delete_file_reimport_race_test(Config) ->
    storage_import_test_base:delete_file_reimport_race_test(Config, ?POSIX_HELPER_NAME).

delete_opened_file_reimport_race_test(Config) ->
    storage_import_test_base:delete_opened_file_reimport_race_test(Config, ?POSIX_HELPER_NAME).

update_syncs_files_after_import_failed_test(Config) ->
    storage_import_test_base:update_syncs_files_after_import_failed_test(Config).

update_syncs_files_after_previous_update_failed_test(Config) ->
    storage_import_test_base:update_syncs_files_after_previous_update_failed_test(Config).

sync_should_not_reimport_deleted_but_still_opened_file(Config) ->
    storage_import_test_base:sync_should_not_reimport_deleted_but_still_opened_file(Config, ?POSIX_HELPER_NAME).

sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage(Config) ->
    storage_import_test_base:sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage(Config).

sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage(Config) ->
    storage_import_test_base:sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage(Config, ?POSIX_HELPER_NAME).

sync_should_not_import_recreated_file_with_suffix_on_storage(Config) ->
    storage_import_test_base:sync_should_not_import_recreated_file_with_suffix_on_storage(Config, ?POSIX_HELPER_NAME).

sync_should_update_blocks_of_recreated_file_with_suffix_on_storage(Config) ->
    storage_import_test_base:sync_should_update_blocks_of_recreated_file_with_suffix_on_storage(Config, ?POSIX_HELPER_NAME).

sync_should_not_import_replicated_file_with_suffix_on_storage(Config) ->
    storage_import_test_base:sync_should_not_import_replicated_file_with_suffix_on_storage(Config, ?POSIX_HELPER_NAME).

sync_should_update_replicated_file_with_suffix_on_storage(Config) ->
    storage_import_test_base:sync_should_update_replicated_file_with_suffix_on_storage(Config, ?POSIX_HELPER_NAME).

sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed(Config) ->
    storage_import_test_base:sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed(Config).

create_delete_import2_test(Config) ->
    storage_import_test_base:create_delete_import2_test(Config, true).

create_subfiles_and_delete_before_import_is_finished_test(Config) ->
    storage_import_test_base:create_subfiles_and_delete_before_import_is_finished_test(Config).

create_file_in_dir_update_test(Config) ->
    storage_import_test_base:create_file_in_dir_update_test(Config).

changing_max_depth_test(Config) ->
    storage_import_test_base:changing_max_depth_test(Config).

create_file_in_dir_exceed_batch_update_test(Config) ->
    storage_import_test_base:create_file_in_dir_exceed_batch_update_test(Config).

force_start_test(Config) ->
    storage_import_test_base:force_start_test(Config).

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

append_file_update_test(Config) ->
    storage_import_test_base:append_file_update_test(Config).

append_file_not_changing_mtime_update_test(Config) ->
    storage_import_test_base:append_file_not_changing_mtime_update_test(Config).

append_empty_file_update_test(Config) ->
    storage_import_test_base:append_empty_file_update_test(Config).

copy_file_update_test(Config) ->
    storage_import_test_base:copy_file_update_test(Config).

move_file_update_test(Config) ->
    storage_import_test_base:move_file_update_test(Config).

truncate_file_update_test(Config) ->
    storage_import_test_base:truncate_file_update_test(Config).

change_file_content_constant_size_test(Config) ->
    storage_import_test_base:change_file_content_constant_size_test(Config).

change_file_content_update_test(Config) ->
    storage_import_test_base:change_file_content_update_test(Config).

change_file_content_the_same_moment_when_sync_performs_stat_on_file_test(Config) ->
    storage_import_test_base:change_file_content_the_same_moment_when_sync_performs_stat_on_file_test(Config).

chmod_file_update_test(Config) ->
    storage_import_test_base:chmod_file_update_test(Config).

chmod_file_update2_test(Config) ->
    storage_import_test_base:chmod_file_update2_test(Config).

change_file_type_test(Config) ->
    storage_import_test_base:change_file_type_test(Config).

change_file_type2_test(Config) ->
    storage_import_test_base:change_file_type2_test(Config).

change_file_type3_test(Config) ->
    storage_import_test_base:change_file_type3_test(Config).

change_file_type4_test(Config) ->
    storage_import_test_base:change_file_type4_test(Config).

update_timestamps_file_import_test(Config) ->
    storage_import_test_base:update_timestamps_file_import_test(Config).

should_not_detect_timestamp_update_test(Config) ->
    storage_import_test_base:should_not_detect_timestamp_update_test(Config).

update_nfs_acl_test(Config) ->
    storage_import_test_base:update_nfs_acl_test(Config).

recreate_file_deleted_by_sync_test(Config) ->
    storage_import_test_base:recreate_file_deleted_by_sync_test(Config).

sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config) ->
    storage_import_test_base:sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config).

sync_should_not_delete_dir_created_in_remote_provider(Config) ->
    storage_import_test_base:sync_should_not_delete_dir_created_in_remote_provider(Config).

sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config) ->
    storage_import_test_base:sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config).

should_not_sync_file_during_replication(Config) ->
    storage_import_test_base:should_not_sync_file_during_replication(Config).

sync_should_not_invalidate_file_after_replication(Config) ->
    storage_import_test_base:sync_should_not_invalidate_file_after_replication(Config).

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