%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage_sync
%%% @end
%%%--------------------------------------------------------------------
-module(storage_sync_test_SUITE).
-author("Rafal Slota").
-author("Jakub Kudzia").

-include("storage_sync_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("kernel/include/file.hrl").

% TODO VFS-6161 divide to smaller test suites

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
    cancel_scan/1,
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
    create_file_in_dir_exceed_batch_update_test/1,

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
    cancel_scan,
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
    create_file_in_dir_exceed_batch_update_test,

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
    storage_sync_test_base:empty_import_test(Config).

create_directory_import_test(Config) ->
    storage_sync_test_base:create_directory_import_test(Config, false).

create_directory_import_error_test(Config) ->
    storage_sync_test_base:create_directory_import_error_test(Config, false).

create_directory_import_check_user_id_test(Config) ->
    storage_sync_test_base:create_directory_import_check_user_id_test(Config, false).

create_directory_import_check_user_id_error_test(Config) ->
    storage_sync_test_base:create_directory_import_check_user_id_error_test(Config, false).

create_directory_import_without_read_permission_test(Config) ->
    storage_sync_test_base:create_directory_import_without_read_permission_test(Config, false).

create_directory_import_many_test(Config) ->
    storage_sync_test_base:create_directory_import_many_test(Config, false).

create_empty_file_import_test(Config) ->
    storage_sync_test_base:create_empty_file_import_test(Config, false).

create_file_import_test(Config) ->
    storage_sync_test_base:create_file_import_test(Config, false).

create_delete_import_test_read_both(Config) ->
    storage_sync_test_base:create_delete_import_test_read_both(Config, false).

create_delete_import_test_read_remote_only(Config) ->
    storage_sync_test_base:create_delete_import_test_read_remote_only(Config, false).

create_file_import_check_user_id_test(Config) ->
    storage_sync_test_base:create_file_import_check_user_id_test(Config, false).

create_file_import_check_user_id_error_test(Config) ->
    storage_sync_test_base:create_file_import_check_user_id_error_test(Config, false).

create_file_in_dir_import_test(Config) ->
    storage_sync_test_base:create_file_in_dir_import_test(Config, false).

create_subfiles_import_many_test(Config) ->
    storage_sync_test_base:create_subfiles_import_many_test(Config, false).

create_subfiles_import_many2_test(Config) ->
    storage_sync_test_base:create_subfiles_import_many2_test(Config, false).

create_remote_file_import_conflict_test(Config) ->
    storage_sync_test_base:create_remote_file_import_conflict_test(Config, false).

create_remote_dir_import_race_test(Config) ->
    storage_sync_test_base:create_remote_dir_import_race_test(Config, false).

create_remote_file_import_race_test(Config) ->
    storage_sync_test_base:create_remote_file_import_race_test(Config, false).

cancel_scan(Config) ->
    storage_sync_test_base:cancel_scan(Config, false).

import_nfs_acl_test(Config) ->
    storage_sync_test_base:import_nfs_acl_test(Config, false).

import_nfs_acl_with_disabled_luma_should_fail_test(Config) ->
    storage_sync_test_base:import_nfs_acl_with_disabled_luma_should_fail_test(Config, false).

create_file_import_race_test(Config) ->
    storage_sync_test_base:create_file_import_race_test(Config).

close_file_import_race_test(Config) ->
    storage_sync_test_base:close_file_import_race_test(Config, ?POSIX_HELPER_NAME).

delete_file_reimport_race_test(Config) ->
    storage_sync_test_base:delete_file_reimport_race_test(Config, ?POSIX_HELPER_NAME).

delete_opened_file_reimport_race_test(Config) ->
    storage_sync_test_base:delete_opened_file_reimport_race_test(Config, ?POSIX_HELPER_NAME).

update_syncs_files_after_import_failed_test(Config) ->
    storage_sync_test_base:update_syncs_files_after_import_failed_test(Config, false).

update_syncs_files_after_previous_update_failed_test(Config) ->
    storage_sync_test_base:update_syncs_files_after_previous_update_failed_test(Config, false).

sync_should_not_reimport_deleted_but_still_opened_file(Config) ->
    storage_sync_test_base:sync_should_not_reimport_deleted_but_still_opened_file(Config, ?POSIX_HELPER_NAME).

sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage(Config) ->
    storage_sync_test_base:sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage(Config).

sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage(Config) ->
    storage_sync_test_base:sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage(Config, ?POSIX_HELPER_NAME).

sync_should_not_import_recreated_file_with_suffix_on_storage(Config) ->
    storage_sync_test_base:sync_should_not_import_recreated_file_with_suffix_on_storage(Config, ?POSIX_HELPER_NAME).

sync_should_update_blocks_of_recreated_file_with_suffix_on_storage(Config) ->
    storage_sync_test_base:sync_should_update_blocks_of_recreated_file_with_suffix_on_storage(Config, ?POSIX_HELPER_NAME).

sync_should_not_import_replicated_file_with_suffix_on_storage(Config) ->
    storage_sync_test_base:sync_should_not_import_replicated_file_with_suffix_on_storage(Config, ?POSIX_HELPER_NAME).

sync_should_update_replicated_file_with_suffix_on_storage(Config) ->
    storage_sync_test_base:sync_should_update_replicated_file_with_suffix_on_storage(Config, ?POSIX_HELPER_NAME).

sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed(Config) ->
    storage_sync_test_base:sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed(Config, false).

create_delete_import2_test(Config) ->
    storage_sync_test_base:create_delete_import2_test(Config, false, true).

create_subfiles_and_delete_before_import_is_finished_test(Config) ->
    storage_sync_test_base:create_subfiles_and_delete_before_import_is_finished_test(Config, false).

create_file_in_dir_update_test(Config) ->
    storage_sync_test_base:create_file_in_dir_update_test(Config, false).

create_file_in_dir_exceed_batch_update_test(Config) ->
    storage_sync_test_base:create_file_in_dir_exceed_batch_update_test(Config, false).

delete_empty_directory_update_test(Config) ->
    storage_sync_test_base:delete_empty_directory_update_test(Config, false).

delete_non_empty_directory_update_test(Config) ->
    storage_sync_test_base:delete_non_empty_directory_update_test(Config, false).

sync_works_properly_after_delete_test(Config) ->
    storage_sync_test_base:sync_works_properly_after_delete_test(Config, false).

delete_and_update_files_simultaneously_update_test(Config) ->
    storage_sync_test_base:delete_and_update_files_simultaneously_update_test(Config, false).

delete_file_update_test(Config) ->
    storage_sync_test_base:delete_file_update_test(Config, false).

delete_file_in_dir_update_test(Config) ->
    storage_sync_test_base:delete_file_in_dir_update_test(Config, false).

delete_many_subfiles_test(Config) ->
    storage_sync_test_base:delete_many_subfiles_test(Config, false).

create_delete_race_test(Config) ->
    storage_sync_test_base:create_delete_race_test(Config, false, ?POSIX_HELPER_NAME).

create_list_race_test(Config) ->
    storage_sync_test_base:create_list_race_test(Config, false).

append_file_update_test(Config) ->
    storage_sync_test_base:append_file_update_test(Config, false).

append_file_not_changing_mtime_update_test(Config) ->
    storage_sync_test_base:append_file_not_changing_mtime_update_test(Config, false).

append_empty_file_update_test(Config) ->
    storage_sync_test_base:append_empty_file_update_test(Config, false).

copy_file_update_test(Config) ->
    %% WARNING in this test we check whether copy of a file was detected
    %% and whether copied file's timestamps were updated
    %% for some reason when volume is mount as read_write to docker
    %% timestamp of a copied file is not updated
    %% because of that, this test was adapted to this bug

    MountSpaceInRoot = false,
    [W1 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    SrcStorageFilePath =
        storage_sync_test_base:storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    DestStorageFilePath2 =
        storage_sync_test_base:storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE2, MountSpaceInRoot),
    %% Create file on storage
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"imported">> => 1,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 1,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    timer:sleep(timer:seconds(2)), %ensure that copy time is different from read time
    %% Copy file
    {ok, _} = file:copy(SrcStorageFilePath, DestStorageFilePath2),

    storage_sync_test_base:enable_update(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"imported">> => 1,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 2,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if copied file was imported
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH2}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle2),
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle3, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle3).

move_file_update_test(Config) ->
    storage_sync_test_base:move_file_update_test(Config, false).

truncate_file_update_test(Config) ->
    storage_sync_test_base:truncate_file_update_test(Config, false).

change_file_content_constant_size_test(Config) ->
    storage_sync_test_base:change_file_content_constant_size_test(Config, false).

change_file_content_update_test(Config) ->
    storage_sync_test_base:change_file_content_update_test(Config, false).

change_file_content_the_same_moment_when_sync_performs_stat_on_file_test(Config) ->
    storage_sync_test_base:change_file_content_the_same_moment_when_sync_performs_stat_on_file_test(Config, false).

chmod_file_update_test(Config) ->
    storage_sync_test_base:chmod_file_update_test(Config, false).

chmod_file_update2_test(Config) ->
    storage_sync_test_base:chmod_file_update2_test(Config, false).

change_file_type_test(Config) ->
    storage_sync_test_base:change_file_type_test(Config, false).

change_file_type2_test(Config) ->
    storage_sync_test_base:change_file_type2_test(Config, false).

change_file_type3_test(Config) ->
    storage_sync_test_base:change_file_type3_test(Config, false).

change_file_type4_test(Config) ->
    storage_sync_test_base:change_file_type4_test(Config, false).

update_timestamps_file_import_test(Config) ->
    storage_sync_test_base:update_timestamps_file_import_test(Config, false).

should_not_detect_timestamp_update_test(Config) ->
    storage_sync_test_base:should_not_detect_timestamp_update_test(Config, false).

update_nfs_acl_test(Config) ->
    storage_sync_test_base:update_nfs_acl_test(Config, false).

recreate_file_deleted_by_sync_test(Config) ->
    storage_sync_test_base:recreate_file_deleted_by_sync_test(Config, false).

sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config) ->
    storage_sync_test_base:sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config, false).

sync_should_not_delete_dir_created_in_remote_provider(Config) ->
    storage_sync_test_base:sync_should_not_delete_dir_created_in_remote_provider(Config, false).

sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config) ->
    storage_sync_test_base:sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config, false).

should_not_sync_file_during_replication(Config) ->
    storage_sync_test_base:should_not_sync_file_during_replication(Config, 100 * 1024 * 1024).

sync_should_not_invalidate_file_after_replication(Config) ->
    storage_sync_test_base:sync_should_not_invalidate_file_after_replication(Config).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    storage_sync_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    storage_sync_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    storage_sync_test_base:init_per_testcase(Case, Config, false).

end_per_testcase(_Case, Config) ->
    storage_sync_test_base:end_per_testcase(_Case, Config, false).