%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% This module contains base test functions for testing storage sync.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_sync_test_base).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("kernel/include/file.hrl").
-include("storage_sync_test.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

% TODO VFS-6161 divide to smaller test suites, maybe get rid of readonly test SUITES as all imported storages are mounted in root
% TODO VFS-6162 move utility functions to storage_sync_test_utils module

% CT functions
-export([init_per_suite/1, init_per_testcase/2, end_per_suite/1, end_per_testcase/2]).

%% util functions
-export([disable_storage_sync/1,
    add_synced_storages/1, clean_synced_storage/1, storage_path/4, create_init_file/2,
    enable_import/3, enable_update/3, enable_update/4, clean_luma_db/1,
    clean_space/1, verify_file_deleted/4, cleanup_storage_sync_monitoring_model/2,
    assertImportTimes/2, assertImportTimes/3, assertUpdateTimes/2, assertUpdateTimes/3, assertUpdateTimes/4,
    disable_update/1, disable_import/1, assertNoImportInProgress/3,
    assertNoUpdateInProgress/3,
    change_time/3, assert_num_results_gte/3, assert_num_results/3,
    to_storage_files/3, parallel_assert/5, uuid/2, space_uuid/2, change_time/2,
    to_storage_file_id/2, assert_monitoring_state/4, verify_file_deleted/5,
    storage_path/3, get_rdwr_storage/2, get_supporting_storage/2, get_host_mount_point/2,
    create_nested_directory_tree/3, generate_nested_directory_tree_file_paths/2, add_rdwr_storages/1,
    get_synced_storage/2, get_host_storage_file_id/4, get_mount_point/1, clean_traverse_tasks/1,
    mock_import_file_error/2, unmock_import_file_error/1, verify_file/5,
    verify_file_in_dir/5, verify_dir/5, schedule_spaces_check/2, cancel/3, remove_link/3, provider_id/1, block_syncing_process/1,
    await_syncing_process/0, release_syncing_process/1, touch/2]).

%% tests
-export([
    % tests of import
    empty_import_test/1,
    create_directory_import_test/2,
    create_directory_import_error_test/2,
    create_directory_import_check_user_id_test/2,
    create_directory_import_check_user_id_error_test/2,
    create_directory_import_without_read_permission_test/2,
    create_directory_import_many_test/2,
    create_empty_file_import_test/2,
    create_file_import_test/2,
    create_delete_import_test_read_both/2,
    create_delete_import_test_read_remote_only/2,
    create_file_import_check_user_id_test/2,
    create_file_import_check_user_id_error_test/2,
    create_file_in_dir_import_test/2,
    create_subfiles_import_many_test/2,
    create_subfiles_import_many2_test/2,
    create_remote_file_import_conflict_test/2,
    create_remote_dir_import_race_test/2,
    create_remote_file_import_race_test/2,
    cancel_scan/2,
    import_nfs_acl_test/2,
    import_nfs_acl_with_disabled_luma_should_fail_test/2,
    create_file_import_race_test/1,
    close_file_import_race_test/2,
    delete_file_reimport_race_test/2,
    delete_opened_file_reimport_race_test/2,

    % tests of update
    update_syncs_files_after_import_failed_test/2,
    update_syncs_files_after_previous_update_failed_test/2,
    sync_should_not_reimport_deleted_but_still_opened_file/2,
    sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage/1,
    sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage/2,
    sync_should_not_import_recreated_file_with_suffix_on_storage/2,
    sync_should_update_replicated_file_with_suffix_on_storage/2,
    sync_should_update_blocks_of_recreated_file_with_suffix_on_storage/2,
    sync_should_not_import_replicated_file_with_suffix_on_storage/2,
    sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed/2,
    create_delete_import2_test/3,
    create_subfiles_and_delete_before_import_is_finished_test/2,
    create_file_in_dir_update_test/2,
    changing_max_depth_test/1,
    create_file_in_dir_exceed_batch_update_test/2,

    delete_empty_directory_update_test/2,
    delete_non_empty_directory_update_test/2,
    sync_works_properly_after_delete_test/2,
    delete_and_update_files_simultaneously_update_test/2,
    delete_file_update_test/2,
    delete_file_in_dir_update_test/2,
    delete_many_subfiles_test/2,
    create_delete_race_test/3,
    create_list_race_test/2,

    append_file_update_test/2,
    append_file_not_changing_mtime_update_test/2,
    append_empty_file_update_test/2,
    copy_file_update_test/2,
    move_file_update_test/2,
    truncate_file_update_test/2,
    change_file_content_constant_size_test/2,
    change_file_content_update_test/2,
    change_file_content_the_same_moment_when_sync_performs_stat_on_file_test/2,
    chmod_file_update_test/2,
    chmod_file_update2_test/2,
    change_file_type_test/2,
    change_file_type2_test/2,
    change_file_type3_test/2,
    change_file_type4_test/2,
    update_timestamps_file_import_test/2,
    should_not_detect_timestamp_update_test/2,
    update_nfs_acl_test/2,
    recreate_file_deleted_by_sync_test/2,
    sync_should_not_delete_not_replicated_file_created_in_remote_provider/2,
    sync_should_not_delete_dir_created_in_remote_provider/2,
    sync_should_not_delete_not_replicated_files_created_in_remote_provider2/2,
    should_not_sync_file_during_replication/1,
    sync_should_not_invalidate_file_after_replication/1
]).

-define(assertBlocks(Worker, SessionId, ExpectedDistribution, FileGuid),
    ?assertEqual(lists:sort(ExpectedDistribution), begin
        case lfm_proxy:get_file_distribution(Worker, SessionId, {guid, FileGuid}) of
            {ok, __FileBlocks} -> lists:sort(__FileBlocks);
            Error -> Error
        end
    end, ?ATTEMPTS)
).

-define(EXEC_ON_POSIX_ONLY(Function, StorageType),
    case StorageType of
        ?POSIX_HELPER_NAME -> Function();
        ?S3_HELPER_NAME -> ok
    end).

%%%===================================================================
%%% Tests of import
%%%===================================================================

empty_import_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),

    % wait till scan is finished
    assertImportTimes(W1, ?SPACE_ID),
    ?assertMatch({ok, []},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10)),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_PATH}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 1,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 0,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_directory_import_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#775),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),

    % wait till scan is finished
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if dir was imported
    ?assertMatch({ok, [{_, ?TEST_DIR}]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10)),

    StorageSDHandleW1 = sd_test_utils:get_storage_mountpoint_handle(W1, ?SPACE_ID, RDWRStorage),
    StorageW2 = get_supporting_storage(W2, ?SPACE_ID),
    StorageSDHandleW2 = sd_test_utils:get_storage_mountpoint_handle(W1, ?SPACE_ID, StorageW2),
    {ok, #statbuf{st_uid = MountUid1}} = sd_test_utils:stat(W1, StorageSDHandleW1),
    {ok, #statbuf{st_uid = MountUid2, st_gid = MountGid2}} = sd_test_utils:stat(W2, StorageSDHandleW2),

    SpaceOwner = ?SPACE_OWNER_ID(?SPACE_ID),
    ?assertMatch({ok, #file_attr{
        owner_id = SpaceOwner,
        uid = MountUid1,
        gid = 0
    }}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

    ?assertMatch({ok, #file_attr{
        owner_id = SpaceOwner,
        uid = MountUid2,
        gid = MountGid2
    }}, lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

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
    }, ?SPACE_ID).

create_directory_import_error_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    ?assertNotMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH})),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#775),

    mock_import_file_error(W1, ?TEST_DIR),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),

    % wait till scan is finished
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if dir was not imported
    ?assertMatch({ok, []}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 1)),
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH})),
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH})),
    %% Check if dir is still on storage
    ?assertMatch({ok, []}, sd_test_utils:ls(W1, SDHandle, 0, 1)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 1,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_directory_import_check_user_id_test(Config, MountSpaceInRoot) ->
    [W1, W2| _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#775),
    ok = sd_test_utils:chown(W1, SDHandle, ?TEST_UID, ?TEST_GID),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{
        owner_id = ?USER1,
        uid = ?TEST_UID,
        gid = ?TEST_GID
    }}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

    GeneratedUid = rpc:call(W2, luma_auto_feed, generate_uid, [?USER1]),

    StorageW2 = get_supporting_storage(W2, ?SPACE_ID),
    StorageSDHandleW2 = sd_test_utils:get_storage_mountpoint_handle(W1, ?SPACE_ID, StorageW2),
    {ok, #statbuf{st_gid = MountGid2}} = sd_test_utils:stat(W2, StorageSDHandleW2),

    ?assertMatch({ok, #file_attr{
        owner_id = ?USER1,
        uid = GeneratedUid,
        gid = MountGid2
    }}, lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

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
    }, ?SPACE_ID).

create_directory_import_check_user_id_error_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#775),
    ok = sd_test_utils:chown(W1, SDHandle, ?TEST_UID, ?TEST_GID),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),
    %% Check if dir was not imported
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 1,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_directory_import_without_read_permission_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#000),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH2}), ?ATTEMPTS),

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

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH2}), 2 * ?ATTEMPTS).

create_directory_import_many_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    DirsNumber = 200,
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dirs on storage
    lists_utils:pforeach(fun(N) ->
        DirPath = storage_path(?SPACE_ID, integer_to_binary(N), MountSpaceInRoot),
        SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, DirPath, RDWRStorage),
        ok = sd_test_utils:mkdir(W1, SDHandle, 8#775)
    end, lists:seq(1, DirsNumber)),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    parallel_assert(?MODULE, verify_dir, [W1, SessId, ?ATTEMPTS], lists:seq(1, DirsNumber), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 201,
        <<"imported">> => 200,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 200,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedHourHist">> => 200,
        <<"importedDayHist">> => 200,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_empty_file_import_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, <<"">>},
        lfm_proxy:read(W1, Handle1, 0, 100)),
    lfm_proxy:close(W1, Handle1),

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

    %% Check if file was imported on W2
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), 5 * ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, <<"">>},
        lfm_proxy:read(W2, Handle2, 0, 100), ?ATTEMPTS).

create_file_import_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),

    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    RDWRStorage = get_rdwr_storage(Config, W1),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

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

    %% Check if file was imported on W2
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), 5 * ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS).

create_delete_import_test_read_both(Config, MountSpaceInRoot) ->
    create_delete_import_test(Config, MountSpaceInRoot, true).

create_delete_import_test_read_remote_only(Config, MountSpaceInRoot) ->
    create_delete_import_test(Config, MountSpaceInRoot, false).

create_delete_import_test(Config, MountSpaceInRoot, ReadBoth) ->
    [W1, W2 | _] = Workers = ?config(op_worker_nodes, Config),
    Attempts = 60,

    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    StorageTestFilePath2 = storage_path(?SPACE_ID, ?TEST_FILE1, false),
    RDWRStorage = get_rdwr_storage(Config, W1),

    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),

    Size = byte_size(?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

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
    }, ?SPACE_ID, ?ATTEMPTS),

    ReadWorkers = case ReadBoth of
        true -> Workers;
        _ -> [W2]
    end,

    multi_provider_file_ops_test_base:verify_workers(ReadWorkers, fun(W) ->
        ?assertMatch({ok, ?TEST_DATA},
            begin
                SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W)}}, Config),
                case lfm_proxy:open(W, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read) of
                    {ok, Handle} ->
                        try
                            lfm_proxy:read(W, Handle, 0, Size)
                        after
                            lfm_proxy:close(W, Handle)
                        end;
                    OpenError ->
                        OpenError
                end
            end, Attempts)
    end),

    Storage2 = get_supporting_storage(W2, ?SPACE_ID),
    SDHandle2 = sd_test_utils:new_handle(W2, ?SPACE_ID, StorageTestFilePath2, Storage2),
    ?assertEqual({ok, ?TEST_DATA}, sd_test_utils:read_file(W1, SDHandle, 0, ?TEST_DATA_SIZE)),
    ?assertEqual({ok, ?TEST_DATA}, sd_test_utils:read_file(W2, SDHandle2, 0, ?TEST_DATA_SIZE)),

    SessIdW2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    {ok, #file_attr{guid = GUID}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessIdW2, {path, ?SPACE_TEST_FILE_PATH1})),

    ?assertMatch(ok, lfm_proxy:unlink(W2, ?ROOT_SESS_ID, {guid, GUID})),
    ?assertEqual({error, ?ENOENT}, sd_test_utils:read_file(W2, SDHandle2, 0, ?TEST_DATA_SIZE), Attempts),
    ?assertEqual({error, ?ENOENT}, sd_test_utils:read_file(W1, SDHandle, 0, ?TEST_DATA_SIZE), Attempts),
    ok.

create_file_import_check_user_id_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    ok = sd_test_utils:chown(W1, SDHandle, ?TEST_UID, ?TEST_GID),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),

    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported on W1
    ?assertMatch({ok, #file_attr{
        owner_id = ?USER1,
        uid = ?TEST_UID,
        gid = ?TEST_GID
    }},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    GeneratedUid = rpc:call(W1, luma_auto_feed, generate_uid, [?USER1]),
    StorageW2 = get_supporting_storage(W2, ?SPACE_ID),
    StorageSDHandleW2 = sd_test_utils:get_storage_mountpoint_handle(W1, ?SPACE_ID, StorageW2),
    {ok, #statbuf{st_gid = MountGid2}} = sd_test_utils:stat(W2, StorageSDHandleW2),
    ?assertMatch({ok, #file_attr{
        owner_id = ?USER1,
        uid = GeneratedUid,
        gid = MountGid2
    }}, lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

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
    }, ?SPACE_ID).

create_file_import_check_user_id_error_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    ok = sd_test_utils:chown(W1, SDHandle, ?TEST_UID, ?TEST_GID),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),

    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported on W1
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 1,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_file_in_dir_import_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFilePath = storage_path(?SPACE_ID, filename:join(?TEST_DIR, ?TEST_FILE2), MountSpaceInRoot),
    timer:sleep(timer:seconds(1)),

    %% Create dir on storage
    SDDirHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDDirHandle, 8#775),
    %% Create file on storage
    SDFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDFileHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDFileHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),

    %% Check if file was imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    %% Check if file was imported on W2
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}), 5 * ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),

    assertImportTimes(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"imported">> => 2,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 2,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 2,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_subfiles_import_many_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    DirsNumber = 200,
    lists_utils:pforeach(fun(N) ->
        NBin = integer_to_binary(N),
        DirPath = storage_path(?SPACE_ID, NBin, MountSpaceInRoot),
        FilePath = filename:join([DirPath, integer_to_binary(N)]),
        SDDirHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, DirPath, RDWRStorage),
        ok = sd_test_utils:mkdir(W1, SDDirHandle, 8#775),
        SDFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, FilePath, RDWRStorage),
        ok = sd_test_utils:create_file(W1, SDFileHandle, 8#664),
        {ok, _} = sd_test_utils:write_file(W1, SDFileHandle, 0, ?TEST_DATA)
    end, lists:seq(1, DirsNumber)),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID, 60),

    parallel_assert(?MODULE, verify_file_in_dir, [W1, SessId, 60], lists:seq(1, DirsNumber), 60),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 401,
        <<"imported">> => 400,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 400,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedHourHist">> => 400,
        <<"importedDayHist">> => 400,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_subfiles_import_many2_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    RootPath = storage_path(?SPACE_ID, <<"">>, MountSpaceInRoot),
    DirStructure = [10, 10, 10],
    RootSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, RootPath, RDWRStorage),

    create_nested_directory_tree(W1, DirStructure, RootSDHandle),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    Files = generate_nested_directory_tree_file_paths(DirStructure, ?SPACE_PATH),

    Timeout = 600,
    parallel_assert(?MODULE, verify_file, [W1, SessId, Timeout], Files, Timeout),
    assertImportTimes(W1, ?SPACE_ID, Timeout),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 1111,
        <<"imported">> => 1110,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 1110,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedDayHist">> => 1110,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_remote_file_import_conflict_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SyncedStorage = get_synced_storage(Config, W1),

    {ok, FileGuid} = lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH1, 8#664),
    {ok, Handle} = lfm_proxy:open(W2, SessId2, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(W2, Handle, 0, ?TEST_DATA),
    lfm_proxy:close(W2, Handle),

    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    ProviderId1 = provider_id(W1),
    ImportedConflictingFileName = ?IMPORTED_CONFLICTING_FILE_NAME(?TEST_FILE1, ProviderId1),
    ImportedConflictingFilePath = ?SPACE_TEST_FILE_PATH(ImportedConflictingFileName),

    %% Create conflicting file on synced storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA2),

    ?assertMatch({ok, _}, lfm_proxy:stat(W1, SessId, {guid, FileGuid}), ?ATTEMPTS),
    ?assertEqual({ok, [{FileGuid, ?TEST_FILE1}]}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 1), ?ATTEMPTS),

    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported on W1
    ?assertMatch({ok, #file_attr{name = ImportedConflictingFileName}},
        lfm_proxy:stat(W1, SessId, {path, ImportedConflictingFilePath}), ?ATTEMPTS),
    ?assertMatch({ok, [_, _]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {path, ImportedConflictingFilePath}, read)),
    ?assertMatch({ok, ?TEST_DATA2}, lfm_proxy:read(W1, Handle1, 0, ?TEST_DATA_SIZE2)),
    lfm_proxy:close(W1, Handle1),

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

    %% Check if file was imported on W2
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ImportedConflictingFilePath}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ImportedConflictingFilePath}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS).

create_remote_dir_import_race_test(Config, MountSpaceInRoot) ->
    % directory is created in remote provider and at the same time, file with the same name is created on storage
    % this tests checks whether we properly handle situation when links are synchronized during scan,
    % but file_meta is not yet synchronized
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SyncedStorage = get_synced_storage(Config, W1),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),

    timer:sleep(timer:seconds(1)),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#775),

    % pretend that only link has been synchronized
    Ctx = rpc:call(W1, file_meta, get_ctx, []),
    TreeId = rpc:call(W1, oneprovider, get_id, []),
    FileUuid = datastore_key:new(),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_ID),
    {ok, _} = rpc:call(W1, datastore_model, add_links,
        [Ctx#{scope => ?SPACE_ID}, SpaceUuid, TreeId, {?TEST_DIR, FileUuid}]),

    % sync should import directory with conflicting name
    ProviderId1 = provider_id(W1),
    ImportedConflictingDirName = ?IMPORTED_CONFLICTING_FILE_NAME(?TEST_DIR, ProviderId1),
    ImportedConflictingDirPath = ?SPACE_TEST_FILE_PATH(ImportedConflictingDirName),

    enable_import(Config, ?SPACE_ID, SyncedStorage),
    % wait till scan is finished
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if dir was imported
    {ok, #file_attr{guid = FileGuid}} = ?assertMatch({ok, #file_attr{name = ImportedConflictingDirName}},
        lfm_proxy:stat(W1, SessId, {path, ImportedConflictingDirPath}), ?ATTEMPTS),
    ?assertNotEqual(FileUuid, file_id:guid_to_uuid(FileGuid)),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ImportedConflictingDirPath}), ?ATTEMPTS),

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
    }, ?SPACE_ID).

create_remote_file_import_race_test(Config, MountSpaceInRoot) ->
    % file is created in remote provider and at the same time, file with the same name is created on storage
    % this tests checks whether we properly handle situation when links are synchronized during scan,
    % but file_location is not yet synchronized
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SyncedStorage = get_synced_storage(Config, W1),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),

    timer:sleep(timer:seconds(1)),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),

    % pretend that only link has been synchronized
    Ctx = rpc:call(W1, file_meta, get_ctx, []),
    TreeId = rpc:call(W1, oneprovider, get_id, []),
    FileUuid = datastore_key:new(),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_ID),
    {ok, _} = rpc:call(W1, datastore_model, add_links,
        [Ctx#{scope => ?SPACE_ID}, SpaceUuid, TreeId, {?TEST_FILE1, FileUuid}]),

    % sync should import file with conflicting name
    ProviderId1 = provider_id(W1),
    ImportedConflictingFileName = ?IMPORTED_CONFLICTING_FILE_NAME(?TEST_FILE1, ProviderId1),
    ImportedConflictingFilePath = ?SPACE_TEST_FILE_PATH(ImportedConflictingFileName),

    enable_import(Config, ?SPACE_ID, SyncedStorage),
    % wait till scan is finished
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported
    {ok, #file_attr{guid = FileGuid}} = ?assertMatch({ok, #file_attr{name = ImportedConflictingFileName}},
        lfm_proxy:stat(W1, SessId, {path, ImportedConflictingFilePath}), ?ATTEMPTS),
    ?assertNotEqual(FileUuid, file_id:guid_to_uuid(FileGuid)),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W2, SessId2, {path, ImportedConflictingFilePath}), ?ATTEMPTS),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, FileGuid}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W1, Handle, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W1, Handle),

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
    }, ?SPACE_ID).

cancel_scan(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    RootPath = storage_path(?SPACE_ID, <<"">>, MountSpaceInRoot),
    DirStructure = [10, 10, 10],
    RootSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, RootPath, RDWRStorage),

    create_nested_directory_tree(W1, DirStructure, RootSDHandle),
    SyncedStorage = get_synced_storage(Config, W1),
    Files = generate_nested_directory_tree_file_paths(DirStructure, ?SPACE_PATH),
    Timeout = 600,
    TestProc = self(),
    test_utils:mock_new(W1, storage_sync_engine, [passthrough]),
    test_utils:mock_expect(W1, storage_sync_engine, import_file_unsafe,
        fun(StorageFileCtx, Info) ->
            TestProc ! start,
            meck:passthrough([StorageFileCtx, Info])
        end),

    enable_import(Config, ?SPACE_ID, SyncedStorage),
    receive start -> ok end,

    cancel(W1, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID, Timeout),

    SSM = ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0
    }, ?SPACE_ID),

    #{
        <<"toProcess">> := ToProcess,
        <<"otherProcessed">> := OtherProcessed,
        <<"updated">> := Updated,
        <<"imported">> := Imported
    } = SSM,

    ?assert(ToProcess =< 1111),
    ?assert((OtherProcessed + Updated + Imported) =< 1111),

    % check whether next scan will import missing files
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID, 2, Timeout),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 1110,
        <<"deletedSum">> => 0,
        <<"importedDayHist">> => 1110,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),
    parallel_assert(?MODULE, verify_file, [W1, SessId, Timeout], Files, Timeout).

import_nfs_acl_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER2, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),

    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

    %% User1 should be allowed to read acl
    {ok, #xattr{value = Value}} = ?assertMatch({ok, #xattr{}},
        lfm_proxy:get_xattr(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, <<"cdmi_acl">>)),
    ?assertMatch(Value, ?ACL_JSON),

    %% User1 should not be allowed to set acl
    ?assertMatch({error, ?EACCES},
        lfm_proxy:set_xattr(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, #xattr{})),

    %% User1 should not be allowed to modify file attrs
    ?assertMatch({error, ?EACCES},
        lfm_proxy:truncate(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, 100)),

    %% User2 should be allowed to read acl
    {ok, #xattr{value = Value}} = ?assertMatch({ok, #xattr{}},
        lfm_proxy:get_xattr(W1, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, <<"cdmi_acl">>)),

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
    }, ?SPACE_ID).

import_nfs_acl_with_disabled_luma_should_fail_test(Config, ImportedStorage) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, ImportedStorage),
    RDWRStorage = get_rdwr_storage(Config, W1),

    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID, ?ATTEMPTS),

    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 1,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_file_import_race_test(Config) ->
    % in this test we check whether file is imported with IMPORTED suffix as conflicting file is created
    % via lfm
    [W1, W2 | _] = Workers = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    TestPid = self(),

    ok = test_utils:mock_new(Workers, storage_sync_engine),
    ok = test_utils:mock_expect(Workers, storage_sync_engine, import_file_unsafe, fun(StorageFileCtx, Info) ->
        block_syncing_process(TestPid),
        meck:passthrough([StorageFileCtx, Info])
    end),

    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, true),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),

    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),

    SyncingProcess = await_syncing_process(),
    {ok, _} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#777),
    {ok, Handle} = lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, write),
    {ok, _} = lfm_proxy:write(W1, Handle, 0, ?WRITE_TEXT),
    ok = lfm_proxy:close(W1, Handle),
    release_syncing_process(SyncingProcess),

    assertImportTimes(W1, ?SPACE_ID),

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

    ProviderId1 = provider_id(W1),
    ImportedConflictingFileName = ?IMPORTED_CONFLICTING_FILE_NAME(?TEST_FILE1, ProviderId1),
    ImportedConflictingFilePath = ?SPACE_TEST_FILE_PATH(ImportedConflictingFileName),

    %% Check if both files are visible in the space
    ?assertMatch({ok, [_, _]}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 2), ?ATTEMPTS),

    %% Check if file created via lfm is visible
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?WRITE_TEXT},
        lfm_proxy:read(W1, Handle1, 0, 1000)),
    lfm_proxy:close(W1, Handle1),

    %% Check if file created on storage was imported
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, ImportedConflictingFilePath}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ImportedConflictingFilePath}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle2, 0, 1000)),
    lfm_proxy:close(W1, Handle2),

    %% Check if file was imported on W2
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ImportedConflictingFilePath}), ?ATTEMPTS),
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ImportedConflictingFilePath}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle3, 0, 100), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle3).

close_file_import_race_test(Config, StorageType) ->
    % in this test we check whether file is not reimported when file is fully deleted before checking
    % deletion links
    [W1 | _] = Workers = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SyncedStorage = get_synced_storage(Config, W1),
    RDWRStorage = get_rdwr_storage(Config, W1),
    MountInRoot = true,

    {ok, {_, CreateHandle}} = lfm_proxy:create_and_open(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#777),
    {ok, _} = lfm_proxy:write(W1, CreateHandle, 0, ?WRITE_TEXT),
    ok = lfm_proxy:unlink(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}),

    TestPid = self(),

    ok = test_utils:mock_new(Workers, link_utils),
    ok = test_utils:mock_expect(Workers, link_utils, try_to_resolve_child_deletion_link, fun(FileName, ParentCtx) ->
        case FileName =:= ?TEST_FILE1 of
            true -> block_syncing_process(TestPid);
            _ -> ok
        end,
        meck:passthrough([FileName, ParentCtx])
    end),

    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    ?EXEC_ON_POSIX_ONLY(fun() ->
        % touch space_dir to ensure that it will be updated
        RDWRStorageMountPoint = get_mount_point(RDWRStorage),
        ContainerStorageSpacePath = storage_path(RDWRStorageMountPoint, ?SPACE_ID, <<"">>, MountInRoot),
        touch(W1, ContainerStorageSpacePath)
    end, StorageType),

    enable_import(Config, ?SPACE_ID, SyncedStorage),

    SyncingProcess = await_syncing_process(),
    ok = lfm_proxy:close(W1, CreateHandle),
    release_syncing_process(SyncingProcess),

    assertImportTimes(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID, ?ATTEMPTS),

    %% Check if file was not imported
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1})).

delete_file_reimport_race_test(Config, StorageType) ->
    % in this test, we check whether sync does not reimport file that is deleted between checking links and file_location
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    MountInRoot = true,
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SyncedStorage = get_synced_storage(Config, W1),

    %% Create file
    {ok, FileGuid} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, Handle1} = lfm_proxy:open(W1, SessId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(W1, Handle1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, Handle1),

    TestProcess = self(),
    ok = test_utils:mock_new(W1, storage_sync_engine),
    ok = test_utils:mock_expect(W1, storage_sync_engine, check_location_and_maybe_sync, fun(StorageFileCtx, FileCtx, Info) ->
        Guid = file_ctx:get_guid_const(FileCtx),
        case Guid =:= FileGuid of
            true -> block_syncing_process(TestProcess);
            false -> ok
        end,
        meck:passthrough([StorageFileCtx, FileCtx, Info])
    end),

    ?EXEC_ON_POSIX_ONLY(fun() ->
        % touch space dir to ensure that it will be scanned
        timer:sleep(timer:seconds(1)),
        RDWRStorageMountPoint = get_mount_point(RDWRStorage),
        ContainerStorageSpacePath = storage_path(RDWRStorageMountPoint, ?SPACE_ID, <<"">>, MountInRoot),
        touch(W1, ContainerStorageSpacePath)
    end, StorageType),

    enable_import(Config, ?SPACE_ID, SyncedStorage),
    SyncingProcess = await_syncing_process(),
    ok = lfm_proxy:unlink(W1, SessId, {guid, FileGuid}),
    release_syncing_process(SyncingProcess),

    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was not reimported on W1
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 1)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1, % space_dir will have updated time
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if file was deleted on W2
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W2, SessId2, {path, ?SPACE_PATH}, 0, 1)).

delete_opened_file_reimport_race_test(Config, StorageType) ->
    % in this test, we check whether sync does not reimport file that is deleted while still opened,
    % between checking links and file_location
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    MountInRoot = true,
    RDWRStorage = get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    SyncedStorage = get_synced_storage(Config, W1),

    %% Create file
    {ok, FileGuid} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, Handle1} = lfm_proxy:open(W1, SessId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(W1, Handle1, 0, ?TEST_DATA),

    TestProcess = self(),
    ok = test_utils:mock_new(W1, storage_sync_engine),
    ok = test_utils:mock_expect(W1, storage_sync_engine, check_location_and_maybe_sync, fun(StorageFileCtx, FileCtx, Info) ->
        Guid = file_ctx:get_guid_const(FileCtx),
        case Guid =:= FileGuid of
            true -> block_syncing_process(TestProcess);
            false -> ok
        end,
        meck:passthrough([StorageFileCtx, FileCtx, Info])
    end),

    ?EXEC_ON_POSIX_ONLY(fun() ->
        % touch space dir to ensure that it will be scanned
        timer:sleep(timer:seconds(1)),
        RDWRStorageMountPoint = get_mount_point(RDWRStorage),
        ContainerStorageSpacePath = storage_path(RDWRStorageMountPoint, ?SPACE_ID, <<"">>, MountInRoot),
        touch(W1, ContainerStorageSpacePath)
    end, StorageType),

    enable_import(Config, ?SPACE_ID, SyncedStorage),

    SyncingProcess = await_syncing_process(),
    ok = lfm_proxy:unlink(W1, SessId, {guid, FileGuid}),
    release_syncing_process(SyncingProcess),

    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was not reimported on W1
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 1)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1, % space_dir will have updated time
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if file was deleted on W2
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W2, SessId2, {path, ?SPACE_PATH}, 0, 1)).

%%%===================================================================
%%% Tests of update
%%%===================================================================

update_syncs_files_after_import_failed_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),

    %% Create file on storage
    timer:sleep(timer:seconds(1)),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    mock_import_file_error(W1, ?TEST_FILE1),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),

    assertImportTimes(W1, ?SPACE_ID),

    %% Check if dir was not imported
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH})),
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH})),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 1,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    unmock_import_file_error(W1),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 1,
        <<"updated">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 1,
        <<"updatedSum">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS).

update_syncs_files_after_previous_update_failed_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 1,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 0,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Create dir on storage
    timer:sleep(timer:seconds(1)),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    timer:sleep(timer:seconds(1)),

    mock_import_file_error(W1, ?TEST_FILE1),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),
    unmock_import_file_error(W1),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 1,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 0,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if dir was not imported
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1})),
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1})),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID, 3, ?ATTEMPTS),
    disable_update(Config),

    %next scan should import file
    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"toProcess">> => 2,
        <<"imported">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"updated">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID, ?ATTEMPTS),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS).

sync_should_not_reimport_deleted_but_still_opened_file(Config, StorageType) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),

    StorageSpacePath = storage_path(?SPACE_ID, <<"">>, true),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SpaceSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageSpacePath, RDWRStorage),

    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change

    % create first file
    {ok, G1} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, H1} = lfm_proxy:open(W1, SessId, {guid, G1}, write),
    {ok, _} = lfm_proxy:write(W1, H1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, H1),

    % open file
    ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, G1}, read), ?ATTEMPTS),

    % delete file
    ok = lfm_proxy:unlink(W1, SessId, {guid, G1}),

    % there should be 1 file on storage
    ?assertMatch({ok, [_]}, sd_test_utils:storage_ls(W1, SpaceSDHandle, 0, 10, StorageType)),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    % there should be no files visible in the space
    ?assertMatch({ok, []},
        lfm_proxy:get_children(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 0,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedMinHist">> => 0,
        <<"updatedHourHist">> => 0,
        <<"updatedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage(Config) ->
    MountSpaceInRoot = true,
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    SyncedStorage = get_synced_storage(Config, W1),
    RDWRStorage = get_rdwr_storage(Config, W1),
    TestDir = ?config(test_dir, Config),

    StorageTestDirPath = storage_path(?SPACE_ID, TestDir, MountSpaceInRoot),
    SpaceTestDirPath = ?SPACE_TEST_DIR_PATH(TestDir),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#755),

    enable_import(Config, ?SPACE_ID, SyncedStorage),
    % wait till scan is finished
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if dir was imported
    ?assertMatch({ok, [{_, TestDir}]}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10)),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, SpaceTestDirPath}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W2, SessId2, {path, SpaceTestDirPath}), ?ATTEMPTS),

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

    % mock error from helpers:rmdir
    ok = test_utils:mock_new(W1, helpers),
    ok = test_utils:mock_expect(W1, helpers, rmdir, fun(_, _) -> {error, ?ENOTEMPTY} end),

    ?assertEqual(ok, lfm_proxy:rm_recursive(W1, SessId, {path, SpaceTestDirPath})),

    % touch space dir to make sure that it will be updated
    timer:sleep(timer:seconds(1)),
    RDWRStorageMountPoint = get_mount_point(RDWRStorage),
    ContainerStorageSpacePath = storage_path(RDWRStorageMountPoint, ?SPACE_ID, <<"">>, MountSpaceInRoot),
    touch(W1, ContainerStorageSpacePath),

    enable_update(Config, ?SPACE_ID, RDWRStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1, % space dir was updated because we performed "touch" on it
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1, % test dir was ignored
        <<"importedSum">> => 1,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    % TestDir should not be reimported
    ?assertMatch({ok, []},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10)),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, SpaceTestDirPath}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W2, SessId2, {path, SpaceTestDirPath}), ?ATTEMPTS).

sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage(Config, StorageType) ->
    MountSpaceInRoot = true,
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    SyncedStorage = get_synced_storage(Config, W1),
    RDWRStorage = get_rdwr_storage(Config, W1),
    TestFile = ?config(test_file, Config),

    StorageTestFilePath = storage_path(?SPACE_ID, TestFile, MountSpaceInRoot),
    SpaceTestFilePath = ?SPACE_TEST_DIR_PATH(TestFile),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),

    enable_import(Config, ?SPACE_ID, SyncedStorage),
    % wait till scan is finished
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported
    ?assertMatch({ok, [{_, TestFile}]}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10)),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, SpaceTestFilePath}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W2, SessId2, {path, SpaceTestFilePath}), ?ATTEMPTS),

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

    % mock error from helpers:rmdir
    ok = test_utils:mock_new(W1, helpers),
    ok = test_utils:mock_expect(W1, helpers, unlink, fun(_, _, _) -> {error, ?EBUSY} end),

    lfm_proxy:rm_recursive(W1, SessId, {path, SpaceTestFilePath}),

    ?EXEC_ON_POSIX_ONLY(fun() ->
        % touch space dir to make sure that it will be updated
        timer:sleep(timer:seconds(1)),
        RDWRStorageMountPoint = get_mount_point(RDWRStorage),
        ContainerStorageSpacePath = storage_path(RDWRStorageMountPoint, ?SPACE_ID, <<"">>, MountSpaceInRoot),
        touch(W1, ContainerStorageSpacePath)
    end, StorageType),

    enable_update(Config, ?SPACE_ID, RDWRStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1, % space dir was updated because we performed "touch" on it
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1, % test file was ignored
        <<"importedSum">> => 1,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    % TestFile should not be reimported
    ?assertMatch({ok, []},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10)),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, SpaceTestFilePath}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W2, SessId2, {path, SpaceTestFilePath}), ?ATTEMPTS).

sync_should_not_import_recreated_file_with_suffix_on_storage(Config, StorageType) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER2, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageSpacePath = storage_path(?SPACE_ID, <<"">>, true),
    SpaceSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageSpacePath, RDWRStorage),
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change

    % create first file
    {ok, G1} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, H1} = lfm_proxy:open(W1, SessId, {guid, G1}, write),
    {ok, _} = lfm_proxy:write(W1, H1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, H1),
    % open file
    {ok, H2} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, G1}, read), ?ATTEMPTS),
    % delete file
    ok = lfm_proxy:unlink(W1, SessId, {guid, G1}),
    % recreate file with the same name as the deleted file

    {ok, G2} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, H3} = lfm_proxy:open(W1, SessId, {guid, G2}, write),
    {ok, _} = lfm_proxy:write(W1, H3, 0, ?TEST_DATA2),
    ok = lfm_proxy:close(W1, H3),

    % there should be 2 files on storage
    ?assertMatch({ok, [_, _]}, sd_test_utils:storage_ls(W1, SpaceSDHandle, 0, 10, StorageType)),
    SyncedStorage = get_synced_storage(Config, W1),

    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    % there should be only 1 file visible in the space
    ?assertMatch({ok, [_]},
        lfm_proxy:get_children(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)),

    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W1, H2, 0, ?TEST_DATA_SIZE2)),
    ok = lfm_proxy:close(W1, H2),

    {ok, H5} = lfm_proxy:open(W1, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read),
    ?assertMatch({ok, ?TEST_DATA2}, lfm_proxy:read(W1, H5, 0, ?TEST_DATA_SIZE2)),
    ok = lfm_proxy:close(W1, H5),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 0,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

sync_should_update_blocks_of_recreated_file_with_suffix_on_storage(Config, StorageType) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),

    StorageSpacePath = storage_path(?SPACE_ID, <<"">>, true),
    SpaceSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageSpacePath, RDWRStorage),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, true),

    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change

    % create first file
    {ok, G1} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, H1} = lfm_proxy:open(W1, SessId, {guid, G1}, write),
    {ok, _} = lfm_proxy:write(W1, H1, 0, ?TEST_DATA2),
    ok = lfm_proxy:close(W1, H1),

    timer:sleep(timer:seconds(1)), %ensure that file1 will be updated

    % open file
    {ok, _} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, G1}, read), ?ATTEMPTS),

    % delete file
    ok = lfm_proxy:unlink(W1, SessId, {guid, G1}),

    % create second file with the same name as the deleted file
    {ok, G2} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#666),
    {ok, H3} = lfm_proxy:open(W1, SessId, {guid, G2}, write),
    {ok, _} = lfm_proxy:write(W1, H3, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, H3),
    U2 = file_id:guid_to_uuid(G2),
    StorageTestFilePathWithSuffix = ?CONFLICTING_STORAGE_FILE_NAME(StorageTestFilePath, U2),
    FileWithSuffixHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePathWithSuffix, RDWRStorage),

    % there should be 2 files on storage
    ?assertMatch({ok, [_, _]}, sd_test_utils:storage_ls(W1, SpaceSDHandle, 0, 10, StorageType)),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    % there should be only 1 file visible in the space
    ?assertMatch({ok, [_]}, lfm_proxy:get_children(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 0,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    % change one byte in the suffixed file
    {ok, 1} = sd_test_utils:write_file(W1, FileWithSuffixHandle, ?CHANGED_BYTE_OFFSET, ?CHANGED_BYTE),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 0,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    {ok, H4} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {guid, G2}, read)),
    ?assertMatch({ok, ?TEST_DATA_ONE_BYTE_CHANGED},
        lfm_proxy:read(W1, H4, 0, ?TEST_DATA_SIZE), ?ATTEMPTS),

    %% Check if file was updated on W2
    {ok, H5} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {guid, G2}, read)),
    ?assertMatch({ok, ?TEST_DATA_ONE_BYTE_CHANGED},
        lfm_proxy:read(W2, H5, 0, ?TEST_DATA_SIZE), ?ATTEMPTS).

sync_should_not_import_replicated_file_with_suffix_on_storage(Config, StorageType) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    timer:sleep(timer:seconds(1)),

    StorageSpacePath = storage_path(?SPACE_ID, <<"">>, true),
    SpaceSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageSpacePath, RDWRStorage),

    {ok, G1} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, H1} = lfm_proxy:open(W1, SessId, {guid, G1}, write),
    {ok, _} = lfm_proxy:write(W1, H1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, H1),

    {ok, G2} = lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, H2} = lfm_proxy:open(W2, SessId2, {guid, G2}, write),
    {ok, _} = lfm_proxy:write(W2, H2, 0, ?TEST_DATA2),
    ok = lfm_proxy:close(W2, H2),

    % replicate file to W1
    {ok, H3} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, G2}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA2}, lfm_proxy:read(W1, H3, 0, 100), ?ATTEMPTS),
    ok = lfm_proxy:close(W1, H3),

    % there should be 2 files on storage
    ?assertMatch({ok, [_, _]}, sd_test_utils:storage_ls(W1, SpaceSDHandle, 0, 10, StorageType)),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    % there should be only 2 files visible in the space
    ?assertMatch({ok, [_, _]}, lfm_proxy:get_children(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 0,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    assertImportTimes(W1, ?SPACE_ID),
    % there still should be only 2 files visible in the space
    ?assertMatch({ok, [_, _]}, lfm_proxy:get_children(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)).

sync_should_update_replicated_file_with_suffix_on_storage(Config, StorageType) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageSpacePath = storage_path(?SPACE_ID, <<"">>, true),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, true),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SpaceSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageSpacePath, RDWRStorage),

    {ok, G1} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, H1} = lfm_proxy:open(W1, SessId, {guid, G1}, write),
    {ok, _} = lfm_proxy:write(W1, H1, 0, ?TEST_DATA2),
    ok = lfm_proxy:close(W1, H1),

    {ok, G2} = lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH1, 8#666),
    {ok, H2} = lfm_proxy:open(W2, SessId2, {guid, G2}, write),
    {ok, _} = lfm_proxy:write(W2, H2, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W2, H2),
    U2 = file_id:guid_to_uuid(G2),
    StorageTestFilePathWithSuffix = ?CONFLICTING_STORAGE_FILE_NAME(StorageTestFilePath, U2),
    FileWithSuffixHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePathWithSuffix, RDWRStorage),

    % replicate file to W1
    {ok, H3} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, G2}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W1, H3, 0, 100)),
    ok = lfm_proxy:close(W1, H3),

    % there should be 2 files on storage
    ?assertMatch({ok, [_, _]}, sd_test_utils:storage_ls(W1, SpaceSDHandle, 0, 10, StorageType)),
    SyncedStorage = get_synced_storage(Config, W1),

    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    % there should be only 2 files visible in the space
    ?assertMatch({ok, [_, _]}, lfm_proxy:get_children(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 0,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    % change one byte in the suffixed file
    {ok, 1} = sd_test_utils:write_file(W1, FileWithSuffixHandle, ?CHANGED_BYTE_OFFSET, ?CHANGED_BYTE),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 0,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    {ok, H4} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {guid, G2}, read)),
    ?assertMatch({ok, ?TEST_DATA_ONE_BYTE_CHANGED},
        lfm_proxy:read(W1, H4, 0, 100), ?ATTEMPTS),

    %% Check if file was imported on W2
    {ok, H5} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {guid, G2}, read)),
    ?assertMatch({ok, ?TEST_DATA_ONE_BYTE_CHANGED},
        lfm_proxy:read(W2, H5, 0, 100), ?ATTEMPTS).

sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

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

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
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
    }, ?SPACE_ID).

create_delete_import2_test(Config, MountSpaceInRoot, ReadBoth) ->
    [W1, W2 | _] = Workers = ?config(op_worker_nodes, Config),
    Attempts = 60,
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    StorageTestFilePath2 = storage_path(?SPACE_ID, ?TEST_FILE1, false),
    RDWRStorage = get_rdwr_storage(Config, W1),

    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    Size = byte_size(?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),

    ReadWorkers = case ReadBoth of
        true -> Workers;
        _ -> [W2]
    end,

    multi_provider_file_ops_test_base:verify_workers(ReadWorkers, fun(W) ->
        ?assertMatch({ok, ?TEST_DATA},
            begin
                SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W)}}, Config),
                case lfm_proxy:open(W, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read) of
                    {ok, Handle} ->
                        try
                            lfm_proxy:read(W, Handle, 0, Size)
                        after
                            lfm_proxy:close(W, Handle)
                        end;
                    OpenError ->
                        OpenError
                end
            end, Attempts)
    end),

    %% Create file on storage
    Storage2 = get_supporting_storage(W2, ?SPACE_ID),
    SDHandle2 = sd_test_utils:new_handle(W2, ?SPACE_ID, StorageTestFilePath2, Storage2),
    ?assertEqual({ok, ?TEST_DATA}, sd_test_utils:read_file(W1, SDHandle, 0, ?TEST_DATA_SIZE)),
    ?assertEqual({ok, ?TEST_DATA}, sd_test_utils:read_file(W2, SDHandle2, 0, ?TEST_DATA_SIZE)),

    SessIdW1 = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessIdW2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),

    {ok, #file_attr{guid = GUID}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessIdW2, {path, ?SPACE_TEST_FILE_PATH1})),
    ?assertMatch(ok, lfm_proxy:unlink(W2, <<"0">>, {guid, GUID})),

    ?assertEqual({error, ?ENOENT}, sd_test_utils:read_file(W1, SDHandle, 0, ?TEST_DATA_SIZE), Attempts),
    ?assertEqual({error, ?ENOENT}, sd_test_utils:read_file(W2, SDHandle2, 0, ?TEST_DATA_SIZE), Attempts),

    enable_update(Config, ?SPACE_ID, SyncedStorage),

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(W2, SessIdW2, ?SPACE_TEST_FILE_PATH1, 8#777)),
    {ok, FileHandle} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessIdW2, {guid, FileGuid}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W2, FileHandle, 0, ?TEST_DATA)),

    {ok, FileHandle2} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessIdW1, {guid, FileGuid}, read), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(W1, FileHandle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, sd_test_utils:read_file(W1, SDHandle, 0, ?TEST_DATA_SIZE), Attempts),
    ok = sd_test_utils:unlink(W1, SDHandle, Size),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:open(W2, SessIdW2, {guid, FileGuid}, read), ?ATTEMPTS).

create_subfiles_and_delete_before_import_is_finished_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#775),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),

    assertImportTimes(W1, ?SPACE_ID),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

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

    %% Create nested tree structure
    DirStructure = [10, 10, 10],
    create_nested_directory_tree(W1, DirStructure, SDHandle),
    enable_update(Config, ?SPACE_ID, SyncedStorage),

    ?assertEqual(true, 10 =< rpc:call(W1, storage_sync_monitoring, get_unhandled_jobs_value,
        [?SPACE_ID, initializer:get_supporting_storage_id(W1, ?SPACE_ID)]), ?ATTEMPTS),

    ok = sd_test_utils:recursive_rm(W1, SDHandle),
    ?assertMatch({error, ?ENOENT}, sd_test_utils:ls(W1, SDHandle, 0, 100)),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 100), ?ATTEMPTS),
    assertUpdateTimes(W1, ?SPACE_ID, 3),
    disable_update(Config).

create_file_in_dir_update_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 = storage_path(?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    StorageTestFileinDirPath1 = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

    %% Create dirs on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    SDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath2, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle2, 8#777),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),

    assertImportTimes(W1, ?SPACE_ID),

    %% Check if dirs were imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH2}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"imported">> => 2,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 2,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 2,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    test_utils:mock_new(W1, storage_sync_hash, [passthrough]),
    test_utils:mock_new(W1, storage_sync_traverse, [passthrough]),
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    %% Check if files were imported on W1
    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"imported">> => 1,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"importedSum">> => 3,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 3,
        <<"importedDayHist">> => 3,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    History = rpc:call(W1, meck, history, [storage_sync_hash]),
    History2 = rpc:call(W1, meck, history, [storage_sync_traverse]),

    {ok, Handle5} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle5, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle5),

    assert_num_results(History, ?assertHashChangedFun(?SPACE_PATH, ?SPACE_ID, true), 0),
    assert_num_results(History2, ?assertMtimeChangedFun(StorageTestDirPath2, ?SPACE_ID, true), 0),
    assert_num_results_gte(History2, ?assertMtimeChangedFun(StorageTestDirPath, ?SPACE_ID, true), 1),
    %% Check if file was imported on W2
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), 5 * ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS).


changing_max_depth_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),

    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, true),

    Dir2Path = filename:join([?TEST_DIR, ?TEST_DIR2]),
    StorageTestDirPath2 = storage_path(?SPACE_ID, Dir2Path, true),
    SpaceTestDirPath2 = ?SPACE_TEST_DIR_PATH(Dir2Path),

    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, true),
    StorageTestFileinDirPath = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), true),

    File2Path = filename:join([?TEST_DIR, ?TEST_DIR2, ?TEST_FILE1]),
    StorageTestFileinDirPath2 = storage_path(?SPACE_ID, File2Path, true),
    SpaceTestFilePath2 = ?SPACE_TEST_DIR_PATH(File2Path),

    %% Create directories and files on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#775),
    SDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath2, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle2, 8#775),
    FileSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileSDHandle, 0, ?TEST_DATA),
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    FileInDirSDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath2, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle2, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle2, 0, ?TEST_DATA),

    SyncedStorage = get_synced_storage(Config, W1),
    % in init_per_testcase, max_depth for import is set to 1
    enable_import(Config, ?SPACE_ID, SyncedStorage),

    assertImportTimes(W1, ?SPACE_ID),

    %% Only directory and file on 1st level should be imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, [_, _]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, []},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 10), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, SpaceTestDirPath2}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:get_children(W1, SessId, {path, SpaceTestDirPath2}, 0, 10), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"imported">> => 2,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 2,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 2,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    % in init_per_testcase, max_depth for update is set to 2, so new file should be detected
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    %% Directory and file on 2nd level should be imported
    ?assertMatch({ok, [_, _]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, SpaceTestDirPath2}), ?ATTEMPTS),
    ?assertMatch({ok, []},
        lfm_proxy:get_children(W1, SessId, {path, SpaceTestDirPath2}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"imported">> => 2,
        <<"updated">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"importedSum">> => 4,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 2,
        <<"importedHourHist">> => 4,
        <<"importedDayHist">> => 4,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),


    % run another scan with max_depth = 3
    enable_update(Config, ?SPACE_ID, SyncedStorage, #{max_depth => 3}),
    assertUpdateTimes(W1, ?SPACE_ID, 3),
    disable_update(Config),

    %% Directory and file on 3rd level should be imported
    ?assertMatch({ok, [_]},
        lfm_proxy:get_children(W1, SessId, {path, SpaceTestDirPath2}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, SpaceTestFilePath2}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"toProcess">> => 4,
        <<"imported">> => 1,
        <<"updated">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 3,
        <<"importedSum">> => 5,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 5,
        <<"importedDayHist">> => 5,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_file_in_dir_exceed_batch_update_test(Config, MountSpaceInRoot) ->
    % in this test storage_sync_dir_batch_size is set in init_per_testcase to 2
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),

    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 = storage_path(?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    StorageTestFilePath1 = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    StorageTestFilePath2 = storage_path(?SPACE_ID, ?TEST_FILE2, MountSpaceInRoot),
    StorageTestFilePath3 = storage_path(?SPACE_ID, ?TEST_FILE3, MountSpaceInRoot),
    StorageTestFilePath4 = storage_path(?SPACE_ID, ?TEST_FILE4, MountSpaceInRoot),
    StorageTestFileinDirPath1 = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

    DirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    DirSDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath2, RDWRStorage),
    FileSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath1, RDWRStorage),
    FileSDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath2, RDWRStorage),
    FileSDHandle3 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath3, RDWRStorage),
    FileSDHandle4 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath4, RDWRStorage),
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),

    %% Create dirs on storage
    ok = sd_test_utils:mkdir(W1, DirSDHandle, 8#777),
    ok = sd_test_utils:mkdir(W1, DirSDHandle2, 8#777),

    %% Create files on storage
    ok = sd_test_utils:create_file(W1, FileSDHandle, 8#664),
    ok = sd_test_utils:create_file(W1, FileSDHandle2, 8#664),
    ok = sd_test_utils:create_file(W1, FileSDHandle3, 8#664),
    ok = sd_test_utils:create_file(W1, FileSDHandle4, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileSDHandle, 0, ?TEST_DATA),
    {ok, _} = sd_test_utils:write_file(W1, FileSDHandle2, 0, ?TEST_DATA),
    {ok, _} = sd_test_utils:write_file(W1, FileSDHandle3, 0, ?TEST_DATA),
    {ok, _} = sd_test_utils:write_file(W1, FileSDHandle4, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),

    timer:sleep(timer:seconds(1)),

    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if files were imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH2}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1})),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH2})),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH3})),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH4})),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 10,
        <<"imported">> => 6,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 3,
        <<"importedSum">> => 6,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 6,
        <<"importedHourHist">> => 6,
        <<"importedDayHist">> => 6,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    timer:sleep(timer:seconds(1)),
    test_utils:mock_new(W1, storage_sync_hash, [passthrough]),
    test_utils:mock_new(W1, storage_sync_traverse, [passthrough]),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 7,
        <<"imported">> => 1,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 5,
        <<"importedSum">> => 7,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 7,
        <<"importedDayHist">> => 7,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if files were imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),

    History = rpc:call(W1, meck, history, [storage_sync_hash]),
    History2 = rpc:call(W1, meck, history, [storage_sync_traverse]),

    {ok, Handle5} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle5, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle5),

    assert_num_results(History, ?assertHashChangedFun(StorageTestDirPath2, ?SPACE_ID, true), 0),
    assert_num_results(History2, ?assertMtimeChangedFun(StorageTestDirPath2, ?SPACE_ID, true), 0),
    assert_num_results_gte(History2, ?assertMtimeChangedFun(StorageTestDirPath, ?SPACE_ID, true), 1),

    %% Check if file was imported on W2
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), 5 * ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS).

delete_empty_directory_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    %% Create dir on storage
    RDWRStorage = get_rdwr_storage(Config, W1),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    %% Delete dir on storage
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    ok = sd_test_utils:rmdir(W1, SDHandle),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 1,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 1,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1
    }, ?SPACE_ID),

    %% Check if dir was deleted in space
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS).

delete_non_empty_directory_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFileinDirPath1 = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    {ok, Handle5} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle5, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle5),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    %% Delete dir on storage
    ok = sd_test_utils:recursive_rm(W1, SDHandle),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 2,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 2,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 2,
        <<"importedMinHist">> => 2,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 2,
        <<"deletedHourHist">> => 2,
        <<"deletedDayHist">> => 2
    }, ?SPACE_ID),

    %% Check if dir was deleted in space
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS).

sync_works_properly_after_delete_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 = storage_path(?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    StorageTestFileinDirPath1 =
        storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),
    StorageTestFileinDirPath2 =
        storage_path(?SPACE_ID, filename:join([?TEST_DIR2, ?TEST_FILE2]), MountSpaceInRoot),

    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID, ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"imported">> => 2,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 2,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 2,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    {ok, Handle5} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle5, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle5),

    % ensure that mtime will be changed
    timer:sleep(timer:seconds(1)),

    %% Delete dir on storage
    ok = sd_test_utils:recursive_rm(W1, SDHandle),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    %% Check if dir was deleted in space
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 2,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 2,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 2,
        <<"importedMinHist">> => 2,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 2,
        <<"deletedHourHist">> => 2,
        <<"deletedDayHist">> => 2
    }, ?SPACE_ID),

    % ensure that mtime will be changed
    timer:sleep(timer:seconds(1)),
    SDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath2, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle2, 8#777),
    FileInDirSDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath2, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle2, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle2, 0, ?TEST_DATA),
    SpaceTestFileInDirPath = ?SPACE_TEST_FILE_IN_DIR_PATH(?TEST_DIR2, ?TEST_FILE2),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID, 3, ?ATTEMPTS),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"toProcess">> => 5,
        <<"imported">> => 2,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"importedSum">> => 4,
        <<"updatedSum">> => 3,
        <<"deletedSum">> => 2,
        <<"importedMinHist">> => 2,
        <<"importedHourHist">> => 4,
        <<"importedDayHist">> => 4,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 3,
        <<"updatedDayHist">> => 3,
        <<"deletedMinHist">> => 2,
        <<"deletedHourHist">> => 2,
        <<"deletedDayHist">> => 2
    }, ?SPACE_ID, ?ATTEMPTS),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH2}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, SpaceTestFileInDirPath}), ?ATTEMPTS),
    {ok, Handle6} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, SpaceTestFileInDirPath}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle6, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle6).

delete_and_update_files_simultaneously_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFileinDirPath1 = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),
    StorageTestFileinDirPath2 = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE2]), MountSpaceInRoot),
    NewMode = 8#600,
    RDWRStorage = get_rdwr_storage(Config, W1),

    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    FileInDirSDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath2, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle2, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle2, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    {ok, Handle5} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle5, 0, byte_size(?TEST_DATA))),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}), ?ATTEMPTS),
    {ok, Handle6} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle6, 0, byte_size(?TEST_DATA))),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 4,
        <<"imported">> => 3,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 3,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 3,
        <<"importedHourHist">> => 3,
        <<"importedDayHist">> => 3,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Delete file on storage
    sd_test_utils:unlink(W1, FileInDirSDHandle, ?TEST_DATA_SIZE),
    sd_test_utils:chmod(W1, FileInDirSDHandle2, NewMode),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 5,
        <<"imported">> => 0,
        <<"deleted">> => 1,
        <<"updated">> => 2,
        <<"otherProcessed">> => 2,
        <<"failed">> => 0,
        <<"importedSum">> => 3,
        <<"updatedSum">> => 3,
        <<"deletedSum">> => 1,
        <<"importedMinHist">> => 3,
        <<"importedHourHist">> => 3,
        <<"importedDayHist">> => 3,
        <<"updatedMinHist">> => 2,
        <<"updatedHourHist">> => 3,
        <<"updatedDayHist">> => 3,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1
    }, ?SPACE_ID),

    %% Check if File1 was deleted in and if File2 was updated
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),

    ?assertMatch({ok, #file_attr{mode = NewMode}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}), ?ATTEMPTS).

delete_file_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    %% Check if file was imported
    {ok, #file_attr{guid = FileGuid}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle1} =
        ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    FileUuid = file_id:guid_to_uuid(FileGuid),
    Xattr = #xattr{name = <<"xattr_name">>, value = <<"xattr_value">>},
    ok = lfm_proxy:set_xattr(W1, ?ROOT_SESS_ID, {guid, FileGuid}, Xattr),

    %% Delete file on storage
    ok = sd_test_utils:unlink(W1, SDHandle, ?TEST_DATA_SIZE),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 1,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 1,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1
    }, ?SPACE_ID),

    %% Check if file was deleted from the space
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, []},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_xattr(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, <<"xattr_name">>)),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_xattr(W1, SessId, {guid, FileGuid}, <<"xattr_name">>)),
    ?assertMatch({error, not_found}, rpc:call(W1, custom_metadata, get, [FileUuid])).

delete_file_in_dir_update_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFilePath = storage_path(?SPACE_ID, filename:join(?TEST_DIR, ?TEST_FILE2), MountSpaceInRoot),
    timer:sleep(timer:seconds(1)),

    %% Create dir on storage
    SDDirHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDDirHandle, 8#775),

    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

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

    %% Check if dir was imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, [{_, ?TEST_DIR}]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, []},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 10), ?ATTEMPTS),

    %% Create file on storage
    SDFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDFileHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDFileHandle, 0, ?TEST_DATA),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"imported">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 2,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if file was imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),
    ?assertMatch({ok, [{_, ?TEST_FILE2}]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 10), ?ATTEMPTS),

    % Delete file in the directory from storage
    ok = sd_test_utils:unlink(W1, SDFileHandle, ?TEST_DATA_SIZE),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID, 3),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"toProcess">> => 4,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"importedSum">> => 2,
        <<"deletedSum">> => 1,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1
    }, ?SPACE_ID),

    %% Check if file was deleted from the space
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}), ?ATTEMPTS),
    ?assertMatch({ok, []},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 10), ?ATTEMPTS).


delete_many_subfiles_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    RootPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, RootPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    DirStructure = [10, 10, 10],
    create_nested_directory_tree(W1, DirStructure, SDHandle),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    Files = generate_nested_directory_tree_file_paths(DirStructure, ?SPACE_TEST_DIR_PATH),

    Timeout = 600,
    parallel_assert(?MODULE, verify_file, [W1, SessId, Timeout], Files, Timeout),
    assertImportTimes(W1, ?SPACE_ID, Timeout),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 1112,
        <<"imported">> => 1111,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 1111,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedHourHist">> => 1111,
        <<"importedDayHist">> => 1111,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    ok = sd_test_utils:recursive_rm(W1, SDHandle),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID, 2, Timeout),
    parallel_assert(?MODULE, verify_file_deleted, [W1, SessId, Timeout], [?SPACE_TEST_DIR_PATH | Files], Timeout),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 1113,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 1111,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 1111,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 1111,
        <<"importedDayHist">> => 1111,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedHourHist">> => 1111,
        <<"deletedDayHist">> => 1111
    }, ?SPACE_ID).

create_delete_race_test(Config, MountSpaceInRoot, StorageType) ->
    % this tests checks whether sync works properly in case of create-delete race
    % description:
    % if the file is created after storage_sync_links tree is built
    % it is necessary to ensure that sync does not delete the file
    % because it's missing in the file_meta links
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),

    SyncedStorage = get_synced_storage(Config, W1),
    RDWRStorage = get_rdwr_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    TestPid = self(),
    ok = test_utils:mock_new(W1, storage_sync_deletion),
    ok = test_utils:mock_expect(W1, storage_sync_deletion, do_master_job, fun(Job, Args) ->
        % hold on sync
        block_syncing_process(TestPid),
        meck:passthrough([Job, Args])
    end),

    ?EXEC_ON_POSIX_ONLY(fun() ->
        % touch space dir to ensure that sync will try to detect deletions
        timer:sleep(timer:seconds(1)),
        RDWRStorageMountPoint = get_mount_point(RDWRStorage),
        ContainerStorageSpacePath = storage_path(RDWRStorageMountPoint, ?SPACE_ID, <<"">>, MountSpaceInRoot),
        touch(W1, ContainerStorageSpacePath)
    end, StorageType),

    enable_update(Config, ?SPACE_ID, SyncedStorage),

    SyncingProcess = await_syncing_process(),

    % create file, it should not be deleted
    {ok, FileGuid2} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#664),
    {ok, Handle2} = lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, write),
    {ok, _} = lfm_proxy:write(W1, Handle2, 0, ?TEST_DATA2),
    ok = lfm_proxy:close(W1, Handle2),

    release_syncing_process(SyncingProcess),

    assertUpdateTimes(W1, ?SPACE_ID),
    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"updated">> => 1,
        <<"importedSum">> => 0,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    {ok, Handle3} = lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read),
    ?assertMatch({ok, ?TEST_DATA2}, lfm_proxy:read(W1, Handle3, 0, byte_size(?TEST_DATA2))),
    ok = lfm_proxy:close(W1, Handle3),

    ?assertMatch({ok, #file_attr{guid = FileGuid2}}, lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}),
        ?ATTEMPTS),
    {ok, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA2}, lfm_proxy:read(W2, Handle4, 0, byte_size(?TEST_DATA2)), ?ATTEMPTS),
    ok = lfm_proxy:close(W2, Handle4).

create_list_race_test(Config, MountSpaceInRoot) ->
    % this tests checks whether sync works properly in case of create-list race
    % description:
    % sync builds storage_sync_links tree for detecting deleted files by listing the storage using offset and limit
    % it is possible that if other files are deleted in the meantime, file may be omitted and therefore missing
    % in the storage_sync_links
    % sync must not delete such file
    % storage_sync_dir_batch_size is set in this test to 2
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SyncedStorage = get_synced_storage(Config, W1),

    % create 3 files
    FilesNum = 3,
    FilePaths = [?SPACE_TEST_FILE_PATH(?TEST_FILE(N)) || N <- lists:seq(1, FilesNum)],
    lists:foreach(fun(F) ->
        {ok, FileGuid} = lfm_proxy:create(W1, SessId, F, 8#664),
        {ok, Handle} = lfm_proxy:open(W1, SessId, {guid, FileGuid}, write),
        {ok, _} = lfm_proxy:write(W1, Handle, 0, ?TEST_DATA),
        lfm_proxy:close(W1, Handle),
        FileGuid
    end, FilePaths),

    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    TestPid = self(),
    ok = test_utils:mock_new(W1, storage_driver),
    ok = test_utils:mock_expect(W1, storage_driver, readdir, fun(SDHandle, Offset, BatchSize) ->
        case SDHandle#sd_handle.file =:= <<"/">> of
            true ->
                % hold on sync
                TestPid ! {waiting, self(), Offset, meck:passthrough([SDHandle, Offset, BatchSize])},
                receive continue -> ok end;
            false ->
                ok
        end,
        meck:passthrough([SDHandle, Offset, BatchSize])
    end),

    % touch space dir to ensure that sync will try to detect deletions
    timer:sleep(timer:seconds(1)),
    RDWRStorageMountPoint = get_mount_point(RDWRStorage),
    ContainerStorageSpacePath = storage_path(RDWRStorageMountPoint, ?SPACE_ID, <<"">>, MountSpaceInRoot),
    touch(W1, ContainerStorageSpacePath),

    enable_update(Config, ?SPACE_ID, SyncedStorage),

    ListedFiles = [FileToDeleteOnStorage, FileToDeleteByLFM] = receive
        {waiting, Pid, 0, {ok, Files}} ->
            % continue sync
            Pid ! continue,
            Files
    end,

    FileToDeleteByLFMPath = ?SPACE_TEST_FILE_PATH(FileToDeleteByLFM),
    FileToDeleteOnStoragePath = storage_path(?SPACE_ID, FileToDeleteOnStorage, MountSpaceInRoot),

    receive
        {waiting, Pid2, 2, _} ->
            % delete 2 first files so that third will be placed in the first batch
            ok = lfm_proxy:unlink(W1, SessId, {path, FileToDeleteByLFMPath}),
            SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, FileToDeleteOnStoragePath, RDWRStorage),
            ok = sd_test_utils:unlink(W1, SDHandle, ?TEST_DATA_SIZE),
            % continue sync
            Pid2 ! continue
    end,

    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"imported">> => 0,
        <<"deleted">> => 0, % no deleted files because FileToDeleteOnStorage deletion will be detected in next scan
        <<"failed">> => 0,
        <<"importedSum">> => 0,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    [LeftFile] = FilePaths -- [?SPACE_TEST_FILE_PATH(F) || F <- ListedFiles],

    % FileToDeleteOnStorage deletion is not detected yet
    ?assertMatch({ok, _}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH(FileToDeleteOnStorage)}), ?ATTEMPTS),

    LeftFilePath = ?SPACE_TEST_FILE_PATH(LeftFile),
    ?assertMatch({ok, _}, lfm_proxy:stat(W1, SessId, {path, LeftFilePath}), ?ATTEMPTS),

    {ok, Handle2} = lfm_proxy:open(W1, SessId, {path, LeftFilePath}, read),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle2),

    ok = test_utils:mock_unload(W1, storage_driver),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID, 3, ?ATTEMPTS),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"toProcess">> => 4,
        <<"imported">> => 0,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"importedSum">> => 0,
        <<"deletedSum">> => 1,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1
    }, ?SPACE_ID, ?ATTEMPTS),


    % File2 deletion is finally detected
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH(FileToDeleteOnStorage)}),
        ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH(FileToDeleteOnStorage)}),
        ?ATTEMPTS),

    {ok, Handle3} = lfm_proxy:open(W1, SessId, {path, LeftFilePath}, read),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W1, Handle3, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle3),

    {ok, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {path, LeftFilePath}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W2, Handle4, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W2, Handle4).

append_file_update_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    %% Check if file is visible on 2nd provider
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle2),

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

    %% Append to file
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, ?TEST_DATA_SIZE, ?TEST_DATA2),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    %% Check if appended bytes were imported on worker1
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    AppendedData = <<(?TEST_DATA)/binary, (?TEST_DATA2)/binary>>,
    ?assertMatch({ok, AppendedData},
        lfm_proxy:read(W1, Handle3, 0, byte_size(AppendedData))),
    lfm_proxy:close(W1, Handle3),

    {ok, Handle4} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, AppendedData},
        lfm_proxy:read(W2, Handle4, 0, byte_size(AppendedData)), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle4),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

append_file_not_changing_mtime_update_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    HostStorageTestFilePath = storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    %% Check if file is visible on 2nd provider
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle2),

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

    %% Append to file
    {ok, #statbuf{st_mtime = StMtime}} = sd_test_utils:stat(W1, SDHandle),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, ?TEST_DATA_SIZE, ?TEST_DATA2),
    change_time(HostStorageTestFilePath, StMtime),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    %% Check if appended bytes were imported on worker1
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    AppendedData = <<(?TEST_DATA)/binary, (?TEST_DATA2)/binary>>,
    ?assertMatch({ok, AppendedData},
        lfm_proxy:read(W1, Handle3, 0, byte_size(AppendedData))),
    lfm_proxy:close(W1, Handle3),

    {ok, Handle4} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, AppendedData},
        lfm_proxy:read(W2, Handle4, 0, byte_size(AppendedData)), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle4),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

append_empty_file_update_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, <<"">>),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, <<"">>},
        lfm_proxy:read(W1, Handle1, 0, 100)),
    lfm_proxy:close(W1, Handle1),

    %% Check if file is visible on 2nd provider
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, <<"">>},
        lfm_proxy:read(W2, Handle2, 0, 100), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle2),

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

    %% Append to file
    ?assertEqual({ok, ?TEST_DATA_SIZE2},
        sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA2)),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    %% Check if appended bytes were imported on worker1
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA2},
        lfm_proxy:read(W1, Handle3, 0, byte_size(?TEST_DATA2))),
    lfm_proxy:close(W1, Handle3),

    {ok, Handle4} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA2},
        lfm_proxy:read(W2, Handle4, 0, byte_size(?TEST_DATA2)), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle4),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

copy_file_update_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    % we need W1MountPoint as cp is performed via file not storage_driver module
    SrcFilePath = storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    DestFilePath = storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE2, MountSpaceInRoot),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

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
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
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

    %% Copy file
    file:copy(SrcFilePath, DestFilePath),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

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
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
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

move_file_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    SrcStorageFilePath = storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    DestStorageFilePath = storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE2, MountSpaceInRoot),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

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
    %% Move file
    ok = file:rename(SrcStorageFilePath, DestStorageFilePath),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),
    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"imported">> => 1,
        <<"updated">> => 1,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 2,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 1,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1
    }, ?SPACE_ID),

    %% Check if file was moved
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH2}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA))),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1})),
    lfm_proxy:close(W1, Handle2).

truncate_file_update_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

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
    {ok, #file_attr{guid = G}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    %% Check if file is visible on 2nd provider and replicate it
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle2),

    timer:sleep(timer:seconds(2)), %ensure that modify time is different from read time

    %% Truncate file
    {ok, SDHandle2} = sd_test_utils:open(W1, SDHandle, write),
    sd_test_utils:truncate(W1, SDHandle2, 1, ?TEST_DATA_SIZE),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    %% Check if file was truncated
    {ok, #file_attr{guid = G}} = ?assertMatch({ok, #file_attr{size = 1}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, <<"t">>},
        lfm_proxy:read(W1, Handle3, 0, 100), ?ATTEMPTS),
    lfm_proxy:close(W1, Handle3),

    ?assertMatch({ok, #file_attr{size = 1}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle4} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, <<"t">>},
        lfm_proxy:read(W2, Handle4, 0, 100), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle4),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

change_file_content_constant_size_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

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

    %% Check if file is visible on 2nd provider
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle2),

    timer:sleep(timer:seconds(2)), %ensure that modify time is different from read time
    %% Truncate file
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, ?CHANGED_BYTE_OFFSET, ?CHANGED_BYTE),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    %% Check if file was updated
    DataSize = byte_size(?TEST_DATA),
    ?assertMatch({ok, #file_attr{size = DataSize}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA_ONE_BYTE_CHANGED},
        lfm_proxy:read(W1, Handle3, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle3),

    ?assertMatch({ok, #file_attr{size = DataSize}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle4} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA_ONE_BYTE_CHANGED},
        lfm_proxy:read(W2, Handle4, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle4),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

change_file_content_update_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

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

    %% Check if file is visible on 2nd provider
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle2),

    timer:sleep(timer:seconds(2)), %ensure that modify time is different from read time
    %% Modify file
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA_CHANGED),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    %% Check if file was updated
    DataSize = byte_size(?TEST_DATA_CHANGED),
    ?assertMatch({ok, #file_attr{size = DataSize}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA_CHANGED},
        lfm_proxy:read(W1, Handle3, 0, DataSize)),
    lfm_proxy:close(W1, Handle3),

    ?assertMatch({ok, #file_attr{size = DataSize}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle4} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA_CHANGED},
        lfm_proxy:read(W2, Handle4, 0, 100), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle4),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

change_file_content_the_same_moment_when_sync_performs_stat_on_file_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    HostStorageTestFilePath = storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

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

    %% Check if file is visible on 2nd provider
    {ok, #file_attr{guid = Guid}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle2),

    Uuid = file_id:guid_to_uuid(Guid),

    %% modify file content and change mtime to time of last stat by sync
    Uuid = file_id:guid_to_uuid(Guid),
    StorageFileId = to_storage_file_id(StorageTestFilePath, W1MountPoint),
    StatTime = get_last_stat_timestamp(W1, StorageFileId, ?SPACE_ID),
    %pretend that there were 2 modifications at the same time and that the second
    %was after sync performed stat on the file
    {ok, _} = rpc:call(W1, storage_sync_info, create_or_update,
        [StorageTestFilePath, ?SPACE_ID, fun(SSI) -> {ok, SSI#storage_sync_info{last_stat = StatTime}} end]),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, ?CHANGED_BYTE_OFFSET, ?CHANGED_BYTE),
    change_time(HostStorageTestFilePath, StatTime),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    %% Check if file was updated
    DataSize = byte_size(?TEST_DATA),
    ?assertMatch({ok, #file_attr{size = DataSize}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA_ONE_BYTE_CHANGED},
        lfm_proxy:read(W1, Handle3, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle3),

    ?assertMatch({ok, #file_attr{size = DataSize}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle4} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA_ONE_BYTE_CHANGED},
        lfm_proxy:read(W2, Handle4, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle4),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

chmod_file_update_test(Config, MountSpaceInRoot) ->
    [W1, W2] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),

    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFileinDirPath1 = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

    NewMode = 8#600,
    %% Create dirs on storage
    DirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, DirSDHandle, 8#775),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported
    {ok, #file_attr{guid = TestDirGuid1}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{mode = 8#664}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    %% Replicate file to second provider
    {ok, #file_attr{guid = TestDirGuid1}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{mode = 8#664}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W2, Handle2),


    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"imported">> => 2,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 2,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 2,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    test_utils:mock_new(W1, storage_sync_hash, [passthrough]),

    timer:sleep(timer:seconds(2)), %ensure that modify time is different from read time
    %% Change file permissions
    sd_test_utils:chmod(W1, SDHandle, NewMode),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    %% Check if file permissions were changed
    ?assertMatch({ok, #file_attr{mode = NewMode}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    History = rpc:call(W1, meck, history, [storage_sync_hash]),
    assert_num_results_gte(History, ?assertHashChangedFun(StorageTestDirPath, ?SPACE_ID, true), 1),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 2,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 2,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

chmod_file_update2_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),

    Files = lists:map(fun(TestFile) ->
        filename:join([?TEST_DIR, TestFile])
    end, [?TEST_FILE1, ?TEST_FILE2, ?TEST_FILE3]),

    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 = storage_path(?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),

    [StTestFile1 | _] = StorageFiles = to_storage_files(Files, ?SPACE_ID, MountSpaceInRoot),
    NewMode = 8#600,

    %% Create files on storage
    DirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, DirSDHandle, 8#775),
    DirSDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath2, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, DirSDHandle2, 8#775),
    lists:foreach(fun(StorageTestFilePath) ->
        SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
        ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
        {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA)
    end, StorageFiles),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),

    %% Check if files were imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    lists:foreach(fun(SpaceFile) ->
        ?assertMatch({ok, #file_attr{mode = 8#664}},
            lfm_proxy:stat(W1, SessId, {path, SpaceFile}), ?ATTEMPTS),
        {ok, Handle1} = ?assertMatch({ok, _},
            lfm_proxy:open(W1, SessId, {path, SpaceFile}, read)),
        ?assertMatch({ok, ?TEST_DATA},
            lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
        lfm_proxy:close(W1, Handle1)
    end, [?SPACE_TEST_FILE_IN_DIR_PATH, ?SPACE_TEST_FILE_IN_DIR_PATH2, ?SPACE_TEST_FILE_IN_DIR_PATH3]),

    test_utils:mock_new(W1, storage_sync_hash, [passthrough]),
    test_utils:mock_new(W1, storage_sync_traverse, [passthrough]),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 8,
        <<"imported">> => 5,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"importedSum">> => 5,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 5,
        <<"importedHourHist">> => 5,
        <<"importedDayHist">> => 5,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Change file permissions
    timer:sleep(timer:seconds(2)),
    SDFileHandle1 = sd_test_utils:new_handle(W1, ?SPACE_ID, StTestFile1, RDWRStorage),
    sd_test_utils:chmod(W1, SDFileHandle1, NewMode),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    %% Check if file permissions were changed
    ?assertMatch({ok, #file_attr{mode = NewMode}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    History = rpc:call(W1, meck, history, [storage_sync_hash]),
    History2 = rpc:call(W1, meck, history, [storage_sync_traverse]),
    test_utils:mock_unload(W1, storage_sync_hash),

    assert_num_results_gte(History, ?assertHashChangedFun(StorageTestDirPath, ?SPACE_ID, true), 1),
    assert_num_results(History2, ?assertMtimeChangedFun(StorageTestDirPath, ?SPACE_ID, true), 0),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 5,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 5,
        <<"importedHourHist">> => 5,
        <<"importedDayHist">> => 5,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

change_file_type_test(Config, MountSpaceInRoot) ->
    % this test checks whether sync properly handles
    % deleting file and creating directory with the same name on storage
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SyncedStorage = get_synced_storage(Config, W1),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),

    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported on W1
    {ok, #file_attr{guid = FileGuid}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

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

    ok = sd_test_utils:unlink(W1, SDHandle, ?TEST_DATA_SIZE),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#755),
    % create file in the directory on storage
    StorageTestFileinDirPath = filename:join([StorageTestFilePath, ?TEST_FILE2]),
    SpaceTestFileinDirPath = filename:join([?SPACE_TEST_FILE_PATH1, ?TEST_FILE2]),
    SDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle2, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle2, 0, ?TEST_DATA2),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"imported">> => 2,
        <<"updated">> => 1,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 3,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 1,
        <<"importedMinHist">> => 2,
        <<"importedHourHist">> => 3,
        <<"importedDayHist">> => 3,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1
    }, ?SPACE_ID),

    % check whether directory has been imported
    ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1})),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {guid, FileGuid})),
    {ok, #file_attr{guid = DirGuid}} = ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W2, SessId2, {guid, FileGuid}), ?ATTEMPTS),

    % check whether file inside directory has been imported
    {ok, #file_attr{guid = FileGuid2}} = ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
        lfm_proxy:stat(W2, SessId2, {path, SpaceTestFileinDirPath}), ?ATTEMPTS),
    ?assertMatch({ok, [{FileGuid2, ?TEST_FILE2}]},
        lfm_proxy:get_children(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, 0, 10), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {path, SpaceTestFileinDirPath}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA2}, lfm_proxy:read(W2, Handle2, 0, ?TEST_DATA_SIZE2), ?ATTEMPTS),

    % check whether we can create directory in the imported directory
    {ok, DirGuid2} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, ?ROOT_SESS_ID, DirGuid, ?TEST_DIR, 8#664)),

    ?assertMatch({ok, _}, lfm_proxy:stat(W1, SessId, {guid, DirGuid2}), ?ATTEMPTS).

change_file_type2_test(Config, MountSpaceInRoot) ->
    % this test checks whether sync properly handles
    % deleting empty directory and creating file with the same name on storage
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SyncedStorage = get_synced_storage(Config, W1),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),

    %% Create dir on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#755),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if dir was imported on W1
    {ok, #file_attr{guid = DirGuid}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, []},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 1)),

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

    ok = sd_test_utils:rmdir(W1, SDHandle),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"imported">> => 1,
        <<"updated">> => 1,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 2,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 1,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1
    }, ?SPACE_ID),

    ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH})),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {guid, DirGuid})),
    ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W2, SessId2, {guid, DirGuid}), ?ATTEMPTS),

    % check whether we can read the imported file
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W1, Handle, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ok = lfm_proxy:close(W1, Handle),

    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ok = lfm_proxy:close(W2, Handle2).

change_file_type3_test(Config, MountSpaceInRoot) ->
    % this test checks whether sync properly handles
    % deleting non-empty directory and creating file with the same name on storage
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    SyncedStorage = get_synced_storage(Config, W1),
    RDWRStorage = get_rdwr_storage(Config, W1),

    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFileInDirPath = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

    %% Create dir on storage and file inside it
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    DirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    FileSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileInDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, DirSDHandle, 8#755),
    ok = sd_test_utils:create_file(W1, FileSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileSDHandle, 0, ?TEST_DATA),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if dir was imported on W1
    {ok, #file_attr{guid = DirGuid}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    {ok, [{FileGuid, _}]} = ?assertMatch({ok, [{_, _}]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 1)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"imported">> => 2,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 2,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 2,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),


    ok = sd_test_utils:unlink(W1, FileSDHandle, ?TEST_DATA_SIZE),
    ok = sd_test_utils:rmdir(W1, DirSDHandle),
    ok = sd_test_utils:create_file(W1, DirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, DirSDHandle, 0, ?TEST_DATA),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"imported">> => 1,
        <<"updated">> => 1,
        <<"deleted">> => 2,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 3,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 2,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 3,
        <<"importedDayHist">> => 3,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 2,
        <<"deletedHourHist">> => 2,
        <<"deletedDayHist">> => 2
    }, ?SPACE_ID),

    ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH})),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {guid, DirGuid})),
    ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W2, SessId2, {guid, DirGuid}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W2, SessId2, {guid, FileGuid}), ?ATTEMPTS),

    % check whether we can read the imported file
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W1, Handle, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ok = lfm_proxy:close(W1, Handle),

    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ok = lfm_proxy:close(W2, Handle2).

change_file_type4_test(Config, MountSpaceInRoot) ->
    % this test checks whether sync properly handles
    % deleting non-empty directory, created in remote provider and creating file with the same name on storage
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SyncedStorage = get_synced_storage(Config, W1),

    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFileInDirPath = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

    %% Create dir and file inside it
    {ok, DirGuid} = lfm_proxy:mkdir(W2, SessId2, ?SPACE_TEST_DIR_PATH, 8#777),
    {ok, FileGuid} = lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_IN_DIR_PATH, 8#777),
    {ok, Handle} = lfm_proxy:open(W2, SessId2, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(W2, Handle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W2, Handle),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {guid, DirGuid}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, FileGuid}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),

    % delete directory and file on storage
    DirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    FileSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileInDirPath, RDWRStorage),

    ok = sd_test_utils:unlink(W1, FileSDHandle, ?TEST_DATA_SIZE),
    sd_test_utils:rmdir(W1, DirSDHandle),
    % create file with the same name like the deleted directory
    ok = sd_test_utils:create_file(W1, DirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, DirSDHandle, 0, ?TEST_DATA),

    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 4,
        <<"imported">> => 1,
        <<"updated">> => 1,
        <<"deleted">> => 2,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 1,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 2,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 2,
        <<"deletedHourHist">> => 2,
        <<"deletedDayHist">> => 2
    }, ?SPACE_ID),

    ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH})),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {guid, DirGuid})),
    ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W2, SessId2, {guid, DirGuid}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W2, SessId2, {guid, FileGuid}), ?ATTEMPTS),

    % check whether we can read the imported file
    {ok, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W1, Handle3, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ok = lfm_proxy:close(W1, Handle3),

    {ok, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W2, Handle4, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ok = lfm_proxy:close(W2, Handle4).

update_timestamps_file_import_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

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

    timer:sleep(timer:seconds(2)),
    %% Change file permissions
    NewTimestamp = 9999999999,
    change_time(StorageTestFilePath, NewTimestamp, NewTimestamp),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID, 2, ?ATTEMPTS),
    disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if timestamps were changed
    ?assertMatch({ok, #file_attr{atime = NewTimestamp, mtime = NewTimestamp}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS).

should_not_detect_timestamp_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID, ?ATTEMPTS),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

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

    %% Change file permissions
    change_time(StorageTestFilePath, 1, 1),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID, 2, ?ATTEMPTS),
    disable_update(Config),

    %% Check if timestamps hasn't changed
    ?assertNotMatch({ok, #file_attr{atime = 1, mtime = 1}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1})),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 1,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

update_nfs_acl_test(Config, MountSpaceInRoot) ->
    Workers = [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER2, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = get_rdwr_storage(Config, W1),

    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID, ?ATTEMPTS),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

    %% User1 should be allowed to read acl
    {ok, #xattr{value = Value}} = ?assertMatch({ok, #xattr{}},
        lfm_proxy:get_xattr(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, <<"cdmi_acl">>)),
    ?assertMatch(Value, ?ACL_JSON),

    %% User1 should not be allowed to set acl
    ?assertMatch({error, ?EACCES},
        lfm_proxy:set_xattr(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, #xattr{})),

    %% User2 should be allowed to read acl
    {ok, #xattr{value = Value}} = ?assertMatch({ok, #xattr{}},
        lfm_proxy:get_xattr(W1, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, <<"cdmi_acl">>)),

    %% User2 should not be allowed to set acl
    ?assertMatch({error, ?EACCES},
        lfm_proxy:set_xattr(W1, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, #xattr{})),

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

    EncACL = storage_sync_acl:encode(?ACL2),
    ok = test_utils:mock_expect(Workers, storage_driver, getxattr, fun
        (Handle = #sd_handle{file = <<"/">>}, Name) ->
            meck:passthrough([Handle, Name]);
        (Handle = #sd_handle{file = <<"/space1">>}, Name) ->
            meck:passthrough([Handle, Name]);
        (#sd_handle{}, _) ->
            {ok, EncACL}
    end),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID, 2, ?ATTEMPTS),
    disable_update(Config),

    %% User1 should not be allowed to read acl
    ?assertMatch({error, ?EACCES},
        lfm_proxy:get_xattr(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, <<"cdmi_acl">>), ?ATTEMPTS),

    %% User2 should be allowed to read acl
    {ok, #xattr{value = Value2}} = ?assertMatch({ok, #xattr{}},
        lfm_proxy:get_xattr(W1, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, <<"cdmi_acl">>)),
    ?assertMatch(Value2, ?ACL2_JSON),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

recreate_file_deleted_by_sync_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),

    {ok, FileGuid} =
        ?assertMatch({ok, _}, lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH1, 8#777)),
    {ok, FileHandle} =
        ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, FileGuid}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W2, FileHandle, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:fsync(W2, FileHandle)),
    ok = lfm_proxy:close(W2, FileHandle),

    %% Replicate file to W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W1, Handle1),

    assertImportTimes(W1, ?SPACE_ID),

    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),

    % ensure that dir mtime will change
    timer:sleep(timer:seconds(1)),

    %% delete file on storage
    ok = sd_test_utils:unlink(W1, SDHandle, ?TEST_DATA_SIZE),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    %% Check if file disappeared
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

    % recreate file
    {ok, FileGuid2} =
        ?assertMatch({ok, _}, lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH1, 8#777)),
    {ok, FileHandle2} =
        ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, FileGuid2}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W2, FileHandle2, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:fsync(W2, FileHandle2)),
    ok = lfm_proxy:close(W2, FileHandle2),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W1, Handle2).

sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),

    {ok, FileGuid} =
        ?assertMatch({ok, _}, lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH1, 8#777)),
    {ok, FileHandle} =
        ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, FileGuid}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W2, FileHandle, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:fsync(W2, FileHandle)),
    ok = lfm_proxy:close(W2, FileHandle),

    assertImportTimes(W1, ?SPACE_ID),

    %check if file_meta was synced
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

    %% Create file on storage to trigger update
    StorageRandomFilePath = storage_path(?SPACE_ID, <<"random_file">>, MountSpaceInRoot),
    RandomSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageRandomFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, RandomSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, RandomSDHandle, 0, ?TEST_DATA),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    %% wait until updates finishes
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),

    %% file shouldn't appear on W1's storage
    ?assertMatch({error, ?ENOENT}, sd_test_utils:stat(W1, SDHandle)),

    % file shouldn't disappear from space
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W1, Handle2),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS).

sync_should_not_delete_dir_created_in_remote_provider(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    RDWRStorage = get_rdwr_storage(Config, W1),

    % Create dir in space
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId2, ?SPACE_TEST_DIR_PATH)),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

    assertImportTimes(W1, ?SPACE_ID),

    %% Create file on storage to trigger update
    StorageRandomFilePath = storage_path(?SPACE_ID, <<"random_file">>, MountSpaceInRoot),
    RandomSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageRandomFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, RandomSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, RandomSDHandle, 0, ?TEST_DATA),
    enable_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),

    %% file shouldn't appear on W1's storage
    ?assertMatch({error, ?ENOENT}, sd_test_utils:stat(W1, SDHandle)),

    % Ensure that sync didn't delete remotely create directory
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS).

sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),

    % Create dir in space
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId2, ?SPACE_TEST_DIR_PATH)),
    {ok, FileGuid} =
        ?assertMatch({ok, _}, lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_IN_DIR_PATH, 8#777)),
    {ok, FileHandle} =
        ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, FileGuid}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W2, FileHandle, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:fsync(W2, FileHandle)),
    ok = lfm_proxy:close(W2, FileHandle),

    assertImportTimes(W1, ?SPACE_ID),

    %check if file_meta was synced
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),

    %% Create file on storage to trigger update
    StorageRandomFilePath =
        storage_path(?SPACE_ID, <<"random_file">>, MountSpaceInRoot),
    RandomSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageRandomFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, RandomSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, RandomSDHandle, 0, ?TEST_DATA),

    enable_update(Config, ?SPACE_ID, SyncedStorage),
    %% wait until updates finishes
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_update(Config),

    StorageTestFilePath = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),

    %% file shouldn't appear on W1's storage
    ?assertMatch({error, ?ENOENT}, sd_test_utils:stat(W1, SDHandle)),

    % file shouldn't disappear from space
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W1, Handle2),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS).

should_not_sync_file_during_replication(Config) ->
    % storage sync scans are set in init_per_testcase to be executed with interval of 1 seconds
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),

    {ok, FileGuid} =
        ?assertMatch({ok, _}, lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH1, 8#777)),

    %check if file_meta was synced
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

    {ok, FileHandle} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, FileGuid}, write)),
    ?assertMatch({ok, _}, lfm_proxy:write(W2, FileHandle, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:fsync(W2, FileHandle)),
    ok = lfm_proxy:close(W2, FileHandle),
    SyncedStorage = get_synced_storage(Config, W1),

    ?assertBlocks(W1, SessId, [
        #{
            <<"blocks">> => [[0, ?TEST_DATA_SIZE]],
            <<"providerId">> => ?GET_DOMAIN_BIN(W2),
            <<"totalBlocksSize">> => ?TEST_DATA_SIZE
        }
    ], FileGuid),

    enable_import(Config, ?SPACE_ID, SyncedStorage),
    enable_update(Config, ?SPACE_ID, SyncedStorage),

    % add sleep after creating file by RTransfer, to pretend that replication lasts longer
    % there will be file on storage which should be detected as "in replication"
    ok = test_utils:mock_new(W1, rtransfer_config),
    ok = test_utils:mock_expect(W1, rtransfer_config, open, fun(Guid, Flag) ->
        R = meck:passthrough([Guid, Flag]),
        timer:sleep(timer:seconds(10)),
        R
    end),

    {ok, TransferId} = lfm_proxy:schedule_file_replication(W1, SessId, {guid, FileGuid}, provider_id(W1)),
    ?assertMatch({ok, #document{value = #transfer{replication_status = completed}}},
        rpc:call(W1, transfer, get, [TransferId]), 600),

    % ensure that sync did not invalidate file blocks
    ?assertBlocks(W1, SessId, [
        #{
            <<"blocks">> => [[0, ?TEST_DATA_SIZE]],
            <<"providerId">> => ?GET_DOMAIN_BIN(W1),
            <<"totalBlocksSize">> => ?TEST_DATA_SIZE
        },
        #{
            <<"blocks">> => [[0, ?TEST_DATA_SIZE]],
            <<"providerId">> => ?GET_DOMAIN_BIN(W2),
            <<"totalBlocksSize">> => ?TEST_DATA_SIZE
        }
    ], FileGuid),
    ?assertBlocks(W2, SessId2, [
        #{
            <<"blocks">> => [[0, ?TEST_DATA_SIZE]],
            <<"providerId">> => ?GET_DOMAIN_BIN(W1),
            <<"totalBlocksSize">> => ?TEST_DATA_SIZE
        },
        #{
            <<"blocks">> => [[0, ?TEST_DATA_SIZE]],
            <<"providerId">> => ?GET_DOMAIN_BIN(W2),
            <<"totalBlocksSize">> => ?TEST_DATA_SIZE
        }
    ], FileGuid),
    FileSize = ?TEST_DATA_SIZE,
    ?assertMatch({ok, #file_attr{size = FileSize}}, lfm_proxy:stat(W2, SessId2, {guid, FileGuid})).

sync_should_not_invalidate_file_after_replication(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),

    {ok, FileGuid} = lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH1, 8#664),
    {ok, Handle} = lfm_proxy:open(W2, SessId2, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(W2, Handle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W2, Handle),

    %% Check if file was synchronized to W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

    % replicate file to W1
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),

    SyncedStorage = get_synced_storage(Config, W1),
    enable_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 2,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 0,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedMinHist">> => 2,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    % wait to ensure that file_location docs are synchronized
    timer:sleep(timer:seconds(10)),

    ?assertBlocks(W2, SessId2, [
        #{
            <<"blocks">> => [[0, 9]],
            <<"providerId">> => ?GET_DOMAIN_BIN(W1),
            <<"totalBlocksSize">> => 9
        },
        #{
            <<"blocks">> => [[0, 9]],
            <<"providerId">> => ?GET_DOMAIN_BIN(W2),
            <<"totalBlocksSize">> => 9
        }
    ], FileGuid).

%%%===================================================================
%%% Util functions
%%%===================================================================

to_storage_file_id(Path, MountPoint) ->
    PathSplit = binary:split(Path, <<"/">>, [global]),
    MountPointSplit = binary:split(MountPoint, <<"/">>, [global]),
    StorageFileIdSplit = PathSplit -- MountPointSplit,
    filename:join(["/" | StorageFileIdSplit]).

create_init_file(Config, Readonly) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SpaceDir = storage_path(?SPACE_ID, <<"">>, Readonly),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, SpaceDir, RDWRStorage),
    case sd_test_utils:mkdir(W1, SDHandle, 8#777) of
        ok -> ok;
        {error, eexist} -> is_empty(W1, SDHandle)
    end.

is_empty(Worker, SDHandle = #sd_handle{storage_id = StorageId}) ->
    Helper = rpc:call(Worker, storage, get_helper, [StorageId]),
    HelperName = helper:get_name(Helper),
    ?assertMatch({ok, []},
        sd_test_utils:storage_ls(Worker, SDHandle, 0, 1, HelperName), ?ATTEMPTS).

cancel(Worker, SpaceId, StorageId) when is_binary(StorageId) ->
    ?assertMatch(ok, rpc:call(Worker, storage_sync, cancel, [SpaceId, StorageId]));
cancel(Worker, SpaceId, Storage) ->
    StorageId = storage:get_id(Storage),
    cancel(Worker, SpaceId, StorageId).

schedule_spaces_check(Worker, IntervalSeconds) ->
    rpc:call(Worker, storage_sync_worker, schedule_spaces_check, [IntervalSeconds]).

enable_import(Config, SpaceId, StorageId) when is_binary(StorageId) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    ImportConfig = ?config(import_config, Config, #{}),
    MaxDepth = maps:get(max_depth, ImportConfig, ?MAX_DEPTH),
    SyncAcl = maps:get(sync_acl, ImportConfig, ?SYNC_ACL),
    ?assertMatch(ok, rpc:call(W1, storage_sync, enable_import,
        [SpaceId, StorageId, #{max_depth => MaxDepth, sync_acl => SyncAcl}]));
enable_import(Config, SpaceId, Storage) ->
    StorageId = storage:get_id(Storage),
    enable_import(Config, SpaceId, StorageId).

enable_update(Config, SpaceId, StorageId) when is_binary(StorageId) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    UpdateConfig = ?config(update_config, Config, #{}),
    MaxDepth = maps:get(max_depth, UpdateConfig, ?MAX_DEPTH),
    ScanInterval = maps:get(scan_interval, UpdateConfig, ?SCAN_INTERVAL),
    WriteOnce = maps:get(write_once, UpdateConfig, ?WRITE_ONCE),
    DeleteEnable = maps:get(delete_enable, UpdateConfig, ?DELETE_ENABLE),
    SyncAcl = maps:get(sync_acl, UpdateConfig, ?SYNC_ACL),
    ok = rpc:call(W1, storage_sync, configure_update,
        [SpaceId, StorageId, true, #{
            max_depth => MaxDepth,
            scan_interval => ScanInterval,
            write_once => WriteOnce,
            delete_enable => DeleteEnable,
            sync_acl => SyncAcl
        }]);
enable_update(Config, SpaceId, Storage) ->
    StorageId = storage:get_id(Storage),
    enable_update(Config, SpaceId, StorageId).

enable_update(Config, SpaceId, StorageId, Opts) when is_binary(StorageId) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    UpdateConfig = ?config(update_config, Config, #{}),
    MaxDepth = maps:get(max_depth, UpdateConfig, ?MAX_DEPTH),
    ScanInterval = maps:get(scan_interval, UpdateConfig, ?SCAN_INTERVAL),
    WriteOnce = maps:get(write_once, UpdateConfig, ?WRITE_ONCE),
    DeleteEnable = maps:get(delete_enable, UpdateConfig, ?DELETE_ENABLE),
    SyncAcl = maps:get(sync_acl, UpdateConfig, ?SYNC_ACL),
    DefaultOpts = #{
        max_depth => MaxDepth,
        scan_interval => ScanInterval,
        write_once => WriteOnce,
        delete_enable => DeleteEnable,
        sync_acl => SyncAcl
    },
    ok = rpc:call(W1, storage_sync, configure_update, [SpaceId, StorageId, true, maps:merge(DefaultOpts, Opts)]);
enable_update(Config, SpaceId, Storage, Opts) ->
    StorageId = storage:get_id(Storage),
    enable_update(Config, SpaceId, StorageId, Opts).

disable_import(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    StorageId = initializer:get_supporting_storage_id(W1, ?SPACE_ID),
    ok = rpc:call(W1, storage_sync, disable_import, [?SPACE_ID, StorageId]),
    ?assertMatch({false, _}, get_import_details(W1, ?SPACE_ID, StorageId), ?ATTEMPTS),
    assertNoImportInProgress(W1, ?SPACE_ID, 600),
    ok.

cleanup_storage_sync_monitoring_model(Worker, SpaceId) ->
    StorageId = initializer:get_supporting_storage_id(Worker, SpaceId),
    rpc:call(Worker, storage_sync_monitoring, delete, [SpaceId, StorageId]).

get_import_details(Worker, SpaceId, StorageId) ->
    rpc:call(Worker, storage_sync, get_import_details, [SpaceId, StorageId]).

get_update_details(Worker, SpaceId, StorageId) ->
    rpc:call(Worker, storage_sync, get_update_details, [SpaceId, StorageId]).

disable_update(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    StorageId = initializer:get_supporting_storage_id(W1, ?SPACE_ID),
    rpc:call(W1, storage_sync, disable_update, [?SPACE_ID, StorageId]),
    ?assertMatch({false, _}, get_update_details(W1, ?SPACE_ID, StorageId), ?ATTEMPTS),
    assertNoUpdateInProgress(W1, ?SPACE_ID, 600),
    ok.

disable_storage_sync(Config) ->
    disable_import(Config),
    disable_update(Config).

clean_synced_storage(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    Storage = get_rdwr_storage(Config, W1),
    SpaceDir = storage_path(?SPACE_ID, <<"">>, true),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, SpaceDir, Storage),
    ok = sd_test_utils:recursive_rm(W1, SDHandle, true).

clean_space(Config) ->
    [W, W2 | _] = ?config(op_worker_nodes, Config),
    SpaceGuid = rpc:call(W, fslogic_uuid, spaceid_to_space_dir_guid, [?SPACE_ID]),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W)}}, Config),
    close_opened_files(W, SessId),
    {ok, Children} = lfm_proxy:get_children(W, ?ROOT_SESS_ID, {guid, SpaceGuid}, 0, 10000),
    Attempts = 5 * ?ATTEMPTS,
    Self = self(),

    % hacky way to remove file, but ignore errors from storage
    % files will be deleted on storage in other function
    ok = test_utils:mock_new(W, helpers),
    test_utils:mock_expect(W, helpers, unlink, fun(HelperHandle, FileId, CurrentSize) ->
        case meck:passthrough([HelperHandle, FileId, CurrentSize]) of
            {error, ?EROFS} -> ok;
            Other -> Other
        end
    end),
    test_utils:mock_expect(W, helpers, rmdir, fun(HelperHandle, FileId) ->
        case meck:passthrough([HelperHandle, FileId]) of
            {error, ?EROFS} -> ok;
            Other -> Other
        end
    end),
    Guids = lists:map(fun({Guid, _}) ->
        ok = lfm_proxy:rm_recursive(W, ?ROOT_SESS_ID, {guid, Guid}),
        ok = worker_pool:cast(?VERIFY_POOL, {?MODULE, verify_file_deleted, [W2, Guid, Self, Attempts]}),
        Guid
    end, Children),
    test_utils:mock_unload(W, helpers),
    verify_deletions(Guids, Attempts),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W, ?ROOT_SESS_ID, {guid, SpaceGuid}, 0, 10000), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W2, ?ROOT_SESS_ID, {guid, SpaceGuid}, 0, 10000), ?ATTEMPTS).

close_opened_files(Worker, SessionId) ->
    {ok, Handles} = rpc:call(Worker, file_handles, list, []),
    lists:foreach(fun(#document{key = Uuid}) ->
        Guid = file_id:pack_guid(Uuid, ?SPACE_ID),
        FileCtx = rpc:call(Worker, file_ctx, new_by_guid, [Guid]),
        ok = rpc:call(Worker, file_handles, register_release, [FileCtx, SessionId, infinity])
    end, Handles).


verify_deletions(Guids, Timeout) ->
    verify_deletions(Guids, [], Timeout).

verify_deletions([], [], _) ->
    ok;
verify_deletions([], _FailedToVerifyGuids, _) ->
    ct:fail("Cleaning space failed");
verify_deletions(FileGuids, FailedToVerifyGuids, Timeout) ->
    receive
        {deleted, FileGuid} ->
            verify_deletions(FileGuids -- [FileGuid], FailedToVerifyGuids, Timeout);
        {deleting_failed, FileGuid} ->
            verify_deletions(FileGuids, [FileGuid | FailedToVerifyGuids], Timeout)
    after
        timer:seconds(Timeout) ->
            ct:fail("Cleaning space failed")
    end.

verify_file_deleted(Worker, FileGuid, Master, Attempts) ->
    try
        ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(Worker, ?ROOT_SESS_ID, {guid, FileGuid}), Attempts),
        Master ! {deleted, FileGuid}
    catch
        _:_ ->
            Master ! {deleting_failed, FileGuid}
    end.

clean_luma_db(Worker) ->
    {ok, StorageIds} = rpc:call(Worker, provider_logic, get_storage_ids, []),
    lists:foreach(fun(StorageId) ->
        ok = rpc:call(Worker, luma, clear_db, [StorageId])
    end, StorageIds).


add_synced_storages(Config) ->
    SyncedStorages = add_storages(Config, fun is_synced/2),
    [{synced_storages, SyncedStorages} | Config].

add_rdwr_storages(Config) ->
    RDWRStorages = add_storages(Config, fun is_rdwr/2),
    [{rdwr_storages, RDWRStorages} | Config].


add_storages(Config, CheckStorageFun) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foldl(fun(W, AccIn) ->
        {ok, StorageIds} = rpc:call(W, provider_logic, get_storage_ids, []),
        Storages = lists:map(fun(StorageId) ->
            {ok, Storage} = rpc:call(W, storage, get, [StorageId]),
            Storage
        end, StorageIds),
        case find_storage(Storages, CheckStorageFun) of
            undefined -> AccIn;
            FoundStorage -> AccIn#{W => FoundStorage}
        end
    end, #{}, Workers).

find_storage(Storages, CheckStorageFun) ->
    lists:foldl(fun
        (Storage, undefined) ->
            Helper = storage:get_helper(Storage),
            Id = storage:get_id(Storage),
            case CheckStorageFun(Id, Helper) of
                true -> Storage;
                false -> undefined
            end;
        (_, FoundStorage) ->
            FoundStorage
    end, undefined, Storages).

is_rdwr(MountPoint, #helper{name = ?POSIX_HELPER_NAME}) ->
    <<"rdwr_storage">> =:= filename:basename(MountPoint);
is_rdwr(<<"rdwr_storage">>, #helper{name = ?S3_HELPER_NAME}) ->
    true;
is_rdwr(_, _) ->
    false.

is_synced(MountPoint, #helper{name = ?POSIX_HELPER_NAME}) ->
    <<"synced_storage">> =:= filename:basename(MountPoint);
is_synced(<<"synced_storage">>, #helper{name = ?S3_HELPER_NAME}) ->
    true.


get_rdwr_storage(Config, Worker) ->
    case maps:get(Worker, ?config(rdwr_storages, Config), undefined) of
        undefined -> get_synced_storage(Config, Worker);
        Storage -> Storage
    end.

get_synced_storage(Config, Worker) ->
    maps:get(Worker, ?config(synced_storages, Config), undefined).

get_supporting_storage(Worker, SpaceId) ->
    StorageId = initializer:get_supporting_storage_id(Worker, SpaceId),
    {ok, Storage} = sd_test_utils:get_storage_record(Worker, StorageId),
    Storage.

get_mount_point(Storage) ->
    % works only on POSIX storages!!!
    Helper = storage:get_helper(Storage),
    HelperArgs = helper:get_args(Helper),
    maps:get(<<"mountPoint">>, HelperArgs).

get_host_mount_point(Config, Storage) ->
    % works only on POSIX storages!!!
    MountPoint = get_mount_point(Storage),
    get_storage_path(Config, MountPoint).

get_host_storage_file_id(Config, CanonicalPath, Storage, ImportedStorage) ->
    Helper = storage:get_helper(Storage),
    case helper:get_name(Helper) of
        ?POSIX_HELPER_NAME ->
            get_host_posix_storage_file_id(Config, CanonicalPath, Storage, ImportedStorage);
        ?S3_HELPER_NAME ->
            get_host_s3_storage_file_id(CanonicalPath, ImportedStorage)
    end.

get_host_posix_storage_file_id(Config, CanonicalPath, Storage, true) ->
    StoragePath = get_host_mount_point(Config, Storage),
    [_Root, _SpaceId | Rest] = fslogic_path:split(CanonicalPath),
    filename:join([StoragePath | Rest]);
get_host_posix_storage_file_id(Config, CanonicalPath, Storage, false) ->
    StoragePath = get_host_mount_point(Config, Storage),
    [_Root, SpaceId | Rest] = fslogic_path:split(CanonicalPath),
    filename:join([StoragePath, SpaceId | Rest]).

get_host_s3_storage_file_id(CanonicalPath, true) ->
    [Root, _SpaceId | Rest] = fslogic_path:split(CanonicalPath),
    fslogic_path:join([Root | Rest]);
get_host_s3_storage_file_id(CanonicalPath, false) ->
    CanonicalPath.

get_storage_path(Config, MountPath) when is_list(MountPath) ->
    get_storage_path(Config, list_to_atom(MountPath));
get_storage_path(Config, MountPath) when is_binary(MountPath) ->
    get_storage_path(Config, binary_to_atom(MountPath, latin1));
get_storage_path(Config, MountPath) when is_atom(MountPath) ->
    atom_to_binary(?config(host_path,
        ?config(MountPath,
            ?config(posix,
                ?config(storages, Config)))), latin1).

storage_path(_SpaceId, File, true) ->
    filename:join([<<"/">>, File]);
storage_path(SpaceId, FileName, false) ->
    filename:join([<<"/">>, SpaceId, FileName]).

storage_path(MountPath, _SpaceId, File, true) ->
    filename:join([MountPath, File]);
storage_path(MountPath, SpaceId, FileName, false) ->
    filename:join([MountPath, SpaceId, FileName]).

touch(Node, FilePath) ->
    ok = rpc:call(Node, file, write_file_info, [FilePath, #file_info{}]).

change_time(FilePath, Mtime) ->
    file:write_file_info(FilePath,
        #file_info{mtime = Mtime}, [{time, posix}]).

change_time(FilePath, Atime, Mtime) ->
    ok = file:write_file_info(FilePath,
        #file_info{atime = Atime, mtime = Mtime}, [{time, posix}]).

assert_num_results_gte(History, AssertionFun, ExpectedResultsNum) ->
    ResultsNum = lists:foldl(fun(E, AccIn) ->
        AccIn + AssertionFun(E)
    end, 0, History),
    ?assert(ExpectedResultsNum =< ResultsNum).

assert_num_results(History, AssertionFun, ExpectedResultsNum) ->
    ResultsNum = lists:foldl(fun(E, AccIn) ->
        AccIn + AssertionFun(E)
    end, 0, History),
    ?assertEqual(ExpectedResultsNum, ResultsNum).

to_storage_files(Files, SpaceId, MountSpaceInRoot) ->
    [storage_path(SpaceId, F, MountSpaceInRoot) || F <- Files].

parallel_assert(M, F, A, List, Attempts) ->
    lists:foreach(fun(N) ->
        spawn_link(M, F, [N, self() | A])
    end, List),

    lists:foldl(fun(_, AccIn) ->
        case sets:size(AccIn) of
            0 -> ok;
            _ ->
                receive
                    {finished, Ans} ->
                        sets:del_element(Ans, AccIn)
                after
                    Attempts * timer:seconds(1) ->
                        ct:pal("Left = ~p", [lists:sort(sets:to_list(AccIn))]),
                        Acc = lists:sort(sets:to_list(AccIn)),
                        ?assertMatch(Acc, [])
                end
        end
    end, sets:from_list([str_utils:to_binary(E) || E <- List]), List).


verify_dir(N, Pid, W1, SessId, Attempts) ->
    NBin = integer_to_binary(N),
    DirPath = ?SPACE_TEST_DIR_PATH(NBin),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, DirPath}), Attempts),
    Pid ! {finished, DirPath}.

verify_file(FilePath, Pid, W1, SessId, Attempts) ->
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, FilePath}), Attempts),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, FilePath}, read), Attempts),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA)), Attempts),
    lfm_proxy:close(W1, Handle1),
    Pid ! {finished, FilePath}.

verify_file_in_dir(N, Pid, W1, SessId, Attempts) ->
    NBin = integer_to_binary(N),
    FileInDirPath = ?SPACE_TEST_FILE_IN_DIR_PATH(NBin, NBin),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, FileInDirPath}), Attempts),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, FileInDirPath}, read), Attempts),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA)), Attempts),
    lfm_proxy:close(W1, Handle1),
    Pid ! {finished, NBin}.

verify_file_deleted(FilePath, Pid, Worker, SessId, Attempts) ->
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(Worker, SessId, {path, FilePath}), Attempts),
    Pid ! {finished, FilePath}.

generate_nested_directory_tree_file_paths([SubFilesNum], Root) ->
    lists:map(fun(N) ->
        filename:join([Root, integer_to_binary(N)])
    end, lists:seq(1, SubFilesNum));
generate_nested_directory_tree_file_paths([SubDirsNum | Rest], Root) ->
    lists:flatmap(fun(N) ->
        NBin = integer_to_binary(N),
        DirPath = filename:join([Root, NBin]),
        generate_nested_directory_tree_file_paths(Rest, DirPath)
    end, lists:seq(1, SubDirsNum)).


create_nested_directory_tree(Worker, [SubFilesNum], RootHandle) ->
    ok = lists:foreach(fun(N) ->
        ChildHandle = sd_test_utils:new_child_handle(RootHandle, integer_to_binary(N)),
        ok = sd_test_utils:create_file(Worker, ChildHandle, 8#664),
        {ok, _} = sd_test_utils:write_file(Worker, ChildHandle, 0, ?TEST_DATA)
    end, lists:seq(1, SubFilesNum));
create_nested_directory_tree(Worker, [SubDirsNum | Rest], RootHandle) ->
    ok = lists:foreach(fun(N) ->
        ChildHandle = sd_test_utils:new_child_handle(RootHandle, integer_to_binary(N)),
        ok = sd_test_utils:mkdir(Worker, ChildHandle, 8#775),
        ok = create_nested_directory_tree(Worker, Rest, ChildHandle)
    end, lists:seq(1, SubDirsNum)).


assertImportTimes(Worker, SpaceId) ->
    assertImportTimes(Worker, SpaceId, ?ATTEMPTS).

assertImportTimes(Worker, SpaceId, Attempts) ->
    StorageId = initializer:get_supporting_storage_id(Worker, ?SPACE_ID),
    ?assertEqual(true, try
        {ok, #document{
            value = #storage_sync_monitoring{
                import_start_time = StartTime,
                import_finish_time = FinishTime
            }}} = rpc:call(Worker, storage_sync_monitoring, get, [SpaceId, StorageId]),
        (StartTime =/= undefined) and (FinishTime =/= undefined) and (StartTime =< FinishTime)
    catch
        _:_ ->
            false
    end, Attempts).

assertUpdateTimes(Worker, SpaceId) ->
    assertUpdateTimes(Worker, SpaceId, 2).

assertUpdateTimes(Worker, SpaceId, ScanNo) ->
    assertUpdateTimes(Worker, SpaceId, ScanNo, ?ATTEMPTS).

assertUpdateTimes(Worker, SpaceId, ScanNo, Attempts) ->
    StorageId = initializer:get_supporting_storage_id(Worker, ?SPACE_ID),
    ?assertEqual(true, try
        {ok, #document{
            value = #storage_sync_monitoring{
                scans = FinishedScansNum,
                last_update_start_time = StartTime,
                last_update_finish_time = FinishTime
        }}} = rpc:call(Worker, storage_sync_monitoring, get, [SpaceId, StorageId]),
        (StartTime =/= undefined) and (FinishTime =/= undefined) and
            (StartTime =< FinishTime) and (FinishedScansNum =:= ScanNo)
    catch
        _:_ ->
            false
    end, Attempts).

assertNoImportInProgress(Worker, SpaceId, Attempts) ->
    StorageId = initializer:get_supporting_storage_id(Worker, ?SPACE_ID),
    ?assertEqual(true, try
        Result = rpc:call(Worker, storage_sync_monitoring, get, [SpaceId, StorageId]),
        case Result of
            {ok, #document{
                value = #storage_sync_monitoring{
                    import_start_time = StartTime,
                    import_finish_time = FinishTime
                }}} ->
                ((StartTime =:= undefined) andalso (FinishTime =:= undefined))
                    orelse ((FinishTime =/= undefined) andalso (StartTime =< FinishTime));
            {error, not_found} ->
                true
        end
    catch
        _:_ ->
            false
    end, Attempts).

assertNoUpdateInProgress(Worker, SpaceId, Attempts) ->
    StorageId = initializer:get_supporting_storage_id(Worker, ?SPACE_ID),
    ?assertEqual(true, try
        Result = rpc:call(Worker, storage_sync_monitoring, get, [SpaceId, StorageId]),
        case Result of
            {ok, #document{
                value = #storage_sync_monitoring{
                    last_update_start_time = StartTime,
                    last_update_finish_time = FinishTime
                }}} ->
                ((StartTime =:= undefined) andalso (FinishTime =:= undefined))
                    orelse ((FinishTime =/= undefined) andalso (StartTime =< FinishTime));
            {error, not_found} ->
                true
        end
    catch
        _:_ ->
            false
    end, Attempts).

uuid(Worker, FileGuid) ->
    rpc:call(Worker, file_id, guid_to_uuid, [FileGuid]).

space_uuid(Worker, SpaceId) ->
    rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_uuid, [SpaceId]).

get_last_stat_timestamp(Worker, FilePath, SpaceId) ->
    {ok, #document{value = #storage_sync_info{last_stat = StatTime}}} =
        rpc:call(Worker, storage_sync_info, get, [FilePath, SpaceId]),
    StatTime.

assert_monitoring_state(Worker, ExpectedSSM, SpaceId, Attempts) ->
    StorageId = initializer:get_supporting_storage_id(Worker, SpaceId),
    SSM = rpc:call(Worker, storage_sync_monitoring, get_info, [SpaceId, StorageId]),
    SSM2 = flatten_histograms(SSM),
    try
        assert(ExpectedSSM, SSM2),
        SSM2
    catch
        throw:{assertion_error, Key, ExpectedValue, Value} ->
            case Attempts == 0 of
                false ->
                    timer:sleep(timer:seconds(1)),
                    assert_monitoring_state(Worker, ExpectedSSM, SpaceId, Attempts - 1);
                true ->
                    {Format, Args} = storage_sync_monitoring_description(SSM),
                    ct:pal(
                        "Assertion of field \"~p\" in storage_sync_monitoring for space ~p and storage ~p failed.~n"
                        "    Expected: ~p~n"
                        "    Value: ~p~n"
                        ++ Format ++
                        "~nStacktrace:~n~p",
                        [Key, SpaceId, StorageId, ExpectedValue, Value] ++ Args ++ [erlang:get_stacktrace()]),
                    ct:fail("assertion failed")
            end
    end.

assert(ExpectedSSM, SSM) ->
    maps:fold(fun(Key, Value, _AccIn) ->
        assert_for_key(Key, Value, SSM)
    end, undefined, ExpectedSSM).

assert_for_key(Key, ExpectedValue, SSM) ->
    Value = maps:get(Key, SSM),
    case Value of
        ExpectedValue -> ok;
        _ ->
            throw({assertion_error, Key, ExpectedValue, Value})
    end.

flatten_histograms(SSM) ->
    SSM#{
        % flatten beginnings of histograms for assertions
        <<"importedMinHist">> => lists:sum(lists:sublist(maps:get(<<"importedMinHist">>, SSM), 2)),
        <<"updatedMinHist">> => lists:sum(lists:sublist(maps:get(<<"updatedMinHist">>, SSM), 2)),
        <<"deletedMinHist">> => lists:sum(lists:sublist(maps:get(<<"deletedMinHist">>, SSM), 2)),

        <<"importedHourHist">> => lists:sum(lists:sublist(maps:get(<<"importedHourHist">>, SSM), 3)),
        <<"updatedHourHist">> => lists:sum(lists:sublist(maps:get(<<"updatedHourHist">>, SSM), 3)),
        <<"deletedHourHist">> => lists:sum(lists:sublist(maps:get(<<"deletedHourHist">>, SSM), 3)),

        <<"importedDayHist">> => lists:sum(lists:sublist(maps:get(<<"importedDayHist">>, SSM), 1)),
        <<"updatedDayHist">> => lists:sum(lists:sublist(maps:get(<<"updatedDayHist">>, SSM), 1)),
        <<"deletedDayHist">> => lists:sum(lists:sublist(maps:get(<<"deletedDayHist">>, SSM), 1))
    }.

storage_sync_monitoring_description(SSM) ->
    maps:fold(fun(Key, Value, {AccFormat, AccArgs}) ->
        {AccFormat ++ "    ~p = ~p~n", AccArgs ++ [Key, Value]}
    end, {"~n#storage_sync_monitoring fields values:~n", []}, SSM).

provider_id(Worker) ->
    rpc:call(Worker, oneprovider, get_id, []).

remove_link(Worker, ParentUuid, FileName) ->
    Ctx = rpc:call(Worker, file_meta, get_ctx, []),
    TreeId = provider_id(Worker),
    ok = rpc:call(Worker, datastore_model, delete_links, [Ctx#{scope => ?SPACE_ID}, ParentUuid, TreeId, FileName]).

remove_deletion_link(Worker, SpaceId, FileName, ParentCtx) ->
    ParentUuid = file_ctx:get_uuid_const(ParentCtx),
    {ok, Uuid} = rpc:call(Worker, link_utils, try_to_resolve_child_deletion_link, [FileName, ParentCtx]),
    Guid = file_id:pack_guid(Uuid, SpaceId),
    FileCtx = file_ctx:new_by_guid(Guid),
    rpc:call(Worker, link_utils, remove_deletion_link, [FileCtx, ParentUuid]).

clean_traverse_tasks(Worker) ->
    Pool = <<"storage_sync_traverse">>,
    ?assertMatch({ok, [], _}, rpc:call(Worker, traverse_task_list, list, [Pool, ongoing]), ?ATTEMPTS),
    {ok, TaskIds, _} = rpc:call(Worker, traverse_task_list, list, [Pool, ended]),
    lists:foreach(fun(T) ->
        ok = rpc:call(Worker, traverse_task, delete_ended, [Pool, T])
    end, TaskIds),
    ?assertMatch({ok, [], _}, rpc:call(Worker, traverse_task_list, list, [Pool, ended])).

mock_import_file_error(Worker, ErroneousFile) ->
    ok = test_utils:mock_new(Worker, storage_sync_engine),
    ok = test_utils:mock_expect(Worker, storage_sync_engine, import_file_unsafe,
        fun(StorageFileCtx, Info) ->
            FileName = storage_file_ctx:get_file_name_const(StorageFileCtx),
            case FileName of
                ErroneousFile -> throw(test_error);
                _ -> meck:passthrough([StorageFileCtx, Info])
            end
        end
    ).

unmock_import_file_error(Worker) ->
    ok = test_utils:mock_unload(Worker, storage_sync_engine).

mock_link_handling_method(Workers) ->
    ok = test_utils:mock_new(Workers, fslogic_delete),
    ok = test_utils:mock_expect(Workers, fslogic_delete, get_open_file_handling_method, fun(Ctx) ->
        {deletion_link, Ctx}
    end).

block_syncing_process(TestProcess) ->
    TestProcess ! {syncing_process, self()},
    receive continue -> ok end.

await_syncing_process() ->
    {syncing_process, SyncingProcess} = ?assertReceivedMatch({syncing_process, _}, 60000),
    SyncingProcess.

release_syncing_process(SyncingProcess) ->
    SyncingProcess ! continue.

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        ssl:start(),
        hackney:start(),
        initializer:disable_quota_limit(NewConfig),
        initializer:mock_provider_ids(NewConfig),
        NewConfig2 = multi_provider_file_ops_test_base:init_env(NewConfig),
        [W1 | _] = ?config(op_worker_nodes, NewConfig2),
        rpc:call(W1, storage_sync_worker, notify_connection_to_oz, []),
        NewConfig2
    end,
    {ok, _} = application:ensure_all_started(worker_pool),
    {ok, _} = worker_pool:start_sup_pool(?VERIFY_POOL, [{workers, 8}]),
    [{?LOAD_MODULES, [initializer, storage_sync_test_base, sd_test_utils]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    ok = wpool:stop_sup_pool(?VERIFY_POOL),
    multi_provider_file_ops_test_base:teardown_env(Config).


init_per_testcase(Case, Config)
    when Case =:= create_directory_import_check_user_id_test
    orelse Case =:= create_file_import_check_user_id_test ->

    [W1 | _] = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(W1, [luma]),
    ok = test_utils:mock_expect(W1, luma, map_uid_to_onedata_user, fun(_, _, _) ->
        {ok, ?USER1}
    end),
    ok = test_utils:mock_expect(W1, luma, map_to_display_credentials, fun(_, _, _) ->
        {ok, {?TEST_UID, ?MOUNT_GID}}
    end),
    init_per_testcase(default, Config);

init_per_testcase(Case, Config)
    when Case =:= create_directory_import_check_user_id_error_test
    orelse Case =:= create_file_import_check_user_id_error_test ->

    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Workers, [luma]),
    ok = test_utils:mock_expect(Workers, luma, map_uid_to_onedata_user, fun(_, _, _) ->
        error(test_error)
    end),
    init_per_testcase(default, Config);

init_per_testcase(cancel_scan, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, OldDirBatchSize} = test_utils:get_env(W1, op_worker, storage_sync_dir_batch_size),
    test_utils:set_env(W1, op_worker, storage_sync_dir_batch_size, 1),
    Config2 = [
        {update_config, #{
            delete_enable => false,
            write_once => true}},
        {old_storage_sync_dir_batch_size, OldDirBatchSize}
        | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config)
    when Case =:= create_file_in_dir_update_test
    orelse Case =:= should_not_detect_timestamp_update_test ->

    Config2 = [
        {update_config, #{
            delete_enable => false,
            write_once => true}} | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config)
    when Case =:= delete_empty_directory_update_test
    orelse Case =:= delete_non_empty_directory_update_test
    orelse Case =:= delete_file_update_test
    orelse Case =:= delete_file_in_dir_update_test
    orelse Case =:= delete_many_subfiles_test
    orelse Case =:= move_file_update_test
    orelse Case =:= create_subfiles_and_delete_before_import_is_finished_test ->

    Config2 = [
        {update_config, #{
            delete_enable => true,
            write_once => true}} | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config)
    when Case =:= delete_and_update_files_simultaneously_update_test
    orelse Case =:= create_delete_import2_test
    orelse Case =:= recreate_file_deleted_by_sync_test
    orelse Case =:= create_delete_race_test
    orelse Case =:= sync_should_not_delete_not_replicated_file_created_in_remote_provider
    orelse Case =:= sync_should_not_delete_dir_created_in_remote_provider
    orelse Case =:= sync_should_not_delete_not_replicated_files_created_in_remote_provider2
    orelse Case =:= sync_should_not_invalidate_file_after_replication
    orelse Case =:= sync_works_properly_after_delete_test ->

    Config2 = [
        {update_config, #{
            delete_enable => true,
            write_once => false}} | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(create_file_in_dir_exceed_batch_update_test, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, OldDirBatchSize} = test_utils:get_env(W1, op_worker, storage_sync_dir_batch_size),
    test_utils:set_env(W1, op_worker, storage_sync_dir_batch_size, 2),
    Config2 = [
        {update_config, #{
            delete_enable => false,
            write_once => true}},
        {old_storage_sync_dir_batch_size, OldDirBatchSize}
        | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(chmod_file_update2_test, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, OldDirBatchSize} = test_utils:get_env(W1, op_worker, storage_sync_dir_batch_size),
    test_utils:set_env(W1, op_worker, storage_sync_dir_batch_size, 2),
    Config2 = [{old_storage_sync_dir_batch_size, OldDirBatchSize} | Config],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config)
    when Case =:= import_nfs_acl_test
    orelse Case =:= update_nfs_acl_test ->

    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Workers, [storage_driver, luma]),
    ok = test_utils:mock_expect(Workers, luma, map_uid_to_onedata_user, fun(_, _, _) ->
        {ok, ?USER1}
    end),
    ok = test_utils:mock_expect(Workers, luma, map_acl_user_to_onedata_user, fun(_, _) ->
        {ok, ?USER1}
    end),
    ok = test_utils:mock_expect(Workers, luma, map_acl_group_to_onedata_group, fun(_, _) ->
        {ok, ?GROUP2}
    end),

    EncACL = storage_sync_acl:encode(?ACL),
    ok = test_utils:mock_expect(Workers, storage_driver, getxattr, fun
        (Handle = #sd_handle{file = <<"/space1">>}, Ctx) ->
            meck:passthrough([Handle, Ctx]);
        (#sd_handle{}, _) ->
            {ok, EncACL}
    end),
    init_per_testcase(default, Config);

init_per_testcase(import_nfs_acl_with_disabled_luma_should_fail_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Workers, [storage_driver]),
    EncACL = storage_sync_acl:encode(?ACL),
    ok = test_utils:mock_expect(Workers, storage_driver, getxattr, fun
        (Handle = #sd_handle{file = <<"/space1">>}, Ctx) ->
            meck:passthrough([Handle, Ctx]);
        (#sd_handle{}, _) ->
            {ok, EncACL}
    end),
    init_per_testcase(default, Config);

init_per_testcase(sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage, Config) ->
    % generate random dir name
    TestDir = <<"random_dir", (integer_to_binary(rand:uniform(1000)))/binary>>,
    init_per_testcase(default, [{test_dir, TestDir} | Config]);

init_per_testcase(sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage, Config) ->
    % generate random dir name
    TestFile = <<"random_file", (integer_to_binary(rand:uniform(1000)))/binary>>,
    init_per_testcase(default, [{test_file, TestFile} | Config]);

init_per_testcase(create_list_race_test, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, OldDirBatchSize} = test_utils:get_env(W1, op_worker, storage_sync_dir_batch_size),
    test_utils:set_env(W1, op_worker, storage_sync_dir_batch_size, 2),
    Config2 = [
        {update_config, #{
            delete_enable => true,
            write_once => false}},
        {old_storage_sync_dir_batch_size, OldDirBatchSize}
        | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(should_not_sync_file_during_replication, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, OldInterval} = test_utils:get_env(W1, op_worker, storage_sync_check_interval),
    test_utils:set_env(W1, op_worker, storage_sync_check_interval, 1),
    Config2 = [
        {update_config, #{
            scan_interval => 1,
            delete_enable => false,
            write_once => true}},
        {old_storage_sync_check_interval, OldInterval}
        | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(changing_max_depth_test, Config) ->
    Config2 = [
        {import_config, #{
            max_depth => 1
        }},
        {update_config, #{
            max_depth => 2,
            scan_interval => 1,
            delete_enable => false,
            write_once => true}}
        | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ct:timetrap({minutes, 20}),
    mock_link_handling_method(Workers),
    ConfigWithProxy = lfm_proxy:init(Config),
    Config2 = add_synced_storages(ConfigWithProxy),
    Config3 = add_rdwr_storages(Config2),
    create_init_file(Config3, true),
    Config3.

end_per_testcase(Case, Config)
    when Case =:= chmod_file_update2_test
    orelse Case =:= create_file_in_dir_exceed_batch_update_test
    orelse Case =:= create_list_race_test ->

    [W1 | _] = Workers = ?config(op_worker_nodes, Config),
    OldDirBatchSize = ?config(old_storage_sync_dir_batch_size, Config),
    test_utils:mock_unload(Workers, [storage_driver]),
    test_utils:set_env(W1, op_worker, storage_sync_dir_batch_size, OldDirBatchSize),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config)
    when Case =:= create_directory_import_check_user_id_test
    orelse Case =:= create_directory_import_check_user_id_error_test
    orelse Case =:= create_file_import_check_user_id_test
    orelse Case =:= create_file_import_check_user_id_error_test ->

    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, [luma]),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config)
    when Case =:= import_nfs_acl_test
    orelse Case =:= update_nfs_acl_test ->

    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, [luma, storage_driver]),
    end_per_testcase(default, Config);

end_per_testcase(import_nfs_acl_with_disabled_luma_should_fail_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, [storage_driver]),
    end_per_testcase(default, Config);

end_per_testcase(cancel_scan, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    OldDirBatchSize = ?config(old_storage_sync_dir_batch_size, Config),
    test_utils:set_env(W1, op_worker, storage_sync_dir_batch_size, OldDirBatchSize),
    end_per_testcase(default, Config);

end_per_testcase(sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, [fslogic_path]),
    end_per_testcase(default, Config);

end_per_testcase(create_remote_dir_import_race_test, Config) ->
    [W1| _] = ?config(op_worker_nodes, Config),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_ID),
    remove_link(W1, SpaceUuid, ?TEST_DIR),
    end_per_testcase(default, Config);

end_per_testcase(create_remote_file_import_race_test, Config) ->
    [W1| _] = ?config(op_worker_nodes, Config),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_ID),
    remove_link(W1, SpaceUuid, ?TEST_FILE1),
    end_per_testcase(default, Config);

end_per_testcase(sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage, Config) ->
    [W1| _] = ?config(op_worker_nodes, Config),
    % remove stalled deletion link
    TestDir = ?config(test_dir, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_ID),
    SpaceCtx = file_ctx:new_by_guid(SpaceGuid),
    remove_deletion_link(W1, ?SPACE_ID, TestDir, SpaceCtx),
    end_per_testcase(default, Config);

end_per_testcase(sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage, Config) ->
    [W1| _] = ?config(op_worker_nodes, Config),
    % remove stalled deletion link
    TestFile = ?config(test_file, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_ID),
    SpaceCtx = file_ctx:new_by_guid(SpaceGuid),
    remove_deletion_link(W1, ?SPACE_ID, TestFile, SpaceCtx),
    end_per_testcase(default, Config);

end_per_testcase(should_not_sync_file_during_replication, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(W1, [rtransfer_config]),
    OldInterval = ?config(old_storage_sync_check_interval, Config),
    test_utils:set_env(W1, op_worker, storage_sync_dir_batch_size, OldInterval),
    end_per_testcase(default, Config);

end_per_testcase(_Case, Config) ->
    Workers = [W1 | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(W) -> lfm_proxy:close_all(W) end, Workers),
    clean_luma_db(W1),
    disable_storage_sync(Config),
    clean_traverse_tasks(W1),
    clean_space(Config),
    clean_synced_storage(Config),
    cleanup_storage_sync_monitoring_model(W1, ?SPACE_ID),
    test_utils:mock_unload(Workers, [storage_sync_engine, storage_sync_hash, link_utils,
        storage_sync_traverse, storage_sync_deletion, storage_driver, helpers]),
    timer:sleep(timer:seconds(1)),
    lfm_proxy:teardown(Config).