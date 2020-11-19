%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% This module contains base test functions for testing storage import.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_test_base).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("kernel/include/file.hrl").
-include("storage_import_test.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

% TODO VFS-6161 divide to smaller test suites
% TODO VFS-6162 move utility functions to storage_import_test_utils module

% CT functions
-export([init_per_suite/1, init_per_testcase/2, end_per_suite/1, end_per_testcase/2]).

%% util functions
-export([
    enable_initial_scan/2, enable_continuous_scans/2, enable_continuous_scans/3, disable_continuous_scan/1,
    assertInitialScanFinished/2, assertInitialScanFinished/3,
    assertSecondScanFinished/2, assertScanFinished/3, assertScanFinished/4,
    assertNoScanInProgress/3,
    parallel_assert/5,
    assert_monitoring_state/4,
    provider_storage_path/2, provider_storage_path/3, get_rdwr_storage/2,
    create_nested_directory_tree/3, generate_nested_directory_tree_file_paths/2,
    clean_traverse_tasks/1,
    mock_import_file_error/2, unmock_import_file_error/1,
    stop_scan/2, start_scan/2,
    get_finished_scans_num/2
]).

% exported for RPC
-export([
    verify_file_deleted/4,
    verify_file_deleted/5,
    verify_file/5,
    verify_file_in_dir/5,
    verify_dir/5
]).

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
    close_file_import_race_test/2,
    delete_file_reimport_race_test/2,
    delete_opened_file_reimport_race_test/2,

    % tests of update
    update_syncs_files_after_import_failed_test/1,
    update_syncs_files_after_previous_update_failed_test/1,
    sync_should_not_reimport_deleted_but_still_opened_file/2,
    sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage/1,
    sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage/2,
    sync_should_not_import_recreated_file_with_suffix_on_storage/2,
    sync_should_update_replicated_file_with_suffix_on_storage/2,
    sync_should_update_blocks_of_recreated_file_with_suffix_on_storage/2,
    sync_should_not_import_replicated_file_with_suffix_on_storage/2,
    sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed/1,
    create_delete_import2_test/2,
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
    create_delete_race_test/2,
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
    enable_initial_scan(Config, ?SPACE_ID),

    % wait till scan is finished
    assertInitialScanFinished(W1, ?SPACE_ID),
    ?assertMatch({ok, []},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10)),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_PATH}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 1,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 0,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_directory_import_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#775),
    enable_initial_scan(Config, ?SPACE_ID),

    % wait till scan is finished
    assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_directory_import_error_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    ?assertNotMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH})),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#775),

    mock_import_file_error(W1, ?TEST_DIR),
    enable_initial_scan(Config, ?SPACE_ID),

    % wait till scan is finished
    assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 1,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 0,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_directory_import_check_user_id_test(Config) ->
    [W1, W2| _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#775),
    ok = sd_test_utils:chown(W1, SDHandle, ?TEST_UID, ?TEST_GID),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_directory_import_check_user_id_error_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#775),
    ok = sd_test_utils:chown(W1, SDHandle, ?TEST_UID, ?TEST_GID),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),
    %% Check if dir was not imported
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 1,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 0,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_directory_import_without_read_permission_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR2),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#000),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH2}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH2}), 2 * ?ATTEMPTS).

create_directory_import_many_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    DirsNumber = 200,
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dirs on storage
    lists_utils:pforeach(fun(N) ->
        DirPath = provider_storage_path(?SPACE_ID, integer_to_binary(N)),
        SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, DirPath, RDWRStorage),
        ok = sd_test_utils:mkdir(W1, SDHandle, 8#775)
    end, lists:seq(1, DirsNumber)),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    parallel_assert(?MODULE, verify_dir, [W1, SessId, ?ATTEMPTS], lists:seq(1, DirsNumber), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 201,
        <<"created">> => 200,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 200,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdHourHist">> => 200,
        <<"createdDayHist">> => 200,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_empty_file_import_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if file was imported on W2
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, <<"">>},
        lfm_proxy:read(W2, Handle2, 0, 100), ?ATTEMPTS).

create_file_import_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    RDWRStorage = get_rdwr_storage(Config, W1),

    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    RDWRStorage = get_rdwr_storage(Config, W1),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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

create_delete_import_test_read_both(Config) ->
    create_delete_import_test(Config, true).

create_delete_import_test_read_remote_only(Config) ->
    create_delete_import_test(Config, false).

create_delete_import_test(Config, ReadBoth) ->
    [W1, W2 | _] = Workers = ?config(op_worker_nodes, Config),
    Attempts = 60,

    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    StorageTestFilePath2 = provider_storage_path(?SPACE_ID, ?TEST_FILE1, false),
    RDWRStorage = get_rdwr_storage(Config, W1),

    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),

    Size = byte_size(?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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
        lfm_proxy:stat(W2, SessIdW2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

    ?assertMatch(ok, lfm_proxy:unlink(W2, ?ROOT_SESS_ID, {guid, GUID})),
    ?assertEqual({error, ?ENOENT}, sd_test_utils:read_file(W2, SDHandle2, 0, ?TEST_DATA_SIZE), Attempts),
    ?assertEqual({error, ?ENOENT}, sd_test_utils:read_file(W1, SDHandle, 0, ?TEST_DATA_SIZE), Attempts),
    ok.

create_file_import_check_user_id_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    ok = sd_test_utils:chown(W1, SDHandle, ?TEST_UID, ?TEST_GID),
    enable_initial_scan(Config, ?SPACE_ID),

    assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_file_import_check_user_id_error_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    ok = sd_test_utils:chown(W1, SDHandle, ?TEST_UID, ?TEST_GID),
    enable_initial_scan(Config, ?SPACE_ID),

    assertInitialScanFinished(W1, ?SPACE_ID),

    %% Check if file was imported on W1
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 1,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 0,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_file_in_dir_import_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, filename:join(?TEST_DIR, ?TEST_FILE2)),
    timer:sleep(timer:seconds(1)),

    %% Create dir on storage
    SDDirHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDDirHandle, 8#775),
    %% Create file on storage
    SDFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDFileHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDFileHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),

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
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),

    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"created">> => 2,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 2,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 2,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_subfiles_import_many_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    DirsNumber = 200,
    lists_utils:pforeach(fun(N) ->
        NBin = integer_to_binary(N),
        DirPath = provider_storage_path(?SPACE_ID, NBin),
        FilePath = filename:join([DirPath, integer_to_binary(N)]),
        SDDirHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, DirPath, RDWRStorage),
        ok = sd_test_utils:mkdir(W1, SDDirHandle, 8#775),
        SDFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, FilePath, RDWRStorage),
        ok = sd_test_utils:create_file(W1, SDFileHandle, 8#664),
        {ok, _} = sd_test_utils:write_file(W1, SDFileHandle, 0, ?TEST_DATA)
    end, lists:seq(1, DirsNumber)),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID, 60),

    parallel_assert(?MODULE, verify_file_in_dir, [W1, SessId, 60], lists:seq(1, DirsNumber), 60),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 401,
        <<"created">> => 400,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 400,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdHourHist">> => 400,
        <<"createdDayHist">> => 400,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_subfiles_import_many2_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    RootPath = provider_storage_path(?SPACE_ID, <<"">>),
    DirStructure = [10, 10, 10],
    RootSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, RootPath, RDWRStorage),

    create_nested_directory_tree(W1, DirStructure, RootSDHandle),
    enable_initial_scan(Config, ?SPACE_ID),
    Files = generate_nested_directory_tree_file_paths(DirStructure, ?SPACE_PATH),

    Timeout = 600,
    parallel_assert(?MODULE, verify_file, [W1, SessId, Timeout], Files, Timeout),
    assertInitialScanFinished(W1, ?SPACE_ID, Timeout),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 1111,
        <<"created">> => 1110,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1110,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdDayHist">> => 1110,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_remote_file_import_conflict_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    {ok, FileGuid} = lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH1, 8#664),
    {ok, Handle} = lfm_proxy:open(W2, SessId2, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(W2, Handle, 0, ?TEST_DATA),
    lfm_proxy:close(W2, Handle),

    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
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

    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if file was imported on W2
    ?assertMatch({ok, [_, _]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ImportedConflictingFilePath}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ImportedConflictingFilePath}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA2},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA2)), ?ATTEMPTS).

create_remote_dir_import_race_test(Config) ->
    % directory is created in remote provider and at the same time, file with the same name is created on storage
    % this tests checks whether we properly handle situation when links are synchronized during scan,
    % but file_meta is not yet synchronized
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),

    timer:sleep(timer:seconds(1)),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#775),

    % pretend that only link has been synchronized
    Ctx = rpc:call(W2, file_meta, get_ctx, []),
    TreeId = rpc:call(W2, oneprovider, get_id, []),
    FileUuid = datastore_key:new(),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_ID),
    {ok, _} = rpc:call(W2, datastore_model, add_links,
        [Ctx#{scope => ?SPACE_ID}, SpaceUuid, TreeId, {?TEST_DIR, FileUuid}]),
    ?assertMatch({ok, _, _}, rpc:call(W1, file_meta, get_child_uuid, [SpaceUuid, ?TEST_DIR]), ?ATTEMPTS),

    % storage import should import directory with conflicting name
    ProviderId1 = provider_id(W1),
    ImportedConflictingDirName = ?IMPORTED_CONFLICTING_FILE_NAME(?TEST_DIR, ProviderId1),
    ImportedConflictingDirPath = ?SPACE_TEST_FILE_PATH(ImportedConflictingDirName),

    enable_initial_scan(Config, ?SPACE_ID),
    % wait till scan is finished
    assertInitialScanFinished(W1, ?SPACE_ID),

    %% Check if dir was imported
    {ok, #file_attr{guid = FileGuid}} = ?assertMatch({ok, #file_attr{name = ImportedConflictingDirName}},
        lfm_proxy:stat(W1, SessId, {path, ImportedConflictingDirPath}), ?ATTEMPTS),
    ?assertNotEqual(FileUuid, file_id:guid_to_uuid(FileGuid)),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ImportedConflictingDirPath}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_remote_file_import_race_test(Config) ->
    % file is created in remote provider and at the same time, file with the same name is created on storage
    % this tests checks whether we properly handle situation when links are synchronized during scan,
    % but file_location is not yet synchronized
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),

    timer:sleep(timer:seconds(1)),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),

    % pretend that only link has been synchronized
    Ctx = rpc:call(W2, file_meta, get_ctx, []),
    TreeId = rpc:call(W2, oneprovider, get_id, []),
    FileUuid = datastore_key:new(),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_ID),
    {ok, _} = rpc:call(W2, datastore_model, add_links,
        [Ctx#{scope => ?SPACE_ID}, SpaceUuid, TreeId, {?TEST_FILE1, FileUuid}]),
    ?assertMatch({ok, _, _}, rpc:call(W1, file_meta, get_child_uuid, [SpaceUuid, ?TEST_FILE1]), ?ATTEMPTS),

    % storage import should import file with conflicting name
    ProviderId1 = provider_id(W1),
    ImportedConflictingFileName = ?IMPORTED_CONFLICTING_FILE_NAME(?TEST_FILE1, ProviderId1),
    ImportedConflictingFilePath = ?SPACE_TEST_FILE_PATH(ImportedConflictingFileName),

    enable_initial_scan(Config, ?SPACE_ID),
    % wait till scan is finished
    assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).


import_nfs_acl_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER2, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    RDWRStorage = get_rdwr_storage(Config, W1),

    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

import_nfs_acl_with_disabled_luma_should_fail_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    RDWRStorage = get_rdwr_storage(Config, W1),

    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID, ?ATTEMPTS),

    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 1,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 0,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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

    ok = test_utils:mock_new(Workers, storage_import_engine),
    ok = test_utils:mock_expect(Workers, storage_import_engine, import_file_unsafe, fun(StorageFileCtx, Info) ->
        block_syncing_process(TestPid),
        meck:passthrough([StorageFileCtx, Info])
    end),

    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),

    enable_initial_scan(Config, ?SPACE_ID),

    SyncingProcess = await_syncing_process(),
    {ok, _} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#777),
    {ok, Handle} = lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, write),
    {ok, _} = lfm_proxy:write(W1, Handle, 0, ?WRITE_TEXT),
    ok = lfm_proxy:close(W1, Handle),
    release_syncing_process(SyncingProcess),

    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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
        lfm_proxy:open(W2, SessId2, {path, ImportedConflictingFilePath}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle3, 0, 100), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle3).

close_file_import_race_test(Config, StorageType) ->
    % in this test we check whether file is not reimported when file is fully deleted before checking
    % deletion links
    [W1 | _] = Workers = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),

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
        ContainerStorageSpacePath = host_storage_path(RDWRStorageMountPoint, ?SPACE_ID, <<"">>),
        touch(W1, ContainerStorageSpacePath)
    end, StorageType),

    enable_initial_scan(Config, ?SPACE_ID),

    SyncingProcess = await_syncing_process(),
    ok = lfm_proxy:close(W1, CreateHandle),
    release_syncing_process(SyncingProcess),

    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"createdSum">> => 0,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID, ?ATTEMPTS),

    %% Check if file was not imported
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1})).

delete_file_reimport_race_test(Config, StorageType) ->
    % in this test, we check whether storage import does not reimport file that is deleted between checking links and file_location
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create file
    {ok, FileGuid} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, Handle1} = lfm_proxy:open(W1, SessId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(W1, Handle1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, Handle1),

    TestProcess = self(),
    ok = test_utils:mock_new(W1, storage_import_engine),
    ok = test_utils:mock_expect(W1, storage_import_engine, check_location_and_maybe_sync, fun(StorageFileCtx, FileCtx, Info) ->
        Guid = file_ctx:get_guid_const(FileCtx),
        case Guid =:= FileGuid of
            true -> block_syncing_process(TestProcess);
            false -> ok
        end,
        meck:passthrough([StorageFileCtx, FileCtx, Info])
    end),

    timer:sleep(timer:seconds(1)),
    ?EXEC_ON_POSIX_ONLY(fun() ->
        % touch space dir to ensure that it will be scanned
        RDWRStorageMountPoint = get_mount_point(RDWRStorage),
        ContainerStorageSpacePath = host_storage_path(RDWRStorageMountPoint, ?SPACE_ID, <<"">>),
        touch(W1, ContainerStorageSpacePath)
    end, StorageType),

    enable_initial_scan(Config, ?SPACE_ID),
    SyncingProcess = await_syncing_process(),
    ok = lfm_proxy:unlink(W1, SessId, {guid, FileGuid}),
    release_syncing_process(SyncingProcess),

    assertInitialScanFinished(W1, ?SPACE_ID),

    %% Check if file was not reimported on W1
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 1)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1, % space_dir will have updated time
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"createdSum">> => 0,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if file was deleted on W2
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W2, SessId2, {path, ?SPACE_PATH}, 0, 1), ?ATTEMPTS).

delete_opened_file_reimport_race_test(Config, StorageType) ->
    % in this test, we check whether storage import does not reimport file that is deleted while still opened,
    % between checking links and file_location
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    %% Create file
    {ok, FileGuid} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, Handle1} = lfm_proxy:open(W1, SessId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(W1, Handle1, 0, ?TEST_DATA),

    TestProcess = self(),
    ok = test_utils:mock_new(W1, storage_import_engine),
    ok = test_utils:mock_expect(W1, storage_import_engine, check_location_and_maybe_sync, fun(StorageFileCtx, FileCtx, Info) ->
        Guid = file_ctx:get_guid_const(FileCtx),
        case Guid =:= FileGuid of
            true -> block_syncing_process(TestProcess);
            false -> ok
        end,
        meck:passthrough([StorageFileCtx, FileCtx, Info])
    end),

    timer:sleep(timer:seconds(1)),
    ?EXEC_ON_POSIX_ONLY(fun() ->
        % touch space dir to ensure that it will be scanned
        RDWRStorageMountPoint = get_mount_point(RDWRStorage),
        ContainerStorageSpacePath = host_storage_path(RDWRStorageMountPoint, ?SPACE_ID, <<"">>),
        touch(W1, ContainerStorageSpacePath)
    end, StorageType),

    enable_initial_scan(Config, ?SPACE_ID),

    SyncingProcess = await_syncing_process(),
    ok = lfm_proxy:unlink(W1, SessId, {guid, FileGuid}),
    release_syncing_process(SyncingProcess),

    assertInitialScanFinished(W1, ?SPACE_ID),

    %% Check if file was not reimported on W1
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 1)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1, % space_dir will have updated time
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"createdSum">> => 0,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if file was deleted on W2
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W2, SessId2, {path, ?SPACE_PATH}, 0, 1), ?ATTEMPTS).

%%%===================================================================
%%% Tests of update
%%%===================================================================

update_syncs_files_after_import_failed_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    RDWRStorage = get_rdwr_storage(Config, W1),

    %% Create file on storage
    timer:sleep(timer:seconds(1)),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    mock_import_file_error(W1, ?TEST_FILE1),
    enable_initial_scan(Config, ?SPACE_ID),

    assertInitialScanFinished(W1, ?SPACE_ID),

    %% Check if dir was not imported
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH})),
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH})),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 1,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 0,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    unmock_import_file_error(W1),
    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS).

update_syncs_files_after_previous_update_failed_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    RDWRStorage = get_rdwr_storage(Config, W1),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 1,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 0,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
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
    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),
    unmock_import_file_error(W1),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 1,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 0,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if dir was not imported
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1})),
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1})),

    enable_continuous_scans(Config, ?SPACE_ID),
    assertScanFinished(W1, ?SPACE_ID, 3, ?ATTEMPTS),
    disable_continuous_scan(Config),

    %next scan should import file
    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"modified">> => 0,
        <<"otherProcessed">> => 1,
        <<"createdSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
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

    StorageSpacePath = provider_storage_path(?SPACE_ID, <<"">>),
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
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    % there should be no files visible in the space
    ?assertMatch({ok, []},
        lfm_proxy:get_children(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 0,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"createdSum">> => 0,
        <<"modifiedSum">> => 0,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"modifiedMinHist">> => 0,
        <<"modifiedHourHist">> => 0,
        <<"modifiedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    TestDir = ?config(test_dir, Config),

    StorageTestDirPath = provider_storage_path(?SPACE_ID, TestDir),
    SpaceTestDirPath = ?SPACE_TEST_DIR_PATH(TestDir),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#755),

    enable_initial_scan(Config, ?SPACE_ID),
    % wait till scan is finished
    assertInitialScanFinished(W1, ?SPACE_ID),

    %% Check if dir was imported
    ?assertMatch({ok, [{_, TestDir}]}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10)),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, SpaceTestDirPath}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W2, SessId2, {path, SpaceTestDirPath}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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
    ContainerStorageSpacePath = host_storage_path(RDWRStorageMountPoint, ?SPACE_ID, <<"">>),
    touch(W1, ContainerStorageSpacePath),

    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1, % space dir was updated because we performed "touch" on it
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1, % test dir was ignored
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 2,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 2,
        <<"modifiedDayHist">> => 2,
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
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    TestFile = ?config(test_file, Config),

    StorageTestFilePath = provider_storage_path(?SPACE_ID, TestFile),
    SpaceTestFilePath = ?SPACE_TEST_DIR_PATH(TestFile),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),

    enable_initial_scan(Config, ?SPACE_ID),
    % wait till scan is finished
    assertInitialScanFinished(W1, ?SPACE_ID),

    %% Check if file was imported
    ?assertMatch({ok, [{_, TestFile}]}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10)),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, SpaceTestFilePath}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W2, SessId2, {path, SpaceTestFilePath}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    % mock error from helpers:rmdir
    ok = test_utils:mock_new(W1, helpers),
    ok = test_utils:mock_expect(W1, helpers, unlink, fun(_, _, _) -> {error, ?EBUSY} end),

    lfm_proxy:rm_recursive(W1, SessId, {path, SpaceTestFilePath}),

    timer:sleep(timer:seconds(1)),
    ?EXEC_ON_POSIX_ONLY(fun() ->
        % touch space dir to make sure that it will be updated
        RDWRStorageMountPoint = get_mount_point(RDWRStorage),
        ContainerStorageSpacePath = host_storage_path(RDWRStorageMountPoint, ?SPACE_ID, <<"">>),
        touch(W1, ContainerStorageSpacePath)
    end, StorageType),

    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1, % space dir was updated because we performed "touch" on it
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1, % test file was ignored
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 2,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 2,
        <<"modifiedDayHist">> => 2,
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
    StorageSpacePath = provider_storage_path(?SPACE_ID, <<"">>),
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
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 0,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

sync_should_update_blocks_of_recreated_file_with_suffix_on_storage(Config, StorageType) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),

    StorageSpacePath = provider_storage_path(?SPACE_ID, <<"">>),
    SpaceSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageSpacePath, RDWRStorage),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),

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
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    % there should be only 1 file visible in the space
    ?assertMatch({ok, [_]}, lfm_proxy:get_children(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 0,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    % change one byte in the suffixed file
    {ok, 1} = sd_test_utils:write_file(W1, FileWithSuffixHandle, ?CHANGED_BYTE_OFFSET, ?CHANGED_BYTE),

    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 0,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
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

    StorageSpacePath = provider_storage_path(?SPACE_ID, <<"">>),
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
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    % there should be only 2 files visible in the space
    ?assertMatch({ok, [_, _]}, lfm_proxy:get_children(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 0,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    assertInitialScanFinished(W1, ?SPACE_ID),
    % there still should be only 2 files visible in the space
    ?assertMatch({ok, [_, _]}, lfm_proxy:get_children(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)).

sync_should_update_replicated_file_with_suffix_on_storage(Config, StorageType) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageSpacePath = provider_storage_path(?SPACE_ID, <<"">>),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
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
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    % there should be only 2 files visible in the space
    ?assertMatch({ok, [_, _]}, lfm_proxy:get_children(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 0,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    % change one byte in the suffixed file
    {ok, 1} = sd_test_utils:write_file(W1, FileWithSuffixHandle, ?CHANGED_BYTE_OFFSET, ?CHANGED_BYTE),

    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 0,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    {ok, H4} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {guid, G2}, read)),
    ?assertMatch({ok, ?TEST_DATA_ONE_BYTE_CHANGED},
        lfm_proxy:read(W1, H4, 0, 100), ?ATTEMPTS),

    %% Check if file was updated on W2
    {ok, H5} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {guid, G2}, read)),
    ?assertMatch({ok, ?TEST_DATA_ONE_BYTE_CHANGED},
        lfm_proxy:read(W2, H5, 0, 100), ?ATTEMPTS).

sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"created">> => 0,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_delete_import2_test(Config, ReadBoth) ->
    [W1, W2 | _] = Workers = ?config(op_worker_nodes, Config),
    Attempts = 60,
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    StorageTestFilePath2 = provider_storage_path(?SPACE_ID, ?TEST_FILE1, false),
    RDWRStorage = get_rdwr_storage(Config, W1),

    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    Size = byte_size(?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),

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
        lfm_proxy:stat(W2, SessIdW2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch(ok, lfm_proxy:unlink(W2, <<"0">>, {guid, GUID})),

    ?assertEqual({error, ?ENOENT}, sd_test_utils:read_file(W1, SDHandle, 0, ?TEST_DATA_SIZE), Attempts),
    ?assertEqual({error, ?ENOENT}, sd_test_utils:read_file(W2, SDHandle2, 0, ?TEST_DATA_SIZE), Attempts),

    enable_continuous_scans(Config, ?SPACE_ID),

    {ok, FileGuid} = ?assertMatch({ok, _}, lfm_proxy:create(W2, SessIdW2, ?SPACE_TEST_FILE_PATH1, 8#777)),
    {ok, FileHandle} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessIdW2, {guid, FileGuid}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W2, FileHandle, 0, ?TEST_DATA)),

    {ok, FileHandle2} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessIdW1, {guid, FileGuid}, read), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(W1, FileHandle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, sd_test_utils:read_file(W1, SDHandle, 0, ?TEST_DATA_SIZE), Attempts),
    ok = sd_test_utils:unlink(W1, SDHandle, Size),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:open(W2, SessIdW2, {guid, FileGuid}, read), ?ATTEMPTS).

create_subfiles_and_delete_before_import_is_finished_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#775),
    enable_initial_scan(Config, ?SPACE_ID),

    assertInitialScanFinished(W1, ?SPACE_ID),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Create nested tree structure
    DirStructure = [10, 10, 10],
    create_nested_directory_tree(W1, DirStructure, SDHandle),
    enable_continuous_scans(Config, ?SPACE_ID),

    ?assertEqual(true, rpc:call(W1, storage_import_monitoring, is_scan_in_progress, [?SPACE_ID]), ?ATTEMPTS),

    ok = sd_test_utils:recursive_rm(W1, SDHandle),
    ?assertMatch({error, ?ENOENT}, sd_test_utils:ls(W1, SDHandle, 0, 100)),
    assertScanFinished(W1, ?SPACE_ID, 3, 5 * ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 100), ?ATTEMPTS),
    disable_continuous_scan(Config).

create_file_in_dir_update_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestDirPath2 = provider_storage_path(?SPACE_ID, ?TEST_DIR2),
    StorageTestFileinDirPath1 = provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),

    %% Create dirs on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    SDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath2, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle2, 8#777),
    enable_initial_scan(Config, ?SPACE_ID),

    assertInitialScanFinished(W1, ?SPACE_ID),

    %% Check if dirs were imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH2}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"created">> => 2,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 2,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 2,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    test_utils:mock_new(W1, storage_import_hash, [passthrough]),
    test_utils:mock_new(W1, storage_sync_traverse, [passthrough]),
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    %% Check if files were imported on W1
    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"created">> => 1,
%%        <<"modified">> => 1,  % TODO VFS-6868 sometimes import detects change because timestamp of space dir was decreased
        <<"deleted">> => 0,
        <<"failed">> => 0,
%%        <<"otherProcessed">> => 2, % TODO VFS-6868 sometimes import detects change because timestamp of space dir was decreased
        <<"createdSum">> => 3,
%%        <<"modifiedSum">> => 2, % TODO VFS-6868 sometimes import detects change because timestamp of space dir was decreased
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 3,
        <<"createdDayHist">> => 3,
        % TODO VFS-6868 sometimes import detects change because timestamp of space dir was decreased
%%        <<"modifiedMinHist">> => 1,
%%        <<"modifiedHourHist">> => 2,
%%        <<"modifiedDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    History = rpc:call(W1, meck, history, [storage_import_hash]),
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
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS).


changing_max_depth_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),

    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),

    Dir2Path = filename:join([?TEST_DIR, ?TEST_DIR2]),
    StorageTestDirPath2 = provider_storage_path(?SPACE_ID, Dir2Path),
    SpaceTestDirPath2 = ?SPACE_TEST_DIR_PATH(Dir2Path),

    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    StorageTestFileinDirPath = provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),

    File2Path = filename:join([?TEST_DIR, ?TEST_DIR2, ?TEST_FILE1]),
    StorageTestFileinDirPath2 = provider_storage_path(?SPACE_ID, File2Path),
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

    % in init_per_testcase, max_depth for import is set to 1
    enable_initial_scan(Config, ?SPACE_ID),

    assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 2,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 2,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 2,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    % in init_per_testcase, max_depth for update is set to 2, so new file should be detected
    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

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
        <<"created">> => 2,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"createdSum">> => 4,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdHourHist">> => 4,
        <<"createdDayHist">> => 4,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),


    % run another scan with max_depth = 3
    enable_continuous_scans(Config, ?SPACE_ID, #{max_depth => 3}),
    assertScanFinished(W1, ?SPACE_ID, 3),
    disable_continuous_scan(Config),

    %% Directory and file on 3rd level should be imported
    ?assertMatch({ok, [_]},
        lfm_proxy:get_children(W1, SessId, {path, SpaceTestDirPath2}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, SpaceTestFilePath2}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"toProcess">> => 4,
        <<"created">> => 1,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 3,
        <<"createdSum">> => 5,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdHourHist">> => 5,
        <<"createdDayHist">> => 5,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_file_in_dir_exceed_batch_update_test(Config) ->
    % in this test storage_import_dir_batch_size is set in init_per_testcase to 2
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),

    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestDirPath2 = provider_storage_path(?SPACE_ID, ?TEST_DIR2),
    StorageTestFilePath1 = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    StorageTestFilePath2 = provider_storage_path(?SPACE_ID, ?TEST_FILE2),
    StorageTestFilePath3 = provider_storage_path(?SPACE_ID, ?TEST_FILE3),
    StorageTestFilePath4 = provider_storage_path(?SPACE_ID, ?TEST_FILE4),
    StorageTestFileinDirPath1 = provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),

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
    timer:sleep(timer:seconds(1)),

    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 6,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 3,
        <<"createdSum">> => 6,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 6,
        <<"createdHourHist">> => 6,
        <<"createdDayHist">> => 6,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    timer:sleep(timer:seconds(1)),
    test_utils:mock_new(W1, storage_import_hash, [passthrough]),
    test_utils:mock_new(W1, storage_sync_traverse, [passthrough]),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 7,
        <<"created">> => 1,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 6,
        <<"createdSum">> => 7,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 7,
        <<"createdDayHist">> => 7,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if files were imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),

    History = rpc:call(W1, meck, history, [storage_import_hash]),
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
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS).

force_start_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFileinDirPath1 = provider_storage_path(?SPACE_ID, ?TEST_FILE1),

    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 1,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 0,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    timer:sleep(timer:seconds(1)),
    % create file on storage
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),

    ?assertEqual(ok, start_scan(W1, ?SPACE_ID)),
    assertSecondScanFinished(W1, ?SPACE_ID),

    %% Check if files were imported on W1
    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"createdSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

    {ok, Handle} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle),

    %% Check if file is visible on W2
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS).


force_stop_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    RootPath = provider_storage_path(?SPACE_ID, <<"">>),
    DirStructure = [10, 10, 10],
    RootSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, RootPath, RDWRStorage),

    create_nested_directory_tree(W1, DirStructure, RootSDHandle),
    Files = generate_nested_directory_tree_file_paths(DirStructure, ?SPACE_PATH),
    Timeout = 600,
    TestProc = self(),
    test_utils:mock_new(W1, storage_import_engine, [passthrough]),
    test_utils:mock_expect(W1, storage_import_engine, import_file_unsafe,
        fun(StorageFileCtx, Info) ->
            TestProc ! start,
            meck:passthrough([StorageFileCtx, Info])
        end),

    enable_initial_scan(Config, ?SPACE_ID),
    receive start -> ok end,

    stop_scan(W1, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID, Timeout),

    SSM = ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0
    }, ?SPACE_ID),

    #{
        <<"toProcess">> := ToProcess,
        <<"otherProcessed">> := OtherProcessed,
        <<"modified">> := Modified,
        <<"created">> := Created
    } = SSM,

    ?assert(ToProcess =< 1111),
    ?assert((OtherProcessed + Modified + Created) =< 1111),

    % check whether next scan will import missing files
    enable_continuous_scans(Config, ?SPACE_ID),
    assertScanFinished(W1, ?SPACE_ID, 2, Timeout),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 1110,
        <<"deletedSum">> => 0,
        <<"createdDayHist">> => 1110,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),
    parallel_assert(?MODULE, verify_file, [W1, SessId, Timeout], Files, Timeout).


delete_empty_directory_update_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    %% Create dir on storage
    RDWRStorage = get_rdwr_storage(Config, W1),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    enable_initial_scan(Config, ?SPACE_ID),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    %% Delete dir on storage
    enable_continuous_scans(Config, ?SPACE_ID),
    ok = sd_test_utils:rmdir(W1, SDHandle),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"created">> => 0,
        <<"modified">> => 0,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 1,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1
    }, ?SPACE_ID),

    %% Check if dir was deleted in space
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS).

delete_non_empty_directory_update_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestFileinDirPath1 = provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
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

    enable_continuous_scans(Config, ?SPACE_ID),
    %% Delete dir on storage
    ok = sd_test_utils:recursive_rm(W1, SDHandle),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"created">> => 0,
        <<"modified">> => 0,
        <<"deleted">> => 2,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"createdSum">> => 2,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 2,
        <<"createdMinHist">> => 2,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 2,
        <<"deletedHourHist">> => 2,
        <<"deletedDayHist">> => 2
    }, ?SPACE_ID),

    %% Check if dir was deleted in space
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS).

sync_works_properly_after_delete_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestDirPath2 = provider_storage_path(?SPACE_ID, ?TEST_DIR2),
    StorageTestFileinDirPath1 =
        provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),
    StorageTestFileinDirPath2 =
        provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR2, ?TEST_FILE2])),

    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID, ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"created">> => 2,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 2,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 2,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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

    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    %% Check if dir was deleted in space
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"deleted">> => 2,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"createdSum">> => 2,
        <<"modifiedSum">> => 2,
        <<"deletedSum">> => 2,
        <<"createdMinHist">> => 2,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 2,
        <<"modifiedDayHist">> => 2,
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

    enable_continuous_scans(Config, ?SPACE_ID),
    assertScanFinished(W1, ?SPACE_ID, 3, ?ATTEMPTS),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"toProcess">> => 5,
        <<"created">> => 2,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"createdSum">> => 4,
        <<"modifiedSum">> => 3,
        <<"deletedSum">> => 2,
        <<"createdMinHist">> => 2,
        <<"createdHourHist">> => 4,
        <<"createdDayHist">> => 4,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 3,
        <<"modifiedDayHist">> => 3,
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

delete_and_update_files_simultaneously_update_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestFileinDirPath1 = provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),
    StorageTestFileinDirPath2 = provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE2])),
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
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),
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
        <<"created">> => 3,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 3,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 3,
        <<"createdHourHist">> => 3,
        <<"createdDayHist">> => 3,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Delete file on storage
    sd_test_utils:unlink(W1, FileInDirSDHandle, ?TEST_DATA_SIZE),
    sd_test_utils:chmod(W1, FileInDirSDHandle2, NewMode),

    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 5,
        <<"created">> => 0,
        <<"deleted">> => 1,
        <<"modified">> => 2,
        <<"otherProcessed">> => 2,
        <<"failed">> => 0,
        <<"createdSum">> => 3,
        <<"modifiedSum">> => 3,
        <<"deletedSum">> => 1,
        <<"createdMinHist">> => 3,
        <<"createdHourHist">> => 3,
        <<"createdDayHist">> => 3,
        <<"modifiedMinHist">> => 2,
        <<"modifiedHourHist">> => 3,
        <<"modifiedDayHist">> => 3,
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

delete_file_update_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
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
    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"created">> => 0,
        <<"modified">> => 0,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 1,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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

delete_file_in_dir_update_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, filename:join(?TEST_DIR, ?TEST_FILE2)),
    timer:sleep(timer:seconds(1)),

    %% Create dir on storage
    SDDirHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDDirHandle, 8#775),

    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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

    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"created">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 2,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
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
    enable_continuous_scans(Config, ?SPACE_ID),
    assertScanFinished(W1, ?SPACE_ID, 3),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"toProcess">> => 4,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"createdSum">> => 2,
        <<"deletedSum">> => 1,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1
    }, ?SPACE_ID),

    %% Check if file was deleted from the space
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}), ?ATTEMPTS),
    ?assertMatch({ok, []},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 10), ?ATTEMPTS).


delete_many_subfiles_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    RootPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, RootPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    DirStructure = [10, 10, 10],
    create_nested_directory_tree(W1, DirStructure, SDHandle),
    enable_initial_scan(Config, ?SPACE_ID),
    Files = generate_nested_directory_tree_file_paths(DirStructure, ?SPACE_TEST_DIR_PATH),

    Timeout = 600,
    parallel_assert(?MODULE, verify_file, [W1, SessId, Timeout], Files, Timeout),
    assertInitialScanFinished(W1, ?SPACE_ID, Timeout),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 1112,
        <<"created">> => 1111,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1111,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdHourHist">> => 1111,
        <<"createdDayHist">> => 1111,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    ok = sd_test_utils:recursive_rm(W1, SDHandle),
    enable_continuous_scans(Config, ?SPACE_ID),
    assertScanFinished(W1, ?SPACE_ID, 2, Timeout),
    parallel_assert(?MODULE, verify_file_deleted, [W1, SessId, Timeout], [?SPACE_TEST_DIR_PATH | Files], Timeout),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 1113,
        <<"created">> => 0,
        <<"modified">> => 0,
        <<"deleted">> => 1111,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"createdSum">> => 1111,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 1111,
        <<"createdDayHist">> => 1111,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedHourHist">> => 1111,
        <<"deletedDayHist">> => 1111
    }, ?SPACE_ID).

create_delete_race_test(Config, StorageType) ->
    % this tests checks whether storage import works properly in case of create-delete race
    % description:
    % if the file is created after storage_sync_links tree is built
    % it is necessary to ensure that storage import does not delete the file
    % because it's missing in the file_meta links
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),

    RDWRStorage = get_rdwr_storage(Config, W1),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    TestPid = self(),
    ok = test_utils:mock_new(W1, storage_import_deletion),
    ok = test_utils:mock_expect(W1, storage_import_deletion, do_master_job, fun(Job, Args) ->
        % hold on sync
        block_syncing_process(TestPid),
        meck:passthrough([Job, Args])
    end),

    timer:sleep(timer:seconds(1)),
    ?EXEC_ON_POSIX_ONLY(fun() ->
        % touch space dir to ensure that storage import will try to detect deletions
        RDWRStorageMountPoint = get_mount_point(RDWRStorage),
        ContainerStorageSpacePath = host_storage_path(RDWRStorageMountPoint, ?SPACE_ID, <<"">>),
        touch(W1, ContainerStorageSpacePath)
    end, StorageType),

    enable_continuous_scans(Config, ?SPACE_ID),

    SyncingProcess = await_syncing_process(),

    % create file, it should not be deleted
    {ok, FileGuid2} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#664),
    {ok, Handle2} = lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, write),
    {ok, _} = lfm_proxy:write(W1, Handle2, 0, ?TEST_DATA2),
    ok = lfm_proxy:close(W1, Handle2),

    release_syncing_process(SyncingProcess),

    assertSecondScanFinished(W1, ?SPACE_ID),
    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"modified">> => 1,
        <<"createdSum">> => 0,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
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

create_list_race_test(Config) ->
    % this tests checks whether storage import works properly in case of create-list race
    % description:
    % storage import builds storage_sync_links tree for detecting deleted files by listing the storage using offset and limit
    % it is possible that if other files are deleted in the meantime, file may be omitted and therefore missing
    % in the storage_sync_links
    % storage import must not delete such file
    % storage_import_dir_batch_size is set in this test to 2
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
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

    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    TestPid = self(),
    ok = test_utils:mock_new(W1, storage_driver),
    ok = test_utils:mock_expect(W1, storage_driver, readdir, fun(SDHandle, Offset, BatchSize) ->
        Result = meck:passthrough([SDHandle, Offset, BatchSize]),
        case SDHandle#sd_handle.file =:= <<"/">> of
            true ->
                % hold on sync
                TestPid ! {waiting, self(), Offset, Result},
                receive continue -> ok end;
            false ->
                ok
        end,
        Result
    end),

    % touch space dir to ensure that storage import will try to detect deletions
    timer:sleep(timer:seconds(1)),
    RDWRStorageMountPoint = get_mount_point(RDWRStorage),
    ContainerStorageSpacePath = host_storage_path(RDWRStorageMountPoint, ?SPACE_ID, <<"">>),
    touch(W1, ContainerStorageSpacePath),

    enable_continuous_scans(Config, ?SPACE_ID),

    ListedFiles = [FileToDeleteOnStorage, FileToDeleteByLFM] = receive
        {waiting, Pid, 0, {ok, Files}} ->
            % continue sync
            Pid ! continue,
            Files
    end,

    FileToDeleteByLFMPath = ?SPACE_TEST_FILE_PATH(FileToDeleteByLFM),
    FileToDeleteOnStoragePath = provider_storage_path(?SPACE_ID, FileToDeleteOnStorage),

    receive
        {waiting, Pid2, 2, _} ->
            % delete 2 first files so that third will be placed in the first batch
            ok = lfm_proxy:unlink(W1, SessId, {path, FileToDeleteByLFMPath}),
            SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, FileToDeleteOnStoragePath, RDWRStorage),
            ok = sd_test_utils:unlink(W1, SDHandle, ?TEST_DATA_SIZE),
            % continue sync
            Pid2 ! continue
    end,

    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 0, % no deleted files because FileToDeleteOnStorage deletion will be detected in next scan
        <<"failed">> => 0,
        <<"createdSum">> => 0,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
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
    enable_continuous_scans(Config, ?SPACE_ID),
    assertScanFinished(W1, ?SPACE_ID, 3, ?ATTEMPTS),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"toProcess">> => 4,
        <<"created">> => 0,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"createdSum">> => 0,
        <<"deletedSum">> => 1,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
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
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W2, Handle4, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle4).

append_file_update_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Append to file
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, ?TEST_DATA_SIZE, ?TEST_DATA2),
    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

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
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

append_file_not_changing_mtime_update_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    RDWRStorage = get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    HostStorageTestFilePath = host_storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Append to file
    {ok, #statbuf{st_mtime = StMtime}} = sd_test_utils:stat(W1, SDHandle),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, ?TEST_DATA_SIZE, ?TEST_DATA2),
    change_time(HostStorageTestFilePath, StMtime),

    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

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
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

append_empty_file_update_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, <<"">>),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Append to file
    ?assertEqual({ok, ?TEST_DATA_SIZE2},
        sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA2)),
    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

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
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

copy_file_update_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    % we need W1MountPoint as cp is performed via file not storage_driver module
    SrcFilePath = host_storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1),
    DestFilePath = host_storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE2),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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

    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"createdSum">> => 2,
        <<"modifiedSum">> => 2,
        <<"deletedSum">> => 0,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedHourHist">> => 2,
        <<"modifiedDayHist">> => 2,
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
    [W1, _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    SrcStorageFilePath = host_storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1),
    DestStorageFilePath = host_storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE2),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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
    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),
    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"created">> => 1,
        <<"modified">> => 0,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"createdSum">> => 2,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 1,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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

truncate_file_update_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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

    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

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
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

change_file_content_constant_size_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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
    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

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
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

change_file_content_update_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    RDWRStorage = get_rdwr_storage(Config, W1),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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
    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

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
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

change_file_content_the_same_moment_when_sync_performs_stat_on_file_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    HostStorageTestFilePath = host_storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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
    %was after storage import performed stat on the file
    {ok, _} = rpc:call(W1, storage_sync_info, create_or_update,
        [StorageTestFilePath, ?SPACE_ID, fun(SSI) -> {ok, SSI#storage_sync_info{last_stat = StatTime}} end]),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, ?CHANGED_BYTE_OFFSET, ?CHANGED_BYTE),
    change_time(HostStorageTestFilePath, StatTime),
    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

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
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

chmod_file_update_test(Config) ->
    [W1, W2] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),

    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestFileinDirPath1 = provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),

    NewMode = 8#600,
    %% Create dirs on storage
    DirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, DirSDHandle, 8#775),
    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

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
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle2),


    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"created">> => 2,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 2,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 2,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    test_utils:mock_new(W1, storage_import_hash, [passthrough]),

    timer:sleep(timer:seconds(2)), %ensure that modify time is different from read time
    %% Change file permissions
    sd_test_utils:chmod(W1, SDHandle, NewMode),

    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    %% Check if file permissions were changed
    ?assertMatch({ok, #file_attr{mode = NewMode}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    History = rpc:call(W1, meck, history, [storage_import_hash]),
    assert_num_results_gte(History, ?assertHashChangedFun(StorageTestDirPath, ?SPACE_ID, true), 1),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 2,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 2,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

chmod_file_update2_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),

    Files = lists:map(fun(TestFile) ->
        filename:join([?TEST_DIR, TestFile])
    end, [?TEST_FILE1, ?TEST_FILE2, ?TEST_FILE3]),

    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestDirPath2 = provider_storage_path(?SPACE_ID, ?TEST_DIR2),

    [StTestFile1 | _] = StorageFiles = to_storage_files(Files, ?SPACE_ID),
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
    enable_initial_scan(Config, ?SPACE_ID),

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

    test_utils:mock_new(W1, storage_import_hash, [passthrough]),
    test_utils:mock_new(W1, storage_sync_traverse, [passthrough]),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 8,
        <<"created">> => 5,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"createdSum">> => 5,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 5,
        <<"createdHourHist">> => 5,
        <<"createdDayHist">> => 5,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Change file permissions
    timer:sleep(timer:seconds(2)),
    SDFileHandle1 = sd_test_utils:new_handle(W1, ?SPACE_ID, StTestFile1, RDWRStorage),
    sd_test_utils:chmod(W1, SDFileHandle1, NewMode),
    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    %% Check if file permissions were changed
    ?assertMatch({ok, #file_attr{mode = NewMode}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    History = rpc:call(W1, meck, history, [storage_import_hash]),
    History2 = rpc:call(W1, meck, history, [storage_sync_traverse]),
    test_utils:mock_unload(W1, storage_import_hash),

    assert_num_results_gte(History, ?assertHashChangedFun(StorageTestDirPath, ?SPACE_ID, true), 1),
    assert_num_results(History2, ?assertMtimeChangedFun(StorageTestDirPath, ?SPACE_ID, true), 0),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 5,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 5,
        <<"createdHourHist">> => 5,
        <<"createdDayHist">> => 5,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

change_file_type_test(Config) ->
    % this test checks whether storage import properly handles
    % deleting file and creating directory with the same name on storage
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),

    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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

    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"created">> => 2,
        <<"modified">> => 1,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 3,
        <<"modifiedSum">> => 2,
        <<"deletedSum">> => 1,
        <<"createdMinHist">> => 2,
        <<"createdHourHist">> => 3,
        <<"createdDayHist">> => 3,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 2,
        <<"modifiedDayHist">> => 2,
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

change_file_type2_test(Config) ->
    % this test checks whether storage import properly handles
    % deleting empty directory and creating file with the same name on storage
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),

    %% Create dir on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#755),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    %% Check if dir was imported on W1
    {ok, #file_attr{guid = DirGuid}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, []},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 1)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    ok = sd_test_utils:rmdir(W1, SDHandle),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),

    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 2,
        <<"modifiedSum">> => 2,
        <<"deletedSum">> => 1,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 2,
        <<"modifiedDayHist">> => 2,
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

    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ok = lfm_proxy:close(W2, Handle2).

change_file_type3_test(Config) ->
    % this test checks whether storage import properly handles
    % deleting non-empty directory and creating file with the same name on storage
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),

    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestFileInDirPath = provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),

    %% Create dir on storage and file inside it
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    DirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    FileSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileInDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, DirSDHandle, 8#755),
    ok = sd_test_utils:create_file(W1, FileSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileSDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    %% Check if dir was imported on W1
    {ok, #file_attr{guid = DirGuid}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    {ok, [{FileGuid, _}]} = ?assertMatch({ok, [{_, _}]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 1)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"created">> => 2,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 2,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 2,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),


    ok = sd_test_utils:unlink(W1, FileSDHandle, ?TEST_DATA_SIZE),
    ok = sd_test_utils:rmdir(W1, DirSDHandle),
    ok = sd_test_utils:create_file(W1, DirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, DirSDHandle, 0, ?TEST_DATA),

    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 2,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 3,
        <<"modifiedSum">> => 2,
        <<"deletedSum">> => 2,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 3,
        <<"createdDayHist">> => 3,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 2,
        <<"modifiedDayHist">> => 2,
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

    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ok = lfm_proxy:close(W2, Handle2).

change_file_type4_test(Config) ->
    % this test checks whether storage import properly handles
    % deleting non-empty directory, created in remote provider and creating file with the same name on storage
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestFileInDirPath = provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),

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

    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 4,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 2,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 2,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
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

    {ok, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W2, Handle4, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ok = lfm_proxy:close(W2, Handle4).

update_timestamps_file_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        host_storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
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
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    timer:sleep(timer:seconds(2)),
    %% Change file permissions
    NewTimestamp = 9999999999,
    change_time(StorageTestFilePath, NewTimestamp, NewTimestamp),
    enable_continuous_scans(Config, ?SPACE_ID),
    assertScanFinished(W1, ?SPACE_ID, 2, ?ATTEMPTS),
    disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if timestamps were changed
    ?assertMatch({ok, #file_attr{atime = NewTimestamp, mtime = NewTimestamp}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS).

should_not_detect_timestamp_update_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = host_storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID, ?ATTEMPTS),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Change file permissions
    change_time(StorageTestFilePath, 1, 1),
    enable_continuous_scans(Config, ?SPACE_ID),
    assertScanFinished(W1, ?SPACE_ID, 2, ?ATTEMPTS),
    disable_continuous_scan(Config),

    %% Check if timestamps hasn't changed
    ?assertNotMatch({ok, #file_attr{atime = 1, mtime = 1}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1})),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 1,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

update_nfs_acl_test(Config) ->
    Workers = [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER2, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    RDWRStorage = get_rdwr_storage(Config, W1),

    %% Create file on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID, ?ATTEMPTS),

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
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 1,
        <<"modifiedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    EncACL = storage_import_acl:encode(?ACL2),
    ok = test_utils:mock_expect(Workers, storage_driver, getxattr, fun
        (Handle = #sd_handle{file = <<"/">>}, Name) ->
            meck:passthrough([Handle, Name]);
        (Handle = #sd_handle{file = <<"/space1">>}, Name) ->
            meck:passthrough([Handle, Name]);
        (#sd_handle{}, _) ->
            {ok, EncACL}
    end),
    enable_continuous_scans(Config, ?SPACE_ID),
    assertScanFinished(W1, ?SPACE_ID, 2, ?ATTEMPTS),
    disable_continuous_scan(Config),

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
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdSum">> => 1,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

recreate_file_deleted_by_sync_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    enable_initial_scan(Config, ?SPACE_ID),

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

    assertInitialScanFinished(W1, ?SPACE_ID),

    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),

    % ensure that dir mtime will change
    timer:sleep(timer:seconds(1)),

    %% delete file on storage
    ok = sd_test_utils:unlink(W1, SDHandle, ?TEST_DATA_SIZE),

    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

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

sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    enable_initial_scan(Config, ?SPACE_ID),

    {ok, FileGuid} =
        ?assertMatch({ok, _}, lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH1, 8#777)),
    {ok, FileHandle} =
        ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, FileGuid}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W2, FileHandle, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:fsync(W2, FileHandle)),
    ok = lfm_proxy:close(W2, FileHandle),

    assertInitialScanFinished(W1, ?SPACE_ID),

    %check if file_meta was synced
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

    %% Create file on storage to trigger update
    StorageRandomFilePath = provider_storage_path(?SPACE_ID, <<"random_file">>),
    RandomSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageRandomFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, RandomSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, RandomSDHandle, 0, ?TEST_DATA),

    enable_continuous_scans(Config, ?SPACE_ID),
    %% wait until updates finishes
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_FILE1),
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

sync_should_not_delete_dir_created_in_remote_provider(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    enable_initial_scan(Config, ?SPACE_ID),
    RDWRStorage = get_rdwr_storage(Config, W1),

    % Create dir in space
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId2, ?SPACE_TEST_DIR_PATH)),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

    assertInitialScanFinished(W1, ?SPACE_ID),

    %% Create file on storage to trigger update
    StorageRandomFilePath = provider_storage_path(?SPACE_ID, <<"random_file">>),
    RandomSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageRandomFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, RandomSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, RandomSDHandle, 0, ?TEST_DATA),
    enable_continuous_scans(Config, ?SPACE_ID),
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    StorageTestFilePath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),

    %% file shouldn't appear on W1's storage
    ?assertMatch({error, ?ENOENT}, sd_test_utils:stat(W1, SDHandle)),

    % Ensure that storage import didn't delete remotely create directory
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS).

sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    enable_initial_scan(Config, ?SPACE_ID),

    % Create dir in space
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId2, ?SPACE_TEST_DIR_PATH)),
    {ok, FileGuid} =
        ?assertMatch({ok, _}, lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_IN_DIR_PATH, 8#777)),
    {ok, FileHandle} =
        ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, FileGuid}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W2, FileHandle, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:fsync(W2, FileHandle)),
    ok = lfm_proxy:close(W2, FileHandle),

    assertInitialScanFinished(W1, ?SPACE_ID),

    %check if file_meta was synced
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),

    %% Create file on storage to trigger update
    StorageRandomFilePath =
        provider_storage_path(?SPACE_ID, <<"random_file">>),
    RandomSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageRandomFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, RandomSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, RandomSDHandle, 0, ?TEST_DATA),

    enable_continuous_scans(Config, ?SPACE_ID),
    %% wait until updates finishes
    assertSecondScanFinished(W1, ?SPACE_ID),
    disable_continuous_scan(Config),

    StorageTestFilePath = provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),
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
    % storage import scans are set in init_per_testcase to be executed with interval of 1 seconds
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
    ?assertBlocks(W1, SessId, [
        #{
            <<"blocks">> => [[0, ?TEST_DATA_SIZE]],
            <<"providerId">> => ?GET_DOMAIN_BIN(W2),
            <<"totalBlocksSize">> => ?TEST_DATA_SIZE
        }
    ], FileGuid),

    enable_initial_scan(Config, ?SPACE_ID),
    enable_continuous_scans(Config, ?SPACE_ID),

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

    % ensure that storage import did not invalidate file blocks
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

    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"created">> => 0,
        <<"modified">> => 2,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"createdSum">> => 0,
        <<"deletedSum">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"modifiedMinHist">> => 2,
        <<"modifiedHourHist">> => 2,
        <<"modifiedDayHist">> => 2,
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

create_init_file(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SpaceDir = provider_storage_path(?SPACE_ID, <<"">>),
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

start_scan(Worker, SpaceId) ->
    ?assertMatch(ok, rpc:call(Worker, storage_import, start_auto_scan, [SpaceId])).

stop_scan(Worker, SpaceId) ->
    case rpc:call(Worker, storage_import, stop_auto_scan, [SpaceId]) of
        ok -> ok;
        {error, not_found} -> ok
    end.

enable_initial_scan(Config, SpaceId) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    ImportConfig = ?config(import_config, Config, #{}),
    MaxDepth = maps:get(max_depth, ImportConfig, ?MAX_DEPTH),
    SyncAcl = maps:get(sync_acl, ImportConfig, ?SYNC_ACL),
    ?assertMatch(ok, rpc:call(W1, storage_import, set_or_configure_auto_mode,
        [SpaceId, #{max_depth => MaxDepth, sync_acl => SyncAcl}])).

enable_continuous_scans(Config, SpaceId) ->
    enable_continuous_scans(Config, SpaceId, #{}).

enable_continuous_scans(Config, SpaceId, Opts) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    UpdateConfig = ?config(update_config, Config, #{}),
    MaxDepth = maps:get(max_depth, UpdateConfig, ?MAX_DEPTH),
    ScanInterval = maps:get(scan_interval, UpdateConfig, ?SCAN_INTERVAL),
    DetectModifications = maps:get(detect_modifications, UpdateConfig, ?DETECT_MODIFICATIONS),
    DetectDeletions = maps:get(detect_deletions, UpdateConfig, ?DETECT_DELETIONS),
    SyncAcl = maps:get(sync_acl, UpdateConfig, ?SYNC_ACL),
    DefaultOpts = #{
        continuous_scan => true,
        max_depth => MaxDepth,
        scan_interval => ScanInterval,
        detect_modifications => DetectModifications,
        detect_deletions => DetectDeletions,
        sync_acl => SyncAcl
    },
    ok = rpc:call(W1, storage_import, set_or_configure_auto_mode, [SpaceId, maps:merge(DefaultOpts, Opts)]).

cleanup_storage_import_monitoring_model(Worker, SpaceId) ->
    rpc:call(Worker, storage_import_monitoring, delete, [SpaceId]).

monitoring_describe(Worker, SpaceId) ->
    rpc:call(Worker, storage_import_monitoring, describe, [SpaceId]).

get_finished_scans_num(Worker, SpaceId) ->
    #{<<"scans">> := Scans} = monitoring_describe(Worker, SpaceId),
    Scans.

get_scan_config(Worker, SpaceId) ->
    {ok, ImportConfig} = rpc:call(Worker, storage_import, get_configuration, [SpaceId]),
    {ok, maps:get(auto_storage_import_config, ImportConfig)}.

disable_continuous_scan(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    rpc:call(W1, storage_import, set_or_configure_auto_mode, [?SPACE_ID, #{continuous_scan => false}]),
    ?assertMatch({ok, #{continuous_scan := false}}, get_scan_config(W1, ?SPACE_ID), ?ATTEMPTS),
    ok.

disable_storage_sync(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    disable_continuous_scan(Config),
    stop_scan(W1, ?SPACE_ID),
    assertNoScanInProgress(W1, ?SPACE_ID, 600),
    ok = rpc:call(W1, storage_import_config, delete, [?SPACE_ID]).

clean_synced_storage(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    Storage = get_rdwr_storage(Config, W1),
    SpaceDir = provider_storage_path(?SPACE_ID, <<"">>),
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
    Guids = lists:map(fun({Guid, _}) ->
        ok = lfm_proxy:rm_recursive(W, ?ROOT_SESS_ID, {guid, Guid}),
        ok = worker_pool:cast(?VERIFY_POOL, {?MODULE, verify_file_deleted, [W2, Guid, Self, Attempts]}),
        Guid
    end, Children),
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

is_rdwr(StorageId, #helper{name = ?POSIX_HELPER_NAME}) ->
    match =:= re:run(StorageId, <<"rdwr_storage">>, [{capture, none}]);
is_rdwr(<<"rdwr_storage">>, #helper{name = ?S3_HELPER_NAME}) ->
    true;
is_rdwr(_, _) ->
    false.

is_synced(StorageId, #helper{name = ?POSIX_HELPER_NAME}) ->
    match =:= re:run(StorageId, <<"synced_storage">>, [{capture, none}]);
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

get_storage_path(Config, MountPath) when is_list(MountPath) ->
    get_storage_path(Config, list_to_atom(MountPath));
get_storage_path(Config, MountPath) when is_binary(MountPath) ->
    get_storage_path(Config, binary_to_atom(MountPath, latin1));
get_storage_path(Config, MountPath) when is_atom(MountPath) ->
    atom_to_binary(?config(host_path,
        ?config(MountPath,
            ?config(posix,
                ?config(storages, Config)))), latin1).

provider_storage_path(SpaceId, File) ->
    provider_storage_path(SpaceId, File, true).

provider_storage_path(_SpaceId, File, _MountInRoot = true) ->
    filename:join([<<"/">>, File]);
provider_storage_path(SpaceId, FileName, _MountInRoot = false) ->
    filename:join([<<"/">>, SpaceId, FileName]).

host_storage_path(MountPath, _SpaceId, File) ->
    filename:join([MountPath, File]).

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

to_storage_files(Files, SpaceId) ->
    [provider_storage_path(SpaceId, F) || F <- Files].

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

assertInitialScanFinished(Worker, SpaceId) ->
    assertInitialScanFinished(Worker, SpaceId, ?ATTEMPTS).

assertInitialScanFinished(Worker, SpaceId, Attempts) ->
    ?assertEqual(true, try
        rpc:call(Worker, storage_import_monitoring, is_initial_scan_finished, [SpaceId])
    catch
        _:_ ->
            error
    end, Attempts).

assertSecondScanFinished(Worker, SpaceId) ->
    assertScanFinished(Worker, SpaceId, 2).

assertScanFinished(Worker, SpaceId, ScanNo) ->
    assertScanFinished(Worker, SpaceId, ScanNo, ?ATTEMPTS).

assertScanFinished(Worker, SpaceId, ScanNo, Attempts) ->
    ?assertEqual(true, try
        rpc:call(Worker, storage_import_monitoring, is_scan_finished, [SpaceId, ScanNo])
    catch
        _:_ ->
            error
    end, Attempts).

assertNoScanInProgress(Worker, SpaceId, Attempts) ->
    ?assertEqual(false, try
        rpc:call(Worker, storage_import_monitoring, is_scan_in_progress, [SpaceId])
    catch
        _:_ ->
            error
    end, Attempts).

get_last_stat_timestamp(Worker, FilePath, SpaceId) ->
    {ok, #document{value = #storage_sync_info{last_stat = StatTime}}} =
        rpc:call(Worker, storage_sync_info, get, [FilePath, SpaceId]),
    StatTime.

assert_monitoring_state(Worker, ExpectedSSM, SpaceId, Attempts) ->
    SSM = monitoring_describe(Worker, SpaceId),
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
                    {Format, Args} = storage_import_monitoring_description(SSM),
                    ct:pal(
                        "Assertion of field \"~p\" in storage_import_monitoring for space ~p failed.~n"
                        "    Expected: ~p~n"
                        "    Value: ~p~n"
                        ++ Format ++
                        "~nStacktrace:~n~p",
                        [Key, SpaceId, ExpectedValue, Value] ++ Args ++ [erlang:get_stacktrace()]),
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
        <<"createdMinHist">> => lists:sum(lists:sublist(maps:get(<<"createdMinHist">>, SSM), 2)),
        <<"modifiedMinHist">> => lists:sum(lists:sublist(maps:get(<<"modifiedMinHist">>, SSM), 2)),
        <<"deletedMinHist">> => lists:sum(lists:sublist(maps:get(<<"deletedMinHist">>, SSM), 2)),

        <<"createdHourHist">> => lists:sum(lists:sublist(maps:get(<<"createdHourHist">>, SSM), 3)),
        <<"modifiedHourHist">> => lists:sum(lists:sublist(maps:get(<<"modifiedHourHist">>, SSM), 3)),
        <<"deletedHourHist">> => lists:sum(lists:sublist(maps:get(<<"deletedHourHist">>, SSM), 3)),

        <<"createdDayHist">> => lists:sum(lists:sublist(maps:get(<<"createdDayHist">>, SSM), 1)),
        <<"modifiedDayHist">> => lists:sum(lists:sublist(maps:get(<<"modifiedDayHist">>, SSM), 1)),
        <<"deletedDayHist">> => lists:sum(lists:sublist(maps:get(<<"deletedDayHist">>, SSM), 1))
    }.

storage_import_monitoring_description(SSM) ->
    maps:fold(fun(Key, Value, {AccFormat, AccArgs}) ->
        {AccFormat ++ "    ~p = ~p~n", AccArgs ++ [Key, Value]}
    end, {"~n#storage_import_monitoring fields values:~n", []}, SSM).

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
    ok = test_utils:mock_new(Worker, storage_import_engine),
    ok = test_utils:mock_expect(Worker, storage_import_engine, import_file_unsafe,
        fun(StorageFileCtx, Info) ->
            FileName = storage_file_ctx:get_file_name_const(StorageFileCtx),
            case FileName of
                ErroneousFile -> throw(test_error);
                _ -> meck:passthrough([StorageFileCtx, Info])
            end
        end
    ).

unmock_import_file_error(Worker) ->
    ok = test_utils:mock_unload(Worker, storage_import_engine).

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
        rpc:call(W1, auto_storage_import_worker, notify_connection_to_oz, []),
        NewConfig2
    end,
    {ok, _} = application:ensure_all_started(worker_pool),
    {ok, _} = worker_pool:start_sup_pool(?VERIFY_POOL, [{workers, 8}]),
    [{?LOAD_MODULES, [initializer, storage_import_test_base, sd_test_utils]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

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

init_per_testcase(force_stop_test, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, OldDirBatchSize} = test_utils:get_env(W1, op_worker, storage_import_dir_batch_size),
    test_utils:set_env(W1, op_worker, storage_import_dir_batch_size, 1),
    Config2 = [
        {update_config, #{
            detect_deletions => false,
            detect_modifications => false}},
        {old_storage_import_dir_batch_size, OldDirBatchSize}
        | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(should_not_detect_timestamp_update_test, Config) ->
    Config2 = [
        {update_config, #{
            detect_deletions => false,
            detect_modifications => false}} | Config
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
            detect_deletions => true,
            detect_modifications => false}} | Config
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
            detect_deletions => true,
            detect_modifications => true}} | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(create_file_in_dir_exceed_batch_update_test, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, OldDirBatchSize} = test_utils:get_env(W1, op_worker, storage_import_dir_batch_size),
    test_utils:set_env(W1, op_worker, storage_import_dir_batch_size, 2),
    Config2 = [
        {update_config, #{
            detect_deletions => false,
            detect_modifications => false}},
        {old_storage_import_dir_batch_size, OldDirBatchSize}
        | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(chmod_file_update2_test, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, OldDirBatchSize} = test_utils:get_env(W1, op_worker, storage_import_dir_batch_size),
    test_utils:set_env(W1, op_worker, storage_import_dir_batch_size, 2),
    Config2 = [{old_storage_import_dir_batch_size, OldDirBatchSize} | Config],
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

    EncACL = storage_import_acl:encode(?ACL),
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
    EncACL = storage_import_acl:encode(?ACL),
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
    {ok, OldDirBatchSize} = test_utils:get_env(W1, op_worker, storage_import_dir_batch_size),
    test_utils:set_env(W1, op_worker, storage_import_dir_batch_size, 2),
    Config2 = [
        {update_config, #{
            detect_deletions => true,
            detect_modifications => true}},
        {old_storage_import_dir_batch_size, OldDirBatchSize}
        | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(should_not_sync_file_during_replication, Config) ->
    Config2 = [
        {update_config, #{
            scan_interval => 1,
            detect_deletions => false,
            detect_modifications => false}}
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
            detect_deletions => false,
            detect_modifications => false}}
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
    create_init_file(Config3),
    Config3.

end_per_testcase(Case, Config)
    when Case =:= chmod_file_update2_test
    orelse Case =:= create_file_in_dir_exceed_batch_update_test
    orelse Case =:= create_list_race_test ->

    [W1 | _] = Workers = ?config(op_worker_nodes, Config),
    OldDirBatchSize = ?config(old_storage_import_dir_batch_size, Config),
    test_utils:mock_unload(Workers, [storage_driver]),
    test_utils:set_env(W1, op_worker, storage_import_dir_batch_size, OldDirBatchSize),
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

end_per_testcase(force_stop_test, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    OldDirBatchSize = ?config(old_storage_import_dir_batch_size, Config),
    test_utils:set_env(W1, op_worker, storage_import_dir_batch_size, OldDirBatchSize),
    end_per_testcase(default, Config);

end_per_testcase(sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, [fslogic_path]),
    end_per_testcase(default, Config);

end_per_testcase(create_remote_dir_import_race_test, Config) ->
    [_W1, W2| _] = ?config(op_worker_nodes, Config),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_ID),
    remove_link(W2, SpaceUuid, ?TEST_DIR),
    end_per_testcase(default, Config);

end_per_testcase(create_remote_file_import_race_test, Config) ->
    [_W1, W2| _] = ?config(op_worker_nodes, Config),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_ID),
    remove_link(W2, SpaceUuid, ?TEST_FILE1),
    end_per_testcase(default, Config);

end_per_testcase(sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage, Config) ->
    [W1| _] = ?config(op_worker_nodes, Config),
    % remove stalled deletion link
    TestDir = ?config(test_dir, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_ID),
    SpaceCtx = file_ctx:new_by_guid(SpaceGuid),
    remove_deletion_link(W1, ?SPACE_ID, TestDir, SpaceCtx),
    ok = test_utils:mock_unload(W1, helpers),
    end_per_testcase(default, Config);

end_per_testcase(sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage, Config) ->
    [W1| _] = ?config(op_worker_nodes, Config),
    % remove stalled deletion link
    TestFile = ?config(test_file, Config),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_ID),
    SpaceCtx = file_ctx:new_by_guid(SpaceGuid),
    remove_deletion_link(W1, ?SPACE_ID, TestFile, SpaceCtx),
    ok = test_utils:mock_unload(W1, helpers),
    end_per_testcase(default, Config);

end_per_testcase(should_not_sync_file_during_replication, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(W1, [rtransfer_config]),
    end_per_testcase(default, Config);

end_per_testcase(_Case, Config) ->
    Workers = [W1 | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(W) -> lfm_proxy:close_all(W) end, Workers),
    clean_luma_db(W1),
    disable_storage_sync(Config),
    clean_traverse_tasks(W1),
    clean_space(Config),
    clean_synced_storage(Config),
    cleanup_storage_import_monitoring_model(W1, ?SPACE_ID),
    test_utils:mock_unload(Workers, [storage_import_engine, storage_import_hash, link_utils,
        storage_sync_traverse, storage_import_deletion, storage_driver, helpers]),
    timer:sleep(timer:seconds(1)),
    lfm_proxy:teardown(Config).