%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage_sync with readonly storage.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_sync_readonly_test_SUITE).
-author("Rafal Slota").
-author("Jakub Kudzia").

-include("modules/storage_sync/strategy_config.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("kernel/include/file.hrl").
-include("storage_sync_test.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").


%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    create_directory_import_test/1,
    create_file_import_test/1,
    import_file_with_link_but_no_doc_test/1,
    create_file_in_dir_update_test/1,
    delete_file_update_test/1,
    append_file_update_test/1,
    copy_file_update_test/1,
    move_file_update_test/1,
    truncate_file_update_test/1,
    chmod_file_update_test/1,
    chmod_file_update2_test/1,
    update_timestamps_file_import_test/1,
    create_file_in_dir_import_test/1,
    create_file_in_dir_exceed_batch_update_test/1,
    create_directory_import_many_test/1,
    create_subfiles_import_many_test/1,
    delete_non_empty_directory_update_test/1,
    delete_empty_directory_update_test/1,
    should_not_detect_timestamp_update_test/1,
    create_directory_import_without_read_permission_test/1,
    create_subfiles_import_many2_test/1,
    create_subfiles_and_delete_before_import_is_finished_test/1,
    create_directory_import_check_user_id_test/1,
    create_directory_import_check_user_id_error_test/1,
    create_file_import_check_user_id_test/1,
    create_file_import_check_user_id_error_test/1,
    import_nfs_acl_test/1,
    update_nfs_acl_test/1,
    import_nfs_acl_with_disabled_luma_should_fail_test/1,
    create_directory_import_error_test/1,
    delete_and_update_files_simultaneously_update_test/1,
    update_syncs_files_after_import_failed_test/1,
    update_syncs_files_after_previous_update_failed_test/1,
    sync_works_properly_after_delete_test/1,
    sync_should_not_delete_not_replicated_file_created_in_remote_provider/1,
    sync_should_not_delete_dir_created_in_remote_provider/1,
    sync_should_not_delete_not_replicated_files_created_in_remote_provider2/1,
    change_file_content_update_test/1,
    change_file_content_update2_test/1, create_empty_file_import_test/1,
    append_empty_file_update_test/1
]).

-define(TEST_CASES, [
    create_directory_import_test,
    create_directory_import_error_test,
    update_syncs_files_after_import_failed_test,
    update_syncs_files_after_previous_update_failed_test,
    create_directory_import_check_user_id_test,
    create_directory_import_check_user_id_error_test,
    create_directory_import_without_read_permission_test,
    create_directory_import_many_test,
    create_file_import_test,
    import_file_with_link_but_no_doc_test,
    create_empty_file_import_test,
    create_file_import_check_user_id_test,
    create_file_import_check_user_id_error_test,
    create_file_in_dir_import_test,
    create_subfiles_import_many_test,
    create_subfiles_import_many2_test,
    create_subfiles_and_delete_before_import_is_finished_test,
    create_file_in_dir_update_test,
    create_file_in_dir_exceed_batch_update_test,
    delete_empty_directory_update_test,
    delete_non_empty_directory_update_test,
    sync_works_properly_after_delete_test,
    delete_and_update_files_simultaneously_update_test,
    delete_file_update_test,
    append_empty_file_update_test,
    append_file_update_test,
    copy_file_update_test,
    move_file_update_test,
    truncate_file_update_test,
    change_file_content_update_test,
    change_file_content_update2_test,
    chmod_file_update_test,
    chmod_file_update2_test,
    update_timestamps_file_import_test,
    should_not_detect_timestamp_update_test,
    sync_should_not_delete_not_replicated_file_created_in_remote_provider,
    sync_should_not_delete_dir_created_in_remote_provider,
    sync_should_not_delete_not_replicated_files_created_in_remote_provider2,
    import_nfs_acl_test,
    update_nfs_acl_test,
    import_nfs_acl_with_disabled_luma_should_fail_test
    %%    import_file_by_path_test, %todo uncomment after resolving and merging with VFS-3052
    %%    get_child_attr_by_path_test,
    %%    import_remote_file_by_path_test
]).

all() -> ?ALL(?TEST_CASES).

%%%==================================================================
%%% Test functions
%%%===================================================================

create_directory_import_test(Config) ->
    storage_sync_test_base:create_directory_import_test(Config, true).

create_directory_import_error_test(Config) ->
    storage_sync_test_base:create_directory_import_error_test(Config, true).

update_syncs_files_after_import_failed_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath =
        storage_sync_test_base:storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, true),

    %% Create dir on storage
    ok = file:make_dir(StorageTestDirPath),
    %ensure that import will start with different timestamp than dir was created
    timer:sleep(timer:seconds(1)),

    test_utils:mock_new(W1, simple_scan),
    test_utils:mock_expect(W1, simple_scan, import_file,
        fun(Job = #space_strategy_job{
            data = #{file_name := FileName}
        }, _) ->
            case FileName of
                ?TEST_DIR ->
                    throw(test_error);
                _ ->
                    meck:passthrough([Job])
            end
        end),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    %% Check if dir was not imported
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH})),
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH})),

    ?assertMonitoring(W1, #{
        <<"scans">> := 1,
        <<"toProcess">> := 2,
        <<"imported">> := 0,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 1,
        <<"otherProcessed">> := 0,
        <<"importedSum">> := 0,
        <<"updatedSum">> := 1,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [0 | _],
        <<"importedHourHist">> := [0 | _],
        <<"importedDayHist">> := [0 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [1 | _],
        <<"updatedDayHist">> := [1 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    test_utils:mock_unload(W1, simple_scan),
    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_storage_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> := 2,
        <<"toProcess">> := 2,
        <<"imported">> := 1,
        <<"updated">> := 0,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 1,
        <<"importedSum">> := 1,
        <<"updatedSum">> := 1,
        <<"importedHourHist">> := [1 | _],
        <<"importedDayHist">> := [1 | _],
        <<"updatedHourHist">> := [1 | _],
        <<"updatedDayHist">> := [1 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS).

update_syncs_files_after_previous_update_failed_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath =
        storage_sync_test_base:storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, true),

    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> := 1,
        <<"toProcess">> := 1,
        <<"imported">> := 0,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 0,
        <<"importedSum">> := 0,
        <<"updatedSum">> := 1,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [0 | _],
        <<"importedHourHist">> := [0 | _],
        <<"importedDayHist">> := [0 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [1 | _],
        <<"updatedDayHist">> := [1 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    %% Create dir on storage
    timer:sleep(timer:seconds(1)),
    ok = file:make_dir(StorageTestDirPath),
    timer:sleep(timer:seconds(1)),
    test_utils:mock_new(W1, simple_scan),
    test_utils:mock_expect(W1, simple_scan, import_file, fun(Job = #space_strategy_job{
        data = #{file_name := FileName}}, _
    ) ->
        case FileName of
            ?TEST_DIR ->
                throw(test_error);
            _ ->
                meck:passthrough([Job])
        end
    end),
    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    test_utils:mock_unload(W1, simple_scan),

    ?assertMonitoring(W1, #{
        <<"scans">> := 2,
        <<"toProcess">> := 2,
        <<"imported">> := 0,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 1,
        <<"otherProcessed">> := 0,
        <<"importedSum">> := 0,
        <<"updatedSum">> := 2,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [0 | _],
        <<"importedHourHist">> := [0 | _],
        <<"importedDayHist">> := [0 | _],
        <<"updatedHourHist">> := [2 | _],
        <<"updatedDayHist">> := [2 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    %% Check if dir was not imported
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH})),
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH})),

    %next scan should import file
    ?assertMonitoring(W1, #{
        <<"scans">> := 3,
        <<"toProcess">> := 2,
        <<"imported">> := 1,
        <<"updated">> := 0,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 1,
        <<"importedSum">> := 1,
        <<"updatedSum">> := 2,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [1 | _],
        <<"importedHourHist">> := [1 | _],
        <<"importedDayHist">> := [1 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [2 | _],
        <<"updatedDayHist">> := [2 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID, ?ATTEMPTS),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS).

create_directory_import_check_user_id_test(Config) ->
    storage_sync_test_base:create_directory_import_check_user_id_test(Config, true).

create_directory_import_check_user_id_error_test(Config) ->
    storage_sync_test_base:create_directory_import_check_user_id_error_test(Config, true).

create_directory_import_without_read_permission_test(Config) ->
    storage_sync_test_base:create_directory_import_without_read_permission_test(Config, true).

create_directory_import_many_test(Config) ->
    storage_sync_test_base:create_directory_import_many_test(Config, true).

create_file_import_test(Config) ->
    storage_sync_test_base:create_file_import_test(Config, true).

import_file_with_link_but_no_doc_test(Config) ->
    storage_sync_test_base:import_file_with_link_but_no_doc_test(Config, true).

create_empty_file_import_test(Config) ->
    storage_sync_test_base:create_empty_file_import_test(Config, true).

create_file_import_check_user_id_test(Config) ->
    storage_sync_test_base:create_file_import_check_user_id_test(Config, true).

create_file_import_check_user_id_error_test(Config) ->
    storage_sync_test_base:create_file_import_check_user_id_error_test(Config, true).

create_file_in_dir_import_test(Config) ->
    storage_sync_test_base:create_file_in_dir_import_test(Config, true).

create_subfiles_import_many_test(Config) ->
    storage_sync_test_base:create_subfiles_import_many_test(Config, true).

create_subfiles_import_many2_test(Config) ->
    storage_sync_test_base:create_subfiles_import_many2_test(Config, true).

create_subfiles_and_delete_before_import_is_finished_test(Config) ->
    storage_sync_test_base:create_subfiles_and_delete_before_import_is_finished_test(Config, true).

create_file_in_dir_update_test(Config) ->
    MountSpaceInRoot = true,
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),

    StorageTestDirPath =
        storage_sync_test_base:storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 =
        storage_sync_test_base:storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    StorageTestFileinDirPath1 =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

    %% Create dirs on storage
    ok = file:make_dir(StorageTestDirPath),
    ok = file:make_dir(StorageTestDirPath2),
    ok = file:change_mode(StorageTestDirPath, 8#777),
    ok = file:change_mode(StorageTestDirPath2, 8#777),

    storage_sync_test_base:enable_storage_import(Config),

    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    %% Check if dirs were imported on W1
    {ok, #file_attr{guid = TestDirGuid1}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    {ok, #file_attr{guid = TestDirGuid2}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH2}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> := 1,
        <<"toProcess">> := 3,
        <<"imported">> := 2,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 0,
        <<"importedSum">> := 2,
        <<"updatedSum">> := 1,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [2 | _],
        <<"importedHourHist">> := [2 | _],
        <<"importedDayHist">> := [2 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [1 | _],
        <<"updatedDayHist">> := [1 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    test_utils:mock_new(W1, storage_sync_changes, [passthrough]),

    ok = file:write_file(StorageTestFileinDirPath1, ?TEST_DATA),
    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_storage_update(Config),

    %% Check if files were imported on W1
    ?assertMonitoring(W1, #{
        <<"scans">> := 2,
        <<"toProcess">> := 4,
        <<"imported">> := 1,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 2,
        <<"importedSum">> := 3,
        <<"updatedSum">> := 2,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [1 | _],
        <<"importedHourHist">> := [3 | _],
        <<"importedDayHist">> := [3 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [2 | _],
        <<"updatedDayHist">> := [2 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    History = rpc:call(W1, meck, history, [storage_sync_changes]),

    {ok, Handle5} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle5, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle5),

    SpaceUuid = storage_sync_test_base:space_uuid(W1, ?SPACE_ID),
    TestDirUuid = storage_sync_test_base:uuid(W1, TestDirGuid1),
    TestDirUuid2 = storage_sync_test_base:uuid(W1, TestDirGuid2),

    storage_sync_test_base:assert_num_results(History, ?assertHashChangedFun(SpaceUuid, true), 0),
    storage_sync_test_base:assert_num_results(History, ?assertMtimeChangedFun(TestDirUuid2, true), 0),
    storage_sync_test_base:assert_num_results_gte(History, ?assertMtimeChangedFun(TestDirUuid, true), 1),

    %% Check if file was imported on W2
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), 5 * ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS).

create_file_in_dir_exceed_batch_update_test(Config) ->
    % in this test dir_batch_size is set in init_per_testcase to 2
    MountSpaceInRoot = true,
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),

    StorageTestDirPath =
        storage_sync_test_base:storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 =
        storage_sync_test_base:storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    StorageTestFilePath1 =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    StorageTestFilePath2 =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE2, MountSpaceInRoot),
    StorageTestFilePath3 =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE3, MountSpaceInRoot),
    StorageTestFilePath4 =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE4, MountSpaceInRoot),
    StorageTestFileinDirPath1 =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

    %% Create dir on storage
    ok = file:make_dir(StorageTestDirPath),
    ok = file:make_dir(StorageTestDirPath2),
    ok = file:change_mode(StorageTestDirPath, 8#777),
    ok = file:change_mode(StorageTestDirPath2, 8#777),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath1, ?TEST_DATA),
    ok = file:write_file(StorageTestFilePath2, ?TEST_DATA),
    ok = file:write_file(StorageTestFilePath3, ?TEST_DATA),
    ok = file:write_file(StorageTestFilePath4, ?TEST_DATA),

    storage_sync_test_base:enable_storage_import(Config),

    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    %% Check if files were imported on W1
    {ok, #file_attr{guid = TestDirGuid1}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    {ok, #file_attr{guid = TestDirGuid2}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH2}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH})),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH2})),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH3})),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH4})),

    ?assertMonitoring(W1, #{
        <<"scans">> := 1,
        <<"toProcess">> := 10,
        <<"imported">> := 6,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 3,
        <<"importedSum">> := 6,
        <<"updatedSum">> := 1,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [6 | _],
        <<"importedHourHist">> := [6 | _],
        <<"importedDayHist">> := [6 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [1 | _],
        <<"updatedDayHist">> := [1 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    test_utils:mock_new(W1, storage_sync_changes, [passthrough]),
    ok = file:write_file(StorageTestFileinDirPath1, ?TEST_DATA),
    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_storage_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> := 2,
        <<"toProcess">> := 7,
        <<"imported">> := 1,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 5,
        <<"importedSum">> := 7,
        <<"updatedSum">> := 2,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [1 | _],
        <<"importedHourHist">> := [7 | _],
        <<"importedDayHist">> := [7 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [2 | _],
        <<"updatedDayHist">> := [2 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    %% Check if files were imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),

    History = rpc:call(W1, meck, history, [storage_sync_changes]),

    {ok, Handle5} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle5, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle5),

    TestDirUuid = storage_sync_test_base:uuid(W1, TestDirGuid1),
    TestDirUuid2 = storage_sync_test_base:uuid(W1, TestDirGuid2),
    storage_sync_test_base:assert_num_results(History, ?assertHashChangedFun(TestDirUuid2, true), 0),
    storage_sync_test_base:assert_num_results(History, ?assertMtimeChangedFun(TestDirUuid2, true), 0),
    storage_sync_test_base:assert_num_results_gte(History, ?assertMtimeChangedFun(TestDirUuid, true), 1),


    %% Check if file was imported on W2
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), 5 * ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS).

delete_empty_directory_update_test(Config) ->
    storage_sync_test_base:delete_empty_directory_update_test(Config, true).

delete_non_empty_directory_update_test(Config) ->
    storage_sync_test_base:delete_non_empty_directory_update_test(Config, true).

sync_works_properly_after_delete_test(Config) ->
    storage_sync_test_base:sync_works_properly_after_delete_test(Config, true).

delete_and_update_files_simultaneously_update_test(Config) ->
    MountSpaceInRoot = true,
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_sync_test_base:storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFileinDirPath1 =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),
    StorageTestFileinDirPath2 =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE2]), MountSpaceInRoot),
    NewMode = 8#600,

    %% Create dir on storage
    ok = file:make_dir(StorageTestDirPath),
    ok = file:write_file(StorageTestFileinDirPath1, ?TEST_DATA),
    ok = file:write_file(StorageTestFileinDirPath2, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),
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
        <<"scans">> := 1,
        <<"toProcess">> := 4,
        <<"imported">> := 3,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 0,
        <<"importedSum">> := 3,
        <<"updatedSum">> := 1,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [3 | _],
        <<"importedHourHist">> := [3 | _],
        <<"importedDayHist">> := [3 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [1 | _],
        <<"updatedDayHist">> := [1 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    %% Delete dir on storage
    storage_sync_test_base:recursive_rm(StorageTestFileinDirPath1),
    file:change_mode(StorageTestFileinDirPath2, NewMode),

    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_storage_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> := 2,
        <<"toProcess">> := 4,
        <<"imported">> := 0,
        <<"updated">> := 2,
        <<"deleted">> := 1,
        <<"failed">> := 0,
        <<"otherProcessed">> := 1,
        <<"importedSum">> := 3,
        <<"updatedSum">> := 3,
        <<"deletedSum">> := 1,
        <<"importedMinHist">> := [3 | _],
        <<"importedHourHist">> := [3 | _],
        <<"importedDayHist">> := [3 | _],
        <<"updatedMinHist">> := [2 | _],
        <<"updatedHourHist">> := [3 | _],
        <<"updatedDayHist">> := [3 | _],
        <<"deletedMinHist">> := [1 | _],
        <<"deletedHourHist">> := [1 | _],
        <<"deletedDayHist">> := [1 | _]
    }, ?SPACE_ID),

    %% Check if File1 was deleted in and if File2 was updated
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),

    ?assertMatch({ok, #file_attr{mode = NewMode}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}), ?ATTEMPTS).

delete_file_update_test(Config) ->
    storage_sync_test_base:delete_file_update_test(Config, true).

append_file_update_test(Config) ->
    MountSpaceInRoot = true,
    [W1 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    ?assertMonitoring(W1, #{
        <<"scans">> := 1,
        <<"toProcess">> := 2,
        <<"imported">> := 1,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 0,
        <<"importedSum">> := 1,
        <<"updatedSum">> := 1,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [1 | _],
        <<"importedHourHist">> := [1 | _],
        <<"importedDayHist">> := [1 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [1 | _],
        <<"updatedDayHist">> := [1 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    %% Append to file
    storage_sync_test_base:append(StorageTestFilePath, ?TEST_DATA2),
    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),

    %% Check if appended bytes were imported on worker1
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    AppendedData = <<(?TEST_DATA)/binary, (?TEST_DATA2)/binary>>,
    ?assertMatch({ok, AppendedData},
        lfm_proxy:read(W1, Handle2, 0, byte_size(AppendedData))),
    lfm_proxy:close(W1, Handle2),

    ?assertMonitoring(W1, #{
        <<"scans">> := 2,
        <<"toProcess">> := 2,
        <<"imported">> := 0,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 1,
        <<"importedSum">> := 1,
        <<"updatedSum">> := 2,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [1 | _],
        <<"importedHourHist">> := [1 | _],
        <<"importedDayHist">> := [1 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [2 | _],
        <<"updatedDayHist">> := [2 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID).

append_empty_file_update_test(Config) ->
    MountSpaceInRoot = true,
    [W1 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, <<"">>),
    storage_sync_test_base:enable_storage_import(Config),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, <<"">>},
        lfm_proxy:read(W1, Handle1, 0, 100)),
    lfm_proxy:close(W1, Handle1),

    ?assertMonitoring(W1, #{
        <<"scans">> := 1,
        <<"toProcess">> := 2,
        <<"imported">> := 1,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 0,
        <<"importedSum">> := 1,
        <<"updatedSum">> := 1,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [1 | _],
        <<"importedHourHist">> := [1 | _],
        <<"importedDayHist">> := [1 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [1 | _],
        <<"updatedDayHist">> := [1 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    %% Append to file
    storage_sync_test_base:append(StorageTestFilePath, ?TEST_DATA2),
    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),

    %% Check if appended bytes were imported on worker1
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA2},
        lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA2))),
    lfm_proxy:close(W1, Handle2),

    ?assertMonitoring(W1, #{
        <<"scans">> := 2,
        <<"toProcess">> := 2,
        <<"imported">> := 0,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 1,
        <<"importedSum">> := 1,
        <<"updatedSum">> := 2,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [1 | _],
        <<"importedHourHist">> := [1 | _],
        <<"importedDayHist">> := [1 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [2 | _],
        <<"updatedDayHist">> := [2 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID).

copy_file_update_test(Config) ->
    storage_sync_test_base:copy_file_update_test(Config, true).

move_file_update_test(Config) ->
    storage_sync_test_base:move_file_update_test(Config, true).

truncate_file_update_test(Config) ->
    MountSpaceInRoot = true,
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> := 1,
        <<"toProcess">> := 2,
        <<"imported">> := 1,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 0,
        <<"importedSum">> := 1,
        <<"updatedSum">> := 1,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [1 | _],
        <<"importedHourHist">> := [1 | _],
        <<"importedDayHist">> := [1 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [1 | _],
        <<"updatedDayHist">> := [1 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    timer:sleep(timer:seconds(2)),  %ensure that modify time is different from read time
    %% Truncate file
    storage_sync_test_base:truncate(StorageTestFilePath, 1),
    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    %% Check if file was truncated
    ?assertMatch({ok, #file_attr{size = 1}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> := 2,
        <<"toProcess">> := 2,
        <<"imported">> := 0,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 1,
        <<"importedSum">> := 1,
        <<"updatedSum">> := 2,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [1 | _],
        <<"importedHourHist">> := [1 | _],
        <<"importedDayHist">> := [1 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [2 | _],
        <<"updatedDayHist">> := [2 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID).

chmod_file_update_test(Config) ->
    MountSpaceInRoot = true,
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),

    StorageTestDirPath =
        storage_sync_test_base:storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),

    StorageTestFileinDirPath1 =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

    NewMode = 8#600,
    %% Create dirs on storage
    ok = file:make_dir(StorageTestDirPath),
    %% Create file on storage
    ok = file:write_file(StorageTestFileinDirPath1, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> := 1,
        <<"toProcess">> := 3,
        <<"imported">> := 2,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 0,
        <<"importedSum">> := 2,
        <<"updatedSum">> := 1,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [2 | _],
        <<"importedHourHist">> := [2 | _],
        <<"importedDayHist">> := [2 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [1 | _],
        <<"updatedDayHist">> := [1 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    %% Check if file was imported
    {ok, #file_attr{guid = TestDirGuid1}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{mode = 8#644}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    test_utils:mock_new(W1, storage_sync_changes, [passthrough]),

    timer:sleep(timer:seconds(2)),  %ensure that modify time is different from read time
    %% Change file permissions
    file:change_mode(StorageTestFileinDirPath1, NewMode),
    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),

    %% Check if file permissions were changed
    ?assertMatch({ok, #file_attr{mode = NewMode}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    History = rpc:call(W1, meck, history, [storage_sync_changes]),

    TestDirUuid = storage_sync_test_base:uuid(W1, TestDirGuid1),
    storage_sync_test_base:assert_num_results_gte(History, ?assertHashChangedFun(TestDirUuid, true), 1),

    ?assertMonitoring(W1, #{
        <<"scans">> := 2,
        <<"toProcess">> := 3,
        <<"imported">> := 0,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 2,
        <<"importedSum">> := 2,
        <<"updatedSum">> := 2,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [2 | _],
        <<"importedHourHist">> := [2 | _],
        <<"importedDayHist">> := [2 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [2 | _],
        <<"updatedDayHist">> := [2 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID).

change_file_content_update_test(Config) ->
    storage_sync_test_base:change_file_content_update_test(Config, true).

change_file_content_update2_test(Config) ->
    storage_sync_test_base:change_file_content_update2_test(Config, true).

chmod_file_update2_test(Config) ->
    % in this test dir_batch_size is set in init_per_testcase to 2
    MountSpaceInRoot = true,
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),

    Files = lists:map(fun(TestFile) ->
        filename:join([?TEST_DIR, TestFile])
    end, [?TEST_FILE1, ?TEST_FILE2, ?TEST_FILE3]),

    StorageTestDirPath =
        storage_sync_test_base:storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 =
        storage_sync_test_base:storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),

    [StTestFile1 | _] = StorageFiles =
        storage_sync_test_base:to_storage_files(Files, W1MountPoint, ?SPACE_ID, MountSpaceInRoot),

    NewMode = 8#600,
    %% Create files on storage
    ok = file:make_dir(StorageTestDirPath),
    ok = file:make_dir(StorageTestDirPath2),
    lists:foreach(fun(StorageTestFilePath) ->
        ok = file:write_file(StorageTestFilePath, ?TEST_DATA)
    end, StorageFiles),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    %% Check if files were imported
    {ok, #file_attr{guid = TestDirGuid1}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    lists:foreach(fun(SpaceFile) ->
        ?assertMatch({ok, #file_attr{mode = 8#644}},
            lfm_proxy:stat(W1, SessId, {path, SpaceFile}), ?ATTEMPTS),
        {ok, Handle1} = ?assertMatch({ok, _},
            lfm_proxy:open(W1, SessId, {path, SpaceFile}, read)),
        ?assertMatch({ok, ?TEST_DATA},
            lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
        lfm_proxy:close(W1, Handle1)
    end, [?SPACE_TEST_FILE_IN_DIR_PATH, ?SPACE_TEST_FILE_IN_DIR_PATH2, ?SPACE_TEST_FILE_IN_DIR_PATH3]),

    test_utils:mock_new(W1, storage_sync_changes, [passthrough]),
    ?assertMonitoring(W1, #{
        <<"scans">> := 1,
        <<"toProcess">> := 8,
        <<"imported">> := 5,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 2,
        <<"importedSum">> := 5,
        <<"updatedSum">> := 1,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [5 | _],
        <<"importedHourHist">> := [5 | _],
        <<"importedDayHist">> := [5 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [1 | _],
        <<"updatedDayHist">> := [1 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    timer:sleep(timer:seconds(2)),
    %% Change file permissions
    ok = file:change_mode(StTestFile1, NewMode),
    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),

    %% Check if file permissions were changed
    ?assertMatch({ok, #file_attr{mode = NewMode}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    History = rpc:call(W1, meck, history, [storage_sync_changes]),
        test_utils:mock_unload(W1, storage_sync_changes),

    TestDirUuid = storage_sync_test_base:uuid(W1, TestDirGuid1),
    storage_sync_test_base:assert_num_results_gte(History, ?assertHashChangedFun(TestDirUuid, true), 1),
    storage_sync_test_base:assert_num_results(History, ?assertMtimeChangedFun(TestDirUuid, true), 0),

    ?assertMonitoring(W1, #{
        <<"scans">> := 2,
        <<"imported">> := 0,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"importedSum">> := 5,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [5 | _],
        <<"importedHourHist">> := [5 | _],
        <<"importedDayHist">> := [5 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID).

update_timestamps_file_import_test(Config) ->
    MountSpaceInRoot = true,
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> := 1,
        <<"toProcess">> := 2,
        <<"imported">> := 1,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 0,
        <<"importedSum">> := 1,
        <<"updatedSum">> := 1,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [1 | _],
        <<"importedHourHist">> := [1 | _],
        <<"importedDayHist">> := [1 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [1 | _],
        <<"updatedDayHist">> := [1 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    %% Change file permissions
    NewTimestamp = 9999999999,
    storage_sync_test_base:change_time(StorageTestFilePath, NewTimestamp, NewTimestamp),
    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID, ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> := 2,
        <<"toProcess">> := 2,
        <<"imported">> := 0,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 1,
        <<"importedSum">> := 1,
        <<"updatedSum">> := 2,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [1 | _],
        <<"importedHourHist">> := [1 | _],
        <<"importedDayHist">> := [1 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [2 | _],
        <<"updatedDayHist">> := [2 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    %% Check if timestamps were changed
    ?assertMatch({ok, #file_attr{atime = NewTimestamp, mtime = NewTimestamp}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS).

should_not_detect_timestamp_update_test(Config) ->
    MountSpaceInRoot = true,
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> := 1,
        <<"toProcess">> := 2,
        <<"imported">> := 1,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 0,
        <<"importedSum">> := 1,
        <<"updatedSum">> := 1,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [1 | _],
        <<"importedHourHist">> := [1 | _],
        <<"importedDayHist">> := [1 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [1 | _],
        <<"updatedDayHist">> := [1 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    %% Change file permissions
    NewTimestamp = 9999999999,
    storage_sync_test_base:change_time(StorageTestFilePath, NewTimestamp, NewTimestamp),
    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID, ?ATTEMPTS),

    %% Check if timestamps hasn't changed
    ?assertNotMatch({ok, #file_attr{atime = NewTimestamp, mtime = NewTimestamp}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH})),

    ?assertMonitoring(W1, #{
        <<"scans">> := 2,
        <<"toProcess">> := 1,
        <<"imported">> := 0,
        <<"updated">> := 0,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 1,
        <<"importedSum">> := 1,
        <<"updatedSum">> := 1,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [1 | _],
        <<"importedHourHist">> := [1 | _],
        <<"importedDayHist">> := [1 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [1 | _],
        <<"updatedDayHist">> := [1 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID).

import_nfs_acl_test(Config) ->
    storage_sync_test_base:import_nfs_acl_test(Config, true).

update_nfs_acl_test(Config) ->
    MountSpaceInRoot = true,
    Workers = [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER2, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),

    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID, ?ATTEMPTS),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),

    %% User1 should be allowed to read acl
    {ok, #xattr{value = Value}} = ?assertMatch({ok, #xattr{}},
        lfm_proxy:get_xattr(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, <<"cdmi_acl">>)),
    ?assertMatch(Value, ?ACL_JSON),

    %% User1 should not be allowed to set acl
    ?assertMatch({error, ?EACCES},
        lfm_proxy:set_xattr(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, #xattr{})),

    %% User2 should be allowed to read acl
    {ok, #xattr{value = Value}} = ?assertMatch({ok, #xattr{}},
        lfm_proxy:get_xattr(W1, SessId2, {path, ?SPACE_TEST_FILE_PATH}, <<"cdmi_acl">>)),

    %% User2 should not be allowed to set acl
    ?assertMatch({error, ?EACCES},
        lfm_proxy:set_xattr(W1, SessId2, {path, ?SPACE_TEST_FILE_PATH}, #xattr{})),

    ?assertMonitoring(W1, #{
        <<"scans">> := 1,
        <<"toProcess">> := 2,
        <<"imported">> := 1,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 0,
        <<"importedSum">> := 1,
        <<"updatedSum">> := 1,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [1 | _],
        <<"importedHourHist">> := [1 | _],
        <<"importedDayHist">> := [1 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [1 | _],
        <<"updatedDayHist">> := [1 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID),

    EncACL = nfs4_acl:encode(?ACL2),
    ok = test_utils:mock_expect(Workers, storage_file_manager, getxattr, fun
        (Handle = #sfm_handle{file = <<"/">>}, Name) ->
            meck:passthrough([Handle, Name]);
        (Handle = #sfm_handle{file = <<"/space1">>}, Name) ->
            meck:passthrough([Handle, Name]);
        (#sfm_handle{}, _) ->
            {ok, EncACL}
    end),
    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID, ?ATTEMPTS),

    %% User1 should not be allowed to read acl
    ?assertMatch({error, ?EACCES},
        lfm_proxy:get_xattr(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, <<"cdmi_acl">>), ?ATTEMPTS),

    %% User2 should be allowed to read acl
    {ok, #xattr{value = Value2}} = ?assertMatch({ok, #xattr{}},
        lfm_proxy:get_xattr(W1, SessId2, {path, ?SPACE_TEST_FILE_PATH}, <<"cdmi_acl">>)),
    ?assertMatch(Value2, ?ACL2_JSON),

    ?assertMonitoring(W1, #{
        <<"scans">> := 2,
        <<"toProcess">> := 2,
        <<"imported">> := 0,
        <<"updated">> := 1,
        <<"deleted">> := 0,
        <<"failed">> := 0,
        <<"otherProcessed">> := 1,
        <<"importedSum">> := 1,
        <<"updatedSum">> := 2,
        <<"deletedSum">> := 0,
        <<"importedMinHist">> := [1 | _],
        <<"importedHourHist">> := [1 | _],
        <<"importedDayHist">> := [1 | _],
        <<"updatedMinHist">> := [1 | _],
        <<"updatedHourHist">> := [2 | _],
        <<"updatedDayHist">> := [2 | _],
        <<"deletedMinHist">> := [0 | _],
        <<"deletedHourHist">> := [0 | _],
        <<"deletedDayHist">> := [0 | _]
    }, ?SPACE_ID).

sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config) ->
    storage_sync_test_base:sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config, true).

sync_should_not_delete_dir_created_in_remote_provider(Config) ->
    storage_sync_test_base:sync_should_not_delete_dir_created_in_remote_provider(Config, true).

sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config) ->
    storage_sync_test_base:sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config, true).

import_file_by_path_test(Config) ->
    storage_sync_test_base:import_file_by_path_test(Config, true).

get_child_attr_by_path_test(Config) ->
    storage_sync_test_base:get_child_attr_by_path_test(Config, true).

import_remote_file_by_path_test(Config) ->
    storage_sync_test_base:import_remote_file_by_path_test(Config, true).

import_nfs_acl_with_disabled_luma_should_fail_test(Config) ->
    storage_sync_test_base:import_nfs_acl_with_disabled_luma_should_fail_test(Config, true).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        ssl:start(),
        hackney:start(),
        initializer:disable_quota_limit(NewConfig),
        initializer:mock_provider_ids(NewConfig),
        multi_provider_file_ops_test_base:init_env(NewConfig)
    end,
    {ok, _} = application:ensure_all_started(worker_pool),
    {ok, _} = worker_pool:start_sup_pool(?VERIFY_POOL, [{workers, 8}]),
    [{?LOAD_MODULES, [initializer, storage_sync_test_base]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    ok = wpool:stop_sup_pool(?VERIFY_POOL),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    initializer:unmock_provider_ids(Config),
    ssl:stop().

init_per_testcase(Case, Config) when
    Case =:= create_directory_import_check_user_id_test;
    Case =:= create_file_import_check_user_id_test ->

    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [reverse_luma_proxy, storage_file_ctx]),
    test_utils:mock_expect(Workers, storage_file_ctx, get_storage_doc, fun(Ctx) ->
        {StorageDoc = #document{value = Storage = #storage{}}, Ctx} = meck:passthrough([Ctx]),
        {StorageDoc#document{value = Storage#storage{luma_config = ?LUMA_CONFIG}}, Ctx}
    end),
    test_utils:mock_expect(Workers, reverse_luma_proxy, get_group_id, fun(_, _, _, _, _) ->
        {ok, ?GROUP}
    end),
    test_utils:mock_expect(Workers, reverse_luma_proxy, get_user_id, fun(_, _, _, _) ->
        {ok, ?USER}
    end),
    init_per_testcase(default, Config);

init_per_testcase(Case, Config) when
    Case =:= create_directory_import_check_user_id_error_test;
    Case =:= create_file_import_check_user_id_error_test ->

    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [reverse_luma_proxy, storage_file_ctx]),
    test_utils:mock_expect(Workers, storage_file_ctx, get_storage_doc, fun(Ctx) ->
        {StorageDoc = #document{value = Storage = #storage{}}, Ctx} = meck:passthrough([Ctx]),
        {StorageDoc#document{value = Storage#storage{luma_config = ?LUMA_CONFIG}}, Ctx}
    end),
    test_utils:mock_expect(Workers, reverse_luma_proxy, get_group_id, fun(_, _, _, _, _) ->
        {ok, ?GROUP}
    end),
    test_utils:mock_expect(Workers, reverse_luma_proxy, get_user_id, fun(_, _, _, _) ->
        error(test_reason)
    end),
    init_per_testcase(default, Config);

init_per_testcase(Case, Config) when
    Case =:= create_file_in_dir_update_test;
    Case =:= should_not_detect_timestamp_update_test
    ->
    Config2 = [
        {update_config, #{
            delete_enable => false,
            write_once => true}} | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config) when
    Case =:= delete_empty_directory_update_test;
    Case =:= delete_non_empty_directory_update_test;
    Case =:= delete_file_update_test;
    Case =:= move_file_update_test;
    Case =:= create_subfiles_and_delete_before_import_is_finished_test
    ->
    Config2 = [
        {update_config, #{
            delete_enable => true,
            write_once => true}} | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config) when
    Case =:= delete_and_update_files_simultaneously_update_test;
    Case =:= sync_should_not_delete_not_replicated_file_created_in_remote_provider;
    Case =:= sync_should_not_delete_dir_created_in_remote_provider;
    Case =:= sync_should_not_delete_not_replicated_files_created_in_remote_provider2;
    Case =:= sync_works_properly_after_delete_test ->
    Config2 = [
        {update_config, #{
            delete_enable => true,
            write_once => false}} | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config) when
    Case =:= create_file_in_dir_exceed_batch_update_test
    ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, OldDirBatchSize} = test_utils:get_env(W1, op_worker, dir_batch_size),
    test_utils:set_env(W1, op_worker, dir_batch_size, 2),
    Config2 = [
        {update_config, #{
            delete_enable => false,
            write_once => true}},
        {old_dir_batch_size, OldDirBatchSize}
        | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config) when
    Case =:= chmod_file_update2_test
    ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, OldDirBatchSize} = test_utils:get_env(W1, op_worker, dir_batch_size),
    test_utils:set_env(W1, op_worker, dir_batch_size, 2),
    Config2 = [{old_dir_batch_size, OldDirBatchSize} | Config],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config) when
    Case =:= import_nfs_acl_test;
    Case =:= update_nfs_acl_test
    ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [storage_file_manager, reverse_luma_proxy, storage_file_ctx]),

    test_utils:mock_expect(Workers, storage_file_ctx, get_storage_doc, fun(Ctx) ->
        {StorageDoc = #document{value = Storage = #storage{}}, Ctx2} = meck:passthrough([Ctx]),
        {StorageDoc#document{value = Storage#storage{luma_config = ?LUMA_CONFIG}}, Ctx2}
    end),

    test_utils:mock_expect(Workers, reverse_luma_proxy, get_user_id_by_name, fun(_, _, _, _) ->
        {ok, ?USER}
    end),

    test_utils:mock_expect(Workers, reverse_luma_proxy, get_user_id, fun(_, _, _, _) ->
        {ok, ?USER}
    end),

    test_utils:mock_expect(Workers, reverse_luma_proxy, get_group_id, fun(_, _, _, _, _) ->
        {ok, ?GROUP}
    end),

    test_utils:mock_expect(Workers, reverse_luma_proxy, get_group_id_by_name, fun(_, _, _, _, _) ->
        {ok, ?GROUP2}
    end),

    EncACL = nfs4_acl:encode(?ACL),
    test_utils:mock_expect(Workers, storage_file_manager, getxattr, fun
        (Handle = #sfm_handle{file = <<"/">>}, Ctx) ->
            meck:passthrough([Handle, Ctx]);
        (#sfm_handle{}, _) ->
            {ok, EncACL}
    end),
    init_per_testcase(default, Config);

init_per_testcase(Case, Config) when
    Case =:= import_nfs_acl_with_disabled_luma_should_fail_test ->

    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [storage_file_manager]),

    EncACL = nfs4_acl:encode(?ACL),
    test_utils:mock_expect(Workers, storage_file_manager, getxattr, fun
        (Handle = #sfm_handle{file = <<"/space1">>}, Ctx) ->
            meck:passthrough([Handle, Ctx]);
        (#sfm_handle{}, _) ->
            {ok, EncACL}
    end),
    init_per_testcase(default, Config);


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    ConfigWithProxy = lfm_proxy:init(Config),
    Config2 = storage_sync_test_base:add_workers_storage_mount_points(ConfigWithProxy),
    storage_sync_test_base:create_init_file(Config2, true),
    Config2.

end_per_testcase(Case, Config) when
    Case =:= import_file_by_path_test;
    Case =:= get_child_attr_by_path_test
    ->
    Workers = ?config(op_worker_nodes, Config),
    storage_sync_test_base:reset_enoent_strategies(Workers, ?SPACE_ID),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config) when
    Case =:= chmod_file_update2_test;
    Case =:= create_file_in_dir_exceed_batch_update_test
    ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    OldDirBatchSize = ?config(old_dir_batch_size, Config),
    test_utils:set_env(W1, op_worker, dir_batch_size, OldDirBatchSize),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config) when
    Case =:= create_directory_import_check_user_id_test;
    Case =:= create_directory_import_check_user_id_error_test;
    Case =:= create_file_import_check_user_id_test;
    Case =:= create_file_import_check_user_id_error_test ->

    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, [reverse_luma_proxy, storage_file_ctx]),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config) when
    Case =:= import_nfs_acl_test;
    Case =:= update_nfs_acl_test
    ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, [reverse_luma_proxy, storage_file_ctx, storage_file_manager]),
    end_per_testcase(default, Config);

end_per_testcase(_Case, Config) ->
    Workers = [W1 | _] = ?config(op_worker_nodes, Config),
    ok = storage_sync_test_base:clean_reverse_luma_cache(W1),
    storage_sync_test_base:disable_storage_sync(Config),
    storage_sync_test_base:clean_storage(Config, true),
    storage_sync_test_base:clean_space(Config),
    storage_sync_test_base:cleanup_storage_sync_monitoring_model(W1, ?SPACE_ID),
    test_utils:mock_unload(Workers, [simple_scan, storage_sync_changes]),
    timer:sleep(timer:seconds(1)),
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================