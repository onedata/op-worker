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

-include("modules/storage_sync/strategy_config.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("kernel/include/file.hrl").
-include("storage_sync_test.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% util functions
-export([disable_storage_sync/1, set_check_locally_enoent_strategy/2,
    set_check_globally_enoent_strategy/2, reset_enoent_strategies/2,
    add_synced_storages/1, clean_synced_storage/2, storage_path/4, create_init_file/2,
    enable_storage_import/3, enable_storage_update/3, clean_reverse_luma_cache/1,
    clean_space/1, verify_file_deleted/4, cleanup_storage_sync_monitoring_model/2,
    assertImportTimes/2, assertImportTimes/3, assertUpdateTimes/2, assertUpdateTimes/3,
    disable_storage_update/1, disable_storage_import/1, assertNoImportInProgress/3,
    assertNoUpdateInProgress/3,
    change_time/3, assert_num_results_gte/3, assert_num_results/3,
    to_storage_files/3, parallel_assert/5, uuid/2, space_uuid/2, change_time/2,
    to_storage_file_id/2, assert_monitoring_state/4, verify_file_deleted/5,
    storage_path/3, get_rdwr_storage/2, get_supporting_storage/2, get_host_mount_point/2,
    create_nested_directory_tree/3, generate_nested_directory_tree_file_paths/2, add_rdwr_storages/1, get_synced_storage/2, get_host_storage_file_id/4, get_mount_point/1]).

%% tests
-export([
    create_directory_import_test/2,
    create_file_import_test/2,
    delete_empty_directory_update_test/2, delete_file_update_test/2,
    append_file_update_test/2, move_file_update_test/2, truncate_file_update_test/2,
    chmod_file_update_test/2, update_timestamps_file_import_test/2,
    create_file_in_dir_import_test/2,
    create_file_in_dir_update_test/2, create_file_in_dir_exceed_batch_update_test/2,
    chmod_file_update2_test/2, should_not_detect_timestamp_update_test/2,
    create_directory_import_many_test/2, create_subfiles_import_many_test/2,
    verify_file_in_dir/5, verify_dir/5,
    create_directory_import_without_read_permission_test/2,
    create_subfiles_import_many2_test/2, verify_file/5,
    create_subfiles_and_delete_before_import_is_finished_test/2,
    create_directory_import_check_user_id_test/2,
    create_directory_import_check_user_id_error_test/2,
    create_file_import_check_user_id_test/2,
    create_file_import_check_user_id_error_test/2,
    delete_non_empty_directory_update_test/2,
    import_nfs_acl_test/2, update_nfs_acl_test/2,
    import_nfs_acl_with_disabled_luma_should_fail_test/2,
    create_directory_import_error_test/2,
    delete_and_update_files_simultaneously_update_test/2,
    update_syncs_files_after_import_failed_test/2,
    update_syncs_files_after_previous_update_failed_test/2,
    sync_works_properly_after_delete_test/2,
    create_delete_import_test_read_both/2,
    create_delete_import2_test/3, recreate_file_deleted_by_sync_test/2,
    sync_should_not_delete_not_replicated_file_created_in_remote_provider/2,
    sync_should_not_delete_dir_created_in_remote_provider/2,
    sync_should_not_delete_not_replicated_files_created_in_remote_provider2/2,
    create_delete_import_test_read_remote_only/2, copy_file_update_test/2,
    change_file_content_constant_size_test/2, change_file_content_update_test/2,
    create_empty_file_import_test/2,
    append_empty_file_update_test/2, import_file_with_link_but_no_doc_test/2,
    append_file_not_changing_mtime_update_test/2,
    change_file_content_the_same_moment_when_sync_performs_stat_on_file_test/2,
    sync_should_not_invalidate_file_after_replication/1,
    sync_should_not_reimport_file_when_link_is_missing_but_file_on_storage_has_not_changed/2,
    delete_many_subfiles_test/2]).

-define(assertBlocks(Worker, SessionId, ExpectedDistribution, FileGuid),
    ?assertEqual(lists:sort(ExpectedDistribution), begin
        case lfm_proxy:get_file_distribution(Worker, SessionId, {guid, FileGuid}) of
            {ok, __FileBlocks} -> lists:sort(__FileBlocks);
            Error -> Error
        end
    end, ?ATTEMPTS)
).

%%%===================================================================
%%% Test functions
%%%===================================================================

create_delete_import_test_read_both(Config, MountSpaceInRoot) ->
    create_delete_import_test(Config, MountSpaceInRoot, true).

create_delete_import_test_read_remote_only(Config, MountSpaceInRoot) ->
    create_delete_import_test(Config, MountSpaceInRoot, false).

create_delete_import_test(Config, MountSpaceInRoot, ReadBoth) ->
    [W1, W2 | _] = Workers = ?config(op_worker_nodes, Config),
    Attempts = 60,

    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    StorageTestFilePath2 = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),

    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),

    Size = byte_size(?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
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
                SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W)}}, Config),
                case lfm_proxy:open(W, SessId, {path, ?SPACE_TEST_FILE_PATH}, read) of
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
    SFMHandle2 = sfm_test_utils:new_handle(W2, ?SPACE_ID, StorageTestFilePath2, Storage2),
    ?assertEqual({ok, ?TEST_DATA}, sfm_test_utils:read_file(W1, SFMHandle, 0, ?TEST_DATA_SIZE)),
    ?assertEqual({ok, ?TEST_DATA}, sfm_test_utils:read_file(W2, SFMHandle2, 0, ?TEST_DATA_SIZE)),

    SessIdW2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    {ok, #file_attr{guid = GUID}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessIdW2, {path, ?SPACE_TEST_FILE_PATH})),

    ?assertMatch(ok, lfm_proxy:unlink(W2, <<"0">>, {guid, GUID})),
    ?assertEqual({error, enoent}, sfm_test_utils:read_file(W2, SFMHandle2, 0, ?TEST_DATA_SIZE), Attempts),
    ?assertEqual({error, enoent}, sfm_test_utils:read_file(W1, SFMHandle, 0, ?TEST_DATA_SIZE), Attempts),
    ok.

create_delete_import2_test(Config, MountSpaceInRoot, ReadBoth) ->
    [W1, W2 | _] = Workers = ?config(op_worker_nodes, Config),
    Attempts = 60,
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    StorageTestFilePath2 = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    Size = byte_size(?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

    ReadWorkers = case ReadBoth of
        true -> Workers;
        _ -> [W2]
    end,

    multi_provider_file_ops_test_base:verify_workers(ReadWorkers, fun(W) ->
        ?assertMatch({ok, ?TEST_DATA},
            begin
                SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W)}}, Config),
                case lfm_proxy:open(W, SessId, {path, ?SPACE_TEST_FILE_PATH}, read) of
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
    SFMHandle2 = sfm_test_utils:new_handle(W2, ?SPACE_ID, StorageTestFilePath2, Storage2),
    ?assertEqual({ok, ?TEST_DATA}, sfm_test_utils:read_file(W1, SFMHandle, 0, ?TEST_DATA_SIZE)),
    ?assertEqual({ok, ?TEST_DATA}, sfm_test_utils:read_file(W2, SFMHandle2, 0, ?TEST_DATA_SIZE)),

    SessIdW1 = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessIdW2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),

    {ok, #file_attr{guid = GUID}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessIdW2, {path, ?SPACE_TEST_FILE_PATH})),
    ?assertMatch(ok, lfm_proxy:unlink(W2, <<"0">>, {guid, GUID})),

    ?assertEqual({error, enoent}, sfm_test_utils:read_file(W1, SFMHandle, 0, ?TEST_DATA_SIZE), Attempts),
    ?assertEqual({error, enoent}, sfm_test_utils:read_file(W2, SFMHandle2, 0, ?TEST_DATA_SIZE), Attempts),

    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),

    {ok, FileGuid} =
        ?assertMatch({ok, _}, lfm_proxy:create(W2, SessIdW2, ?SPACE_TEST_FILE_PATH, 8#777)),
    {ok, FileHandle} =
        ?assertMatch({ok, _}, lfm_proxy:open(W2, SessIdW2, {guid, FileGuid}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W2, FileHandle, 0, ?TEST_DATA)),

    {ok, FileHandle2} =
        ?assertMatch({ok, _}, lfm_proxy:open(W1, SessIdW1, {guid, FileGuid}, read), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(W1, FileHandle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, sfm_test_utils:read_file(W1, SFMHandle, 0, ?TEST_DATA_SIZE), Attempts),
    ok = sfm_test_utils:unlink(W1, SFMHandle, Size),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:open(W2, SessIdW2, {guid, FileGuid}, read), ?ATTEMPTS).

create_directory_import_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle, 8#775),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

    % wait till scan is finished
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if dir was imported
    ?assertMatch({ok, [{_, ?TEST_DIR}]},
        lfm_proxy:ls(W1, SessId, {path, ?SPACE_PATH}, 0, 10)),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

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

import_file_with_link_but_no_doc_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle, 8#775),
    Ctx = rpc:call(W1, file_meta, get_ctx, []),
    TreeId = rpc:call(W1, oneprovider, get_id, []),
    FileUuid = datastore_utils:gen_key(),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_ID),
    {ok, _} = rpc:call(W1, datastore_model, add_links,
        [Ctx#{scope=>?SPACE_ID}, SpaceUuid, TreeId, {?TEST_DIR, FileUuid}]),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

    % wait till scan is finished
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if dir was imported
    {ok, #file_attr{guid = FileGuid}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch(FileUuid, file_id:guid_to_uuid(FileGuid)),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

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
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    ?assertNotMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH})),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle, 8#775),

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
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

    % wait till scan is finished
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if dir was not imported
    ?assertMatch({ok, []}, lfm_proxy:ls(W1, SessId, {path, ?SPACE_PATH}, 0, 1)),
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
    }, ?SPACE_ID).

update_syncs_files_after_import_failed_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),

    %% Create dir on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    %ensure that import will start with different timestamp than file was created
    timer:sleep(timer:seconds(1)),

    test_utils:mock_new(W1, simple_scan),
    test_utils:mock_expect(W1, simple_scan, import_file,
        fun(Job = #space_strategy_job{
            data = #{file_name := FileName}
        }, _) ->
            case FileName of
                ?TEST_FILE1 ->
                    throw(test_error);
                _ ->
                    meck:passthrough([Job])
            end
        end),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

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

    test_utils:mock_unload(W1, simple_scan),
    enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_storage_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 1,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 1,
        <<"updatedSum">> => 2,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS).

update_syncs_files_after_previous_update_failed_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 1,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
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

    %% Create dir on storage
    timer:sleep(timer:seconds(1)),
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#775),
    timer:sleep(timer:seconds(1)),
    test_utils:mock_new(W1, simple_scan),
    test_utils:mock_expect(W1, simple_scan, import_file, fun(Job = #space_strategy_job{
        data = #{file_name := FileName}}, _
    ) ->
        case FileName of
            ?TEST_FILE1 ->
                throw(test_error);
            _ ->
                meck:passthrough([Job])
        end
    end),
    enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    test_utils:mock_unload(W1, simple_scan),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 1,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if dir was not imported
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH})),
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH})),

    %next scan should import file
    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"toProcess">> => 2,
        <<"imported">> => 1,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 1,
        <<"updatedSum">> => 3,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 3,
        <<"updatedDayHist">> => 3,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID, ?ATTEMPTS),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS).

create_directory_import_check_user_id_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle, 8#775),
    ok = sfm_test_utils:chown(W1, SFMHandle, ?TEST_UID, ?TEST_GID),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{owner_id = ?USER}},
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
    }, ?SPACE_ID).

create_directory_import_check_user_id_error_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle, 8#775),
    ok = sfm_test_utils:chown(W1, SFMHandle, ?TEST_UID, ?TEST_GID),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
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
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle, 8#775),
    ok = sfm_test_utils:chmod(W1, SFMHandle, 8#000),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

    assertImportTimes(W1, ?SPACE_ID),

    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH2}), ?ATTEMPTS),

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
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    DirsNumber = 200,
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dirs on storage
    utils:pforeach(fun(N) ->
        DirPath = storage_path(?SPACE_ID, integer_to_binary(N), MountSpaceInRoot),
        SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, DirPath, RDWRStorage),
        ok = sfm_test_utils:mkdir(W1, SFMHandle, 8#775)
    end, lists:seq(1, DirsNumber)),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    parallel_assert(?MODULE, verify_dir, [W1, SessId, ?ATTEMPTS], lists:seq(1, DirsNumber), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 203,
        <<"imported">> => 200,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
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

create_file_import_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),

    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
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
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), 5 * ?ATTEMPTS),
        {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS).

create_empty_file_import_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
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
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), 5 * ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, <<"">>},
        lfm_proxy:read(W2, Handle2, 0, 100), ?ATTEMPTS).

sync_should_not_reimport_file_when_link_is_missing_but_file_on_storage_has_not_changed(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    HostStorageTestDirPath = storage_path(W1MountPoint, ?SPACE_ID, <<"">>, MountSpaceInRoot),
    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
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
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), 5 * ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),

    test_utils:mock_new(W1, fslogic_path),
    test_utils:mock_expect(W1, fslogic_path, to_uuid, fun(ParentUuid, FileName) ->
        case FileName of
            ?TEST_FILE1 ->
                {error, not_found};
            _ ->
                meck:passthrough([ParentUuid, FileName])
        end
    end),
    timer:sleep(timer:seconds(1)),
    change_time(HostStorageTestDirPath, time_utils:system_time_seconds()),

    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_storage_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
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
    }, ?SPACE_ID).

create_file_import_check_user_id_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    ok = sfm_test_utils:chown(W1, SFMHandle, ?TEST_UID, ?TEST_GID),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported on W1
    ?assertMatch({ok, #file_attr{owner_id = ?USER}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
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
    }, ?SPACE_ID).

create_file_import_check_user_id_error_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    ok = sfm_test_utils:chown(W1, SFMHandle, ?TEST_UID, ?TEST_GID),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported on W1
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),

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

create_subfiles_import_many_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    DirsNumber = 200,
    utils:pforeach(fun(N) ->
        NBin = integer_to_binary(N),
        DirPath = storage_path(?SPACE_ID, NBin, MountSpaceInRoot),
        FilePath = filename:join([DirPath, integer_to_binary(N)]),
        SFMDirHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, DirPath, RDWRStorage),
        ok = sfm_test_utils:mkdir(W1, SFMDirHandle, 8#775),
        SFMFileHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, FilePath, RDWRStorage),
        ok = sfm_test_utils:create_file(W1, SFMFileHandle, 8#664),
        {ok, _} = sfm_test_utils:write_file(W1, SFMFileHandle, 0, ?TEST_DATA)
    end, lists:seq(1, DirsNumber)),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    parallel_assert(?MODULE, verify_file_in_dir, [W1, SessId, 60], lists:seq(1, DirsNumber), 60),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 403,
        <<"imported">> => 400,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
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
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    RootPath = storage_path(?SPACE_ID, <<"">>, MountSpaceInRoot),
    DirStructure = [10, 10, 10],
    RootSFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, RootPath, RDWRStorage),

    create_nested_directory_tree(W1, DirStructure, RootSFMHandle),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
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

create_subfiles_and_delete_before_import_is_finished_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle, 8#775),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

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
    create_nested_directory_tree(W1, DirStructure, SFMHandle),
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),

    ?assertEqual(true, 10 =< rpc:call(W1, storage_sync_monitoring, get_unhandled_jobs_value,
        [?SPACE_ID, sfm_test_utils:get_storage_id(W1, ?SPACE_ID)]), ?ATTEMPTS),

    ok = sfm_test_utils:recursive_rm(W1, SFMHandle),
    ?assertMatch({error, ?ENOENT}, sfm_test_utils:ls(W1, SFMHandle, 0, 100)),
    ?assertMatch({ok, []},
        lfm_proxy:ls(W1, SessId, {path, ?SPACE_PATH}, 0, 100), 5 * ?ATTEMPTS),
    disable_storage_update(Config),
    assertUpdateTimes(W1, ?SPACE_ID).

create_file_in_dir_import_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFilePath = storage_path(?SPACE_ID, filename:join(?TEST_DIR, ?TEST_FILE2), MountSpaceInRoot),
    timer:sleep(timer:seconds(1)),

    %% Create dir on storage
    SFMDirHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMDirHandle, 8#775),
    %% Create file on storage
    SFMFileHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMFileHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMFileHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

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

create_file_in_dir_update_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 = storage_path(?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    StorageTestFileinDirPath1 = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

    %% Create dirs on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle, 8#777),
    SFMHandle2 = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath2, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle2, 8#777),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

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

    test_utils:mock_new(W1, storage_sync_changes, [passthrough]),
    FileInDirSFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, FileInDirSFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, FileInDirSFMHandle, 0, ?TEST_DATA),
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_storage_update(Config),

    %% Check if files were imported on W1
    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"imported">> => 1,
        <<"updated">> => 3,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 3,
        <<"updatedSum">> => 4,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 3,
        <<"importedDayHist">> => 3,
        <<"updatedMinHist">> => 3,
        <<"updatedHourHist">> => 4,
        <<"updatedDayHist">> => 4,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    History = rpc:call(W1, meck, history, [storage_sync_changes]),

    {ok, Handle5} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle5, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle5),

    assert_num_results(History, ?assertHashChangedFun(?SPACE_PATH, ?SPACE_ID, true), 0),
    assert_num_results(History, ?assertMtimeChangedFun(StorageTestDirPath2, ?SPACE_ID, true), 0),
    assert_num_results_gte(History, ?assertMtimeChangedFun(StorageTestDirPath, ?SPACE_ID, true), 1),
    %% Check if file was imported on W2
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), 5 * ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS).

create_file_in_dir_exceed_batch_update_test(Config, MountSpaceInRoot) ->
    % in this test storage_sync_dir_batch_size is set in init_per_testcase to 2
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),

    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 = storage_path(?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    StorageTestFilePath1 = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    StorageTestFilePath2 = storage_path(?SPACE_ID, ?TEST_FILE2, MountSpaceInRoot),
    StorageTestFilePath3 = storage_path(?SPACE_ID, ?TEST_FILE3, MountSpaceInRoot),
    StorageTestFilePath4 = storage_path(?SPACE_ID, ?TEST_FILE4, MountSpaceInRoot),
    StorageTestFileinDirPath1 = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

    DirSFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    DirSFMHandle2 = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath2, RDWRStorage),
    FileSFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath1, RDWRStorage),
    FileSFMHandle2 = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath2, RDWRStorage),
    FileSFMHandle3 = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath3, RDWRStorage),
    FileSFMHandle4 = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath4, RDWRStorage),
    FileInDirSFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),

    %% Create dirs on storage
    ok = sfm_test_utils:mkdir(W1, DirSFMHandle, 8#777),
    ok = sfm_test_utils:mkdir(W1, DirSFMHandle2, 8#777),

    %% Create files on storage
    ok = sfm_test_utils:create_file(W1, FileSFMHandle, 8#664),
    ok = sfm_test_utils:create_file(W1, FileSFMHandle2, 8#664),
    ok = sfm_test_utils:create_file(W1, FileSFMHandle3, 8#664),
    ok = sfm_test_utils:create_file(W1, FileSFMHandle4, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, FileSFMHandle, 0, ?TEST_DATA),
    {ok, _} = sfm_test_utils:write_file(W1, FileSFMHandle2, 0, ?TEST_DATA),
    {ok, _} = sfm_test_utils:write_file(W1, FileSFMHandle3, 0, ?TEST_DATA),
    {ok, _} = sfm_test_utils:write_file(W1, FileSFMHandle4, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

    assertImportTimes(W1, ?SPACE_ID),

    %% Check if files were imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
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

    test_utils:mock_new(W1, storage_sync_changes, [passthrough]),
    ok = sfm_test_utils:create_file(W1, FileInDirSFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, FileInDirSFMHandle, 0, ?TEST_DATA),
    enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_storage_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 7,
        <<"imported">> => 1,
        <<"updated">> => 3,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 3,
        <<"importedSum">> => 7,
        <<"updatedSum">> => 4,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 7,
        <<"importedDayHist">> => 7,
        <<"updatedMinHist">> => 3,
        <<"updatedHourHist">> => 4,
        <<"updatedDayHist">> => 4,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
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

    assert_num_results(History, ?assertHashChangedFun(StorageTestDirPath2, ?SPACE_ID, true), 0),
    assert_num_results(History, ?assertMtimeChangedFun(StorageTestDirPath2, ?SPACE_ID, true), 0),
    assert_num_results_gte(History, ?assertMtimeChangedFun(StorageTestDirPath, ?SPACE_ID, true), 1),

    %% Check if file was imported on W2
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), 5 * ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS).

delete_empty_directory_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    %% Create dir on storage
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle, 8#777),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    %% Delete dir on storage
    enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    ok = sfm_test_utils:rmdir(W1, SFMHandle),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_storage_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
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
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFileinDirPath1 = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle, 8#777),
    FileInDirSFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, FileInDirSFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, FileInDirSFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
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
    enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    %% Delete dir on storage
    ok = sfm_test_utils:recursive_rm(W1, SFMHandle),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_storage_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 2,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
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
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 = storage_path(?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    StorageTestFileinDirPath1 =
        storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),
    StorageTestFileinDirPath2 =
        storage_path(?SPACE_ID, filename:join([?TEST_DIR2, ?TEST_FILE2]), MountSpaceInRoot),
    %% Create dir on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle, 8#777),
    FileInDirSFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, FileInDirSFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, FileInDirSFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
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

    assertImportTimes(W1, ?SPACE_ID, ?ATTEMPTS),
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),

    %% Delete dir on storage
    ok = sfm_test_utils:recursive_rm(W1, SFMHandle),

    %% Check if dir was deleted in space
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

    assertUpdateTimes(W1, ?SPACE_ID),
    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 2,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
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

    SFMHandle2 = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath2, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle2, 8#777),
    FileInDirSFMHandle2 = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath2, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, FileInDirSFMHandle2, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, FileInDirSFMHandle2, 0, ?TEST_DATA),

    SpaceTestFileInDirPath = ?SPACE_TEST_FILE_IN_DIR_PATH(?TEST_DIR2, ?TEST_FILE2),

    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"toProcess">> => 3,
        <<"imported">> => 2,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
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
    lfm_proxy:close(W1, Handle6),
    assertUpdateTimes(W1, ?SPACE_ID).

delete_and_update_files_simultaneously_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFileinDirPath1 = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),
    StorageTestFileinDirPath2 = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE2]), MountSpaceInRoot),
    NewMode = 8#600,
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),

    %% Create dir on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle, 8#777),
    FileInDirSFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, FileInDirSFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, FileInDirSFMHandle, 0, ?TEST_DATA),
    FileInDirSFMHandle2 = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath2, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, FileInDirSFMHandle2, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, FileInDirSFMHandle2, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
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
    sfm_test_utils:unlink(W1, FileInDirSFMHandle, ?TEST_DATA_SIZE),
    sfm_test_utils:chmod(W1, FileInDirSFMHandle2, NewMode),

    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_storage_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"imported">> => 0,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"importedSum">> => 3,
        <<"deletedSum">> => 1,
        <<"importedMinHist">> => 3,
        <<"importedHourHist">> => 3,
        <<"importedDayHist">> => 3,
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
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    %% Check if file was imported
    {ok, #file_attr{guid = FileGuid}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} =
        ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    FileUuid = file_id:guid_to_uuid(FileGuid),
    Xattr = #xattr{name = <<"xattr_name">>, value = <<"xattr_value">>},
    ok = lfm_proxy:set_xattr(W1, ?ROOT_SESS_ID, {guid, FileGuid}, Xattr),

    %% Delete file on storage
    ok = sfm_test_utils:unlink(W1, SFMHandle, ?TEST_DATA_SIZE),
    enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_storage_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
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

    %% Check if file was deleted in space
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_xattr(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, <<"xattr_name">>)),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_xattr(W1, SessId, {guid, FileGuid}, <<"xattr_name">>)),
    ?assertMatch({error, not_found}, rpc:call(W1, custom_metadata, get, [FileUuid])).

delete_many_subfiles_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    RootPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, RootPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle, 8#777),
    DirStructure = [1000],
    create_nested_directory_tree(W1, DirStructure, SFMHandle),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    Files = generate_nested_directory_tree_file_paths(DirStructure, ?SPACE_TEST_DIR_PATH),

    Timeout = 600,
    parallel_assert(?MODULE, verify_file, [W1, SessId, Timeout], Files, Timeout),
    assertImportTimes(W1, ?SPACE_ID, Timeout),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 1012,
        <<"imported">> => 1001,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 10,
        <<"importedSum">> => 1001,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedHourHist">> => 1001,
        <<"importedDayHist">> => 1001,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    ok = sfm_test_utils:recursive_rm(W1, SFMHandle),
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    parallel_assert(?MODULE, verify_file_deleted, [W1, SessId, Timeout], [?SPACE_TEST_DIR_PATH | Files], Timeout),
    assertUpdateTimes(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 1002,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 1001,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 1001,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 1001,
        <<"importedDayHist">> => 1001,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedHourHist">> => 1001,
        <<"deletedDayHist">> => 1001
    }, ?SPACE_ID).


append_file_update_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    %% Check if file is visible on 2nd provider
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
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
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, ?TEST_DATA_SIZE, ?TEST_DATA2),
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),

    %% Check if appended bytes were imported on worker1
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    AppendedData = <<(?TEST_DATA)/binary, (?TEST_DATA2)/binary>>,
    ?assertMatch({ok, AppendedData},
        lfm_proxy:read(W1, Handle3, 0, byte_size(AppendedData))),
    lfm_proxy:close(W1, Handle3),

    {ok, Handle4} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
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
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    HostStorageTestFilePath = storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    %% Check if file is visible on 2nd provider
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
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
    {ok, #statbuf{st_mtime = StMtime}} = sfm_test_utils:stat(W1, SFMHandle),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, ?TEST_DATA_SIZE, ?TEST_DATA2),
    change_time(HostStorageTestFilePath, StMtime),

    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),

    %% Check if appended bytes were imported on worker1
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    AppendedData = <<(?TEST_DATA)/binary, (?TEST_DATA2)/binary>>,
    ?assertMatch({ok, AppendedData},
        lfm_proxy:read(W1, Handle3, 0, byte_size(AppendedData))),
    lfm_proxy:close(W1, Handle3),

    {ok, Handle4} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
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
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, <<"">>),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, <<"">>},
        lfm_proxy:read(W1, Handle1, 0, 100)),
    lfm_proxy:close(W1, Handle1),

    %% Check if file is visible on 2nd provider
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
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
        sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA2)),
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),

    %% Check if appended bytes were imported on worker1
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA2},
        lfm_proxy:read(W1, Handle3, 0, byte_size(?TEST_DATA2))),
    lfm_proxy:close(W1, Handle3),

    {ok, Handle4} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
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
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    % we need W1MountPoint as cp is performed via file not storage_file_manager
    SrcFilePath = storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    DestFilePath = storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE2, MountSpaceInRoot),
    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
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
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    timer:sleep(timer:seconds(10)), %ensure that copy time is different from read time
    %% Copy file
    file:copy(SrcFilePath, DestFilePath),

    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"imported">> => 1,
        <<"updated">> => 2,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 2,
        <<"updatedSum">> => 3,
        <<"deletedSum">> => 0,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"updatedHourHist">> => 3,
        <<"updatedDayHist">> => 3,
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
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle3, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle3).

move_file_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    SrcStorageFilePath = storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    DestStorageFilePath = storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE2, MountSpaceInRoot),
    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
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
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    timer:sleep(timer:seconds(2)), %ensure that copy time is different from read time
    %% Move file
    ok = file:rename(SrcStorageFilePath, DestStorageFilePath),
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
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

    %% Check if file was moved
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH2}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA))),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH})),
    lfm_proxy:close(W1, Handle2).

truncate_file_update_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
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
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    %% Check if file is visible on 2nd provider and replicate it
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle2),

    timer:sleep(timer:seconds(2)), %ensure that modify time is different from read time

    %% Truncate file
    {ok, SFMHandle2} = sfm_test_utils:open(W1, SFMHandle, write),
    sfm_test_utils:truncate(W1, SFMHandle2, 1, ?TEST_DATA_SIZE),

        storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_storage_update(Config),

    %% Check if file was truncated
    {ok, #file_attr{guid = G}} = ?assertMatch({ok, #file_attr{size = 1}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, <<"t">>},
        lfm_proxy:read(W1, Handle3, 0, 100), ?ATTEMPTS),
    lfm_proxy:close(W1, Handle3),

    ?assertMatch({ok, #file_attr{size = 1}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle4} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
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
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
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
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    %% Check if file is visible on 2nd provider
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle2),

    timer:sleep(timer:seconds(2)), %ensure that modify time is different from read time
    %% Truncate file
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, ?CHANGED_BYTE_OFFSET, ?CHANGED_BYTE),
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_storage_update(Config),

    %% Check if file was updated
    DataSize = byte_size(?TEST_DATA),
    ?assertMatch({ok, #file_attr{size = DataSize}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA_ONE_BYTE_CHANGED},
        lfm_proxy:read(W1, Handle3, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle3),

    ?assertMatch({ok, #file_attr{size = DataSize}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle4} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
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

change_file_content_the_same_moment_when_sync_performs_stat_on_file_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    HostStorageTestFilePath = storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
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
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    %% Check if file is visible on 2nd provider
    {ok, #file_attr{guid = Guid}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
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
        [StorageTestFilePath, fun(SSI) ->
            {ok, SSI#storage_sync_info{last_stat = StatTime}} end, ?SPACE_ID]
    ),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, ?CHANGED_BYTE_OFFSET, ?CHANGED_BYTE),
    change_time(HostStorageTestFilePath, StatTime),
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_storage_update(Config),

    %% Check if file was updated
    DataSize = byte_size(?TEST_DATA),
    ?assertMatch({ok, #file_attr{size = DataSize}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA_ONE_BYTE_CHANGED},
        lfm_proxy:read(W1, Handle3, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle3),

    ?assertMatch({ok, #file_attr{size = DataSize}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle4} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
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
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
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
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    %% Check if file is visible on 2nd provider
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W2, Handle2),

    timer:sleep(timer:seconds(2)), %ensure that modify time is different from read time
    %% Modify file
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA_CHANGED),
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),
    disable_storage_update(Config),

    %% Check if file was updated
    DataSize = byte_size(?TEST_DATA_CHANGED),
    ?assertMatch({ok, #file_attr{size = DataSize}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA_CHANGED},
        lfm_proxy:read(W1, Handle3, 0, DataSize)),
    lfm_proxy:close(W1, Handle3),

    ?assertMatch({ok, #file_attr{size = DataSize}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle4} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
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

chmod_file_update_test(Config, MountSpaceInRoot) ->
    [W1, W2] = ?config(op_worker_nodes, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),

    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFileinDirPath1 = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

    NewMode = 8#600,
    %% Create dirs on storage
    DirSFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, DirSFMHandle, 8#775),
    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

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

    test_utils:mock_new(W1, storage_sync_changes, [passthrough]),

    timer:sleep(timer:seconds(2)), %ensure that modify time is different from read time
    %% Change file permissions
    sfm_test_utils:chmod(W1, SFMHandle, NewMode),

    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),

    %% Check if file permissions were changed
    ?assertMatch({ok, #file_attr{mode = NewMode}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    History = rpc:call(W1, meck, history, [storage_sync_changes]),
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
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),

    Files = lists:map(fun(TestFile) ->
        filename:join([?TEST_DIR, TestFile])
    end, [?TEST_FILE1, ?TEST_FILE2, ?TEST_FILE3]),

    StorageTestDirPath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 = storage_path(?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),

    [StTestFile1 | _] = StorageFiles = to_storage_files(Files, ?SPACE_ID, MountSpaceInRoot),
    NewMode = 8#600,

    %% Create files on storage
    DirSFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, DirSFMHandle, 8#775),
    DirSFMHandle2 = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath2, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, DirSFMHandle2, 8#775),
    lists:foreach(fun(StorageTestFilePath) ->
        SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
        ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
        {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA)
    end, StorageFiles),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

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

    test_utils:mock_new(W1, storage_sync_changes, [passthrough]),

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
    SFMFileHandle1 = sfm_test_utils:new_handle(W1, ?SPACE_ID, StTestFile1, RDWRStorage),
    sfm_test_utils:chmod(W1, SFMFileHandle1, NewMode),
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),

    %% Check if file permissions were changed
    ?assertMatch({ok, #file_attr{mode = NewMode}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    History = rpc:call(W1, meck, history, [storage_sync_changes]),
    test_utils:mock_unload(W1, storage_sync_changes),

    assert_num_results_gte(History, ?assertHashChangedFun(StorageTestDirPath, ?SPACE_ID, true), 1),
    assert_num_results(History, ?assertMtimeChangedFun(StorageTestDirPath, ?SPACE_ID, true), 0),

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


update_timestamps_file_import_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
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
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID, ?ATTEMPTS),

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
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS).

should_not_detect_timestamp_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = storage_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID, ?ATTEMPTS),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),

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
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID, ?ATTEMPTS),

    %% Check if timestamps hasn't changed
    ?assertNotMatch({ok, #file_attr{atime = 1, mtime = 1}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH})),

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

import_nfs_acl_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER2, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),

    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID),

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

    %% User1 should not be allowed to modify file attrs
    ?assertMatch({error, ?EACCES},
        lfm_proxy:truncate(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, 100)),

    %% User2 should be allowed to read acl
    {ok, #xattr{value = Value}} = ?assertMatch({ok, #xattr{}},
        lfm_proxy:get_xattr(W1, SessId2, {path, ?SPACE_TEST_FILE_PATH}, <<"cdmi_acl">>)),

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


update_nfs_acl_test(Config, MountSpaceInRoot) ->
    Workers = [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER2, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),

    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID, ?ATTEMPTS),

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

    EncACL = nfs4_acl:encode(?ACL2),
    ok = test_utils:mock_expect(Workers, storage_file_manager, getxattr, fun
        (Handle = #sfm_handle{file = <<"/">>}, Name) ->
            meck:passthrough([Handle, Name]);
        (Handle = #sfm_handle{file = <<"/space1">>}, Name) ->
            meck:passthrough([Handle, Name]);
        (#sfm_handle{}, _) ->
            {ok, EncACL}
    end),
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID, ?ATTEMPTS),

    %% User1 should not be allowed to read acl
    ?assertMatch({error, ?EACCES},
        lfm_proxy:get_xattr(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, <<"cdmi_acl">>), ?ATTEMPTS),

    %% User2 should be allowed to read acl
    {ok, #xattr{value = Value2}} = ?assertMatch({ok, #xattr{}},
        lfm_proxy:get_xattr(W1, SessId2, {path, ?SPACE_TEST_FILE_PATH}, <<"cdmi_acl">>)),
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

import_nfs_acl_with_disabled_luma_should_fail_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),

    %% Create file on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    assertImportTimes(W1, ?SPACE_ID, ?ATTEMPTS),

    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),

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


recreate_file_deleted_by_sync_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

    {ok, FileGuid} =
        ?assertMatch({ok, _}, lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH, 8#777)),
    {ok, FileHandle} =
        ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, FileGuid}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W2, FileHandle, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:fsync(W2, FileHandle)),
    ok = lfm_proxy:close(W2, FileHandle),

    %% Replicate file to W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W1, Handle1),

    assertImportTimes(W1, ?SPACE_ID),

    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),

    %% delete file on storage
    ok = sfm_test_utils:unlink(W1, SFMHandle, ?TEST_DATA_SIZE),

    enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),

    %% Check if file disappeared
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),

    % recreate file
    {ok, FileGuid2} =
        ?assertMatch({ok, _}, lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH, 8#777)),
    {ok, FileHandle2} =
        ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, FileGuid2}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W2, FileHandle2, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:fsync(W2, FileHandle2)),
    ok = lfm_proxy:close(W2, FileHandle2),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W1, Handle2).

sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

    {ok, FileGuid} =
        ?assertMatch({ok, _}, lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH, 8#777)),
    {ok, FileHandle} =
        ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, FileGuid}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W2, FileHandle, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:fsync(W2, FileHandle)),
    ok = lfm_proxy:close(W2, FileHandle),

    assertImportTimes(W1, ?SPACE_ID),

    %check if file_meta was synced
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),

    %% Create file on storage to trigger update
    StorageRandomFilePath = storage_path(?SPACE_ID, <<"random_file">>, MountSpaceInRoot),
    RandomSFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageRandomFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, RandomSFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, RandomSFMHandle, 0, ?TEST_DATA),

    enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    %% wait until updates finishes
    assertUpdateTimes(W1, ?SPACE_ID),

    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),

    %% file shouldn't appear on W1's storage
    ?assertMatch({error, ?ENOENT}, sfm_test_utils:stat(W1, SFMHandle)),

    % file shouldn't disappear from space
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    lfm_proxy:close(W1, Handle2),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS).

sync_should_not_delete_dir_created_in_remote_provider(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),

    % Create dir in space
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId2, ?SPACE_TEST_DIR_PATH)),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

    assertImportTimes(W1, ?SPACE_ID),

    %% Create file on storage to trigger update
    StorageRandomFilePath = storage_path(?SPACE_ID, <<"random_file">>, MountSpaceInRoot),
    RandomSFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageRandomFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, RandomSFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, RandomSFMHandle, 0, ?TEST_DATA),
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    assertUpdateTimes(W1, ?SPACE_ID),

    StorageTestFilePath = storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),

    %% file shouldn't appear on W1's storage
    ?assertMatch({error, ?ENOENT}, sfm_test_utils:stat(W1, SFMHandle)),

    % Ensure that sync didn't delete remotely create directory
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS).

sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    W1MountPoint = get_host_mount_point(Config, RDWRStorage),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

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
        storage_path(W1MountPoint, ?SPACE_ID, <<"random_file">>, MountSpaceInRoot),
    RandomSFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageRandomFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, RandomSFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, RandomSFMHandle, 0, ?TEST_DATA),

    enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    %% wait until updates finishes
    assertUpdateTimes(W1, ?SPACE_ID),

    StorageTestFilePath = storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),

    %% file shouldn't appear on W1's storage
    ?assertMatch({error, ?ENOENT}, sfm_test_utils:stat(W1, SFMHandle)),

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

sync_should_not_invalidate_file_after_replication(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),

    {ok, FileGuid} = lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH, 8#664),
    {ok, Handle} = lfm_proxy:open(W2, SessId2, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(W2, Handle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W2, Handle),

    %% Check if file was synchronized to W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),

    % replicate file to W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    SyncedStorage = get_synced_storage(Config, W1),
    enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

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

    timer:sleep(timer:seconds(20)),

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
    % todo remove
    PathSplit = binary:split(Path, <<"/">>, [global]),
    MountPointSplit = binary:split(MountPoint, <<"/">>, [global]),
    StorageFileIdSplit = PathSplit -- MountPointSplit,
    filename:join(["/" | StorageFileIdSplit]).

create_init_file(Config, Readonly) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SpaceDir = storage_sync_test_base:storage_path(?SPACE_ID, <<"">>, Readonly),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, SpaceDir, RDWRStorage),
    case sfm_test_utils:mkdir(W1, SFMHandle, 8#777) of
        ok ->
            ok;
        {error, eexist} ->
            ok = sfm_test_utils:recursive_rm(W1, SFMHandle),
            sfm_test_utils:mkdir(W1, SFMHandle, 8#777)
    end.

enable_storage_import(Config, SpaceId, #document{key = StorageId}) ->
    enable_storage_import(Config, SpaceId, StorageId);
enable_storage_import(Config, SpaceId, StorageId) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    ?assertMatch({ok, _}, rpc:call(W1, storage_sync, start_simple_scan_import,
        [SpaceId, StorageId, ?MAX_DEPTH, ?SYNC_ACL])).

enable_storage_update(Config, SpaceId, #document{key = StorageId}) ->
    enable_storage_update(Config, SpaceId, StorageId);
enable_storage_update(Config, SpaceId, StorageId) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    UpdateConfig = ?config(update_config, Config, #{}),
    ScanInterval = maps:get(scan_interval, UpdateConfig, ?SCAN_INTERVAL),
    WriteOnce = maps:get(write_once, UpdateConfig, ?WRITE_ONCE),
    DeleteEnable = maps:get(delete_enable, UpdateConfig, ?DELETE_ENABLE),
    SyncAcl = maps:get(sync_acl, UpdateConfig, ?SYNC_ACL),
    {ok, _} = rpc:call(W1, storage_sync, start_simple_scan_update,
        [SpaceId, StorageId, ?MAX_DEPTH, ScanInterval, WriteOnce, DeleteEnable, SyncAcl]).

disable_storage_import(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    StorageId = sfm_test_utils:get_storage_id(W1, ?SPACE_ID),
    rpc:call(W1, storage_sync, stop_storage_import, [?SPACE_ID]),
    ?assertMatch({no_import, _}, get_storage_import_details(W1, ?SPACE_ID, StorageId), ?ATTEMPTS),
    assertNoImportInProgress(W1, ?SPACE_ID, ?ATTEMPTS),
    ok.

cleanup_storage_sync_monitoring_model(Worker, SpaceId) ->
    StorageId = sfm_test_utils:get_storage_id(Worker, SpaceId),
    rpc:call(Worker, storage_sync_monitoring, delete, [SpaceId, StorageId]).

get_storage_import_details(Worker, SpaceId, StorageId) ->
    rpc:call(Worker, space_strategies, get_storage_import_details, [SpaceId, StorageId]).

get_storage_update_details(Worker, SpaceId, StorageId) ->
    rpc:call(Worker, space_strategies, get_storage_update_details, [SpaceId, StorageId]).

disable_storage_update(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    StorageId = sfm_test_utils:get_storage_id(W1, ?SPACE_ID),
    rpc:call(W1, storage_sync, stop_storage_update, [?SPACE_ID]),
    ?assertMatch({no_update, _}, get_storage_update_details(W1, ?SPACE_ID, StorageId), ?ATTEMPTS),
    assertNoUpdateInProgress(W1, ?SPACE_ID, ?ATTEMPTS),
    ok.

disable_storage_sync(Config) ->
    disable_storage_import(Config),
    disable_storage_update(Config).

clean_synced_storage(Config, Readonly) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(W) ->
        {ok, Storages} = rpc:call(W, storage, list, []),
        lists:foreach(fun(Storage) ->
            SpaceDir = storage_sync_test_base:storage_path(?SPACE_ID, <<"">>, Readonly),
            case storage:is_readonly(Storage) of
                true ->
                    ok;
                false ->
                    SFMHandle = sfm_test_utils:new_handle(W, ?SPACE_ID, SpaceDir, Storage),
                    sfm_test_utils:recursive_rm(W, SFMHandle)
            end
        end, Storages)
    end, Workers),
    timer:sleep(timer:seconds(3)).

clean_space(Config) ->
    [W, W2 | _] = ?config(op_worker_nodes, Config),
    SpaceGuid = rpc:call(W, fslogic_uuid, spaceid_to_space_dir_guid, [?SPACE_ID]),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W)}}, Config),
    close_opened_files(W, SessId),
    {ok, Children} = lfm_proxy:ls(W, ?ROOT_SESS_ID, {guid, SpaceGuid}, 0, 10000),
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
    ?assertMatch({ok, []}, lfm_proxy:ls(W, ?ROOT_SESS_ID, {guid, SpaceGuid}, 0, 10000), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:ls(W2, ?ROOT_SESS_ID, {guid, SpaceGuid}, 0, 10000), ?ATTEMPTS).

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

clean_reverse_luma_cache(Worker) ->
    {ok, Storages} = rpc:call(Worker, storage, list, []),
    lists:foreach(fun(#document{key = StorageId}) ->
        ok = rpc:call(Worker, luma_cache, invalidate, [StorageId])
    end, Storages).

set_check_locally_enoent_strategy(Worker, SpaceId) ->
    {ok, _} = rpc:call(Worker, storage_sync, set_check_locally_enoent_strategy, [SpaceId]).

set_check_globally_enoent_strategy(Worker, SpaceId) ->
    {ok, _} = rpc:call(Worker, storage_sync, set_check_globally_enoent_strategy, [SpaceId]).

reset_enoent_strategies(Workers, SpaceId) when is_list(Workers) ->
    rpc:multicall(Workers, storage_sync, set_error_passthrough_enoent_strategy, [SpaceId]).

add_synced_storages(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    RDWRStorages = lists:foldl(fun(W, AccIn) ->
        {ok, Storages} = rpc:call(W, storage, list, []),
        case find_synced_storage(Storages) of
            undefined -> AccIn;
            SyncedStorage -> AccIn#{W => SyncedStorage}
        end
    end, #{}, Workers),
    [{synced_storages, RDWRStorages} | Config].

add_rdwr_storages(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    RDWRStorages = lists:foldl(fun(W, AccIn) ->
        {ok, Storages} = rpc:call(W, storage, list, []),
        case find_rdwr_storage(Storages) of
            undefined -> AccIn;
            RDWRStorage -> AccIn#{W => RDWRStorage}
        end
    end, #{}, Workers),
    [{rdwr_storages, RDWRStorages} | Config].

get_rdwr_storage(Config, Worker) ->
    case maps:get(Worker, ?config(rdwr_storages, Config), undefined) of
        undefined -> get_synced_storage(Config, Worker);
        Storage -> Storage
    end.

get_synced_storage(Config, Worker) ->
    maps:get(Worker, ?config(synced_storages, Config), undefined).

get_supporting_storage(Worker, SpaceId) ->
    StorageId = sfm_test_utils:get_storage_id(Worker, SpaceId),
    {ok, StorageDoc} = sfm_test_utils:get_storage_doc(Worker, StorageId),
    StorageDoc.

find_rdwr_storage(Storages) ->
    lists:foldl(fun
        (StorageDoc = #document{value = Storage}, undefined) ->
            case is_rdwr(Storage) of
                true -> StorageDoc;
                false -> undefined
            end;
        (_, RDWRStorage) ->
            RDWRStorage
    end, undefined, Storages).

is_rdwr(#storage{name = MountPoint, helpers = [#helper{name = ?POSIX_HELPER_NAME} | _]}) ->
    <<"rdwr_storage">> =:= filename:basename(MountPoint);
is_rdwr(#storage{name = <<"rdwr_storage">>, helpers = [#helper{name = ?S3_HELPER_NAME} | _]}) ->
    true;
is_rdwr(_) ->
    false.

find_synced_storage(Storages) ->
    lists:foldl(fun
        (StorageDoc = #document{value = Storage}, undefined) ->
            case is_synced(Storage) of
                true -> StorageDoc;
                false -> undefined
            end;
        (_, RDWRStorageIn) ->
            RDWRStorageIn
    end, undefined, Storages).

is_synced(#storage{name = MountPoint, helpers = [#helper{name = ?POSIX_HELPER_NAME} | _]}) ->
    <<"synced_storage">> =:= filename:basename(MountPoint);
is_synced(#storage{name = <<"synced_storage">>, helpers = [#helper{name = ?S3_HELPER_NAME} | _]}) ->
    true.

get_mount_point(Storage) ->
    % works only on POSIX storages!!!
    Helper = storage:get_helper(Storage),
    HelperArgs = helper:get_args(Helper),
    maps:get(<<"mountPoint">>, HelperArgs).

get_host_mount_point(Config, Storage) ->
    % works only on POSIX storages!!!
    MountPoint = get_mount_point(Storage),
    get_storage_path(Config, MountPoint).

get_host_storage_file_id(Config, CanonicalPath, Storage, MountInRoot) ->
    Helper = storage:get_helper(Storage),
    case helper:get_name(Helper) of
        ?POSIX_HELPER_NAME ->
            get_host_posix_storage_file_id(Config, CanonicalPath, Storage, MountInRoot);
        ?S3_HELPER_NAME ->
            get_host_s3_storage_file_id(CanonicalPath, MountInRoot)
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
        ChildHandle = sfm_test_utils:new_child_handle(RootHandle, integer_to_binary(N)),
        ok = sfm_test_utils:create_file(Worker, ChildHandle, 8#664),
        {ok, _} = sfm_test_utils:write_file(Worker, ChildHandle, 0, ?TEST_DATA)
    end, lists:seq(1, SubFilesNum));
create_nested_directory_tree(Worker, [SubDirsNum | Rest], RootHandle) ->
    ok = lists:foreach(fun(N) ->
        ChildHandle = sfm_test_utils:new_child_handle(RootHandle, integer_to_binary(N)),
        ok = sfm_test_utils:mkdir(Worker, ChildHandle, 8#775),
        ok = create_nested_directory_tree(Worker, Rest, ChildHandle)
    end, lists:seq(1, SubDirsNum)).


assertImportTimes(Worker, SpaceId) ->
    assertImportTimes(Worker, SpaceId, ?ATTEMPTS).

assertImportTimes(Worker, SpaceId, Attempts) ->
    StorageId = sfm_test_utils:get_storage_id(Worker, ?SPACE_ID),
    ?assertEqual(true, begin
        {ok, #document{
            value = #storage_sync_monitoring{
                import_start_time = StartTime,
                import_finish_time = FinishTime
            }}} = rpc:call(Worker, storage_sync_monitoring, get, [SpaceId, StorageId]),
        (StartTime =/= undefined) and (FinishTime =/= undefined) and (StartTime =< FinishTime)
    end, Attempts).

assertUpdateTimes(Worker, SpaceId) ->
    assertUpdateTimes(Worker, SpaceId, ?ATTEMPTS).

assertUpdateTimes(Worker, SpaceId, Attempts) ->
    StorageId = sfm_test_utils:get_storage_id(Worker, ?SPACE_ID),
    ?assertEqual(true, begin
        {ok, #document{
            value = #storage_sync_monitoring{
                last_update_start_time = StartTime,
                last_update_finish_time = FinishTime
            }}} = rpc:call(Worker, storage_sync_monitoring, get, [SpaceId, StorageId]),
        (StartTime =/= undefined) and (FinishTime =/= undefined) and (StartTime =< FinishTime)
    end, Attempts).

assertNoImportInProgress(Worker, SpaceId, Attempts) ->
    StorageId = sfm_test_utils:get_storage_id(Worker, ?SPACE_ID),
    ?assertEqual(true, begin
        {ok, #document{
            value = #storage_sync_monitoring{
                import_start_time = StartTime,
                import_finish_time = FinishTime
            }}} = rpc:call(Worker, storage_sync_monitoring, get, [SpaceId, StorageId]),
        ((StartTime =:= undefined) andalso (FinishTime =:= undefined))
            orelse ((FinishTime =/= undefined) andalso (StartTime =< FinishTime))
    end, Attempts).

assertNoUpdateInProgress(Worker, SpaceId, Attempts) ->
    StorageId = sfm_test_utils:get_storage_id(Worker, ?SPACE_ID),
    ?assertEqual(true, begin
        {ok, #document{
            value = #storage_sync_monitoring{
                last_update_start_time = StartTime,
                last_update_finish_time = FinishTime
            }}} = rpc:call(Worker, storage_sync_monitoring, get, [SpaceId, StorageId]),
        ((StartTime =:= undefined) andalso (FinishTime =:= undefined))
            orelse ((FinishTime =/= undefined) andalso (StartTime =< FinishTime))
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
    StorageId = sfm_test_utils:get_storage_id(Worker, SpaceId),
    SSM = rpc:call(Worker, storage_sync_monitoring, get_info, [SpaceId, StorageId]),
    SSM2 = flatten_histograms(SSM),
    try
        assert(ExpectedSSM, SSM2)
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
                        ++ Format, [Key, SpaceId, StorageId, ExpectedValue, Value | Args]),
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