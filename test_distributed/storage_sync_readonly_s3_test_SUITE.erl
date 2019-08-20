%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage_sync
%%% @end
%%%--------------------------------------------------------------------
-module(storage_sync_readonly_s3_test_SUITE).
-author("Jakub Kudzia").

-include("storage_sync_test.hrl").
-include("modules/storage_sync/strategy_config.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_sufix.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("kernel/include/file.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    create_empty_file_import_test/1,
    create_file_import_test/1,
    import_file_with_link_but_no_doc_test/1,
    create_file_in_dir_import_test/1,
    create_subfiles_import_many_test/1,
    create_subfiles_import_many2_test/1,
    create_file_in_dir_update_test/1,
    create_file_in_dir_exceed_batch_update_test/1,
    update_syncs_files_after_import_failed_test/1,
    update_syncs_files_after_previous_update_failed_test/1,
    append_file_update_test/1,
    truncate_file_update_test/1,
    change_file_content_constant_size_test/1,
    change_file_content_update_test/1,
    sync_should_not_invalidate_file_after_replication/1
]).

-define(TEST_CASES,[
    create_empty_file_import_test,
    create_file_import_test,
    import_file_with_link_but_no_doc_test,
    create_file_in_dir_import_test,
    create_subfiles_import_many_test,
    create_subfiles_import_many2_test,
    create_file_in_dir_update_test,
    create_file_in_dir_exceed_batch_update_test,
    update_syncs_files_after_import_failed_test,
    update_syncs_files_after_previous_update_failed_test,
    append_file_update_test,
    truncate_file_update_test,
    change_file_content_constant_size_test,
    change_file_content_update_test,
    sync_should_not_invalidate_file_after_replication
]).

all() -> ?ALL(?TEST_CASES).

-define(WRITE_TEXT, <<"overwrite_test_data">>).

%%%==================================================================
%%% Test functions
%%%===================================================================

create_empty_file_import_test(Config) ->
    storage_sync_test_base:create_empty_file_import_test(Config, true).

create_file_import_test(Config) ->
    storage_sync_test_base:create_file_import_test(Config, true).

import_file_with_link_but_no_doc_test(Config) ->
    MountSpaceInRoot = true,
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),

    Ctx = rpc:call(W1, file_meta, get_ctx, []),
    TreeId = rpc:call(W1, oneprovider, get_id, []),
    FileUuid = datastore_utils:gen_key(),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_ID),
    {ok, _} = rpc:call(W1, datastore_model, add_links,
        [Ctx#{scope => ?SPACE_ID}, SpaceUuid, TreeId, {?TEST_FILE1, FileUuid}]),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

    % wait till scan is finished
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was imported
    {ok, #file_attr{guid = FileGuid}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    ?assertMatch(FileUuid, file_id:guid_to_uuid(FileGuid)),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),

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

create_file_in_dir_import_test(Config) ->
    MountSpaceInRoot = true,
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    StorageTestDirPath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFilePath = storage_sync_test_base:storage_path(?SPACE_ID, filename:join(?TEST_DIR, ?TEST_FILE2), MountSpaceInRoot),
    timer:sleep(timer:seconds(1)),

    %% Create dir on storage
    SFMDirHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMDirHandle, 8#775),
    %% Create file on storage
    SFMFileHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMFileHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMFileHandle, 0, ?TEST_DATA),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

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
    }, ?SPACE_ID).

create_subfiles_import_many_test(Config) ->
    MountSpaceInRoot = true,
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage

    DirsNumber = 200,
    utils:pforeach(fun(N) ->
        NBin = integer_to_binary(N),
        DirPath = storage_sync_test_base:storage_path(?SPACE_ID, NBin, MountSpaceInRoot),
        FilePath = filename:join([DirPath, integer_to_binary(N)]),
        SFMDirHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, DirPath, RDWRStorage),
        ok = sfm_test_utils:mkdir(W1, SFMDirHandle, 8#775),
        SFMFileHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, FilePath, RDWRStorage),
        ok = sfm_test_utils:create_file(W1, SFMFileHandle, 8#664),
        {ok, _} = sfm_test_utils:write_file(W1, SFMFileHandle, 0, ?TEST_DATA)
    end, lists:seq(1, DirsNumber)),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    storage_sync_test_base:parallel_assert(storage_sync_test_base, verify_file_in_dir, [W1, SessId, 60], lists:seq(1, DirsNumber), 60),

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

create_subfiles_import_many2_test(Config) ->
    MountSpaceInRoot = true,
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    RootPath = storage_sync_test_base:storage_path(?SPACE_ID, <<"">>, MountSpaceInRoot),
    DirStructure = [1000],
    RootSFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, RootPath, RDWRStorage),
    storage_sync_test_base:create_nested_directory_tree(W1, DirStructure, RootSFMHandle),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    Files = storage_sync_test_base:generate_nested_directory_tree_file_paths(DirStructure, ?SPACE_PATH),

    Timeout = 600,
    storage_sync_test_base:parallel_assert(storage_sync_test_base, verify_file, [W1, SessId, Timeout], Files, Timeout),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID, Timeout),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 1002,
        <<"imported">> => 1000,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 1000,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedDayHist">> => 1000,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

create_file_in_dir_update_test(Config) ->
    MountSpaceInRoot = true,
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    StorageTestFileinDirPath1 = storage_sync_test_base:storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

    %% Create dirs on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle, 8#777),
    SFMHandle2 = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath2, RDWRStorage),
    ok = sfm_test_utils:mkdir(W1, SFMHandle2, 8#777),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),
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

    FileInDirSFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, FileInDirSFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, FileInDirSFMHandle, 0, ?TEST_DATA),
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_storage_update(Config),

    %% Check if files were imported on W1
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

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),

    {ok, Handle5} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle5, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle5),


    %% Check if file was imported on W2
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), 5 * ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS).

create_file_in_dir_exceed_batch_update_test(Config) ->
    MountSpaceInRoot = true,
    % in this test storage_sync_object_dir_batch_size is set in init_per_testcase to 2
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),

    StorageTestDirPath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    StorageTestFilePath1 = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    StorageTestFilePath2 = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE2, MountSpaceInRoot),
    StorageTestFilePath3 = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE3, MountSpaceInRoot),
    StorageTestFilePath4 = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE4, MountSpaceInRoot),
    StorageTestFileinDirPath1 = storage_sync_test_base:storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

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
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_storage_import(Config, ?SPACE_ID, SyncedStorage),

    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    %% Check if files were imported on W1
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
        <<"toProcess">> => 7,
        <<"imported">> => 4,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"importedSum">> => 4,
        <<"updatedSum">> => 1,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 4,
        <<"importedHourHist">> => 4,
        <<"importedDayHist">> => 4,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    ok = sfm_test_utils:create_file(W1, FileInDirSFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, FileInDirSFMHandle, 0, ?TEST_DATA),
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_storage_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 8,
        <<"imported">> => 1,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 6,
        <<"importedSum">> => 5,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 5,
        <<"importedDayHist">> => 5,
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

    {ok, Handle5} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle5, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle5),

    %% Check if file was imported on W2
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), 5 * ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS).

update_syncs_files_after_import_failed_test(Config) ->
    MountSpaceInRoot = true,
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),

    %% Create dir on storage
    SFMHandle = sfm_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    %ensure that import will start with different timestamp than dir was created
    timer:sleep(timer:seconds(1)),

    test_utils:mock_new(W1, simple_scan),
    test_utils:mock_expect(W1, simple_scan, import_file,
        fun(Job = #space_strategy_job{
            data = #{file_name := FileName}
        }, Uuid) ->
            case FileName of
                ?TEST_FILE1 ->
                    throw(test_error);
                _ ->
                    meck:passthrough([Job, Uuid])
            end
        end),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),

    storage_sync_test_base:enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

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
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_storage_update(Config),

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

update_syncs_files_after_previous_update_failed_test(Config) ->
    MountSpaceInRoot = true,
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_storage_import(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

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
    ok = sfm_test_utils:create_file(W1, SFMHandle, 8#664),
    {ok, _} = sfm_test_utils:write_file(W1, SFMHandle, 0, ?TEST_DATA),
    timer:sleep(timer:seconds(1)),
    test_utils:mock_new(W1, simple_scan),
    test_utils:mock_expect(W1, simple_scan, import_file, fun(Job = #space_strategy_job{
        data = #{file_name := FileName}}, Uuid
    ) ->
        case FileName of
            ?TEST_FILE1 ->
                throw(test_error);
            _ ->
                meck:passthrough([Job, Uuid])
        end
    end),
    storage_sync_test_base:enable_storage_update(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
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
        <<"updatedMinHist">> => 1,
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


append_file_update_test(Config) ->
    storage_sync_test_base:append_file_update_test(Config, true).

truncate_file_update_test(Config) ->
    storage_sync_test_base:truncate_file_update_test(Config, true).

change_file_content_constant_size_test(Config) ->
    storage_sync_test_base:change_file_content_constant_size_test(Config, true).

change_file_content_update_test(Config) ->
    storage_sync_test_base:change_file_content_update_test(Config, true).

sync_should_not_invalidate_file_after_replication(Config) ->
    storage_sync_test_base:sync_should_not_invalidate_file_after_replication(Config).

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
    [{?LOAD_MODULES, [initializer, storage_sync_test_base, sfm_test_utils, ?MODULE]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    ok = wpool:stop_sup_pool(?VERIFY_POOL),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    initializer:unmock_provider_ids(Config),
    ssl:stop().

init_per_testcase(create_file_in_dir_update_test, Config) ->
    Config2 = [
        {update_config, #{
            delete_enable => false,
            write_once => true}} | Config
    ],
    init_per_testcase(default, Config2);


init_per_testcase(create_file_in_dir_exceed_batch_update_test, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, OldDirBatchSize} = test_utils:get_env(W1, op_worker, storage_sync_object_dir_batch_size),
    test_utils:set_env(W1, op_worker, storage_sync_object_dir_batch_size, 2),
    Config2 = [
        {update_config, #{
            delete_enable => false,
            write_once => true}},
        {old_storage_sync_object_dir_batch_size, OldDirBatchSize}
        | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 20}),
    ConfigWithProxy = lfm_proxy:init(Config),
    Config2 = storage_sync_test_base:add_synced_storages(ConfigWithProxy),
    Config3 = storage_sync_test_base:add_rdwr_storages(Config2),
    storage_sync_test_base:create_init_file(Config3, false),
    Config3.

end_per_testcase(create_file_in_dir_exceed_batch_update_test, Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    OldDirBatchSize = ?config(old_storage_sync_object_dir_batch_size, Config),
    test_utils:set_env(W1, op_worker, storage_sync_object_dir_batch_size, OldDirBatchSize),
    end_per_testcase(default, Config);

end_per_testcase(_Case, Config) ->
    Workers = [W1 | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(W) -> lfm_proxy:close_all(W) end, Workers),
    storage_sync_test_base:clean_reverse_luma_cache(W1),
    storage_sync_test_base:disable_storage_sync(Config),
    storage_sync_test_base:clean_synced_storage(Config, false),
    storage_sync_test_base:clean_space(Config),
    storage_sync_test_base:cleanup_storage_sync_monitoring_model(W1, ?SPACE_ID),
    test_utils:mock_unload(Workers, [simple_scan, storage_sync_changes, link_utils]),
    timer:sleep(timer:seconds(1)),
    lfm_proxy:teardown(Config).