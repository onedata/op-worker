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
-module(storage_sync_s3_test_base).
-author("Jakub Kudzia").

-include("storage_sync_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("kernel/include/file.hrl").

%% export for ct
-export([init_per_suite/1, end_per_suite/1, init_per_testcase/3, end_per_testcase/3]).

%% tests
-export([
    import_file_with_link_but_no_doc_test/2,
    create_file_in_dir_import_test/2,
    create_subfiles_import_many_test/2,
    create_subfiles_import_many2_test/2,
    cancel_scan/2,
    create_file_in_dir_update_test/2,
    create_file_in_dir_exceed_batch_update_test/2,
    update_syncs_files_after_import_failed_test/2,
    sync_should_not_reimport_deleted_but_still_opened_file/1,
    sync_should_not_import_recreated_file_with_suffix_on_storage/1,
    sync_should_update_blocks_of_recreated_file_with_suffix_on_storage/1,
    sync_should_not_import_replicated_file_with_suffix_on_storage/1,
    sync_should_update_replicated_file_with_suffix_on_storage/1,
    create_subfiles_and_delete_before_import_is_finished_test/2,
    delete_non_empty_directory_update_test/2,
    sync_works_properly_after_delete_test/2,
    delete_and_update_files_simultaneously_update_test/2,
    delete_many_subfiles_test/2,
    should_not_sync_file_during_replication/1,
    create_file_import_race_test/1,
    close_file_import_race_test/1
]).

-define(WRITE_TEXT, <<"overwrite_test_data">>).

%%%==================================================================
%%% Test functions
%%%===================================================================

import_file_with_link_but_no_doc_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),

    Ctx = rpc:call(W1, file_meta, get_ctx, []),
    TreeId = rpc:call(W1, oneprovider, get_id, []),
    FileUuid = datastore_key:new(),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_ID),
    {ok, _} = rpc:call(W1, datastore_model, add_links,
        [Ctx#{scope => ?SPACE_ID}, SpaceUuid, TreeId, {?TEST_FILE1, FileUuid}]),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),

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

create_file_in_dir_import_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    StorageTestDirPath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFilePath = storage_sync_test_base:storage_path(?SPACE_ID, filename:join(?TEST_DIR, ?TEST_FILE2), MountSpaceInRoot),
    timer:sleep(timer:seconds(1)),

    %% Create dir on storage
    SFMDirHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SFMDirHandle, 8#775),
    %% Create file on storage
    SFMFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SFMFileHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SFMFileHandle, 0, ?TEST_DATA),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),

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

create_subfiles_import_many_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage

    DirsNumber = 200,
    utils:pforeach(fun(N) ->
        NBin = integer_to_binary(N),
        DirPath = storage_sync_test_base:storage_path(?SPACE_ID, NBin, MountSpaceInRoot),
        FilePath = filename:join([DirPath, integer_to_binary(N)]),
        SFMDirHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, DirPath, RDWRStorage),
        ok = sd_test_utils:mkdir(W1, SFMDirHandle, 8#775),
        SFMFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, FilePath, RDWRStorage),
        ok = sd_test_utils:create_file(W1, SFMFileHandle, 8#664),
        {ok, _} = sd_test_utils:write_file(W1, SFMFileHandle, 0, ?TEST_DATA)
    end, lists:seq(1, DirsNumber)),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    Start = time_utils:system_time_millis(),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID, 60),
    End = time_utils:system_time_millis(),
    ct:print("Import took ~p", [(End - Start) / 1000]),

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

create_subfiles_import_many2_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    RootPath = storage_sync_test_base:storage_path(?SPACE_ID, <<"">>, MountSpaceInRoot),
    DirStructure = [10, 10, 10],
    RootSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, RootPath, RDWRStorage),
    storage_sync_test_base:create_nested_directory_tree(W1, DirStructure, RootSDHandle),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),

    Files = storage_sync_test_base:generate_nested_directory_tree_file_paths(DirStructure, ?SPACE_PATH),
    Start = time_utils:system_time_millis(),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),

    Timeout = 600,
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID, Timeout),
    End = time_utils:system_time_millis(),
    ct:print("Import took ~p", [(End - Start) / 1000]),
    storage_sync_test_base:parallel_assert(storage_sync_test_base, verify_file, [W1, SessId, Timeout], Files, Timeout),

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

cancel_scan(Config, MountSpaceInRoot) ->
    % storage_sync_dir_batch_size is set to 1 in init_per_testcase
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    RootPath = storage_sync_test_base:storage_path(?SPACE_ID, <<"">>, MountSpaceInRoot),
    DirStructure = [10, 10, 10],
    RootSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, RootPath, RDWRStorage),
    Timeout = 600,

    storage_sync_test_base:create_nested_directory_tree(W1, DirStructure, RootSDHandle),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    Files = storage_sync_test_base:generate_nested_directory_tree_file_paths(DirStructure, ?SPACE_PATH),

    TestProc = self(),
    test_utils:mock_new(W1, storage_sync_engine, [passthrough]),
    test_utils:mock_expect(W1, storage_sync_engine, import_file_unsafe,
        fun(StorageFileCtx, FileUuid, Info) ->
            TestProc ! start,
            meck:passthrough([StorageFileCtx, FileUuid, Info])
        end),

    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),
    receive start -> ok end,
    storage_sync_test_base:cancel(W1, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID, Timeout),

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

    ?assert(ToProcess < 1000),
    ?assert((OtherProcessed + Updated + Imported) < 1000),

    % check whether next scan will import missing files
    storage_sync_test_base:enable_update(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID, Timeout),
    storage_sync_test_base:disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"importedSum">> => 1000,
        <<"deletedSum">> => 0,
        <<"importedDayHist">> => 1000,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),
    storage_sync_test_base:parallel_assert(storage_sync_test_base, verify_file, [W1, SessId, Timeout], Files, Timeout).


create_file_in_dir_update_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    StorageTestFileinDirPath1 = storage_sync_test_base:storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

    %% Create dirs on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    SDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath2, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle2, 8#777),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),

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

    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    storage_sync_test_base:enable_update(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_update(Config),

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

create_file_in_dir_exceed_batch_update_test(Config, MountSpaceInRoot) ->
    % in this test storage_sync_dir_batch_size is set in init_per_testcase to 2
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),

    StorageTestDirPath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    StorageTestFilePath1 = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    StorageTestFilePath2 = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE2, MountSpaceInRoot),
    StorageTestFilePath3 = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE3, MountSpaceInRoot),
    StorageTestFilePath4 = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE4, MountSpaceInRoot),
    StorageTestFileinDirPath1 = storage_sync_test_base:storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

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
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),

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

    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    storage_sync_test_base:enable_update(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_update(Config),

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

delete_non_empty_directory_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFileinDirPath1 = storage_sync_test_base:storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),
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

    storage_sync_test_base:enable_update(Config, ?SPACE_ID, SyncedStorage),
    %% Delete dir on storage
    ok = sd_test_utils:recursive_rm(W1, SDHandle),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 4,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 2,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 1,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 2,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
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
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    StorageTestDirPath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    StorageTestFileinDirPath1 =
        storage_sync_test_base:storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),
    StorageTestFileinDirPath2 =
        storage_sync_test_base:storage_path(?SPACE_ID, filename:join([?TEST_DIR2, ?TEST_FILE2]), MountSpaceInRoot),

    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID, ?ATTEMPTS),

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

    storage_sync_test_base:enable_update(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_update(Config),

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
        <<"importedSum">> => 1,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 2,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 1,
        <<"importedDayHist">> => 1,
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
    storage_sync_test_base:enable_update(Config, ?SPACE_ID, SyncedStorage),

    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"toProcess">> => 4,
        <<"imported">> => 1,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"importedSum">> => 2,
        <<"updatedSum">> => 3,
        <<"deletedSum">> => 2,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
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
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID).

delete_and_update_files_simultaneously_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFileinDirPath1 = storage_sync_test_base:storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),
    StorageTestFileinDirPath2 = storage_sync_test_base:storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE2]), MountSpaceInRoot),
    NewMode = 8#600,
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),

    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    FileInDirSDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath2, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle2, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle2, 0, ?TEST_DATA),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),
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

    %% Delete file on storage
    sd_test_utils:unlink(W1, FileInDirSDHandle, ?TEST_DATA_SIZE),
    sd_test_utils:chmod(W1, FileInDirSDHandle2, NewMode),

    storage_sync_test_base:enable_update(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 5,
        <<"imported">> => 0,
        <<"deleted">> => 1,
        <<"updated">> => 1,
        <<"otherProcessed">> => 3,
        <<"failed">> => 0,
        <<"importedSum">> => 2,
        <<"deletedSum">> => 1,
        <<"importedMinHist">> => 2,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1
    }, ?SPACE_ID),

    %% Check if File1 was deleted in and if File2 was updated
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS).

delete_many_subfiles_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    RootPath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, RootPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    DirStructure = [10, 10, 10],
    storage_sync_test_base:create_nested_directory_tree(W1, DirStructure, SDHandle),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),
    Files = storage_sync_test_base:generate_nested_directory_tree_file_paths(DirStructure, ?SPACE_TEST_DIR_PATH),

    Timeout = 900,
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
        <<"importedHourHist">> => 1000,
        <<"importedDayHist">> => 1000,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 1,
        <<"updatedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    ok = sd_test_utils:recursive_rm(W1, SDHandle),
    storage_sync_test_base:enable_update(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID, Timeout),
    storage_sync_test_base:disable_update(Config),
    ?assertEqual({ok, []}, lfm_proxy:ls(W1, SessId, {path, ?SPACE_PATH}, 0, 1)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 1113,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 1111,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 1000,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 1111,
        <<"importedDayHist">> => 1000,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedHourHist">> => 1111,
        <<"deletedDayHist">> => 1111
    }, ?SPACE_ID).

update_syncs_files_after_import_failed_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),

    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    %ensure that import will start with different timestamp than dir was created
    timer:sleep(timer:seconds(1)),

    storage_sync_test_base:mock_import_file_error(W1, ?TEST_FILE1),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),

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

    storage_sync_test_base:unmock_import_file_error(W1),
    storage_sync_test_base:enable_update(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_update(Config),

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


sync_should_not_reimport_deleted_but_still_opened_file(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),

    StorageSpacePath = storage_sync_test_base:storage_path(?SPACE_ID, <<"">>, false),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SpaceSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageSpacePath, RDWRStorage),

    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change

    % create first file
    {ok, G1} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH, 8#644),
    {ok, H1} = lfm_proxy:open(W1, SessId, {guid, G1}, write),
    {ok, _} = lfm_proxy:write(W1, H1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, H1),

    % open file
    ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, G1}, read), ?ATTEMPTS),

    % delete file
    ok = lfm_proxy:unlink(W1, SessId, {guid, G1}),

    % there should be 1 file on storage
    ?assertMatch({ok, [_]}, sd_test_utils:listobjects(W1, SpaceSDHandle, ?SPACE_CANONICAL_PATH, 0, 10)),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    % there should be no files visible in the space
    ?assertMatch({ok, []},
        lfm_proxy:ls(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)),

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
    }, ?SPACE_ID).

sync_should_not_import_recreated_file_with_suffix_on_storage(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER2, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    StorageSpacePath = storage_sync_test_base:storage_path(?SPACE_ID, <<"">>, false),
    SpaceSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageSpacePath, RDWRStorage),
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change

    % create first file
    {ok, G1} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH, 8#644),
    {ok, H1} = lfm_proxy:open(W1, SessId, {guid, G1}, write),
    {ok, _} = lfm_proxy:write(W1, H1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, H1),
    % open file
    {ok, H2} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, G1}, read), ?ATTEMPTS),
    % delete file
    ok = lfm_proxy:unlink(W1, SessId, {guid, G1}),
    % recreate file with the same name as the deleted file

    {ok, G2} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH, 8#644),
    {ok, H3} = lfm_proxy:open(W1, SessId, {guid, G2}, write),
    {ok, _} = lfm_proxy:write(W1, H3, 0, ?TEST_DATA2),
    ok = lfm_proxy:close(W1, H3),

    % there should be 2 files on storage
    ?assertMatch({ok, [_, _]}, sd_test_utils:listobjects(W1, SpaceSDHandle, ?SPACE_CANONICAL_PATH, 0, 10)),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),

    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    % there should be only 1 file visible in the space
    ?assertMatch({ok, [_]},
        lfm_proxy:ls(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)),

    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W1, H2, 0, ?TEST_DATA_SIZE2)),
    ok = lfm_proxy:close(W1, H2),

    {ok, H5} = lfm_proxy:open(W1, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read),
    ?assertMatch({ok, ?TEST_DATA2}, lfm_proxy:read(W1, H5, 0, ?TEST_DATA_SIZE2)),
    ok = lfm_proxy:close(W1, H5),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"updated">> => 2,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 2,
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
    }, ?SPACE_ID).

sync_should_update_blocks_of_recreated_file_with_suffix_on_storage(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),

    StorageSpacePath = storage_sync_test_base:storage_path(?SPACE_ID, <<"">>, false),
    SpaceSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageSpacePath, RDWRStorage),
    StorageTestFilePath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE1, false),

    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change

    % create first file
    {ok, G1} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH, 8#644),
    {ok, H1} = lfm_proxy:open(W1, SessId, {guid, G1}, write),
    {ok, _} = lfm_proxy:write(W1, H1, 0, ?TEST_DATA2),
    ok = lfm_proxy:close(W1, H1),

    timer:sleep(timer:seconds(1)), %ensure that file1 will be updated

    % open file
    {ok, _} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, G1}, read), ?ATTEMPTS),

    % delete file
    ok = lfm_proxy:unlink(W1, SessId, {guid, G1}),

    % create second file with the same name as the deleted file
    {ok, G2} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH, 8#666),
    {ok, H3} = lfm_proxy:open(W1, SessId, {guid, G2}, write),
    {ok, _} = lfm_proxy:write(W1, H3, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, H3),
    U2 = file_id:guid_to_uuid(G2),
    StorageTestFilePathWithSuffix = ?CONFLICTING_STORAGE_FILE_NAME(StorageTestFilePath, U2),
    FileWithSuffixHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePathWithSuffix, RDWRStorage),

    % there should be 2 files on storage
    ?assertMatch({ok, [_, _]}, sd_test_utils:listobjects(W1, SpaceSDHandle, ?SPACE_CANONICAL_PATH, 0, 10)),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    % there should be only 1 file visible in the space
    ?assertMatch({ok, [_]}, lfm_proxy:ls(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"updated">> => 2,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 2,
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

    % change one byte in the suffixed file
    {ok, 1} = sd_test_utils:write_file(W1, FileWithSuffixHandle, ?CHANGED_BYTE_OFFSET, ?CHANGED_BYTE),

    storage_sync_test_base:enable_update(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"updated">> => 2,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 4,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedMinHist">> => 2,
        <<"updatedHourHist">> => 4,
        <<"updatedDayHist">> => 4,
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


sync_should_not_import_replicated_file_with_suffix_on_storage(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),

    StorageSpacePath = storage_sync_test_base:storage_path(?SPACE_ID, <<"">>, false),
    SpaceSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageSpacePath, RDWRStorage),

    {ok, G1} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH, 8#644),
    {ok, H1} = lfm_proxy:open(W1, SessId, {guid, G1}, write),
    {ok, _} = lfm_proxy:write(W1, H1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, H1),

    {ok, G2} = lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH, 8#644),
    {ok, H2} = lfm_proxy:open(W2, SessId2, {guid, G2}, write),
    {ok, _} = lfm_proxy:write(W2, H2, 0, ?TEST_DATA2),
    ok = lfm_proxy:close(W2, H2),

    % replicate file to W1
    {ok, H3} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, G2}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA2}, lfm_proxy:read(W1, H3, 0, 100), ?ATTEMPTS),
    ok = lfm_proxy:close(W1, H3),

    % there should be 2 files on storage
    ?assertMatch({ok, [_, _]}, sd_test_utils:listobjects(W1, SpaceSDHandle, ?SPACE_CANONICAL_PATH, 0, 10)),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    % there should be only 2 files visible in the space
    ?assertMatch({ok, [_, _]}, lfm_proxy:ls(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"updated">> => 3,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 3,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedMinHist">> => 3,
        <<"updatedHourHist">> => 3,
        <<"updatedDayHist">> => 3,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID).

sync_should_update_replicated_file_with_suffix_on_storage(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageSpacePath = storage_sync_test_base:storage_path(?SPACE_ID, <<"">>, false),
    StorageTestFilePath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE1, false),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    SpaceSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageSpacePath, RDWRStorage),

    {ok, G1} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH, 8#644),
    {ok, H1} = lfm_proxy:open(W1, SessId, {guid, G1}, write),
    {ok, _} = lfm_proxy:write(W1, H1, 0, ?TEST_DATA2),
    ok = lfm_proxy:close(W1, H1),

    {ok, G2} = lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH, 8#666),
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
    ?assertMatch({ok, [_, _]}, sd_test_utils:listobjects(W1, SpaceSDHandle, ?SPACE_CANONICAL_PATH, 0, 10)),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),

    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"updated">> => 3,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 0,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 3,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedMinHist">> => 3,
        <<"updatedHourHist">> => 3,
        <<"updatedDayHist">> => 3,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    % there should be only 2 files visible in the space
    ?assertMatch({ok, [_, _]}, lfm_proxy:ls(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100), ?ATTEMPTS),

    % change one byte in the suffixed file
    {ok, 1} = sd_test_utils:write_file(W1, FileWithSuffixHandle, ?CHANGED_BYTE_OFFSET, ?CHANGED_BYTE),

    storage_sync_test_base:enable_update(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"updated">> => 2,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 5,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedMinHist">> => 2,
        <<"updatedHourHist">> => 5,
        <<"updatedDayHist">> => 5,
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

create_subfiles_and_delete_before_import_is_finished_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#775),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    %% Create nested tree structure
    DirStructure = [10, 10, 10],
    storage_sync_test_base:create_nested_directory_tree(W1, DirStructure, SDHandle),
    storage_sync_test_base:enable_update(Config, ?SPACE_ID, SyncedStorage),

    ?assertEqual(true, 10 =< rpc:call(W1, storage_sync_monitoring, get_unhandled_jobs_value,
        [?SPACE_ID, initializer:get_supporting_storage_id(W1, ?SPACE_ID)]), ?ATTEMPTS),

    ok = sd_test_utils:recursive_rm(W1, SDHandle),
    ?assertMatch({ok, []}, sd_test_utils:listobjects(W1, SDHandle, ?SPACE_PATH, 0, 100)),
    ?assertMatch({ok, []},
        lfm_proxy:ls(W1, SessId, {path, ?SPACE_PATH}, 0, 100), 5 * ?ATTEMPTS),
    storage_sync_test_base:disable_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID).

should_not_sync_file_during_replication(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),

    {ok, FileGuid} =
        ?assertMatch({ok, _}, lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH, 8#777)),

    %check if file_meta was synced
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, FileHandle} =
        ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, FileGuid}, write)),
    Size = 1024 * 1024 * 64,
    TestData = crypto:strong_rand_bytes(Size),
    ?assertEqual({ok, Size}, lfm_proxy:write(W2, FileHandle, 0, TestData)),
    ?assertEqual(ok, lfm_proxy:fsync(W2, FileHandle)),
    ok = lfm_proxy:close(W2, FileHandle),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),

    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),
    storage_sync_test_base:schedule_spaces_check(W1, 2),

    {ok, FileHandle2} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, FileGuid}, read)),
    ?assertMatch({ok, TestData}, lfm_proxy:read(W1, FileHandle2, 0, Size), 600),
    ?assertMatch({ok, #file_attr{size = Size}}, lfm_proxy:stat(W2, SessId2, {guid, FileGuid})).


create_file_import_race_test(Config) ->
    [W1, W2 | _] = Workers = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_sync_test_base:get_rdwr_storage(Config, W1),
    Master = self(),
    CreateProc = spawn(fun() ->
        Ans = receive
            create ->
                try
                    {ok, _} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH, 8#777),
                    {ok, Handle} = lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, write),
                    {ok, _} = lfm_proxy:write(W1, Handle, 0, ?WRITE_TEXT),
                    ok = lfm_proxy:close(W1, Handle)
                catch
                    E1:E2 ->
                        {E1, E2}
                end
        after
            60000 ->
                timeout
        end,
        Master ! {create_ans, Ans}
    end),

    test_utils:mock_new(Workers, storage_sync_engine, [passthrough]),
    test_utils:mock_expect(Workers, storage_sync_engine, import_file_unsafe,
        fun(StorageFileCtx, FileUuid, Info) ->
            CreateProc ! create,
            timer:sleep(5000),
            meck:passthrough([StorageFileCtx, FileUuid, Info])
        end),

    StorageTestFilePath = storage_sync_test_base:storage_path(?SPACE_ID, ?TEST_FILE1, false),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    ok = sd_test_utils:create_file(W1, SDHandle, 8#664),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),

    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    CreateAns = receive
        {create_ans, A} -> A
    after
        60000 -> timeout
    end,
    ?assertEqual(ok, CreateAns),

    %% Check if file was imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?WRITE_TEXT},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?WRITE_TEXT))),
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
    ?assertMatch({ok, ?WRITE_TEXT},
        lfm_proxy:read(W2, Handle2, 0, 100), ?ATTEMPTS).

close_file_import_race_test(Config) ->
    [W1 | _] = Workers = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),

    {ok, {_, CreateHandle}} = lfm_proxy:create_and_open(W1, SessId, ?SPACE_TEST_FILE_PATH, 8#777),
    {ok, _} = lfm_proxy:write(W1, CreateHandle, 0, ?WRITE_TEXT),
    ok = lfm_proxy:unlink(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}),

    Master = self(),
    ActionProc = spawn(fun() ->
        Ans = receive
            do_action ->
                try
                    ok = lfm_proxy:close(W1, CreateHandle)
                catch
                    E1:E2 ->
                        {E1, E2}
                end
        after
            60000 ->
                timeout
        end,
        Master ! {action_result, Ans}
    end),

    test_utils:mock_new(Workers, link_utils, [passthrough]),
    test_utils:mock_expect(Workers, link_utils, try_to_resolve_child_deletion_link, fun
        (?TEST_FILE1, ParentCtx) ->
            ActionProc ! do_action,
            timer:sleep(5000),
            meck:passthrough([?TEST_FILE1, ParentCtx]);
        (FileName, ParentCtx) ->
            meck:passthrough([FileName, ParentCtx])
    end),
    SyncedStorage = storage_sync_test_base:get_synced_storage(Config, W1),
    storage_sync_test_base:enable_import(Config, ?SPACE_ID, SyncedStorage),

    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    ActionResult = receive
        {action_result, A} -> A
    after
        60000 -> timeout
    end,
    ?assertEqual(ok, ActionResult),
    timer:sleep(10000),

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

    %% Check if file was imported on W1
    ?assertMatch({error, enoent}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH})).

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
    [{?LOAD_MODULES, [initializer, storage_sync_test_base, sd_test_utils, ?MODULE]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    ok = wpool:stop_sup_pool(?VERIFY_POOL),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    initializer:unmock_provider_ids(Config),
    ssl:stop().

init_per_testcase(create_file_in_dir_update_test, Config, Readonly) ->
    Config2 = [
        {update_config, #{
            delete_enable => false,
            write_once => true}} | Config
    ],
    init_per_testcase(default, Config2, Readonly);

init_per_testcase(cancel_scan, Config, Readonly) ->
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
    init_per_testcase(default, Config2, Readonly);

init_per_testcase(create_file_in_dir_exceed_batch_update_test, Config, Readonly) ->
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
    init_per_testcase(default, Config2, Readonly);

init_per_testcase(Case, Config, Readonly) when
    Case =:= delete_empty_directory_update_test;
    Case =:= delete_non_empty_directory_update_test;
    Case =:= delete_file_update_test;
    Case =:= delete_many_subfiles_test;
    Case =:= move_file_update_test;
    Case =:= create_subfiles_and_delete_before_import_is_finished_test
    ->
    Config2 = [
        {update_config, #{
            delete_enable => true,
            write_once => true}} | Config
    ],
    init_per_testcase(default, Config2, Readonly);

init_per_testcase(Case, Config, Readonly) when
    Case =:= delete_and_update_files_simultaneously_update_test;
    Case =:= create_delete_import2_test;
    Case =:= recreate_file_deleted_by_sync_test;
    Case =:= sync_should_not_delete_not_replicated_file_created_in_remote_provider;
    Case =:= sync_should_not_delete_dir_created_in_remote_provider;
    Case =:= sync_should_not_delete_not_replicated_files_created_in_remote_provider2;
    Case =:= sync_should_not_invalidate_file_after_replication;
    Case =:= sync_works_properly_after_delete_test ->
    Config2 = [
        {update_config, #{
            delete_enable => true,
            write_once => false}} | Config
    ],
    init_per_testcase(default, Config2, Readonly);

init_per_testcase(_Case, Config, Readonly) ->
    ct:timetrap({minutes, 20}),
    ConfigWithProxy = lfm_proxy:init(Config),
    Config2 = storage_sync_test_base:add_synced_storages(ConfigWithProxy),
    Config3 = storage_sync_test_base:add_rdwr_storages(Config2),
    storage_sync_test_base:create_init_file(Config3, Readonly),
    Config3.

end_per_testcase(cancel_scan, Config, Readonly) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    OldDirBatchSize = ?config(old_storage_sync_dir_batch_size, Config),
    test_utils:set_env(W1, op_worker, storage_sync_dir_batch_size, OldDirBatchSize),
    end_per_testcase(default, Config, Readonly);

end_per_testcase(create_file_in_dir_exceed_batch_update_test, Config, Readonly) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    OldDirBatchSize = ?config(old_storage_sync_dir_batch_size, Config),
    test_utils:set_env(W1, op_worker, storage_sync_dir_batch_size, OldDirBatchSize),
    end_per_testcase(default, Config, Readonly);

end_per_testcase(_Case, Config, Readonly) ->
    Workers = [W1 | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(W) -> lfm_proxy:close_all(W) end, Workers),
    storage_sync_test_base:clean_reverse_luma_cache(W1),
    storage_sync_test_base:disable_storage_sync(Config),
    storage_sync_test_base:clean_traverse_tasks(W1),
    storage_sync_test_base:clean_synced_storage(Config, Readonly),
    storage_sync_test_base:clean_space(Config),
    storage_sync_test_base:cleanup_storage_sync_monitoring_model(W1, ?SPACE_ID),
    test_utils:mock_unload(Workers, [storage_sync_engine, storage_sync_hash, link_utils]),
    timer:sleep(timer:seconds(1)),
    lfm_proxy:teardown(Config).