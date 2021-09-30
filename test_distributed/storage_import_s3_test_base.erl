%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module stores test bases of storage import on S3.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_import_s3_test_base).
-author("Jakub Kudzia").

-include("storage_import_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("kernel/include/file.hrl").

% TODO VFS-6161 divide to smaller test suites

%% export for ct
-export([init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    create_file_in_dir_import_test/1,
    create_subfiles_import_many_test/1,
    create_subfiles_import_many2_test/1,

    update_syncs_files_after_import_failed_test/1,
    update_syncs_files_after_previous_update_failed_test/1,
    sync_should_not_reimport_deleted_but_still_opened_file/2,
    sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed/1,
    create_subfiles_and_delete_before_import_is_finished_test/1,
    changing_max_depth_test/1,
    create_file_in_dir_update_test/1,
    create_file_in_dir_exceed_batch_update_test/1,
    force_stop_test/1,

    delete_non_empty_directory_update_test/1,
    sync_works_properly_after_delete_test/1,
    delete_and_update_files_simultaneously_update_test/1,
    delete_file_in_dir_update_test/1,
    delete_many_subfiles_test/1,
    create_list_race_test/1,

    change_file_type_test/1,
    change_file_type3_test/1
]).

%%%==================================================================
%%% Test functions
%%%===================================================================

create_file_in_dir_import_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    StorageTestDirPath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestFilePath = storage_import_test_base:provider_storage_path(?SPACE_ID, filename:join(?TEST_DIR, ?TEST_FILE2)),
    timer:sleep(timer:seconds(1)),

    %% Create dir on storage
    SFMDirHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SFMDirHandle, ?DEFAULT_DIR_PERMS),
    %% Create file on storage
    SFMFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SFMFileHandle, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, SFMFileHandle, 0, ?TEST_DATA),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),

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

    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID).

create_subfiles_import_many_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage

    DirsNumber = 200,
    lists_utils:pforeach(fun(N) ->
        NBin = integer_to_binary(N),
        DirPath = storage_import_test_base:provider_storage_path(?SPACE_ID, NBin),
        FilePath = filename:join([DirPath, integer_to_binary(N)]),
        SFMDirHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, DirPath, RDWRStorage),
        ok = sd_test_utils:mkdir(W1, SFMDirHandle, ?DEFAULT_DIR_PERMS),
        SFMFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, FilePath, RDWRStorage),
        ok = sd_test_utils:create_file(W1, SFMFileHandle, ?DEFAULT_FILE_PERMS),
        {ok, _} = sd_test_utils:write_file(W1, SFMFileHandle, 0, ?TEST_DATA)
    end, lists:seq(1, DirsNumber)),
    Stopwatch = stopwatch:start(),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),
    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID, 60),
    ct:pal("Import took ~p", [stopwatch:read_seconds(Stopwatch, float)]),

    storage_import_test_base:parallel_assert(storage_import_test_base, verify_file_in_dir, [W1, SessId, 60], lists:seq(1, DirsNumber), 60),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"created">> => 200,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
        <<"createdHourHist">> => 200,
        <<"createdDayHist">> => 200,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID).

create_subfiles_import_many2_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    RootPath = storage_import_test_base:provider_storage_path(?SPACE_ID, <<"">>),
    DirStructure = [10, 10, 10],
    RootSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, RootPath, RDWRStorage),
    storage_import_test_base:create_nested_directory_tree(W1, DirStructure, RootSDHandle),
    Files = storage_import_test_base:generate_nested_directory_tree_file_paths(DirStructure, ?SPACE_PATH),
    Stopwatch = stopwatch:start(),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),

    Timeout = 600,
    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID, Timeout),
    ct:pal("Import took ~p", [stopwatch:read_seconds(Stopwatch, float)]),
    storage_import_test_base:parallel_assert(storage_import_test_base, verify_file, [W1, SessId, Timeout], Files, Timeout),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"created">> => 1000,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 1,
        <<"createdDayHist">> => 1000,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID).


create_file_in_dir_update_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestDirPath2 = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_DIR2),
    StorageTestFileinDirPath1 = storage_import_test_base:provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),

    %% Create dirs on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    SDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath2, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle2, 8#777),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),

    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID),
    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertSecondScanFinished(W1, ?SPACE_ID),
    storage_import_test_base:disable_continuous_scan(Config),

    %% Check if files were imported on W1
    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 2,
        <<"modifiedDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
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
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS).

changing_max_depth_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),

    StorageTestDirPath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_DIR),

    Dir2Path = filename:join([?TEST_DIR, ?TEST_DIR2]),
    StorageTestDirPath2 = storage_import_test_base:provider_storage_path(?SPACE_ID, Dir2Path),
    SpaceTestDirPath2 = ?SPACE_TEST_DIR_PATH(Dir2Path),

    StorageTestFilePath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    StorageTestFileinDirPath = storage_import_test_base:provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),

    File2Path = filename:join([?TEST_DIR, ?TEST_DIR2, ?TEST_FILE1]),
    StorageTestFileinDirPath2 = storage_import_test_base:provider_storage_path(?SPACE_ID, File2Path),
    SpaceTestFilePath2 = ?SPACE_TEST_DIR_PATH(File2Path),

    %% Create directories and files on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, ?DEFAULT_DIR_PERMS),
    SDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath2, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle2, ?DEFAULT_DIR_PERMS),
    FileSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileSDHandle, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, FileSDHandle, 0, ?TEST_DATA),
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    FileInDirSDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath2, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle2, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle2, 0, ?TEST_DATA),

    % in init_per_testcase, max_depth for import is set to 1
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),

    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID),

    %% Only file on 1st level should be imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, [_]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 10), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),


    % in init_per_testcase, max_depth for update is set to 2, so new file should be detected
    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertSecondScanFinished(W1, ?SPACE_ID),
    storage_import_test_base:disable_continuous_scan(Config),

    %% File on 2nd level and directory on 1 st level should be imported
    ?assertMatch({ok, [_, _]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, [_]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    {ok, Handle5} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle5, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle5),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 2,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    % run another scan with max_depth = 3
    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID, #{max_depth => 3}),
    storage_import_test_base:assertScanFinished(W1, ?SPACE_ID, 3),
    storage_import_test_base:disable_continuous_scan(Config),

    %% Directory on 2nd and file on 3rd level should be imported
    ?assertMatch({ok, [_, _]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, SpaceTestDirPath2}), ?ATTEMPTS),
    ?assertMatch({ok, [_]},
        lfm_proxy:get_children(W1, SessId, {path, SpaceTestDirPath2}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, SpaceTestFilePath2}), ?ATTEMPTS),
    {ok, Handle6} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, SpaceTestFilePath2}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle6, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle6),

    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"created">> => 1,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 3,
        <<"createdHourHist">> => 3,
        <<"createdDayHist">> => 3,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID).

create_file_in_dir_exceed_batch_update_test(Config) ->
    % in this test storage_import_dir_batch_size is set in init_per_testcase to 2
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),

    StorageTestDirPath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestDirPath2 = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_DIR2),
    StorageTestFilePath1 = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    StorageTestFilePath2 = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_FILE2),
    StorageTestFilePath3 = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_FILE3),
    StorageTestFilePath4 = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_FILE4),
    StorageTestFileinDirPath1 = storage_import_test_base:provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),

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
    ok = sd_test_utils:create_file(W1, FileSDHandle, ?DEFAULT_FILE_PERMS),
    ok = sd_test_utils:create_file(W1, FileSDHandle2, ?DEFAULT_FILE_PERMS),
    ok = sd_test_utils:create_file(W1, FileSDHandle3, ?DEFAULT_FILE_PERMS),
    ok = sd_test_utils:create_file(W1, FileSDHandle4, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, FileSDHandle, 0, ?TEST_DATA),
    {ok, _} = sd_test_utils:write_file(W1, FileSDHandle2, 0, ?TEST_DATA),
    {ok, _} = sd_test_utils:write_file(W1, FileSDHandle3, 0, ?TEST_DATA),
    {ok, _} = sd_test_utils:write_file(W1, FileSDHandle4, 0, ?TEST_DATA),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),

    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID),

    %% Check if files were imported on W1
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
        <<"created">> => 4,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 2,
        <<"createdMinHist">> => 4,
        <<"createdHourHist">> => 4,
        <<"createdDayHist">> => 4,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertSecondScanFinished(W1, ?SPACE_ID),
    storage_import_test_base:disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 7,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 5,
        <<"createdDayHist">> => 5,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
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
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS).

update_syncs_files_after_import_failed_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),

    %% Create file on storage
    timer:sleep(timer:seconds(1)),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, ?DEFAULT_FILE_PERMS),
    storage_import_test_base:mock_import_file_error(W1, ?TEST_FILE1),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),

    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID),

    %% Check if dir was not imported
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH})),
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH})),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 1,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    storage_import_test_base:unmock_import_file_error(W1),
    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertSecondScanFinished(W1, ?SPACE_ID),
    storage_import_test_base:disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedHourHist">> => 2,
        <<"modifiedDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS).


update_syncs_files_after_previous_update_failed_test(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),
    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    %% Create dir on storage
    timer:sleep(timer:seconds(1)),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, ?DEFAULT_FILE_PERMS),
    timer:sleep(timer:seconds(1)),

    storage_import_test_base:mock_import_file_error(W1, ?TEST_FILE1),
    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertSecondScanFinished(W1, ?SPACE_ID),
    storage_import_test_base:disable_continuous_scan(Config),
    storage_import_test_base:unmock_import_file_error(W1),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 1,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if dir was not imported
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1})),
    ?assertNotMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1})),

    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertScanFinished(W1, ?SPACE_ID, 3, ?ATTEMPTS),
    storage_import_test_base:disable_continuous_scan(Config),

    %next scan should import file
    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"created">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"modified">> => 1,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID, ?ATTEMPTS),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS).


sync_should_not_reimport_deleted_but_still_opened_file(Config, StorageType) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),

    StorageSpacePath = storage_import_test_base:provider_storage_path(?SPACE_ID, <<"">>),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    SpaceSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageSpacePath, RDWRStorage),

    % create first file
    {ok, G1} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1),
    {ok, H1} = lfm_proxy:open(W1, SessId, ?FILE_REF(G1), write),
    {ok, _} = lfm_proxy:write(W1, H1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, H1),

    % open file
    ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, ?FILE_REF(G1), read), ?ATTEMPTS),
    % delete file
    ok = lfm_proxy:unlink(W1, SessId, ?FILE_REF(G1)),
    %ensure that space_dir mtime will change
    timer:sleep(timer:seconds(1)),

    % there should be 1 file on storage
    ?assertMatch({ok, [_]}, sd_test_utils:storage_ls(W1, SpaceSDHandle, 0, 10, StorageType)),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),
    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID),

    % there should be no files visible in the space
    ?assertMatch({ok, []},
        lfm_proxy:get_children(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 1,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID).


sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    StorageTestFilePath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_FILE1),
    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),
    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertSecondScanFinished(W1, ?SPACE_ID),
    storage_import_test_base:disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 1,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 2,
        <<"modifiedDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID).

create_subfiles_and_delete_before_import_is_finished_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_DIR),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, ?DEFAULT_DIR_PERMS),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),
    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID),

    %% Create nested tree structure
    DirStructure = [10, 10, 10],
    storage_import_test_base:create_nested_directory_tree(W1, DirStructure, SDHandle),
    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),

    ?assertEqual(true, rpc:call(W1, storage_import_monitoring, is_scan_in_progress, [?SPACE_ID]), ?ATTEMPTS),

    ok = sd_test_utils:recursive_rm(W1, SDHandle),
    ?assertMatch({ok, []}, sd_test_utils:listobjects(W1, SDHandle,  ?DEFAULT_MARKER, 0, 100)),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId,  {path, ?SPACE_TEST_DIR_PATH}), 10 * ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 100), 2 * ?ATTEMPTS).


force_stop_test(Config) ->
    % storage_import_dir_batch_size is set to 1 in init_per_testcase
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    RootPath = storage_import_test_base:provider_storage_path(?SPACE_ID, <<"">>),
    DirStructure = [10, 10, 10],
    RootSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, RootPath, RDWRStorage),
    Timeout = 600,

    storage_import_test_base:create_nested_directory_tree(W1, DirStructure, RootSDHandle),
    Files = storage_import_test_base:generate_nested_directory_tree_file_paths(DirStructure, ?SPACE_PATH),

    TestProc = self(),
    ok = test_utils:mock_new(W1, storage_import_engine, [passthrough]),
    ok = test_utils:mock_expect(W1, storage_import_engine, import_file_unsafe,
        fun(StorageFileCtx, Info) ->
            TestProc ! start,
            meck:passthrough([StorageFileCtx, Info])
        end),

    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),
    receive start -> ok end,
    storage_import_test_base:stop_scan(W1, ?SPACE_ID),
    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID, Timeout),

    SSM = ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0
    }, ?SPACE_ID),

    #{
        <<"unmodified">> := Unmodified,
        <<"modified">> := Modified,
        <<"created">> := Created
    } = SSM,

    ?assert((Unmodified + Modified + Created) < 1000),

    % check whether next scan will import missing files
    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertScanFinished(W1, ?SPACE_ID, 2, Timeout),
    storage_import_test_base:disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdDayHist">> => 1000,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),
    storage_import_test_base:parallel_assert(storage_import_test_base, verify_file, [W1, SessId, Timeout], Files, Timeout).


delete_non_empty_directory_update_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestFileinDirPath1 = storage_import_test_base:provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),
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

    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    %% Delete dir on storage
    ok = sd_test_utils:recursive_rm(W1, SDHandle),
    storage_import_test_base:assertSecondScanFinished(W1, ?SPACE_ID),
    storage_import_test_base:disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"modified">> => 0,
        <<"deleted">> => 2,
        <<"failed">> => 0,
        <<"unmodified">> => 1,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 2,
        <<"deletedHourHist">> => 2,
        <<"deletedDayHist">> => 2,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if dir was deleted in space
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS).

sync_works_properly_after_delete_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    StorageTestDirPath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestDirPath2 = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_DIR2),
    StorageTestFileinDirPath1 =
        storage_import_test_base:provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),
    StorageTestFileinDirPath2 =
        storage_import_test_base:provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR2, ?TEST_FILE2])),

    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),
    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID, ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
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

    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertSecondScanFinished(W1, ?SPACE_ID),
    storage_import_test_base:disable_continuous_scan(Config),

    %% Check if dir was deleted in space
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"deleted">> => 2,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 2,
        <<"modifiedDayHist">> => 2,
        <<"deletedMinHist">> => 2,
        <<"deletedHourHist">> => 2,
        <<"deletedDayHist">> => 2,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    % ensure that mtime will be changed
    timer:sleep(timer:seconds(1)),
    SDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath2, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle2, 8#777),
    FileInDirSDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath2, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle2, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle2, 0, ?TEST_DATA),

    SpaceTestFileInDirPath = ?SPACE_TEST_FILE_IN_DIR_PATH(?TEST_DIR2, ?TEST_FILE2),
    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertScanFinished(W1, ?SPACE_ID, 3, ?ATTEMPTS),
    storage_import_test_base:disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 3,
        <<"modifiedDayHist">> => 3,
        <<"deletedMinHist">> => 2,
        <<"deletedHourHist">> => 2,
        <<"deletedDayHist">> => 2,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
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
    StorageTestDirPath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestFileinDirPath1 = storage_import_test_base:provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),
    StorageTestFileinDirPath2 = storage_import_test_base:provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE2])),
    NewMode = 8#600,
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),

    %% Create dir on storage
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    FileInDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath1, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle, 0, ?TEST_DATA),
    FileInDirSDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath2, RDWRStorage),
    ok = sd_test_utils:create_file(W1, FileInDirSDHandle2, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, FileInDirSDHandle2, 0, ?TEST_DATA),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),
    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 2,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 2,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    %% Delete file on storage
    sd_test_utils:unlink(W1, FileInDirSDHandle, ?TEST_DATA_SIZE),
    sd_test_utils:chmod(W1, FileInDirSDHandle2, NewMode),

    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertSecondScanFinished(W1, ?SPACE_ID),
    storage_import_test_base:disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 1,
        <<"modified">> => 1,
        <<"unmodified">> => 1,
        <<"failed">> => 0,
        <<"createdMinHist">> => 2,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if File1 was deleted in and if File2 was updated
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS).

delete_file_in_dir_update_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    StorageTestDirPath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestFilePath = storage_import_test_base:provider_storage_path(
        ?SPACE_ID, filename:join(?TEST_DIR, ?TEST_FILE1)),
    StorageTestFilePath2 = storage_import_test_base:provider_storage_path(
        ?SPACE_ID, filename:join(?TEST_DIR, ?TEST_FILE2)),
    timer:sleep(timer:seconds(1)),

    %% Create dir on storage
    SDDirHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDDirHandle, ?DEFAULT_DIR_PERMS),

    %% Create file on storage
    SDFileHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDFileHandle, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, SDFileHandle, 0, ?TEST_DATA),

    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),
    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if file was imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),
    ?assertMatch({ok, [{_, ?TEST_FILE1}]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 10), ?ATTEMPTS),

    %% Create 2nd file on storage
    SDFileHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath2, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDFileHandle2, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, SDFileHandle2, 0, ?TEST_DATA2),

    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertSecondScanFinished(W1, ?SPACE_ID),
    storage_import_test_base:disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if file was imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle2),
    ?assertMatch({ok, [_, _]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 10), ?ATTEMPTS),

    % Delete file in the directory from storage
    ok = sd_test_utils:unlink(W1, SDFileHandle2, ?TEST_DATA_SIZE2),
    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertScanFinished(W1, ?SPACE_ID, 3),
    storage_import_test_base:disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if file was deleted from the space
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}), ?ATTEMPTS),
    ?assertMatch({ok, [{_, ?TEST_FILE1}]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 10), ?ATTEMPTS).


delete_many_subfiles_test(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    %% Create dirs and files on storage
    RootPath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_DIR),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, RootPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, SDHandle, 8#777),
    DirStructure = [10, 10, 10],
    storage_import_test_base:create_nested_directory_tree(W1, DirStructure, SDHandle),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),
    Files = storage_import_test_base:generate_nested_directory_tree_file_paths(DirStructure, ?SPACE_TEST_DIR_PATH),

    Timeout = 900,
    storage_import_test_base:parallel_assert(storage_import_test_base, verify_file, [W1, SessId, Timeout], Files, Timeout),
    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID, Timeout),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"created">> => 1000,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 1,
        <<"createdHourHist">> => 1000,
        <<"createdDayHist">> => 1000,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    ok = sd_test_utils:recursive_rm(W1, SDHandle),
    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertScanFinished(W1, ?SPACE_ID, 2, Timeout),
    storage_import_test_base:disable_continuous_scan(Config),
    ?assertEqual({ok, []}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 1)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"modified">> => 0,
        <<"deleted">> => 1111,
        <<"failed">> => 0,
        <<"unmodified">> => 1,
        <<"createdDayHist">> => 1000,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedHourHist">> => 1111,
        <<"deletedDayHist">> => 1111,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID).

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
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    % create 3 files
    FilesNum = 3,
    FilePaths = [?SPACE_TEST_FILE_PATH(?TEST_FILE(N)) || N <- lists:seq(1, FilesNum)],
    lists:foreach(fun(F) ->
        {ok, FileGuid} = lfm_proxy:create(W1, SessId, F),
        {ok, Handle} = lfm_proxy:open(W1, SessId, ?FILE_REF(FileGuid), write),
        {ok, _} = lfm_proxy:write(W1, Handle, 0, ?TEST_DATA),
        lfm_proxy:close(W1, Handle),
        FileGuid
    end, FilePaths),

    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),
    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID),

    TestPid = self(),
    ok = test_utils:mock_new(W1, storage_driver),
    ok = test_utils:mock_expect(W1, storage_driver, listobjects, fun(SDHandle, Marker, Offset, BatchSize) ->
        Result = meck:passthrough([SDHandle, Marker, Offset, BatchSize]),
        case SDHandle#sd_handle.file =:= <<"/">> of
            true ->
                % hold on storage import
                TestPid ! {waiting, self(), Offset, Result},
                receive continue -> ok end;
            false ->
                ok
        end,
        Result
    end),

    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),

    ListedFiles = [FileToDeleteOnStorage, FileToDeleteByLFM] = receive
        {waiting, Pid, 0, {ok, FilesAndStats}} ->
            % continue storage import
            Pid ! continue,
            [filename:basename(F) || {F, _} <- FilesAndStats]
    end,

    FileToDeleteByLFMPath = ?SPACE_TEST_FILE_PATH(FileToDeleteByLFM),
    FileToDeleteOnStoragePath = storage_import_test_base:provider_storage_path(?SPACE_ID, FileToDeleteOnStorage),


    receive
        {waiting, Pid2, 2, _} ->
            % delete 2 first files so that third will be placed in the first batch
            ok = lfm_proxy:unlink(W1, SessId, {path, FileToDeleteByLFMPath}),
            SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, FileToDeleteOnStoragePath, RDWRStorage),
            ok = sd_test_utils:unlink(W1, SDHandle, ?TEST_DATA_SIZE),
            % continue storage import
            Pid2 ! continue
    end,

    storage_import_test_base:assertSecondScanFinished(W1, ?SPACE_ID),
    storage_import_test_base:disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 0, % no deleted files because FileToDeleteOnStorage deletion will be detected in next scan
        <<"failed">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
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
    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertScanFinished(W1, ?SPACE_ID, 3, ?ATTEMPTS),
    storage_import_test_base:disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 3,
        <<"created">> => 0,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
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

change_file_type_test(Config) ->
    % this test checks whether storage import properly handles
    % deleting file and creating directory with the same name on storage
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),
    StorageTestFilePath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_FILE1),

    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFilePath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle, 0, ?TEST_DATA),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),
    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID),

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
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    ok = sd_test_utils:unlink(W1, SDHandle, ?TEST_DATA_SIZE),
    ok = sd_test_utils:mkdir(W1, SDHandle, ?DEFAULT_DIR_PERMS),
    % create file in the directory on storage
    StorageTestFileinDirPath = filename:join([StorageTestFilePath, ?TEST_FILE2]),
    SpaceTestFileinDirPath = filename:join([?SPACE_TEST_FILE_PATH1, ?TEST_FILE2]),
    SDHandle2 = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileinDirPath, RDWRStorage),
    ok = sd_test_utils:create_file(W1, SDHandle2, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, SDHandle2, 0, ?TEST_DATA2),

    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertSecondScanFinished(W1, ?SPACE_ID),
    storage_import_test_base:disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 1,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 2,
        <<"modifiedDayHist">> => 2,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    % check whether directory has been imported
    ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1})),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, ?FILE_REF(FileGuid))),
    {ok, #file_attr{guid = DirGuid}} = ?assertMatch({ok, #file_attr{type = ?DIRECTORY_TYPE}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W2, SessId2, ?FILE_REF(FileGuid)), ?ATTEMPTS),

    % check whether file inside directory has been imported
    {ok, #file_attr{guid = FileGuid2}} = ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
        lfm_proxy:stat(W2, SessId2, {path, SpaceTestFileinDirPath}), ?ATTEMPTS),
    ?assertMatch({ok, [{FileGuid2, ?TEST_FILE2}]},
        lfm_proxy:get_children(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, 0, 10), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {path, SpaceTestFileinDirPath}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA2}, lfm_proxy:read(W2, Handle2, 0, ?TEST_DATA_SIZE2), ?ATTEMPTS),

    % check whether we can create directory in the imported directory
    {ok, DirGuid2} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, ?ROOT_SESS_ID, DirGuid, ?TEST_DIR, ?DEFAULT_FILE_PERMS)),

    ?assertMatch({ok, _}, lfm_proxy:stat(W1, SessId, ?FILE_REF(DirGuid2)), ?ATTEMPTS).

change_file_type3_test(Config) ->
    % this test checks whether storage import properly handles
    % deleting non-empty directory and creating file with the same name on storage
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = storage_import_test_base:get_rdwr_storage(Config, W1),

    StorageTestDirPath = storage_import_test_base:provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestFileInDirPath = storage_import_test_base:provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),

    %% Create dir on storage and file inside it
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    DirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    FileSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileInDirPath, RDWRStorage),
    ok = sd_test_utils:mkdir(W1, DirSDHandle, ?DEFAULT_DIR_PERMS),
    ok = sd_test_utils:create_file(W1, FileSDHandle, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, FileSDHandle, 0, ?TEST_DATA),
    storage_import_test_base:enable_initial_scan(Config, ?SPACE_ID),
    storage_import_test_base:assertInitialScanFinished(W1, ?SPACE_ID),

    %% Check if dir was imported on W1
    {ok, #file_attr{guid = DirGuid}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    {ok, [{FileGuid, _}]} = ?assertMatch({ok, [{_, _}]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 1)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),


    ok = sd_test_utils:unlink(W1, FileSDHandle, ?TEST_DATA_SIZE),
    sd_test_utils:rmdir(W1, DirSDHandle),
    ok = sd_test_utils:create_file(W1, DirSDHandle, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, DirSDHandle, 0, ?TEST_DATA),

    storage_import_test_base:enable_continuous_scans(Config, ?SPACE_ID),
    storage_import_test_base:assertSecondScanFinished(W1, ?SPACE_ID),
    storage_import_test_base:disable_continuous_scan(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 2,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 2,
        <<"modifiedDayHist">> => 2,
        <<"deletedMinHist">> => 2,
        <<"deletedHourHist">> => 2,
        <<"deletedDayHist">> => 2,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, ?SPACE_ID),

    ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH})),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, ?FILE_REF(DirGuid))),
    ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W2, SessId2, ?FILE_REF(DirGuid)), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W2, SessId2, ?FILE_REF(FileGuid)), ?ATTEMPTS),

    % check whether we can read the imported file
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W1, Handle, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ok = lfm_proxy:close(W1, Handle),

    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ok = lfm_proxy:close(W2, Handle2).


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        ssl:start(),
        application:ensure_all_started(hackney),
        initializer:disable_quota_limit(NewConfig),
        initializer:mock_provider_ids(NewConfig),
        NewConfig2 = multi_provider_file_ops_test_base:init_env(NewConfig),
        [W1 | _] = ?config(op_worker_nodes, NewConfig2),
        rpc:call(W1, auto_storage_import_worker, notify_connection_to_oz, []),
        NewConfig2
    end,
    {ok, _} = application:ensure_all_started(worker_pool),
    {ok, _} = worker_pool:start_sup_pool(?VERIFY_POOL, [{workers, 8}]),
    [{?LOAD_MODULES, [initializer, storage_import_test_base, sd_test_utils, ?MODULE]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    ok = wpool:stop_sup_pool(?VERIFY_POOL),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    initializer:unmock_provider_ids(?config(op_worker_nodes, Config)),
    ssl:stop().

init_per_testcase(Case, Config) ->
    storage_import_test_base:init_per_testcase(Case, Config).

end_per_testcase(Case, Config) ->
    storage_import_test_base:end_per_testcase(Case, Config).