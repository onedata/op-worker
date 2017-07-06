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
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("kernel/include/file.hrl").
-include("storage_sync_test.hrl").

%% util functions
-export([disable_storage_sync/1, set_check_locally_enoent_strategy/2,
    set_check_globally_enoent_strategy/2, reset_enoent_strategies/2,
    add_workers_storage_mount_points/1, get_mount_point/2, clean_storage/2,
    get_host_mount_point/2, storage_test_file_path/4, create_init_file/1,
    enable_storage_import/1, enable_storage_update/1,
    delete_non_empty_directory_update_test/2
]).

%% tests
-export([
    create_directory_import_test/2, create_directory_export_test/2,
    create_file_import_test/2, create_file_export_test/2,
    delete_empty_directory_update_test/2, delete_directory_export_test/2,
    delete_file_update_test/2, delete_file_export_test/2,
    copy_file_update_test/2, append_file_update_test/2,
    move_file_update_test/2, truncate_file_update_test/2,
    chmod_file_update_test/2, update_timestamps_file_import_test/2,
    append_file_export_test/2, import_remote_file_by_path_test/2,
    import_file_by_path_test/2, get_child_attr_by_path_test/2,
    create_file_in_dir_import_test/2, create_file_in_dir_update_test/2,
    create_file_in_dir_exceed_batch_update_test/2, chmod_file_update2_test/2,
    should_not_detect_timestamp_update_test/2, create_directory_import_many_test/2,
    create_subfiles_import_many_test/2, verify_file_in_dir/5, verify_dir/5,
    create_directory_import_without_read_permission_test/2,
    create_subfiles_import_many2_test/2, verify_file/5,
    create_subfiles_and_delete_before_import_is_finished_test/2]).


-define(assertHashChangedFun(File, ExpectedResult0),
    fun
        ({_, {storage_sync_changes, children_attrs_hash_has_changed, Args}, ExpectedResult})
            when ExpectedResult =:= ExpectedResult0
            ->
            case hd(Args) of
                #file_meta{name = File} -> 1;
                _ -> 0
            end;
        (_) ->
            0
    end
).

-define(assertMtimeChangedFun(File, ExpectedResult0),
    fun
        ({_, {storage_sync_changes, mtime_has_changed, Args}, ExpectedResult})
            when ExpectedResult =:= ExpectedResult0
            ->
            case hd(Args) of
                #file_meta{name = File} -> 1;
                _ -> 0
            end;
        (_) ->
            0
    end
).



%%%===================================================================
%%% Test functions
%%%===================================================================

create_directory_import_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath =
        storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    %% Create dir on storage
    ok = file:make_dir(StorageTestDirPath),
    storage_sync_test_base:enable_storage_import(Config),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS).

create_directory_import_without_read_permission_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath =
        storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    %% Create dir on storage
    ok = file:make_dir(StorageTestDirPath),
    ok = file:change_mode(StorageTestDirPath, 8#000),
    storage_sync_test_base:enable_storage_import(Config),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), 2 * ?ATTEMPTS).

create_directory_import_many_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    DirsNumber = 200,

    %% Create dirs on storage
    utils:pforeach(fun(N) ->
        DirPath = storage_test_dir_path(W1MountPoint, ?SPACE_ID,
            integer_to_binary(N), MountSpaceInRoot),
        ok = file:make_dir(DirPath)
    end, lists:seq(1, DirsNumber)),

    storage_sync_test_base:enable_storage_import(Config),

    parallel_assert(?MODULE, verify_dir, [W1, SessId, ?ATTEMPTS], lists:seq(1, DirsNumber), ?ATTEMPTS).

create_file_import_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),

    StorageTestFilePath =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),

    %% Check if file was imported on W1
    {ok, Attr} =
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))).

%%todo fix
%%     Check if file was imported on W2
%%    ?assertMatch({ok, #file_attr{}},
%%        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}), 5 * ?ATTEMPTS),
%%    {ok, Handle2} = ?assertMatch({ok, _},
%%        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read)),
%%    ?assertMatch({ok, ?TEST_DATA},
%%        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA))).


create_subfiles_import_many_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    %% Create dirs and files on storage
    DirsNumber = 200,
    utils:pforeach(fun(N) ->
        NBin = integer_to_binary(N),
        DirPath = storage_test_dir_path(W1MountPoint, ?SPACE_ID, NBin, MountSpaceInRoot),
        FilePath = filename:join([DirPath, integer_to_binary(N)]),
        ok = file:make_dir(DirPath),
        ok = file:write_file(FilePath, ?TEST_DATA)
    end, lists:seq(1, DirsNumber)),
    storage_sync_test_base:enable_storage_import(Config),
    parallel_assert(?MODULE, verify_file_in_dir, [W1, SessId, 60], lists:seq(1, DirsNumber), 60).

create_subfiles_import_many2_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    %% Create dirs and files on storage
    RootPath = storage_test_dir_path(W1MountPoint, ?SPACE_ID, <<"">>, MountSpaceInRoot),
    DirStructure = [10, 10, 10],

    create_nested_directory_tree(DirStructure, RootPath),
    storage_sync_test_base:enable_storage_import(Config),
    Files = generate_nested_directory_tree_file_paths(DirStructure, ?SPACE_PATH),

    parallel_assert(?MODULE, verify_file, [W1, SessId, 60], Files, 60).


create_subfiles_and_delete_before_import_is_finished_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath =
        storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    %% Create dir on storage
    ok = file:make_dir(StorageTestDirPath),
    storage_sync_test_base:enable_storage_import(Config),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

    %% Create nested tree structure
    DirStructure = [10, 10, 10],
    create_nested_directory_tree(DirStructure, StorageTestDirPath),
    storage_sync_test_base:enable_storage_update(Config),
    timer:sleep(timer:seconds(?SCAN_INTERVAL)),
    recursive_rm(StorageTestDirPath),
    ?assertMatch({error, enoent},
        file:list_dir(StorageTestDirPath)),
    ?assertMatch({ok, []},
            lfm_proxy:ls(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, 0, 100),  2 * ?ATTEMPTS).

create_directory_export_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath =
        storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    % Create dir in space
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, ?SPACE_TEST_DIR_PATH)),
    % Create file in created dir in space as directory won't be created if there
    % is no file in it
    ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_IN_DIR_PATH2, 8#777)),
    % Check if dir was exported
    ?assert(filelib:is_dir(StorageTestDirPath)).

create_file_export_test(Config, MountInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountInRoot),
    % Create file in space
    {ok, FileGuid} =
        ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH, 8#777)),
    {ok, FileHandle} =
        ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, FileGuid}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W1, FileHandle, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:fsync(W1, FileHandle)),
    % Check if file was exported
    ?assert(filelib:is_regular(StorageTestFilePath)),
    ?assertEqual({ok, ?TEST_DATA}, file:read_file(StorageTestFilePath)).

create_file_in_dir_import_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestDirPath =
        storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFilePath =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, filename:join(?TEST_DIR, ?TEST_FILE2), MountSpaceInRoot),

    %% Create dir on storage
    ok = file:make_dir(StorageTestDirPath),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),

    %% Check if file was imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))).

%%    todo fix
%%    %% Check if file was imported on W2
%%    ?assertMatch({ok, #file_attr{}},
%%        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}), 5 * ?ATTEMPTS),
%%    {ok, Handle2} = ?assertMatch({ok, _},
%%        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH2}, read)),
%%    ?assertMatch({ok, ?TEST_DATA},
%%        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA))).

create_file_in_dir_update_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),

    StorageTestDirPath =
        storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 =
        storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    StorageTestFileinDirPath1 =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

    %% Create dirs on storage
    ok = file:make_dir(StorageTestDirPath),
    ok = file:make_dir(StorageTestDirPath2),

    storage_sync_test_base:enable_storage_import(Config),

    %% Check if dirs were imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH2}), ?ATTEMPTS),

    test_utils:mock_new(W1, storage_sync_changes, [passthrough]),

    ok = file:write_file(StorageTestFileinDirPath1, ?TEST_DATA),
    storage_sync_test_base:enable_storage_update(Config),

    %% Check if files were imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),

    History = rpc:call(W1, meck, history, [storage_sync_changes]),

    {ok, Handle5} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle5, 0, byte_size(?TEST_DATA))),

    test_utils:mock_unload(W1, storage_sync_changes),
    assert_num_results(History, ?assertHashChangedFun(?SPACE_ID, true), 0),
    assert_num_results(History, ?assertMtimeChangedFun(?TEST_DIR2, true), 0),
    assert_num_results_gte(History, ?assertMtimeChangedFun(?TEST_DIR, true), 1).

%%todo fix
%%    %% Check if file was imported on W2
%%    ?assertMatch({ok, #file_attr{}},
%%        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), 5 * ?ATTEMPTS),
%%    {ok, Handle2} = ?assertMatch({ok, _},
%%        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
%%    ?assertMatch({ok, ?TEST_DATA},
%%        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA))).


create_file_in_dir_exceed_batch_update_test(Config, MountSpaceInRoot) ->
    % in this test dir_batch_size is set in init_per_testcase to 2
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),

    StorageTestDirPath =
        storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 =
        storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),
    StorageTestFilePath1 =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    StorageTestFilePath2 =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE2, MountSpaceInRoot),
    StorageTestFilePath3 =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE3, MountSpaceInRoot),
    StorageTestFilePath4 =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE4, MountSpaceInRoot),
    StorageTestFileinDirPath1 =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

    %% Create dir on storage
    ok = file:make_dir(StorageTestDirPath),
    ok = file:make_dir(StorageTestDirPath2),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath1, ?TEST_DATA),
    ok = file:write_file(StorageTestFilePath2, ?TEST_DATA),
    ok = file:write_file(StorageTestFilePath3, ?TEST_DATA),
    ok = file:write_file(StorageTestFilePath4, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),

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

    test_utils:mock_new(W1, storage_sync_changes, [passthrough]),
    ok = file:write_file(StorageTestFileinDirPath1, ?TEST_DATA),
    storage_sync_test_base:enable_storage_update(Config),

    %% Check if files were imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),

    History = rpc:call(W1, meck, history, [storage_sync_changes]),

    {ok, Handle5} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle5, 0, byte_size(?TEST_DATA))),

    assert_num_results(History, ?assertHashChangedFun(?TEST_DIR2, true), 0),
    assert_num_results(History, ?assertMtimeChangedFun(?TEST_DIR2, true), 0),
    assert_num_results_gte(History, ?assertMtimeChangedFun(?TEST_DIR, true), 1).

%% todo fix
%%    %% Check if file was imported on W2
%%    ?assertMatch({ok, #file_attr{}},
%%        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), 5 * ?ATTEMPTS),
%%    {ok, Handle2} = ?assertMatch({ok, _},
%%        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
%%    ?assertMatch({ok, ?TEST_DATA},
%%        lfm_proxy:read(W2, Handle2, 0, byte_size(?TEST_DATA))),
%%    test_utils:mock_unload(W1, storage_sync_changes).


delete_empty_directory_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    %% Create dir on storage
    ok = file:make_dir(StorageTestDirPath),
    storage_sync_test_base:enable_storage_import(Config),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    %% Delete dir on storage
    storage_sync_test_base:enable_storage_update(Config),
    ok = file:del_dir(StorageTestDirPath),

    %% Check if dir was deleted in space

    ?assertMatch({error, enoent},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS).

delete_non_empty_directory_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestFileinDirPath1 =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),
    %% Create dir on storage
    ok = file:make_dir(StorageTestDirPath),
    ok = file:write_file(StorageTestFileinDirPath1, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:enable_storage_update(Config),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    {ok, Handle5} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle5, 0, byte_size(?TEST_DATA))),
    %% Delete dir on storage
    recursive_rm(StorageTestDirPath),
    %% Check if dir was deleted in space

    ?assertMatch({error, enoent},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS).

delete_directory_export_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    % Create dir in space
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, ?SPACE_TEST_DIR_PATH)),
    % Create file in created dir in space as directory won't be created if there
    % is no file in it
    {ok, _} = ?assertMatch({ok, _},
        lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_IN_DIR_PATH2, 8#777)),
    % Check if dir was exported
    ?assert(filelib:is_dir(StorageTestDirPath)),
    %%    ?assert(file:list_dir(StorageTestDirPath)),
    ok = lfm_proxy:rm_recursive(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}),
    ?assertMatch({error, enoent},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    %% Check if dir was deleted on storage
    ?assertMatch({error, enoent}, file:list_dir(StorageTestDirPath)).

delete_file_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:enable_storage_update(Config),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} =
        ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    %% Delete file on storage
    ok = file:delete(StorageTestFilePath),
    %% Check if file was deleted in space
    ?assertMatch({error, enoent},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS).

delete_file_export_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    % Create file in space
    {ok, FileGuid} = ?assertMatch({ok, _},
        lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH, 8#777)),
    {ok, FileHandle} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {guid, FileGuid}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W1, FileHandle, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:fsync(W1, FileHandle)),
    % Check if file was exported
    ?assert(filelib:is_regular(StorageTestFilePath)),
    ?assertEqual({ok, ?TEST_DATA}, file:read_file(StorageTestFilePath)),
    % Delete file in space
    lfm_proxy:close(W1, FileHandle),
    ok = lfm_proxy:unlink(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}),
    %% Check if file was deleted on storage
    ?assertMatch({error, enoent}, file:read_file(StorageTestFilePath), ?ATTEMPTS).

append_file_update_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:enable_storage_update(Config),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    %% Append to file
    append(StorageTestFilePath, ?TEST_DATA2),

    %% Check if appended bytes were imported on worker1
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    AppendedData = <<(?TEST_DATA)/binary, (?TEST_DATA2)/binary>>,
    ?assertMatch({ok, AppendedData},
        lfm_proxy:read(W1, Handle2, 0, byte_size(AppendedData))).

append_file_export_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    % Create file in space
    {ok, FileGuid} =
        ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH, 8#777)),
    {ok, FileHandle} =
        ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, FileGuid}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W1, FileHandle, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:fsync(W1, FileHandle)),
    % Check if file was exported
    ?assert(filelib:is_regular(StorageTestFilePath)),
    ?assertEqual({ok, ?TEST_DATA}, file:read_file(StorageTestFilePath)),
    % Append
    AppendedData = <<(?TEST_DATA)/binary, (?TEST_DATA2)/binary>>,
    ?assertEqual({ok, byte_size(?TEST_DATA2)},
        lfm_proxy:write(W1, FileHandle, byte_size(?TEST_DATA), ?TEST_DATA2)),
    ?assertEqual(ok, lfm_proxy:fsync(W1, FileHandle)),
    ?assertEqual({ok, AppendedData}, file:read_file(StorageTestFilePath)).

copy_file_update_test(Config, MountSpaceInRoot) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    StorageTestFilePath2 =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE2, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:enable_storage_update(Config),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    %% Copy file
    file:copy(StorageTestFilePath, StorageTestFilePath2),

    %% Check if appended bytes were imported
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH2}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA))),
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle3, 0, byte_size(?TEST_DATA))).

move_file_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    StorageTestFilePath2 =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE2, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:enable_storage_update(Config),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    %% Move file
    %%    tracer:trace_calls(storage_update),
    ok = file:rename(StorageTestFilePath, StorageTestFilePath2),
    %% Check if file was moved
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH2}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA))),
    ?assertMatch({error, enoent},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH})).

truncate_file_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:enable_storage_update(Config),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    %% Truncate file
    truncate(StorageTestFilePath, 1),
    %% Check if file was truncated
    ?assertMatch({ok, #file_attr{size = 1}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS).

chmod_file_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),

    StorageTestDirPath =
        storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),

    StorageTestFileinDirPath1 =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1]), MountSpaceInRoot),

    %%    StorageTestFilePath =
    %%        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    NewMode = 8#600,

    %% Create dirs on storage
    ok = file:make_dir(StorageTestDirPath),
    %% Create file on storage
    ok = file:write_file(StorageTestFileinDirPath1, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:enable_storage_update(Config),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{mode = 8#644}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),

    test_utils:mock_new(W1, storage_sync_changes, [passthrough]),

    %% Change file permissions
    file:change_mode(StorageTestFileinDirPath1, NewMode),
    %% Check if file permissions were changed
    ?assertMatch({ok, #file_attr{mode = NewMode}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    History = rpc:call(W1, meck, history, [storage_sync_changes]),

    test_utils:mock_unload(W1, storage_sync_changes),
    assert_num_results_gte(History, ?assertHashChangedFun(?TEST_DIR, true), 1).

chmod_file_update2_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),

    Files = lists:map(fun(TestFile) ->
        filename:join([?TEST_DIR, TestFile])
    end, [?TEST_FILE1, ?TEST_FILE2, ?TEST_FILE3]),

    StorageTestDirPath =
        storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    StorageTestDirPath2 =
        storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR2, MountSpaceInRoot),

    [StTestFile1 | _] = StorageFiles =
        to_storage_files(Files, W1MountPoint, ?SPACE_ID, MountSpaceInRoot),

    NewMode = 8#600,
    %% Create files on storage
    ok = file:make_dir(StorageTestDirPath),
    ok = file:make_dir(StorageTestDirPath2),
    lists:foreach(fun(StorageTestFilePath) ->
        ok = file:write_file(StorageTestFilePath, ?TEST_DATA)
    end, StorageFiles),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:enable_storage_update(Config),

    %% Check if files were imported
    lists:foreach(fun(SpaceFile) ->
        ?assertMatch({ok, #file_attr{mode = 8#644}},
            lfm_proxy:stat(W1, SessId, {path, SpaceFile}), ?ATTEMPTS),
        {ok, Handle1} = ?assertMatch({ok, _},
            lfm_proxy:open(W1, SessId, {path, SpaceFile}, read)),
        ?assertMatch({ok, ?TEST_DATA},
            lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA)))
    end, [?SPACE_TEST_FILE_IN_DIR_PATH, ?SPACE_TEST_FILE_IN_DIR_PATH2, ?SPACE_TEST_FILE_IN_DIR_PATH3]),

    test_utils:mock_new(W1, storage_sync_changes, [passthrough]),

    %% Change file permissions
    ok = file:change_mode(StTestFile1, NewMode),
    %% Check if file permissions were changed
    ?assertMatch({ok, #file_attr{mode = NewMode}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_IN_DIR_PATH}), ?ATTEMPTS),
    History = rpc:call(W1, meck, history, [storage_sync_changes]),
    test_utils:mock_unload(W1, storage_sync_changes),

    assert_num_results_gte(History, ?assertHashChangedFun(?TEST_DIR, true), 1),
    assert_num_results(History, ?assertMtimeChangedFun(?TEST_DIR, true), 0).

update_timestamps_file_import_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:enable_storage_update(Config),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    %% Change file permissions
    change_time(StorageTestFilePath, 1, 1),
    %% Check if timestamps were changed
    ?assertMatch({ok, #file_attr{atime = 1, mtime = 1}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS).

should_not_detect_timestamp_update_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:enable_storage_update(Config),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    %% Change file permissions
    timer:sleep(timer:seconds(?SCAN_INTERVAL)), % to be sure that it'll be imported before changing timestamps
    change_time(StorageTestFilePath, 1, 1),
    %% Check if timestamps hasn't changed
    ?assertNotMatch({ok, #file_attr{atime = 1, mtime = 1}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH})).


import_file_by_path_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:enable_storage_update(Config),
    ?assertMatch({error, enoent},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH})),
    set_check_locally_enoent_strategy(W1, ?SPACE_ID),
    %% Check if file will be imported on demand
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))).

get_child_attr_by_path_test(Config, MountSpaceInRoot) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath =
        storage_test_dir_path(W1MountPoint, ?SPACE_ID, ?TEST_DIR, MountSpaceInRoot),
    %% Create dir on storage
    ok = file:make_dir(StorageTestDirPath),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:enable_storage_update(Config),
    %% Check if dir was imported
    {ok, #file_attr{guid = Guid}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),

    %% Create file on storage
    StorageTestFilePath = <<StorageTestDirPath/binary, "/test_file">>,
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    set_check_locally_enoent_strategy(W1, ?SPACE_ID),
    ?assertMatch({ok, #file_attr{}},
        rpc:call(W1, logical_file_manager, get_child_attr, [SessId, Guid, <<"test_file">>]), ?ATTEMPTS).

import_remote_file_by_path_test(Config, MountSpaceInRoot) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),

    W1MountPoint = get_host_mount_point(W1, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageTestFilePath =
        storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    %% Create file on storage
    set_check_globally_enoent_strategy(W1, ?SPACE_ID),
    set_check_globally_enoent_strategy(W2, ?SPACE_ID),
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:enable_storage_update(Config),
    {ok, Handle} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W2, Handle, 0, byte_size(?TEST_DATA))).

%%%===================================================================
%%% Util functions
%%%===================================================================

create_init_file(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    {ok, _} = lfm_proxy:create(W1, SessId, ?SPACE_INIT_FILE_PATH, 8#777).

enable_storage_import(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, [#document{key = StorageId} | _]} = rpc:call(W1, storage, list, []),
    {ok, _} = rpc:call(W1, storage_sync, start_simple_scan_import,
        [?SPACE_ID, StorageId, ?MAX_DEPTH]).

enable_storage_update(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),

    UpdateConfig = ?config(update_config, Config, #{}),
    ScanInterval = maps:get(scan_interval, UpdateConfig, ?SCAN_INTERVAL),
    WriteOnce = maps:get(write_once, UpdateConfig, ?WRITE_ONCE),
    DeleteEnable = maps:get(delete_enable, UpdateConfig, ?DELETE_ENABLE),

    {ok, [#document{key = StorageId} | _]} = rpc:call(W1, storage, list, []),
    {ok, _} = rpc:call(W1, storage_sync, start_simple_scan_update,
        [?SPACE_ID, StorageId, ?MAX_DEPTH, ScanInterval, WriteOnce, DeleteEnable]).


disable_storage_import(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    rpc:call(W1, storage_sync, stop_storage_import, [?SPACE_ID]).

disable_storage_update(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    rpc:call(W1, storage_sync, stop_storage_update, [?SPACE_ID]).

disable_storage_sync(Config) ->
    {ok, _} = disable_storage_import(Config),
    {ok, _} = disable_storage_update(Config).

clean_storage(Config, Readonly) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(W) ->
        SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W)}}, Config),
        DockerMountPoint = get_docker_mount_point(W, Config),
        HostMountPoint = get_host_mount_point(W, Config),
        lfm_proxy:unlink(W, SessId, {path, ?SPACE_INIT_FILE_PATH}),
        case Readonly of
            true ->
                rpc:multicall(Workers, os, cmd, ["rm -r " ++ binary_to_list(DockerMountPoint) ++ "/*"]),
                os:cmd("rm -r " ++ binary_to_list(HostMountPoint) ++ "/*");
            false ->
                rpc:multicall(Workers, os, cmd, ["rm -r " ++ binary_to_list(DockerMountPoint) ++ "/" ++ binary_to_list(?SPACE_ID) ++ "/*"]),
                os:cmd("rm -r " ++ binary_to_list(HostMountPoint) ++ "/" ++ binary_to_list(?SPACE_ID))
        end
    end, Workers),
    timer:sleep(timer:seconds(3))
.

set_check_locally_enoent_strategy(Worker, SpaceId) ->
    {ok, _} = rpc:call(Worker, storage_sync, set_check_locally_enoent_strategy, [SpaceId]).

set_check_globally_enoent_strategy(Worker, SpaceId) ->
    {ok, _} = rpc:call(Worker, storage_sync, set_check_globally_enoent_strategy, [SpaceId]).

reset_enoent_strategies(Workers, SpaceId) when is_list(Workers) ->
    rpc:multicall(Workers, storage_sync, set_error_passthrough_enoent_strategy, [SpaceId]).

add_workers_storage_mount_points(Config) ->
    Workers = ?config(op_worker_nodes, Config),

    MountPoints = lists:foldl(fun(W, AccIn) ->
        {ok, [WorkerStorage | _]} = rpc:call(W, storage, list, []),
        #document{value = #storage{helpers = [Helpers]}} = WorkerStorage,
        #{<<"mountPoint">> := DockerMountPath} = helper:get_args(Helpers),
        HostMountPath = get_storage_path(Config, binary_to_atom(DockerMountPath, latin1)),

        AccIn#{
            W => #{
                docker_path => DockerMountPath,
                host_path => HostMountPath
            }}
    end, #{}, Workers),

    [{mount_points, MountPoints} | Config].

get_host_mount_point(Worker, Config) ->
    maps:get(host_path, get_mount_point(Worker, Config)).

get_docker_mount_point(Worker, Config) ->
    maps:get(docker_path, get_mount_point(Worker, Config)).

get_mount_point(Worker, Config) ->
    MountPoints = ?config(mount_points, Config),
    maps:get(Worker, MountPoints).


get_storage_path(Config, MountPath) when is_list(MountPath) ->
    get_storage_path(Config, list_to_atom(MountPath));
get_storage_path(Config, MountPath) when is_binary(MountPath) ->
    get_storage_path(Config, binary_to_atom(MountPath, latin1));
get_storage_path(Config, MountPath) when is_atom(MountPath) ->
    atom_to_binary(?config(host_path,
        ?config(MountPath,
            ?config(posix,
                ?config(storages, Config)))), latin1).

storage_test_dir_path(MountPath, _SpaceId, Dir, true) ->
    filename:join([MountPath, Dir]);
storage_test_dir_path(MountPath, SpaceId, Dir, false) ->
    filename:join([MountPath, SpaceId, Dir]).

storage_test_file_path(MountPath, _SpaceId, File, true) ->
    filename:join([MountPath, File]);
storage_test_file_path(MountPath, SpaceId, FileName, false) ->
    filename:join([MountPath, SpaceId, FileName]).

append(FilePath, Bytes) ->
    {ok, IoDevice} = file:open(FilePath, [append]),
    ok = file:write(IoDevice, Bytes).

recursive_rm(DirPath) ->
    [] = os:cmd("rm -rf " ++ str_utils:to_list(DirPath)).

truncate(FilePath, NewSize) ->
    {ok, IoDevice} = file:open(FilePath, [read, write]),
    {ok, NewSize} = file:position(IoDevice, NewSize),
    ok = file:truncate(IoDevice).

change_time(FilePath, Atime, Mtime) ->
    file:write_file_info(FilePath,
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

to_storage_files(Files, MountPoint, SpaceId, MountSpaceInRoot) ->
    [storage_test_file_path(MountPoint, SpaceId, F, MountSpaceInRoot) || F <- Files].

parallel_assert(M, F, A, List, Attempts) ->
    lists:foreach(fun(N) ->
        spawn_link(M, F, [N, self() | A])
    end, List),

    lists:foldl(fun(_, AccIn) ->
        case sets:size(AccIn) of
            0  -> ok;
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
        lfm_proxy:open(W1, SessId, {path, FilePath}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    Pid ! {finished, FilePath}.

verify_file_in_dir(N, Pid, W1, SessId, Attempts) ->
    NBin = integer_to_binary(N),
    FileInDirPath = ?SPACE_TEST_FILE_IN_DIR_PATH(NBin, NBin),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, FileInDirPath}), Attempts),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, FileInDirPath}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    Pid ! {finished, NBin}.


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


create_nested_directory_tree([SubFilesNum], Root) ->
    ok = lists:foreach(fun(N) ->
        FilePath = filename:join([Root, integer_to_binary(N)]),
        ok = file:write_file(FilePath, ?TEST_DATA)
    end, lists:seq(1, SubFilesNum));
create_nested_directory_tree([SubDirsNum | Rest], Root) ->
    ok = utils:pforeach(fun(N) ->
        NBin = integer_to_binary(N),
        DirPath = filename:join([Root, NBin]),
        ok = file:make_dir(DirPath),
        ok = create_nested_directory_tree(Rest, DirPath)
    end, lists:seq(1, SubDirsNum)).