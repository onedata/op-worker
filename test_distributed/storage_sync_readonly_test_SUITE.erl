%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests file deletion.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_sync_readonly_test_SUITE).
-author("Rafal Slota").
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("kernel/include/file.hrl").


%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    create_directory_import_test/1,
    create_file_import_test/1,
    delete_directory_import_test/1,
    delete_file_import_test/1,
    append_file_import_test/1,
    copy_file_import_test/1, move_file_import_test/1,
    truncate_file_import_test/1, chmod_file_import_test/1,
    update_timestamps_file_import_test/1]).

-define(TEST_CASES, [
    create_directory_import_test,
    create_file_import_test,
    delete_directory_import_test,
    delete_file_import_test,
    append_file_import_test,
    copy_file_import_test,
    move_file_import_test,
    truncate_file_import_test,
    chmod_file_import_test,
    update_timestamps_file_import_test
]).

all() -> ?ALL(?TEST_CASES).

-define(ATTEMPTS, 15).

%% test data
-define(USER, <<"user1">>).
-define(SPACE_ID, <<"space1">>).
-define(SPACE_NAME, <<"space_name1">>).
-define(TEST_DIR, <<"test_dir">>).
-define(TEST_FILE, <<"test_file">>).
-define(TEST_FILE2, <<"test_file2">>).
-define(INIT_FILE, <<"___init_file">>).
-define(STORAGE_TEST_DIR_PATH(MountPath), filename:join([MountPath, ?TEST_DIR])).
-define(STORAGE_TEST_FILE_PATH(MountPath), filename:join([MountPath, ?TEST_FILE])).
-define(STORAGE_TEST_FILE_PATH2(MountPath), filename:join([MountPath, ?TEST_FILE2])).
-define(STORAGE_TEST_FILE_IN_DIR_PATH(MountPath),
    filename:join([?STORAGE_TEST_DIR_PATH(MountPath), ?TEST_FILE2])).
-define(STORAGE_INIT_FILE_PATH(MountPath), filename:join([MountPath, ?INIT_FILE])).
-define(SPACE_TEST_DIR_PATH, filename:join(["/", ?SPACE_NAME, ?TEST_DIR])).
-define(SPACE_TEST_FILE_PATH, filename:join(["/", ?SPACE_NAME, ?TEST_FILE])).
-define(SPACE_TEST_FILE_PATH2, filename:join(["/", ?SPACE_NAME, ?TEST_FILE2])).
-define(SPACE_TEST_FILE_IN_DIR_PATH, filename:join([?SPACE_TEST_DIR_PATH, ?TEST_FILE2])).
-define(SPACE_INIT_FILE_PATH, filename:join(["/", ?SPACE_NAME, ?INIT_FILE])).

-define(W1_STORAGE(Config),
    atom_to_binary(?config(host_path, ?config('/mnt/st2', ?config(posix, ?config(storages, Config)))), latin1)).

-define(TEST_DATA, <<"test_data">>).
-define(TEST_DATA2, <<"test_data2">>).

%%%==================================================================
%%% Test functions
%%%===================================================================

create_directory_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = ?STORAGE_TEST_DIR_PATH(W1MountPoint),
    %% Create dir on storage
    ok = file:make_dir(StorageTestDirPath),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    tracer:stop().

create_file_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))).

delete_directory_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = ?STORAGE_TEST_DIR_PATH(W1MountPoint),
    %% Create dir on storage
    ok = file:make_dir(StorageTestDirPath),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    %% Delete dir on storage
    ok = file:del_dir(StorageTestDirPath),
    %% Check if dir was deleted in space
    ?assertMatch({error, enoent}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS).

delete_file_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
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
    ?assertMatch({error, enoent}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS).

append_file_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    %% Append to file
    append(StorageTestFilePath, ?TEST_DATA2),
    %% Check if appended bytes were imported
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    AppendedData = <<(?TEST_DATA)/binary, (?TEST_DATA2)/binary>>,
    ?assertMatch({ok, AppendedData},
        lfm_proxy:read(W1, Handle2, 0, byte_size(AppendedData))).

copy_file_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    StorageTestFilePath2 = ?STORAGE_TEST_FILE_PATH2(W1MountPoint),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
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

move_file_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    StorageTestFilePath2 = ?STORAGE_TEST_FILE_PATH2(W1MountPoint),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    %% Move file
    file:rename(StorageTestFilePath, StorageTestFilePath2),
    %% Check if appended bytes were imported
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH2}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA))),
    ?assertMatch({error, enoent},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)).

truncate_file_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
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

chmod_file_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    NewMode = 8#600,
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    ?assertMatch({ok, #file_attr{mode = 8#666}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    %% Change file permissions
    file:change_mode(StorageTestFilePath, NewMode),
    %% Check if file permissions were changed
    ?assertMatch({ok, #file_attr{mode = NewMode}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS).

update_timestamps_file_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    ?assertMatch({ok, #file_attr{mode = 8#666}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    %% Change file permissions
    change_time(StorageTestFilePath, 1, 1),
    %% Check if timestamps were changed
    ?assertMatch({ok, #file_attr{atime = 1, mtime = 1}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer]} | Config].

init_per_testcase(_Case, Config) ->
    application:start(etls),
    hackney:start(),
    initializer:disable_quota_limit(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    initializer:enable_grpca_based_communication(Config),
    ConfigWithProxy = lfm_proxy:init(ConfigWithSessionInfo),
    enable_storage_sync(ConfigWithProxy).

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    rpc:multicall(Workers, os, cmd, ["rm -rf " ++ binary_to_list(W1MountPoint) ++ "/*"]),
    os:cmd("rm -rf " ++ binary_to_list(W1MountPoint) ++ "/*"),
    disable_storage_sync(Config),
    lfm_proxy:teardown(Config),
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    initializer:disable_grpca_based_communication(Config),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

enable_storage_sync(Config) ->
    case ?config(w1_mount_point, Config) of
        undefined ->
            [W1, _] = ?config(op_worker_nodes, Config),
            %% Enable import
            {ok, _ } = rpc:call(W1, storage_sync, start_storage_import, [?SPACE_ID, 10]),
            W1MountPoint = ?W1_STORAGE(Config),
            [{w1_mount_point, W1MountPoint} | Config];
        _ ->
            Config
    end.

disable_storage_sync(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    %% Disable import
    {ok, _ } = rpc:call(W1, storage_sync, stop_storage_import, [?SPACE_ID]).

append(FilePath, Bytes) ->
    {ok, IoDevice} = file:open(FilePath, [append]),
    ok = file:write(IoDevice, Bytes).

truncate(FilePath, NewSize) ->
    {ok, IoDevice} = file:open(FilePath, [read, write]),
    {ok, NewSize} =  file:position(IoDevice, NewSize),
    ok = file:truncate(IoDevice).

change_time(FilePath, Atime, Mtime) ->
    file:write_file_info(FilePath,
        #file_info{atime=Atime, mtime=Mtime}, [{time, posix}]).