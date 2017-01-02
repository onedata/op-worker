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
-module(storage_sync_test_SUITE).
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
    create_directory_import_test/1, create_directory_export_test/1,
    create_file_import_test/1, create_file_export_test/1,
    delete_directory_import_test/1, delete_directory_export_test/1,
    delete_file_import_test/1, delete_file_export_test/1,
    append_file_import_test/1, append_file_export_test/1,
    copy_file_import_test/1, move_file_import_test/1,
    truncate_file_import_test/1, chmod_file_import_test/1,
    update_timestamps_file_import_test/1]).

-define(TEST_CASES, [
    create_directory_import_test,
    create_directory_export_test,
    create_file_import_test,
    create_file_export_test,
    delete_directory_import_test,
    delete_directory_export_test,
    delete_file_import_test,
    delete_file_export_test,
    append_file_import_test,
    append_file_export_test,
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
-define(STORAGE_TEST_DIR_PATH(MountPath), filename:join([MountPath, ?SPACE_ID, ?TEST_DIR])).
-define(STORAGE_TEST_FILE_PATH(MountPath), filename:join([MountPath, ?SPACE_ID, ?TEST_FILE])).
-define(STORAGE_TEST_FILE_PATH2(MountPath), filename:join([MountPath, ?SPACE_ID, ?TEST_FILE2])).
-define(STORAGE_TEST_FILE_IN_DIR_PATH(MountPath),
    filename:join([?STORAGE_TEST_DIR_PATH(MountPath), ?TEST_FILE2])).
-define(STORAGE_INIT_FILE_PATH(MountPath), filename:join([MountPath, ?SPACE_ID, ?INIT_FILE])).
-define(SPACE_TEST_DIR_PATH, filename:join(["/", ?SPACE_NAME, ?TEST_DIR])).
-define(SPACE_TEST_FILE_PATH, filename:join(["/", ?SPACE_NAME, ?TEST_FILE])).
-define(SPACE_TEST_FILE_PATH2, filename:join(["/", ?SPACE_NAME, ?TEST_FILE2])).
-define(SPACE_TEST_FILE_IN_DIR_PATH, filename:join([?SPACE_TEST_DIR_PATH, ?TEST_FILE2])).
-define(SPACE_INIT_FILE_PATH, filename:join(["/", ?SPACE_NAME, ?INIT_FILE])).

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
    ok = mkdir(W1, StorageTestDirPath),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS).

create_directory_export_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = ?STORAGE_TEST_DIR_PATH(W1MountPoint),
    % Create dir in space
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, ?SPACE_TEST_DIR_PATH)),
    % Create file in created dir in space as directory won't be created if there
    % is no file in it
    ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_IN_DIR_PATH, 8#777)),
    % Check if dir and file were exported
    ?assert(is_dir(W1, StorageTestDirPath)).

create_file_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    %% Create file on storage
    ok = write_file(W1, StorageTestFilePath, ?TEST_DATA),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, rdwr)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))).

create_file_export_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    % Create file in space
    {ok, FileGuid} =
        ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH, 8#777)),
    {ok, FileHandle} =
        ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, FileGuid}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W1, FileHandle, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:fsync(W1, FileHandle)),
    % Check if file was exported
    ?assert(is_regular(W1, StorageTestFilePath)),
    ?assertEqual({ok, ?TEST_DATA}, read_file(W1, StorageTestFilePath)).

delete_directory_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = ?STORAGE_TEST_DIR_PATH(W1MountPoint),
    %% Create dir on storage
    ok = mkdir(W1, StorageTestDirPath),
    %% Check if dir was imported
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    %% Delete dir on storage
    ok = rmdir(W1, StorageTestDirPath),
    %% Check if dir was deleted in space
    ?assertMatch({error, enoent}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS).

delete_directory_export_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestDirPath = ?STORAGE_TEST_DIR_PATH(W1MountPoint),
    % Create dir in space
    ?assertMatch({ok, _}, lfm_proxy:mkdir(W1, SessId, ?SPACE_TEST_DIR_PATH)),
    % Create file in created dir in space as directory won't be created if there
    % is no file in it
    {ok, _} = ?assertMatch({ok, _},
        lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_IN_DIR_PATH, 8#777)),
    % Check if dir was exported
    ?assert(is_dir(W1, StorageTestDirPath)),
    %% Delete dir in space
    ok = lfm_proxy:rm_recursive(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}),
    %% Check if dir was deleted on storage
    ?assertMatch({error, enoent}, ls(W1, StorageTestDirPath)).

delete_file_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    %% Create file on storage
    ok = write_file(W1, StorageTestFilePath, ?TEST_DATA),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} =
        ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, rdwr)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    %% Delete file on storage
    ok = rm(W1, StorageTestFilePath),
    %% Check if file was deleted in space
    ?assertMatch({error, enoent}, lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS).

delete_file_export_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    % Create file in space
    {ok, FileGuid} = ?assertMatch({ok, _},
        lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH, 8#777)),
    {ok, FileHandle} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {guid, FileGuid}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W1, FileHandle, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:fsync(W1, FileHandle)),
    % Check if file was exported
    ?assert(is_regular(W1, StorageTestFilePath)),
    ?assertEqual({ok, ?TEST_DATA}, read_file(W1, StorageTestFilePath)),
    % Delete file in space
    ok = lfm_proxy:unlink(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}),
    %% Check if file was deleted on storage
    ?assertMatch({error, enoent}, read_file(W1, StorageTestFilePath)).

append_file_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    %% Create file on storage
    ok = write_file(W1, StorageTestFilePath, ?TEST_DATA),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, rdwr)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    %% Append to file
    append(W1, StorageTestFilePath, ?TEST_DATA2),
    %% Check if appended bytes were imported
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, rdwr)),
    AppendedData = <<(?TEST_DATA)/binary, (?TEST_DATA2)/binary>>,
    ?assertMatch({ok, AppendedData},
        lfm_proxy:read(W1, Handle2, 0, byte_size(AppendedData))).


append_file_export_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    % Create file in space
    {ok, FileGuid} =
        ?assertMatch({ok, _}, lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH, 8#777)),
    {ok, FileHandle} =
        ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, FileGuid}, write)),
    ?assertEqual({ok, byte_size(?TEST_DATA)}, lfm_proxy:write(W1, FileHandle, 0, ?TEST_DATA)),
    ?assertEqual(ok, lfm_proxy:fsync(W1, FileHandle)),
    % Check if file was exported
    ?assert(is_regular(W1, StorageTestFilePath)),
    ?assertEqual({ok, ?TEST_DATA}, read_file(W1, StorageTestFilePath)),
    % Append
    AppendedData = <<(?TEST_DATA)/binary, (?TEST_DATA2)/binary>>,
    ?assertEqual({ok, byte_size(?TEST_DATA2)},
        lfm_proxy:write(W1, FileHandle, byte_size(?TEST_DATA), ?TEST_DATA2)),
    ?assertEqual(ok, lfm_proxy:fsync(W1, FileHandle)),
    ?assertEqual({ok, AppendedData}, read_file(W1, StorageTestFilePath)).

copy_file_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    StorageTestFilePath2 = ?STORAGE_TEST_FILE_PATH2(W1MountPoint),
    %% Create file on storage
    ok = write_file(W1, StorageTestFilePath, ?TEST_DATA),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
    lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
    lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, rdwr)),
    ?assertMatch({ok, ?TEST_DATA},
    lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    %% Copy file
    copy(W1, StorageTestFilePath, StorageTestFilePath2),
    %% Check if appended bytes were imported
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH2}, rdwr), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA))),
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, rdwr)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle3, 0, byte_size(?TEST_DATA))).


move_file_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    StorageTestFilePath2 = ?STORAGE_TEST_FILE_PATH2(W1MountPoint),
    %% Create file on storage
    ok = write_file(W1, StorageTestFilePath, ?TEST_DATA),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, rdwr)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    %% Move file
    move(W1, StorageTestFilePath, StorageTestFilePath2),
    %% Check if appended bytes were imported
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH2}, rdwr), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA))),
    ?assertMatch({error, enoent},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, rdwr)).

truncate_file_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    %% Create file on storage
    ok = write_file(W1, StorageTestFilePath, ?TEST_DATA),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, rdwr)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    %% Truncate file
    truncate(W1, StorageTestFilePath, 1),
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
    ok = write_file(W1, StorageTestFilePath, ?TEST_DATA),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, rdwr)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    ?assertMatch({ok, #file_attr{mode = 8#666}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    %% Change file permissions
    chmod(W1, StorageTestFilePath, NewMode),
    %% Check if file permissions were changed
    ?assertMatch({ok, #file_attr{mode = NewMode}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS).


update_timestamps_file_import_test(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath = ?STORAGE_TEST_FILE_PATH(W1MountPoint),
    %% Create file on storage
    ok = write_file(W1, StorageTestFilePath, ?TEST_DATA),
    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}, rdwr)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    ?assertMatch({ok, #file_attr{mode = 8#666}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS),
    %% Change file permissions
    change_time(W1, StorageTestFilePath, 1, 1),
    %% Check if timestamps were changed
    ?assertMatch({ok, #file_attr{atime = 1, mtime = 1}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH}), ?ATTEMPTS).


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer]} | Config].

init_per_testcase(Case, Config) ->
    application:start(etls),
    hackney:start(),
    initializer:disable_quota_limit(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    initializer:enable_grpca_based_communication(Config),
    ConfigWithProxy = lfm_proxy:init(ConfigWithSessionInfo),
    enable_storage_sync(ConfigWithProxy).

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    clean_storage(Config),
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
            SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
            {ok, _} = lfm_proxy:create(W1, SessId, ?SPACE_INIT_FILE_PATH, 8#777),
            {ok, [W1Storage | _]} = rpc:call(W1, storage, list, []),

            %% Enable import
            #document{key = W1StorageId, value = #storage{helpers = [W1Helpers]}} = W1Storage,
            {ok, _} = rpc:call(W1, space_strategies, set_strategy, [
                ?SPACE_ID, W1StorageId, storage_import, bfs_scan, #{scan_interval => 10}]),
            #helper_init{args = #{<<"root_path">> := W1MountPoint}} = W1Helpers,
            [{w1_mount_point, W1MountPoint} | Config];
        _ ->
            Config
    end.

clean_storage(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    W1MountPoint = ?config(w1_mount_point, Config),
    ok = rpc:call(W1, file, delete, [?STORAGE_INIT_FILE_PATH(W1MountPoint)]),
    rpc:call(W1, file, delete, [?STORAGE_TEST_FILE_PATH(W1MountPoint)]),
    rpc:call(W1, file, delete, [?STORAGE_TEST_FILE_IN_DIR_PATH(W1MountPoint)]),
    rpc:call(W1, file, del_dir, [?STORAGE_TEST_DIR_PATH(W1MountPoint)]).

mkdir(Worker, DirPath) ->
    rpc:call(Worker, file, make_dir, [DirPath]).

rmdir(Worker, DirPath) ->
    rpc:call(Worker, file, del_dir, [DirPath]).

rm(Worker, FilePath) ->
    rpc:call(Worker, file, delete, [FilePath]).

ls(Worker, DirPath) ->
    rpc:call(Worker, file, list_dir, [DirPath]).

is_dir(Worker, DirPath) ->
    rpc:call(Worker, filelib, is_dir, [DirPath]).

is_regular(Worker, FilePath) ->
    rpc:call(Worker, filelib, is_regular, [FilePath]).

write_file(Worker, FilePath, Data) ->
    rpc:call(Worker, file, write_file, [FilePath, Data]).

read_file(Worker, FilePath) ->
    rpc:call(Worker, file, read_file, [FilePath]).

append(Worker, FilePath, Bytes) ->
    Append = fun() ->
        {ok, IoDevice} = file:open(FilePath, [append]),
        ok = file:write(IoDevice, Bytes)
    end,
    ?assertMatch(ok, rpc:call(Worker, erlang, apply, [Append, []] )).

copy(Worker, SourcePath, DestinationPath) ->
    rpc:call(Worker, file, copy, [SourcePath, DestinationPath]).

move(Worker, SourcePath, DestinationPath) ->
    rpc:call(Worker, file, rename, [SourcePath, DestinationPath]).

truncate(Worker, FilePath, NewSize) ->
    Truncate = fun() ->
        {ok, IoDevice} = file:open(FilePath, [read, write]),
        {ok, NewSize} =  file:position(IoDevice, NewSize),
        ok = file:truncate(IoDevice)
    end,
    ?assertMatch(ok, rpc:call(Worker, erlang, apply, [Truncate, []])).

chmod(Worker, FilePath, NewMode) ->
    rpc:call(Worker, file, change_mode, [FilePath, NewMode]).

change_time(Worker, FilePath, Atime, Mtime) ->
    rpc:call(Worker, file, write_file_info, [FilePath,
        #file_info{atime=Atime, mtime=Mtime}, [{time, posix}]]).