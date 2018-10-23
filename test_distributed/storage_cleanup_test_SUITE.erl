%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of cleaning up files from storage.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_cleanup_test_SUITE).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").
-include_lib("kernel/include/file.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    file_should_be_deleted_from_storage_after_deletion/1,
    file_should_be_truncated_on_storage_after_truncate/1,
    directory_should_be_deleted_from_storage_after_deletion/1,
    empty_directory_should_be_deleted_from_storage_after_deletion/1,
    file_should_be_deleted_from_storage_after_releasing_handle/1,
    remote_replica_should_be_deleted_from_storage_after_deletion/1,
    remote_directory_replica_should_be_deleted_from_storage_after_deletion/1,
    remote_replica_should_be_truncated_on_storage_after_truncate/1,
    replica_should_be_deleted_from_storage_after_releasing_handle_to_remotely_deleted_file/1,
    empty_remote_directory_replica_should_be_deleted_from_storage_after_deletion/1
]).

-define(ATTEMPTS, 60).

-define(TEST_DATA, <<"test_data">>).
-define(TEST_DATA_LENGTH, byte_size(?TEST_DATA)).
-define(DIR_NAME, <<"dir_", (atom_to_binary(?FUNCTION_NAME, latin1))/binary>>).
-define(FILE_NAME, <<"file_", (atom_to_binary(?FUNCTION_NAME, latin1))/binary>>).

%%%===================================================================
%%% API
%%%===================================================================

all() -> [
    file_should_be_deleted_from_storage_after_deletion,
    file_should_be_truncated_on_storage_after_truncate,
    directory_should_be_deleted_from_storage_after_deletion,
    empty_directory_should_be_deleted_from_storage_after_deletion,
    file_should_be_deleted_from_storage_after_releasing_handle,
    remote_replica_should_be_deleted_from_storage_after_deletion,
    remote_replica_should_be_truncated_on_storage_after_truncate,
    remote_directory_replica_should_be_deleted_from_storage_after_deletion,
    empty_remote_directory_replica_should_be_deleted_from_storage_after_deletion,
    replica_should_be_deleted_from_storage_after_releasing_handle_to_remotely_deleted_file
].

%%%===================================================================
%%% Test functions
%%%===================================================================

file_should_be_deleted_from_storage_after_deletion(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    StorageFilePath = storage_file_path(Worker, SpaceId, ?FILE_NAME),

    % when
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, <<"/", SpaceName/binary, "/", ?FILE_NAME/binary>>, 8#770),
    {ok, FileHandle} = lfm_proxy:open(Worker, SessId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(Worker, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(Worker, FileHandle),
    ?assertEqual({ok, ?TEST_DATA}, read_file(Worker, StorageFilePath)),
    ?assertMatch({ok, _}, read_file_info(Worker, StorageFilePath)),

    % and
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, {guid, FileGuid})),

    % then
    ?assertEqual({error, ?ENOENT}, read_file(Worker, StorageFilePath)),
    ?assertEqual({error, ?ENOENT}, read_file_info(Worker, StorageFilePath)).

file_should_be_truncated_on_storage_after_truncate(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    TruncateSize = 5,
    TestDataSize2 = ?TEST_DATA_LENGTH - TruncateSize,
    TestData2 = binary_part(?TEST_DATA, 0, TestDataSize2),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    StorageFilePath = storage_file_path(Worker, SpaceId, ?FILE_NAME),

    % when
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, <<"/", SpaceName/binary, "/", ?FILE_NAME/binary>>, 8#770),
    {ok, FileHandle} = lfm_proxy:open(Worker, SessId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(Worker, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(Worker, FileHandle),
    ?assertEqual({ok, ?TEST_DATA}, read_file(Worker, StorageFilePath)),
    ?assertMatch({ok, _}, read_file_info(Worker, StorageFilePath)),

    % and
    ?assertEqual(ok, lfm_proxy:truncate(Worker, SessId, {guid, FileGuid}, TestDataSize2)),

    % then
    ?assertEqual({ok, TestData2}, read_file(Worker, StorageFilePath), ?ATTEMPTS),
    ?assertMatch({ok, #file_info{size = TestDataSize2}}, read_file_info(Worker, StorageFilePath), ?ATTEMPTS).

directory_should_be_deleted_from_storage_after_deletion(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    DirPath = filename:join([<<"/">>, SpaceName, ?DIR_NAME]),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    StorageDirPath = storage_file_path(Worker, SpaceId, ?DIR_NAME),
    StorageFilePath = storage_file_path(Worker, SpaceId, filename:join(?DIR_NAME, ?FILE_NAME)),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(Worker, SessId, DirPath, 8#775),
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, DirGuid, ?FILE_NAME, 8#664),
    {ok, FileHandle} = lfm_proxy:open(Worker, SessId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(Worker, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(Worker, FileHandle),
    ?assertEqual({ok, [binary_to_list(?FILE_NAME)]}, list_dir(Worker, StorageDirPath)),
    ?assertMatch({ok, _}, read_file_info(Worker, StorageDirPath)),
    ?assertEqual({ok, ?TEST_DATA}, read_file(Worker, StorageFilePath)),
    ?assertMatch({ok, _}, read_file_info(Worker, StorageFilePath)),

    % and
    ?assertEqual(ok, lfm_proxy:rm_recursive(Worker, SessId, {guid, DirGuid})),

    % then
    ?assertEqual({error, ?ENOENT}, list_dir(Worker, StorageDirPath)),
    ?assertEqual({error, ?ENOENT}, read_file_info(Worker, StorageFilePath)),
    ?assertEqual({error, ?ENOENT}, read_file(Worker, StorageFilePath)),
    ?assertEqual({error, ?ENOENT}, read_file_info(Worker, StorageDirPath)).

empty_directory_should_be_deleted_from_storage_after_deletion(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    DirPath = filename:join([<<"/">>, SpaceName, ?DIR_NAME]),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    StorageDirPath = storage_file_path(Worker, SpaceId, ?DIR_NAME),
    StorageFilePath = storage_file_path(Worker, SpaceId, filename:join(?DIR_NAME, ?FILE_NAME)),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(Worker, SessId, DirPath, 8#775),
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, DirGuid, ?FILE_NAME, 8#664),
    {ok, FileHandle} = lfm_proxy:open(Worker, SessId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(Worker, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(Worker, FileHandle),
    ?assertEqual({ok, ?TEST_DATA}, read_file(Worker, StorageFilePath)),
    ?assertMatch({ok, _}, read_file_info(Worker, StorageFilePath)),
    ?assertEqual({ok, [binary_to_list(?FILE_NAME)]}, list_dir(Worker, StorageDirPath)),
    ?assertMatch({ok, _}, read_file_info(Worker, StorageDirPath)),

    % remove file to leave empty directory on storage
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, {guid, FileGuid})),
    ?assertEqual({ok, []}, list_dir(Worker, StorageDirPath)),
    ?assertEqual({error, ?ENOENT}, read_file(Worker, StorageFilePath)),

    % and
    ?assertEqual(ok, lfm_proxy:rm_recursive(Worker, SessId, {guid, DirGuid})),

    % then
    ?assertEqual({error, ?ENOENT}, list_dir(Worker, StorageDirPath)),
    ?assertMatch({error, ?ENOENT}, read_file_info(Worker, StorageDirPath)).

file_should_be_deleted_from_storage_after_releasing_handle(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    StorageFilePath = storage_file_path(Worker, SpaceId, ?FILE_NAME),

    % when
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, <<"/", SpaceName/binary, "/", ?FILE_NAME/binary>>, 8#770),
    {ok, FileHandle} = lfm_proxy:open(Worker, SessId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(Worker, FileHandle, 0, ?TEST_DATA),
    ?assertEqual({ok, ?TEST_DATA}, read_file(Worker, StorageFilePath)),

    % and
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, {guid, FileGuid})),
    ?assertEqual({ok, ?TEST_DATA}, read_file(Worker, StorageFilePath)),
    ok = lfm_proxy:close(Worker, FileHandle),

    % then
    ?assertEqual({error, ?ENOENT}, read_file(Worker, StorageFilePath), ?ATTEMPTS),
    ?assertEqual({error, ?ENOENT}, read_file_info(Worker, StorageFilePath), ?ATTEMPTS).

remote_replica_should_be_deleted_from_storage_after_deletion(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    FilePath = filename:join([<<"/">>, SpaceName, ?FILE_NAME]),
    StorageFilePath2 = storage_file_path(WorkerP2, SpaceId, ?FILE_NAME),

    % when
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, FilePath, 8#644),
    {ok, FileHandle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(WorkerP1, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(WorkerP1, FileHandle),

    % and
    {ok, FileHandle2} = ?assertMatch({ok, _},
        lfm_proxy:open(WorkerP2, SessionId2, {guid, FileGuid}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(WorkerP2, FileHandle2, 0, ?TEST_DATA_LENGTH), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ok = lfm_proxy:close(WorkerP2, FileHandle2),
    ok = lfm_proxy:unlink(WorkerP1, SessionId, {guid, FileGuid}),

    %then
    ?assertMatch({error, ?ENOENT}, read_file_info(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS).

remote_replica_should_be_truncated_on_storage_after_truncate(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    TruncateSize = 5,
    TestDataSize2 = ?TEST_DATA_LENGTH - TruncateSize,
    TestData2 = binary_part(?TEST_DATA, 0, TestDataSize2),
    FilePath = filename:join([<<"/">>, SpaceName, ?FILE_NAME]),
    StorageFilePath2 = storage_file_path(WorkerP2, SpaceId, ?FILE_NAME),

    % when
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, FilePath, 8#644),
    {ok, FileHandle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(WorkerP1, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(WorkerP1, FileHandle),

    % and
    {ok, FileHandle2} = ?assertMatch({ok, _},
        lfm_proxy:open(WorkerP2, SessionId2, {guid, FileGuid}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(WorkerP2, FileHandle2, 0, ?TEST_DATA_LENGTH), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ok = lfm_proxy:close(WorkerP2, FileHandle2),
    ?assertEqual(ok, lfm_proxy:truncate(WorkerP1, SessionId, {guid, FileGuid}, TestDataSize2)),

    %then
    ?assertMatch({ok, TestData2}, read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ?assertMatch({ok, #file_info{size = TestDataSize2}}, read_file_info(WorkerP2, StorageFilePath2), ?ATTEMPTS).

remote_directory_replica_should_be_deleted_from_storage_after_deletion(Config) ->
    [WorkerP2, WorkerP1 | _] = ?config(op_worker_nodes, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    DirPath = filename:join([<<"/">>, SpaceName, ?DIR_NAME]),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    StorageDirPath2 = storage_file_path(WorkerP2, SpaceId, ?DIR_NAME),
    StorageFilePath2 = storage_file_path(WorkerP2, SpaceId, filename:join(?DIR_NAME, ?FILE_NAME)),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(WorkerP1, SessionId, DirPath, 8#775),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, DirGuid, ?FILE_NAME, 8#664),
    {ok, FileHandle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(WorkerP1, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(WorkerP1, FileHandle),

    %and
    {ok, FileHandle2} = ?assertMatch({ok, _},
        lfm_proxy:open(WorkerP2, SessionId2, {guid, FileGuid}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(WorkerP2, FileHandle2, 0, ?TEST_DATA_LENGTH), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ok = lfm_proxy:close(WorkerP2, FileHandle2),
    ?assertEqual({ok, [binary_to_list(?FILE_NAME)]}, list_dir(WorkerP2, StorageDirPath2)),
    ?assertEqual({ok, ?TEST_DATA}, read_file(WorkerP2, StorageFilePath2)),
    ?assertEqual(ok, lfm_proxy:rm_recursive(WorkerP1, SessionId, {guid, DirGuid})),

    % then
    ?assertEqual({error, ?ENOENT}, list_dir(WorkerP2, StorageDirPath2), ?ATTEMPTS),
    ?assertEqual({error, ?ENOENT}, read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS).

empty_remote_directory_replica_should_be_deleted_from_storage_after_deletion(Config) ->
    [WorkerP2, WorkerP1 | _] = ?config(op_worker_nodes, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    DirPath = filename:join([<<"/">>, SpaceName, ?DIR_NAME]),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    StorageDirPath2 = storage_file_path(WorkerP2, SpaceId, ?DIR_NAME),
    StorageFilePath2 = storage_file_path(WorkerP2, SpaceId, filename:join(?DIR_NAME, ?FILE_NAME)),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(WorkerP1, SessionId, DirPath, 8#775),
    % create file so that directory will be created on storage
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, DirGuid, ?FILE_NAME, 8#664),
    {ok, FileHandle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(WorkerP1, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(WorkerP1, FileHandle),

    %and
    {ok, FileHandle2} = ?assertMatch({ok, _},
        lfm_proxy:open(WorkerP2, SessionId2, {guid, FileGuid}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(WorkerP2, FileHandle2, 0, ?TEST_DATA_LENGTH), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ok = lfm_proxy:close(WorkerP2, FileHandle2),
    ?assertEqual({ok, [binary_to_list(?FILE_NAME)]}, list_dir(WorkerP2, StorageDirPath2)),
    ?assertEqual({ok, ?TEST_DATA}, read_file(WorkerP2, StorageFilePath2)),
    % delete file to leave an empty directory on storage
    ?assertEqual(ok, lfm_proxy:unlink(WorkerP1, SessionId, {guid, FileGuid})),
    ?assertEqual({error, ?ENOENT}, read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ?assertEqual({error, ?ENOENT}, read_file_info(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    % delete empty directory
    ?assertEqual(ok, lfm_proxy:rm_recursive(WorkerP1, SessionId, {guid, DirGuid})),

    % then
    ?assertEqual({error, ?ENOENT}, list_dir(WorkerP2, StorageDirPath2), ?ATTEMPTS),
    ?assertEqual({error, ?ENOENT}, read_file_info(WorkerP2, StorageDirPath2), ?ATTEMPTS).

replica_should_be_deleted_from_storage_after_releasing_handle_to_remotely_deleted_file(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    SessionId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP2)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    FilePath = filename:join([<<"/">>, SpaceName, ?FILE_NAME]),
    StorageFilePath1 = storage_file_path(WorkerP1, SpaceId, ?FILE_NAME),
    StorageFilePath2 = storage_file_path(WorkerP2, SpaceId, ?FILE_NAME),

    % when
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, FilePath, 8#644),
    {ok, FileHandle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(WorkerP1, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(WorkerP1, FileHandle),

    % and
    {ok, FileHandle2} = ?assertMatch({ok, _},
        lfm_proxy:open(WorkerP2, SessionId2, {guid, FileGuid}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(WorkerP2, FileHandle2, 0, ?TEST_DATA_LENGTH), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ok = lfm_proxy:unlink(WorkerP1, SessionId, {guid, FileGuid}),

    %then
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(WorkerP1, SessionId, {guid, FileGuid})),
    ?assertMatch({error, ?ENOENT}, read_file(WorkerP1, StorageFilePath1)),

    % ensure that unlinking file is synced
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(WorkerP2, SessionId2, {guid, FileGuid}), ?ATTEMPTS),

    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(WorkerP2, FileHandle2, 0, ?TEST_DATA_LENGTH), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, read_file(WorkerP2, StorageFilePath2)),

    ok = lfm_proxy:close(WorkerP2, FileHandle2),
    ?assertMatch({error, ?ENOENT}, read_file(WorkerP2, StorageFilePath1)).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(1)), % TODO - change to 2 seconds
            test_utils:set_env(Worker, ?APP_NAME, prefetching, off)
        end, ?config(op_worker_nodes, NewConfig2)),

        application:start(ssl),
        hackney:start(),
        NewConfig3 = initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig2, "env_desc.json"), NewConfig2),
        NewConfig3
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer]}
        | Config
    ].

end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 60}),
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

read_file(Worker, FilePath) ->
    rpc:call(Worker, file, read_file, [FilePath]).

read_file_info(Worker, FilePath) ->
    rpc:call(Worker, file, read_file_info, [FilePath]).

list_dir(Worker, DirPath) ->
    rpc:call(Worker, file, list_dir, [DirPath]).

storage_file_path(Worker, SpaceId, FilePath) ->
    SpaceMnt = get_space_mount_point(Worker, SpaceId),
    filename:join([SpaceMnt, SpaceId, FilePath]).

get_space_mount_point(Worker, SpaceId) ->
    StorageId = get_supporting_storage_id(Worker, SpaceId),
    storage_mount_point(Worker, StorageId).

get_supporting_storage_id(Worker, SpaceId) ->
    [StorageId] = rpc:call(Worker, space_storage, get_storage_ids, [SpaceId]),
    StorageId.

storage_mount_point(Worker, StorageId) ->
    [Helper | _] = rpc:call(Worker, storage, get_helpers, [StorageId]),
    HelperArgs = helper:get_args(Helper),
    maps:get(<<"mountPoint">>, HelperArgs).
