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
-include("modules/fslogic/fslogic_suffix.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/errors.hrl").
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
    directory_should_be_deleted_from_storage_after_releasing_handle_to_its_child/1,
    remote_replica_should_be_deleted_from_storage_after_deletion/1,
    remote_directory_replica_should_be_deleted_from_storage_after_deletion/1,
    remote_replica_should_be_truncated_on_storage_after_truncate/1,
    empty_remote_directory_replica_should_be_deleted_from_storage_after_deletion/1,
    replica_should_be_deleted_from_storage_after_releasing_handle_to_remotely_deleted_file/1,
    parent_dir_of_replica_should_be_deleted_from_storage_after_releasing_handle_to_remotely_deleted_file/1,
    race_on_remote_deletion_of_parent_and_child/1,

    % Tests of conflicting files
    file_with_suffix_is_deleted_from_storage_after_deletion/1,
    deleted_open_file_with_suffix_is_deleted_from_storage_after_release/1,
    suffix_in_metadata_and_storage_test/1,
    suffix_in_dir_metadata_test/1
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
    directory_should_be_deleted_from_storage_after_releasing_handle_to_its_child,
    remote_replica_should_be_deleted_from_storage_after_deletion,
    remote_replica_should_be_truncated_on_storage_after_truncate,
    remote_directory_replica_should_be_deleted_from_storage_after_deletion,
    empty_remote_directory_replica_should_be_deleted_from_storage_after_deletion,
    replica_should_be_deleted_from_storage_after_releasing_handle_to_remotely_deleted_file,
    parent_dir_of_replica_should_be_deleted_from_storage_after_releasing_handle_to_remotely_deleted_file,
    race_on_remote_deletion_of_parent_and_child,

    % Tests of conflicting files
    file_with_suffix_is_deleted_from_storage_after_deletion,
    deleted_open_file_with_suffix_is_deleted_from_storage_after_release,
    suffix_in_metadata_and_storage_test,
    suffix_in_dir_metadata_test
].

-define(USER1, <<"user1">>).
-define(SPACE_ID(Config), element(1, ?SPACE_ID_AND_NAME(Config))).
-define(SPACE_NAME(Config), element(2, ?SPACE_ID_AND_NAME(Config))).

-define(SPACE_ID_AND_NAME(Config), ?SPACE_ID_AND_NAME(Config, ?USER1)).
-define(SPACE_ID_AND_NAME(Config, User), begin
    [{SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),
    {SpaceId, SpaceName}
end).
-define(SESS_ID(Worker, Config), ?SESS_ID(Worker, Config, ?USER1)).
-define(SESS_ID(Worker, Config, User),
    ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config)).

%%%===================================================================
%%% Test functions
%%%===================================================================

file_should_be_deleted_from_storage_after_deletion(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(Config),
    SpaceName = ?SPACE_NAME(Config),
    SessId = ?SESS_ID(Worker, Config),
    StorageFilePath = storage_test_utils:file_path(Worker, SpaceId, ?FILE_NAME),
    FilePath =  filename:join(["/", SpaceName, ?FILE_NAME]),
    % when
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, FilePath),
    {ok, FileHandle} = lfm_proxy:open(Worker, SessId, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(Worker, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(Worker, FileHandle),
    ?assertEqual({ok, ?TEST_DATA}, storage_test_utils:read_file(Worker, StorageFilePath)),
    ?assertMatch({ok, _}, storage_test_utils:read_file_info(Worker, StorageFilePath)),

    % and
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(FileGuid))),

    % then
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file(Worker, StorageFilePath), ?ATTEMPTS),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file_info(Worker, StorageFilePath), ?ATTEMPTS).

file_should_be_truncated_on_storage_after_truncate(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(Config),
    SpaceName = ?SPACE_NAME(Config),
    SessId = ?SESS_ID(Worker, Config),
    TruncateSize = 5,
    TestDataSize2 = ?TEST_DATA_LENGTH - TruncateSize,
    TestData2 = binary_part(?TEST_DATA, 0, TestDataSize2),
    StorageFilePath = storage_test_utils:file_path(Worker, SpaceId, ?FILE_NAME),
    FilePath =  filename:join(["/", SpaceName, ?FILE_NAME]),
    % when
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, FilePath),


    {ok, FileHandle} = lfm_proxy:open(Worker, SessId, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(Worker, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(Worker, FileHandle),
    ?assertEqual({ok, ?TEST_DATA}, storage_test_utils:read_file(Worker, StorageFilePath)),
    ?assertMatch({ok, _}, storage_test_utils:read_file_info(Worker, StorageFilePath)),

    % and
    ?assertEqual(ok, lfm_proxy:truncate(Worker, SessId, ?FILE_REF(FileGuid), TestDataSize2)),

    % then
    ?assertEqual({ok, TestData2}, storage_test_utils:read_file(Worker, StorageFilePath), ?ATTEMPTS),
    ?assertMatch({ok, #file_info{size = TestDataSize2}}, storage_test_utils:read_file_info(Worker, StorageFilePath),
        ?ATTEMPTS).

directory_should_be_deleted_from_storage_after_deletion(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(Config),
    SpaceName = ?SPACE_NAME(Config),
    SessId = ?SESS_ID(Worker, Config),
    DirPath = filename:join([<<"/">>, SpaceName, ?DIR_NAME]),
    StorageDirPath = storage_test_utils:file_path(Worker, SpaceId, ?DIR_NAME),
    StorageFilePath = storage_test_utils:file_path(Worker, SpaceId, filename:join(?DIR_NAME, ?FILE_NAME)),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(Worker, SessId, DirPath),
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, DirGuid, ?FILE_NAME, ?DEFAULT_FILE_PERMS),
    {ok, FileHandle} = lfm_proxy:open(Worker, SessId, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(Worker, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(Worker, FileHandle),

    ?assertEqual({ok, [binary_to_list(?FILE_NAME)]}, storage_test_utils:list_dir(Worker, StorageDirPath)),
    ?assertMatch({ok, _}, storage_test_utils:read_file_info(Worker, StorageDirPath)),
    ?assertEqual({ok, ?TEST_DATA}, storage_test_utils:read_file(Worker, StorageFilePath)),
    ?assertMatch({ok, _}, storage_test_utils:read_file_info(Worker, StorageFilePath)),

    % and
    ?assertEqual(ok, lfm_proxy:rm_recursive(Worker, SessId, ?FILE_REF(DirGuid))),

    % then
    ?assertEqual({error, ?ENOENT}, storage_test_utils:list_dir(Worker, StorageDirPath), ?ATTEMPTS),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file_info(Worker, StorageFilePath), ?ATTEMPTS),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file(Worker, StorageFilePath), ?ATTEMPTS),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file_info(Worker, StorageDirPath), ?ATTEMPTS).

empty_directory_should_be_deleted_from_storage_after_deletion(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(Config),
    SpaceName = ?SPACE_NAME(Config),
    SessId = ?SESS_ID(Worker, Config),
    DirPath = filename:join([<<"/">>, SpaceName, ?DIR_NAME]),
    StorageDirPath = storage_test_utils:file_path(Worker, SpaceId, ?DIR_NAME),
    StorageFilePath = storage_test_utils:file_path(Worker, SpaceId, filename:join(?DIR_NAME, ?FILE_NAME)),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(Worker, SessId, DirPath),
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, DirGuid, ?FILE_NAME, ?DEFAULT_FILE_PERMS),
    {ok, FileHandle} = lfm_proxy:open(Worker, SessId, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(Worker, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(Worker, FileHandle),
    ?assertEqual({ok, ?TEST_DATA}, storage_test_utils:read_file(Worker, StorageFilePath)),
    ?assertMatch({ok, _}, storage_test_utils:read_file_info(Worker, StorageFilePath)),
    ?assertEqual({ok, [binary_to_list(?FILE_NAME)]}, storage_test_utils:list_dir(Worker, StorageDirPath)),
    ?assertMatch({ok, _}, storage_test_utils:read_file_info(Worker, StorageDirPath)),

    % remove file to leave empty directory on storage
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(FileGuid))),
    ?assertEqual({ok, []}, storage_test_utils:list_dir(Worker, StorageDirPath)),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file(Worker, StorageFilePath), ?ATTEMPTS),

    % and
    ?assertEqual(ok, lfm_proxy:rm_recursive(Worker, SessId, ?FILE_REF(DirGuid))),

    % then
    ?assertEqual({error, ?ENOENT}, storage_test_utils:list_dir(Worker, StorageDirPath), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, storage_test_utils:read_file_info(Worker, StorageDirPath), ?ATTEMPTS).

file_should_be_deleted_from_storage_after_releasing_handle(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(Config),
    SpaceName = ?SPACE_NAME(Config),
    SessId = ?SESS_ID(Worker, Config),
    StorageFilePath = storage_test_utils:file_path(Worker, SpaceId, ?FILE_NAME),
    FilePath =  filename:join(["/", SpaceName, ?FILE_NAME]),
    % when
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, FilePath),
    {ok, FileHandle} = lfm_proxy:open(Worker, SessId, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(Worker, FileHandle, 0, ?TEST_DATA),
    ?assertEqual({ok, ?TEST_DATA}, storage_test_utils:read_file(Worker, StorageFilePath)),

    % and
    ?assertEqual(ok, lfm_proxy:unlink(Worker, SessId, ?FILE_REF(FileGuid))),
    ?assertEqual({ok, ?TEST_DATA}, storage_test_utils:read_file(Worker, StorageFilePath)),
    ok = lfm_proxy:close(Worker, FileHandle),

    % then
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file(Worker, StorageFilePath), ?ATTEMPTS),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file_info(Worker, StorageFilePath), ?ATTEMPTS).

directory_should_be_deleted_from_storage_after_releasing_handle_to_its_child(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(Config),
    SpaceName = ?SPACE_NAME(Config),
    SessId = ?SESS_ID(Worker, Config),
    DirPath = filename:join([<<"/">>, SpaceName, ?DIR_NAME]),
    StorageDirPath = storage_test_utils:file_path(Worker, SpaceId, ?DIR_NAME),
    StorageFilePath = storage_test_utils:file_path(Worker, SpaceId, filename:join(?DIR_NAME, ?FILE_NAME)),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(Worker, SessId, DirPath),
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, DirGuid, ?FILE_NAME, ?DEFAULT_FILE_PERMS),
    {ok, FileHandle} = lfm_proxy:open(Worker, SessId, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(Worker, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:fsync(Worker, FileHandle),
    ?assertEqual({ok, [binary_to_list(?FILE_NAME)]}, storage_test_utils:list_dir(Worker, StorageDirPath)),
    ?assertMatch({ok, _}, storage_test_utils:read_file_info(Worker, StorageDirPath)),
    ?assertEqual({ok, ?TEST_DATA}, storage_test_utils:read_file(Worker, StorageFilePath)),
    ?assertMatch({ok, _}, storage_test_utils:read_file_info(Worker, StorageFilePath)),

    % and
    ?assertEqual(ok, lfm_proxy:rm_recursive(Worker, SessId, ?FILE_REF(DirGuid))),

    % then
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(Worker, SessId, ?FILE_REF(DirGuid)), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_children(Worker, SessId, ?FILE_REF(DirGuid), 0, 1), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(Worker, SessId, ?FILE_REF(FileGuid)), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:open(Worker, SessId, ?FILE_REF(FileGuid), read), ?ATTEMPTS),

    ?assertEqual({ok, [binary_to_list(?FILE_NAME)]}, storage_test_utils:list_dir(Worker, StorageDirPath)),
    ?assertMatch({ok, _}, storage_test_utils:read_file_info(Worker, StorageDirPath)),
    ?assertEqual({ok, ?TEST_DATA}, storage_test_utils:read_file(Worker, StorageFilePath)),
    ?assertMatch({ok, _}, storage_test_utils:read_file_info(Worker, StorageFilePath)),

    % and
    lfm_proxy:close(Worker, FileHandle),

    % then
    ?assertEqual({error, ?ENOENT}, storage_test_utils:list_dir(Worker, StorageDirPath), ?ATTEMPTS),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file_info(Worker, StorageFilePath), ?ATTEMPTS),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file(Worker, StorageFilePath), ?ATTEMPTS),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file_info(Worker, StorageDirPath), ?ATTEMPTS).

remote_replica_should_be_deleted_from_storage_after_deletion(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(Config),
    SpaceName = ?SPACE_NAME(Config),
    SessionId = ?SESS_ID(WorkerP1, Config),
    SessionId2 = ?SESS_ID(WorkerP2, Config),
    FilePath = filename:join([<<"/">>, SpaceName, ?FILE_NAME]),
    StorageFilePath2 = storage_test_utils:file_path(WorkerP2, SpaceId, ?FILE_NAME),

    % when
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, FilePath),
    {ok, FileHandle} = lfm_proxy:open(WorkerP1, SessionId, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(WorkerP1, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(WorkerP1, FileHandle),

    % and
    {ok, FileHandle2} = ?assertMatch({ok, _},
        lfm_proxy:open(WorkerP2, SessionId2, ?FILE_REF(FileGuid), read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(WorkerP2, FileHandle2, 0, ?TEST_DATA_LENGTH), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, storage_test_utils:read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ok = lfm_proxy:close(WorkerP2, FileHandle2),
    ok = lfm_proxy:unlink(WorkerP1, SessionId, ?FILE_REF(FileGuid)),

    %then
    ?assertMatch({error, ?ENOENT}, storage_test_utils:read_file_info(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, storage_test_utils:read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS).

remote_replica_should_be_truncated_on_storage_after_truncate(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(Config),
    SpaceName = ?SPACE_NAME(Config),
    SessionId = ?SESS_ID(WorkerP1, Config),
    SessionId2 = ?SESS_ID(WorkerP2, Config),
    TruncateSize = 5,
    TestDataSize2 = ?TEST_DATA_LENGTH - TruncateSize,
    TestData2 = binary_part(?TEST_DATA, 0, TestDataSize2),
    FilePath = filename:join([<<"/">>, SpaceName, ?FILE_NAME]),
    StorageFilePath2 = storage_test_utils:file_path(WorkerP2, SpaceId, ?FILE_NAME),

    % when
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, FilePath),
    {ok, FileHandle} = lfm_proxy:open(WorkerP1, SessionId, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(WorkerP1, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(WorkerP1, FileHandle),

    % and
    {ok, FileHandle2} = ?assertMatch({ok, _},
        lfm_proxy:open(WorkerP2, SessionId2, ?FILE_REF(FileGuid), read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(WorkerP2, FileHandle2, 0, ?TEST_DATA_LENGTH), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, storage_test_utils:read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ok = lfm_proxy:close(WorkerP2, FileHandle2),
    ?assertEqual(ok, lfm_proxy:truncate(WorkerP1, SessionId, ?FILE_REF(FileGuid), TestDataSize2)),

    %then
    ?assertMatch({ok, TestData2}, storage_test_utils:read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ?assertMatch({ok, #file_info{size = TestDataSize2}}, storage_test_utils:read_file_info(WorkerP2, StorageFilePath2),
        ?ATTEMPTS).

remote_directory_replica_should_be_deleted_from_storage_after_deletion(Config) ->
    [WorkerP2, WorkerP1 | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(Config),
    SpaceName = ?SPACE_NAME(Config),
    SessionId = ?SESS_ID(WorkerP1, Config),
    SessionId2 = ?SESS_ID(WorkerP2, Config),
    DirPath = filename:join([<<"/">>, SpaceName, ?DIR_NAME]),
    StorageDirPath2 = storage_test_utils:file_path(WorkerP2, SpaceId, ?DIR_NAME),
    StorageFilePath2 = storage_test_utils:file_path(WorkerP2, SpaceId, filename:join(?DIR_NAME, ?FILE_NAME)),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(WorkerP1, SessionId, DirPath),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, DirGuid, ?FILE_NAME, ?DEFAULT_FILE_PERMS),
    {ok, FileHandle} = lfm_proxy:open(WorkerP1, SessionId, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(WorkerP1, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(WorkerP1, FileHandle),

    %and
    {ok, FileHandle2} = ?assertMatch({ok, _},
        lfm_proxy:open(WorkerP2, SessionId2, ?FILE_REF(FileGuid), read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(WorkerP2, FileHandle2, 0, ?TEST_DATA_LENGTH), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, storage_test_utils:read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ok = lfm_proxy:close(WorkerP2, FileHandle2),
    ?assertEqual({ok, [binary_to_list(?FILE_NAME)]}, storage_test_utils:list_dir(WorkerP2, StorageDirPath2)),
    ?assertEqual({ok, ?TEST_DATA}, storage_test_utils:read_file(WorkerP2, StorageFilePath2)),

    ?assertEqual(ok, lfm_proxy:rm_recursive(WorkerP1, SessionId, ?FILE_REF(DirGuid))),

    % then
    ?assertEqual({error, ?ENOENT}, storage_test_utils:list_dir(WorkerP2, StorageDirPath2), ?ATTEMPTS),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS).

empty_remote_directory_replica_should_be_deleted_from_storage_after_deletion(Config) ->
    [WorkerP2, WorkerP1 | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(Config),
    SpaceName = ?SPACE_NAME(Config),
    SessionId = ?SESS_ID(WorkerP1, Config),
    SessionId2 = ?SESS_ID(WorkerP2, Config),
    DirPath = filename:join([<<"/">>, SpaceName, ?DIR_NAME]),
    StorageDirPath2 = storage_test_utils:file_path(WorkerP2, SpaceId, ?DIR_NAME),
    StorageFilePath2 = storage_test_utils:file_path(WorkerP2, SpaceId, filename:join(?DIR_NAME, ?FILE_NAME)),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(WorkerP1, SessionId, DirPath),
    % create file so that directory will be created on storage
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, DirGuid, ?FILE_NAME, ?DEFAULT_FILE_PERMS),
    {ok, FileHandle} = lfm_proxy:open(WorkerP1, SessionId, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(WorkerP1, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(WorkerP1, FileHandle),

    %and
    {ok, FileHandle2} = ?assertMatch({ok, _},
        lfm_proxy:open(WorkerP2, SessionId2, ?FILE_REF(FileGuid), read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(WorkerP2, FileHandle2, 0, ?TEST_DATA_LENGTH), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, storage_test_utils:read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ok = lfm_proxy:close(WorkerP2, FileHandle2),
    ?assertEqual({ok, [binary_to_list(?FILE_NAME)]}, storage_test_utils:list_dir(WorkerP2, StorageDirPath2)),
    ?assertEqual({ok, ?TEST_DATA}, storage_test_utils:read_file(WorkerP2, StorageFilePath2)),
    % delete file to leave an empty directory on storage
    ?assertEqual(ok, lfm_proxy:unlink(WorkerP1, SessionId, ?FILE_REF(FileGuid))),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file_info(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    % delete empty directory
    ?assertEqual(ok, lfm_proxy:rm_recursive(WorkerP1, SessionId, ?FILE_REF(DirGuid))),

    % then
    ?assertEqual({error, ?ENOENT}, storage_test_utils:list_dir(WorkerP2, StorageDirPath2), ?ATTEMPTS),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file_info(WorkerP2, StorageDirPath2), ?ATTEMPTS).

replica_should_be_deleted_from_storage_after_releasing_handle_to_remotely_deleted_file(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(Config),
    SpaceName = ?SPACE_NAME(Config),
    SessionId = ?SESS_ID(WorkerP1, Config),
    SessionId2 = ?SESS_ID(WorkerP2, Config),
    FilePath = filename:join([<<"/">>, SpaceName, ?FILE_NAME]),
    StorageFilePath1 = storage_test_utils:file_path(WorkerP1, SpaceId, ?FILE_NAME),
    StorageFilePath2 = storage_test_utils:file_path(WorkerP2, SpaceId, ?FILE_NAME),

    % when
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, FilePath),
    {ok, FileHandle} = lfm_proxy:open(WorkerP1, SessionId, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(WorkerP1, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(WorkerP1, FileHandle),

    % and
    {ok, FileHandle2} = ?assertMatch({ok, _},
        lfm_proxy:open(WorkerP2, SessionId2, ?FILE_REF(FileGuid), read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(WorkerP2, FileHandle2, 0, ?TEST_DATA_LENGTH), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, storage_test_utils:read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ok = lfm_proxy:unlink(WorkerP1, SessionId, ?FILE_REF(FileGuid)),

    %then
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(WorkerP1, SessionId, ?FILE_REF(FileGuid))),
    ?assertMatch({error, ?ENOENT}, storage_test_utils:read_file(WorkerP1, StorageFilePath1), ?ATTEMPTS),

    % ensure that unlinking file is synced
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(WorkerP2, SessionId2, ?FILE_REF(FileGuid)), ?ATTEMPTS),

    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(WorkerP2, FileHandle2, 0, ?TEST_DATA_LENGTH), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, storage_test_utils:read_file(WorkerP2, StorageFilePath2)),

    ok = lfm_proxy:close(WorkerP2, FileHandle2),
    ?assertMatch({error, ?ENOENT}, storage_test_utils:read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS).

parent_dir_of_replica_should_be_deleted_from_storage_after_releasing_handle_to_remotely_deleted_file(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(Config),
    SpaceName = ?SPACE_NAME(Config),
    SessionId = ?SESS_ID(WorkerP1, Config),
    SessionId2 = ?SESS_ID(WorkerP2, Config),
    DirPath = filename:join([<<"/">>, SpaceName, ?DIR_NAME]),
    FilePath = filename:join([DirPath, ?FILE_NAME]),
    StorageDirPath = storage_test_utils:file_path(WorkerP1, SpaceId, ?DIR_NAME),
    StorageFilePath1 = filename:join([StorageDirPath, ?FILE_NAME]),
    StorageDirPath2 = storage_test_utils:file_path(WorkerP2, SpaceId, ?DIR_NAME),
    StorageFilePath2 = filename:join([StorageDirPath2, ?FILE_NAME]),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(WorkerP1, SessionId, DirPath),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, FilePath),
    {ok, FileHandle} = lfm_proxy:open(WorkerP1, SessionId, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(WorkerP1, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(WorkerP1, FileHandle),

    % and
    {ok, FileHandle2} = ?assertMatch({ok, _},
        lfm_proxy:open(WorkerP2, SessionId2, ?FILE_REF(FileGuid), read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(WorkerP2, FileHandle2, 0, ?TEST_DATA_LENGTH), ?ATTEMPTS),


    % File is intentionally left opened on WorkerP2
    ?assertMatch({ok, ?TEST_DATA}, storage_test_utils:read_file(WorkerP2, StorageFilePath2), ?ATTEMPTS),
    ok = lfm_proxy:rm_recursive(WorkerP1, SessionId, ?FILE_REF(DirGuid)),

    %then
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(WorkerP1, SessionId, ?FILE_REF(DirGuid)), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(WorkerP1, SessionId, ?FILE_REF(FileGuid)), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, storage_test_utils:list_dir(WorkerP1, StorageDirPath), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, storage_test_utils:read_file(WorkerP1, StorageFilePath1), ?ATTEMPTS),

    % ensure that directory was removed on 2nd provider
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(WorkerP2, SessionId2, ?FILE_REF(DirGuid)), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(WorkerP2, SessionId2, ?FILE_REF(FileGuid)), ?ATTEMPTS),

    % but not on it's storage as the file is still opened
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(WorkerP2, FileHandle2, 0, ?TEST_DATA_LENGTH), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, storage_test_utils:read_file(WorkerP2, StorageFilePath2)),
    ?assertEqual({ok, [binary_to_list(?FILE_NAME)]}, storage_test_utils:list_dir(WorkerP2, StorageDirPath2)),

    ok = lfm_proxy:close(WorkerP2, FileHandle2),

    % after closing the file, it should be deleted, as well as its parent directory
    ?assertMatch({error, ?ENOENT}, storage_test_utils:list_dir(WorkerP2, StorageDirPath2)),
    ?assertMatch({error, ?ENOENT}, storage_test_utils:read_file(WorkerP2, StorageFilePath2)).

race_on_remote_deletion_of_parent_and_child(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(Config),
    SpaceName = ?SPACE_NAME(Config),
    SessId = ?SESS_ID(Worker, Config),
    DirPath = filename:join([<<"/">>, SpaceName, ?DIR_NAME]),
    StorageDirPath = storage_test_utils:file_path(Worker, SpaceId, ?DIR_NAME),
    StorageFilePath = storage_test_utils:file_path(Worker, SpaceId, filename:join(?DIR_NAME, ?FILE_NAME)),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(Worker, SessId, DirPath),
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, DirGuid, ?FILE_NAME, ?DEFAULT_FILE_PERMS),
    {ok, FileHandle} = lfm_proxy:open(Worker, SessId, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(Worker, FileHandle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(Worker, FileHandle),
    ?assertEqual({ok, [binary_to_list(?FILE_NAME)]}, storage_test_utils:list_dir(Worker, StorageDirPath)),
    ?assertMatch({ok, _}, storage_test_utils:read_file_info(Worker, StorageDirPath)),
    ?assertEqual({ok, ?TEST_DATA}, storage_test_utils:read_file(Worker, StorageFilePath)),
    ?assertMatch({ok, _}, storage_test_utils:read_file_info(Worker, StorageFilePath)),

    % and
    {DirUuid, SpaceId} = file_id:unpack_guid(DirGuid),
    {FileUuid, SpaceId} = file_id:unpack_guid(FileGuid),

    % pretend that files were deleted
    ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [FileUuid])),
    ?assertEqual(ok, rpc:call(Worker, file_meta, delete, [DirUuid])),

    % pretend that directory doc is synchronized before its child doc
    {ok, DirDoc} = rpc:call(Worker, file_meta, get_including_deleted, [DirUuid]),
    {ok, FileDoc} = rpc:call(Worker, file_meta, get_including_deleted, [FileUuid]),
    ok = rpc:call(Worker, dbsync_events, change_replicated, [SpaceId, DirDoc]),
    % check that directory was not deleted from storage
    ?assertMatch({ok, _}, storage_test_utils:read_file_info(Worker, StorageDirPath), 10),
    ok = rpc:call(Worker, dbsync_events, change_replicated, [SpaceId, FileDoc]),

    % then
    ?assertEqual({error, ?ENOENT}, storage_test_utils:list_dir(Worker, StorageDirPath), 10),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file_info(Worker, StorageFilePath), 10),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file(Worker, StorageFilePath), 10),
    ?assertEqual({error, ?ENOENT}, storage_test_utils:read_file_info(Worker, StorageDirPath), 10).


file_with_suffix_is_deleted_from_storage_after_deletion(Config) ->
    file_with_suffix_is_deleted_from_storage_after_deletion_base(Config, true).

deleted_open_file_with_suffix_is_deleted_from_storage_after_release(Config) ->
    file_with_suffix_is_deleted_from_storage_after_deletion_base(Config, false).

suffix_in_metadata_and_storage_test(Config) ->
    [Worker2, Worker1] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(Config),
    SpaceName = ?SPACE_NAME(Config),
    SessionId1 = ?SESS_ID(Worker1, Config),
    SessionId2 = ?SESS_ID(Worker2, Config),
    SpacePath = <<"/", SpaceName/binary>>,

    FileName = generator:gen_name(),
    FilePath =  filename:join([SpacePath, FileName]),
    StorageSpacePathW1 = storage_test_utils:space_path(Worker1, SpaceId),

    ListDir = fun(Worker, Session, Path) ->
        {ok, List} = lfm_proxy:get_children(Worker, Session, {path, Path}, 0, 100),
        List
    end,
    {ok, StorageFiles} = storage_test_utils:list_dir(Worker1, StorageSpacePathW1),
    ListStorageDir = fun() ->
        {ok, List} = storage_test_utils:list_dir(Worker1, StorageSpacePathW1),
        lists:sort(List -- StorageFiles)
    end,

    % create files
    {ok, Guid1} = lfm_proxy:create(Worker1, SessionId1, FilePath),
    {ok, Guid2} = lfm_proxy:create(Worker2, SessionId2, FilePath),

    StorageFilePath1 = storage_test_utils:file_path(Worker1, SpaceId, FileName),
    Uuid = file_id:guid_to_uuid(Guid2),
    StorageFilePath2 = storage_test_utils:file_path(Worker1, SpaceId, ?CONFLICTING_STORAGE_FILE_NAME(FileName, Uuid)),

    ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessionId1, ?FILE_REF(Guid1)), ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessionId1, ?FILE_REF(Guid2)), ?ATTEMPTS),

    ?assertEqual(2, length(ListDir(Worker1, SessionId1, SpacePath)), ?ATTEMPTS),

    % open, write and read
    Content1 = <<"data_file1">>,
    Content2 = <<"data_file2">>,

    {ok, Handle1} = lfm_proxy:open(Worker1, SessionId1, ?FILE_REF(Guid1), rdwr),
    {ok, Handle2} = lfm_proxy:open(Worker1, SessionId1, ?FILE_REF(Guid2), rdwr),

    ExpectedStorageFileList =
        [binary_to_list(FileName), binary_to_list(?CONFLICTING_STORAGE_FILE_NAME(FileName, Uuid))],

    ?assertEqual(ExpectedStorageFileList, ListStorageDir(), ?ATTEMPTS),

    {ok, _} = lfm_proxy:write(Worker1, Handle1, 0, Content1),
    {ok, _} = lfm_proxy:write(Worker1, Handle2, 0, Content2),

    ?assertMatch({ok, Content1}, lfm_proxy:read(Worker1, Handle1, 0, byte_size(Content1)), ?ATTEMPTS),
    ?assertMatch({ok, Content2}, lfm_proxy:read(Worker1, Handle2, 0, byte_size(Content2)), ?ATTEMPTS),

    % check data on storage
    ?assertMatch({ok, Content1}, storage_test_utils:read_file(Worker1, StorageFilePath1)),
    ?assertMatch({ok, Content2}, storage_test_utils:read_file(Worker1, StorageFilePath2)),

    ?assertEqual(ok, lfm_proxy:close(Worker1, Handle1)),
    ?assertEqual(ok, lfm_proxy:unlink(Worker1, SessionId1, ?FILE_REF(Guid1))),

    [{_, Name}] = ListDir(Worker1, SessionId1, SpacePath),
    ?assertEqual(FileName, Name),
    % File on storage is not renamed currently
    %%    ?assertEqual([Name], ListStorageDir()),

    ok = lfm_proxy:close_all(Worker1),
    ok = lfm_proxy:unlink(Worker1, SessionId1, {path, FilePath}).

suffix_in_dir_metadata_test(Config) ->
    [Worker2, Worker1] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(Config),
    SpaceName = ?SPACE_NAME(Config),
    SessionId1 = ?SESS_ID(Worker1, Config),
    SessionId2 = ?SESS_ID(Worker2, Config),
    SpacePath = <<"/", SpaceName/binary>>,

    DirName = generator:gen_name(),
    FileName = generator:gen_name(),
    DirPath = filename:join([SpacePath, DirName]),
    FilePath = filename:join([DirPath, FileName]),
    StorageSpacePathW1 = storage_test_utils:space_path(Worker1, SpaceId),

    ListDir = fun(Worker, Session, Path) ->
        {ok, List} = lfm_proxy:get_children(Worker, Session, {path, Path}, 0, 100),
        List
    end,

    % create files
    {ok, _} = lfm_proxy:mkdir(Worker1, SessionId1, DirPath),
    {ok, _} = lfm_proxy:mkdir(Worker2, SessionId2, DirPath),
    {ok, {Guid1, H1}} = lfm_proxy:create_and_open(Worker1, SessionId1, FilePath),
    {ok, {Guid2, H2}} = lfm_proxy:create_and_open(Worker2, SessionId2, FilePath),
    ok = lfm_proxy:close(Worker1, H1),
    ok = lfm_proxy:close(Worker2, H2),


    DirStoragePath = storage_test_utils:file_path(Worker1, SpaceId, DirName),
    StorageFilePath1 = filename:join([DirStoragePath, FileName]),
    Uuid = file_id:guid_to_uuid(Guid2),
    StorageFilePath2 = filename:join([DirStoragePath, ?CONFLICTING_STORAGE_FILE_NAME(FileName, Uuid)]),

    {ok, StorageFiles} = storage_test_utils:list_dir(Worker1, StorageSpacePathW1),
    ListStorageDir = fun() ->
        {ok, List} = storage_test_utils:list_dir(Worker1, DirStoragePath),
        lists:sort(List -- StorageFiles)
    end,

    ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessionId1, ?FILE_REF(Guid1)), ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessionId1, ?FILE_REF(Guid2)), ?ATTEMPTS),

    ?assertEqual(2, length(ListDir(Worker1, SessionId1, SpacePath)), ?ATTEMPTS),
    [{_, D1}, {_, D2}] = ListDir(Worker1, SessionId1, SpacePath),

    % open, write and read
    Content1 = <<"data_file1">>,
    Content2 = <<"data_file2">>,

    {ok, Handle1} = lfm_proxy:open(Worker1, SessionId1, ?FILE_REF(Guid1), rdwr),
    {ok, Handle2} = lfm_proxy:open(Worker1, SessionId1, ?FILE_REF(Guid2), rdwr),

    ExpectedStorageFileList =
        [binary_to_list(FileName), binary_to_list(?CONFLICTING_STORAGE_FILE_NAME(FileName, Uuid))],

    ?assertEqual(ExpectedStorageFileList, ListStorageDir(), ?ATTEMPTS),

    {ok, _} = lfm_proxy:write(Worker1, Handle1, 0, Content1),
    {ok, _} = lfm_proxy:write(Worker1, Handle2, 0, Content2),

    ?assertMatch({ok, Content1}, lfm_proxy:read(Worker1, Handle1, 0, byte_size(Content1)), ?ATTEMPTS),
    ?assertMatch({ok, Content2}, lfm_proxy:read(Worker1, Handle2, 0, byte_size(Content2)), ?ATTEMPTS),

    % check data on storage
    ?assertMatch({ok, Content1}, storage_test_utils:read_file(Worker1, StorageFilePath1)),
    ?assertMatch({ok, Content2}, storage_test_utils:read_file(Worker1, StorageFilePath2)),

    ?assertEqual(ok, lfm_proxy:close(Worker1, Handle1)),
    ?assertEqual(ok, lfm_proxy:close(Worker1, Handle2)),

    ?assertMatch({ok, _}, lfm_proxy:mv(Worker1, SessionId1,
        {path, <<SpacePath/binary, "/", D1/binary, "/", FileName/binary>>},
        <<SpacePath/binary, "/", D2/binary, "/test">>)),
    ?assertMatch({ok, _}, lfm_proxy:create_and_open(Worker1, SessionId1,
        <<SpacePath/binary, "/", D1/binary, "/", FileName/binary>>)),
    ?assertEqual(3, length(ListStorageDir())),

    ?assertMatch(ok, lfm_proxy:rm_recursive(Worker1, SessionId1,
        {path, <<SpacePath/binary, "/", D2/binary, "/">>})),
    ?assertEqual(1, length(ListStorageDir()), ?ATTEMPTS),
    ok = lfm_proxy:close_all(Worker1).

%%%===================================================================
%%% Test base functions
%%%===================================================================

file_with_suffix_is_deleted_from_storage_after_deletion_base(Config, ReleaseBeforeDeletion) ->
    [Worker2, Worker1] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID(Config),
    SpaceName = ?SPACE_NAME(Config),
    SessionId1 = ?SESS_ID(Worker1, Config),
    SessionId2 = ?SESS_ID(Worker2, Config),
    SpacePath = <<"/", SpaceName/binary>>,
    FileName = generator:gen_name(),
    FilePath =  filename:join([SpacePath, FileName]),
    StorageSpacePathW1 = storage_test_utils:space_path(Worker1, SpaceId),

    ListDir = fun(Worker, Session, Path) ->
        {ok, List} = lfm_proxy:get_children(Worker, Session, {path, Path}, 0, 100),
        List
    end,
    StorageFiles = case storage_test_utils:list_dir(Worker1, StorageSpacePathW1) of
        {ok, Files} -> Files;
        {error, _} -> []
    end,
    ListStorageDir = fun() ->
        {ok, List} = storage_test_utils:list_dir(Worker1, StorageSpacePathW1),
        lists:sort(List -- StorageFiles)
    end,

    % create files
    {ok, Guid1} = lfm_proxy:create(Worker1, SessionId1, FilePath),
    {ok, Guid2} = lfm_proxy:create(Worker2, SessionId2, FilePath),

    StorageFilePath1 = storage_test_utils:file_path(Worker1, SpaceId, FileName),
    Uuid = file_id:guid_to_uuid(Guid2),
    StorageFilePath2 = storage_test_utils:file_path(Worker1, SpaceId, ?CONFLICTING_STORAGE_FILE_NAME(FileName, Uuid)),

    ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessionId1, ?FILE_REF(Guid1)), ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:stat(Worker1, SessionId1, ?FILE_REF(Guid2)), ?ATTEMPTS),

    ?assertEqual(2, length(ListDir(Worker1, SessionId1, SpacePath)), ?ATTEMPTS),

    % open, write and read
    Content1 = <<"data_file1">>,
    Content2 = <<"data_file2">>,

    {ok, Handle1} = lfm_proxy:open(Worker1, SessionId1, ?FILE_REF(Guid1), rdwr),
    {ok, Handle2} = lfm_proxy:open(Worker1, SessionId1, ?FILE_REF(Guid2), rdwr),

    ExpectedStorageFileList =
        [binary_to_list(FileName), binary_to_list(?CONFLICTING_STORAGE_FILE_NAME(FileName, Uuid))],

    ?assertEqual(ExpectedStorageFileList, ListStorageDir(), ?ATTEMPTS),

    {ok, _} = lfm_proxy:write(Worker1, Handle1, 0, Content1),
    {ok, _} = lfm_proxy:write(Worker1, Handle2, 0, Content2),

    ?assertMatch({ok, Content1}, lfm_proxy:read(Worker1, Handle1, 0, byte_size(Content1)), ?ATTEMPTS),
    ?assertMatch({ok, Content2}, lfm_proxy:read(Worker1, Handle2, 0, byte_size(Content2)), ?ATTEMPTS),

    % check data on storage
    ?assertMatch({ok, Content1}, storage_test_utils:read_file(Worker1, StorageFilePath1)),
    ?assertMatch({ok, Content2}, storage_test_utils:read_file(Worker1, StorageFilePath2)),

    case ReleaseBeforeDeletion of
        true ->
            ok = lfm_proxy:close_all(Worker1),
            ok = lfm_proxy:unlink(Worker1, SessionId1, {path, FilePath});
        false ->
            ok = lfm_proxy:unlink(Worker1, SessionId1, {path, FilePath}),
            ?assertEqual(1, length(ListDir(Worker1, SessionId1, SpacePath)), ?ATTEMPTS),
            ?assertEqual(ExpectedStorageFileList, ListStorageDir(), ?ATTEMPTS),
            ok = lfm_proxy:close_all(Worker1)
    end,

    ?assertEqual([{Guid2, FileName}], ListDir(Worker1, SessionId1, SpacePath), ?ATTEMPTS),
    ?assertEqual([binary_to_list(?CONFLICTING_STORAGE_FILE_NAME(FileName, Uuid))], ListStorageDir(), ?ATTEMPTS),

    ok = lfm_proxy:unlink(Worker2, SessionId2, {path, FilePath}),

    ?assertEqual([], ListDir(Worker1, SessionId1, SpacePath), ?ATTEMPTS),
    ?assertEqual([], ListStorageDir(), ?ATTEMPTS).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = initializer:setup_storage(NewConfig),
        initializer:mock_provider_ids(NewConfig1),
        initializer:mock_auth_manager(NewConfig1),
        multi_provider_file_ops_test_base:init_env(NewConfig1)
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer]}
        | Config
    ].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config),
    initializer:unmock_auth_manager(Config),
    initializer:unmock_provider_ids(?config(op_worker_nodes, Config)),
    initializer:teardown_storage(Config).

init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, fslogic_delete, [passthrough]),
    test_utils:mock_expect(Workers, fslogic_delete, get_open_file_handling_method,
        fun(Ctx) -> {?SET_DELETION_MARKER, Ctx} end
    ),
    ct:timetrap({minutes, 60}),
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    [W2, W1 | _] = ?config(op_worker_nodes, Config),
    lfm_proxy:close_all(W1),
    lfm_proxy:close_all(W2),
    cleanup_space(Config, ?SPACE_ID(Config)),
    ensure_empty_storages(Config, ?SPACE_ID(Config)),
    lfm_proxy:teardown(Config),
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [fslogic_delete]).

cleanup_space(Config, SpaceId) ->
    [W2, W1 | _] = ?config(op_worker_nodes, Config),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    cleanup_space_children(W1, ?ROOT_SESS_ID, SpaceGuid, 1000),
    cleanup_space_children(W2, ?ROOT_SESS_ID, SpaceGuid, 1000),
    ?assertEqual({ok, []}, lfm_proxy:get_children(W1, ?ROOT_SESS_ID, ?FILE_REF(SpaceGuid), 0, 1)),
    ?assertEqual({ok, []}, lfm_proxy:get_children(W2, ?ROOT_SESS_ID, ?FILE_REF(SpaceGuid), 0, 1), ?ATTEMPTS).

ensure_empty_storages(Config, SpaceId) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(W) -> ensure_empty_storage(W, SpaceId) end, Workers).

ensure_empty_storage(Worker, SpaceId) ->
    SpacePath = storage_test_utils:space_path(Worker, SpaceId),
    ?assertEqual(true, begin
        Result = storage_test_utils:list_dir(Worker, SpacePath),
        Result =:= {ok, []} orelse Result =:= {error, ?ENOENT}
    end, ?ATTEMPTS).

cleanup_space_children(Worker, SessionId, SpaceGuid, BatchSize) ->
    {ok, Children} = lfm_proxy:get_children(Worker, SessionId, ?FILE_REF(SpaceGuid), 0, BatchSize),
    lists:foreach(fun({G, _}) ->
        lfm_proxy:rm_recursive(Worker, SessionId, ?FILE_REF(G))
    end, Children),
    case length(Children) < BatchSize of
        true -> ok;
        false -> cleanup_space_children(Worker, SessionId, SpaceGuid, BatchSize)
    end.