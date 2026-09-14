%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module tests that files, directories and their remote replicas
%%% are cleaned up (deleted or truncated) from a POSIX storage in reaction
%%% to logical file operations.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_cleanup_test_SUITE).
-author("Jakub Kudzia").

-include("env/space_setup_utils.hrl").
-include("storage/storage_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_delete.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% export for ct
-export([
    all/0, groups/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    file_should_be_deleted_from_storage_after_deletion/1,
    file_should_be_truncated_on_storage_after_truncate/1,
    directory_should_be_deleted_from_storage_after_deletion/1,
    empty_directory_should_be_deleted_from_storage_after_deletion/1,
    file_should_be_deleted_from_storage_after_releasing_handle/1,
    directory_should_be_deleted_from_storage_after_releasing_handle_to_its_child/1,
    race_on_remote_deletion_of_parent_and_child/1,

    remote_replica_should_be_deleted_from_storage_after_deletion/1,
    remote_replica_should_be_truncated_on_storage_after_truncate/1,
    remote_directory_replica_should_be_deleted_from_storage_after_deletion/1,
    empty_remote_directory_replica_should_be_deleted_from_storage_after_deletion/1,
    replica_should_be_deleted_from_storage_after_releasing_handle_to_remotely_deleted_file/1,
    parent_dir_of_replica_should_be_deleted_from_storage_after_releasing_handle_to_remotely_deleted_file/1,

    file_with_suffix_is_deleted_from_storage_after_deletion/1,
    deleted_open_file_with_suffix_is_deleted_from_storage_after_release/1,
    suffix_in_metadata_and_storage_test/1,
    suffix_in_dir_metadata_test/1
]).

groups() -> [
    {single_provider_tests, [], [
        file_should_be_deleted_from_storage_after_deletion,
        file_should_be_truncated_on_storage_after_truncate,
        directory_should_be_deleted_from_storage_after_deletion,
        empty_directory_should_be_deleted_from_storage_after_deletion,
        file_should_be_deleted_from_storage_after_releasing_handle,
        directory_should_be_deleted_from_storage_after_releasing_handle_to_its_child,
        race_on_remote_deletion_of_parent_and_child
    ]},
    {remote_replica_tests, [], [
        remote_replica_should_be_deleted_from_storage_after_deletion,
        remote_replica_should_be_truncated_on_storage_after_truncate,
        remote_directory_replica_should_be_deleted_from_storage_after_deletion,
        empty_remote_directory_replica_should_be_deleted_from_storage_after_deletion,
        replica_should_be_deleted_from_storage_after_releasing_handle_to_remotely_deleted_file,
        parent_dir_of_replica_should_be_deleted_from_storage_after_releasing_handle_to_remotely_deleted_file
    ]},
    {conflicting_files_tests, [], [
        file_with_suffix_is_deleted_from_storage_after_deletion,
        deleted_open_file_with_suffix_is_deleted_from_storage_after_release,
        suffix_in_metadata_and_storage_test,
        suffix_in_dir_metadata_test
    ]}
].

all() -> [
    {group, single_provider_tests},
    {group, remote_replica_tests},
    {group, conflicting_files_tests}
].

%% description of the environment shared by all the test cases - see describe_test_env/1
-type test_env() :: #{
    krk_node := node(),
    paris_node := node(),
    krk_session_id := session:id(),
    paris_session_id := session:id(),
    space_id := od_space:id(),
    space_guid := file_id:file_guid(),
    space_path := file_meta:path()
}.

% A file whose name conflicts with another file's - see create_conflicting_files/3.
-record(conflicting_file, {
    guid :: file_id:file_guid(),
    % handle to the file opened for reading and writing on the krakow provider
    handle :: lfm:handle(),
    % name under which the file is kept on the krakow provider's storage
    storage_name :: file_meta:name(),
    content :: binary()
}).

% storage path of the space directory itself, relative to which all the other ones are given
-define(SPACE_DIR_REL_PATH, <<>>).
-define(SUPPORT_SIZE, 1073741824).

-define(TEST_DATA, <<"test_data">>).
-define(TEST_DATA_SIZE, byte_size(?TEST_DATA)).
-define(DIR_NAME, <<"dir_", (atom_to_binary(?FUNCTION_NAME, latin1))/binary>>).
-define(FILE_NAME, <<"file_", (atom_to_binary(?FUNCTION_NAME, latin1))/binary>>).

-define(ATTEMPTS, 60).


%%%===================================================================
%%% Single provider tests
%%%===================================================================


file_should_be_deleted_from_storage_after_deletion(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),

    % when
    FileGuid = create_file(KrkNode, KrkSessId, SpaceGuid, ?FILE_NAME, ?TEST_DATA),
    ?assertStorageFileContent(KrkNode, SpaceId, ?FILE_NAME, ?TEST_DATA),

    % and
    ?assertEqual(ok, lfm_proxy:unlink(KrkNode, KrkSessId, ?FILE_REF(FileGuid))),

    % then
    ?assertNoStorageFile(KrkNode, SpaceId, ?FILE_NAME).


file_should_be_truncated_on_storage_after_truncate(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    SizeAfterTruncate = ?TEST_DATA_SIZE - 5,
    ExpDataAfterTruncate = binary_part(?TEST_DATA, 0, SizeAfterTruncate),

    % when
    FileGuid = create_file(KrkNode, KrkSessId, SpaceGuid, ?FILE_NAME, ?TEST_DATA),
    ?assertStorageFileContent(KrkNode, SpaceId, ?FILE_NAME, ?TEST_DATA),

    % and
    ?assertEqual(ok, lfm_proxy:truncate(KrkNode, KrkSessId, ?FILE_REF(FileGuid), SizeAfterTruncate)),

    % then
    ?assertStorageFileContent(KrkNode, SpaceId, ?FILE_NAME, ExpDataAfterTruncate).


directory_should_be_deleted_from_storage_after_deletion(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    FileRelPath = filename:join(?DIR_NAME, ?FILE_NAME),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(KrkNode, KrkSessId, SpaceGuid, ?DIR_NAME, ?DEFAULT_DIR_PERMS),
    create_file(KrkNode, KrkSessId, DirGuid, ?FILE_NAME, ?TEST_DATA),
    ?assertStorageDirChildren(KrkNode, SpaceId, ?DIR_NAME, [?FILE_NAME]),
    ?assertStorageFileContent(KrkNode, SpaceId, FileRelPath, ?TEST_DATA),

    % and
    ?assertEqual(ok, lfm_proxy:rm_recursive(KrkNode, KrkSessId, ?FILE_REF(DirGuid))),

    % then
    ?assertNoStorageFile(KrkNode, SpaceId, FileRelPath),
    ?assertNoStorageDir(KrkNode, SpaceId, ?DIR_NAME).


empty_directory_should_be_deleted_from_storage_after_deletion(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    FileRelPath = filename:join(?DIR_NAME, ?FILE_NAME),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(KrkNode, KrkSessId, SpaceGuid, ?DIR_NAME, ?DEFAULT_DIR_PERMS),
    % the file is created so that the directory is created on storage as well
    FileGuid = create_file(KrkNode, KrkSessId, DirGuid, ?FILE_NAME, ?TEST_DATA),
    ?assertStorageDirChildren(KrkNode, SpaceId, ?DIR_NAME, [?FILE_NAME]),
    ?assertStorageFileContent(KrkNode, SpaceId, FileRelPath, ?TEST_DATA),

    % remove the file to leave an empty directory on storage
    ?assertEqual(ok, lfm_proxy:unlink(KrkNode, KrkSessId, ?FILE_REF(FileGuid))),
    ?assertNoStorageFile(KrkNode, SpaceId, FileRelPath),
    ?assertStorageDirChildren(KrkNode, SpaceId, ?DIR_NAME, []),

    % and
    ?assertEqual(ok, lfm_proxy:rm_recursive(KrkNode, KrkSessId, ?FILE_REF(DirGuid))),

    % then
    ?assertNoStorageDir(KrkNode, SpaceId, ?DIR_NAME).


file_should_be_deleted_from_storage_after_releasing_handle(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    mock_opened_file_deletion_to_use_deletion_marker(),

    % when
    {FileGuid, FileHandle} = create_file_and_keep_open(KrkNode, KrkSessId, SpaceGuid, ?FILE_NAME, ?TEST_DATA),
    ?assertStorageFileContent(KrkNode, SpaceId, ?FILE_NAME, ?TEST_DATA),

    % and
    ?assertEqual(ok, lfm_proxy:unlink(KrkNode, KrkSessId, ?FILE_REF(FileGuid))),
    % the file must stay on storage as long as it is opened
    ?assertStorageFileContent(KrkNode, SpaceId, ?FILE_NAME, ?TEST_DATA, 1),
    ok = lfm_proxy:close(KrkNode, FileHandle),

    % then
    ?assertNoStorageFile(KrkNode, SpaceId, ?FILE_NAME).


directory_should_be_deleted_from_storage_after_releasing_handle_to_its_child(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    FileRelPath = filename:join(?DIR_NAME, ?FILE_NAME),
    mock_opened_file_deletion_to_use_deletion_marker(),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(KrkNode, KrkSessId, SpaceGuid, ?DIR_NAME, ?DEFAULT_DIR_PERMS),
    {FileGuid, FileHandle} = create_file_and_keep_open(KrkNode, KrkSessId, DirGuid, ?FILE_NAME, ?TEST_DATA),
    ?assertStorageDirChildren(KrkNode, SpaceId, ?DIR_NAME, [?FILE_NAME]),
    ?assertStorageFileContent(KrkNode, SpaceId, FileRelPath, ?TEST_DATA),

    % and
    ?assertEqual(ok, lfm_proxy:rm_recursive(KrkNode, KrkSessId, ?FILE_REF(DirGuid))),

    % then the files are gone from the logical file system ...
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(KrkNode, KrkSessId, ?FILE_REF(DirGuid)), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_children(KrkNode, KrkSessId, ?FILE_REF(DirGuid), 0, 1), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(KrkNode, KrkSessId, ?FILE_REF(FileGuid)), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:open(KrkNode, KrkSessId, ?FILE_REF(FileGuid), read), ?ATTEMPTS),

    % ... but not from the storage, as the file is still opened
    ?assertStorageDirChildren(KrkNode, SpaceId, ?DIR_NAME, [?FILE_NAME], 1),
    ?assertStorageFileContent(KrkNode, SpaceId, FileRelPath, ?TEST_DATA, 1),

    % and
    ok = lfm_proxy:close(KrkNode, FileHandle),

    % then
    ?assertNoStorageFile(KrkNode, SpaceId, FileRelPath),
    ?assertNoStorageDir(KrkNode, SpaceId, ?DIR_NAME).


race_on_remote_deletion_of_parent_and_child(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = describe_test_env(Config),
    FileRelPath = filename:join(?DIR_NAME, ?FILE_NAME),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(KrkNode, KrkSessId, SpaceGuid, ?DIR_NAME, ?DEFAULT_DIR_PERMS),
    FileGuid = create_file(KrkNode, KrkSessId, DirGuid, ?FILE_NAME, ?TEST_DATA),
    ?assertStorageDirChildren(KrkNode, SpaceId, ?DIR_NAME, [?FILE_NAME]),
    ?assertStorageFileContent(KrkNode, SpaceId, FileRelPath, ?TEST_DATA),

    % and
    DirUuid = file_id:guid_to_uuid(DirGuid),
    FileUuid = file_id:guid_to_uuid(FileGuid),

    % pretend that the files were deleted
    ?assertEqual(ok, rpc:call(KrkNode, file_meta, delete, [FileUuid])),
    ?assertEqual(ok, rpc:call(KrkNode, file_meta, delete, [DirUuid])),

    % pretend that the directory doc is synchronized before its child doc
    {ok, DirDoc} = rpc:call(KrkNode, file_meta, get_including_deleted, [DirUuid]),
    {ok, FileDoc} = rpc:call(KrkNode, file_meta, get_including_deleted, [FileUuid]),
    ok = rpc:call(KrkNode, dbsync_events, change_replicated, [SpaceId, DirDoc]),

    % the directory must not be deleted from storage as long as it still holds the child
    % (whose deletion is withheld until after the assertion); the provider is given time
    % to (incorrectly) react to the parent's deletion first, as right after the deletion
    % the directory would still be there even if the provider did
    timer:sleep(timer:seconds(5)),
    ?assertStorageDirChildren(KrkNode, SpaceId, ?DIR_NAME, [?FILE_NAME], 1),

    ok = rpc:call(KrkNode, dbsync_events, change_replicated, [SpaceId, FileDoc]),

    % then
    ?assertNoStorageFile(KrkNode, SpaceId, FileRelPath),
    ?assertNoStorageDir(KrkNode, SpaceId, ?DIR_NAME).


%%%===================================================================
%%% Remote replica tests
%%%===================================================================


remote_replica_should_be_deleted_from_storage_after_deletion(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId, paris_node := ParisNode,
        space_id := SpaceId, space_guid := SpaceGuid
    } = TestEnv = describe_test_env(Config),

    % when
    FileGuid = create_file_and_replicate_to_paris(TestEnv, SpaceGuid, ?FILE_NAME, ?TEST_DATA),
    ?assertStorageFileContent(ParisNode, SpaceId, ?FILE_NAME, ?TEST_DATA),

    % and
    ?assertEqual(ok, lfm_proxy:unlink(KrkNode, KrkSessId, ?FILE_REF(FileGuid))),

    % then
    ?assertNoStorageFile(ParisNode, SpaceId, ?FILE_NAME).


remote_replica_should_be_truncated_on_storage_after_truncate(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId, paris_node := ParisNode,
        space_id := SpaceId, space_guid := SpaceGuid
    } = TestEnv = describe_test_env(Config),
    SizeAfterTruncate = ?TEST_DATA_SIZE - 5,
    ExpDataAfterTruncate = binary_part(?TEST_DATA, 0, SizeAfterTruncate),

    % when
    FileGuid = create_file_and_replicate_to_paris(TestEnv, SpaceGuid, ?FILE_NAME, ?TEST_DATA),
    ?assertStorageFileContent(ParisNode, SpaceId, ?FILE_NAME, ?TEST_DATA),

    % and
    ?assertEqual(ok, lfm_proxy:truncate(KrkNode, KrkSessId, ?FILE_REF(FileGuid), SizeAfterTruncate)),

    % then
    ?assertStorageFileContent(ParisNode, SpaceId, ?FILE_NAME, ExpDataAfterTruncate).


remote_directory_replica_should_be_deleted_from_storage_after_deletion(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId, paris_node := ParisNode,
        space_id := SpaceId, space_guid := SpaceGuid
    } = TestEnv = describe_test_env(Config),
    FileRelPath = filename:join(?DIR_NAME, ?FILE_NAME),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(KrkNode, KrkSessId, SpaceGuid, ?DIR_NAME, ?DEFAULT_DIR_PERMS),
    create_file_and_replicate_to_paris(TestEnv, DirGuid, ?FILE_NAME, ?TEST_DATA),
    ?assertStorageDirChildren(ParisNode, SpaceId, ?DIR_NAME, [?FILE_NAME]),
    ?assertStorageFileContent(ParisNode, SpaceId, FileRelPath, ?TEST_DATA),

    % and
    ?assertEqual(ok, lfm_proxy:rm_recursive(KrkNode, KrkSessId, ?FILE_REF(DirGuid))),

    % then
    ?assertNoStorageFile(ParisNode, SpaceId, FileRelPath),
    ?assertNoStorageDir(ParisNode, SpaceId, ?DIR_NAME).


empty_remote_directory_replica_should_be_deleted_from_storage_after_deletion(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId, paris_node := ParisNode,
        space_id := SpaceId, space_guid := SpaceGuid
    } = TestEnv = describe_test_env(Config),
    FileRelPath = filename:join(?DIR_NAME, ?FILE_NAME),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(KrkNode, KrkSessId, SpaceGuid, ?DIR_NAME, ?DEFAULT_DIR_PERMS),
    % the file is created so that the directory is created on both storages as well
    FileGuid = create_file_and_replicate_to_paris(TestEnv, DirGuid, ?FILE_NAME, ?TEST_DATA),
    ?assertStorageDirChildren(ParisNode, SpaceId, ?DIR_NAME, [?FILE_NAME]),
    ?assertStorageFileContent(ParisNode, SpaceId, FileRelPath, ?TEST_DATA),

    % delete the file to leave an empty directory on storage
    ?assertEqual(ok, lfm_proxy:unlink(KrkNode, KrkSessId, ?FILE_REF(FileGuid))),
    ?assertNoStorageFile(ParisNode, SpaceId, FileRelPath),
    ?assertStorageDirChildren(ParisNode, SpaceId, ?DIR_NAME, []),

    % and
    ?assertEqual(ok, lfm_proxy:rm_recursive(KrkNode, KrkSessId, ?FILE_REF(DirGuid))),

    % then
    ?assertNoStorageDir(ParisNode, SpaceId, ?DIR_NAME).


replica_should_be_deleted_from_storage_after_releasing_handle_to_remotely_deleted_file(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId,
        paris_node := ParisNode, paris_session_id := ParisSessId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = TestEnv = describe_test_env(Config),
    mock_opened_file_deletion_to_use_deletion_marker(),

    % when - the file is intentionally left opened on the paris provider
    {FileGuid, ParisHandle} = create_file_and_replicate_to_paris_keeping_handle(
        TestEnv, SpaceGuid, ?FILE_NAME, ?TEST_DATA
    ),
    ?assertStorageFileContent(ParisNode, SpaceId, ?FILE_NAME, ?TEST_DATA),

    % and
    ?assertEqual(ok, lfm_proxy:unlink(KrkNode, KrkSessId, ?FILE_REF(FileGuid))),

    % then the file is gone from the provider that deleted it ...
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(KrkNode, KrkSessId, ?FILE_REF(FileGuid))),
    ?assertNoStorageFile(KrkNode, SpaceId, ?FILE_NAME),

    % ... and, once the deletion is synchronized, from the logical file system of the other one ...
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(ParisNode, ParisSessId, ?FILE_REF(FileGuid)), ?ATTEMPTS),

    % ... but not from its storage, as the file is still opened there
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(ParisNode, ParisHandle, 0, ?TEST_DATA_SIZE), ?ATTEMPTS),
    ?assertStorageFileContent(ParisNode, SpaceId, ?FILE_NAME, ?TEST_DATA, 1),

    % and
    ok = lfm_proxy:close(ParisNode, ParisHandle),

    % then
    ?assertNoStorageFile(ParisNode, SpaceId, ?FILE_NAME).


parent_dir_of_replica_should_be_deleted_from_storage_after_releasing_handle_to_remotely_deleted_file(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId,
        paris_node := ParisNode, paris_session_id := ParisSessId,
        space_id := SpaceId, space_guid := SpaceGuid
    } = TestEnv = describe_test_env(Config),
    FileRelPath = filename:join(?DIR_NAME, ?FILE_NAME),
    mock_opened_file_deletion_to_use_deletion_marker(),

    % when - the file is intentionally left opened on the paris provider
    {ok, DirGuid} = lfm_proxy:mkdir(KrkNode, KrkSessId, SpaceGuid, ?DIR_NAME, ?DEFAULT_DIR_PERMS),
    {FileGuid, ParisHandle} = create_file_and_replicate_to_paris_keeping_handle(
        TestEnv, DirGuid, ?FILE_NAME, ?TEST_DATA
    ),
    ?assertStorageFileContent(ParisNode, SpaceId, FileRelPath, ?TEST_DATA),

    % and
    ?assertEqual(ok, lfm_proxy:rm_recursive(KrkNode, KrkSessId, ?FILE_REF(DirGuid))),

    % then the directory is gone from the provider that deleted it ...
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(KrkNode, KrkSessId, ?FILE_REF(DirGuid)), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(KrkNode, KrkSessId, ?FILE_REF(FileGuid)), ?ATTEMPTS),
    ?assertNoStorageFile(KrkNode, SpaceId, FileRelPath),
    ?assertNoStorageDir(KrkNode, SpaceId, ?DIR_NAME),

    % ... and, once the deletion is synchronized, from the logical file system of the other one ...
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(ParisNode, ParisSessId, ?FILE_REF(DirGuid)), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(ParisNode, ParisSessId, ?FILE_REF(FileGuid)), ?ATTEMPTS),

    % ... but not from its storage, as the file is still opened there
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(ParisNode, ParisHandle, 0, ?TEST_DATA_SIZE), ?ATTEMPTS),
    ?assertStorageFileContent(ParisNode, SpaceId, FileRelPath, ?TEST_DATA, 1),
    ?assertStorageDirChildren(ParisNode, SpaceId, ?DIR_NAME, [?FILE_NAME], 1),

    % and
    ok = lfm_proxy:close(ParisNode, ParisHandle),

    % then - after closing the file it is deleted, as well as its parent directory
    ?assertNoStorageFile(ParisNode, SpaceId, FileRelPath),
    ?assertNoStorageDir(ParisNode, SpaceId, ?DIR_NAME).


%%%===================================================================
%%% Conflicting files (storage suffix) tests
%%%===================================================================


file_with_suffix_is_deleted_from_storage_after_deletion(Config) ->
    file_with_suffix_is_deleted_from_storage_base(describe_test_env(Config), release_before_deletion).

deleted_open_file_with_suffix_is_deleted_from_storage_after_release(Config) ->
    mock_opened_file_deletion_to_use_deletion_marker(),
    file_with_suffix_is_deleted_from_storage_base(describe_test_env(Config), delete_before_release).

suffix_in_metadata_and_storage_test(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId,
        space_id := SpaceId, space_path := SpacePath
    } = TestEnv = describe_test_env(Config),
    FileName = generator:gen_name(),

    % when
    {
        #conflicting_file{guid = LocalGuid, handle = LocalHandle},
        #conflicting_file{storage_name = RemoteStorageName}
    } = create_conflicting_files(TestEnv, [], FileName),

    ?assertEqual(2, length(ls(KrkNode, KrkSessId, SpacePath)), ?ATTEMPTS),
    ?assertStorageDirChildren(KrkNode, SpaceId, ?SPACE_DIR_REL_PATH, [FileName, RemoteStorageName]),

    % and
    ?assertEqual(ok, lfm_proxy:close(KrkNode, LocalHandle)),
    ?assertEqual(ok, lfm_proxy:unlink(KrkNode, KrkSessId, ?FILE_REF(LocalGuid))),

    % then the file created on the remote provider takes over the plain name in the
    % logical file system, but keeps its suffix on storage, where it is not renamed
    ?assertEqual([FileName], [Name || {_Guid, Name} <- ls(KrkNode, KrkSessId, SpacePath)], ?ATTEMPTS),
    ?assertStorageDirChildren(KrkNode, SpaceId, ?SPACE_DIR_REL_PATH, [RemoteStorageName]),

    % and
    ok = lfm_proxy:close_all(KrkNode),
    ok = lfm_proxy:unlink(KrkNode, KrkSessId, {path, filename:join(SpacePath, FileName)}),

    % then
    ?assertEqual([], ls(KrkNode, KrkSessId, SpacePath), ?ATTEMPTS),
    ?assertStorageDirChildren(KrkNode, SpaceId, ?SPACE_DIR_REL_PATH, []).

suffix_in_dir_metadata_test(Config) ->
    #{
        krk_node := KrkNode, krk_session_id := KrkSessId,
        paris_node := ParisNode, paris_session_id := ParisSessId,
        space_id := SpaceId, space_path := SpacePath
    } = TestEnv = describe_test_env(Config),
    DirName = generator:gen_name(),
    FileName = generator:gen_name(),
    DirPath = filename:join(SpacePath, DirName),

    % when - two conflicting directories are created, one on each provider; both of them
    % are backed by the same directory on the krakow provider's storage, so it is only
    % the files inside them that get a storage suffix
    {ok, LocalDirGuid} = lfm_proxy:mkdir(KrkNode, KrkSessId, DirPath),
    {ok, RemoteDirGuid} = lfm_proxy:mkdir(ParisNode, ParisSessId, DirPath),
    ?assertEqual(2, length(ls(KrkNode, KrkSessId, SpacePath)), ?ATTEMPTS),
    % both directories are seen under the requested name, one of them with a conflict
    % suffix - which one it is depends on the providers' ids, hence they are told apart
    % by their guids rather than by the order in which they are listed
    SpaceChildren = ls(KrkNode, KrkSessId, SpacePath),
    {_, LocalDirName} = lists:keyfind(LocalDirGuid, 1, SpaceChildren),
    {_, RemoteDirName} = lists:keyfind(RemoteDirGuid, 1, SpaceChildren),

    {_, #conflicting_file{storage_name = RemoteStorageName}} =
        create_conflicting_files(TestEnv, [DirName], FileName),
    ?assertStorageDirChildren(KrkNode, SpaceId, DirName, [FileName, RemoteStorageName]),

    % and - the local file is moved to the conflicting directory and recreated in place
    ok = lfm_proxy:close_all(KrkNode),
    ?assertMatch({ok, _}, lfm_proxy:mv(KrkNode, KrkSessId,
        {path, filename:join([SpacePath, LocalDirName, FileName])},
        filename:join([SpacePath, RemoteDirName, <<"test">>])
    )),
    ?assertMatch({ok, _}, lfm_proxy:create_and_open(KrkNode, KrkSessId,
        filename:join([SpacePath, LocalDirName, FileName])
    )),

    % then
    ?assertStorageDirChildren(KrkNode, SpaceId, DirName, [FileName, <<"test">>, RemoteStorageName]),

    % and - removing the conflicting directory takes away everything that belongs to it
    ?assertMatch(ok, lfm_proxy:rm_recursive(KrkNode, KrkSessId,
        {path, filename:join(SpacePath, RemoteDirName)}
    )),

    % then
    ?assertStorageDirChildren(KrkNode, SpaceId, DirName, [FileName]),
    ok = lfm_proxy:close_all(KrkNode).

%%%===================================================================
%%% Test base functions
%%%===================================================================

%% @private
-spec file_with_suffix_is_deleted_from_storage_base(
    test_env(), release_before_deletion | delete_before_release
) ->
    ok.
file_with_suffix_is_deleted_from_storage_base(TestEnv = #{
    krk_node := KrkNode, krk_session_id := KrkSessId,
    paris_node := ParisNode, paris_session_id := ParisSessId,
    space_id := SpaceId, space_path := SpacePath
}, DeletionOrder) ->
    FileName = generator:gen_name(),
    FilePath = filename:join(SpacePath, FileName),

    % when
    {
        #conflicting_file{storage_name = LocalStorageName},
        #conflicting_file{guid = RemoteGuid, storage_name = RemoteStorageName}
    } = create_conflicting_files(TestEnv, [], FileName),

    ?assertEqual(2, length(ls(KrkNode, KrkSessId, SpacePath)), ?ATTEMPTS),
    ?assertStorageDirChildren(KrkNode, SpaceId, ?SPACE_DIR_REL_PATH, [LocalStorageName, RemoteStorageName]),

    % and
    case DeletionOrder of
        release_before_deletion ->
            ok = lfm_proxy:close_all(KrkNode),
            ok = lfm_proxy:unlink(KrkNode, KrkSessId, {path, FilePath});
        delete_before_release ->
            ok = lfm_proxy:unlink(KrkNode, KrkSessId, {path, FilePath}),
            % the deleted file is gone from the logical file system, but stays on
            % storage until its handle is released
            ?assertEqual(1, length(ls(KrkNode, KrkSessId, SpacePath)), ?ATTEMPTS),
            ?assertStorageDirChildren(
                KrkNode, SpaceId, ?SPACE_DIR_REL_PATH, [LocalStorageName, RemoteStorageName], 1
            ),
            ok = lfm_proxy:close_all(KrkNode)
    end,

    % then only the file created on the remote provider is left - it takes over the plain
    % name in the logical file system, but keeps its suffix on storage
    ?assertEqual([{RemoteGuid, FileName}], ls(KrkNode, KrkSessId, SpacePath), ?ATTEMPTS),
    ?assertStorageDirChildren(KrkNode, SpaceId, ?SPACE_DIR_REL_PATH, [RemoteStorageName]),

    % and
    ok = lfm_proxy:unlink(ParisNode, ParisSessId, {path, FilePath}),

    % then
    ?assertEqual([], ls(KrkNode, KrkSessId, SpacePath), ?ATTEMPTS),
    ?assertStorageDirChildren(KrkNode, SpaceId, ?SPACE_DIR_REL_PATH, []).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    opt:init_per_suite([{?LOAD_MODULES, [?MODULE]} | Config], #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}],
        posthook = fun(NewConfig) ->
            space_setup_utils:clean_up_after_previous_run(all_test_cases(), [krakow, paris]),
            NewConfig
        end
    }).

end_per_suite(_Config) ->
    oct_background:end_per_suite().

init_per_testcase(Case, Config) ->
    ct:timetrap({minutes, 10}),
    SpaceId = space_setup_utils:set_up_space(#space_spec{
        name = Case,
        owner = user1,
        supports = [
            #support_spec{
                provider = krakow, storage_spec = create_posix_storage(krakow), size = ?SUPPORT_SIZE
            },
            #support_spec{
                provider = paris, storage_spec = create_posix_storage(paris), size = ?SUPPORT_SIZE
            }
        ]
    }),
    lfm_proxy:init([{space_id, SpaceId}, {space_name, atom_to_binary(Case)} | Config]).

end_per_testcase(_Case, Config) ->
    % NOTE: the space and the storages are deliberately left behind - they are named
    % after the test case and disposed of by the next run (see init_per_suite/1), which
    % keeps whatever a failing case left for inspection. Nothing has to be cleaned up
    % for the sake of the following cases either, as each of them gets a space and a
    % pair of storages of its own.
    #{krk_node := KrkNode, paris_node := ParisNode} = describe_test_env(Config),
    Nodes = [KrkNode, ParisNode],
    lists:foreach(fun(Node) -> lfm_proxy:close_all(Node) end, Nodes),
    lfm_proxy:teardown(Config),
    test_utils:mock_unload(Nodes, [fslogic_delete]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
%% @doc
%% Flat list of all the test cases. Each of them runs in a space of its own, named after
%% the case, hence this is also the list of spaces left behind by a previous run.
%% @end
-spec all_test_cases() -> [atom()].
all_test_cases() ->
    lists:flatmap(fun({_GroupName, _Opts, Cases}) -> Cases end, groups()).

%% @private
%% @doc
%% All the test cases share the same environment - a space set up for the test case
%% (see init_per_testcase/2) and supported by two providers, out of which krakow is
%% always the one that creates and deletes the files, and paris the one that keeps
%% their remote replicas.
%% @end
-spec describe_test_env(test_config:config()) -> test_env().
describe_test_env(Config) ->
    SpaceId = ?config(space_id, Config),
    SpaceName = ?config(space_name, Config),
    #{
        krk_node => oct_background:get_random_provider_node(krakow),
        paris_node => oct_background:get_random_provider_node(paris),
        krk_session_id => oct_background:get_user_session_id(user1, krakow),
        paris_session_id => oct_background:get_user_session_id(user1, paris),
        space_id => SpaceId,
        space_guid => space_dir:guid(SpaceId),
        space_path => <<"/", SpaceName/binary>>
    }.

%% @private
%% @doc
%% Every test case gets a storage of its own, so that it always starts with an empty one
%% and whatever it happens to leave behind cannot affect any other case.
%% @end
-spec create_posix_storage(oct_background:entity_selector()) -> storage:id().
create_posix_storage(ProviderSelector) ->
    space_setup_utils:create_storage(ProviderSelector, #posix_storage_params{
        mount_point = <<"/mnt/st_", (generator:gen_name())/binary>>
    }).

%% @private
%% @doc
%% Forces the deletion marker method of handling deletion of an opened file, so that the
%% file is left on storage under its original path until the handle is released. Without
%% it, a POSIX storage would use the rename method instead (see fslogic_delete) and move
%% the file to a hidden directory the moment it is deleted.
%% @end
-spec mock_opened_file_deletion_to_use_deletion_marker() -> ok.
mock_opened_file_deletion_to_use_deletion_marker() ->
    Nodes = oct_background:get_all_providers_nodes(),
    ok = test_utils:mock_new(Nodes, fslogic_delete, [passthrough]),
    ok = test_utils:mock_expect(Nodes, fslogic_delete, get_open_file_handling_method, fun(FileCtx) ->
        {?SET_DELETION_MARKER, FileCtx}
    end).

%% @private
-spec create_file(node(), session:id(), file_id:file_guid(), file_meta:name(), binary()) ->
    file_id:file_guid().
create_file(Node, SessionId, ParentGuid, Name, Content) ->
    {FileGuid, Handle} = create_file_and_keep_open(Node, SessionId, ParentGuid, Name, Content),
    ok = lfm_proxy:close(Node, Handle),
    FileGuid.

%% @private
%% @doc
%% Like create_file/5, but the write handle is left open - required by the test cases
%% that check how deletion of an opened file is reflected on the storage.
%% @end
-spec create_file_and_keep_open(node(), session:id(), file_id:file_guid(), file_meta:name(), binary()) ->
    {file_id:file_guid(), lfm:handle()}.
create_file_and_keep_open(Node, SessionId, ParentGuid, Name, Content) ->
    {ok, FileGuid} = lfm_proxy:create(Node, SessionId, ParentGuid, Name, ?DEFAULT_FILE_PERMS),
    {ok, Handle} = lfm_proxy:open(Node, SessionId, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(Node, Handle, 0, Content),
    ok = lfm_proxy:fsync(Node, Handle),
    {FileGuid, Handle}.

%% @private
%% @doc
%% Creates a file on the krakow provider and pulls its replica onto the paris provider
%% by reading the file there.
%% @end
-spec create_file_and_replicate_to_paris(test_env(), file_id:file_guid(), file_meta:name(), binary()) ->
    file_id:file_guid().
create_file_and_replicate_to_paris(TestEnv = #{paris_node := ParisNode}, ParentGuid, Name, Content) ->
    {FileGuid, ParisHandle} = create_file_and_replicate_to_paris_keeping_handle(
        TestEnv, ParentGuid, Name, Content
    ),
    ok = lfm_proxy:close(ParisNode, ParisHandle),
    FileGuid.

%% @private
%% @doc
%% Like create_file_and_replicate_to_paris/4, but the read handle opened on the paris
%% provider is left open.
%% @end
-spec create_file_and_replicate_to_paris_keeping_handle(
    test_env(), file_id:file_guid(), file_meta:name(), binary()
) ->
    {file_id:file_guid(), lfm:handle()}.
create_file_and_replicate_to_paris_keeping_handle(#{
    krk_node := KrkNode, krk_session_id := KrkSessId,
    paris_node := ParisNode, paris_session_id := ParisSessId
}, ParentGuid, Name, Content) ->
    FileGuid = create_file(KrkNode, KrkSessId, ParentGuid, Name, Content),
    {ok, ParisHandle} = ?assertMatch({ok, _},
        lfm_proxy:open(ParisNode, ParisSessId, ?FILE_REF(FileGuid), read), ?ATTEMPTS),
    ?assertMatch({ok, Content},
        lfm_proxy:read(ParisNode, ParisHandle, 0, byte_size(Content)), ?ATTEMPTS),
    {FileGuid, ParisHandle}.

%% @private
%% @doc
%% Creates two files with the same name under the given parent (identified by its path
%% segments relative to the space directory) - one on each provider - which makes them
%% conflict. Both are then opened for reading and writing on the krakow provider and
%% written with distinct content, which is verified on its storage: the file created
%% locally keeps the plain name there, while the one created on the paris provider gets
%% a storage suffix. The returned handles are left open.
%% @end
-spec create_conflicting_files(test_env(), [file_meta:name()], file_meta:name()) ->
    {#conflicting_file{}, #conflicting_file{}}.
create_conflicting_files(#{
    krk_node := KrkNode, krk_session_id := KrkSessId,
    paris_node := ParisNode, paris_session_id := ParisSessId,
    space_id := SpaceId, space_path := SpacePath
}, ParentRelPathSegments, Name) ->
    FilePath = filename:join([SpacePath | ParentRelPathSegments] ++ [Name]),
    {ok, LocalGuid} = lfm_proxy:create(KrkNode, KrkSessId, FilePath),
    {ok, RemoteGuid} = lfm_proxy:create(ParisNode, ParisSessId, FilePath),

    ?assertMatch({ok, _}, lfm_proxy:stat(KrkNode, KrkSessId, ?FILE_REF(LocalGuid)), ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:stat(KrkNode, KrkSessId, ?FILE_REF(RemoteGuid)), ?ATTEMPTS),

    {ok, LocalHandle} = lfm_proxy:open(KrkNode, KrkSessId, ?FILE_REF(LocalGuid), rdwr),
    {ok, RemoteHandle} = lfm_proxy:open(KrkNode, KrkSessId, ?FILE_REF(RemoteGuid), rdwr),

    RemoteUuid = file_id:guid_to_uuid(RemoteGuid),
    LocalFile = #conflicting_file{
        guid = LocalGuid, handle = LocalHandle,
        storage_name = Name, content = <<"data_file1">>
    },
    RemoteFile = #conflicting_file{
        guid = RemoteGuid, handle = RemoteHandle,
        storage_name = ?CONFLICTING_STORAGE_FILE_NAME(Name, RemoteUuid),
        content = <<"data_file2">>
    },

    lists:foreach(fun(#conflicting_file{handle = Handle, storage_name = StorageName, content = Content}) ->
        {ok, _} = lfm_proxy:write(KrkNode, Handle, 0, Content),
        ?assertMatch({ok, Content}, lfm_proxy:read(KrkNode, Handle, 0, byte_size(Content)), ?ATTEMPTS),
        StorageRelPath = filename:join(ParentRelPathSegments ++ [StorageName]),
        ?assertStorageFileContent(KrkNode, SpaceId, StorageRelPath, Content)
    end, [LocalFile, RemoteFile]),

    {LocalFile, RemoteFile}.

%% @private
-spec ls(node(), session:id(), file_meta:path()) -> [{file_id:file_guid(), file_meta:name()}].
ls(Node, SessionId, Path) ->
    {ok, Children} = lfm_proxy:get_children(Node, SessionId, {path, Path}, 0, 100),
    Children.
