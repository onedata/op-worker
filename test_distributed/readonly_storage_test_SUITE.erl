%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of operations on files in space
%%% that is supported by a readonly storage.
%%%
%%% ATTENTION !!!
%%% Provider1 has 2 storages (docker volumes) which are mounted
%%% UNDER THE SAME PATH on host machine.
%%% One storage (the one that supports the space) is readonly
%%% and files cannot be created on it using helpers.
%%% Seconds one does not support any space and is used only to create
%%% files on the storage using helpers. Thanks to the fact that both
%%% docker volumes point to the same directory on the host machine, files
%%% created on 2nd storage appear on the 1st too.
%%% ATTENTION2 !!!
%%% Provider1's storage which is marked as readonly is not mounted
%%% as a readonly volume INTENTIONALLY.
%%% Thanks to that we may be sure that files on the readonly (in Onedata context)
%%% storage were not modified thanks to provider logic, not thanks to
%%% filesystem restrictions in the docker container.
%%% @end
%%%-------------------------------------------------------------------
-module(readonly_storage_test_SUITE).
-author("Jakub Kudzia").

-include("modules/storage/helpers/helpers.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("distribution_assert.hrl").
-include("lfm_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    create_should_fail/1,
    create_and_open_should_fail/1,
    mkdir_should_fail/1,
    read_should_succeed/1,
    write_should_fail/1,
    chmod_should_succeed_but_not_change_mode_on_storage/1,
    rename_should_fail/1,
    mv_should_fail/1,
    unlink_should_succeed_but_should_leave_files_on_storage/1,
    recursive_rm_should_succeed_but_should_leave_files_on_storage/1,
    truncate_should_fail/1,
    remote_chmod_should_not_change_mode_on_storage/1,
    remote_rename_should_not_rename_file_on_storage/1,
    remote_move_should_not_rename_file_on_storage/1,
    remote_unlink_should_not_trigger_unlinking_files_on_local_storage/1,
    remote_recursive_rm_should_not_trigger_removal_of_files_on_local_storage/1,
    remote_truncate_should_not_trigger_truncate_on_storage/1,
    replication_on_the_fly_should_fail/1,
    remote_change_should_invalidate_local_file_but_leave_storage_file_unchanged/1,
    remote_change_should_invalidate_local_file_but_leave_storage_file_unchanged2/1,
    replication_job_should_fail/1,
    eviction_job_should_succeed/1,
    migration_job_should_fail/1]).

%% test data
-define(USER1, <<"user1">>).
-define(SPACE_ID, <<"space1">>).
-define(RO_STORAGE_ID, <<"mntst1_ro">>).
-define(RW_STORAGE_ID, <<"mntst1_rdwr">>).
-define(SPACE_NAME, <<"space_name1">>).
-define(SPACE_PATH, ?PATH(<<"">>)).
-define(SESS_ID(W, Config), ?SESS_ID(?USER1, W, Config)).
-define(PATH(FileRelativePath), filepath_utils:join([<<"/">>, ?SPACE_NAME, FileRelativePath])).
-define(FILE_NAME, <<"file_", (?RAND_NAME)/binary>>).
-define(DIR_NAME, <<"dir_", (?RAND_NAME)/binary>>).
-define(RAND_NAME,
    <<(str_utils:to_binary(?FUNCTION_NAME))/binary, "_", (integer_to_binary(rand:uniform(?RAND_RANGE)))/binary>>).
-define(RAND_RANGE, 1000000000).
-define(TEST_DATA, <<"abcdefgh">>).
-define(TEST_DATA2, <<"0123456789">>).
-define(ATTEMPTS, 30).

%%%===================================================================
%%% API
%%%===================================================================

all() -> [
    create_should_fail,
    create_and_open_should_fail,
    mkdir_should_fail,
    read_should_succeed,
    write_should_fail,
    chmod_should_succeed_but_not_change_mode_on_storage,
    rename_should_fail,
    mv_should_fail,
    unlink_should_succeed_but_should_leave_files_on_storage,
    recursive_rm_should_succeed_but_should_leave_files_on_storage,
    truncate_should_fail,
    remote_chmod_should_not_change_mode_on_storage,
    remote_rename_should_not_rename_file_on_storage,
    remote_move_should_not_rename_file_on_storage,
    remote_unlink_should_not_trigger_unlinking_files_on_local_storage,
    remote_recursive_rm_should_not_trigger_removal_of_files_on_local_storage,
    remote_truncate_should_not_trigger_truncate_on_storage,
    replication_on_the_fly_should_fail,
    remote_change_should_invalidate_local_file_but_leave_storage_file_unchanged,
    remote_change_should_invalidate_local_file_but_leave_storage_file_unchanged2,
    replication_job_should_fail,
    eviction_job_should_succeed,
    migration_job_should_fail
].

%%%===================================================================
%%% Test functions
%%%===================================================================

create_should_fail(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    FileName = ?FILE_NAME,

    % it should be impossible to create file
    ?assertEqual({error, ?EROFS}, lfm_proxy:create(W1, SessId, ?PATH(FileName))).

create_and_open_should_fail(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    FileName = ?FILE_NAME,

    % it should be impossible to create file
    ?assertEqual({error, ?EROFS}, lfm_proxy:create_and_open(W1, SessId, ?PATH(FileName))).

mkdir_should_fail(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    DirName = ?DIR_NAME,

    % it should be impossible to create file
    ?assertEqual({error, ?EROFS}, lfm_proxy:mkdir(W1, SessId, ?PATH(DirName))).

read_should_succeed(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    FileName = ?FILE_NAME,

    {Guid, _} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileName, ?TEST_DATA),

    % check whether file can be read
    {ok, H} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, Guid}, read)),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(W1, H, 0, 100)),
    ok = lfm_proxy:close(W1, H).

write_should_fail(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    FileName = ?FILE_NAME,

    {Guid, _} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileName, ?TEST_DATA),

    % it should be impossible to open the file for writing
    ?assertEqual({error, ?EROFS}, lfm_proxy:open(W1, SessId, {guid, Guid}, rdwr)),
    ?assertEqual({error, ?EROFS}, lfm_proxy:open(W1, SessId, {guid, Guid}, write)).

chmod_should_succeed_but_not_change_mode_on_storage(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    FileName = ?FILE_NAME,

    {Guid, SDHandle} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileName, ?TEST_DATA),

    % chmod file
    NewMode = 8#440,
    ?assertEqual(ok, lfm_proxy:set_perms(W1, ?ROOT_SESS_ID, {guid, Guid}, NewMode)),
    ?assertMatch({ok, #file_attr{mode = NewMode}}, lfm_proxy:stat(W1, SessId, {guid, Guid})),

    % file should still have old mode on storage
    ?assertMatch({ok, #statbuf{st_mode = ?DEFAULT_FILE_MODE}}, sd_test_utils:stat(W1, SDHandle)).

rename_should_fail(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    FileName = ?FILE_NAME,
    TargetName = ?FILE_NAME,
    {Guid, SDHandle} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileName, ?TEST_DATA),

    % rename should fail
    ?assertMatch({error, ?EROFS}, lfm_proxy:mv(W1, SessId, {guid, Guid}, ?PATH(TargetName))),

    ?assertMatch({ok, [{Guid, FileName}]}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10)),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, ?PATH(FileName)})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId, {path, ?PATH(TargetName)})),

    % file should still be visible on storage under old path
    ?assertMatch({ok, #statbuf{st_mode = ?DEFAULT_FILE_MODE}}, sd_test_utils:stat(W1, SDHandle)),
    SpaceDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, <<"">>),

    % target file shouldn't have been created on storage
    ?assertMatch({ok, [FileName]}, sd_test_utils:ls(W1, SpaceDirSDHandle, 0, 10)),
    TargetFileSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, filepath_utils:join([<<"/">>, TargetName])),
    ?assertMatch({error, ?ENOENT}, sd_test_utils:stat(W1, TargetFileSDHandle)).


mv_should_fail(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    SessId2 = ?SESS_ID(W2, Config),
    FileName = ?FILE_NAME,
    TargetFileName = ?FILE_NAME,
    TargetDir = ?DIR_NAME,
    TargetPath = filepath_utils:join([TargetDir, TargetFileName]),

    {Guid, SDHandle} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileName, ?TEST_DATA),

    % create directory in W2
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId2, ?PATH(TargetDir))),
    % wait for the directory to be synchronized to W1
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, ?PATH(TargetDir)}), ?ATTEMPTS),
    ?assertMatch({ok, [{DirGuid, TargetDir}, {Guid, FileName}]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),

    % wait for the file to be synchronized to W2
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W2, SessId2, {path, ?PATH(FileName)}), ?ATTEMPTS),
    ?assertMatch({ok, [{DirGuid, TargetDir}, {Guid, FileName}]},
        lfm_proxy:get_children(W2, SessId2, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),

    % mv should fail
    ?assertMatch({error, ?EROFS}, lfm_proxy:mv(W1, SessId, {guid, Guid}, ?PATH(TargetPath))),

    % file should still be visible on storage under old path
    ?assertMatch({ok, #statbuf{st_mode = ?DEFAULT_FILE_MODE}}, sd_test_utils:stat(W1, SDHandle)),

    % target file shouldn't have been created on storage
    SpaceDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, <<"">>),
    ?assertMatch({ok, [FileName]}, sd_test_utils:ls(W1, SpaceDirSDHandle, 0, 10)),
    DirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, filepath_utils:join([<<"/">>, TargetDir])),
    ?assertMatch({error, ?ENOENT}, sd_test_utils:stat(W1, DirSDHandle)),
    TargetFileSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, filepath_utils:join([<<"/">>, TargetPath])),
    ?assertMatch({error, ?ENOENT}, sd_test_utils:stat(W1, TargetFileSDHandle)).


unlink_should_succeed_but_should_leave_files_on_storage(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    FileName = ?FILE_NAME,
    DirName = ?DIR_NAME,
    FileRelativePath = filepath_utils:join([DirName, FileName]),
    {Guid, SDFileHandle} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileRelativePath, ?TEST_DATA),

    % it should be possible to remove the file (only its metadata)
    ?assertEqual(ok, lfm_proxy:unlink(W1, SessId, {guid, Guid})),
    % file should still exist on storage
    ?assertMatch({ok, #statbuf{}}, sd_test_utils:stat(W1, SDFileHandle)),

    % it should be possible to remove the directory (only its metadata)
    ?assertEqual(ok, lfm_proxy:unlink(W1, SessId, {path, ?PATH(DirName)})),
    StorageDirId = filepath_utils:join([<<"/">>, DirName]),
    SDDirHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageDirId),
    % directory should still exist on storage
    ?assertMatch({ok, #statbuf{}}, sd_test_utils:stat(W1, SDDirHandle)).


recursive_rm_should_succeed_but_should_leave_files_on_storage(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    FileName = ?FILE_NAME,
    DirName = ?DIR_NAME,
    FileRelativePath = filepath_utils:join([DirName, FileName]),
    {_Guid, SDFileHandle} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileRelativePath, ?TEST_DATA),
    {ok, #file_attr{guid = DirGuid}} = lfm_proxy:stat(W1, SessId, {path, ?PATH(DirName)}),

    % it should be possible to remove the file (only its metadata)
    ?assertEqual(ok, lfm_proxy:rm_recursive(W1, SessId, {path, ?PATH(DirName)})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId, {guid, DirGuid}), ?ATTEMPTS),

    % file should still exist on storage
    ?assertMatch({ok, #statbuf{}}, sd_test_utils:stat(W1, SDFileHandle)),

    StorageDirId = filepath_utils:join([<<"/">>, DirName]),
    SDDirHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageDirId),
    % directory should still exist on storage
    ?assertMatch({ok, #statbuf{}}, sd_test_utils:stat(W1, SDDirHandle)).

truncate_should_fail(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    FileName = ?FILE_NAME,

    {Guid, SDFileHandle} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileName, ?TEST_DATA),

    % it should not be possible to truncate the file (only its metadata)
    ?assertEqual({error, ?EROFS}, lfm_proxy:truncate(W1, SessId, {guid, Guid}, 0)),

    % file should still have old size
    TestDataSize = byte_size(?TEST_DATA),
    ?assertMatch({ok, #statbuf{st_size = TestDataSize}}, sd_test_utils:stat(W1, SDFileHandle)).

remote_chmod_should_not_change_mode_on_storage(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    SessId2 = ?SESS_ID(W2, Config),
    FileName = ?FILE_NAME,

    {Guid, SDHandle} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileName, ?TEST_DATA),

    % wait for file to be synchronized to W2
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W2, SessId2, {guid, Guid}), ?ATTEMPTS),

    % chmod file
    NewMode = 8#440,
    ?assertEqual(ok, lfm_proxy:set_perms(W2, ?ROOT_SESS_ID, {guid, Guid}, NewMode)),
    ?assertMatch({ok, #file_attr{mode = NewMode}}, lfm_proxy:stat(W1, SessId, {guid, Guid}), ?ATTEMPTS),

    % file should still have old mode on storage
    ?assertMatch({ok, #statbuf{st_mode = ?DEFAULT_FILE_MODE}}, sd_test_utils:stat(W1, SDHandle)).

remote_rename_should_not_rename_file_on_storage(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    SessId2 = ?SESS_ID(W2, Config),
    FileName = ?FILE_NAME,
    TargetFileName = ?FILE_NAME,

    {Guid, SDHandle} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileName, ?TEST_DATA),

    % wait for file to be synchronized to W2
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W2, SessId2, {guid, Guid}), ?ATTEMPTS),
    ?assertMatch({ok, [{Guid, FileName}]}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),

    {ok, H} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, Guid}, read), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(W2, H, 0, 100), ?ATTEMPTS),
    ok = lfm_proxy:close(W2, H),

    % rename file
    ?assertEqual({ok, Guid}, lfm_proxy:mv(W2, SessId2, {guid, Guid}, ?PATH(TargetFileName))),

    % file should be renamed in W1
    ?assertMatch({ok, [{Guid, TargetFileName}]}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, ?PATH(TargetFileName)})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId, {path, ?PATH(FileName)})),

    % wait for file_location to be synchronized as that's what triggers rename of fie on storage
    timer:sleep(timer:seconds(20)),

    % W1 should still have up to date version of the file
    {ok, H2} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, Guid}, read), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(W1, H2, 0, 100), ?ATTEMPTS),
    ok = lfm_proxy:close(W1, H2),

    SpaceDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, <<"">>),
    NewFileSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, filepath_utils:join([<<"/">>, TargetFileName])),
    % file should still be visible on storage with old name
    ?assertMatch({ok, [FileName]}, sd_test_utils:ls(W1, SpaceDirSDHandle, 0, 10)),
    ?assertMatch({ok, _}, sd_test_utils:stat(W1, SDHandle)),
    % new file shouldn't have been created
    ?assertMatch({error, ?ENOENT}, sd_test_utils:stat(W1, NewFileSDHandle)).


remote_move_should_not_rename_file_on_storage(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    SessId2 = ?SESS_ID(W2, Config),
    FileName = ?FILE_NAME,
    TargetDir = ?DIR_NAME,
    TargetFileName = ?FILE_NAME,
    TargetPath = filepath_utils:join([TargetDir, TargetFileName]),

    {Guid, SDHandle} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileName, ?TEST_DATA),


    % create directory in W2
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(W2, SessId2, ?PATH(TargetDir))),
    % wait for the directory to be synchronized to W1
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, ?PATH(TargetDir)}), ?ATTEMPTS),
    ?assertMatch({ok, [{DirGuid, TargetDir}, {Guid, FileName}]},
        lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),

    % wait for file to be synchronized to W2
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W2, SessId2, {guid, Guid}), ?ATTEMPTS),
    ?assertMatch({ok, [{DirGuid, TargetDir}, {Guid, FileName}]},
        lfm_proxy:get_children(W2, SessId2, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),

    % move file
    ?assertEqual({ok, Guid}, lfm_proxy:mv(W2, SessId2, {guid, Guid}, ?PATH(TargetPath))),

    % file should be renamed in W1
    ?assertMatch({ok, [{Guid, TargetFileName}]}, lfm_proxy:get_children(W1, SessId, {path, ?PATH(TargetDir)}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, ?PATH(TargetPath)})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId, {path, ?PATH(FileName)})),

    % wait for file_location to be synchronized as that's what triggers rename of fie on storage
    timer:sleep(timer:seconds(20)),

    % W1 should still have up to date version of the file
    {ok, H2} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, Guid}, read), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(W1, H2, 0, 100), ?ATTEMPTS),
    ok = lfm_proxy:close(W1, H2),


    % file should still be visible on storage with old name
    SpaceDirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, <<"">>),
    ?assertMatch({ok, [FileName]}, sd_test_utils:ls(W1, SpaceDirSDHandle, 0, 10)),
    ?assertMatch({ok, _}, sd_test_utils:stat(W1, SDHandle)),

    % new files shouldn't have been created
    DirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, filepath_utils:join([<<"/">>, TargetDir])),
    ?assertMatch({error, ?ENOENT}, sd_test_utils:stat(W1, DirSDHandle)),
    NewFileSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, filepath_utils:join([<<"/">>, TargetPath])),
    ?assertMatch({error, ?ENOENT}, sd_test_utils:stat(W1, NewFileSDHandle)).


remote_unlink_should_not_trigger_unlinking_files_on_local_storage(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    SessId2 = ?SESS_ID(W2, Config),
    FileName = ?FILE_NAME,
    DirName = ?DIR_NAME,
    FileRelativePath = filepath_utils:join([DirName, FileName]),
    {Guid, SDFileHandle} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileRelativePath, ?TEST_DATA),

    % wait for file to be synchronized to W2
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W2, SessId2, {guid, Guid}), ?ATTEMPTS),
    ?assertMatch({ok, [{_, DirName}]}, lfm_proxy:get_children(W2, SessId2, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, [{Guid, FileName}]}, lfm_proxy:get_children(W2, SessId2, {path, ?PATH(DirName)}, 0, 10), ?ATTEMPTS),

    ?assertEqual(ok, lfm_proxy:unlink(W2, SessId2, {guid, Guid})),

    % wait for file to be unlinked
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId, {guid, Guid}), ?ATTEMPTS),

    % file should still exist on storage
    ?assertMatch({ok, #statbuf{}}, sd_test_utils:stat(W1, SDFileHandle)),

    % wait for dir to be synchronized to W2
    {ok, #file_attr{guid = DirGuid}} =
        ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W2, SessId2, {path, ?PATH(DirName)}), ?ATTEMPTS),
    ?assertMatch({ok, [{DirGuid, DirName}]}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),
    % it should be possible to remove the directory (only its metadata)
    ?assertEqual(ok, lfm_proxy:unlink(W2, SessId2, {path, ?PATH(DirName)})),

    % wait for dir to be unlinked
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId, {guid, DirGuid}), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W1, SessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId, {guid, Guid}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_children(W1, SessId, {guid, DirGuid}, 0, 10), ?ATTEMPTS),

    StorageDirId = filepath_utils:join([<<"/">>, DirName]),
    SDDirHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageDirId),
    % directory should still exist on storage
    ?assertMatch({ok, #statbuf{}}, sd_test_utils:stat(W1, SDDirHandle)).


remote_recursive_rm_should_not_trigger_removal_of_files_on_local_storage(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    SessId2 = ?SESS_ID(W2, Config),
    FileName = ?FILE_NAME,
    DirName = ?DIR_NAME,
    FileRelativePath = filepath_utils:join([DirName, FileName]),
    {Guid, SDFileHandle} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileRelativePath, ?TEST_DATA),

    % wait for file to be synchronized to W2
    {ok, #file_attr{parent_guid = DirGuid}} =
        ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W2, SessId2, {guid, Guid}), ?ATTEMPTS),

    ?assertEqual(ok, lfm_proxy:rm_recursive(W2, SessId2, {path, ?PATH(DirName)})),

    % wait for files to be removed
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId, {guid, DirGuid}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W1, SessId, {guid, Guid}), ?ATTEMPTS),

    % file should still exist on storage
    ?assertMatch({ok, #statbuf{}}, sd_test_utils:stat(W1, SDFileHandle)),

    StorageDirId = filepath_utils:join([<<"/">>, DirName]),
    SDDirHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageDirId),
    % directory should still exist on storage
    ?assertMatch({ok, #statbuf{}}, sd_test_utils:stat(W1, SDDirHandle)).

remote_truncate_should_not_trigger_truncate_on_storage(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    SessId2 = ?SESS_ID(W2, Config),
    FileName = ?FILE_NAME,
    FileName2 = ?FILE_NAME,

    {Guid, SDFileHandle} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileName, ?TEST_DATA),
    {Guid2, SDFileHandle2} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileName2, ?TEST_DATA),

    % replicate both files to W2
    {ok, H} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, Guid}, read), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(W2, H, 0, 100), ?ATTEMPTS),
    ok = lfm_proxy:close(W2, H),

    {ok, H2} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, Guid2}, read), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(W2, H2, 0, 100), ?ATTEMPTS),
    ok = lfm_proxy:close(W2, H2),

    NewSize1 = 0,
    NewSize2 = 1000,
    ?assertEqual(ok, lfm_proxy:truncate(W2, SessId2, {guid, Guid}, NewSize1)),
    ?assertEqual(ok, lfm_proxy:truncate(W2, SessId2, {guid, Guid2}, NewSize2)),

    % logical files' sizes should change
    ?assertMatch({ok, #file_attr{size = NewSize1}}, lfm_proxy:stat(W1, SessId, {guid, Guid}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{size = NewSize2}}, lfm_proxy:stat(W1, SessId, {guid, Guid2}), ?ATTEMPTS),

    % files should still have old sizes on storage
    TestDataSize = byte_size(?TEST_DATA),
    ?assertMatch({ok, #statbuf{st_size = TestDataSize}}, sd_test_utils:stat(W1, SDFileHandle)),
    ?assertMatch({ok, #statbuf{st_size = TestDataSize}}, sd_test_utils:stat(W1, SDFileHandle2)).

replication_on_the_fly_should_fail(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    SessId2 = ?SESS_ID(W2, Config),
    FileName = ?FILE_NAME,
    TestDataSize = byte_size(?TEST_DATA),

    {ok, {Guid, Handle}} = lfm_proxy:create_and_open(W2, SessId2, ?PATH(FileName)),
    {ok, _} = lfm_proxy:write(W2, Handle, 0, ?TEST_DATA),
    lfm_proxy:close(W2, Handle),

    ?assertMatch({ok, #file_attr{size = TestDataSize}}, lfm_proxy:stat(W1, SessId, {guid, Guid}), ?ATTEMPTS),
    ?assertEqual({error, ?EROFS}, lfm_proxy:open(W1, SessId, {guid, Guid}, read)).

remote_change_should_invalidate_local_file_but_leave_storage_file_unchanged(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    SessId2 = ?SESS_ID(W2, Config),
    FileName = ?FILE_NAME,
    TestDataSize = byte_size(?TEST_DATA),
    TestDataSize2 = byte_size(?TEST_DATA2),
    ProviderId1 = provider_id(W1),
    ProviderId2 = provider_id(W2),
    {Guid, SDFileHandle} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileName, ?TEST_DATA),

    {ok, H} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, Guid}, rdwr), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W2, H, 0, TestDataSize), ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:write(W2, H, 0, ?TEST_DATA2)),
    lfm_proxy:close(W2, H),

    % whole file on W1 should be invalidated
    ?assertDistribution(W1, SessId, ?DISTS([ProviderId1, ProviderId2], [0, TestDataSize2]), Guid, ?ATTEMPTS),

    ?assertEqual({error, ?EROFS}, lfm_proxy:open(W1, SessId, {guid, Guid}, rdwr), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{size = TestDataSize2}}, lfm_proxy:stat(W1, SessId, {guid, Guid}), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, sd_test_utils:read_file(W1, SDFileHandle, 0, TestDataSize)).

remote_change_should_invalidate_local_file_but_leave_storage_file_unchanged2(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    SessId2 = ?SESS_ID(W2, Config),
    FileName = ?FILE_NAME,
    TestDataSize = byte_size(?TEST_DATA),
    ChangedByteOffset = 5,
    ProviderId1 = provider_id(W1),
    ProviderId2 = provider_id(W2),
    {Guid, SDFileHandle} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileName, ?TEST_DATA),

    {ok, H} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, Guid}, rdwr), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W2, H, 0, TestDataSize), ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:write(W2, H, ChangedByteOffset, <<"#">>)),
    lfm_proxy:close(W2, H),

    % whole file on W1 should be invalidated
    ExpectedP1Blocks = [[0, ChangedByteOffset], [ChangedByteOffset + 1, TestDataSize - (ChangedByteOffset + 1)]],
    ?assertDistribution(W1, SessId, ?DISTS([ProviderId1, ProviderId2], [ExpectedP1Blocks, TestDataSize]), Guid, ?ATTEMPTS),

    ?assertEqual({error, ?EROFS}, lfm_proxy:open(W1, SessId, {guid, Guid}, rdwr), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{size = TestDataSize}}, lfm_proxy:stat(W1, SessId, {guid, Guid}), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, sd_test_utils:read_file(W1, SDFileHandle, 0, TestDataSize)).

replication_job_should_fail(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    SessId2 = ?SESS_ID(W2, Config),
    ProviderId1 = provider_id(W1),
    ProviderId2 = provider_id(W2),
    FileName = ?FILE_NAME,
    TestDataSize = byte_size(?TEST_DATA),

    {ok, {Guid, Handle}} = lfm_proxy:create_and_open(W2, SessId2, ?PATH(FileName)),
    {ok, _} = lfm_proxy:write(W2, Handle, 0, ?TEST_DATA),
    lfm_proxy:close(W2, Handle),

    ?assertMatch({ok, #file_attr{size = TestDataSize}}, lfm_proxy:stat(W1, SessId, {guid, Guid}), ?ATTEMPTS),
    ?assertMatch({ok, [#{
        <<"blocks">> := [[0, TestDataSize]],
        <<"providerId">> := ProviderId2,
        <<"totalBlocksSize">> := TestDataSize
    }]}, lfm_proxy:get_file_distribution(W1, SessId, {guid, Guid}), ?ATTEMPTS),
    ?assertEqual({error, ?EROFS}, lfm_proxy:schedule_file_replication(W1, SessId, {guid, Guid}, ProviderId1)).


eviction_job_should_succeed(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    SessId2 = ?SESS_ID(W2, Config),
    FileName = ?FILE_NAME,
    TestDataSize = byte_size(?TEST_DATA),
    ProviderId1 = provider_id(W1),
    ProviderId2 = provider_id(W2),
    {Guid, SDFileHandle} = create_file_on_storage_and_register(W1, SessId, ?SPACE_ID, FileName, ?TEST_DATA),

    {ok, H} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, Guid}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W2, H, 0, TestDataSize), ?ATTEMPTS),
    lfm_proxy:close(W2, H),

    ?assertDistribution(W1, SessId, ?DISTS([ProviderId1, ProviderId2], [TestDataSize, TestDataSize]), Guid, ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:schedule_file_replica_eviction(W1, SessId, {guid, Guid}, ProviderId1, undefined)),

    % whole file on W1 should be invalidated
    ?assertDistribution(W1, SessId, ?DISTS([ProviderId1, ProviderId2], [0, TestDataSize]), Guid, ?ATTEMPTS),
    % file on storage should be unchanged
    ?assertEqual({ok, ?TEST_DATA}, sd_test_utils:read_file(W1, SDFileHandle, 0, TestDataSize)).


migration_job_should_fail(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(W1, Config),
    SessId2 = ?SESS_ID(W2, Config),
    ProviderId1 = provider_id(W1),
    ProviderId2 = provider_id(W2),
    FileName = ?FILE_NAME,
    TestDataSize = byte_size(?TEST_DATA),

    {ok, {Guid, Handle}} = lfm_proxy:create_and_open(W2, SessId2, ?PATH(FileName)),
    {ok, _} = lfm_proxy:write(W2, Handle, 0, ?TEST_DATA),
    lfm_proxy:close(W2, Handle),

    ?assertMatch({ok, #file_attr{size = TestDataSize}}, lfm_proxy:stat(W1, SessId, {guid, Guid}), ?ATTEMPTS),
    ?assertMatch({ok, [#{
        <<"blocks">> := [[0, TestDataSize]],
        <<"providerId">> := ProviderId2,
        <<"totalBlocksSize">> := TestDataSize
    }]}, lfm_proxy:get_file_distribution(W1, SessId, {guid, Guid}), ?ATTEMPTS),
    ?assertEqual({error, ?EROFS}, lfm_proxy:schedule_file_replica_eviction(W1, SessId, {guid, Guid}, ProviderId2, ProviderId1)).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        initializer:mock_provider_ids(NewConfig),
        initializer:mock_auth_manager(NewConfig),
        NewConfig2 = multi_provider_file_ops_test_base:init_env(NewConfig),
        sort_workers(NewConfig2)
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, sd_test_utils, ?MODULE]}
        | Config
    ].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config),
    initializer:unmock_auth_manager(Config),
    initializer:unmock_provider_ids(Config).

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    [W1 | _] = Workers = ?config(op_worker_nodes, Config),
    lfm_test_utils:clean_space(W1, Workers, ?SPACE_ID, ?ATTEMPTS),
    clean_storage(W1, ?SPACE_ID, ?RW_STORAGE_ID),
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

sort_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:keyreplace(op_worker_nodes, 1, Config, {op_worker_nodes, lists:sort(Workers)}).

create_file_on_storage(Worker, StorageFileId, TestData) ->
    % create file on ?RW_STORAGE_ID
    SDFileHandle = sd_test_utils:new_handle(Worker, ?SPACE_ID, StorageFileId, ?RW_STORAGE_ID),
    ok = sd_test_utils:create_file(Worker, SDFileHandle, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(Worker, SDFileHandle, 0, TestData),
    SDFileHandle.

create_file_on_storage_and_register(Worker, SessionId, SpaceId, FileRelativePath, TestData) ->
    StorageFileId = filepath_utils:join([<<"/">>, FileRelativePath]),
    ensure_parent_dirs_created_on_storage(Worker, SpaceId, StorageFileId),
    SDFileHandle = create_file_on_storage(Worker, StorageFileId, TestData),
    {ok, Guid} = register_file(Worker, SessionId, SpaceId, FileRelativePath, byte_size(TestData)),
    {Guid, SDFileHandle}.

register_file(Worker, SessionId, SpaceId, FileRelativePath, Size) ->
    StorageFileId = filepath_utils:join([<<"/">>, FileRelativePath]),
    StorageId = initializer:get_supporting_storage_id(Worker, SpaceId),
    register_file(Worker, SessionId, SpaceId, FileRelativePath, StorageId, StorageFileId, #{<<"size">> => Size}).


register_file(Worker, SessionId, SpaceId, DestinationPath, StorageId, StorageFileId, Spec) ->
    rpc:call(Worker, file_registration, register,
        [SessionId, SpaceId, DestinationPath, StorageId, StorageFileId, Spec]
    ).


ensure_parent_dirs_created_on_storage(Worker, SpaceId, StorageFileId) ->
    {_BaseName, ParentStorageFileId} = filepath_utils:basename_and_parent_dir(StorageFileId),
    ParentSDHandle = sd_test_utils:new_handle(Worker, SpaceId, ParentStorageFileId, ?RW_STORAGE_ID),
    sd_test_utils:mkdir(Worker, ParentSDHandle, ?DEFAULT_DIR_PERMS).

provider_id(Worker) ->
    rpc:call(Worker, oneprovider, get_id, []).

clean_storage(Worker, SpaceId, StorageId) ->
    SDHandle = sd_test_utils:new_handle(Worker, SpaceId, <<"/">>, StorageId),
    sd_test_utils:recursive_rm(Worker, SDHandle, true),
    ?assertMatch({ok, []}, sd_test_utils:ls(Worker, SDHandle, 0, 1)).