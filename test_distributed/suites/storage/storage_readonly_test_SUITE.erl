%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of operations on files in a space that is
%%% supported by a readonly storage.
%%%
%%% The space is supported by two providers: krakow with an imported, readonly
%%% POSIX storage and paris with a regular read-write one. A readonly storage
%%% is always an imported one - the Oneprovider rejects any other combination
%%% (see storage:sanitize_readonly_option/2).
%%%
%%% ATTENTION !!!
%%% Krakow registers a second, regular read-write storage under THE SAME MOUNT
%%% POINT as the readonly one. It supports no space and exists solely so that
%%% the tests can place files on (and wipe them from) the readonly storage -
%%% operations they could not perform through the readonly storage itself.
%%% Both storages are merely paths in the same container, so a file written
%%% through one is visible through the other.
%%%
%%% ATTENTION2 !!!
%%% The readonly storage is NOT backed by a readonly mount, INTENTIONALLY.
%%% Thanks to that the assertions verify that the storage files were left
%%% untouched by the Oneprovider logic, rather than by filesystem restrictions
%%% that would mask a missing check.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_readonly_test_SUITE).
-author("Jakub Kudzia").

-include("env/space_setup_utils.hrl").
-include("file/distribution_assert.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    create_should_fail/1,
    create_and_open_should_fail/1,
    mkdir_should_succeed/1,
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
    migration_job_should_fail/1
]).

all() -> [
    create_should_fail,
    create_and_open_should_fail,
    mkdir_should_succeed,
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

% The provider whose storage supporting the space is readonly, and the one backing
% it with a regular read-write storage.
-define(RO_PROVIDER, krakow).
-define(OTHER_PROVIDER, paris).
-define(TEST_USER, user1).

% Doubles as the name of the space, which is how clean_up_after_previous_run/2
% recognizes the leftovers of a previous run.
-define(SPACE_NAME, storage_readonly_space).
-define(SPACE_PATH, <<"/", (atom_to_binary(?SPACE_NAME))/binary>>).
-define(SUPPORT_SIZE, 10 * 1024 * 1024 * 1024).

-define(PATH(FileRelativePath), filepath_utils:join([?SPACE_PATH, FileRelativePath])).
-define(STORAGE_PATH(FileRelativePath), filepath_utils:join([<<"/">>, FileRelativePath])).

-define(FILE_NAME, <<"file_", (?RAND_NAME)/binary>>).
-define(DIR_NAME, <<"dir_", (?RAND_NAME)/binary>>).
-define(RAND_NAME,
    <<(str_utils:to_binary(?FUNCTION_NAME))/binary, "_", (integer_to_binary(rand:uniform(?RAND_RANGE)))/binary>>).
-define(RAND_RANGE, 1000000000).
-define(TEST_DATA, <<"abcdefgh">>).
-define(TEST_DATA2, <<"0123456789">>).
-define(ATTEMPTS, 30).

-record(test_ctx, {
    space_id :: od_space:id(),
    % supports the space on ?RO_PROVIDER; imported and readonly
    readonly_storage_id :: storage:id(),
    % supports no space; shares the mount point with the readonly storage and is
    % the only way for the tests to write to it (see the module doc)
    backdoor_storage_id :: storage:id(),
    ro_node :: node(),
    ro_sess_id :: session:id(),
    other_node :: node(),
    other_sess_id :: session:id()
}).
-type test_ctx() :: #test_ctx{}.

%%%===================================================================
%%% Test functions
%%%===================================================================

create_should_fail(Config) ->
    #test_ctx{ro_node = RoNode, ro_sess_id = RoSessId} = get_test_ctx(Config),

    % it should be impossible to create file
    ?assertEqual({error, ?EROFS}, lfm_proxy:create(RoNode, RoSessId, ?PATH(?FILE_NAME))).


create_and_open_should_fail(Config) ->
    #test_ctx{ro_node = RoNode, ro_sess_id = RoSessId} = get_test_ctx(Config),

    % it should be impossible to create file
    ?assertEqual({error, ?EROFS}, lfm_proxy:create_and_open(RoNode, RoSessId, ?PATH(?FILE_NAME))).


mkdir_should_succeed(Config) ->
    #test_ctx{ro_node = RoNode, ro_sess_id = RoSessId} = get_test_ctx(Config),

    % creating a directory affects metadata only, so it is allowed
    ?assertMatch({ok, _}, lfm_proxy:mkdir(RoNode, RoSessId, ?PATH(?DIR_NAME))).


read_should_succeed(Config) ->
    TestCtx = #test_ctx{ro_node = RoNode, ro_sess_id = RoSessId} = get_test_ctx(Config),

    {Guid, _} = create_file_on_storage_and_register(TestCtx, ?FILE_NAME, ?TEST_DATA),

    % check whether file can be read
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(RoNode, RoSessId, ?FILE_REF(Guid), read)),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(RoNode, Handle, 0, 100)),
    ok = lfm_proxy:close(RoNode, Handle).


write_should_fail(Config) ->
    TestCtx = #test_ctx{ro_node = RoNode, ro_sess_id = RoSessId} = get_test_ctx(Config),

    {Guid, _} = create_file_on_storage_and_register(TestCtx, ?FILE_NAME, ?TEST_DATA),

    % it should be impossible to open the file for writing
    ?assertEqual({error, ?EROFS}, lfm_proxy:open(RoNode, RoSessId, ?FILE_REF(Guid), rdwr)),
    ?assertEqual({error, ?EROFS}, lfm_proxy:open(RoNode, RoSessId, ?FILE_REF(Guid), write)).


chmod_should_succeed_but_not_change_mode_on_storage(Config) ->
    TestCtx = #test_ctx{ro_node = RoNode, ro_sess_id = RoSessId} = get_test_ctx(Config),

    {Guid, StorageFileId} = create_file_on_storage_and_register(TestCtx, ?FILE_NAME, ?TEST_DATA),

    % chmod file
    NewMode = 8#440,
    ?assertEqual(ok, lfm_proxy:set_perms(RoNode, ?ROOT_SESS_ID, ?FILE_REF(Guid), NewMode)),
    ?assertMatch({ok, #file_attr{mode = NewMode}}, lfm_proxy:stat(RoNode, RoSessId, ?FILE_REF(Guid))),

    % file should still have old mode on storage
    ?assertMatch({ok, #statbuf{st_mode = ?DEFAULT_FILE_MODE}}, stat_on_storage(TestCtx, StorageFileId)).


rename_should_fail(Config) ->
    TestCtx = #test_ctx{ro_node = RoNode, ro_sess_id = RoSessId} = get_test_ctx(Config),
    FileName = ?FILE_NAME,
    TargetName = ?FILE_NAME,

    {Guid, StorageFileId} = create_file_on_storage_and_register(TestCtx, FileName, ?TEST_DATA),

    % rename should fail
    ?assertMatch({error, ?EROFS}, lfm_proxy:mv(RoNode, RoSessId, ?FILE_REF(Guid), ?PATH(TargetName))),

    ?assertMatch({ok, [{Guid, FileName}]}, lfm_proxy:get_children(RoNode, RoSessId, {path, ?SPACE_PATH}, 0, 10)),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(RoNode, RoSessId, {path, ?PATH(FileName)})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(RoNode, RoSessId, {path, ?PATH(TargetName)})),

    % file should still be visible on storage under old path
    ?assertMatch({ok, #statbuf{st_mode = ?DEFAULT_FILE_MODE}}, stat_on_storage(TestCtx, StorageFileId)),

    % target file shouldn't have been created on storage
    ?assertMatch({ok, [FileName]}, list_space_dir_on_storage(TestCtx)),
    ?assertMatch({error, ?ENOENT}, stat_on_storage(TestCtx, ?STORAGE_PATH(TargetName))).


mv_should_fail(Config) ->
    TestCtx = #test_ctx{
        ro_node = RoNode, ro_sess_id = RoSessId,
        other_node = OtherNode, other_sess_id = OtherSessId
    } = get_test_ctx(Config),
    FileName = ?FILE_NAME,
    TargetDir = ?DIR_NAME,
    TargetFileName = ?FILE_NAME,
    TargetPath = filepath_utils:join([TargetDir, TargetFileName]),

    {Guid, StorageFileId} = create_file_on_storage_and_register(TestCtx, FileName, ?TEST_DATA),

    % create directory on the other provider
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(OtherNode, OtherSessId, ?PATH(TargetDir))),
    % wait for the directory to be synchronized to the readonly provider
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(RoNode, RoSessId, {path, ?PATH(TargetDir)}), ?ATTEMPTS),
    ?assertMatch({ok, [{DirGuid, TargetDir}, {Guid, FileName}]},
        lfm_proxy:get_children(RoNode, RoSessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),

    % wait for the file to be synchronized to the other provider
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(OtherNode, OtherSessId, {path, ?PATH(FileName)}), ?ATTEMPTS),
    ?assertMatch({ok, [{DirGuid, TargetDir}, {Guid, FileName}]},
        lfm_proxy:get_children(OtherNode, OtherSessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),

    % mv should fail
    ?assertMatch({error, ?EROFS}, lfm_proxy:mv(RoNode, RoSessId, ?FILE_REF(Guid), ?PATH(TargetPath))),

    % file should still be visible on storage under old path
    ?assertMatch({ok, #statbuf{st_mode = ?DEFAULT_FILE_MODE}}, stat_on_storage(TestCtx, StorageFileId)),

    % target file shouldn't have been created on storage
    ?assertMatch({ok, [FileName]}, list_space_dir_on_storage(TestCtx)),
    ?assertMatch({error, ?ENOENT}, stat_on_storage(TestCtx, ?STORAGE_PATH(TargetDir))),
    ?assertMatch({error, ?ENOENT}, stat_on_storage(TestCtx, ?STORAGE_PATH(TargetPath))).


unlink_should_succeed_but_should_leave_files_on_storage(Config) ->
    TestCtx = #test_ctx{ro_node = RoNode, ro_sess_id = RoSessId} = get_test_ctx(Config),
    DirName = ?DIR_NAME,
    FileRelativePath = filepath_utils:join([DirName, ?FILE_NAME]),

    {Guid, StorageFileId} = create_file_on_storage_and_register(TestCtx, FileRelativePath, ?TEST_DATA),

    % it should be possible to remove the file (only its metadata)
    ?assertEqual(ok, lfm_proxy:unlink(RoNode, RoSessId, ?FILE_REF(Guid))),
    % file should still exist on storage
    ?assertMatch({ok, #statbuf{}}, stat_on_storage(TestCtx, StorageFileId)),

    % it should be possible to remove the directory (only its metadata)
    ?assertEqual(ok, lfm_proxy:unlink(RoNode, RoSessId, {path, ?PATH(DirName)})),
    % directory should still exist on storage
    ?assertMatch({ok, #statbuf{}}, stat_on_storage(TestCtx, ?STORAGE_PATH(DirName))).


recursive_rm_should_succeed_but_should_leave_files_on_storage(Config) ->
    TestCtx = #test_ctx{ro_node = RoNode, ro_sess_id = RoSessId} = get_test_ctx(Config),
    DirName = ?DIR_NAME,
    FileRelativePath = filepath_utils:join([DirName, ?FILE_NAME]),

    {_Guid, StorageFileId} = create_file_on_storage_and_register(TestCtx, FileRelativePath, ?TEST_DATA),
    {ok, #file_attr{guid = DirGuid}} = lfm_proxy:stat(RoNode, RoSessId, {path, ?PATH(DirName)}),

    % it should be possible to remove the whole tree (only its metadata)
    ?assertEqual(ok, lfm_proxy:rm_recursive(RoNode, RoSessId, {path, ?PATH(DirName)})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(RoNode, RoSessId, ?FILE_REF(DirGuid)), ?ATTEMPTS),

    % file and directory should still exist on storage
    ?assertMatch({ok, #statbuf{}}, stat_on_storage(TestCtx, StorageFileId)),
    ?assertMatch({ok, #statbuf{}}, stat_on_storage(TestCtx, ?STORAGE_PATH(DirName))).


truncate_should_fail(Config) ->
    TestCtx = #test_ctx{ro_node = RoNode, ro_sess_id = RoSessId} = get_test_ctx(Config),

    {Guid, StorageFileId} = create_file_on_storage_and_register(TestCtx, ?FILE_NAME, ?TEST_DATA),

    % it should not be possible to truncate the file
    ?assertEqual({error, ?EROFS}, lfm_proxy:truncate(RoNode, RoSessId, ?FILE_REF(Guid), 0)),

    % file should still have old size
    TestDataSize = byte_size(?TEST_DATA),
    ?assertMatch({ok, #statbuf{st_size = TestDataSize}}, stat_on_storage(TestCtx, StorageFileId)).


remote_chmod_should_not_change_mode_on_storage(Config) ->
    TestCtx = #test_ctx{
        ro_node = RoNode, ro_sess_id = RoSessId,
        other_node = OtherNode, other_sess_id = OtherSessId
    } = get_test_ctx(Config),

    {Guid, StorageFileId} = create_file_on_storage_and_register(TestCtx, ?FILE_NAME, ?TEST_DATA),

    % wait for file to be synchronized to the other provider
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(OtherNode, OtherSessId, ?FILE_REF(Guid)), ?ATTEMPTS),

    % chmod file
    NewMode = 8#440,
    ?assertEqual(ok, lfm_proxy:set_perms(OtherNode, ?ROOT_SESS_ID, ?FILE_REF(Guid), NewMode)),
    ?assertMatch({ok, #file_attr{mode = NewMode}}, lfm_proxy:stat(RoNode, RoSessId, ?FILE_REF(Guid)), ?ATTEMPTS),

    % file should still have old mode on storage
    ?assertMatch({ok, #statbuf{st_mode = ?DEFAULT_FILE_MODE}}, stat_on_storage(TestCtx, StorageFileId)).


remote_rename_should_not_rename_file_on_storage(Config) ->
    TestCtx = #test_ctx{
        ro_node = RoNode, ro_sess_id = RoSessId,
        other_node = OtherNode, other_sess_id = OtherSessId
    } = get_test_ctx(Config),
    FileName = ?FILE_NAME,
    TargetFileName = ?FILE_NAME,

    {Guid, StorageFileId} = create_file_on_storage_and_register(TestCtx, FileName, ?TEST_DATA),

    % wait for file to be synchronized to the other provider
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(OtherNode, OtherSessId, ?FILE_REF(Guid)), ?ATTEMPTS),
    ?assertMatch({ok, [{Guid, FileName}]},
        lfm_proxy:get_children(RoNode, RoSessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),

    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(OtherNode, OtherSessId, ?FILE_REF(Guid), read), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(OtherNode, Handle, 0, 100), ?ATTEMPTS),
    ok = lfm_proxy:close(OtherNode, Handle),

    % rename file
    ?assertEqual({ok, Guid}, lfm_proxy:mv(OtherNode, OtherSessId, ?FILE_REF(Guid), ?PATH(TargetFileName))),

    % file should be renamed on the readonly provider
    ?assertMatch({ok, [{Guid, TargetFileName}]},
        lfm_proxy:get_children(RoNode, RoSessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(RoNode, RoSessId, {path, ?PATH(TargetFileName)}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(RoNode, RoSessId, {path, ?PATH(FileName)}), ?ATTEMPTS),

    await_file_location_synchronization(),

    % the readonly provider should still have up to date version of the file
    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(RoNode, RoSessId, ?FILE_REF(Guid), read), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(RoNode, Handle2, 0, 100), ?ATTEMPTS),
    ok = lfm_proxy:close(RoNode, Handle2),

    % file should still be visible on storage with old name
    ?assertMatch({ok, [FileName]}, list_space_dir_on_storage(TestCtx)),
    ?assertMatch({ok, _}, stat_on_storage(TestCtx, StorageFileId)),
    % new file shouldn't have been created
    ?assertMatch({error, ?ENOENT}, stat_on_storage(TestCtx, ?STORAGE_PATH(TargetFileName))).


remote_move_should_not_rename_file_on_storage(Config) ->
    TestCtx = #test_ctx{
        ro_node = RoNode, ro_sess_id = RoSessId,
        other_node = OtherNode, other_sess_id = OtherSessId
    } = get_test_ctx(Config),
    FileName = ?FILE_NAME,
    TargetDir = ?DIR_NAME,
    TargetFileName = ?FILE_NAME,
    TargetPath = filepath_utils:join([TargetDir, TargetFileName]),

    {Guid, StorageFileId} = create_file_on_storage_and_register(TestCtx, FileName, ?TEST_DATA),

    % create directory on the other provider
    {ok, DirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(OtherNode, OtherSessId, ?PATH(TargetDir))),
    % wait for the directory to be synchronized to the readonly provider
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(RoNode, RoSessId, {path, ?PATH(TargetDir)}), ?ATTEMPTS),
    ?assertMatch({ok, [{DirGuid, TargetDir}, {Guid, FileName}]},
        lfm_proxy:get_children(RoNode, RoSessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),

    % wait for file to be synchronized to the other provider
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(OtherNode, OtherSessId, ?FILE_REF(Guid)), ?ATTEMPTS),
    ?assertMatch({ok, [{DirGuid, TargetDir}, {Guid, FileName}]},
        lfm_proxy:get_children(OtherNode, OtherSessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),

    % move file
    ?assertEqual({ok, Guid}, lfm_proxy:mv(OtherNode, OtherSessId, ?FILE_REF(Guid), ?PATH(TargetPath))),

    % file should be moved on the readonly provider
    ?assertMatch({ok, [{Guid, TargetFileName}]},
        lfm_proxy:get_children(RoNode, RoSessId, {path, ?PATH(TargetDir)}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(RoNode, RoSessId, {path, ?PATH(TargetPath)}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(RoNode, RoSessId, {path, ?PATH(FileName)}), ?ATTEMPTS),

    await_file_location_synchronization(),

    % the readonly provider should still have up to date version of the file
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(RoNode, RoSessId, ?FILE_REF(Guid), read), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(RoNode, Handle, 0, 100), ?ATTEMPTS),
    ok = lfm_proxy:close(RoNode, Handle),

    % file should still be visible on storage with old name
    ?assertMatch({ok, [FileName]}, list_space_dir_on_storage(TestCtx)),
    ?assertMatch({ok, _}, stat_on_storage(TestCtx, StorageFileId)),

    % new files shouldn't have been created
    ?assertMatch({error, ?ENOENT}, stat_on_storage(TestCtx, ?STORAGE_PATH(TargetDir))),
    ?assertMatch({error, ?ENOENT}, stat_on_storage(TestCtx, ?STORAGE_PATH(TargetPath))).


remote_unlink_should_not_trigger_unlinking_files_on_local_storage(Config) ->
    TestCtx = #test_ctx{
        ro_node = RoNode, ro_sess_id = RoSessId,
        other_node = OtherNode, other_sess_id = OtherSessId
    } = get_test_ctx(Config),
    FileName = ?FILE_NAME,
    DirName = ?DIR_NAME,
    FileRelativePath = filepath_utils:join([DirName, FileName]),

    {Guid, StorageFileId} = create_file_on_storage_and_register(TestCtx, FileRelativePath, ?TEST_DATA),

    % wait for file to be synchronized to the other provider
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(OtherNode, OtherSessId, ?FILE_REF(Guid)), ?ATTEMPTS),
    ?assertMatch({ok, [{_, DirName}]},
        lfm_proxy:get_children(OtherNode, OtherSessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, [{Guid, FileName}]},
        lfm_proxy:get_children(OtherNode, OtherSessId, {path, ?PATH(DirName)}, 0, 10), ?ATTEMPTS),

    ?assertEqual(ok, lfm_proxy:unlink(OtherNode, OtherSessId, ?FILE_REF(Guid))),

    % wait for file to be unlinked
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(RoNode, RoSessId, ?FILE_REF(Guid)), ?ATTEMPTS),

    % file should still exist on storage
    ?assertMatch({ok, #statbuf{}}, stat_on_storage(TestCtx, StorageFileId)),

    % wait for dir to be synchronized to the other provider
    {ok, #file_attr{guid = DirGuid}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(OtherNode, OtherSessId, {path, ?PATH(DirName)}), ?ATTEMPTS),
    ?assertMatch({ok, [{DirGuid, DirName}]},
        lfm_proxy:get_children(RoNode, RoSessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),
    % it should be possible to remove the directory (only its metadata)
    ?assertEqual(ok, lfm_proxy:unlink(OtherNode, OtherSessId, {path, ?PATH(DirName)})),

    % wait for dir to be unlinked
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(RoNode, RoSessId, ?FILE_REF(DirGuid)), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(RoNode, RoSessId, {path, ?SPACE_PATH}, 0, 10), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(RoNode, RoSessId, ?FILE_REF(Guid)), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:get_children(RoNode, RoSessId, ?FILE_REF(DirGuid), 0, 10), ?ATTEMPTS),

    % directory should still exist on storage
    ?assertMatch({ok, #statbuf{}}, stat_on_storage(TestCtx, ?STORAGE_PATH(DirName))).


remote_recursive_rm_should_not_trigger_removal_of_files_on_local_storage(Config) ->
    TestCtx = #test_ctx{
        ro_node = RoNode, ro_sess_id = RoSessId,
        other_node = OtherNode, other_sess_id = OtherSessId
    } = get_test_ctx(Config),
    DirName = ?DIR_NAME,
    FileRelativePath = filepath_utils:join([DirName, ?FILE_NAME]),

    {Guid, StorageFileId} = create_file_on_storage_and_register(TestCtx, FileRelativePath, ?TEST_DATA),

    % wait for file to be synchronized to the other provider
    {ok, #file_attr{parent_guid = DirGuid}} = ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(OtherNode, OtherSessId, ?FILE_REF(Guid)), ?ATTEMPTS),

    ?assertEqual(ok, lfm_proxy:rm_recursive(OtherNode, OtherSessId, {path, ?PATH(DirName)})),

    % wait for files to be removed
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(RoNode, RoSessId, ?FILE_REF(DirGuid)), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(RoNode, RoSessId, ?FILE_REF(Guid)), ?ATTEMPTS),

    % file and directory should still exist on storage
    ?assertMatch({ok, #statbuf{}}, stat_on_storage(TestCtx, StorageFileId)),
    ?assertMatch({ok, #statbuf{}}, stat_on_storage(TestCtx, ?STORAGE_PATH(DirName))).


remote_truncate_should_not_trigger_truncate_on_storage(Config) ->
    TestCtx = #test_ctx{
        ro_node = RoNode, ro_sess_id = RoSessId,
        other_node = OtherNode, other_sess_id = OtherSessId
    } = get_test_ctx(Config),

    {Guid, StorageFileId} = create_file_on_storage_and_register(TestCtx, ?FILE_NAME, ?TEST_DATA),
    {Guid2, StorageFileId2} = create_file_on_storage_and_register(TestCtx, ?FILE_NAME, ?TEST_DATA),

    % replicate both files to the other provider
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(OtherNode, OtherSessId, ?FILE_REF(Guid), read), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(OtherNode, Handle, 0, 100), ?ATTEMPTS),
    ok = lfm_proxy:close(OtherNode, Handle),

    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(OtherNode, OtherSessId, ?FILE_REF(Guid2), read), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(OtherNode, Handle2, 0, 100), ?ATTEMPTS),
    ok = lfm_proxy:close(OtherNode, Handle2),

    NewSize1 = 0,
    NewSize2 = 1000,
    ?assertEqual(ok, lfm_proxy:truncate(OtherNode, OtherSessId, ?FILE_REF(Guid), NewSize1)),
    ?assertEqual(ok, lfm_proxy:truncate(OtherNode, OtherSessId, ?FILE_REF(Guid2), NewSize2)),

    % logical files' sizes should change
    ?assertMatch({ok, #file_attr{size = NewSize1}}, lfm_proxy:stat(RoNode, RoSessId, ?FILE_REF(Guid)), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{size = NewSize2}}, lfm_proxy:stat(RoNode, RoSessId, ?FILE_REF(Guid2)), ?ATTEMPTS),

    % files should still have old sizes on storage
    TestDataSize = byte_size(?TEST_DATA),
    ?assertMatch({ok, #statbuf{st_size = TestDataSize}}, stat_on_storage(TestCtx, StorageFileId)),
    ?assertMatch({ok, #statbuf{st_size = TestDataSize}}, stat_on_storage(TestCtx, StorageFileId2)).


replication_on_the_fly_should_fail(Config) ->
    #test_ctx{
        ro_node = RoNode, ro_sess_id = RoSessId,
        other_node = OtherNode, other_sess_id = OtherSessId
    } = get_test_ctx(Config),
    TestDataSize = byte_size(?TEST_DATA),

    {ok, {Guid, Handle}} = lfm_proxy:create_and_open(OtherNode, OtherSessId, ?PATH(?FILE_NAME)),
    {ok, _} = lfm_proxy:write(OtherNode, Handle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(OtherNode, Handle),

    ?assertMatch({ok, #file_attr{size = TestDataSize}}, lfm_proxy:stat(RoNode, RoSessId, ?FILE_REF(Guid)), ?ATTEMPTS),
    ?assertEqual({error, ?EROFS}, lfm_proxy:open(RoNode, RoSessId, ?FILE_REF(Guid), read)).


remote_change_should_invalidate_local_file_but_leave_storage_file_unchanged(Config) ->
    TestCtx = #test_ctx{
        ro_node = RoNode, ro_sess_id = RoSessId,
        other_node = OtherNode, other_sess_id = OtherSessId
    } = get_test_ctx(Config),
    TestDataSize = byte_size(?TEST_DATA),
    TestDataSize2 = byte_size(?TEST_DATA2),

    {Guid, StorageFileId} = create_file_on_storage_and_register(TestCtx, ?FILE_NAME, ?TEST_DATA),

    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(OtherNode, OtherSessId, ?FILE_REF(Guid), rdwr), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(OtherNode, Handle, 0, TestDataSize), ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:write(OtherNode, Handle, 0, ?TEST_DATA2)),
    ok = lfm_proxy:close(OtherNode, Handle),

    % whole file on the readonly provider should be invalidated
    ?assertDistribution(RoNode, RoSessId, ?DISTS(provider_ids(), [0, TestDataSize2]), Guid, ?ATTEMPTS),

    ?assertEqual({error, ?EROFS}, lfm_proxy:open(RoNode, RoSessId, ?FILE_REF(Guid), rdwr), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{size = TestDataSize2}}, lfm_proxy:stat(RoNode, RoSessId, ?FILE_REF(Guid)), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, read_from_storage(TestCtx, StorageFileId, 0, TestDataSize)).


remote_change_should_invalidate_local_file_but_leave_storage_file_unchanged2(Config) ->
    TestCtx = #test_ctx{
        ro_node = RoNode, ro_sess_id = RoSessId,
        other_node = OtherNode, other_sess_id = OtherSessId
    } = get_test_ctx(Config),
    TestDataSize = byte_size(?TEST_DATA),
    ChangedByteOffset = 5,

    {Guid, StorageFileId} = create_file_on_storage_and_register(TestCtx, ?FILE_NAME, ?TEST_DATA),

    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(OtherNode, OtherSessId, ?FILE_REF(Guid), rdwr), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(OtherNode, Handle, 0, TestDataSize), ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:write(OtherNode, Handle, ChangedByteOffset, <<"#">>)),
    ok = lfm_proxy:close(OtherNode, Handle),

    % only the changed byte on the readonly provider should be invalidated
    ExpectedRoProviderBlocks = [
        [0, ChangedByteOffset],
        [ChangedByteOffset + 1, TestDataSize - (ChangedByteOffset + 1)]
    ],
    ?assertDistribution(
        RoNode, RoSessId, ?DISTS(provider_ids(), [ExpectedRoProviderBlocks, TestDataSize]), Guid, ?ATTEMPTS
    ),

    ?assertEqual({error, ?EROFS}, lfm_proxy:open(RoNode, RoSessId, ?FILE_REF(Guid), rdwr), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{size = TestDataSize}}, lfm_proxy:stat(RoNode, RoSessId, ?FILE_REF(Guid)), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, read_from_storage(TestCtx, StorageFileId, 0, TestDataSize)).


replication_job_should_fail(Config) ->
    #test_ctx{
        ro_node = RoNode, ro_sess_id = RoSessId,
        other_node = OtherNode, other_sess_id = OtherSessId
    } = get_test_ctx(Config),
    TestDataSize = byte_size(?TEST_DATA),
    [RoProviderId, OtherProviderId] = provider_ids(),

    {ok, {Guid, Handle}} = lfm_proxy:create_and_open(OtherNode, OtherSessId, ?PATH(?FILE_NAME)),
    {ok, _} = lfm_proxy:write(OtherNode, Handle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(OtherNode, Handle),

    ?assertMatch({ok, #file_attr{size = TestDataSize}}, lfm_proxy:stat(RoNode, RoSessId, ?FILE_REF(Guid)), ?ATTEMPTS),
    ?assertDistribution(RoNode, RoSessId, ?DISTS([RoProviderId, OtherProviderId], [0, TestDataSize]), Guid, ?ATTEMPTS),

    ?assertEqual(
        ?ERR_POSIX(?EROFS),
        opt_transfers:schedule_file_replication(RoNode, RoSessId, ?FILE_REF(Guid), RoProviderId)
    ).


eviction_job_should_succeed(Config) ->
    TestCtx = #test_ctx{
        ro_node = RoNode, ro_sess_id = RoSessId,
        other_node = OtherNode, other_sess_id = OtherSessId
    } = get_test_ctx(Config),
    TestDataSize = byte_size(?TEST_DATA),
    [RoProviderId, OtherProviderId] = provider_ids(),

    {Guid, StorageFileId} = create_file_on_storage_and_register(TestCtx, ?FILE_NAME, ?TEST_DATA),

    % replicate the file to the other provider so that the replica can be evicted
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(OtherNode, OtherSessId, ?FILE_REF(Guid), read), ?ATTEMPTS),
    ?assertEqual({ok, ?TEST_DATA}, lfm_proxy:read(OtherNode, Handle, 0, 100), ?ATTEMPTS),
    ok = lfm_proxy:close(OtherNode, Handle),
    ?assertDistribution(
        RoNode, RoSessId, ?DISTS([RoProviderId, OtherProviderId], [TestDataSize, TestDataSize]), Guid, ?ATTEMPTS
    ),

    % Ensure that evicting provider has knowledge of remote provider blocks (through dbsync),
    % as otherwise it will skip eviction.
    % @TODO VFS-9498 not needed after replica_deletion uses fetched file location instead of dbsynced
    ?assertEqual({ok, [[0, TestDataSize]]},
        opt_file_metadata:get_local_knowledge_of_remote_provider_blocks(RoNode, Guid, OtherProviderId), ?ATTEMPTS),

    % evicting the local replica only drops blocks from metadata, so it is allowed
    ?assertMatch(
        {ok, _},
        opt_transfers:schedule_file_replica_eviction(RoNode, RoSessId, ?FILE_REF(Guid), RoProviderId, undefined)
    ),
    ?assertDistribution(
        RoNode, RoSessId, ?DISTS([RoProviderId, OtherProviderId], [0, TestDataSize]), Guid, ?ATTEMPTS
    ),

    % file should still exist on storage
    ?assertEqual({ok, ?TEST_DATA}, read_from_storage(TestCtx, StorageFileId, 0, TestDataSize)).


migration_job_should_fail(Config) ->
    #test_ctx{
        ro_node = RoNode, ro_sess_id = RoSessId,
        other_node = OtherNode, other_sess_id = OtherSessId
    } = get_test_ctx(Config),
    TestDataSize = byte_size(?TEST_DATA),
    [RoProviderId, OtherProviderId] = provider_ids(),

    {ok, {Guid, Handle}} = lfm_proxy:create_and_open(OtherNode, OtherSessId, ?PATH(?FILE_NAME)),
    {ok, _} = lfm_proxy:write(OtherNode, Handle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(OtherNode, Handle),

    ?assertMatch({ok, #file_attr{size = TestDataSize}}, lfm_proxy:stat(RoNode, RoSessId, ?FILE_REF(Guid)), ?ATTEMPTS),
    ?assertDistribution(RoNode, RoSessId, ?DISTS([RoProviderId, OtherProviderId], [0, TestDataSize]), Guid, ?ATTEMPTS),

    % migration is a replication to the readonly provider followed by an eviction
    % on the other one - it must be rejected for the same reason a replication is
    ?assertEqual(
        ?ERR_POSIX(?EROFS),
        opt_transfers:schedule_file_replica_eviction(
            RoNode, RoSessId, ?FILE_REF(Guid), OtherProviderId, RoProviderId
        )
    ).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, storage_file_tree_test_utils],
    opt:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {dbsync_changes_broadcast_interval, timer:seconds(1)}
        ]}],
        posthook = fun(NewConfig) ->
            space_setup_utils:clean_up_after_previous_run([?SPACE_NAME], [?RO_PROVIDER, ?OTHER_PROVIDER]),
            [{test_ctx, set_up_space()} | NewConfig]
        end
    }).


end_per_suite(Config) ->
    % the storage supporting the space is deleted along with it, but the backdoor
    % one supports none and must be disposed of explicitly
    #test_ctx{backdoor_storage_id = BackdoorStorageId} = get_test_ctx(Config),
    space_setup_utils:delete_storage(?RO_PROVIDER, BackdoorStorageId),
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    #test_ctx{
        space_id = SpaceId,
        backdoor_storage_id = BackdoorStorageId,
        ro_node = RoNode,
        other_node = OtherNode
    } = get_test_ctx(Config),

    lfm_test_utils:clean_space(RoNode, [RoNode, OtherNode], SpaceId, ?ATTEMPTS),
    % deleting files through lfm never removes them from a readonly storage, so the
    % storage has to be wiped through the backdoor - otherwise leftovers of one case
    % would show up in the storage listings asserted by another
    wipe_storage(RoNode, SpaceId, BackdoorStorageId),

    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec set_up_space() -> test_ctx().
set_up_space() ->
    % both storages of the readonly provider share a mount point, which is what makes
    % the backdoor one a window onto the readonly one (see the module doc)
    MountPoint = <<"/mnt/st_", (?RAND_STR())/binary>>,
    ReadonlyStorageId = space_setup_utils:create_storage(?RO_PROVIDER, #posix_storage_params{
        mount_point = MountPoint, imported_storage = true, readonly = true
    }),
    BackdoorStorageId = space_setup_utils:create_storage(?RO_PROVIDER, #posix_storage_params{
        mount_point = MountPoint
    }),
    OtherStorageId = space_setup_utils:create_storage(?OTHER_PROVIDER, #posix_storage_params{
        mount_point = <<"/mnt/st_", (?RAND_STR())/binary>>
    }),

    SpaceId = space_setup_utils:set_up_space(#space_spec{
        name = ?SPACE_NAME,
        owner = space_owner,
        users = [?TEST_USER],
        supports = [
            #support_spec{
                provider = ?RO_PROVIDER,
                storage_spec = ReadonlyStorageId,
                size = ?SUPPORT_SIZE,
                % files are put on the readonly storage by the tests themselves,
                % so no scanning is wanted - only manual registration
                storage_import = #{mode => <<"manual">>}
            },
            #support_spec{
                provider = ?OTHER_PROVIDER, storage_spec = OtherStorageId, size = ?SUPPORT_SIZE
            }
        ]
    }),

    #test_ctx{
        space_id = SpaceId,
        readonly_storage_id = ReadonlyStorageId,
        backdoor_storage_id = BackdoorStorageId,
        ro_node = oct_background:get_random_provider_node(?RO_PROVIDER),
        ro_sess_id = oct_background:get_user_session_id(?TEST_USER, ?RO_PROVIDER),
        other_node = oct_background:get_random_provider_node(?OTHER_PROVIDER),
        other_sess_id = oct_background:get_user_session_id(?TEST_USER, ?OTHER_PROVIDER)
    }.


%% @private
-spec get_test_ctx(test_config:config()) -> test_ctx().
get_test_ctx(Config) ->
    ?config(test_ctx, Config).


%% @private
-spec provider_ids() -> [od_provider:id()].
provider_ids() ->
    [oct_background:get_provider_id(?RO_PROVIDER), oct_background:get_provider_id(?OTHER_PROVIDER)].


%% @private
%% @doc Creates the file on the readonly storage (through the backdoor storage,
%% along with its parent directories) and registers it in the space, mirroring
%% how files get onto an imported storage in production.
-spec create_file_on_storage_and_register(test_ctx(), file_meta:path(), binary()) ->
    {file_id:file_guid(), helpers:file_id()}.
create_file_on_storage_and_register(TestCtx = #test_ctx{
    space_id = SpaceId,
    readonly_storage_id = ReadonlyStorageId,
    backdoor_storage_id = BackdoorStorageId,
    ro_node = RoNode,
    ro_sess_id = RoSessId
}, FileRelativePath, Content) ->
    StorageFileId = ?STORAGE_PATH(FileRelativePath),
    ensure_parent_dir_created_on_storage(TestCtx, StorageFileId),
    ok = storage_file_tree_test_utils:create_file(
        ?RO_PROVIDER, BackdoorStorageId, StorageFileId, Content, ?DEFAULT_FILE_PERMS
    ),

    {ok, Guid} = ?assertMatch({ok, _}, opw_test_rpc:call(RoNode, file_registration, register, [
        RoSessId, SpaceId, FileRelativePath, ReadonlyStorageId, StorageFileId,
        #{<<"size">> => byte_size(Content)}
    ])),
    {Guid, StorageFileId}.


%% @private
%% @doc The parent is either the storage root (always there, as it is the space
%% root on an imported storage) or a directory the case has yet to create.
-spec ensure_parent_dir_created_on_storage(test_ctx(), helpers:file_id()) -> ok.
ensure_parent_dir_created_on_storage(#test_ctx{backdoor_storage_id = BackdoorStorageId}, StorageFileId) ->
    {_BaseName, ParentStorageFileId} = filepath_utils:basename_and_parent_dir(StorageFileId),
    case storage_file_tree_test_utils:stat(?RO_PROVIDER, BackdoorStorageId, ParentStorageFileId) of
        {ok, _} ->
            ok;
        {error, ?ENOENT} ->
            storage_file_tree_test_utils:create_dir(
                ?RO_PROVIDER, BackdoorStorageId, ParentStorageFileId, ?DEFAULT_DIR_PERMS
            )
    end.


%% @private
%% @doc Stats the entry through the readonly storage - the very storage the
%% Oneprovider sees, so its view is what the assertions are about.
-spec stat_on_storage(test_ctx(), helpers:file_id()) -> {ok, helpers:stat()} | {error, term()}.
stat_on_storage(#test_ctx{readonly_storage_id = ReadonlyStorageId}, StorageFileId) ->
    storage_file_tree_test_utils:stat(?RO_PROVIDER, ReadonlyStorageId, StorageFileId).


%% @private
-spec read_from_storage(test_ctx(), helpers:file_id(), non_neg_integer(), non_neg_integer()) ->
    {ok, binary()} | {error, term()}.
read_from_storage(#test_ctx{readonly_storage_id = ReadonlyStorageId}, StorageFileId, Offset, Size) ->
    storage_file_tree_test_utils:read_file(?RO_PROVIDER, ReadonlyStorageId, StorageFileId, Offset, Size).


%% @private
-spec list_space_dir_on_storage(test_ctx()) -> {ok, [helpers:file_id()]} | {error, term()}.
list_space_dir_on_storage(#test_ctx{readonly_storage_id = ReadonlyStorageId}) ->
    % on an imported storage the storage root is the space root
    storage_file_tree_test_utils:list_dir(?RO_PROVIDER, ReadonlyStorageId, <<"/">>, 0, 10).


%% @private
-spec wipe_storage(node(), od_space:id(), storage:id()) -> ok.
wipe_storage(Node, SpaceId, BackdoorStorageId) ->
    SDHandle = sd_test_utils:new_handle(Node, SpaceId, <<"/">>, BackdoorStorageId),
    sd_test_utils:recursive_rm(Node, SDHandle, true),
    ?assertMatch({ok, []}, sd_test_utils:ls(Node, SDHandle, 0, 1)),
    ok.


%% @private
%% @doc Renaming a file on storage is triggered by the synchronization of its
%% file_location, and the assertions that follow are negative ones (nothing may
%% change on the storage), so there is no state to await on - only time to give.
-spec await_file_location_synchronization() -> ok.
await_file_location_synchronization() ->
    timer:sleep(timer:seconds(5)).
