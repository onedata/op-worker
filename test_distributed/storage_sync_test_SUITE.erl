%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage_sync
%%% @end
%%%--------------------------------------------------------------------
-module(storage_sync_test_SUITE).
-author("Rafal Slota").
-author("Jakub Kudzia").

-include("storage_sync_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("kernel/include/file.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2, create_empty_file_import_test/1]).

%% tests
-export([
    create_directory_import_test/1, create_directory_export_test/1,
    create_file_import_test/1,
    create_remote_file_import_conflict_test/1,
    create_remote_dir_import_race_test/1,
    create_remote_file_import_race_test/1,
    sync_should_not_import_replicated_file_with_suffix_on_storage/1,
    sync_should_update_blocks_of_recreated_file_with_suffix_on_storage/1,
    sync_should_not_import_recreated_file_with_suffix_on_storage/1,
    sync_should_update_replicated_file_with_suffix_on_storage/1,
    create_delete_import_test_read_remote_only/1,
    create_file_export_test/1,
    delete_empty_directory_update_test/1,
    delete_non_empty_directory_update_test/1,
    sync_works_properly_after_delete_test/1,
    delete_and_update_files_simultaneously_update_test/1,
    delete_directory_export_test/1,
    delete_file_update_test/1,
    delete_file_export_test/1,
    append_file_update_test/1,
    append_file_export_test/1,
    copy_file_update_test/1,
    move_file_update_test/1,
    truncate_file_update_test/1,
    chmod_file_update_test/1,
    change_file_type_test/1,
    change_file_type2_test/1,
    change_file_type3_test/1,
    change_file_type4_test/1,
    update_timestamps_file_import_test/1,
    create_file_in_dir_import_test/1,
    create_file_in_dir_update_test/1,
    create_file_in_dir_exceed_batch_update_test/1,
    chmod_file_update2_test/1,
    should_not_detect_timestamp_update_test/1,
    create_directory_import_many_test/1,
    create_subfiles_import_many_test/1,
    create_directory_import_without_read_permission_test/1,
    create_subfiles_import_many2_test/1,
    create_subfiles_and_delete_before_import_is_finished_test/1,
    create_directory_import_check_user_id_test/1,
    create_directory_import_check_user_id_error_test/1,
    create_file_import_check_user_id_test/1,
    create_file_import_check_user_id_error_test/1,
    import_nfs_acl_test/1,
    update_nfs_acl_test/1,
    create_directory_import_error_test/1,
    update_syncs_files_after_import_failed_test/1,
    update_syncs_files_after_previous_update_failed_test/1,
    create_delete_import2_test/1,
    sync_should_not_reimport_file_when_link_is_missing_but_file_on_storage_has_not_changed/1,
    sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage/1,
    recreate_file_deleted_by_sync_test/1,
    sync_should_not_delete_not_replicated_file_created_in_remote_provider/1,
    sync_should_not_delete_dir_created_in_remote_provider/1,
    sync_should_not_delete_not_replicated_files_created_in_remote_provider2/1,
    create_delete_import_test_read_both/1,
    should_not_sync_file_during_replication/1,
    change_file_content_constant_size_test/1,
    change_file_content_update_test/1,
    change_file_content_the_same_moment_when_sync_performs_stat_on_file_test/1,
    append_empty_file_update_test/1,
    append_file_not_changing_mtime_update_test/1,
    sync_should_not_invalidate_file_after_replication/1,
    import_nfs_acl_with_disabled_luma_should_fail_test/1,
    delete_many_subfiles_test/1,
    sync_should_not_reimport_deleted_but_still_opened_file/1,
    create_file_import_race_test/1,
    close_file_import_race_test/1,
    delete_file_reimport_race_test/1,
    delete_opened_file_reimport_race_test/1,
    recreate_delete_race_test/1,
    create_list_race_test/1
]).

-define(TEST_CASES, [
    create_directory_import_test,
    create_directory_import_error_test,
    update_syncs_files_after_import_failed_test,
    update_syncs_files_after_previous_update_failed_test,
    create_directory_import_check_user_id_test,
    create_directory_import_check_user_id_error_test,
    create_directory_import_without_read_permission_test,
    create_directory_import_many_test,
    create_directory_export_test,
    create_file_import_test,
    create_remote_file_import_conflict_test,
    create_remote_dir_import_race_test,
    create_remote_file_import_race_test,
    % todo VFS-5203 add more tests of sync handling suffixed files
    sync_should_not_reimport_deleted_but_still_opened_file,
    sync_should_not_import_recreated_file_with_suffix_on_storage,
    sync_should_update_blocks_of_recreated_file_with_suffix_on_storage,
    sync_should_not_import_replicated_file_with_suffix_on_storage,
    sync_should_update_replicated_file_with_suffix_on_storage,
    create_empty_file_import_test,
    create_delete_import_test_read_both,
    create_delete_import_test_read_remote_only,
    create_delete_import2_test,
    sync_should_not_reimport_file_when_link_is_missing_but_file_on_storage_has_not_changed,
    sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage,
    create_file_import_check_user_id_test,
    create_file_import_check_user_id_error_test,
    create_file_export_test,
    create_file_in_dir_import_test,
    create_subfiles_import_many_test,
    create_subfiles_import_many2_test,
    create_subfiles_and_delete_before_import_is_finished_test,
    create_file_in_dir_update_test,
    create_file_in_dir_exceed_batch_update_test,
    delete_empty_directory_update_test,
    delete_non_empty_directory_update_test,
    sync_works_properly_after_delete_test,
    delete_and_update_files_simultaneously_update_test,
    delete_directory_export_test,
    delete_file_update_test,
    delete_many_subfiles_test,
    delete_file_export_test,
    append_file_update_test,
    append_file_not_changing_mtime_update_test,
    append_empty_file_update_test,
    append_file_export_test,
    copy_file_update_test,
    move_file_update_test,
    truncate_file_update_test,
    change_file_content_constant_size_test,
    change_file_content_update_test,
    change_file_content_the_same_moment_when_sync_performs_stat_on_file_test,
    chmod_file_update_test,
    chmod_file_update2_test,
    change_file_type_test,
    change_file_type2_test,
    change_file_type3_test,
    change_file_type4_test,
    update_timestamps_file_import_test,
    should_not_detect_timestamp_update_test,
    recreate_file_deleted_by_sync_test,
    sync_should_not_delete_not_replicated_file_created_in_remote_provider,
    sync_should_not_delete_dir_created_in_remote_provider,
    sync_should_not_delete_not_replicated_files_created_in_remote_provider2,
    import_nfs_acl_test,
    update_nfs_acl_test,
    import_nfs_acl_with_disabled_luma_should_fail_test,
    should_not_sync_file_during_replication,
    sync_should_not_invalidate_file_after_replication,
    create_file_import_race_test,
    close_file_import_race_test,
    delete_file_reimport_race_test,
    delete_opened_file_reimport_race_test,
    recreate_delete_race_test,
    create_list_race_test
]).

all() -> ?ALL(?TEST_CASES).

-define(WRITE_TEXT, <<"overwrite_test_data">>).

%%%==================================================================
%%% Test functions
%%%===================================================================

create_file_import_race_test(Config) ->
    [W1, W2 | _] = Workers = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),

    Master = self(),
    CreateProc = spawn(fun() ->
        Ans = receive
            create ->
                try
                    {ok, _} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#777),
                    {ok, Handle} = lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, write),
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

    test_utils:mock_new(Workers, simple_scan, [passthrough]),
    test_utils:mock_expect(Workers, simple_scan, import_file,
        fun(Job) ->
            CreateProc ! create,
            timer:sleep(5000),
            meck:passthrough([Job])
        end),

    StorageTestFilePath = storage_sync_test_base:storage_test_file_path(
        W1MountPoint, ?SPACE_ID, ?TEST_FILE1, false),
    %% Create file on storage
    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),

    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    CreateAns = receive
        {create_ans, A} -> A
    after
        60000 -> timeout
    end,
    ?assertEqual(ok, CreateAns),

    %% Check if file was imported on W1
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?WRITE_TEXT},
        lfm_proxy:read(W1, Handle1, 0, 1000)),
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
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), 5 * ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?WRITE_TEXT},
        lfm_proxy:read(W2, Handle2, 0, 100), ?ATTEMPTS).

close_file_import_race_test(Config) ->
    [W1 | _] = Workers = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),

    {ok, {_, CreateHandle}} = lfm_proxy:create_and_open(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#777),
    {ok, _} = lfm_proxy:write(W1, CreateHandle, 0, ?WRITE_TEXT),
    ok = lfm_proxy:unlink(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}),

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

    storage_sync_test_base:enable_storage_import(Config),

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
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1})).

delete_file_reimport_race_test(Config) ->
    % todo ogarnac czy mockować tą funckje czy moze jednak check_location_and_maybe_sync
    % in this test, we check whether sync does not reimport file that is deleted between checking links and file_location
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    MountInRoot = false,
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageSpacePath = storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, <<"">>, MountInRoot),

    %% Create file
    {ok, FileGuid} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, Handle1} = lfm_proxy:open(W1, SessId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(W1, Handle1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, Handle1),

    timer:sleep(timer:seconds(1)),

    TestProcess = self(),
    ok = test_utils:mock_new(W1, simple_scan),
    ok = test_utils:mock_expect(W1, simple_scan, check_location_and_maybe_sync, fun(Job, FileCtx) ->
        Guid = file_ctx:get_guid_const(FileCtx),
        case Guid =:= FileGuid of
            true ->
                TestProcess ! {syncing_process, self()},
                receive
                    continue ->
                        meck:passthrough([Job, FileCtx])
                end;
            false ->
                meck:passthrough([Job, FileCtx])
        end
    end),

    % touch space dir to ensure that it will be scanned
    {ok, #file_info{mtime = Mtime}} = file:read_file_info(StorageSpacePath, [{time, posix}]),
    ok = storage_sync_test_base:change_time(StorageSpacePath, Mtime + 1),

    storage_sync_test_base:enable_storage_import(Config),
    receive
        {syncing_process, SyncingProcess} ->
            ok = lfm_proxy:unlink(W1, SessId, {guid, FileGuid}),
            SyncingProcess ! continue
    end,

    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was not reimported on W1
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:ls(W1, SessId, {path, ?SPACE_PATH}, 0, 1)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1, % space_dir will have updated time
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
    }, ?SPACE_ID),

    %% Check if file was deleted on W2
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:ls(W2, SessId2, {path, ?SPACE_PATH}, 0, 1)).

delete_opened_file_reimport_race_test(Config) ->
    % in this test, we check whether sync does not reimport file that is deleted while still opened and moved
    % to hidden directory between checking links and file_location
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    MountInRoot = false,
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    StorageSpacePath = storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, <<"">>, MountInRoot),

    %% Create file
    {ok, FileGuid} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, Handle1} = lfm_proxy:open(W1, SessId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(W1, Handle1, 0, ?TEST_DATA),

    timer:sleep(timer:seconds(1)),

    TestProcess = self(),
    ok = test_utils:mock_new(W1, simple_scan),
    ok = test_utils:mock_expect(W1, simple_scan, check_location_and_maybe_sync, fun(Job, FileCtx) ->
        Guid = file_ctx:get_guid_const(FileCtx),
        case Guid =:= FileGuid of
            true ->
                TestProcess ! {syncing_process, self()},
                receive continue -> meck:passthrough([Job, FileCtx]) end;
            false ->
                meck:passthrough([Job, FileCtx])
        end
    end),

    % touch space dir to ensure that it will be scanned
    {ok, #file_info{mtime = Mtime}} = file:read_file_info(StorageSpacePath, [{time, posix}]),
    ok = storage_sync_test_base:change_time(StorageSpacePath, Mtime + 1),

    storage_sync_test_base:enable_storage_import(Config),
    receive
        {syncing_process, SyncingProcess} ->
            ok = lfm_proxy:unlink(W1, SessId, {guid, FileGuid}),
            SyncingProcess ! continue
    end,

    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    %% Check if file was not reimported on W1
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:ls(W1, SessId, {path, ?SPACE_PATH}, 0, 1)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1, % space_dir will have updated time
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
    }, ?SPACE_ID),

    %% Check if file was deleted on W2
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:ls(W2, SessId2, {path, ?SPACE_PATH}, 0, 1)).

recreate_delete_race_test(Config) ->
    storage_sync_test_base:recreate_delete_race_test(Config, false).

create_list_race_test(Config) ->
    storage_sync_test_base:create_list_race_test(Config, false).

create_directory_import_test(Config) ->
    storage_sync_test_base:create_directory_import_test(Config, false).

create_directory_import_error_test(Config) ->
    storage_sync_test_base:create_directory_import_error_test(Config, false).

update_syncs_files_after_import_failed_test(Config) ->
    storage_sync_test_base:update_syncs_files_after_import_failed_test(Config, false).

update_syncs_files_after_previous_update_failed_test(Config) ->
    storage_sync_test_base:update_syncs_files_after_previous_update_failed_test(Config, false).

create_directory_import_check_user_id_test(Config) ->
    storage_sync_test_base:create_directory_import_check_user_id_test(Config, false).

create_directory_import_check_user_id_error_test(Config) ->
    storage_sync_test_base:create_directory_import_check_user_id_error_test(Config, false).

create_directory_import_without_read_permission_test(Config) ->
    storage_sync_test_base:create_directory_import_without_read_permission_test(Config, false).

create_directory_import_many_test(Config) ->
    storage_sync_test_base:create_directory_import_many_test(Config, false).

create_directory_export_test(Config) ->
    storage_sync_test_base:create_directory_export_test(Config, false).

create_file_import_test(Config) ->
    storage_sync_test_base:create_file_import_test(Config, false).

create_remote_file_import_conflict_test(Config) ->
    storage_sync_test_base:create_remote_file_import_conflict_test(Config, false).

create_remote_dir_import_race_test(Config) ->
    storage_sync_test_base:create_remote_dir_import_race_test(Config, false).

create_remote_file_import_race_test(Config) ->
    storage_sync_test_base:create_remote_file_import_race_test(Config, false).

sync_should_not_reimport_deleted_but_still_opened_file(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),

    StorageSpacePath =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, <<"">>, false),

    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change

    % create first file
    {ok, G1} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, H1} = lfm_proxy:open(W1, SessId, {guid, G1}, write),
    {ok, _} = lfm_proxy:write(W1, H1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, H1),

    % open file
    ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, G1}, read), ?ATTEMPTS),

    % delete file
    ok = lfm_proxy:unlink(W1, SessId, {guid, G1}),

    % there should be 1 file on storage
    ?assertMatch({ok, [_]}, file:list_dir(StorageSpacePath)),

    storage_sync_test_base:enable_storage_import(Config),
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

sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage(Config) ->
    MountSpaceInRoot = false,
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),
    TestDir = ?config(test_dir, Config),
    StorageTestDirPath =
        storage_sync_test_base:storage_test_dir_path(W1MountPoint, ?SPACE_ID, TestDir, MountSpaceInRoot),
    StorageSpaceDirPath =
        storage_sync_test_base:storage_test_dir_path(W1MountPoint, ?SPACE_ID, <<"">>, MountSpaceInRoot),
    SpaceTetDirPath = ?SPACE_TEST_DIR_PATH(TestDir),
    ok = file:make_dir(StorageTestDirPath),
    storage_sync_test_base:enable_storage_import(Config),

    % wait till scan is finished
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    %% Check if dir was imported
    ?assertMatch({ok, [{_, TestDir}]},
        lfm_proxy:ls(W1, SessId, {path, ?SPACE_PATH}, 0, 10)),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, SpaceTetDirPath}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W2, SessId2, {path, SpaceTetDirPath}), ?ATTEMPTS),

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

    % mock error from helpers:rmdir
    ok = test_utils:mock_new(W1, helpers),
    ok = test_utils:mock_expect(W1, helpers, rmdir, fun(_, _) -> {error, ?ENOTEMPTY} end),

    ?assertEqual(ok,
        lfm_proxy:rm_recursive(W1, SessId, {path, SpaceTetDirPath})),

    % touch space dir to make sure that it will be updated
    {ok, #file_info{mtime = StMtime}} = file:read_file_info(StorageSpaceDirPath, [{time, posix}]),
    timer:sleep(timer:seconds(1)),
    storage_sync_test_base:change_time(StorageSpaceDirPath, StMtime + 1),

    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_storage_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 2,
        <<"imported">> => 0,
        <<"updated">> => 1, % space dir was updated because we performed "touch" on it
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1, % test dir was ignored
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

    % TestDir should not be reimported
    ?assertMatch({ok, []},
        lfm_proxy:ls(W1, SessId, {path, ?SPACE_PATH}, 0, 10)),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, {path, SpaceTetDirPath}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W2, SessId2, {path, SpaceTetDirPath}), ?ATTEMPTS).


sync_should_not_import_recreated_file_with_suffix_on_storage(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),

    StorageSpacePath =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, <<"">>, false),

    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change

    % create first file
    {ok, G1} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, H1} = lfm_proxy:open(W1, SessId, {guid, G1}, write),
    {ok, _} = lfm_proxy:write(W1, H1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, H1),

    % open file
    ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, G1}, read), ?ATTEMPTS),

    % delete file
    ok = lfm_proxy:unlink(W1, SessId, {guid, G1}),

    % recreate file with the same name as the deleted file
    {ok, G2} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, H3} = lfm_proxy:open(W1, SessId, {guid, G2}, write),
    {ok, _} = lfm_proxy:write(W1, H3, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, H3),

    % there should be 2 files on storage
    ?assertMatch({ok, [_, _]}, file:list_dir(StorageSpacePath)),

    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    % there should be only 1 file visible in the space
    ?assertMatch({ok, [_]},
        lfm_proxy:ls(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
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

sync_should_update_blocks_of_recreated_file_with_suffix_on_storage(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),

    StorageSpacePath =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, <<"">>, false),
    StorageTestFilePath =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, false),

    timer:sleep(timer:seconds(1)), %ensure that space_dir mtime will change

    % create first file
    {ok, G1} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, H1} = lfm_proxy:open(W1, SessId, {guid, G1}, write),
    {ok, _} = lfm_proxy:write(W1, H1, 0, ?TEST_DATA2),
    ok = lfm_proxy:close(W1, H1),

    timer:sleep(timer:seconds(1)), %ensure that file1 will be updated

    % open file
    {ok, _} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, G1}, read), ?ATTEMPTS),

    % delete file
    ok = lfm_proxy:unlink(W1, SessId, {guid, G1}),

    % create second file with the same name as the deleted file
    {ok, G2} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#666),
    {ok, H3} = lfm_proxy:open(W1, SessId, {guid, G2}, write),
    {ok, _} = lfm_proxy:write(W1, H3, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, H3),
    U2 = file_id:guid_to_uuid(G2),
    StorageTestFilePathWithSuffix = ?CONFLICTING_STORAGE_FILE_NAME(StorageTestFilePath, U2),

    % there should be 2 files on storage
    ?assertMatch({ok, [_, _]}, file:list_dir(StorageSpacePath)),

    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    % there should be only 1 file visible in the space
    ?assertMatch({ok, [_]},
        lfm_proxy:ls(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
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

    % change one byte in the suffixed file
    ok = file:write_file(StorageTestFilePathWithSuffix, ?TEST_DATA_ONE_BYTE_CHANGED),

    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    {ok, H4} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {guid, G2}, read)),
    ?assertMatch({ok, ?TEST_DATA_ONE_BYTE_CHANGED},
        lfm_proxy:read(W1, H4, 0, 100), ?ATTEMPTS),

    %% Check if file was updated on W2
    {ok, H5} = ?assertMatch({ok, _},
        lfm_proxy:open(W2, SessId2, {guid, G2}, read)),
    ?assertMatch({ok, ?TEST_DATA_ONE_BYTE_CHANGED},
        lfm_proxy:read(W2, H5, 0, 100), ?ATTEMPTS).


sync_should_not_import_replicated_file_with_suffix_on_storage(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),

    StorageSpacePath =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, <<"">>, false),

    {ok, G1} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, H1} = lfm_proxy:open(W1, SessId, {guid, G1}, write),
    {ok, _} = lfm_proxy:write(W1, H1, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W1, H1),

    {ok, G2} = lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, H2} = lfm_proxy:open(W2, SessId2, {guid, G2}, write),
    {ok, _} = lfm_proxy:write(W2, H2, 0, ?TEST_DATA2),
    ok = lfm_proxy:close(W2, H2),

    % replicate file to W1
    {ok, H3} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, G2}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA2}, lfm_proxy:read(W1, H3, 0, 100), ?ATTEMPTS),
    ok = lfm_proxy:close(W1, H3),


    % there should be 2 files on storage
    ?assertMatch({ok, [_, _]}, file:list_dir(StorageSpacePath)),

    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    % there should be only 2 files visible in the space
    ?assertMatch({ok, [_, _]},
        lfm_proxy:ls(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)),

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

sync_should_update_replicated_file_with_suffix_on_storage(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),

    StorageSpacePath =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, <<"">>, false),
    StorageTestFilePath =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, false),

    {ok, G1} = lfm_proxy:create(W1, SessId, ?SPACE_TEST_FILE_PATH1, 8#644),
    {ok, H1} = lfm_proxy:open(W1, SessId, {guid, G1}, write),
    {ok, _} = lfm_proxy:write(W1, H1, 0, ?TEST_DATA2),
    ok = lfm_proxy:close(W1, H1),

    {ok, G2} = lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH1, 8#666),
    {ok, H2} = lfm_proxy:open(W2, SessId2, {guid, G2}, write),
    {ok, _} = lfm_proxy:write(W2, H2, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W2, H2),
    U2 = file_id:guid_to_uuid(G2),
    StorageTestFilePathWithSuffix = ?CONFLICTING_STORAGE_FILE_NAME(StorageTestFilePath, U2),

    % replicate file to W1
    {ok, H3} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, G2}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W1, H3, 0, 100)),
    ok = lfm_proxy:close(W1, H3),

    % there should be 2 files on storage
    ?assertMatch({ok, [_, _]}, file:list_dir(StorageSpacePath)),

    storage_sync_test_base:enable_storage_import(Config),
    storage_sync_test_base:assertImportTimes(W1, ?SPACE_ID),

    % there should be only 2 files visible in the space
    ?assertMatch({ok, [_, _]},
        lfm_proxy:ls(W1, SessId, {path, <<"/", (?SPACE_NAME)/binary>>}, 0, 100)),

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
    ok = file:write_file(StorageTestFilePathWithSuffix, ?TEST_DATA_ONE_BYTE_CHANGED),

    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),
    storage_sync_test_base:disable_storage_update(Config),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"imported">> => 0,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 2,
        <<"importedSum">> => 0,
        <<"updatedSum">> => 3,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 0,
        <<"importedHourHist">> => 0,
        <<"importedDayHist">> => 0,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 3,
        <<"updatedDayHist">> => 3,
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

create_empty_file_import_test(Config) ->
    storage_sync_test_base:create_empty_file_import_test(Config, false).

create_delete_import_test_read_both(Config) ->
    storage_sync_test_base:create_delete_import_test_read_both(Config, false).

create_delete_import_test_read_remote_only(Config) ->
    storage_sync_test_base:create_delete_import_test_read_remote_only(Config, false).

create_delete_import2_test(Config) ->
    storage_sync_test_base:create_delete_import2_test(Config, false, true).

sync_should_not_reimport_file_when_link_is_missing_but_file_on_storage_has_not_changed(Config) ->
    storage_sync_test_base:sync_should_not_reimport_file_when_link_is_missing_but_file_on_storage_has_not_changed(Config, false).

create_file_import_check_user_id_test(Config) ->
    storage_sync_test_base:create_file_import_check_user_id_test(Config, false).

create_file_import_check_user_id_error_test(Config) ->
    storage_sync_test_base:create_file_import_check_user_id_error_test(Config, false).

create_file_export_test(Config) ->
    storage_sync_test_base:create_file_export_test(Config, false).

create_file_in_dir_import_test(Config) ->
    storage_sync_test_base:create_file_in_dir_import_test(Config, false).

create_subfiles_import_many_test(Config) ->
    storage_sync_test_base:create_subfiles_import_many_test(Config, false).

create_subfiles_import_many2_test(Config) ->
    storage_sync_test_base:create_subfiles_import_many2_test(Config, false).

create_subfiles_and_delete_before_import_is_finished_test(Config) ->
    storage_sync_test_base:create_subfiles_and_delete_before_import_is_finished_test(Config, false).

create_file_in_dir_update_test(Config) ->
    storage_sync_test_base:create_file_in_dir_update_test(Config, false).

create_file_in_dir_exceed_batch_update_test(Config) ->
    storage_sync_test_base:create_file_in_dir_exceed_batch_update_test(Config, false).

delete_empty_directory_update_test(Config) ->
    storage_sync_test_base:delete_empty_directory_update_test(Config, false).

delete_non_empty_directory_update_test(Config) ->
    storage_sync_test_base:delete_non_empty_directory_update_test(Config, false).

sync_works_properly_after_delete_test(Config) ->
    storage_sync_test_base:sync_works_properly_after_delete_test(Config, false).

delete_and_update_files_simultaneously_update_test(Config) ->
    storage_sync_test_base:delete_and_update_files_simultaneously_update_test(Config, false).

delete_directory_export_test(Config) ->
    storage_sync_test_base:delete_directory_export_test(Config, false).

delete_file_update_test(Config) ->
    storage_sync_test_base:delete_file_update_test(Config, false).

delete_many_subfiles_test(Config) ->
    storage_sync_test_base:delete_many_subfiles_test(Config, false).

delete_file_export_test(Config) ->
    storage_sync_test_base:delete_file_export_test(Config, false).

append_file_update_test(Config) ->
    storage_sync_test_base:append_file_update_test(Config, false).

append_file_not_changing_mtime_update_test(Config) ->
    storage_sync_test_base:append_file_not_changing_mtime_update_test(Config, false).

append_empty_file_update_test(Config) ->
    storage_sync_test_base:append_empty_file_update_test(Config, false).

append_file_export_test(Config) ->
    storage_sync_test_base:append_file_export_test(Config, false).

copy_file_update_test(Config) ->
    %% WARNING in this test we check whether copy of a file was detected
    %% and whether copied file's timestamps were updated
    %% for some reason when volume is mount as read_write to docker
    %% timestamp of a copied file is not updated
    %% because of that, this test was adapted to this bug

    MountSpaceInRoot = false,
    [W1 | _] = ?config(op_worker_nodes, Config),
    W1MountPoint = storage_sync_test_base:get_host_mount_point(W1, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    StorageTestFilePath =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE1, MountSpaceInRoot),
    StorageTestFilePath2 =
        storage_sync_test_base:storage_test_file_path(W1MountPoint, ?SPACE_ID, ?TEST_FILE2, MountSpaceInRoot),
    %% Create file on storage
    ok = file:write_file(StorageTestFilePath, ?TEST_DATA),
    storage_sync_test_base:enable_storage_import(Config),
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
    }, ?SPACE_ID),

    %% Check if file was imported
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),
    {ok, Handle1} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle1, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle1),

    timer:sleep(timer:seconds(2)), %ensure that copy time is different from read time
    %% Copy file
    {ok, _} = file:copy(StorageTestFilePath, StorageTestFilePath2),

    storage_sync_test_base:enable_storage_update(Config),
    storage_sync_test_base:assertUpdateTimes(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 2,
        <<"toProcess">> => 3,
        <<"imported">> => 1,
        <<"updated">> => 1,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"otherProcessed">> => 1,
        <<"importedSum">> => 2,
        <<"updatedSum">> => 2,
        <<"deletedSum">> => 0,
        <<"importedMinHist">> => 1,
        <<"importedHourHist">> => 2,
        <<"importedDayHist">> => 2,
        <<"updatedMinHist">> => 1,
        <<"updatedHourHist">> => 2,
        <<"updatedDayHist">> => 2,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }, ?SPACE_ID),

    %% Check if copied file was imported
    {ok, Handle2} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH2}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle2),
    {ok, Handle3} = ?assertMatch({ok, _},
        lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}, read)),
    ?assertMatch({ok, ?TEST_DATA},
        lfm_proxy:read(W1, Handle3, 0, byte_size(?TEST_DATA))),
    lfm_proxy:close(W1, Handle3).

move_file_update_test(Config) ->
    storage_sync_test_base:move_file_update_test(Config, false).

truncate_file_update_test(Config) ->
    storage_sync_test_base:truncate_file_update_test(Config, false).

change_file_content_constant_size_test(Config) ->
    storage_sync_test_base:change_file_content_constant_size_test(Config, false).

change_file_content_update_test(Config) ->
    storage_sync_test_base:change_file_content_update_test(Config, false).

change_file_content_the_same_moment_when_sync_performs_stat_on_file_test(Config) ->
    storage_sync_test_base:change_file_content_the_same_moment_when_sync_performs_stat_on_file_test(Config, false).

chmod_file_update_test(Config) ->
    storage_sync_test_base:chmod_file_update_test(Config, false).

chmod_file_update2_test(Config) ->
    storage_sync_test_base:chmod_file_update2_test(Config, false).

change_file_type_test(Config) ->
    storage_sync_test_base:change_file_type_test(Config, false).

change_file_type2_test(Config) ->
    storage_sync_test_base:change_file_type2_test(Config, false).

change_file_type3_test(Config) ->
    storage_sync_test_base:change_file_type3_test(Config, false).

change_file_type4_test(Config) ->
    storage_sync_test_base:change_file_type4_test(Config, false).

update_timestamps_file_import_test(Config) ->
    storage_sync_test_base:update_timestamps_file_import_test(Config, false).

should_not_detect_timestamp_update_test(Config) ->
    storage_sync_test_base:should_not_detect_timestamp_update_test(Config, false).

import_nfs_acl_test(Config) ->
    storage_sync_test_base:import_nfs_acl_test(Config, false).

update_nfs_acl_test(Config) ->
    storage_sync_test_base:update_nfs_acl_test(Config, false).

recreate_file_deleted_by_sync_test(Config) ->
    storage_sync_test_base:recreate_file_deleted_by_sync_test(Config, false).

sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config) ->
    storage_sync_test_base:sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config, false).

sync_should_not_delete_dir_created_in_remote_provider(Config) ->
    storage_sync_test_base:sync_should_not_delete_dir_created_in_remote_provider(Config, false).

sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config) ->
    storage_sync_test_base:sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config, false).

should_not_sync_file_during_replication(Config) ->
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER, ?GET_DOMAIN(W2)}}, Config),

    {ok, FileGuid} =
        ?assertMatch({ok, _}, lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_PATH1, 8#777)),

    %check if file_meta was synced
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_FILE_PATH1}), ?ATTEMPTS),

    {ok, FileHandle} =
        ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {guid, FileGuid}, write)),
    Size = 1024 * 1024 * 100,
    TestData = crypto:strong_rand_bytes(Size),
    ?assertEqual({ok, Size}, lfm_proxy:write(W2, FileHandle, 0, TestData)),
    ?assertEqual(ok, lfm_proxy:fsync(W2, FileHandle)),
    ok = lfm_proxy:close(W2, FileHandle),

    spawn(fun() ->
        timer:sleep(timer:seconds(2)),
        storage_sync_test_base:enable_storage_import(Config),
        storage_sync_test_base:enable_storage_update(Config)
    end),

    {ok, FileHandle2} =
        ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {guid, FileGuid}, read)),
    ?assertMatch({ok, TestData},
        lfm_proxy:read(W1, FileHandle2, 0, Size), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{size = Size}},
        lfm_proxy:stat(W2, SessId2, {guid, FileGuid})).

import_nfs_acl_with_disabled_luma_should_fail_test(Config) ->
    storage_sync_test_base:import_nfs_acl_with_disabled_luma_should_fail_test(Config, false).

sync_should_not_invalidate_file_after_replication(Config) ->
    storage_sync_test_base:sync_should_not_invalidate_file_after_replication(Config).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        ssl:start(),
        hackney:start(),
        initializer:disable_quota_limit(NewConfig),
        initializer:mock_provider_ids(NewConfig),
        multi_provider_file_ops_test_base:init_env(NewConfig)
    end,
    {ok, _} = application:ensure_all_started(worker_pool),
    {ok, _} = worker_pool:start_sup_pool(?VERIFY_POOL, [{workers, 8}]),
    [{?LOAD_MODULES, [initializer, storage_sync_test_base]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    ok = wpool:stop_sup_pool(?VERIFY_POOL),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    initializer:unmock_provider_ids(Config),
    ssl:stop().


init_per_testcase(Case, Config) when
    Case =:= create_directory_import_check_user_id_test;
    Case =:= create_file_import_check_user_id_test ->

    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [reverse_luma_proxy, storage_file_ctx]),
    test_utils:mock_expect(Workers, storage_file_ctx, get_storage_doc, fun(Ctx) ->
        {StorageDoc = #document{value = Storage = #storage{}}, Ctx2} = meck:passthrough([Ctx]),
        {StorageDoc#document{value = Storage#storage{luma_config = ?LUMA_CONFIG}}, Ctx2}
    end),
    test_utils:mock_expect(Workers, reverse_luma_proxy, get_group_id, fun(_, _, _, _, _) ->
        {ok, ?GROUP}
    end),
    test_utils:mock_expect(Workers, reverse_luma_proxy, get_user_id, fun(_, _, _, _) ->
        {ok, ?USER}
    end),
    init_per_testcase(default, Config);

init_per_testcase(Case, Config) when
    Case =:= create_directory_import_check_user_id_error_test;
    Case =:= create_file_import_check_user_id_error_test ->

    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [reverse_luma_proxy, storage_file_ctx]),
    test_utils:mock_expect(Workers, storage_file_ctx, get_storage_doc, fun(Ctx) ->
        {StorageDoc = #document{value = Storage = #storage{}}, Ctx2} = meck:passthrough([Ctx]),
        {StorageDoc#document{value = Storage#storage{luma_config = ?LUMA_CONFIG}}, Ctx2}
    end),
    test_utils:mock_expect(Workers, reverse_luma_proxy, get_group_id, fun(_, _, _, _, _) ->
        {ok, ?GROUP}
    end),
    test_utils:mock_expect(Workers, reverse_luma_proxy, get_user_id, fun(_, _, _, _) ->
        error(test_error)
    end),
    init_per_testcase(default, Config);

init_per_testcase(Case, Config) when
    Case =:= create_file_in_dir_update_test;
    Case =:= should_not_detect_timestamp_update_test ->

    Config2 = [
        {update_config, #{
            delete_enable => false,
            write_once => true}} | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config) when
    Case =:= delete_empty_directory_update_test;
    Case =:= delete_non_empty_directory_update_test;
    Case =:= delete_file_update_test;
    Case =:= delete_many_subfiles_test;
    Case =:= move_file_update_test;
    Case =:= change_file_type_test;
    Case =:= change_file_type2_test;
    Case =:= change_file_type3_test;
    Case =:= change_file_type4_test;
    Case =:= create_subfiles_and_delete_before_import_is_finished_test
    ->
    Config2 = [
        {update_config, #{
            delete_enable => true,
            write_once => true}} | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config) when
    Case =:= delete_and_update_files_simultaneously_update_test;
    Case =:= create_delete_import2_test;
    Case =:= recreate_file_deleted_by_sync_test;
    Case =:= recreate_delete_race_test;
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
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config) when
    Case =:= create_file_in_dir_exceed_batch_update_test
    ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, OldDirBatchSize} = test_utils:get_env(W1, op_worker, dir_batch_size),
    test_utils:set_env(W1, op_worker, dir_batch_size, 2),
    Config2 = [
        {update_config, #{
            delete_enable => false,
            write_once => true}},
        {old_dir_batch_size, OldDirBatchSize}
        | Config
    ],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config) when
    Case =:= create_list_race_test
    ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, OldDirBatchSize} = test_utils:get_env(W1, op_worker, dir_batch_size),
    test_utils:set_env(W1, op_worker, dir_batch_size, 2),
    Config2 = [
        {update_config, #{
            delete_enable => true,
            write_once => false}},
        {old_dir_batch_size, OldDirBatchSize}
        | Config
    ],
    init_per_testcase(default, Config2);


init_per_testcase(Case, Config) when
    Case =:= chmod_file_update2_test
    ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    {ok, OldDirBatchSize} = test_utils:get_env(W1, op_worker, dir_batch_size),
    test_utils:set_env(W1, op_worker, dir_batch_size, 2),
    Config2 = [{old_dir_batch_size, OldDirBatchSize} | Config],
    init_per_testcase(default, Config2);

init_per_testcase(Case, Config) when
    Case =:= import_nfs_acl_test;
    Case =:= update_nfs_acl_test
    ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [storage_file_manager, reverse_luma_proxy, storage_file_ctx]),

    test_utils:mock_expect(Workers, storage_file_ctx, get_storage_doc, fun(Ctx) ->
        {StorageDoc = #document{value = Storage = #storage{}}, Ctx2} = meck:passthrough([Ctx]),
        {StorageDoc#document{value = Storage#storage{luma_config = ?LUMA_CONFIG}}, Ctx2}
    end),

    test_utils:mock_expect(Workers, reverse_luma_proxy, get_user_id, fun(_, _, _, _) ->
        {ok, ?USER}
    end),

    test_utils:mock_expect(Workers, reverse_luma_proxy, get_user_id_by_name, fun(_, _, _, _) ->
        {ok, ?USER}
    end),

    test_utils:mock_expect(Workers, reverse_luma_proxy, get_group_id, fun(_, _, _, _, _) ->
        {ok, ?GROUP}
    end),

    test_utils:mock_expect(Workers, reverse_luma_proxy, get_group_id_by_name, fun(_, _, _, _, _) ->
        {ok, ?GROUP2}
    end),

    EncACL = nfs4_acl:encode(?ACL),
    test_utils:mock_expect(Workers, storage_file_manager, getxattr, fun
        (Handle = #sfm_handle{file = <<"/space1">>}, Ctx) ->
            meck:passthrough([Handle, Ctx]);
        (#sfm_handle{}, _) ->
            {ok, EncACL}
    end),
    init_per_testcase(default, Config);

init_per_testcase(Case, Config) when
    Case =:= import_nfs_acl_with_disabled_luma_should_fail_test ->

    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [storage_file_manager]),

    EncACL = nfs4_acl:encode(?ACL),
    test_utils:mock_expect(Workers, storage_file_manager, getxattr, fun
        (Handle = #sfm_handle{file = <<"/space1">>}, Ctx) ->
            meck:passthrough([Handle, Ctx]);
        (#sfm_handle{}, _) ->
            {ok, EncACL}
    end),
    init_per_testcase(default, Config);

init_per_testcase(sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage, Config) ->
    % generate random dir name
    TestDir = <<"<random_dir", (integer_to_binary(rand:uniform(1000)))/binary>>,
    init_per_testcase(default, [{test_dir, TestDir} | Config]);

init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, fslogic_delete, [passthrough]),
    test_utils:mock_expect(Workers, fslogic_delete, get_open_file_handling_method,
        fun(Ctx) -> {deletion_link, Ctx} end),
    ct:timetrap({minutes, 20}),
    ConfigWithProxy = lfm_proxy:init(Config),
    Config2 = storage_sync_test_base:add_workers_storage_mount_points(ConfigWithProxy),
    storage_sync_test_base:create_init_file(Config2, false),
    Config2.

end_per_testcase(Case, Config) when
    Case =:= chmod_file_update2_test orelse
    Case =:= create_file_in_dir_exceed_batch_update_test orelse
    Case =:= create_list_race_test
    ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    OldDirBatchSize = ?config(old_dir_batch_size, Config),
    test_utils:set_env(W1, op_worker, dir_batch_size, OldDirBatchSize),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config) when
    Case =:= create_directory_import_check_user_id_test;
    Case =:= create_directory_import_check_user_id_error_test;
    Case =:= create_file_import_check_user_id_test;
    Case =:= create_file_import_check_user_id_error_test ->

    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, [reverse_luma_proxy, storage_file_ctx]),
    end_per_testcase(default, Config);

end_per_testcase(Case, Config) when
    Case =:= import_nfs_acl_test;
    Case =:= update_nfs_acl_test
    ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, [reverse_luma_proxy, storage_file_ctx, storage_file_manager]),
    end_per_testcase(default, Config);

end_per_testcase(import_nfs_acl_with_disabled_luma_should_fail_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, [storage_file_manager]),
    end_per_testcase(default, Config);

end_per_testcase(sync_should_not_reimport_file_when_link_is_missing_but_file_on_storage_has_not_changed, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, [fslogic_path]),
    end_per_testcase(default, Config);

end_per_testcase(sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage, Config) ->
    [W1| _] = ?config(op_worker_nodes, Config),
    % remove stalled deletion link
    TestDir = ?config(test_dir, Config),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_ID),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_ID),
    SpaceCtx = file_ctx:new_by_guid(SpaceGuid),
    {ok, DirUuid} = rpc:call(W1, link_utils, try_to_resolve_child_deletion_link, [TestDir, SpaceCtx]),
    DirGuid = file_id:pack_guid(DirUuid, ?SPACE_ID),
    FileCtx = file_ctx:new_by_guid(DirGuid),
    rpc:call(W1, link_utils, remove_deletion_link, [FileCtx, SpaceUuid]),
    end_per_testcase(default, Config);

end_per_testcase(create_remote_dir_import_race_test, Config) ->
    [W1| _] = ?config(op_worker_nodes, Config),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_ID),
    storage_sync_test_base:delete_link(W1, SpaceUuid, ?TEST_DIR),
    end_per_testcase(default, Config);

end_per_testcase(create_remote_file_import_race_test, Config) ->
    [W1| _] = ?config(op_worker_nodes, Config),
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(?SPACE_ID),
    storage_sync_test_base:delete_link(W1, SpaceUuid, ?TEST_FILE1),
    end_per_testcase(default, Config);

end_per_testcase(_Case, Config) ->
    Workers = [W1 | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(W) -> lfm_proxy:close_all(W) end, Workers),
    storage_sync_test_base:clean_reverse_luma_cache(W1),
    storage_sync_test_base:disable_storage_sync(Config),
    storage_sync_test_base:clean_storage(Config, false),
    storage_sync_test_base:clean_space(Config),
    storage_sync_test_base:cleanup_storage_sync_monitoring_model(W1, ?SPACE_ID),
    lfm_proxy:teardown(Config),
    test_utils:mock_unload(Workers, [simple_scan, storage_sync_changes, link_utils, fslogic_delete, helpers, full_update]),
    timer:sleep(timer:seconds(1)).

%%%===================================================================
%%% Internal functions
%%%===================================================================
