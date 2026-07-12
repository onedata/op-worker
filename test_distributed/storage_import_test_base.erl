%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% This module contains base test functions for testing storage import.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_test_base).
-author("Jakub Kudzia").

-include("storage_import_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

% TODO VFS-6162 move utility functions to storage_import_test_utils module
% TODO VFS-12937 - add dir stats check to import tests

% CT functions
-export([init_per_suite/1, init_per_testcase/2, end_per_suite/1, end_per_testcase/2]).

%% util functions
-export([
    enable_initial_scan/2, disable_continuous_scan/1,
    assertInitialScanFinished/2, assertInitialScanFinished/3,
    assertNoScanInProgress/3,
    assert_monitoring_state/4,
    provider_storage_path/2, provider_storage_path/3, get_rdwr_storage/2,
    clean_traverse_tasks/1,
    stop_scan/2,
    get_finished_scans_num/2
]).

% exported for RPC
-export([
    verify_file_deleted/4
]).

%% tests
-export([
    symlink_is_ignored_by_initial_scan/1,
    change_file_type4_test/1
]).

%%%===================================================================
%%% Tests
%%%===================================================================


symlink_is_ignored_by_initial_scan(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SymlinkName = <<"symlink">>,
    SymlinkPath = ?SPACE_TEST_FILE_PATH(SymlinkName),

    % create symlink to file
    {ok, _} = lfm_proxy:make_symlink(W1, SessId, SymlinkPath, <<"dummy symlink value">>),

    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    % check whether symlink was not deleted
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(W1, SessId, {path, SymlinkPath}), ?ATTEMPTS).


change_file_type4_test(Config) ->
    % this test checks whether storage import properly handles
    % deleting non-empty directory, created in remote provider and creating file with the same name on storage
    [W1, W2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {?USER1, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {?USER1, ?GET_DOMAIN(W2)}}, Config),
    RDWRStorage = get_rdwr_storage(Config, W1),
    StorageTestDirPath = provider_storage_path(?SPACE_ID, ?TEST_DIR),
    StorageTestFileInDirPath = provider_storage_path(?SPACE_ID, filename:join([?TEST_DIR, ?TEST_FILE1])),

    %% Create dir and file inside it
    {ok, DirGuid} = lfm_proxy:mkdir(W2, SessId2, ?SPACE_TEST_DIR_PATH),
    {ok, FileGuid} = lfm_proxy:create(W2, SessId2, ?SPACE_TEST_FILE_IN_DIR_PATH),
    {ok, Handle} = lfm_proxy:open(W2, SessId2, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(W2, Handle, 0, ?TEST_DATA),
    ok = lfm_proxy:close(W2, Handle),

    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    {ok, Handle2} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, ?FILE_REF(FileGuid), read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W1, Handle2, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),

    % delete directory and file on storage
    DirSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestDirPath, RDWRStorage),
    FileSDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, StorageTestFileInDirPath, RDWRStorage),

    ok = sd_test_utils:unlink(W1, FileSDHandle, ?TEST_DATA_SIZE),
    sd_test_utils:rmdir(W1, DirSDHandle),
    % create file with the same name like the deleted directory
    ok = sd_test_utils:create_file(W1, DirSDHandle, ?DEFAULT_FILE_PERMS),
    {ok, _} = sd_test_utils:write_file(W1, DirSDHandle, 0, ?TEST_DATA),

    enable_initial_scan(Config, ?SPACE_ID),
    assertInitialScanFinished(W1, ?SPACE_ID),

    ?assertMonitoring(W1, #{
        <<"scans">> => 1,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 2,
        <<"failed">> => 0,
        <<"unmodified">> => 0,
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

    ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
        lfm_proxy:stat(W1, SessId, {path, ?SPACE_TEST_DIR_PATH})),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(W1, SessId, ?FILE_REF(DirGuid))),
    ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
        lfm_proxy:stat(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W2, SessId2, ?FILE_REF(DirGuid)), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(W2, SessId2, ?FILE_REF(FileGuid)), ?ATTEMPTS),

    % check whether we can read the imported file
    {ok, Handle3} = ?assertMatch({ok, _}, lfm_proxy:open(W1, SessId, {path, ?SPACE_TEST_DIR_PATH}, read)),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W1, Handle3, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ok = lfm_proxy:close(W1, Handle3),

    {ok, Handle4} = ?assertMatch({ok, _}, lfm_proxy:open(W2, SessId2, {path, ?SPACE_TEST_DIR_PATH}, read), ?ATTEMPTS),
    ?assertMatch({ok, ?TEST_DATA}, lfm_proxy:read(W2, Handle4, 0, byte_size(?TEST_DATA)), ?ATTEMPTS),
    ok = lfm_proxy:close(W2, Handle4).

%%%===================================================================
%%% Util functions
%%%===================================================================

create_init_file(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    SpaceDir = provider_storage_path(?SPACE_ID, <<"">>),
    RDWRStorage = get_rdwr_storage(Config, W1),
    SDHandle = sd_test_utils:new_handle(W1, ?SPACE_ID, SpaceDir, RDWRStorage),
    case sd_test_utils:mkdir(W1, SDHandle, 8#777) of
        ok -> ok;
        {error, eexist} -> is_empty(W1, SDHandle)
    end.

is_empty(Worker, SDHandle = #sd_handle{storage_id = StorageId}) ->
    Helper = rpc:call(Worker, storage, get_helper, [StorageId]),
    HelperName = helper:get_name(Helper),
    ?assertMatch({ok, []},
        sd_test_utils:storage_ls(Worker, SDHandle, 0, 1, HelperName), ?ATTEMPTS).

stop_scan(Worker, SpaceId) ->
    case rpc:call(Worker, storage_import, stop_auto_scan, [SpaceId]) of
        ok -> ok;
        {error, not_found} -> ok
    end.

enable_initial_scan(Config, SpaceId) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    ImportConfig = ?config(import_config, Config, #{}),
    MaxDepth = maps:get(max_depth, ImportConfig, ?MAX_DEPTH),
    SyncAcl = maps:get(sync_acl, ImportConfig, ?SYNC_ACL),
    ?assertMatch(ok, rpc:call(W1, storage_import, set_or_configure_auto_mode,
        [SpaceId, #{max_depth => MaxDepth, sync_acl => SyncAcl}])).

cleanup_storage_import_monitoring_model(Worker, SpaceId) ->
    rpc:call(Worker, storage_import_monitoring, delete, [SpaceId]).

monitoring_describe(Worker, SpaceId) ->
    rpc:call(Worker, storage_import_monitoring, describe, [SpaceId]).

get_finished_scans_num(Worker, SpaceId) ->
    #{<<"scans">> := Scans} = monitoring_describe(Worker, SpaceId),
    Scans.

get_scan_config(Worker, SpaceId) ->
    {ok, ImportConfig} = rpc:call(Worker, storage_import, get_configuration, [SpaceId]),
    {ok, maps:get(auto_storage_import_config, ImportConfig)}.

disable_continuous_scan(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    rpc:call(W1, storage_import, set_or_configure_auto_mode, [?SPACE_ID, #{continuous_scan => false}]),
    ?assertMatch({ok, #{continuous_scan := false}}, get_scan_config(W1, ?SPACE_ID), ?ATTEMPTS),
    ok.

disable_storage_sync(Config) ->
    [W1, _] = ?config(op_worker_nodes, Config),
    disable_continuous_scan(Config),
    stop_scan(W1, ?SPACE_ID),
    assertNoScanInProgress(W1, ?SPACE_ID, 600),
    ok = rpc:call(W1, storage_import_config, delete, [?SPACE_ID]).

clean_not_imported_storage(Config) ->
    [_W1, W2 | _] = ?config(op_worker_nodes, Config),
    Storage = initializer:get_supporting_storage_id(W2, ?SPACE_ID),
    clean_storage(W2, Storage, false).

clean_synced_storage(Config) ->
    [W1 | _] = ?config(op_worker_nodes, Config),
    Storage = get_rdwr_storage(Config, W1),
    clean_storage(W1, Storage, true).

clean_storage(Worker, Storage, ImportedStorage) ->
    SpaceDir = provider_storage_path(?SPACE_ID, <<"">>, ImportedStorage),
    SDHandle = sd_test_utils:new_handle(Worker, ?SPACE_ID, SpaceDir, Storage),
    ok = sd_test_utils:recursive_rm(Worker, SDHandle, true).

clean_space(Config) ->
    [W, W2 | _] = ?config(op_worker_nodes, Config),
    SpaceDirGuid = space_dir:guid(?SPACE_ID),
    lfm_proxy:close_all(W),
    {ok, Children} = lfm_proxy:get_children(W, ?ROOT_SESS_ID, ?FILE_REF(SpaceDirGuid), 0, 10000),
    Attempts = 600,
    Self = self(),
    Guids = lists:filtermap(fun({Guid, Name}) ->
        case Name =:= ?TRASH_DIR_NAME of
            true ->
                false;
            false ->
                ok = lfm_proxy:rm_recursive(W, ?ROOT_SESS_ID, ?FILE_REF(Guid)),
                ok = worker_pool:cast(?VERIFY_POOL, {?MODULE, verify_file_deleted, [W2, Guid, Self, Attempts]}),
                {true, Guid}
        end
    end, Children),
    verify_deletions(Guids, Attempts),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W, ?ROOT_SESS_ID, ?FILE_REF(SpaceDirGuid), 0, 10000), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(W2, ?ROOT_SESS_ID, ?FILE_REF(SpaceDirGuid), 0, 10000), ?ATTEMPTS).


verify_deletions(Guids, Timeout) ->
    verify_deletions(Guids, [], Timeout).

verify_deletions([], [], _) ->
    ok;
verify_deletions([], _FailedToVerifyGuids, _) ->
    ct:fail("Cleaning space failed");
verify_deletions(FileGuids, FailedToVerifyGuids, Timeout) ->
    receive
        {deleted, FileGuid} ->
            verify_deletions(FileGuids -- [FileGuid], FailedToVerifyGuids, Timeout);
        {deleting_failed, FileGuid} ->
            verify_deletions(FileGuids, [FileGuid | FailedToVerifyGuids], Timeout)
    after
        timer:seconds(Timeout) ->
            ct:fail("Cleaning space failed")
    end.

verify_file_deleted(Worker, FileGuid, Master, Attempts) ->
    try
        ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(Worker, ?ROOT_SESS_ID, ?FILE_REF(FileGuid)), Attempts),
        Master ! {deleted, FileGuid}
    catch
        _:_ ->
            Master ! {deleting_failed, FileGuid}
    end.

clean_luma_db(Worker) ->
    {ok, StorageIds} = rpc:call(Worker, provider_logic, get_storages, []),
    lists:foreach(fun(StorageId) ->
        ok = rpc:call(Worker, luma, clear_db, [StorageId])
    end, StorageIds).


add_synced_storages(Config) ->
    SyncedStorages = add_storages(Config, fun is_synced/2),
    [{synced_storages, SyncedStorages} | Config].

add_rdwr_storages(Config) ->
    RDWRStorages = add_storages(Config, fun is_rdwr/2),
    [{rdwr_storages, RDWRStorages} | Config].


add_storages(Config, CheckStorageFun) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foldl(fun(W, AccIn) ->
        {ok, StorageIds} = rpc:call(W, provider_logic, get_storages, []),
        Storages = lists:map(fun(StorageId) ->
            {ok, Storage} = rpc:call(W, storage, get, [StorageId]),
            Storage
        end, StorageIds),
        case find_storage(Storages, CheckStorageFun) of
            undefined -> AccIn;
            FoundStorage -> AccIn#{W => FoundStorage}
        end
    end, #{}, Workers).

find_storage(Storages, CheckStorageFun) ->
    lists:foldl(fun
        (Storage, undefined) ->
            Helper = storage:get_helper(Storage),
            Id = storage:get_id(Storage),
            case CheckStorageFun(Id, Helper) of
                true -> Storage;
                false -> undefined
            end;
        (_, FoundStorage) ->
            FoundStorage
    end, undefined, Storages).

is_rdwr(StorageId, #helper{name = ?POSIX_HELPER_NAME}) ->
    match =:= re:run(StorageId, <<"rdwr_storage">>, [{capture, none}]);
is_rdwr(<<"rdwr_storage">>, #helper{name = ?S3_HELPER_NAME}) ->
    true;
is_rdwr(_, _) ->
    false.

is_synced(StorageId, #helper{name = ?POSIX_HELPER_NAME}) ->
    match =:= re:run(StorageId, <<"synced_storage">>, [{capture, none}]);
is_synced(<<"synced_storage">>, #helper{name = ?S3_HELPER_NAME}) ->
    true.


get_rdwr_storage(Config, Worker) ->
    case maps:get(Worker, ?config(rdwr_storages, Config), undefined) of
        undefined -> get_synced_storage(Config, Worker);
        Storage -> Storage
    end.

get_synced_storage(Config, Worker) ->
    maps:get(Worker, ?config(synced_storages, Config), undefined).

provider_storage_path(SpaceId, File) ->
    provider_storage_path(SpaceId, File, true).

provider_storage_path(_SpaceId, File, _MountInRoot = true) ->
    filename:join([<<"/">>, File]);
provider_storage_path(SpaceId, FileName, _MountInRoot = false) ->
    filename:join([<<"/">>, SpaceId, FileName]).

assertInitialScanFinished(Worker, SpaceId) ->
    assertInitialScanFinished(Worker, SpaceId, ?ATTEMPTS).

assertInitialScanFinished(Worker, SpaceId, Attempts) ->
    ?assertEqual(true, try
        rpc:call(Worker, storage_import_monitoring, is_initial_scan_finished, [SpaceId])
    catch
        _:_ ->
            error
    end, Attempts).

assertNoScanInProgress(Worker, SpaceId, Attempts) ->
    ?assertEqual(false, try
        rpc:call(Worker, storage_import_monitoring, is_scan_in_progress, [SpaceId])
    catch
        _:_ ->
            error
    end, Attempts).

assert_monitoring_state(Worker, ExpectedSSM, SpaceId, Attempts) ->
    SSM = monitoring_describe(Worker, SpaceId),
    SSM2 = flatten_histograms(SSM),
    try
        assert(ExpectedSSM, SSM2),
        SSM2
    catch
        throw:{assertion_error, Key, ExpectedValue, Value}:Stacktrace ->
            case Attempts == 0 of
                false ->
                    timer:sleep(timer:seconds(1)),
                    assert_monitoring_state(Worker, ExpectedSSM, SpaceId, Attempts - 1);
                true ->
                    {Format, Args} = storage_import_monitoring_description(SSM),
                    ct:pal(
                        "Assertion of field \"~tp\" in storage_import_monitoring for space ~tp failed.~n"
                        "    Expected: ~tp~n"
                        "    Value: ~tp~n"
                        ++ Format ++
                        "~nStacktrace:~n~tp",
                        [Key, SpaceId, ExpectedValue, Value] ++ Args ++ [Stacktrace]),
                    ct:fail("assertion failed")
            end
    end.

assert(ExpectedSSM, SSM) ->
    maps:fold(fun(Key, Value, _AccIn) ->
        assert_for_key(Key, Value, SSM)
    end, undefined, ExpectedSSM).

assert_for_key(Key, ExpectedValue, SSM) ->
    Value = maps:get(Key, SSM),
    case Value of
        ExpectedValue -> ok;
        _ ->
            throw({assertion_error, Key, ExpectedValue, Value})
    end.

flatten_histograms(SSM) ->
    SSM#{
        % flatten beginnings of histograms for assertions
        <<"createdMinHist">> => lists:sum(lists:sublist(maps:get(<<"createdMinHist">>, SSM), 2)),
        <<"modifiedMinHist">> => lists:sum(lists:sublist(maps:get(<<"modifiedMinHist">>, SSM), 2)),
        <<"deletedMinHist">> => lists:sum(lists:sublist(maps:get(<<"deletedMinHist">>, SSM), 2)),
        <<"queueLengthMinHist">> => hd(maps:get(<<"queueLengthMinHist">>, SSM)),

        <<"createdHourHist">> => lists:sum(lists:sublist(maps:get(<<"createdHourHist">>, SSM), 3)),
        <<"modifiedHourHist">> => lists:sum(lists:sublist(maps:get(<<"modifiedHourHist">>, SSM), 3)),
        <<"deletedHourHist">> => lists:sum(lists:sublist(maps:get(<<"deletedHourHist">>, SSM), 3)),
        <<"queueLengthHourHist">> => hd(maps:get(<<"queueLengthHourHist">>, SSM)),

        <<"createdDayHist">> => lists:sum(lists:sublist(maps:get(<<"createdDayHist">>, SSM), 1)),
        <<"modifiedDayHist">> => lists:sum(lists:sublist(maps:get(<<"modifiedDayHist">>, SSM), 1)),
        <<"deletedDayHist">> => lists:sum(lists:sublist(maps:get(<<"deletedDayHist">>, SSM), 1)),
        <<"queueLengthDayHist">> => hd(maps:get(<<"queueLengthDayHist">>, SSM))
    }.

storage_import_monitoring_description(SSM) ->
    maps:fold(fun(Key, Value, {AccFormat, AccArgs}) ->
        {AccFormat ++ "    ~tp = ~tp~n", AccArgs ++ [Key, Value]}
    end, {"~n#storage_import_monitoring fields values:~n", []}, SSM).

clean_traverse_tasks(Worker) ->
    Pool = <<"storage_sync_traverse">>,
    ?assertMatch({ok, [], _}, rpc:call(Worker, traverse_task_list, list, [Pool, ongoing]), ?ATTEMPTS),
    {ok, TaskIds, _} = rpc:call(Worker, traverse_task_list, list, [Pool, ended]),
    lists:foreach(fun(T) ->
        ok = rpc:call(Worker, traverse_task, delete_ended, [Pool, T])
    end, TaskIds),
    ?assertMatch({ok, [], _}, rpc:call(Worker, traverse_task_list, list, [Pool, ended])).

mock_link_handling_method(Workers) ->
    ok = test_utils:mock_new(Workers, fslogic_delete),
    ok = test_utils:mock_expect(Workers, fslogic_delete, get_open_file_handling_method, fun(Ctx) ->
        {?SET_DELETION_MARKER, Ctx}
    end).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        ssl:start(),
        application:ensure_all_started(hackney),
        initializer:disable_quota_limit(NewConfig),
        initializer:mock_provider_ids(NewConfig),
        initializer:mock_auth_manager(NewConfig),
        NewConfig2 = multi_provider_file_ops_test_base:init_env(NewConfig),
        [W1 | _] = ?config(op_worker_nodes, NewConfig2),
        rpc:call(W1, auto_storage_import_worker, notify_connection_to_oz, []),
        NewConfig2
    end,
    {ok, _} = application:ensure_all_started(worker_pool),
    {ok, _} = worker_pool:start_sup_pool(?VERIFY_POOL, [{workers, 8}]),
    [{?LOAD_MODULES, [initializer, storage_import_test_base, sd_test_utils]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    ok = wpool:stop_sup_pool(?VERIFY_POOL),
    multi_provider_file_ops_test_base:teardown_env(Config),
    initializer:unmock_auth_manager(Config),
    initializer:unmock_provider_ids(?config(op_worker_nodes, Config)).

init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ct:timetrap({minutes, 60}),
    mock_link_handling_method(Workers),
    ConfigWithProxy = lfm_proxy:init(Config),
    Config2 = add_synced_storages(ConfigWithProxy),
    Config3 = add_rdwr_storages(Config2),
    create_init_file(Config3),
    Config3.

end_per_testcase(_Case, Config) ->
    Workers = [W1 | _] = ?config(op_worker_nodes, Config),
    lists:foreach(fun(W) -> lfm_proxy:close_all(W) end, Workers),
    clean_luma_db(W1),
    disable_storage_sync(Config),
    clean_traverse_tasks(W1),
    clean_space(Config),
    test_utils:mock_unload(Workers, [helpers]),
    clean_synced_storage(Config),
    clean_not_imported_storage(Config),
    cleanup_storage_import_monitoring_model(W1, ?SPACE_ID),
    test_utils:mock_unload(Workers, [storage_import_engine, storage_import_hash, deletion_marker,
        storage_sync_traverse, storage_import_deletion, storage_driver, helpers]),
    timer:sleep(timer:seconds(1)),
    lfm_proxy:teardown(Config).