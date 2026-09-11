%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test verifies if cluster upgrade procedures (employed during software
%%% upgrades) work as expected.
%%%
%%% Each upgrade_from_<release>_* case prepares data the way the given release
%%% left it and runs a single cluster upgrade step. Just like during an actual
%%% upgrade, the step runs on the current code, so documents stored in an older
%%% record version are upgraded to the current one when first read - before the
%%% step gets to process them. That is why the cases assert in terms of the
%%% current records. Record upgrades themselves are covered by
%%% datastore_record_upgrade_test.
%%% @end
%%%-------------------------------------------------------------------
-module(cluster_upgrade_test_SUITE).
-author("Michal Stanisz").

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/storage/import/storage_import.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/dataset/archivisation_tree.hrl").
-include("luma_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").


%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).
-export([
    upgrade_from_20_02_1_space_strategies/1,
    upgrade_from_20_02_1_storage_sync_monitoring/1,
    upgrade_from_21_02_2_tmp_dir/1,
    upgrade_from_21_02_3_missing_dirs/1,
    upgrade_from_21_02_5_links_reconciliation_traverses/1,
    upgrade_from_21_02_8_upgrade_swift_storage/1,
    upgrade_from_21_02_8_luma/1,
    upgrade_from_25_0_trash/1
]).

-define(SPACE1_ID, <<"space_id1">>).

-define(DUMMY_SPACE_ID1, <<"dummy">>).
-define(DUMMY_SPACE_ID2, <<"bigger_dummy">>).

-define(HELPERS_21_02_8, [
    ?POSIX_HELPER(?POSIX_ADMIN_CREDENTIALS),
    ?CEPH_HELPER(?CEPH_ADMIN_CREDENTIALS),
    ?S3_HELPER(?S3_ADMIN_CREDENTIALS),
    % swift storages are upgraded differently (see upgrade_from_21_02_8_upgrade_swift_storage)
%%    ?SWIFT_HELPER(?SWIFT_ADMIN_CREDENTIALS),
    ?CEPHRADOS_HELPER(?CEPHRADOS_ADMIN_CREDENTIALS),
    ?GLUSTERFS_HELPER(?GLUSTERFS_ADMIN_CREDENTIALS),
    ?NULLDEVICE_HELPER(?NULLDEVICE_ADMIN_CREDENTIALS),
    ?WEBDAV_HELPER(?WEBDAV_BASIC_ADMIN_CREDENTIALS)
]).


%%%===================================================================
%%% API functions
%%%===================================================================

all() -> ?ALL([
    upgrade_from_20_02_1_space_strategies,
    upgrade_from_20_02_1_storage_sync_monitoring,
    upgrade_from_21_02_2_tmp_dir,
    upgrade_from_21_02_3_missing_dirs,
    upgrade_from_21_02_5_links_reconciliation_traverses,
    upgrade_from_21_02_8_upgrade_swift_storage,
    upgrade_from_21_02_8_luma,
    upgrade_from_25_0_trash
]).

%%%===================================================================
%%% Tests
%%%===================================================================


upgrade_from_20_02_1_space_strategies(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceStrategiesCtx = space_strategies:get_ctx(),

    % space supported by not imported storage
    SpaceId1 = <<"space_id1">>,
    StorageId1 = <<"storage1">>,
    SpaceStrategiesDoc1 = #document{
        key = SpaceId1,
        value = #space_strategies{
            sync_configs = #{
                StorageId1 => #storage_sync_config{
                    import_enabled = false
                }
            }
        }
    },

    % space supported by imported storage, with disabled import
    SpaceId2 = <<"space_id2">>,
    StorageId2 = <<"storage2">>,
    SpaceStrategiesDoc2 = #document{
        key = SpaceId2,
        value = #space_strategies{
            sync_configs = #{
                StorageId2 => #storage_sync_config{
                    import_enabled = false
                }
            }
        }
    },
    ExpectedConfig2 = #{mode => ?MANUAL_IMPORT},

    % space supported by imported storage, with enabled import
    SpaceId3 = <<"space_id3">>,
    StorageId3 = <<"storage3">>,
    MaxDepth3 = 10,
    SyncAcl3 = true,
    SpaceStrategiesDoc3 = #document{
        key = SpaceId3,
        value = #space_strategies{
            sync_configs = #{
                StorageId3 => #storage_sync_config{
                    import_enabled = true,
                    import_config = #{
                        max_depth => MaxDepth3,
                        sync_acl => SyncAcl3
                    }
                }
            }
        }
    },
    ExpectedConfig3 = #{
        mode => ?AUTO_IMPORT,
        auto_storage_import_config => #{
            continuous_scan => false,
            max_depth => MaxDepth3,
            sync_acl => SyncAcl3,
            detect_deletions => true,
            detect_modifications => true,
            scan_interval => 60
        }
    },

    % space supported by imported storage, with enabled import and continuous scans
    SpaceId4 = <<"space_id4">>,
    StorageId4 = <<"storage4">>,
    MaxDepth4 = 100,
    SyncAcl4 = false,
    ScanInterval = 60,
    DeleteEnable = false,
    WriteOnce = true,
    SpaceStrategiesDoc4 = #document{
        key = SpaceId4,
        value = #space_strategies{
            sync_configs = #{
                StorageId4 => #storage_sync_config{
                    import_enabled = true,
                    import_config = #{
                        max_depth => MaxDepth3,
                        sync_acl => SyncAcl3
                    },
                    update_enabled = true,
                    update_config = #{
                        max_depth => MaxDepth4,
                        sync_acl => SyncAcl4,
                        scan_interval => ScanInterval,
                        delete_enable => DeleteEnable,
                        write_once => WriteOnce
                    }
                }
            }
        }
    },
    ExpectedConfig4 = #{
        mode => ?AUTO_IMPORT,
        auto_storage_import_config => #{
            continuous_scan => true,
            max_depth => MaxDepth4,
            sync_acl => SyncAcl4,
            detect_deletions => DeleteEnable,
            detect_modifications => not WriteOnce,
            scan_interval => ScanInterval
        }
    },

    {ok, _} = create_doc(Worker, SpaceStrategiesCtx, SpaceStrategiesDoc1),
    {ok, _} = create_doc(Worker, SpaceStrategiesCtx, SpaceStrategiesDoc2),
    {ok, _} = create_doc(Worker, SpaceStrategiesCtx, SpaceStrategiesDoc3),
    {ok, _} = create_doc(Worker, SpaceStrategiesCtx, SpaceStrategiesDoc4),

    ?assertEqual({ok, 4}, rpc:call(Worker, node_manager_plugin, upgrade_cluster, [3])),

    % space_strategies docs should be deleted
    ?assertEqual({error, not_found}, get_doc(Worker, SpaceStrategiesCtx, SpaceId1)),
    ?assertEqual({error, not_found}, get_doc(Worker, SpaceStrategiesCtx, SpaceId2)),
    ?assertEqual({error, not_found}, get_doc(Worker, SpaceStrategiesCtx, SpaceId3)),

    % storage_import_config docs should be created for spaces in which auto import is enabled
    ?assertEqual({error, not_found}, get_storage_import_scan_config(Worker, SpaceId1)),
    ?assertEqual({ok, ExpectedConfig2}, get_storage_import_scan_config(Worker, SpaceId2)),
    ?assertEqual({ok, ExpectedConfig3}, get_storage_import_scan_config(Worker, SpaceId3)),
    ?assertEqual({ok, ExpectedConfig4}, get_storage_import_scan_config(Worker, SpaceId4)).


upgrade_from_20_02_1_storage_sync_monitoring(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SSMCtx = storage_sync_monitoring:get_ctx(),

    SpaceId1 = <<"space_id1">>,
    {ok, Storage1} = rpc:call(Worker, space_logic, get_local_supporting_storage, [SpaceId1]),
    SpaceId2 = <<"space_id2">>,
    {ok, Storage2} = rpc:call(Worker, space_logic, get_local_supporting_storage, [SpaceId2]),
    SpaceId3 = <<"space_id3">>,
    {ok, Storage3} = rpc:call(Worker, space_logic, get_local_supporting_storage, [SpaceId3]),
    SpaceId4 = <<"space_id4">>,
    {ok, Storage4} = rpc:call(Worker, space_logic, get_local_supporting_storage, [SpaceId4]),
    SpaceId5 = <<"space_id5">>,
    {ok, Storage5} = rpc:call(Worker, space_logic, get_local_supporting_storage, [SpaceId5]),

    Scans = 10,
    ToProcess = 100,
    Imported = 50,
    Updated = 25,
    Deleted = 5,
    Failed = 0,
    OtherProcessed = 20,
    ImportedSum = 1000,
    UpdatedSum = 2000,
    DeletedSum = 3000,
    Timestamp = global_clock:timestamp_seconds(),
    HistLength = 12,
    EmptyMinHist = time_slot_histogram:new(Timestamp, 60 div HistLength, HistLength),
    EmptyHourHist = time_slot_histogram:new(Timestamp, 3600 div HistLength, HistLength),
    EmptyDayHist = time_slot_histogram:new(Timestamp, 86400 div HistLength, HistLength),
    EmptyCumulativeMinHist = time_slot_histogram:new_cumulative(Timestamp, 60 div HistLength, HistLength),
    EmptyCumulativeHourHist = time_slot_histogram:new_cumulative(Timestamp, 3600 div HistLength, HistLength),
    EmptyCumulativeDayHist = time_slot_histogram:new_cumulative(Timestamp, 86400 div HistLength, HistLength),

    SSMBase = #storage_sync_monitoring{
        scans = Scans,
        to_process = ToProcess,
        imported = Imported,
        updated = Updated,
        deleted = Deleted,
        failed = Failed,
        other_processed = OtherProcessed,

        imported_sum = ImportedSum,
        updated_sum = UpdatedSum,
        deleted_sum = DeletedSum,

        imported_min_hist = EmptyMinHist,
        imported_hour_hist = EmptyHourHist,
        imported_day_hist = EmptyDayHist,

        updated_min_hist = EmptyMinHist,
        updated_hour_hist = EmptyHourHist,
        updated_day_hist = EmptyDayHist,

        deleted_min_hist = EmptyMinHist,
        deleted_hour_hist = EmptyHourHist,
        deleted_day_hist = EmptyDayHist,

        queue_length_min_hist = EmptyCumulativeMinHist,
        queue_length_hour_hist = EmptyCumulativeHourHist,
        queue_length_day_hist = EmptyCumulativeDayHist
    },

    SIMBase = #storage_import_monitoring{
        finished_scans = Scans,
        created = Imported,
        modified = Updated,
        deleted = Deleted,
        failed = Failed,
        unmodified = 0,
        created_min_hist = EmptyMinHist,
        created_hour_hist = EmptyHourHist,
        created_day_hist = EmptyDayHist,
        modified_min_hist = EmptyMinHist,
        modified_hour_hist = EmptyHourHist,
        modified_day_hist = EmptyDayHist,
        deleted_min_hist = EmptyMinHist,
        deleted_hour_hist = EmptyHourHist,
        deleted_day_hist = EmptyDayHist,
        queue_length_min_hist = EmptyCumulativeMinHist,
        queue_length_hour_hist = EmptyCumulativeHourHist,
        queue_length_day_hist = EmptyCumulativeDayHist
    },

    SSMDoc1 = #document{
        key = rpc:call(Worker, storage_sync_monitoring, id, [SpaceId1, Storage1]),
        value = SSMBase
    },
    SIMDoc1 = #document{
        key = SpaceId1,
        value = SIMBase#storage_import_monitoring{status = ?ENQUEUED},
        version = storage_import_monitoring:get_record_version()
    },

    ImportStartTime = 10,
    SSMDoc2 = #document{
        key = rpc:call(Worker, storage_sync_monitoring, id, [SpaceId2, Storage2]),
        value = SSMBase#storage_sync_monitoring{
            import_start_time = ImportStartTime
        }
    },
    SIMDoc2 = #document{
        key = SpaceId2,
        value = SIMBase#storage_import_monitoring{
            scan_start_time = ImportStartTime * 1000,
            status = ?RUNNING
        },
        version = storage_import_monitoring:get_record_version()
    },

    ImportFinishTime = 15,
    SSMDoc3 = #document{
        key = rpc:call(Worker, storage_sync_monitoring, id, [SpaceId3, Storage3]),
        value = SSMBase#storage_sync_monitoring{
            import_start_time = ImportStartTime,
            import_finish_time = ImportFinishTime
        }
    },
    SIMDoc3 = #document{
        key = SpaceId3,
        value = SIMBase#storage_import_monitoring{
            scan_start_time = ImportStartTime * 1000,
            scan_stop_time = ImportFinishTime * 1000,
            status = ?COMPLETED
        },
        version = storage_import_monitoring:get_record_version()
    },

    LastUpdateStartTime = 20,
    SSMDoc4 = #document{
        key = rpc:call(Worker, storage_sync_monitoring, id, [SpaceId4, Storage4]),
        value = SSMBase#storage_sync_monitoring{
            import_start_time = ImportStartTime,
            import_finish_time = ImportFinishTime,
            last_update_start_time = LastUpdateStartTime
        }
    },
    SIMDoc4 = #document{
        key = SpaceId4,
        value = SIMBase#storage_import_monitoring{
            scan_start_time = LastUpdateStartTime * 1000,
            scan_stop_time = ImportFinishTime * 1000,
            status = ?RUNNING
        },
        version = storage_import_monitoring:get_record_version()
    },

    LastUpdateStopTime = 25,
    SSMDoc5 = #document{
        key = rpc:call(Worker, storage_sync_monitoring, id, [SpaceId5, Storage5]),
        value = SSMBase#storage_sync_monitoring{
            import_start_time = ImportStartTime,
            import_finish_time = ImportFinishTime,
            last_update_start_time = LastUpdateStartTime,
            last_update_finish_time = LastUpdateStopTime
        }
    },
    SIMDoc5 = #document{
        key = SpaceId5,
        value = SIMBase#storage_import_monitoring{
            scan_start_time = LastUpdateStartTime * 1000,
            scan_stop_time = LastUpdateStopTime * 1000,
            status = ?COMPLETED
        },
        version = storage_import_monitoring:get_record_version()
    },

    create_doc(Worker, SSMCtx, SSMDoc1),
    create_doc(Worker, SSMCtx, SSMDoc2),
    create_doc(Worker, SSMCtx, SSMDoc3),
    create_doc(Worker, SSMCtx, SSMDoc4),
    create_doc(Worker, SSMCtx, SSMDoc5),

    ?assertEqual({ok, 4}, rpc:call(Worker, node_manager_plugin, upgrade_cluster, [3])),

    % storage_sync_monitoring doc should be deleted
    ?assertEqual({error, not_found}, rpc:call(Worker, storage_sync_monitoring, get, [SpaceId1, Storage1])),
    ?assertEqual({error, not_found}, rpc:call(Worker, storage_sync_monitoring, get, [SpaceId2, Storage2])),
    ?assertEqual({error, not_found}, rpc:call(Worker, storage_sync_monitoring, get, [SpaceId3, Storage3])),
    ?assertEqual({error, not_found}, rpc:call(Worker, storage_sync_monitoring, get, [SpaceId4, Storage4])),
    ?assertEqual({error, not_found}, rpc:call(Worker, storage_sync_monitoring, get, [SpaceId5, Storage5])),

    % storage_import_monitoring doc should be created
    ?assertMatch({ok, SIMDoc1}, rpc:call(Worker, storage_import_monitoring, get, [SpaceId1])),
    ?assertMatch({ok, SIMDoc2}, rpc:call(Worker, storage_import_monitoring, get, [SpaceId2])),
    ?assertMatch({ok, SIMDoc3}, rpc:call(Worker, storage_import_monitoring, get, [SpaceId3])),
    ?assertMatch({ok, SIMDoc4}, rpc:call(Worker, storage_import_monitoring, get, [SpaceId4])),
    ?assertMatch({ok, SIMDoc5}, rpc:call(Worker, storage_import_monitoring, get, [SpaceId5])).


upgrade_from_21_02_2_tmp_dir(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    SpaceIds = [?DUMMY_SPACE_ID1, ?DUMMY_SPACE_ID2],
    TmdDirUuids = lists:map(fun tmp_dir:uuid/1, SpaceIds),

    TmpDirExistsFun = fun(Uuid) -> rpc:call(Worker, file_meta, exists, [Uuid]) end,

    ?assertNot(lists:any(TmpDirExistsFun, TmdDirUuids)),

    % Assert tmp dirs are created on upgrade
    ?assertEqual({ok, 5}, rpc:call(Worker, node_manager_plugin, upgrade_cluster, [4])),
    ?assert(lists:all(TmpDirExistsFun, TmdDirUuids)),

    % Assert upgrade is idempotent
    ?assertEqual({ok, 5}, rpc:call(Worker, node_manager_plugin, upgrade_cluster, [4])),
    ?assert(lists:all(TmpDirExistsFun, TmdDirUuids)).


upgrade_from_21_02_3_missing_dirs(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    
    DirUuids = [?TRASH_DIR_UUID(?DUMMY_SPACE_ID1), ?SPACE_ARCHIVES_DIR_UUID(?DUMMY_SPACE_ID1)],
    DirExistsFun = fun(Uuid) -> rpc:call(Worker, file_meta, exists, [Uuid]) end,
    
    ?assertNot(lists:any(DirExistsFun, DirUuids)),
    % Assert dirs are created on upgrade
    ?assertEqual({ok, 6}, rpc:call(Worker, node_manager_plugin, upgrade_cluster, [5])),
    ?assert(lists:all(DirExistsFun, DirUuids)),
    
    % Assert upgrade is idempotent
    ?assertEqual({ok, 6}, rpc:call(Worker, node_manager_plugin, upgrade_cluster, [5])),
    ?assert(lists:all(DirExistsFun, DirUuids)).


upgrade_from_21_02_5_links_reconciliation_traverses(Config) ->
    [Worker | _] = AllWorkers = ?config(op_worker_nodes, Config),

    % the module is mocked in initializer
    test_utils:mock_unload(AllWorkers, file_links_reconciliation_traverse),
    
    rpc:call(Worker, file_links_reconciliation_traverse, start_for_space, [?SPACE1_ID]),
    ?assertMatch({ok, #document{value = #traverse_task{status = finished}}},
        rpc:call(Worker, traverse_task, get, [qos_traverse:pool_name(), ?SPACE1_ID]), 10),
    
    ?assertEqual({ok, 7}, rpc:call(Worker, node_manager_plugin, upgrade_cluster, [6])),
    ?assertMatch({error, not_found}, rpc:call(Worker, traverse_task, get, [qos_traverse:pool_name(), ?SPACE1_ID])),
    
    % Assert upgrade is idempotent
    ?assertEqual({ok, 7}, rpc:call(Worker, node_manager_plugin, upgrade_cluster, [6])),
    ?assertMatch({error, not_found}, rpc:call(Worker, traverse_task, get, [qos_traverse:pool_name(), ?SPACE1_ID])),
    
    rpc:call(Worker, file_links_reconciliation_traverse, start_for_space, [?SPACE1_ID]),
    ?assertMatch({ok, #document{value = #traverse_task{status = finished}}},
        rpc:call(Worker, traverse_task, get, [qos_traverse:pool_name(), ?SPACE1_ID]), 10).


upgrade_from_21_02_8_upgrade_swift_storage(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    TenantName = <<"some_project">>,
    BaseConfigurationParams = #{
        <<"authUrl">> => <<"some_url">>,
        <<"containerName">> => <<"some_container">>
    },
    BaseCredentialsParams = #{
        <<"username">> => <<"user">>,
        <<"password">> => <<"password">>
    },
    StorageId = ?RAND_STR(),
    % storage as left by 21.02.8 (record version 3) - the upgrade step gets it already in the current record version
    create_storage_configs_in_version_3(Worker, [{StorageId, {storage_config,
        {helper, ?SWIFT_HELPER_NAME, BaseConfigurationParams#{<<"tenantName">> => TenantName}, BaseCredentialsParams},
        {luma_config, ?AUTO_FEED, undefined, undefined}
    }}]),

    ?assertEqual({ok, 8}, rpc:call(Worker, node_manager_plugin, upgrade_cluster, [7])),

    ExpNewHelper = #helper_spec{
        name = ?SWIFT_HELPER_NAME,
        configuration = BaseConfigurationParams,
        credentials = BaseCredentialsParams#{<<"projectName">> => TenantName}
    },
    ?assertMatch(
        {ok, #document{value = #storage_config{helper_spec = ExpNewHelper}}},
        rpc:call(Worker, storage_config, get, [StorageId])
    ),

    % Assert upgrade is idempotent
    ?assertEqual({ok, 8}, rpc:call(Worker, node_manager_plugin, upgrade_cluster, [7])),

    ?assertMatch(
        {ok, #document{value = #storage_config{helper_spec = ExpNewHelper}}},
        rpc:call(Worker, storage_config, get, [StorageId])
    ).


upgrade_from_21_02_8_luma(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    UserId = <<"user_id">>,

    % storages and LUMA entries as left by 21.02.8 - upgrading a storage record from version 3 sets its
    % luma_generation to 0, which keeps the LUMA doc ids and links forest keys of 21.02.8 valid
    create_storage_configs_in_version_3(Worker, [
        {luma_storage_id(Feed, Helper), {storage_config, helper_21_02_8(Helper), {luma_config, Feed, undefined, undefined}}}
        || Feed <- [?AUTO_FEED, ?LOCAL_FEED], Helper <- ?HELPERS_21_02_8
    ]),
    StoragesAutoLuma = lists:map(fun(Helper) -> setup_luma_21_02_8(Worker, Helper, UserId, ?AUTO_FEED) end, ?HELPERS_21_02_8),
    StoragesLocalLuma = lists:map(fun(Helper) -> setup_luma_21_02_8(Worker, Helper, UserId, ?LOCAL_FEED) end, ?HELPERS_21_02_8),

    lists:foreach(fun({LumaStorageUser, Storage}) ->
        ?assertEqual({ok, LumaStorageUser}, rpc:call(Worker, luma_storage_users, get_or_acquire, [Storage, UserId])),
        % luma returns cached entry
        {_, ChangedStorage} = luma_test_utils:change_admin_creds(Storage),
        ?assertEqual({ok, LumaStorageUser}, rpc:call(Worker, luma_storage_users, get_or_acquire, [ChangedStorage, UserId]))
    end, StoragesAutoLuma ++ StoragesLocalLuma),

    ?assertEqual({ok, 8}, rpc:call(Worker, node_manager_plugin, upgrade_cluster, [7])),

    % for auto luma cache should be cleared
    lists:foreach(fun({LumaStorageUser, Storage}) ->
        {_, ChangedStorage} = luma_test_utils:change_admin_creds(Storage),
        ?assertNotEqual({ok, LumaStorageUser}, rpc:call(Worker, luma_storage_users, get_or_acquire, [ChangedStorage, UserId]))
    end, StoragesAutoLuma),

    % for local luma cache should NOT be cleared
    lists:foreach(fun({LumaStorageUser, Storage}) ->
        {_, ChangedStorage} = luma_test_utils:change_admin_creds(Storage),
        ?assertEqual({ok, LumaStorageUser}, rpc:call(Worker, luma_storage_users, get_or_acquire, [ChangedStorage, UserId]))
    end, StoragesLocalLuma).


upgrade_from_25_0_trash(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceGuid = space_dir:guid(?SPACE1_ID),
    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    
    {ok, DirGuid} = lfm_proxy:mkdir(Worker, SessId1, SpaceGuid, <<"dir">>, ?DEFAULT_DIR_MODE),
    {ok, _FileGuid} = lfm_proxy:create(Worker, SessId1, DirGuid, <<"file">>, ?DEFAULT_FILE_MODE),
    rpc:call(Worker, trash_dir, move_to_trash, [file_ctx:new_by_guid(DirGuid), rpc:call(Worker, user_ctx, new, [?ROOT_SESS_ID])]),
    
    {L, _} = rpc:call(Worker, trash_dir, list, [?SPACE1_ID]),
    ?assertEqual(1, length(L)),
    
    ?assertEqual({ok, 9}, rpc:call(Worker, node_manager_plugin, upgrade_cluster, [8])),
    
    % disable safe mode, so the waiting async upgrade process can start
    ok = rpc:call(Worker, safe_mode, report_node_initialized, []),

    ?assertEqual(0, length(element(1, rpc:call(Worker, trash_dir, list, [?SPACE1_ID]))), 20).

%%%===================================================================
%%% Helper functions
%%%===================================================================

%% Docs are written straight to disc, so that the first read of each runs the record upgrade.
%% Their fold links go through memory instead, as that is where storage_config:list_all/0 reads
%% them from (a disc only fold link would stay invisible to it).
create_storage_configs_in_version_3(Worker, StorageIdsAndRecords) ->
    test_utils:mock_new(Worker, storage_config, [passthrough]),
    test_utils:mock_expect(Worker, storage_config, get_record_version, fun() -> 3 end),
    lists:foreach(fun({StorageId, Record}) ->
        ?assertMatch({ok, _}, rpc:call(Worker, datastore_model, save, [
            #{model => storage_config, memory_driver => undefined},
            #document{key = StorageId, value = Record}
        ]))
    end, StorageIdsAndRecords),
    test_utils:mock_unload(Worker, [storage_config]),

    lists:foreach(fun({StorageId, _Record}) ->
        ?assertMatch({ok, _}, rpc:call(Worker, datastore_model, add_links, [
            storage_config:get_ctx(), <<"storage_config">>, ?MODEL_ALL_TREE_ID, {StorageId, <<>>}
        ]))
    end, StorageIdsAndRecords).


helper_21_02_8(#helper_spec{name = Name, configuration = Configuration, credentials = Credentials}) ->
    {helper, Name, Configuration, Credentials}.


luma_storage_id(Feed, Helper) ->
    <<"storage_id_", (atom_to_binary(Feed))/binary, "_", (helper_spec:get_name(Helper))/binary>>.


%% In 21.02.8 neither the LUMA doc id nor the links forest key had a luma generation component.
setup_luma_21_02_8(Worker, Helper, UserId, Feed) ->
    StorageId = luma_storage_id(Feed, Helper),
    {ok, StorageDoc} = rpc:call(Worker, storage_config, get, [StorageId]),
    LumaStorageUser = rpc:call(Worker, luma_storage_user, new,
        [UserId, #{<<"storageCredentials">> => helper_spec:get_credentials(Helper)}, StorageDoc]),

    Table = luma_storage_users,
    TableBin = atom_to_binary(Table),
    DocId = datastore_key:new_from_digest([StorageId, TableBin, UserId]),
    LumaDbCtx = luma_db:get_ctx(),
    ?assertMatch({ok, _}, rpc:call(Worker, datastore_model, save, [LumaDbCtx, #document{
        key = DocId,
        value = #luma_db{table = Table, record = LumaStorageUser, storage_id = StorageId, feed = Feed}
    }])),
    ForestKey = <<"LUMA_DB_LINKS##", TableBin/binary, "##", StorageId/binary>>,
    ProviderId = rpc:call(Worker, oneprovider, get_id, []),
    ?assertMatch({ok, _}, rpc:call(Worker, datastore_model, add_links, [
        LumaDbCtx, ForestKey, ProviderId, {UserId, DocId}
    ])),
    {LumaStorageUser, StorageDoc}.


%%%===================================================================
%%% Setup/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = initializer:setup_storage(NewConfig),
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig1, "env_desc.json"), NewConfig1)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, luma_test_utils]} | Config].


init_per_testcase(Case = upgrade_from_20_02_1_space_strategies, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Worker, storage_logic, [passthrough]),
    test_utils:mock_expect(Worker, storage_logic, is_imported, fun(StorageId) ->
        {ok, lists:member(StorageId, [<<"storage2">>, <<"storage3">>, <<"storage4">>])}
    end),

    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case = upgrade_from_21_02_2_tmp_dir, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Worker, provider_logic, [passthrough]),
    test_utils:mock_expect(Worker, provider_logic, get_spaces, fun() ->
        {ok, [?DUMMY_SPACE_ID1, ?DUMMY_SPACE_ID2]}
    end),

    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case = upgrade_from_21_02_3_missing_dirs, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    
    test_utils:mock_new(Worker, provider_logic, [passthrough]),
    test_utils:mock_new(Worker, space_logic, [passthrough]),
    test_utils:mock_expect(Worker, provider_logic, get_spaces, fun() ->
        {ok, [?DUMMY_SPACE_ID1]}
    end),
    test_utils:mock_expect(Worker, space_logic, get_name, fun(_, SpaceId) ->
        {ok, SpaceId}
    end),
    
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case = upgrade_from_21_02_5_links_reconciliation_traverses, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    
    test_utils:mock_new(Worker, provider_logic, [passthrough]),
    test_utils:mock_expect(Worker, provider_logic, get_spaces, fun() ->
        {ok, [?SPACE1_ID]}
    end),
    
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case = upgrade_from_21_02_8_upgrade_swift_storage, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Worker, storage_logic, [passthrough]),
    test_utils:mock_expect(Worker, storage_logic, get_name_of_local_storage, fun(StorageId) ->
        {ok, StorageId}
    end),
    test_utils:mock_new(Worker, provider_logic, [passthrough]),
    test_utils:mock_expect(Worker, provider_logic, get_storages, fun() ->
        {ok, StorageConfigDocs} = storage_config:list_all(),
        {ok, [StorageId || #document{key = StorageId} <- StorageConfigDocs]}
    end),
    test_utils:mock_expect(Worker, provider_logic, get_spaces, fun() ->
        {ok, [?SPACE1_ID]}
    end),

    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case = upgrade_from_21_02_8_luma, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Worker, storage_logic, [passthrough]),
    test_utils:mock_expect(Worker, storage_logic, get_name_of_local_storage, fun(StorageId) ->
        {ok, StorageId}
    end),
    test_utils:mock_new(Worker, provider_logic, [passthrough]),
    test_utils:mock_expect(Worker, provider_logic, get_storages, fun() ->
        {ok,
            [<<"storage_id_auto_", (helper_spec:get_name(Helper))/binary>> || Helper <- ?HELPERS_21_02_8] ++
                [<<"storage_id_local_", (helper_spec:get_name(Helper))/binary>> || Helper <- ?HELPERS_21_02_8]
        }
    end),
    test_utils:mock_expect(Worker, provider_logic, get_spaces, fun() ->
        {ok, [?SPACE1_ID]}
    end),

    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case = upgrade_from_25_0_trash, Config) ->
    Config1 = initializer:setup_storage(Config),
    initializer:create_test_users_and_spaces(?TEST_FILE(Config1, "env_desc.json"), Config1),

    init_per_testcase(?DEFAULT_CASE(Case), Config1);

init_per_testcase(_Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker, gs_channel_service, [passthrough]),
    test_utils:mock_expect(Worker, gs_channel_service, is_connected_and_initialized, fun() -> true end),
    lfm_proxy:init(Config).


end_per_testcase(Case = upgrade_from_21_02_2_tmp_dir, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker, [provider_logic]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case = upgrade_from_21_02_3_missing_dirs, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker, [provider_logic, space_logic]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case = upgrade_from_21_02_5_links_reconciliation_traverses, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker, [provider_logic]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case = upgrade_from_25_0_trash, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker, [provider_logic]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker, [storage_logic, storage_config, gs_channel_service]),
    lfm_proxy:teardown(Config).


end_per_suite(_Config) ->
    ok.

create_doc(Worker, Ctx, Doc) ->
    rpc:call(Worker, datastore_model, create, [Ctx, Doc]).

get_doc(Worker, Ctx, Key) ->
    rpc:call(Worker, datastore_model, get, [Ctx, Key]).

get_storage_import_scan_config(Worker, SpaceId) ->
    rpc:call(Worker, storage_import, get_configuration, [SpaceId]).