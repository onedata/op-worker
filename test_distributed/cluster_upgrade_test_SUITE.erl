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
%%% @end
%%%-------------------------------------------------------------------
-module(cluster_upgrade_test_SUITE).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/storage/import/storage_import.hrl").
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
    upgrade_20_02_1_to_20_02_2_space_strategies/1,
    upgrade_20_02_1_to_20_02_2_storage_sync_monitoring/1,
    upgrade_20_02_x_to_21_02_1_space_supports/1
]).

%%%===================================================================
%%% API functions
%%%===================================================================

all() -> ?ALL([
    upgrade_20_02_1_to_20_02_2_space_strategies,
    upgrade_20_02_1_to_20_02_2_storage_sync_monitoring,
    upgrade_20_02_x_to_21_02_1_space_supports
]).

%%%===================================================================
%%% Tests
%%%===================================================================

upgrade_20_02_1_to_20_02_2_space_strategies(Config) ->
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


upgrade_20_02_1_to_20_02_2_storage_sync_monitoring(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SSMCtx = storage_sync_monitoring:get_ctx(),

    SpaceId1 = <<"space_id1">>,
    {ok, Storage1} = rpc:call(Worker, space_logic, get_local_storage_id, [SpaceId1]),
    SpaceId2 = <<"space_id2">>,
    {ok, Storage2} = rpc:call(Worker, space_logic, get_local_storage_id, [SpaceId2]),
    SpaceId3 = <<"space_id3">>,
    {ok, Storage3} = rpc:call(Worker, space_logic, get_local_storage_id, [SpaceId3]),
    SpaceId4 = <<"space_id4">>,
    {ok, Storage4} = rpc:call(Worker, space_logic, get_local_storage_id, [SpaceId4]),
    SpaceId5 = <<"space_id5">>,
    {ok, Storage5} = rpc:call(Worker, space_logic, get_local_storage_id, [SpaceId5]),

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
    EmptyMinHist = time_slot_histogram:new(Timestamp, 60 div HistLength , HistLength),
    EmptyHourHist = time_slot_histogram:new(Timestamp, 3600 div HistLength, HistLength),
    EmptyDayHist = time_slot_histogram:new(Timestamp, 86400 div HistLength, HistLength),
    EmptyCumulativeMinHist = time_slot_histogram:new_cumulative(Timestamp, 60 div HistLength , HistLength),
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


upgrade_20_02_x_to_21_02_1_space_supports(Config) ->
    AllSpaces = [<<"space_id1">>, <<"space_id2">>, <<"space_id3">>, <<"space_id4">>, <<"space_id5">>],
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual({ok, 5}, rpc:call(Worker, node_manager_plugin, upgrade_cluster, [4])),
    test_utils:mock_assert_num_calls_sum(Worker, storage_logic, upgrade_support_to_21_02, 2, 5),
    lists:foreach(fun(SpaceId) ->
        {ok, StorageId} = rpc:call(Worker, space_logic, get_local_storage_id, [SpaceId]),
        test_utils:mock_assert_num_calls_sum(Worker, storage_logic, upgrade_support_to_21_02, [StorageId, SpaceId], 1)
    end, AllSpaces).

%%%===================================================================
%%% Setup/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = initializer:setup_storage(NewConfig),
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig1, "env_desc.json"), NewConfig1)
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


init_per_testcase(Case = upgrade_20_02_1_to_20_02_2_space_strategies, Config) ->
    Workers = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Workers, storage_logic, [passthrough]),
    test_utils:mock_expect(Workers, storage_logic, is_imported, fun(StorageId) ->
        {ok, lists:member(StorageId, [<<"storage2">>, <<"storage3">>, <<"storage4">>])}
    end),

    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(Case = upgrade_20_02_x_to_21_02_1_space_supports, Config) ->
    Workers = ?config(op_worker_nodes, Config),

    test_utils:mock_new(Workers, storage_logic, [passthrough]),
    test_utils:mock_expect(Workers, storage_logic, upgrade_support_to_21_02, fun(_, _) -> ok end),

    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, gs_channel_service, [passthrough]),
    test_utils:mock_expect(Workers, gs_channel_service, is_connected, fun() -> true end),
    Config.


end_per_testcase(_, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker, [storage_logic, gs_channel_service]),
    ok.


end_per_suite(_Config) ->
    ok.

create_doc(Worker, Ctx, Doc) ->
    rpc:call(Worker, datastore_model, create, [Ctx, Doc]).

get_doc(Worker, Ctx, Key) ->
    rpc:call(Worker, datastore_model, get, [Ctx, Key]).

get_storage_import_scan_config(Worker, SpaceId) ->
    rpc:call(Worker, storage_import, get_configuration, [SpaceId]).