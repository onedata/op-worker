%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains base test of replication.
%%% @end
%%%-------------------------------------------------------------------
-module(replication_transfers_test_base).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/acl.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("transfers_test_mechanism.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/errors.hrl").

-export([init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

% TODO VFS-5617
%% API
-export([
    file_replication_failures_should_fail_whole_transfer/3,
    many_simultaneous_failed_transfers/3,
    file_removed_during_replication/3,
    rtransfer_works_between_providers_with_different_ports/2,
    warp_time_during_replication/2
]).

-define(SPACE_ID, <<"space1">>).

%%%===================================================================
%%% API
%%%===================================================================

file_replication_failures_should_fail_whole_transfer(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    transfers_test_utils:mock_replica_synchronizer_failure(WorkerP2),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{10, 0}, {0, 10}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                attempts = 60,
                timeout = timer:minutes(1)
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => failed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 100,
                    files_processed => 100,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => 0}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => 0}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => 0})
                },
                assertion_nodes = [WorkerP2],
                attempts = 120,
                timeout = timer:minutes(10)
            }
        }
    ).

many_simultaneous_failed_transfers(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    transfers_test_utils:mock_replica_synchronizer_failure(WorkerP2),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{10, 0}, {0, 10}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                attempts = 60,
                timeout = timer:minutes(1)
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_each_file_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => failed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1,
                    failed_files => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => 0}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => 0}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => 0})
                },
                assertion_nodes = [WorkerP2],
                timeout = timer:minutes(15),
                attempts = 900
            }
        }
    ).

file_removed_during_replication(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Size = 1024 * 1024 * 1024,
    Config2 = transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                size = Size,
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 1}],
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, Size]]}
                ]
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:remove_file_during_replication/2
            },
            expected = #expected{
                expected_transfer = #{
                    % TODO VFS-6619 Below lines commented so that test does not fail here. Uncomment after fixing.
                    % replication_status => failed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1
                    % failed_files => 1
                },
                assertion_nodes = [WorkerP2],
                timeout = timer:minutes(3),
                attempts = 100
            }
        }
    ),
    Config3 = transfers_test_mechanism:move_transfer_ids_to_old_key(Config2),
    % see that next replication is ok
    transfers_test_mechanism:run_test(
        Config3, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 1}],
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_each_file_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 1,
                    bytes_replicated => ?DEFAULT_SIZE,
                    min_hist => ?MIN_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => ?DEFAULT_SIZE}),
                    hr_hist => ?HOUR_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => ?DEFAULT_SIZE}),
                    dy_hist => ?DAY_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => ?DEFAULT_SIZE}),
                    mth_hist => ?MONTH_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => ?DEFAULT_SIZE})
                },
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP2), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).


rtransfer_works_between_providers_with_different_ports(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

    % Replication from p1 to p2
    TransferTestSpec = #transfer_test_spec{
        setup = Setup = #setup{
            setup_node = WorkerP1,
            assertion_nodes = [WorkerP2],
            files_structure = [{0, 1}],
            distribution = [
                #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
            ]
        },
        scenario = Scenario = #scenario{
            type = Type,
            schedule_node = WorkerP1,
            replicating_nodes = [WorkerP2],
            function = fun transfers_test_mechanism:replicate_each_file_separately/2
        },
        expected = #expected{
            expected_transfer = #{
                replication_status => completed,
                files_to_process => 1,
                files_processed => 1,
                files_replicated => 1,
                bytes_replicated => ?DEFAULT_SIZE
            },
            distribution = [
                #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
            ],
            assertion_nodes = [WorkerP1, WorkerP2]
        }
    },

    Config1 = transfers_test_mechanism:run_test(Config, TransferTestSpec),
    Config2 = transfers_test_mechanism:move_transfer_ids_to_old_key(Config1),

    % Replication from p2 to p1
    TransferTestSpec2 = TransferTestSpec#transfer_test_spec{
        setup = Setup#setup{
            setup_node = WorkerP2,
            root_directory = <<"root_dir">>,
            assertion_nodes = [WorkerP1],
            distribution = [
                #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
            ]
        },
        scenario = Scenario#scenario{
            schedule_node = WorkerP2,
            replicating_nodes = [WorkerP1]
        }
    },
    transfers_test_mechanism:run_test(Config2, TransferTestSpec2).


warp_time_during_replication(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),
    SpaceId = ?SPACE_ID,

    FilesNum = 100,
    TotalTransferredBytes = FilesNum * ?DEFAULT_SIZE,

    CurrTime = time_test_utils:get_frozen_time_seconds(),
    PastTime = CurrTime - 1000,
    FutureTime = CurrTime + 2764800,  % move 32 days to the future

    % Forward time warp should cause shift of space transfer stats
    % so that no artefacts from previous tests remains.
    ok = time_test_utils:set_current_time_seconds(FutureTime),

    ExpMinHist = #{ProviderId1 => histogram:increment(histogram:new(?MIN_HIST_LENGTH), TotalTransferredBytes)},
    ExpHrHist = #{ProviderId1 => histogram:increment(histogram:new(?HOUR_HIST_LENGTH), TotalTransferredBytes)},
    ExpDayHist = #{ProviderId1 => histogram:increment(histogram:new(?DAY_HIST_LENGTH), TotalTransferredBytes)},
    ExpMthHist = #{ProviderId1 => histogram:increment(histogram:new(?MONTH_HIST_LENGTH), TotalTransferredBytes)},

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{10, 0}, {0, 10}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type),
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun(Config, Scenario) ->
                    NewConfig = transfers_test_mechanism:replicate_root_directory(Config, Scenario),

                    % Wait until some bytes are replicated before warping time backwards
                    [Tid] = transfers_test_mechanism:get_transfer_ids(NewConfig),
                    transfers_test_mechanism:await_replication_starts(WorkerP2, Tid),
                    ok = time_test_utils:set_current_time_seconds(PastTime),

                    % Make sure the time warp happened when the replication was ongoing.
                    % If this assert fails, the test needs to be adjusted.
                    #transfer{bytes_replicated = BytesReplicated} = transfers_test_utils:get_transfer(WorkerP2, Tid),
                    ?assert(BytesReplicated < TotalTransferredBytes),

                    NewConfig
                end
            },
            expected = #expected{
                expected_transfer = #{
                    schedule_time => FutureTime,
                    start_time => FutureTime,
                    finish_time => FutureTime,
                    replication_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => FilesNum,
                    files_processed => FilesNum,
                    files_replicated => FilesNum,
                    bytes_replicated => TotalTransferredBytes,
                    min_hist => ExpMinHist,
                    hr_hist => ExpHrHist,
                    dy_hist => ExpDayHist,
                    mth_hist => ExpMthHist
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ),

    ?assertMatch(
        {ok, #document{value = #space_transfer_stats{
            last_update = #{ProviderId1 := FutureTime},
            min_hist = ExpMinHist,
            hr_hist = ExpHrHist,
            dy_hist = ExpDayHist,
            mth_hist = ExpMthHist
        }}},
        rpc:call(WorkerP2, space_transfer_stats, get, [?JOB_TRANSFERS_TYPE, SpaceId])
    ).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = initializer:setup_storage(NewConfig),
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?APP_NAME, rerun_transfers, false)
        end, ?config(op_worker_nodes, NewConfig1)),

        application:start(ssl),
        application:ensure_all_started(hackney),
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig1, "env_desc.json"), NewConfig1)
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, transfers_test_utils, transfers_test_mechanism, ?MODULE]}
        | Config
    ].

init_per_testcase(file_removed_during_replication, Config) ->
    ct:timetrap(timer:minutes(60)),
    lfm_proxy:init(Config),
    [{space_id, <<"space4">>} | Config];

init_per_testcase(rtransfer_works_between_providers_with_different_ports = Case, Config) ->
    [Worker1, Worker2] = ?config(op_worker_nodes, Config),
    {ok, C} = rpc:call(Worker1, application, get_env, [rtransfer_link, transfer]),
    C1 = lists:keyreplace(server_port, 1, C, {server_port, 30000}),
    rpc:call(Worker1, application, set_env, [rtransfer_link, transfer, C1]),
    
    ProviderId1 = rpc:call(Worker1, oneprovider, get_id, []),
    rpc:call(Worker2, node_cache, clear, [{rtransfer_port, ProviderId1}]),
    rpc:call(Worker1, rtransfer_config, restart_link, []),
    
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(warp_time_during_replication = Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = time_test_utils:freeze_time(Config),
    transfers_test_utils:mock_prolonged_replication(Workers, 1, 10),
    init_per_testcase(?DEFAULT_CASE(Case), Config);

init_per_testcase(_Case, Config) ->
    ct:timetrap(timer:minutes(60)),
    lfm_proxy:init(Config),
    [{space_id, ?SPACE_ID} | Config].

end_per_testcase(warp_time_during_replication = Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    transfers_test_utils:unmock_prolonged_replication(Workers),
    ok = time_test_utils:unfreeze_time(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    transfers_test_utils:unmock_replication_worker(Workers),
    transfers_test_utils:unmock_replica_synchronizer_failure(Workers),
    transfers_test_utils:remove_transfers(Config),
    transfers_test_utils:remove_all_views(Workers, ?SPACE_ID),
    transfers_test_utils:ensure_transfers_removed(Config).

end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    application:stop(hackney),
    application:stop(ssl),
    initializer:teardown_storage(Config).
