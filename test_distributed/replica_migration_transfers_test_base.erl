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
-module(replica_migration_transfers_test_base).
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
    fail_to_migrate_file_replica_without_permissions/3,
    cancel_migration_on_target_nodes_by_scheduling_user/2,
    cancel_migration_on_target_nodes_by_other_user/2,
    rerun_file_migration/3,
    rerun_view_migration/2
]).

-define(SPACE_ID, <<"space1">>).

%%%===================================================================
%%% API
%%%===================================================================

fail_to_migrate_file_replica_without_permissions(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = #scenario{
                user = <<"user2">>,
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP1],
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:schedule_replica_migration_without_permissions/2
            },
            expected = #expected{
                expected_transfer = undefined,
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

cancel_migration_on_target_nodes_by_scheduling_user(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{10, 0}, {0, 10}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type),
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                attempts = 120,
                timeout = timer:minutes(10)
            },
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP1],
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:cancel_migration_on_target_nodes_by_scheduling_user/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => cancelled,
                    eviction_status => cancelled,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => fun(X) -> X =< 100 end,
                    files_processed => fun(X) -> X =< 100 end,
                    failed_files => 0,
                    files_replicated => fun(X) -> X =< 100 end,
                    files_evicted => fun(X) -> X =< 100 end
                },
                attempts = 120,
                distribution = undefined,
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

cancel_migration_on_target_nodes_by_other_user(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    User1 = <<"user1">>,
    User2 = <<"user2">>,

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{10, 0}, {0, 10}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type),
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                attempts = 120,
                timeout = timer:minutes(10)
            },
            scenario = #scenario{
                type = Type,
                user = User1,
                cancelling_user = User2,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP1],
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:cancel_migration_on_target_nodes_by_other_user/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => cancelled,
                    eviction_status => cancelled,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => fun(X) -> X =< 100 end,
                    files_processed => fun(X) -> X =< 100 end,
                    failed_files => 0,
                    files_replicated => fun(X) -> X =< 100 end,
                    files_evicted => fun(X) -> X =< 100 end
                },
                distribution = undefined,
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

rerun_file_migration(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

    transfers_test_utils:mock_replica_synchronizer_failure(WorkerP2),

    Config2 = transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP1],
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => failed,
                    eviction_status => failed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 1,
                    files_processed => 1,
                    failed_files => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 0
                },
                distribution = undefined,
                assertion_nodes = [WorkerP1, WorkerP2],
                attempts = 120,
                timeout = timer:minutes(2)
            }
        }
    ),
    Config3 = transfers_test_mechanism:move_transfer_ids_to_old_key(Config2),
    transfers_test_utils:unmock_replica_synchronizer_failure(WorkerP2),

    transfers_test_mechanism:run_test(
        Config3, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                function = fun transfers_test_mechanism:rerun_migrations/2
            },
            expected = #expected{
                expected_transfer = #{
                    user_id => ?DEFAULT_USER,
                    replication_status => completed,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 2,
                    files_processed => 2,
                    failed_files => 0,
                    files_replicated => 1,
                    bytes_replicated => ?DEFAULT_SIZE,
                    files_evicted => 1,
                    min_hist => ?MIN_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => ?DEFAULT_SIZE})
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => []},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            }
        }
    ).

rerun_view_migration(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId2 = ?DEFAULT_SESSION(WorkerP2, Config),
    SpaceId = ?SPACE_ID,
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

    Config2 = transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type),
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }}),

    [{FileGuid, _}] = ?config(?FILES_KEY, Config2),
    {ok, FileId} = file_id:guid_to_objectid(FileGuid),

    % set xattr on file to be replicated
    XattrName = transfers_test_utils:random_job_name(?FUNCTION_NAME),
    XattrValue = 1,
    Xattr = #xattr{name = XattrName, value = XattrValue},
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, ?FILE_REF(FileGuid), Xattr),
    ViewName = transfers_test_utils:random_view_name(?FUNCTION_NAME),
    MapFunction = transfers_test_utils:test_map_function(XattrName),
    transfers_test_utils:create_view(WorkerP2, SpaceId, ViewName, MapFunction, [], [ProviderId1, ProviderId2]),
    ?assertViewQuery([FileId], WorkerP1, SpaceId, ViewName, [{key, XattrValue}]),
    ?assertViewQuery([FileId], WorkerP2, SpaceId, ViewName, [{key, XattrValue}]),

    transfers_test_utils:mock_replica_synchronizer_failure(WorkerP2),

    Config3 = transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                evicting_nodes = [WorkerP1],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:migrate_replicas_from_view/2,
                query_view_params = [{key, XattrValue}],
                view_name = ViewName
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => failed,
                    eviction_status => failed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1,
                    failed_files => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 0
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = undefined,
                assert_transferred_file_model = false,
                attempts = 120,
                timeout = timer:minutes(2)
            }
        }
    ),
    Config4 = transfers_test_mechanism:move_transfer_ids_to_old_key(Config3),
    transfers_test_utils:unmock_replica_synchronizer_failure(WorkerP2),

    transfers_test_mechanism:run_test(
        Config4, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                function = fun transfers_test_mechanism:rerun_view_migrations/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 2,
                    files_processed => 2,
                    files_replicated => 1,
                    bytes_replicated => ?DEFAULT_SIZE,
                    files_evicted => 1,
                    min_hist => ?MIN_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => ?DEFAULT_SIZE})
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                assert_transferred_file_model = false,
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => []},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            }
        }
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
        NewConfig3 = initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig1, "env_desc.json"), NewConfig1),
        NewConfig3
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, transfers_test_utils, transfers_test_mechanism, ?MODULE]}
        | Config
    ].

init_per_testcase(Case, Config)
    when Case =:= cancel_migration_on_target_nodes_by_other_user
    orelse Case =:= cancel_migration_on_target_nodes_by_scheduling_user
->
    Workers = ?config(op_worker_nodes, Config),
    transfers_test_utils:mock_prolonged_replication(Workers, 1, 10),
    init_per_testcase(all, Config);

init_per_testcase(_Case, Config) ->
    ct:timetrap(timer:minutes(10)),
    lfm_proxy:init(Config),
    [{space_id, ?SPACE_ID} | Config].

end_per_testcase(Case, Config)
    when Case =:= cancel_migration_on_target_nodes_by_other_user
    orelse Case =:= cancel_migration_on_target_nodes_by_scheduling_user
->
    Workers = ?config(op_worker_nodes, Config),
    transfers_test_utils:unmock_prolonged_replication(Workers),
    end_per_testcase(all, Config);

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
