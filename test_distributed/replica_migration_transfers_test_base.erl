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
-include("transfers_test_mechanism.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

-export([init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

% TODO VFS-5617
%% API
-export([
    migrate_empty_dir/3,
    migrate_tree_of_empty_dirs/3,
    migrate_100_files_in_one_request/3,
    migrate_100_files_each_file_separately/3,
    migrate_regular_file_replica/3,
    migrate_regular_file_replica_in_directory/3,
    migrate_big_file_replica/3,
    fail_to_migrate_file_replica_without_permissions/3,
    schedule_migration_by_view/2,
    scheduling_migration_by_not_existing_view_should_fail/2,
    scheduling_replica_migration_by_view_with_function_returning_wrong_value_should_fail/2,
    scheduling_replica_migration_by_view_returning_not_existing_file_should_not_fail/2,
    scheduling_migration_by_empty_view_should_succeed/2,
    scheduling_migration_by_not_existing_key_in_view_should_succeed/2,
    schedule_migration_of_100_regular_files_by_view/2,
    schedule_migration_of_regular_file_by_view_with_reduce/2,
    cancel_migration_on_target_nodes_by_scheduling_user/2,
    cancel_migration_on_target_nodes_by_other_user/2,
    rerun_file_migration/3,
    rerun_view_migration/2
]).

-define(SPACE_ID, <<"space1">>).

%%%===================================================================
%%% API
%%%===================================================================

migrate_empty_dir(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType)
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP1],
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:migrate_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 2,
                    files_processed => 2,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 0
                },
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

migrate_tree_of_empty_dirs(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{10, 0}, {10, 0}, {10, 0}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType)
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP1],
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:migrate_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 2222,
                    files_processed => 2222,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 0
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                attempts = 120
            }
        }
    ).

migrate_regular_file_replica(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

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
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP1],
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 2,
                    files_processed => 2,
                    files_replicated => 1,
                    bytes_replicated => ?DEFAULT_SIZE,
                    files_evicted => 1
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => []},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2],
                attempts = 120,
                timeout = timer:minutes(2)
            }
        }
    ).

migrate_regular_file_replica_in_directory(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

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
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP1],
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:migrate_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 4,
                    files_processed => 4,
                    files_replicated => 1,
                    bytes_replicated => ?DEFAULT_SIZE,
                    files_evicted => 1
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => []},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2],
                attempts = 120,
                timeout = timer:minutes(2)
            }
        }
    ).

migrate_big_file_replica(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Size = 1024 * 1024 * 1024,
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                size = Size,
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, Size]]}
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
                    replication_status => completed,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 2,
                    files_processed => 2,
                    files_replicated => 1,
                    bytes_replicated => Size,
                    files_evicted => 1
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => []},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, Size]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2],
                attempts = 120
            }
        }
    ).

migrate_100_files_in_one_request(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 100}],
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
                function = fun transfers_test_mechanism:migrate_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 202,
                    files_processed => 202,
                    files_replicated => 100,
                    bytes_replicated => 100 * ?DEFAULT_SIZE,
                    files_evicted => 100
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => []},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2],
                attempts = 600,
                timeout = timer:minutes(10)
            }
        }
    ).

migrate_100_files_each_file_separately(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 100}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                attempts = 600,
                timeout = timer:minutes(10)
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
                    replication_status => completed,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 2,
                    files_processed => 2,
                    files_replicated => 1,
                    bytes_replicated => ?DEFAULT_SIZE,
                    files_evicted => 1
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => []},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                attempts = 600,
                timeout = timer:minutes(10)
            }
        }
    ).

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

schedule_migration_by_view(Config, Type) ->
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
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),
    ViewName = transfers_test_utils:random_view_name(?FUNCTION_NAME),
    MapFunction = transfers_test_utils:test_map_function(XattrName),
    transfers_test_utils:create_view(WorkerP2, SpaceId, ViewName, MapFunction, [], [ProviderId1, ProviderId2]),
    ?assertViewQuery([FileId], WorkerP1, SpaceId, ViewName, [{key, XattrValue}]),
    ?assertViewQuery([FileId], WorkerP2, SpaceId, ViewName, [{key, XattrValue}]),

    transfers_test_mechanism:run_test(
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
                    replication_status => completed,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 4,
                    files_processed => 4,
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
                ],
                assert_transferred_file_model = false,
                attempts = 120,
                timeout = timer:minutes(2)
            }
        }
    ).

schedule_migration_of_regular_file_by_view_with_reduce(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId2 = ?DEFAULT_SESSION(WorkerP2, Config),
    SpaceId = ?SPACE_ID,
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

    Config2 = transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                files_structure = [{0, 6}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type),
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }}),


    FileGuidsAndPaths = ?config(?FILES_KEY, Config2),
    [Guid1, Guid2, Guid3, Guid4, _Guid5, Guid6 | _] = [G || {G, _} <- FileGuidsAndPaths],

    % XattrName1 will be used by map function
    % only files with XattrName1 = XattrValue11 will be emitted
    % XattrName2 will be used by reduce function
    % only files with XattrName2 = XattrValue12 will be filtered
    XattrName1 = transfers_test_utils:random_job_name(?FUNCTION_NAME),
    XattrName2 = transfers_test_utils:random_job_name(?FUNCTION_NAME),
    XattrValue11 = 1,
    XattrValue12 = 2,
    XattrValue21 = 1,
    XattrValue22 = 2,
    Xattr11 = #xattr{name = XattrName1, value = XattrValue11},
    Xattr12 = #xattr{name = XattrName1, value = XattrValue12},
    Xattr21 = #xattr{name = XattrName2, value = XattrValue21},
    Xattr22 = #xattr{name = XattrName2, value = XattrValue22},

    % File1: xattr1=1, xattr2=1, should be replicated
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, Guid1}, Xattr11),
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, Guid1}, Xattr21),

    % File2: xattr1=1, xattr2=2, should not be replicated
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, Guid2}, Xattr11),
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, Guid2}, Xattr22),

    % File3: xattr1=1, xattr2=null, should not be replicated
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, Guid3}, Xattr11),

    % File4: xattr1=2, xattr2=null, should not be replicated
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, Guid4}, Xattr12),

    % File5: xattr1=null, xattr2=null, should not be replicated

    % File6: xattr1=1, xattr2=1, should be replicated
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, Guid6}, Xattr11),
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, Guid6}, Xattr21),

    ViewName = transfers_test_utils:random_view_name(?FUNCTION_NAME),
    MapFunction = transfers_test_utils:test_map_function(XattrName1, XattrName2),
    ReduceFunction = transfers_test_utils:test_reduce_function(XattrValue21),

    ok = transfers_test_utils:create_view(WorkerP2, SpaceId, ViewName,
        MapFunction, ReduceFunction, [{group, 1}, {key, XattrValue11}],
        [ProviderId1, ProviderId2]
    ),

    {ok, ObjectId1} = file_id:guid_to_objectid(Guid1),
    {ok, ObjectId6} = file_id:guid_to_objectid(Guid6),

    ?assertViewQuery([ObjectId1, ObjectId6], WorkerP1, SpaceId, ViewName,  [{key, XattrValue11}]),
    ?assertViewQuery([ObjectId1, ObjectId6], WorkerP2, SpaceId, ViewName,  [{key, XattrValue11}]),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                evicting_nodes = [WorkerP1],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:migrate_replicas_from_view/2,
                query_view_params = [{group, true}, {key, XattrValue11}],
                view_name = ViewName
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 6,
                    files_processed => 6,
                    files_replicated => 2,
                    bytes_replicated => 2 * ?DEFAULT_SIZE,
                    files_evicted => 2,
                    min_hist => ?MIN_HIST(#{ProviderId1 => 2 * ?DEFAULT_SIZE}),
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => 2 * ?DEFAULT_SIZE}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => 2 * ?DEFAULT_SIZE}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => 2 * ?DEFAULT_SIZE})
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => []},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assert_distribution_for_files = [Guid1, Guid6],
                assertion_nodes = [WorkerP1, WorkerP2],
                assert_transferred_file_model = false,
                attempts = 120,
                timeout = timer:minutes(2)
            }
        }
    ).

scheduling_migration_by_not_existing_view_should_fail(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId2 = ?DEFAULT_SESSION(WorkerP2, Config),
    SpaceId = ?SPACE_ID,
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),

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

    % set xattr on file to be replicated
    XattrName = transfers_test_utils:random_job_name(?FUNCTION_NAME),
    XattrValue = 1,
    Xattr = #xattr{name = XattrName, value = XattrValue},
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),
    ViewName = transfers_test_utils:random_view_name(?FUNCTION_NAME),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                evicting_nodes = [WorkerP1],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:fail_to_migrate_replicas_from_view/2,
                query_view_params = [{key, XattrValue}],
                view_name = ViewName
            },
            expected = #expected{
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assert_transferred_file_model = false
            }
        }
    ).

scheduling_replica_migration_by_view_with_function_returning_wrong_value_should_fail(Config, Type) ->
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

    % set xattr on file to be replicated
    XattrName = transfers_test_utils:random_job_name(?FUNCTION_NAME),
    XattrValue = 1,
    Xattr = #xattr{name = XattrName, value = XattrValue},
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),
    WrongValue = <<"random_value_instead_of_file_id">>,
    %functions does not emit file id in values
    MapFunction = <<
        "function (id, type, meta, ctx) {
            if(type == 'custom_metadata' && meta['", XattrName/binary,"']) {
                return [meta['", XattrName/binary, "'], '", WrongValue/binary, "'];
            }
        return null;
    }">>,
    ViewName = transfers_test_utils:random_view_name(?FUNCTION_NAME),
    transfers_test_utils:create_view(WorkerP2, SpaceId, ViewName, MapFunction, [], [ProviderId1, ProviderId2]),
    ?assertViewQuery([WrongValue], WorkerP2, SpaceId, ViewName,  [{key, XattrValue}]),
    ?assertViewQuery([WrongValue], WorkerP1, SpaceId, ViewName,  [{key, XattrValue}]),

    transfers_test_mechanism:run_test(
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
                    files_to_process => 2,
                    files_processed => 2,
                    files_evicted => 0,
                    failed_files => 1
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assert_transferred_file_model = false,
                attempts = 120,
                timeout = timer:minutes(2)
            }
        }
    ).

scheduling_replica_migration_by_view_returning_not_existing_file_should_not_fail(Config, Type) ->
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

    % set xattr
    XattrName = transfers_test_utils:random_job_name(?FUNCTION_NAME),
    XattrValue = 1,
    Xattr = #xattr{name = XattrName, value = XattrValue},
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),

    NotExistingUuid = <<"not_existing_uuid">>,
    NotExistingGuid = file_id:pack_guid(NotExistingUuid, SpaceId),
    {ok, NotExistingFileId} = file_id:guid_to_objectid(NotExistingGuid),

    %functions emits not existing file id
    MapFunction = <<
        "function (id, type, meta, ctx) {
            if(type == 'custom_metadata' && meta['", XattrName/binary,"']) {
                return [meta['", XattrName/binary, "'], '", NotExistingFileId/binary, "'];
            }
        return null;
    }">>,
    ViewName = transfers_test_utils:random_view_name(?FUNCTION_NAME),
    transfers_test_utils:create_view(WorkerP2, SpaceId, ViewName, MapFunction, [], [ProviderId1, ProviderId2]),
    ?assertViewQuery([NotExistingFileId], WorkerP2, SpaceId, ViewName,  [{key, XattrValue}]),
    ?assertViewQuery([NotExistingFileId], WorkerP1, SpaceId, ViewName,  [{key, XattrValue}]),

    transfers_test_mechanism:run_test(
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
                    replication_status => completed,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 4,
                    files_processed => 4,
                    files_evicted => 0,
                    failed_files => 0
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                assert_transferred_file_model = false,
                attempts = 120,
                timeout = timer:minutes(2)
            }
        }
    ).

scheduling_migration_by_empty_view_should_succeed(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID,
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

    XattrName = transfers_test_utils:random_job_name(?FUNCTION_NAME),
    ViewName = transfers_test_utils:random_view_name(?FUNCTION_NAME),
    MapFunction = transfers_test_utils:test_map_function(XattrName),
    transfers_test_utils:create_view(WorkerP2, SpaceId, ViewName, MapFunction, [], [ProviderId1, ProviderId2]),
    ?assertViewQuery([], WorkerP1, SpaceId, ViewName, []),
    ?assertViewQuery([], WorkerP2, SpaceId, ViewName, []),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                evicting_nodes = [WorkerP1],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:migrate_replicas_from_view/2,
                query_view_params = [],
                view_name = ViewName
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
    ).

scheduling_migration_by_not_existing_key_in_view_should_succeed(Config, Type) ->
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
    XattrValue2 = 2,
    Xattr = #xattr{name = XattrName, value = XattrValue},
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),
    ViewName = transfers_test_utils:random_view_name(?FUNCTION_NAME),
    MapFunction = transfers_test_utils:test_map_function(XattrName),
    transfers_test_utils:create_view(WorkerP2, SpaceId, ViewName, MapFunction, [], [ProviderId1, ProviderId2]),

    ?assertViewQuery([FileId], WorkerP1, SpaceId, ViewName, [{key, XattrValue}]),
    ?assertViewQuery([FileId], WorkerP2, SpaceId, ViewName, [{key, XattrValue}]),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                evicting_nodes = [WorkerP1],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:migrate_replicas_from_view/2,
                query_view_params = [{key, XattrValue2}],
                view_name = ViewName
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
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 0
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assert_transferred_file_model = false,
                attempts = 120,
                timeout = timer:minutes(2)
            }
        }
    ).

schedule_migration_of_100_regular_files_by_view(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId2 = ?DEFAULT_SESSION(WorkerP2, Config),
    SpaceId = ?SPACE_ID,
    NumberOfFiles = 100,
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

    Config2 = transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                files_structure = [{0, NumberOfFiles}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type),
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }}),

    FileGuidsAndPaths = ?config(?FILES_KEY, Config2),

    % set xattr on file to be replicated
    XattrName = transfers_test_utils:random_job_name(?FUNCTION_NAME),
    XattrValue = 1,
    Xattr = #xattr{name = XattrName, value = XattrValue},

    FileIds = lists:map(fun({FileGuid, _}) ->
        ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),
        {ok, FileId} = file_id:guid_to_objectid(FileGuid),
        FileId
    end, FileGuidsAndPaths),

    ViewName = transfers_test_utils:random_view_name(?FUNCTION_NAME),
    MapFunction = transfers_test_utils:test_map_function(XattrName),
    transfers_test_utils:create_view(WorkerP2, SpaceId, ViewName, MapFunction, [], [ProviderId1, ProviderId2]),
    ?assertViewQuery(FileIds, WorkerP1, SpaceId, ViewName, [{key, XattrValue}]),
    ?assertViewQuery(FileIds, WorkerP2, SpaceId, ViewName, [{key, XattrValue}]),

    transfers_test_mechanism:run_test(
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
                    replication_status => completed,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    replicating_provider => transfers_test_utils:provider_id(WorkerP2),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 2 * (NumberOfFiles + 1),
                    files_processed => 2 * (NumberOfFiles + 1),
                    files_replicated => NumberOfFiles,
                    bytes_replicated => NumberOfFiles * ?DEFAULT_SIZE,
                    files_evicted => NumberOfFiles
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => []},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assert_transferred_file_model = false,
                attempts = 600,
                timeout = timer:minutes(10)
            }
        }
    ).

cancel_migration_on_target_nodes_by_scheduling_user(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    transfers_test_utils:mock_prolonged_replication(WorkerP2, 0.5, 15),
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
                    files_to_process => 111,
                    files_processed => 111,
                    failed_files => 0,
                    files_replicated => fun(X) -> X < 111 end
                },
                distribution = undefined,
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

cancel_migration_on_target_nodes_by_other_user(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    transfers_test_utils:mock_prolonged_replication(WorkerP2, 0.5, 15),
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
                    files_to_process => 111,
                    files_processed => 111,
                    failed_files => 0,
                    files_replicated => fun(X) -> X < 111 end
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
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),
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
                    files_to_process => 2,
                    files_processed => 2,
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
                    files_to_process => 4,
                    files_processed => 4,
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
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?APP_NAME, prefetching, off),
            test_utils:set_env(Worker, ?APP_NAME, rerun_transfers, false)
        end, ?config(op_worker_nodes, NewConfig2)),

        application:start(ssl),
        hackney:start(),
        NewConfig3 = initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig2, "env_desc.json"), NewConfig2),
        NewConfig3
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, transfers_test_utils, transfers_test_mechanism, ?MODULE]}
        | Config
    ].

init_per_testcase(schedule_migration_of_100_regular_files_by_view_with_batch_100, Config) ->
    Nodes = [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    {ok, OldReplicationBatch} = test_utils:get_env(WorkerP2, op_worker, replication_by_view_batch),
    test_utils:set_env(Nodes, op_worker, replication_by_view_batch, 100),
    {ok, OldEvictionBatch} = test_utils:get_env(WorkerP2, op_worker, replica_eviction_by_view_batch),
    test_utils:set_env(Nodes, op_worker, replica_eviction_by_view_batch, 100),
    init_per_testcase(all, [
        {replication_by_view_batch, OldReplicationBatch},
        {replica_eviction_by_view_batch, OldEvictionBatch} | Config
    ]);

init_per_testcase(schedule_migration_of_100_regular_files_by_view_with_batch_10, Config) ->
    Nodes = [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    {ok, OldReplicationBatch} = test_utils:get_env(WorkerP2, op_worker, replication_by_view_batch),
    test_utils:set_env(Nodes, op_worker, replication_by_view_batch, 10),
    {ok, OldEvictionBatch} = test_utils:get_env(WorkerP2, op_worker, replica_eviction_by_view_batch),
    test_utils:set_env(Nodes, op_worker, replica_eviction_by_view_batch, 10),
    init_per_testcase(all, [
        {replication_by_view_batch, OldReplicationBatch},
        {replica_eviction_by_view_batch, OldEvictionBatch} | Config
    ]);

init_per_testcase(_Case, Config) ->
    ct:timetrap(timer:minutes(10)),
    lfm_proxy:init(Config),
    [{space_id, ?SPACE_ID} | Config].

end_per_testcase(Case, Config) when
    Case =:= schedule_migration_of_100_regular_files_by_view_with_batch_100;
    Case =:= schedule_migration_of_100_regular_files_by_view_with_batch_10
    ->
    Nodes = ?config(op_worker_nodes, Config),
    OldReplicationBatch = ?config(replication_by_view_batch, Config),
    OldEvictionBatch = ?config(replica_eviction_by_view_batch, Config),
    test_utils:set_env(Nodes, op_worker, replication_by_view_batch, OldReplicationBatch),
    test_utils:set_env(Nodes, op_worker, replica_eviction_by_view_batch, OldEvictionBatch),
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
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).
