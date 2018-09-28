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
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

-export([init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

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

    schedule_migration_by_index/2,
    scheduling_migration_by_not_existing_index_should_fail/2,
    scheduling_migration_by_empty_index_should_succeed/2,
    scheduling_migration_by_not_existing_key_in_index_should_succeed/2,
    schedule_migration_of_100_regular_files_by_index/2
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
    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
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
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => []},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP2), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

migrate_regular_file_replica_in_directory(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
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
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => []},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP2), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

migrate_big_file_replica(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Size = 1024 * 1024 * 1024,
    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                size = Size,
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, Size]]}
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
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => []},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP2), <<"blocks">> => [[0, Size]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2],
                attempts = 120
            }
        }
    ).

migrate_100_files_in_one_request(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 100}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
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
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => []},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP2), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2],
                attempts = 600,
                timeout = timer:minutes(10)
            }
        }
    ).

migrate_100_files_each_file_separately(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 100}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
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
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => []},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP2), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                attempts = 600,
                timeout = timer:minutes(10)
            }
        }
    ).

fail_to_migrate_file_replica_without_permissions(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
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
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

schedule_migration_by_index(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId2 = ?DEFAULT_SESSION(WorkerP2, Config),
    SpaceId = ?SPACE_ID,
    Config2 = transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type),
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }}),

    [{FileGuid, _}] = ?config(?FILES_KEY, Config2),

    % set xattr on file to be replicated
    XattrName = transfers_test_utils:random_job_name(),
    XattrValue = 1,
    Xattr = #xattr{name = XattrName, value = XattrValue},
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),
    ViewName = <<"migration_jobs">>,
    ViewFunction = transfers_test_utils:view_function(XattrName),
    {ok, ViewId} = rpc:call(WorkerP1, indexes, add_index, [?DEFAULT_USER, ViewName, ViewFunction, SpaceId, false]),
    {ok, ViewId} = rpc:call(WorkerP2, indexes, add_index, [?DEFAULT_USER, ViewName, ViewFunction, SpaceId, false]),

    ?assertMatch({ok, [FileGuid]},
        rpc:call(WorkerP1, indexes, query_view_and_filter_values, [ViewId, [{key, XattrValue}]]),
        ?ATTEMPTS),
    ?assertMatch({ok, [FileGuid]},
        rpc:call(WorkerP2, indexes, query_view_and_filter_values, [ViewId, [{key, XattrValue}]]),
        ?ATTEMPTS),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                evicting_nodes = [WorkerP1],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:migrate_replicas_from_index/2,
                query_view_params = [{key, XattrValue}],
                index_id = ViewId
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
                    min_hist => ?MIN_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => ?DEFAULT_SIZE}),
                    hr_hist => ?HOUR_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => ?DEFAULT_SIZE}),
                    dy_hist => ?DAY_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => ?DEFAULT_SIZE}),
                    mth_hist => ?MONTH_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => ?DEFAULT_SIZE})
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => []},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP2), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assert_transferred_file_model = false
            }
        }
    ).

scheduling_migration_by_not_existing_index_should_fail(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId2 = ?DEFAULT_SESSION(WorkerP2, Config),
    SpaceId = ?SPACE_ID,

    Config2 = transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type),
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }}),

    [{FileGuid, _}] = ?config(?FILES_KEY, Config2),

    % set xattr on file to be replicated
    XattrName = transfers_test_utils:random_job_name(),
    XattrValue = 1,
    Xattr = #xattr{name = XattrName, value = XattrValue},
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),
    IndexId = <<"not_existing_index">>,

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                evicting_nodes = [WorkerP1],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:migrate_replicas_from_index/2,
                query_view_params = [{key, XattrValue}],
                index_id = IndexId
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
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 0,
                    failed_files => 1
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assert_transferred_file_model = false
            }
        }
    ).

scheduling_migration_by_empty_index_should_succeed(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID,
    XattrName = transfers_test_utils:random_job_name(),
    ViewName = <<"migration_jobs">>,
    ViewFunction = transfers_test_utils:view_function(XattrName),
    {ok, ViewId} = rpc:call(WorkerP1, indexes, add_index, [?DEFAULT_USER, ViewName, ViewFunction, SpaceId, false]),
    {ok, ViewId} = rpc:call(WorkerP2, indexes, add_index, [?DEFAULT_USER, ViewName, ViewFunction, SpaceId, false]),
    ?assertMatch({ok, []},
        rpc:call(WorkerP1, indexes, query_view_and_filter_values, [ViewId, []]),
        ?ATTEMPTS),
    ?assertMatch({ok, []},
        rpc:call(WorkerP2, indexes, query_view_and_filter_values, [ViewId, []]),
        ?ATTEMPTS),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                evicting_nodes = [WorkerP1],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:migrate_replicas_from_index/2,
                query_view_params = [],
                index_id = ViewId
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
                assert_transferred_file_model = false
            }
        }
    ).

scheduling_migration_by_not_existing_key_in_index_should_succeed(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId2 = ?DEFAULT_SESSION(WorkerP2, Config),
    SpaceId = ?SPACE_ID,

    Config2 = transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type),
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }}),

    [{FileGuid, _}] = ?config(?FILES_KEY, Config2),

    % set xattr on file to be replicated
    XattrName = transfers_test_utils:random_job_name(),
    XattrValue = 1,
    XattrValue2 = 2,
    Xattr = #xattr{name = XattrName, value = XattrValue},
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),
    ViewName = <<"migration_jobs">>,
    ViewFunction = transfers_test_utils:view_function(XattrName),
    {ok, ViewId} = rpc:call(WorkerP1, indexes, add_index, [?DEFAULT_USER, ViewName, ViewFunction, SpaceId, false]),
    {ok, ViewId} = rpc:call(WorkerP2, indexes, add_index, [?DEFAULT_USER, ViewName, ViewFunction, SpaceId, false]),

    ?assertMatch({ok, [FileGuid]},
        rpc:call(WorkerP1, indexes, query_view_and_filter_values, [ViewId, [{key, XattrValue}]]),
        ?ATTEMPTS),
    ?assertMatch({ok, [FileGuid]},
        rpc:call(WorkerP2, indexes, query_view_and_filter_values, [ViewId, [{key, XattrValue}]]),
        ?ATTEMPTS),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                evicting_nodes = [WorkerP1],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:migrate_replicas_from_index/2,
                query_view_params = [{key, XattrValue2}],
                index_id = ViewId
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
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assert_transferred_file_model = false
            }
        }
    ).

schedule_migration_of_100_regular_files_by_index(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId2 = ?DEFAULT_SESSION(WorkerP2, Config),
    SpaceId = ?SPACE_ID,
    NumberOfFiles = 100,
    Config2 = transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                files_structure = [{0, NumberOfFiles}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type),
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }}),

    FileGuidsAndPaths = ?config(?FILES_KEY, Config2),

    % set xattr on file to be replicated
    XattrName = transfers_test_utils:random_job_name(),
    XattrValue = 1,
    Xattr = #xattr{name = XattrName, value = XattrValue},

    FileGuids = lists:map(fun({FileGuid, _}) ->
        ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),
        FileGuid
    end, FileGuidsAndPaths),

    ViewName = <<"migration_jobs">>,
    ViewFunction = transfers_test_utils:view_function(XattrName),
    {ok, ViewId} = rpc:call(WorkerP1, indexes, add_index, [?DEFAULT_USER, ViewName, ViewFunction, SpaceId, false]),
    {ok, ViewId} = rpc:call(WorkerP2, indexes, add_index, [?DEFAULT_USER, ViewName, ViewFunction, SpaceId, false]),

    FileGuidsSorted = lists:sort(FileGuids),
    ?assertMatch(FileGuidsSorted,
        case rpc:call(WorkerP1, indexes, query_view_and_filter_values, [ViewId, [{key, XattrValue}]]) of
            {ok, Results} -> lists:sort(Results);
            Other -> Other
        end,
        ?ATTEMPTS),
    ?assertMatch(FileGuidsSorted,
        case rpc:call(WorkerP2, indexes, query_view_and_filter_values, [ViewId, [{key, XattrValue}]]) of
            {ok, Results} -> lists:sort(Results);
            Other -> Other
        end,
        ?ATTEMPTS),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                evicting_nodes = [WorkerP1],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:migrate_replicas_from_index/2,
                query_view_params = [{key, XattrValue}],
                index_id = ViewId
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
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => []},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP2), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assert_transferred_file_model = false
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
            test_utils:set_env(Worker, ?APP_NAME, prefetching, off)
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

init_per_testcase(schedule_migration_of_100_regular_files_by_index_with_batch_100, Config) ->
    Nodes = [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    {ok, OldReplicationBatch} = test_utils:get_env(WorkerP2, op_worker, replication_by_index_batch),
    test_utils:set_env(Nodes, op_worker, replication_by_index_batch, 100),
    {ok, OldEvictionBatch} = test_utils:get_env(WorkerP2, op_worker, replica_eviction_by_index_batch),
    test_utils:set_env(Nodes, op_worker, replica_eviction_by_index_batch, 100),
    init_per_testcase(all, [
        {replication_by_index_batch, OldReplicationBatch},
        {replica_eviction_by_index_batch, OldEvictionBatch} | Config
    ]);

init_per_testcase(schedule_migration_of_100_regular_files_by_index_with_batch_10, Config) ->
    Nodes = [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    {ok, OldReplicationBatch} = test_utils:get_env(WorkerP2, op_worker, replication_by_index_batch),
    test_utils:set_env(Nodes, op_worker, replication_by_index_batch, 10),
    {ok, OldEvictionBatch} = test_utils:get_env(WorkerP2, op_worker, replica_eviction_by_index_batch),
    test_utils:set_env(Nodes, op_worker, replica_eviction_by_index_batch, 10),
    init_per_testcase(all, [
        {replication_by_index_batch, OldReplicationBatch},
        {replica_eviction_by_index_batch, OldEvictionBatch} | Config
    ]);

init_per_testcase(_Case, Config) ->
    ct:timetrap(timer:minutes(10)),
    lfm_proxy:init(Config),
    [{space_id, ?SPACE_ID} | Config].

end_per_testcase(Case, Config) when
    Case =:= schedule_migration_of_100_regular_files_by_index_with_batch_100;
    Case =:= schedule_migration_of_100_regular_files_by_index_with_batch_10
    ->
    Nodes = ?config(op_worker_nodes, Config),
    OldReplicationBatch = ?config(replication_by_index_batch, Config),
    OldEvictionBatch = ?config(replica_eviction_by_index_batch, Config),
    test_utils:set_env(Nodes, op_worker, replication_by_index_batch, OldReplicationBatch),
    test_utils:set_env(Nodes, op_worker, replica_eviction_by_index_batch, OldEvictionBatch),
    end_per_testcase(all, Config);

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    transfers_test_utils:unmock_sync_req(Workers),
    transfers_test_utils:unmock_replica_synchronizer_failure(Workers),
    transfers_test_utils:remove_transfers(Config),
    rpc:multicall(Workers, transfer, restart_pools, []),
    transfers_test_utils:remove_all_indexes(Workers, ?DEFAULT_USER),
    transfers_test_utils:ensure_transfers_removed(Config).

end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).
