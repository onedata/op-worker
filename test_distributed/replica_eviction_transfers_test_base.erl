%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains base test of eviction.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_eviction_transfers_test_base).
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
    evict_empty_dir/3,
    evict_tree_of_empty_dirs/3,
    evict_regular_file_replica/3,
    evict_regular_file_replica_in_directory/3,
    evict_big_file_replica/3,
    evict_100_files_in_one_request/3,
    evict_100_files_each_file_separately/3,
    many_simultaneous_failed_replica_evictions/3,
    rerun_replica_eviction/3,
    rerun_replica_eviction_by_other_user/3,
    rerun_dir_eviction/3,
    rerun_view_eviction/2,
    cancel_replica_eviction_on_target_nodes_by_scheduling_user/2,
    cancel_replica_eviction_on_target_nodes_by_other_user/2,
    fail_to_evict_file_replica_without_permissions/3,
    eviction_should_succeed_when_remote_provider_modified_file_replica/3,
    eviction_should_fail_when_evicting_provider_modified_file_replica/3,
    quota_decreased_after_eviction/3,
    schedule_replica_eviction_by_view/2,
    schedule_eviction_of_regular_file_by_view_with_reduce/2,
    scheduling_replica_eviction_by_not_existing_view_should_fail/2,
    scheduling_replica_eviction_by_view_with_function_returning_wrong_value_should_fail/2,
    scheduling_replica_eviction_by_view_returning_not_existing_file_should_not_fail/2,
    scheduling_replica_eviction_by_empty_view_should_succeed/2,
    scheduling_replica_eviction_by_not_existing_key_in_view_should_succeed/2,
    schedule_replica_eviction_of_100_regular_files_by_view/2,
    remove_file_during_eviction/3
]).

-define(SPACE_ID, <<"space1">>).
-define(assertQuota(Worker, SpaceId, ExpectedSize),
    ?assertEqual(ExpectedSize, case rpc:call(Worker, space_quota, get, [SpaceId]) of
        {ok, #document{
            value = #space_quota{
                current_size = CurrentSize
            }
        }} -> CurrentSize;
        Error -> Error
    end
    )).

%%%===================================================================
%%% API
%%%===================================================================

evict_empty_dir(Config, Type, FileKeyType) ->
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
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:evict_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => skipped,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 0
                },
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

evict_tree_of_empty_dirs(Config, Type, FileKeyType) ->
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
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:evict_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => skipped,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 1111,
                    files_processed => 1111,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 0
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                attempts = 120
            }
        }
    ).

evict_regular_file_replica(Config, Type, FileKeyType) ->
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
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => skipped,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 1
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => []}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

evict_regular_file_replica_in_directory(Config, Type, FileKeyType) ->
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
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:evict_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => skipped,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 2,
                    files_processed => 2,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 1
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => []}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

evict_big_file_replica(Config, Type, FileKeyType) ->
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
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, Size]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, Size]]}
                ]
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => skipped,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 1
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, Size]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => []}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

evict_100_files_in_one_request(Config, Type, FileKeyType) ->
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
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:evict_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => skipped,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 101,
                    files_processed => 101,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 100
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => []}
                ],
                attempts = 600,
                timeout = timer:minutes(10)
            }
        }
    ).

evict_100_files_each_file_separately(Config, Type, FileKeyType) ->
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
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => skipped,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 1
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => []}
                ],
                attempts = 3600,
                timeout = timer:minutes(60)
            }
        }
    ).

many_simultaneous_failed_replica_evictions(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    transfers_test_utils:mock_replica_eviction_failure(WorkerP2),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{10, 0}, {0, 10}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                attempts = 60,
                timeout = timer:minutes(1)
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    eviction_status => failed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 1,
                    files_processed => 1,
                    failed_files => 1,
                    files_evicted => 0
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                attempts = 3600,
                timeout = timer:minutes(60)
            }
        }
    ),
    transfers_test_utils:unmock_replica_eviction_failure(WorkerP2).

rerun_replica_eviction(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

    transfers_test_utils:mock_replica_eviction_failure(WorkerP2),

    Config2 = transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    user_id => ?DEFAULT_USER,
                    eviction_status => failed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 1,
                    files_processed => 1,
                    failed_files => 1,
                    files_evicted => 0
                },
                assertion_nodes = [WorkerP2]
            }
        }
    ),
    Config3 = transfers_test_mechanism:move_transfer_ids_to_old_key(Config2),
    transfers_test_utils:unmock_replica_eviction_failure(WorkerP2),

    transfers_test_mechanism:run_test(
        Config3, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                function = fun transfers_test_mechanism:rerun_evictions/2
            },
            expected = #expected{
                expected_transfer = #{
                    user_id => ?DEFAULT_USER,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP2),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 1,
                    files_processed => 1,
                    failed_files => 0,
                    files_evicted => 1
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => []}
                ]
            }
        }
    ).

rerun_replica_eviction_by_other_user(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    User1 = <<"user1">>,
    User2 = <<"user2">>,
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

    transfers_test_utils:mock_replica_eviction_failure(WorkerP2),

    Config2 = transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = #scenario{
                type = Type,
                user = User1,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    user_id => User1,
                    eviction_status => failed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 1,
                    files_processed => 1,
                    failed_files => 1,
                    files_evicted => 0
                },
                assertion_nodes = [WorkerP2]
            }
        }
    ),
    Config3 = transfers_test_mechanism:move_transfer_ids_to_old_key(Config2),
    transfers_test_utils:unmock_replica_eviction_failure(WorkerP2),

    transfers_test_mechanism:run_test(
        Config3, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                user = User2,
                function = fun transfers_test_mechanism:rerun_evictions/2
            },
            expected = #expected{
                expected_transfer = #{
                    user_id => User2,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP2),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 1,
                    files_processed => 1,
                    failed_files => 0,
                    files_evicted => 1
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => []}
                ]
            }
        }
    ).

rerun_dir_eviction(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

    transfers_test_utils:mock_replica_eviction_failure(WorkerP2),

    Config2 = transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{1, 0}, {1, 2}, {0, 5}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:evict_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    user_id => ?DEFAULT_USER,
                    eviction_status => failed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 10,
                    files_processed => 10,
                    failed_files => 7,
                    files_evicted => 0
                },
                assertion_nodes = [WorkerP2]
            }
        }
    ),
    Config3 = transfers_test_mechanism:move_transfer_ids_to_old_key(Config2),
    transfers_test_utils:unmock_replica_eviction_failure(WorkerP2),

    transfers_test_mechanism:run_test(
        Config3, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                function = fun transfers_test_mechanism:rerun_evictions/2
            },
            expected = #expected{
                expected_transfer = #{
                    user_id => ?DEFAULT_USER,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP2),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 10,
                    files_processed => 10,
                    failed_files => 0,
                    files_evicted => 7
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => []}
                ]
            }
        }
    ).

rerun_view_eviction(Config, Type) ->
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
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
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
    transfers_test_utils:create_view(WorkerP2, SpaceId, ViewName, MapFunction, [], [ProviderId2]),
    ?assertViewQuery([FileId], WorkerP2, SpaceId, ViewName,  [{key, XattrValue}]),
    ?assertViewVisible(WorkerP1, SpaceId, ViewName),

    transfers_test_utils:mock_replica_eviction_failure(WorkerP2),

    Config3 = transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:evict_replicas_from_view/2,
                query_view_params = [{key, XattrValue}],
                view_name = ViewName
            },
            expected = #expected{
                expected_transfer = #{
                    eviction_status => failed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 2,
                    files_processed => 2,
                    failed_files => 1,
                    files_evicted =>  0
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                assert_transferred_file_model = false
            }
        }
    ),
    Config4 = transfers_test_mechanism:move_transfer_ids_to_old_key(Config3),
    transfers_test_utils:unmock_replica_eviction_failure(WorkerP2),

    transfers_test_mechanism:run_test(
        Config4, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                function = fun transfers_test_mechanism:rerun_view_evictions/2
            },
            expected = #expected{
                expected_transfer = #{
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP2),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 2,
                    files_processed => 2,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted =>  1
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => []}
                ],
                assert_transferred_file_model = false
            }
        }
    ).


cancel_replica_eviction_on_target_nodes_by_scheduling_user(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{10, 0}, {0, 10}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type),
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                attempts = 120,
                timeout = timer:minutes(10)
            },
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:cancel_replica_eviction_on_target_nodes_by_scheduling_user/2
            },
            expected = #expected{
                expected_transfer = #{
                    eviction_status => cancelled,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => fun(X) -> X =< 111 end,
                    files_processed => fun(X) -> X =< 111 end,
                    failed_files => 0,
                    files_evicted => fun(X) -> X  < 100 end
                },
                distribution = undefined,
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

cancel_replica_eviction_on_target_nodes_by_other_user(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    User1 = <<"user1">>,
    User2 = <<"user2">>,
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{10, 0}, {0, 10}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type),
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                attempts = 120,
                timeout = timer:minutes(10)
            },
            scenario = #scenario{
                user = User1,
                cancelling_user = User2,
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:cancel_replica_eviction_on_target_nodes_by_other_user/2
            },
            expected = #expected{
                expected_transfer = #{
                    eviction_status => cancelled,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => fun(X) -> X =< 111 end,
                    files_processed => fun(X) -> X =< 111 end,
                    failed_files => 0,
                    files_evicted => fun(X) -> X < 100 end
                },
                distribution = undefined,
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

fail_to_evict_file_replica_without_permissions(Config, Type, FileKeyType) ->
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
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = #scenario{
                user = <<"user2">>,
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:schedule_replica_eviction_without_permissions/2
            },
            expected = #expected{
                expected_transfer = undefined,
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

eviction_should_succeed_when_remote_provider_modified_file_replica(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),
    
    Config2 = transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = undefined,
            expected = undefined
        }
    ),
    [{FileGuid, _}] = ?config(?FILES_KEY, Config2),

    % bump version on WorkerP1
    SessionId = ?USER_SESSION(WorkerP1, ?DEFAULT_USER, Config2),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(WorkerP1, Handle, 1, <<"#">>),
    lfm_proxy:close(WorkerP1, Handle),

    ?assertVersion(2, WorkerP2, FileGuid, ProviderId1, ?ATTEMPTS),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => skipped,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 1
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => []}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

eviction_should_fail_when_evicting_provider_modified_file_replica(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),
    
    Config2 = transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = undefined,
            expected = undefined
        }
    ),
    [{FileGuid, _}] = ?config(?FILES_KEY, Config2),

    % pretend that file is modified on WorkerP2 just before deleting blocks from storage
    SessionId2 = ?USER_SESSION(WorkerP2, ?DEFAULT_USER, Config2),
    ok = test_utils:mock_new(WorkerP2, replica_deletion_req),
    ok = test_utils:mock_expect(WorkerP2, replica_deletion_req, delete_blocks, fun(FileCtx, Blocks, AllowedVV) ->
        {ok, Handle} = lfm:open(SessionId2, {guid, FileGuid}, write),
        {ok, _, 1} = lfm:write(Handle, 1, <<"#">>),
        ok = lfm:fsync(Handle),
        ok = lfm:release(Handle),
        % meck:passthrough does not work for functions that use other mocked functions inside
        erlang:apply(meck_util:original_name(replica_deletion_req), delete_blocks, [FileCtx, Blocks, AllowedVV])
    end),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => skipped,
                    eviction_status => failed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 1,
                    files_processed => 1,
                    failed_files => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 0
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0,1], [2, ?DEFAULT_SIZE - 2]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

quota_decreased_after_eviction(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SpaceId = ?config(?SPACE_ID_KEY, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),
    
    Config2 = transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = undefined,
            expected = undefined
        }
    ),
    ?assertQuota(WorkerP2, SpaceId, ?DEFAULT_SIZE),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => skipped,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 1
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => []}
                ],
                assertion_nodes = [WorkerP1, WorkerP2],
                attempts = 120
            }
        }
    ),
    ?assertQuota(WorkerP2, SpaceId, 0).

schedule_replica_eviction_by_view(Config, Type) ->
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
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
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
    transfers_test_utils:create_view(WorkerP2, SpaceId, ViewName, MapFunction, [], [ProviderId2]),
    ?assertViewQuery([FileId], WorkerP2, SpaceId, ViewName,  [{key, XattrValue}]),
    ?assertViewVisible(WorkerP1, SpaceId, ViewName),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:evict_replicas_from_view/2,
                query_view_params = [{key, XattrValue}],
                view_name = ViewName
            },
            expected = #expected{
                expected_transfer = #{
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 2,
                    files_processed => 2,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted =>  1
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => []}
                ],
                assert_transferred_file_model = false
            }
        }
    ).

schedule_eviction_of_regular_file_by_view_with_reduce(Config, Type) ->
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
                replicate_to_nodes = [WorkerP2],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type),
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
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
        [ProviderId2]
    ),

    {ok, ObjectId1} = file_id:guid_to_objectid(Guid1),
    {ok, ObjectId6} = file_id:guid_to_objectid(Guid6),

    ?assertViewQuery([ObjectId1, ObjectId6], WorkerP2, SpaceId, ViewName,  [{key, XattrValue11}]),
    ?assertViewVisible(WorkerP1, SpaceId, ViewName),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:evict_replicas_from_view/2,
                query_view_params = [{group, true}, {key, XattrValue11}],
                view_name = ViewName
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => skipped,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 3,
                    files_processed => 3,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted => 2
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => []}
                ],
                assert_distribution_for_files = [Guid1, Guid6],
                assertion_nodes = [WorkerP1, WorkerP2],
                assert_transferred_file_model = false
            }
        }
    ).

scheduling_replica_eviction_by_not_existing_view_should_fail(Config, Type) ->
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
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
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
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:fail_to_evict_replicas_from_view/2,
                query_view_params = [{key, XattrValue}],
                view_name = ViewName
            },
            expected = #expected{
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assert_transferred_file_model = false
            }
        }
    ).

scheduling_replica_eviction_by_view_with_function_returning_wrong_value_should_fail(Config, Type) ->
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
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
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
    transfers_test_utils:create_view(WorkerP2, SpaceId, ViewName, MapFunction, [], [ProviderId2]),
    ?assertViewQuery([WrongValue], WorkerP2, SpaceId, ViewName,  [{key, XattrValue}]),
    ?assertViewVisible(WorkerP1, SpaceId, ViewName),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:evict_replicas_from_view/2,
                query_view_params = [{key, XattrValue}],
                view_name = ViewName
            },
            expected = #expected{
                expected_transfer = #{
                    eviction_status => failed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 2,
                    files_processed => 2,
                    files_evicted => 0,
                    failed_files => 1
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assert_transferred_file_model = false
            }
        }
    ).

scheduling_replica_eviction_by_view_returning_not_existing_file_should_not_fail(Config, Type) ->
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
    transfers_test_utils:create_view(WorkerP2, SpaceId, ViewName, MapFunction, [], [ProviderId2]),
    ?assertViewQuery([NotExistingFileId], WorkerP2, SpaceId, ViewName,  [{key, XattrValue}]),
    ?assertViewVisible(WorkerP1, SpaceId, ViewName),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:evict_replicas_from_view/2,
                query_view_params = [{key, XattrValue}],
                view_name = ViewName
            },
            expected = #expected{
                expected_transfer = #{
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 2,
                    files_processed => 2,
                    files_evicted => 0,
                    failed_files => 0
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                assert_transferred_file_model = false
            }
        }
    ).


scheduling_replica_eviction_by_empty_view_should_succeed(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID,
    XattrName = transfers_test_utils:random_job_name(?FUNCTION_NAME),
    ViewName = transfers_test_utils:random_view_name(?FUNCTION_NAME),
    MapFunction = transfers_test_utils:test_map_function(XattrName),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),
    
    transfers_test_utils:create_view(WorkerP2, SpaceId, ViewName, MapFunction, [], [ProviderId2]),
    ?assertViewQuery([], WorkerP2, SpaceId, ViewName,  []),
    ?assertViewVisible(WorkerP1, SpaceId, ViewName),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:evict_replicas_from_view/2,
                query_view_params = [],
                view_name = ViewName
            },
            expected = #expected{
                expected_transfer = #{
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 1,
                    files_processed => 1,
                    files_evicted => 0
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = undefined,
                assert_transferred_file_model = false
            }
        }
    ).

scheduling_replica_eviction_by_not_existing_key_in_view_should_succeed(Config, Type) ->
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
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
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
    transfers_test_utils:create_view(WorkerP2, SpaceId, ViewName, MapFunction, [], [ProviderId2]),
    ?assertViewQuery([FileId], WorkerP2, SpaceId, ViewName,  [{key, XattrValue}]),
    ?assertViewVisible(WorkerP1, SpaceId, ViewName),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:evict_replicas_from_view/2,
                query_view_params = [{key, XattrValue2}],
                view_name = ViewName
            },
            expected = #expected{
                expected_transfer = #{
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 1,
                    files_processed => 1,
                    files_evicted => 0
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assert_transferred_file_model = false
            }
        }
    ).

schedule_replica_eviction_of_100_regular_files_by_view(Config, Type) ->
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
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
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
    transfers_test_utils:create_view(WorkerP2, SpaceId, ViewName, MapFunction, [], [ProviderId2]),
    ?assertViewQuery(FileIds, WorkerP2, SpaceId, ViewName, [{key, XattrValue}]),
    ?assertViewVisible(WorkerP1, SpaceId, ViewName),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:evict_replicas_from_view/2,
                query_view_params = [{key, XattrValue}],
                view_name = ViewName
            },
            expected = #expected{
                expected_transfer = #{
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => NumberOfFiles + 1,
                    files_processed => NumberOfFiles + 1,
                    files_evicted => NumberOfFiles
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => []}
                ],
                assert_transferred_file_model = false,
                attempts = 600,
                timeout = timer:minutes(10)
            }
        }
    ).

remove_file_during_eviction(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                replicate_to_nodes = [WorkerP2],
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP2), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:remove_file_during_eviction/2
            },
            expected = #expected{
                expected_transfer = #{
                    eviction_status => failed,
                    files_to_process => 1,
                    files_processed => 1,
                    failed_files => 1,
                    files_evicted => 0
                },
                assertion_nodes = [WorkerP1, WorkerP2]
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
            test_utils:set_env(Worker, ?APP_NAME, dbsync_out_stream_handling_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
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

init_per_testcase(Case, Config)
    when Case =:= cancel_replica_eviction_on_target_nodes_by_other_user
    orelse Case =:= cancel_replica_eviction_on_target_nodes_by_scheduling_user
    ->
    Workers = ?config(op_worker_nodes, Config),
    transfers_test_utils:mock_prolonged_replica_eviction(Workers, 1, 10),
    init_per_testcase(all, Config);

init_per_testcase(quota_decreased_after_eviction, Config) ->
    init_per_testcase(all, [{?SPACE_ID_KEY, <<"space3">>} | Config]);

init_per_testcase(schedule_replica_eviction_of_100_regular_files_by_view_with_batch_100, Config) ->
    Nodes = [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    {ok, OldValue} = test_utils:get_env(WorkerP2, op_worker, replica_eviction_by_view_batch),
    test_utils:set_env(Nodes, op_worker, replica_eviction_by_view_batch, 100),
    init_per_testcase(all, [{replica_eviction_by_view_batch, OldValue} | Config]);

init_per_testcase(schedule_replica_eviction_of_100_regular_files_by_view_with_batch_10, Config) ->
    Nodes = [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    {ok, OldValue} = test_utils:get_env(WorkerP2, op_worker, replica_eviction_by_view_batch),
    test_utils:set_env(Nodes, op_worker, replica_eviction_by_view_batch, 10),
    init_per_testcase(all, [{replica_eviction_by_view_batch, OldValue} | Config]);

init_per_testcase(_Case, Config) ->
    ct:timetrap(timer:minutes(60)),
    lfm_proxy:init(Config),
    case ?config(?SPACE_ID_KEY, Config) of
        undefined ->
            [{?SPACE_ID_KEY, ?SPACE_ID} | Config];
        _ ->
            Config
    end.

end_per_testcase(Case, Config)
    when Case =:= cancel_replica_eviction_on_target_nodes_by_other_user
    orelse Case =:= cancel_replica_eviction_on_target_nodes_by_scheduling_user
    ->
    Workers = ?config(op_worker_nodes, Config),
    transfers_test_utils:unmock_prolonged_replica_eviction(Workers),
    end_per_testcase(all, Config);

end_per_testcase(eviction_should_fail_when_evicting_provider_modified_file_replica, Config) ->
    [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(WorkerP2, replica_deletion_req),
    end_per_testcase(all, Config);

end_per_testcase(Case, Config) when
    Case =:= schedule_replica_eviction_of_100_regular_files_by_view_with_batch_100;
    Case =:= schedule_replica_eviction_of_100_regular_files_by_view_with_batch_10
    ->
    Nodes = ?config(op_worker_nodes, Config),
    OldValue = ?config(replica_eviction_by_view_batch, Config),
    test_utils:set_env(Nodes, op_worker, replica_eviction_by_view_batch, OldValue),
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
