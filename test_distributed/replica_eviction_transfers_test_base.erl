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
-module(replica_eviction_transfers_test_base).
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
    evict_empty_dir/3,
    evict_tree_of_empty_dirs/3,
    evict_regular_file_replica/3,
    evict_regular_file_replica_in_directory/3,
    evict_big_file_replica/3,
    evict_100_files_in_one_request/3,
    evict_100_files_each_file_separately/3,
    fail_to_evict_file_replica_without_permissions/3,
    eviction_should_succeed_when_remote_provider_modified_file_replica/3,
    eviction_should_fail_when_evicting_provider_modified_file_replica/3,
    quota_decreased_after_eviction/3,
    schedule_replica_eviction_by_index/2,
    schedule_eviction_of_regular_file_by_index_with_reduce/2,
    scheduling_replica_eviction_by_not_existing_index_should_fail/2,
    scheduling_replica_eviction_by_index_with_function_returning_wrong_value_should_fail/2,
    scheduling_replica_eviction_by_index_returning_not_existing_file_should_not_fail/2,
    scheduling_replica_eviction_by_empty_index_should_succeed/2,
    scheduling_replica_eviction_by_not_existing_key_in_index_should_succeed/2,
    schedule_replica_eviction_of_100_regular_files_by_index/2
]).

-define(SPACE_ID, <<"space1">>).
-define(assertQuota(Worker, SpaceId, ExpectedSize), {
    ?assertEqual(ExpectedSize, case rpc:call(Worker, space_quota, get, [SpaceId]) of
        {ok, #document{
            value = #space_quota{
                current_size = CurrentSize
            }
        }} -> CurrentSize;
        Error -> Error
    end
    )}).

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

    % bump version on WorkerP2
    SessionId = ?USER_SESSION(WorkerP1, ?DEFAULT_USER, Config2),
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
    {ok, _} = lfm_proxy:write(WorkerP1, Handle, 1, <<"#">>),
    lfm_proxy:close(WorkerP1, Handle),

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
        {ok, Handle} = logical_file_manager:open(SessionId2, {guid, FileGuid}, write),
        {ok, _, 1} = logical_file_manager:write(Handle, 1, <<"#">>),
        ok = logical_file_manager:fsync(Handle),
        ok = logical_file_manager:release(Handle),
        meck:passthrough([FileCtx, Blocks, AllowedVV])
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

schedule_replica_eviction_by_index(Config, Type) ->
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
    {ok, FileId} = cdmi_id:guid_to_objectid(FileGuid),

    % set xattr on file to be replicated
    XattrName = transfers_test_utils:random_job_name(?FUNCTION_NAME),
    XattrValue = 1,
    Xattr = #xattr{name = XattrName, value = XattrValue},
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),
    IndexName = transfers_test_utils:random_index_name(?FUNCTION_NAME),
    MapFunction = transfers_test_utils:test_map_function(XattrName),
    transfers_test_utils:create_index(WorkerP2, SpaceId, IndexName, MapFunction, [], [ProviderId2]),
    ?assertIndexQuery([FileId], WorkerP2, SpaceId, IndexName,  [{key, XattrValue}]),
    ?assertIndexVisible(WorkerP1, SpaceId, IndexName),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:evict_replicas_from_index/2,
                query_view_params = [{key, XattrValue}],
                index_name = IndexName
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

schedule_eviction_of_regular_file_by_index_with_reduce(Config, Type) ->
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

    IndexName = transfers_test_utils:random_index_name(?FUNCTION_NAME),
    MapFunction = transfers_test_utils:test_map_function(XattrName1, XattrName2),
    ReduceFunction = transfers_test_utils:test_reduce_function(XattrValue21),

    ok = transfers_test_utils:create_index(WorkerP2, SpaceId, IndexName,
        MapFunction, ReduceFunction, [{group, 1}, {key, XattrValue11}],
        [ProviderId2]
    ),

    {ok, ObjectId1} = cdmi_id:guid_to_objectid(Guid1),
    {ok, ObjectId6} = cdmi_id:guid_to_objectid(Guid6),

    ?assertIndexQuery([ObjectId1, ObjectId6], WorkerP2, SpaceId, IndexName,  [{key, XattrValue11}]),
    ?assertIndexVisible(WorkerP1, SpaceId, IndexName),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:evict_replicas_from_index/2,
                query_view_params = [{group, true}, {key, XattrValue11}],
                index_name = IndexName
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

scheduling_replica_eviction_by_not_existing_index_should_fail(Config, Type) ->
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
    IndexName = transfers_test_utils:random_index_name(?FUNCTION_NAME),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:fail_to_evict_replicas_from_index/2,
                query_view_params = [{key, XattrValue}],
                index_name = IndexName
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

scheduling_replica_eviction_by_index_with_function_returning_wrong_value_should_fail(Config, Type) ->
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
        "function (id, meta) {
            if(meta['", XattrName/binary,"']) {
                return [meta['", XattrName/binary, "'], '", WrongValue/binary, "'];
            }
        return null;
    }">>,
    IndexName = transfers_test_utils:random_index_name(?FUNCTION_NAME),
    transfers_test_utils:create_index(WorkerP2, SpaceId, IndexName, MapFunction, [], [ProviderId2]),
    ?assertIndexQuery([WrongValue], WorkerP2, SpaceId, IndexName,  [{key, XattrValue}]),
    ?assertIndexVisible(WorkerP1, SpaceId, IndexName),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:evict_replicas_from_index/2,
                query_view_params = [{key, XattrValue}],
                index_name = IndexName
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

scheduling_replica_eviction_by_index_returning_not_existing_file_should_not_fail(Config, Type) ->
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
    NotExistingGuid = fslogic_uuid:uuid_to_guid(NotExistingUuid, SpaceId),
    {ok, NotExistingFileId} = cdmi_id:guid_to_objectid(NotExistingGuid),

    %functions emits not existing file id
    MapFunction = <<
        "function (id, meta) {
            if(meta['", XattrName/binary,"']) {
                return [meta['", XattrName/binary, "'], '", NotExistingFileId/binary, "'];
            }
        return null;
    }">>,
    IndexName = transfers_test_utils:random_index_name(?FUNCTION_NAME),
    transfers_test_utils:create_index(WorkerP2, SpaceId, IndexName, MapFunction, [], [ProviderId2]),
    ?assertIndexQuery([NotExistingFileId], WorkerP2, SpaceId, IndexName,  [{key, XattrValue}]),
    ?assertIndexVisible(WorkerP1, SpaceId, IndexName),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:evict_replicas_from_index/2,
                query_view_params = [{key, XattrValue}],
                index_name = IndexName
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


scheduling_replica_eviction_by_empty_index_should_succeed(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID,
    XattrName = transfers_test_utils:random_job_name(?FUNCTION_NAME),
    IndexName = transfers_test_utils:random_index_name(?FUNCTION_NAME),
    MapFunction = transfers_test_utils:test_map_function(XattrName),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),
    
    transfers_test_utils:create_index(WorkerP2, SpaceId, IndexName, MapFunction, [], [ProviderId2]),
    ?assertIndexQuery([], WorkerP2, SpaceId, IndexName,  []),
    ?assertIndexVisible(WorkerP1, SpaceId, IndexName),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:evict_replicas_from_index/2,
                query_view_params = [],
                index_name = IndexName
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

scheduling_replica_eviction_by_not_existing_key_in_index_should_succeed(Config, Type) ->
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
    {ok, FileId} = cdmi_id:guid_to_objectid(FileGuid),

    % set xattr on file to be replicated
    XattrName = transfers_test_utils:random_job_name(?FUNCTION_NAME),
    XattrValue = 1,
    XattrValue2 = 2,
    Xattr = #xattr{name = XattrName, value = XattrValue},
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),
    IndexName = transfers_test_utils:random_index_name(?FUNCTION_NAME),
    MapFunction = transfers_test_utils:test_map_function(XattrName),
    transfers_test_utils:create_index(WorkerP2, SpaceId, IndexName, MapFunction, [], [ProviderId2]),
    ?assertIndexQuery([FileId], WorkerP2, SpaceId, IndexName,  [{key, XattrValue}]),
    ?assertIndexVisible(WorkerP1, SpaceId, IndexName),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:evict_replicas_from_index/2,
                query_view_params = [{key, XattrValue2}],
                index_name = IndexName
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

schedule_replica_eviction_of_100_regular_files_by_index(Config, Type) ->
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
        {ok, FileId} = cdmi_id:guid_to_objectid(FileGuid),
        FileId
    end, FileGuidsAndPaths),

    IndexName = transfers_test_utils:random_index_name(?FUNCTION_NAME),
    MapFunction = transfers_test_utils:test_map_function(XattrName),
    transfers_test_utils:create_index(WorkerP2, SpaceId, IndexName, MapFunction, [], [ProviderId2]),
    ?assertIndexQuery(FileIds, WorkerP2, SpaceId, IndexName, [{key, XattrValue}]),
    ?assertIndexVisible(WorkerP1, SpaceId, IndexName),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                evicting_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:evict_replicas_from_index/2,
                query_view_params = [{key, XattrValue}],
                index_name = IndexName
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

init_per_testcase(quota_decreased_after_eviction, Config) ->
    init_per_testcase(all, [{?SPACE_ID_KEY, <<"space3">>} | Config]);

init_per_testcase(schedule_replica_eviction_of_100_regular_files_by_index_with_batch_100, Config) ->
    Nodes = [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    {ok, OldValue} = test_utils:get_env(WorkerP2, op_worker, replica_eviction_by_index_batch),
    test_utils:set_env(Nodes, op_worker, replica_eviction_by_index_batch, 100),
    init_per_testcase(all, [{replica_eviction_by_index_batch, OldValue} | Config]);

init_per_testcase(schedule_replica_eviction_of_100_regular_files_by_index_with_batch_10, Config) ->
    Nodes = [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    {ok, OldValue} = test_utils:get_env(WorkerP2, op_worker, replica_eviction_by_index_batch),
    test_utils:set_env(Nodes, op_worker, replica_eviction_by_index_batch, 10),
    init_per_testcase(all, [{replica_eviction_by_index_batch, OldValue} | Config]);

init_per_testcase(_Case, Config) ->
    ct:timetrap(timer:minutes(60)),
    lfm_proxy:init(Config),
    case ?config(?SPACE_ID_KEY, Config) of
        undefined ->
            [{?SPACE_ID_KEY, ?SPACE_ID} | Config];
        _ ->
            Config
    end.

end_per_testcase(eviction_should_fail_when_evicting_provider_modified_file_replica, Config) ->
    [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(WorkerP2, replica_deletion_req),
    end_per_testcase(all, Config);

end_per_testcase(Case, Config) when
    Case =:= schedule_replica_eviction_of_100_regular_files_by_index_with_batch_100;
    Case =:= schedule_replica_eviction_of_100_regular_files_by_index_with_batch_10
    ->
    Nodes = ?config(op_worker_nodes, Config),
    OldValue = ?config(replica_eviction_by_index_batch, Config),
    test_utils:set_env(Nodes, op_worker, replica_eviction_by_index_batch, OldValue),
    end_per_testcase(all, Config);

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    transfers_test_utils:unmock_sync_req(Workers),
    transfers_test_utils:unmock_replica_synchronizer_failure(Workers),
    transfers_test_utils:remove_transfers(Config),
    transfers_test_utils:remove_all_indexes(Workers, ?SPACE_ID),
    transfers_test_utils:ensure_transfers_removed(Config).

end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).
