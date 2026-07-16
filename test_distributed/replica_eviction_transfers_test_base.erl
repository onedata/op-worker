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
                    files_to_process => 7,
                    files_processed => 7,
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
                    files_to_process => 7,
                    files_processed => 7,
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
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, ?FILE_REF(FileGuid), Xattr),
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
                    files_to_process => 1,
                    files_processed => 1,
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
                    files_to_process => 1,
                    files_processed => 1,
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
                    files_to_process => fun(X) -> X =< 100 end,
                    files_processed => fun(X) -> X =< 100 end,
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
                    files_to_process => fun(X) -> X =< 100 end,
                    files_processed => fun(X) -> X =< 100 end,
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
                files_structure = [{1, 2}, {0, 3}],
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
                expected_transfer = #{
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP2),
                    evicting_provider => transfers_test_utils:provider_id(WorkerP2),
                    files_to_process => 2,
                    files_processed => 2,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    files_evicted =>  2
                },
                distribution = undefined,
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
    {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, ?FILE_REF(FileGuid), write),
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
        {ok, Handle} = lfm:open(SessionId2, ?FILE_REF(FileGuid), write),
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
            expected = [
                % File was removed before replica_deletion_master received request
                #expected{
                    expected_transfer = #{
                        eviction_status => completed,
                        files_to_process => 1,
                        files_processed => 1,
                        failed_files => 0,
                        files_evicted => 0
                    },
                    assertion_nodes = [WorkerP1, WorkerP2]
                },
                % File was removed after replica_deletion_master received request
                #expected{
                    expected_transfer = #{
                        eviction_status => failed,
                        files_to_process => 1,
                        files_processed => 1,
                        failed_files => 1,
                        files_evicted => 0
                    },
                    assertion_nodes = [WorkerP1, WorkerP2]
                }
            ]
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
        NewConfig2 = initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig1, "env_desc.json"), NewConfig1),
        NewConfig2
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
