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
-include("transfers_test_mechanism.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

-export([init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

%% API
-export([
    replicate_empty_dir/3,
    replicate_tree_of_empty_dirs/3,
    replicate_regular_file/3,
    replicate_big_file/3,
    replicate_file_in_directory/3,
    schedule_replication_to_source_provider/3,
    not_synced_file_should_not_be_replicated/3,
    replicate_already_replicated_file/3,
    replicate_100_files_separately/3,
    replicate_100_files_in_one_transfer/3,
    replication_should_succeed_when_there_is_enough_space_for_file/3,
    replication_should_fail_when_space_is_full/3,
    replicate_to_missing_provider/3,
    replicate_to_not_supporting_provider/3,
    schedule_replication_on_not_supporting_provider/3,
    cancel_replication_on_target_nodes/2,
    file_replication_failures_should_fail_whole_transfer/3,
    many_simultaneous_failed_transfers/3,
    schedule_replication_of_regular_file_by_index/2,
    schedule_replication_of_regular_file_by_index_with_reduce/2,
    scheduling_replication_by_not_existing_index_should_fail/2,
    scheduling_replication_by_index_with_wrong_function_should_fail/2,
    scheduling_replication_by_empty_index_should_succeed/2,
    scheduling_replication_by_not_existing_key_in_index_should_succeed/2,
    schedule_replication_of_100_regular_files_by_index/2]).

-define(SPACE_ID, <<"space1">>).

%%%===================================================================
%%% API
%%%===================================================================

replicate_empty_dir(Config, Type, FileKeyType) ->
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
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 0,
                    bytes_replicated => 0
                },
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

replicate_tree_of_empty_dirs(Config, Type, FileKeyType) ->
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
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1111,
                    files_processed => 1111,
                    files_replicated => 0,
                    bytes_replicated => 0
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                attempts = 120
            }
        }
    ).

replicate_regular_file(Config, Type, FileKeyType) ->
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
                    min_hist => ?MIN_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => ?DEFAULT_SIZE})
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

replicate_file_in_directory(Config, Type, FileKeyType) ->
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
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 2,
                    files_processed => 2,
                    files_replicated => 1,
                    bytes_replicated => ?DEFAULT_SIZE,
                    min_hist => ?MIN_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => ?DEFAULT_SIZE})
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

replicate_big_file(Config, Type, FileKeyType) ->
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
                    bytes_replicated => Size
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, Size]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, Size]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

schedule_replication_to_source_provider(Config, Type, FileKeyType) ->
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
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP1],
                function = fun transfers_test_mechanism:replicate_each_file_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    min_hist => ?MIN_HIST(#{ProviderId1 => 0}),
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => 0}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => 0}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => 0})
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

replicate_already_replicated_file(Config, Type, FileKeyType) ->
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
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
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
                    min_hist => ?MIN_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => ?DEFAULT_SIZE})
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ),
    Config3 = transfers_test_mechanism:move_transfer_ids_to_old_key(Config2),
    transfers_test_mechanism:run_test(
        Config3, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP1],
                function = fun transfers_test_mechanism:replicate_each_file_separately/2,
                type = Type,
                file_key_type = FileKeyType
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    min_hist => ?MIN_HIST(#{ProviderId1 => 0}),
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => 0}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => 0}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => 0})
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

not_synced_file_should_not_be_replicated(Config, Type, FileKeyType) ->
    % list on Dir1 is mocked to return not existing file
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType)
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
                    files_replicated => 0,
                    bytes_replicated => 0,
                    min_hist => ?MIN_HIST(#{ProviderId1 => 0}),
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => 0}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => 0}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => 0})
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

replicate_100_files_separately(Config, Type, FileKeyType) ->
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
                    min_hist => ?MIN_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => ?DEFAULT_SIZE})
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP2],
                attempts = 600,
                timeout = timer:minutes(10)
            }
        }
    ).

replicate_100_files_in_one_transfer(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    FilesNum = 100,
    TotalTransferredBytes = FilesNum * ?DEFAULT_SIZE,
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),

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
                attempts = 600,
                timeout = timer:minutes(10)
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
                    replication_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 111,
                    files_processed => 111,
                    files_replicated => FilesNum,
                    bytes_replicated => TotalTransferredBytes,
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => TotalTransferredBytes}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => TotalTransferredBytes}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => TotalTransferredBytes})
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP2],
                attempts = 600,
                timeout = timer:minutes(10)
            }
        }
    ).

replication_should_succeed_when_there_is_enough_space_for_file(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Config2 = [{space_id, <<"space3">>} | Config],
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),
    Support = transfers_test_utils:get_space_support(WorkerP2, ?SPACE_ID),
    transfers_test_utils:mock_space_occupancy(WorkerP2, ?SPACE_ID, Support - ?DEFAULT_SIZE),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
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
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_each_file_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1,
                    failed_files => 0,
                    files_replicated => 1,
                    bytes_replicated => ?DEFAULT_SIZE,
                    min_hist => ?MIN_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => ?DEFAULT_SIZE})
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

replication_should_fail_when_space_is_full(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Support = transfers_test_utils:get_space_support(WorkerP2, ?SPACE_ID),
    transfers_test_utils:mock_space_occupancy(WorkerP2, ?SPACE_ID, Support),
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
                    min_hist => ?MIN_HIST(#{ProviderId1 => 0}),
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => 0}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => 0}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => 0})
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => []}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

replicate_to_missing_provider(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP1],
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
                replicating_nodes = [?MISSING_PROVIDER_NODE],
                function = fun transfers_test_mechanism:error_on_replicating_files/2
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

replicate_to_not_supporting_provider(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    % space2 is supported only by P1
    Config2 = [{space_id, <<"space2">>} | Config],
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
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
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:error_on_replicating_files/2
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

schedule_replication_on_not_supporting_provider(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Config2 = [{space_id, <<"space2">>} | Config],
    ProviderId1 = ?GET_DOMAIN_BIN(WorkerP1),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP2,
                assertion_nodes = [WorkerP1],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP2,
                replicating_nodes = [WorkerP1],
                function = fun transfers_test_mechanism:error_on_replicating_files/2
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

cancel_replication_on_target_nodes(Config, Type) ->
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
                replicating_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:cancel_replication_on_target_nodes/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => cancelled,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    failed_files => 0
                },
                distribution = undefined,
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

file_replication_failures_should_fail_whole_transfer(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    AllFiles = 111,
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
                    files_to_process => AllFiles,
                    files_processed => AllFiles,
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

schedule_replication_of_regular_file_by_index(Config, Type) ->
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
    {ok, FileId} = cdmi_id:guid_to_objectid(FileGuid),

    % set xattr on file to be replicated
    XattrName = transfers_test_utils:random_job_name(),
    XattrValue = 1,
    Xattr = #xattr{name = XattrName, value = XattrValue},
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),
    IndexName = <<"replication_jobs">>,
    MapFunction = transfers_test_utils:test_map_function(XattrName),
    transfers_test_utils:create_index(WorkerP2, SpaceId, IndexName, MapFunction,
        [], [ProviderId2]),
    ?assertIndexQuery([FileId], WorkerP2, SpaceId, IndexName,  [{key, XattrValue}]),
    ?assertIndexVisible(WorkerP1, SpaceId, IndexName),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:replicate_files_from_index/2,
                query_view_params = [{key, XattrValue}],
                index_name = IndexName
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 2,
                    files_processed => 2,
                    files_replicated => 1,
                    bytes_replicated => ?DEFAULT_SIZE,
                    min_hist => ?MIN_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => ?DEFAULT_SIZE}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => ?DEFAULT_SIZE})
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

schedule_replication_of_regular_file_by_index_with_reduce(Config, Type) ->
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
    XattrName1 = transfers_test_utils:random_job_name(),
    XattrName2 = transfers_test_utils:random_job_name(),
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

    IndexName = <<"replication_jobs">>,
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
                replicating_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:replicate_files_from_index/2,
                query_view_params = [{group, true}, {key, XattrValue11}],
                index_name = IndexName
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 3,
                    files_processed => 3,
                    files_replicated => 2,
                    bytes_replicated => 2 * ?DEFAULT_SIZE,
                    min_hist => ?MIN_HIST(#{ProviderId1 => 2 * ?DEFAULT_SIZE}),
                    hr_hist => ?HOUR_HIST(#{ProviderId1 => 2 * ?DEFAULT_SIZE}),
                    dy_hist => ?DAY_HIST(#{ProviderId1 => 2 * ?DEFAULT_SIZE}),
                    mth_hist => ?MONTH_HIST(#{ProviderId1 => 2 * ?DEFAULT_SIZE})
                },
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assert_distribution_for_files = [Guid1, Guid6],
                assertion_nodes = [WorkerP1, WorkerP2],
                assert_transferred_file_model = false
            }
        }
    ).

scheduling_replication_by_not_existing_index_should_fail(Config, Type) ->
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
    XattrName = transfers_test_utils:random_job_name(),
    XattrValue = 1,
    Xattr = #xattr{name = XattrName, value = XattrValue},
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),
    IndexName = <<"not_existing_index">>,

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:fail_to_replicate_files_from_index/2,
                query_view_params = [{key, XattrValue}],
                index_name = IndexName
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

scheduling_replication_by_index_with_wrong_function_should_fail(Config, Type) ->
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
    XattrName = transfers_test_utils:random_job_name(),
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
    IndexName = <<"index_with_wrong_function">>,
    ok = transfers_test_utils:create_index(WorkerP2, SpaceId, IndexName,
        MapFunction, [], [ProviderId2]
    ),
    ?assertIndexQuery([WrongValue], WorkerP2, SpaceId, IndexName,  [{key, XattrValue}]),
    ?assertIndexVisible(WorkerP1, SpaceId, IndexName),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:replicate_files_from_index/2,
                query_view_params = [{key, XattrValue}],
                index_name = IndexName
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => failed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 2,
                    files_processed => 2,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    failed_files => 1
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assert_transferred_file_model = false
            }
        }
    ).

scheduling_replication_by_empty_index_should_succeed(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SpaceId = ?SPACE_ID,
    XattrName = transfers_test_utils:random_job_name(),
    IndexName = <<"replication_jobs">>,
    MapFunction = transfers_test_utils:test_map_function(XattrName),
    ProviderId2 = ?GET_DOMAIN_BIN(WorkerP2),
    transfers_test_utils:create_index(WorkerP2, SpaceId, IndexName, MapFunction,
        [], [ProviderId2]
    ),
    ?assertIndexQuery([], WorkerP2, SpaceId, IndexName,  []),
    ?assertIndexVisible(WorkerP1, SpaceId, IndexName),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:replicate_files_from_index/2,
                query_view_params = [],
                index_name = IndexName
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 0,
                    bytes_replicated => 0
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = undefined,
                assert_transferred_file_model = false
            }
        }
    ).

scheduling_replication_by_not_existing_key_in_index_should_succeed(Config, Type) ->
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
    {ok, FileId} = cdmi_id:guid_to_objectid(FileGuid),

    % set xattr on file to be replicated
    XattrName = transfers_test_utils:random_job_name(),
    XattrValue = 1,
    XattrValue2 = 2,
    Xattr = #xattr{name = XattrName, value = XattrValue},
    ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),
    IndexName = <<"replication_jobs">>,
    MapFunction = transfers_test_utils:test_map_function(XattrName),
    transfers_test_utils:create_index(WorkerP2, SpaceId, IndexName, MapFunction,
        [], [ProviderId2]
    ),
    ?assertIndexQuery([FileId], WorkerP2, SpaceId, IndexName,  [{key, XattrValue}]),
    ?assertIndexVisible(WorkerP1, SpaceId, IndexName),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:replicate_files_from_index/2,
                query_view_params = [{key, XattrValue2}],
                index_name = IndexName
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 0,
                    bytes_replicated => 0
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assert_transferred_file_model = false
            }
        }
    ).

schedule_replication_of_100_regular_files_by_index(Config, Type) ->
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
    XattrName = transfers_test_utils:random_job_name(),
    XattrValue = 1,
    Xattr = #xattr{name = XattrName, value = XattrValue},

    FileIds = lists:map(fun({FileGuid, _}) ->
        ok = lfm_proxy:set_xattr(WorkerP2, SessionId2, {guid, FileGuid}, Xattr),
        {ok, FileId} = cdmi_id:guid_to_objectid(FileGuid),
        FileId
    end, FileGuidsAndPaths),

    IndexName = <<"replication_jobs">>,
    MapFunction = transfers_test_utils:test_map_function(XattrName),
    transfers_test_utils:create_index(WorkerP2, SpaceId, IndexName, MapFunction,
        [], [ProviderId2]
    ),
    ?assertIndexQuery(FileIds, WorkerP2, SpaceId, IndexName, [{key, XattrValue}]),
    ?assertIndexVisible(WorkerP1, SpaceId, IndexName),

    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                replicating_nodes = [WorkerP2],
                space_id = SpaceId,
                function = fun transfers_test_mechanism:replicate_files_from_index/2,
                query_view_params = [{key, XattrValue}],
                index_name = IndexName
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => NumberOfFiles + 1,
                    files_processed => NumberOfFiles + 1,
                    files_replicated => NumberOfFiles,
                    bytes_replicated => NumberOfFiles * ?DEFAULT_SIZE
                },
                assertion_nodes = [WorkerP1, WorkerP2],
                distribution = [
                    #{<<"providerId">> => ProviderId1, <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ProviderId2, <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
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

init_per_testcase(not_synced_file_should_not_be_replicated, Config) ->
    [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(WorkerP2, sync_req),
    ok = test_utils:mock_expect(WorkerP2, sync_req, replicate_file, fun(_, _, _, _, _, _) ->
        {error, not_found}
    end),
    init_per_testcase(all, Config);

init_per_testcase(schedule_replication_of_100_regular_files_by_index_with_batch_100, Config) ->
    Nodes = [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    {ok, OldValue} = test_utils:get_env(WorkerP2, op_worker, replication_by_index_batch),
    test_utils:set_env(Nodes, op_worker, replication_by_index_batch, 100),
    init_per_testcase(all, [{replication_by_index_batch, OldValue} | Config]);

init_per_testcase(schedule_replication_of_100_regular_files_by_index_with_batch_10, Config) ->
    Nodes = [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    {ok, OldValue} = test_utils:get_env(WorkerP2, op_worker, replication_by_index_batch),
    test_utils:set_env(Nodes, op_worker, replication_by_index_batch, 10),
    init_per_testcase(all, [{replication_by_index_batch, OldValue} | Config]);

init_per_testcase(_Case, Config) ->
    ct:timetrap(timer:minutes(60)),
    lfm_proxy:init(Config),
    [{space_id, ?SPACE_ID} | Config].

end_per_testcase(not_synced_file_should_not_be_replicated, Config) ->
    [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(WorkerP2, sync_req),
    end_per_testcase(all, Config);

end_per_testcase(Case, Config) when
    Case =:= replication_should_succeed_when_there_is_enough_space_for_file;
    Case =:= replication_should_fail_when_space_is_full
    ->
    [WorkerP2 | _] = ?config(op_worker_nodes, Config),
    transfers_test_utils:unmock_space_occupancy(WorkerP2, ?SPACE_ID),
    end_per_testcase(all, Config);

end_per_testcase(Case, Config) when
    Case =:= schedule_replication_of_100_regular_files_by_index_with_batch_100;
    Case =:= schedule_replication_of_100_regular_files_by_index_with_batch_10
->
    Nodes = ?config(op_worker_nodes, Config),
    OldValue = ?config(replication_by_index_batch, Config),
    test_utils:set_env(Nodes, op_worker, replication_by_index_batch, OldValue),
    end_per_testcase(all, Config);

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    transfers_test_utils:unmock_sync_req(Workers),
    transfers_test_utils:unmock_replica_synchronizer_failure(Workers),
    transfers_test_utils:remove_transfers(Config),
    rpc:multicall(Workers, transfer, restart_pools, []),
    transfers_test_utils:remove_all_indexes(Workers, ?SPACE_ID),
    transfers_test_utils:ensure_transfers_removed(Config).

end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).
