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
    restart_replication_on_target_nodes/2,
    file_replication_failures_should_fail_whole_transfer/3,
    many_simultaneous_failed_transfers/3]).

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
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
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
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
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
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_each_file_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
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

replicate_file_in_directory(Config, Type, FileKeyType) ->
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
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 2,
                    files_processed => 2,
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

replicate_big_file(Config, Type, FileKeyType) ->
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
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_each_file_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 1,
                    bytes_replicated => Size,
                    hr_hist => ?HOUR_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => Size}),
                    dy_hist => ?DAY_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => Size}),
                    mth_hist => ?MONTH_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => Size})
                },
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, Size]]},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP2), <<"blocks">> => [[0, Size]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

schedule_replication_to_source_provider(Config, Type, FileKeyType) ->
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
                target_nodes = [WorkerP1],
                function = fun transfers_test_mechanism:replicate_each_file_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    min_hist => ?MIN_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0}),
                    hr_hist => ?HOUR_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0}),
                    dy_hist => ?DAY_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0}),
                    mth_hist => ?MONTH_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0})
                },
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

replicate_already_replicated_file(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Config2 = transfers_test_mechanism:run_test(
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
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_each_file_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
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
    ),
    Config3 = transfers_test_mechanism:move_transfer_ids_to_old_key(Config2),
    transfers_test_mechanism:run_test(
        Config3, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                schedule_node = WorkerP1,
                target_nodes = [WorkerP1],
                function = fun transfers_test_mechanism:replicate_each_file_separately/2,
                type = Type,
                file_key_type = FileKeyType
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    min_hist => ?MIN_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0}),
                    hr_hist => ?HOUR_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0}),
                    dy_hist => ?DAY_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0}),
                    mth_hist => ?MONTH_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0})
                },
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP2), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

not_synced_file_should_not_be_replicated(Config, Type, FileKeyType) ->
    % list on Dir1 is mocked to return not existing file
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

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
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_each_file_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    min_hist => ?MIN_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0}),
                    hr_hist => ?HOUR_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0}),
                    dy_hist => ?DAY_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0}),
                    mth_hist => ?MONTH_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0})
                },
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

replicate_100_files_separately(Config, Type, FileKeyType) ->
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
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_each_file_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
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
    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{10, 0}, {0, 10}],
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
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 111,
                    files_processed => 111,
                    files_replicated => FilesNum,
                    bytes_replicated => TotalTransferredBytes,
                    hr_hist => ?HOUR_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => TotalTransferredBytes}),
                    dy_hist => ?DAY_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => TotalTransferredBytes}),
                    mth_hist => ?MONTH_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => TotalTransferredBytes})
                },
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP2), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP2],
                attempts = 600,
                timeout = timer:minutes(10)
            }
        }
    ).

replication_should_succeed_when_there_is_enough_space_for_file(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    Support = transfers_test_utils:get_space_support(WorkerP2, ?SPACE_ID),
    transfers_test_utils:mock_space_occupancy(WorkerP2, ?SPACE_ID, Support - ?DEFAULT_SIZE),

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
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_each_file_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1,
                    failed_files => 0,
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

replication_should_fail_when_space_is_full(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Support = transfers_test_utils:get_space_support(WorkerP2, ?SPACE_ID),
    transfers_test_utils:mock_space_occupancy(WorkerP2, ?SPACE_ID, Support),

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
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_each_file_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => failed,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1,
                    failed_files => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    min_hist => ?MIN_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0}),
                    hr_hist => ?HOUR_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0}),
                    dy_hist => ?DAY_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0}),
                    mth_hist => ?MONTH_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0})
                },
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP2), <<"blocks">> => []}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

replicate_to_missing_provider(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP1],
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
                target_nodes = [?MISSING_PROVIDER_NODE],
                function = fun transfers_test_mechanism:error_on_replicating_files/2
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

replicate_to_not_supporting_provider(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Config2 = [{space_id, <<"space2">>} | Config],
    % space2 is supported only by P1
    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
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
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:error_on_replicating_files/2
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

schedule_replication_on_not_supporting_provider(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Config2 = [{space_id, <<"space2">>} | Config],
    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP2,
                assertion_nodes = [WorkerP1],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP2,
                target_nodes = [WorkerP1],
                function = fun transfers_test_mechanism:error_on_replicating_files/2
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

cancel_replication_on_target_nodes(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    transfers_test_utils:mock_prolonged_replication(WorkerP2, 0.5, 15),
    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{10, 0}, {0, 10}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type),
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                attempts = 120,
                timeout = timer:minutes(10)
            },
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:cancel_replication_on_target_nodes/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => cancelled,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
                    failed_files => 0
                },
                distribution = undefined,
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

restart_replication_on_target_nodes(Config, Type) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    transfers_test_utils:mock_replica_synchronizer_failure(WorkerP2),

    Config2 = transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{0, 1}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type),
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ]
            },
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => failed,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 2,
                    files_processed => 2,
                    failed_files => 1,
                    files_replicated => 0,
                    bytes_replicated => 0
                },
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP2), <<"blocks">> => []}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ),
    transfers_test_utils:unmock_replica_synchronizer_failure(WorkerP2),
    transfers_test_mechanism:run_test(
        Config2, #transfer_test_spec{
            setup = undefined,
            scenario = #scenario{
                type = Type,
                schedule_node = WorkerP1,
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:restart_replication_on_target_nodes/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => completed,
                    files_to_process => 2,
                    files_processed => 2,
                    failed_files => 0,
                    files_replicated => 1,
                    bytes_replicated => ?DEFAULT_SIZE
                },
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]},
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP2), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                assertion_nodes = [WorkerP1, WorkerP2]
            }
        }
    ).

file_replication_failures_should_fail_whole_transfer(Config, Type, FileKeyType) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    AllFiles = 111,
    transfers_test_utils:mock_replica_synchronizer_failure(WorkerP2),
    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{10, 0}, {0, 10}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                attempts = 60,
                timeout = timer:minutes(1)
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_root_directory/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => failed,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => AllFiles,
                    files_processed => AllFiles,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    hr_hist => ?HOUR_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0}),
                    dy_hist => ?DAY_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0}),
                    mth_hist => ?MONTH_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0})
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
    transfers_test_mechanism:run_test(
        Config, #transfer_test_spec{
            setup = #setup{
                setup_node = WorkerP1,
                assertion_nodes = [WorkerP2],
                files_structure = [{10, 0}, {0, 10}],
                root_directory = transfers_test_utils:root_name(?FUNCTION_NAME, Type, FileKeyType),
                distribution = [
                    #{<<"providerId">> => ?GET_DOMAIN_BIN(WorkerP1), <<"blocks">> => [[0, ?DEFAULT_SIZE]]}
                ],
                attempts = 60,
                timeout = timer:minutes(1)
            },
            scenario = #scenario{
                type = Type,
                file_key_type = FileKeyType,
                schedule_node = WorkerP1,
                target_nodes = [WorkerP2],
                function = fun transfers_test_mechanism:replicate_each_file_separately/2
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => failed,
                    scheduling_provider_id => transfers_test_utils:provider_id(WorkerP1),
                    files_to_process => 1,
                    files_processed => 1,
                    failed_files => 1,
                    files_replicated => 0,
                    bytes_replicated => 0,
                    hr_hist => ?HOUR_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0}),
                    dy_hist => ?DAY_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0}),
                    mth_hist => ?MONTH_HIST(#{?GET_DOMAIN_BIN(WorkerP1) => 0})
                },
                assertion_nodes = [WorkerP2],
                attempts = 600
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
    ok = test_utils:mock_expect(WorkerP2, sync_req, replicate_file, fun(_, _, _, _) ->
        {error, not_found}
    end),
    init_per_testcase(all, Config);

init_per_testcase(_Case, Config) ->
    ct:timetrap(timer:minutes(10)),
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

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    transfers_test_utils:unmock_sync_req(Workers),
    transfers_test_utils:unmock_replica_synchronizer_failure(Workers),
    transfers_test_utils:remove_transfers(Config),
    rpc:multicall(Workers, transfer, restart_pools, []),
    transfers_test_utils:ensure_transfers_removed(Config).

end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).
