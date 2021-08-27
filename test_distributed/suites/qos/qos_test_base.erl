%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains QoS specification for tests (includes specification
%%% of requirements, expected qos_entry records, expected file_qos /
%%% effective_file_qos records).
%%% @end
%%%--------------------------------------------------------------------
-module(qos_test_base).
-author("Michal Cwiertnia").


-include("qos_tests_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

% QoS specification for tests
-export([
    % Single QoS expression specs
    simple_key_val_qos_spec/4,
    qos_with_intersection_spec/4,
    qos_with_complement_spec/4,
    qos_with_union_spec/4,
    qos_with_multiple_replicas_spec/4,
    qos_with_multiple_replicas_and_union_spec/4,
    qos_with_intersection_and_union_spec/4,
    qos_with_union_and_complement_spec/4,
    qos_with_intersection_and_complement_spec/4,
    key_val_qos_that_cannot_be_fulfilled_spec/4,
    qos_that_cannot_be_fulfilled_spec/4,
    qos_with_parens_spec/4,

    % Multi QoS for single file specs
    multi_qos_resulting_in_the_same_storages_spec/4,
    same_qos_multiple_times_spec/4,
    contrary_qos_spec/4,
    multi_qos_where_one_cannot_be_satisfied_spec/4,
    multi_qos_that_overlaps_spec/4,
    multi_qos_resulting_in_different_storages_spec/4,

    % Effective QoS specs
    effective_qos_for_file_in_directory_spec/5,
    effective_qos_for_file_in_nested_directories_spec/5,
    effective_qos_for_files_in_different_directories_of_tree_structure_spec/5,
    
    % QoS status test bases
    qos_status_during_traverse_test_base/3,
    qos_status_during_traverse_with_hardlinks_test_base/2,
    qos_status_during_traverse_with_file_deletion_test_base/4,
    qos_status_during_traverse_with_dir_deletion_test_base/4,
    qos_status_during_traverse_file_without_qos_test_base/2,
    qos_status_during_reconciliation_test_base/4,
    qos_status_during_reconciliation_with_file_deletion_test_base/4,
    qos_status_during_reconciliation_with_dir_deletion_test_base/4,
    qos_status_after_failed_transfer/3,
    qos_status_after_failed_transfer_deleted_file/3,
    qos_status_after_failed_transfer_deleted_entry/3,
    
    % QoS with hardlinks test bases
    qos_with_hardlink_test_base/3,
    qos_with_hardlink_deletion_test_base/3,
    qos_on_symlink_test_base/2,
    effective_qos_with_symlink_test_base/2,
    create_hardlink_in_dir_with_qos/2
]).

-export([
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/1, end_per_testcase/1
]).

% file_type can be one of 
%   * reg_file - created file is a regular file
%   * hardlink - created file is a hardlink
%   * random - created file is randomly chosen between regular file and hardlink
-type file_type() :: reg_file | hardlink | random.

-define(ATTEMPTS, 120).

%%%===================================================================
%%% Group of tests that adds single QoS expression for file or directory
%%% and checks QoS docs.
%%% Each test case is executed once for file and once for directory.
%%%===================================================================

simple_key_val_qos_spec(Path, WorkerAddingQos, AssertionWorkers, ProviderMap) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"country=FR">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"country=FR">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{maps:get(?P2, ProviderMap) => [?QOS1]}
            }
        ]
    }.


qos_with_intersection_spec(Path, WorkerAddingQos, AssertionWorkers, ProviderMap) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=disk & tier=t2">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=disk">>, <<"tier=t2">>, <<"&">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{maps:get(?P3, ProviderMap) => [?QOS1]}
            }
        ]
    }.


qos_with_complement_spec(Path, WorkerAddingQos, AssertionWorkers, ProviderMap) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=disk\\country=PT">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=disk">>, <<"country=PT">>, <<"-">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{maps:get(?P1, ProviderMap) => [?QOS1]}
            }
        ]
    }.


qos_with_union_spec(Path, WorkerAddingQos, AssertionWorkers, ProviderMap) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"country=PL|tier=t3">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"country=PL">>, <<"tier=t3">>, <<"|">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{maps:get(?P1, ProviderMap) => [?QOS1]}
            }
        ]
    }.


qos_with_multiple_replicas_spec(Path, WorkerAddingQos, AssertionWorkers, ProviderMap) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=disk">>,
                replicas_num = 2
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=disk">>],
                replicas_num = 2,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{
                    maps:get(?P1, ProviderMap) => [?QOS1],
                    maps:get(?P3, ProviderMap) => [?QOS1]
                }
            }
        ]
    }.


qos_with_intersection_and_union_spec(Path, WorkerAddingQos, AssertionWorkers, ProviderMap) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=disk&tier=t2|country=FR">>,
                replicas_num = 2
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=disk">>, <<"tier=t2">>, <<"&">>, <<"country=FR">>, <<"|">>],
                replicas_num = 2,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{
                    maps:get(?P2, ProviderMap) => [?QOS1],
                    maps:get(?P3, ProviderMap) => [?QOS1]
                }
            }
        ]
    }.


qos_with_union_and_complement_spec(Path, WorkerAddingQos, AssertionWorkers, ProviderMap) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"country=PL|country=FR\\type=tape">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"country=PL">>, <<"country=FR">>, <<"|">>, <<"type=tape">>, <<"-">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{maps:get(?P1, ProviderMap) => [?QOS1]}
            }
        ]
    }.


qos_with_intersection_and_complement_spec(Path, WorkerAddingQos, AssertionWorkers, ProviderMap) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=disk & param1=val1 \\ country=PL">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=disk">>, <<"param1=val1">>, <<"&">>, <<"country=PL">>, <<"-">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{maps:get(?P3, ProviderMap) => [?QOS1]}
            }
        ]
    }.


qos_with_multiple_replicas_and_union_spec(Path, WorkerAddingQos, AssertionWorkers, ProviderMap) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"country=PL | country=FR | country=PT">>,
                replicas_num = 3
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"country=PL">>, <<"country=FR">>, <<"|">>, <<"country=PT">>, <<"|">>],
                replicas_num = 3,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{
                    maps:get(?P1, ProviderMap) => [?QOS1],
                    maps:get(?P2, ProviderMap) => [?QOS1],
                    maps:get(?P3, ProviderMap) => [?QOS1]
                }
            }
        ]
    }.


key_val_qos_that_cannot_be_fulfilled_spec(Path, WorkerAddingQos, AssertionWorkers, _ProviderMap) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"country=IT">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"country=IT">>],
                replicas_num = 1,
                possibility_check = {impossible, ?GET_DOMAIN_BIN(WorkerAddingQos)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{}
            }
        ]
    }.


qos_that_cannot_be_fulfilled_spec(Path, WorkerAddingQos, AssertionWorkers, _ProviderMap) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"country=PL|country=PT\\type=disk">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"country=PL">>, <<"country=PT">>, <<"|">>, <<"type=disk">>, <<"-">>],
                replicas_num = 1,
                possibility_check = {impossible, ?GET_DOMAIN_BIN(WorkerAddingQos)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{}
            }
        ]
    }.


qos_with_parens_spec(Path, WorkerAddingQos, AssertionWorkers, ProviderMap) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"country=PL | (country=PT \\ type=disk)">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"country=PL">>, <<"country=PT">>, <<"type=disk">>, <<"-">>, <<"|">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos)}

            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1],
                assigned_entries = #{maps:get(?P1, ProviderMap) => [?QOS1]}
            }
        ]
    }.


%%%===================================================================
%%% Group of tests that adds multiple QoS expression for single file or
%%% directory and checks QoS docs.
%%% Each test case is executed once for file and once for directory.
%%%===================================================================

multi_qos_resulting_in_different_storages_spec(
    Path, WorkerAddingQos, AssertionWorkers, ProviderMap
) when not is_list(WorkerAddingQos) ->
    multi_qos_resulting_in_different_storages_spec(
        Path, [WorkerAddingQos, WorkerAddingQos], AssertionWorkers, ProviderMap
    );

multi_qos_resulting_in_different_storages_spec(
    Path, [WorkerAddingQos1, WorkerAddingQos2], AssertionWorkers, ProviderMap
) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos1,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=disk&tier=t2">>,
                replicas_num = 1
            },
            #qos_to_add{
                worker = WorkerAddingQos2,
                path = Path,
                qos_name = ?QOS2,
                expression = <<"country=FR">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=disk">>, <<"tier=t2">>, <<"&">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos1)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression = [<<"country=FR">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos2)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1, ?QOS2],
                assigned_entries = #{
                    maps:get(?P2, ProviderMap) => [?QOS2],
                    maps:get(?P3, ProviderMap) => [?QOS1]
                }
            }
        ]
    }.


multi_qos_resulting_in_the_same_storages_spec(
    Path, WorkerAddingQos, AssertionWorkers, ProviderMap
) when not is_list(WorkerAddingQos) ->
    multi_qos_resulting_in_the_same_storages_spec(
        Path, [WorkerAddingQos, WorkerAddingQos], AssertionWorkers, ProviderMap
    );

multi_qos_resulting_in_the_same_storages_spec(
    Path, [WorkerAddingQos1, WorkerAddingQos2], AssertionWorkers, ProviderMap
) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos1,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=tape">>,
                replicas_num = 1
            },
            #qos_to_add{
                worker = WorkerAddingQos2,
                path = Path,
                qos_name = ?QOS2,
                expression = <<"country=FR">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=tape">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos1)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression = [<<"country=FR">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos2)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1, ?QOS2],
                assigned_entries = #{
                    maps:get(?P2, ProviderMap) => [?QOS1, ?QOS2]
                }
            }
        ]
    }.


same_qos_multiple_times_spec(
    Path, WorkerAddingQos, AssertionWorkers, ProviderMap
) when not is_list(WorkerAddingQos) ->
    same_qos_multiple_times_spec(
        Path, [WorkerAddingQos, WorkerAddingQos, WorkerAddingQos], AssertionWorkers, ProviderMap
    );

same_qos_multiple_times_spec(
    Path, [WorkerAddingQos1, WorkerAddingQos2, WorkerAddingQos3], AssertionWorkers, ProviderMap
) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos1,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=tape">>,
                replicas_num = 1
            },
            #qos_to_add{
                worker = WorkerAddingQos2,
                path = Path,
                qos_name = ?QOS2,
                expression = <<"type=tape">>,
                replicas_num = 1
            },
            #qos_to_add{
                worker = WorkerAddingQos3,
                path = Path,
                qos_name = ?QOS3,
                expression = <<"type=tape">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=tape">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos1)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression = [<<"type=tape">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos2)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS3,
                file_key = {path, Path},
                qos_expression = [<<"type=tape">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos3)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1, ?QOS2, ?QOS3],
                assigned_entries = #{maps:get(?P2, ProviderMap) => [?QOS1, ?QOS2, ?QOS3]}
            }
        ]
    }.


contrary_qos_spec(Path, WorkerAddingQos, AssertionWorkers, ProviderMap) when not is_list(WorkerAddingQos) ->
    contrary_qos_spec(Path, [WorkerAddingQos, WorkerAddingQos], AssertionWorkers, ProviderMap);

contrary_qos_spec(Path, [WorkerAddingQos1, WorkerAddingQos2], AssertionWorkers, ProviderMap) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos1,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"country=PL">>,
                replicas_num = 1
            },
            #qos_to_add{
                worker = WorkerAddingQos2,
                path = Path,
                qos_name = ?QOS2,
                expression = <<"type=tape \\ country=PL">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"country=PL">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos1)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression = [<<"type=tape">>, <<"country=PL">>, <<"-">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos2)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1, ?QOS2],
                assigned_entries = #{
                    maps:get(?P1, ProviderMap) => [?QOS1],
                    maps:get(?P2, ProviderMap) => [?QOS2]
                }
            }
        ]
    }.


multi_qos_where_one_cannot_be_satisfied_spec(
    Path, WorkerAddingQos, AssertionWorkers, ProviderMap
) when not is_list(WorkerAddingQos) ->
    multi_qos_where_one_cannot_be_satisfied_spec(
        Path, [WorkerAddingQos, WorkerAddingQos], AssertionWorkers, ProviderMap
    );

multi_qos_where_one_cannot_be_satisfied_spec(
    Path, [WorkerAddingQos1, WorkerAddingQos2], AssertionWorkers, ProviderMap
) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos1,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"country=FR">>,
                replicas_num = 1
            },
            #qos_to_add{
                worker = WorkerAddingQos2,
                path = Path,
                qos_name = ?QOS2,
                expression = <<"country=IT">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"country=FR">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos1)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression = [<<"country=IT">>],
                replicas_num = 1,
                possibility_check = {impossible, ?GET_DOMAIN_BIN(WorkerAddingQos2)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1, ?QOS2],
                assigned_entries = #{maps:get(?P2, ProviderMap) => [?QOS1]}
            }
        ]
    }.


multi_qos_that_overlaps_spec(
    Path, WorkerAddingQos, AssertionWorkers, ProviderMap
) when not is_list(WorkerAddingQos) ->
    multi_qos_that_overlaps_spec(
        Path, [WorkerAddingQos, WorkerAddingQos], AssertionWorkers, ProviderMap
    );

multi_qos_that_overlaps_spec(
    Path, [WorkerAddingQos1, WorkerAddingQos2], AssertionWorkers, ProviderMap
) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos1,
                path = Path,
                qos_name = ?QOS1,
                expression = <<"type=disk">>,
                replicas_num = 2
            },
            #qos_to_add{
                worker = WorkerAddingQos2,
                path = Path,
                qos_name = ?QOS2,
                expression = <<"tier=t2">>,
                replicas_num = 2
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression = [<<"type=disk">>],
                replicas_num = 2,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos1)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression = [<<"tier=t2">>],
                replicas_num = 2,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos2)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = Path,
                qos_entries = [?QOS1, ?QOS2],
                assigned_entries = #{
                    maps:get(?P1, ProviderMap) => [?QOS1],
                    maps:get(?P2, ProviderMap) => [?QOS2],
                    maps:get(?P3, ProviderMap) => [?QOS1, ?QOS2]
                }
            }
        ]
    }.


%%%===================================================================
%%% Group of tests that creates directory structure, adds QoS on different
%%% levels of created structure and checks effective QoS and QoS docs.
%%%===================================================================

effective_qos_for_file_in_directory_spec(DirPath, FilePath, WorkerAddingQos, AssertionWorkers, ProviderMap) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos,
                path = DirPath,
                qos_name = ?QOS1,
                expression = <<"country=FR">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                qos_expression = [<<"country=FR">>],
                replicas_num = 1,
                file_key = {path, DirPath},
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos)}
            }
        ],
        expected_effective_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = FilePath,
                qos_entries = [?QOS1],
                assigned_entries = #{maps:get(?P2, ProviderMap) => [?QOS1]}
            }
        ]
    }.


effective_qos_for_file_in_nested_directories_spec(
    [Dir1Path, Dir2Path, Dir3Path], [FileInDir2Path, FileInDir3Path], WorkerAddingQos, AssertionWorkers, ProviderMap
) when not is_list(WorkerAddingQos) ->
    effective_qos_for_file_in_nested_directories_spec(
        [Dir1Path, Dir2Path, Dir3Path], [FileInDir2Path, FileInDir3Path],
        [WorkerAddingQos, WorkerAddingQos, WorkerAddingQos], AssertionWorkers, ProviderMap
    );

effective_qos_for_file_in_nested_directories_spec(
    [Dir1Path, Dir2Path, Dir3Path], [FileInDir2Path, FileInDir3Path],
    [WorkerAddingQos1, WorkerAddingQos2, WorkerAddingQos3], AssertionWorkers, ProviderMap
) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos1,
                path = Dir1Path,
                qos_name = ?QOS1,
                expression = <<"country=PL">>,
                replicas_num = 1
            },
            #qos_to_add{
                worker = WorkerAddingQos2,
                path = Dir2Path,
                qos_name = ?QOS2,
                expression = <<"country=FR">>,
                replicas_num = 1
            },
            #qos_to_add{
                worker = WorkerAddingQos3,
                path = Dir3Path,
                qos_name = ?QOS3,
                expression = <<"country=PT">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                qos_expression = [<<"country=PL">>],
                replicas_num = 1,
                file_key = {path, Dir1Path},
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos1)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS2,
                qos_expression = [<<"country=FR">>],
                replicas_num = 1,
                file_key = {path, Dir2Path},
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos2)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS3,
                qos_expression = [<<"country=PT">>],
                replicas_num = 1,
                file_key = {path, Dir3Path},
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos3)}
            }
        ],
        expected_effective_qos = [
            #expected_file_qos{
                path = FileInDir2Path,
                qos_entries = [?QOS1, ?QOS2],
                assigned_entries = #{
                    maps:get(?P1, ProviderMap) => [?QOS1],
                    maps:get(?P2, ProviderMap) => [?QOS2]
                }
            },
            #expected_file_qos{
                path = FileInDir3Path,
                qos_entries = [?QOS1, ?QOS2, ?QOS3],
                assigned_entries = #{
                    maps:get(?P1, ProviderMap) => [?QOS1],
                    maps:get(?P2, ProviderMap) => [?QOS2],
                    maps:get(?P3, ProviderMap) => [?QOS3]
                }
            }
        ]
    }.


effective_qos_for_files_in_different_directories_of_tree_structure_spec(
    [Dir1Path, Dir2Path, Dir3Path], [FileInDir2Path, FileInDir3Path], WorkerAddingQos, AssertionWorkers, ProviderMap
) when not is_list(WorkerAddingQos) ->
    effective_qos_for_files_in_different_directories_of_tree_structure_spec(
        [Dir1Path, Dir2Path, Dir3Path], [FileInDir2Path, FileInDir3Path],
        [WorkerAddingQos, WorkerAddingQos, WorkerAddingQos], AssertionWorkers, ProviderMap
    );

effective_qos_for_files_in_different_directories_of_tree_structure_spec(
    [Dir1Path, Dir2Path, Dir3Path], [FileInDir2Path, FileInDir3Path],
    [WorkerAddingQos1, WorkerAddingQos2, WorkerAddingQos3], AssertionWorkers, ProviderMap
) ->
    #qos_spec{
        qos_to_add = [
            #qos_to_add{
                worker = WorkerAddingQos1,
                path = Dir1Path,
                qos_name = ?QOS1,
                expression = <<"country=PL">>,
                replicas_num = 1
            },
            #qos_to_add{
                worker = WorkerAddingQos2,
                path = Dir2Path,
                qos_name = ?QOS2,
                expression = <<"country=FR">>,
                replicas_num = 1
            },
            #qos_to_add{
                worker = WorkerAddingQos3,
                path = Dir3Path,
                qos_name = ?QOS3,
                expression = <<"country=PT">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                qos_expression = [<<"country=PL">>],
                replicas_num = 1,
                file_key = {path, Dir1Path},
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos1)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS2,
                qos_expression = [<<"country=FR">>],
                replicas_num = 1,
                file_key = {path, Dir2Path},
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos2)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS3,
                qos_expression = [<<"country=PT">>],
                replicas_num = 1,
                file_key = {path, Dir3Path},
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos3)}
            }
        ],
        expected_effective_qos = [
            #expected_file_qos{
                workers = AssertionWorkers,
                path = FileInDir3Path,
                qos_entries = [?QOS1, ?QOS3],
                assigned_entries = #{
                    maps:get(?P1, ProviderMap) => [?QOS1],
                    maps:get(?P3, ProviderMap) => [?QOS3]
                }
            },
            #expected_file_qos{
                workers = AssertionWorkers,
                path = FileInDir2Path,
                qos_entries = [?QOS1, ?QOS2],
                assigned_entries = #{
                    maps:get(?P1, ProviderMap) => [?QOS1],
                    maps:get(?P2, ProviderMap) => [?QOS2]
                }
            }
        ]
    }.


%%%===================================================================
%%% QoS status tests bases
%%%===================================================================

qos_status_during_traverse_test_base(Config, SpaceId, NumberOfFilesInDir) ->
    [Worker1 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    Name = generator:gen_name(),
    DirStructure =
        {SpaceId, [
            {Name, [ % Dir1
                {?filename(Name, 1), % Dir2
                    lists:map(fun(Num) -> {?filename(Name, Num), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]} end, lists:seq(1, NumberOfFilesInDir)) % Guids2
                },
                {?filename(Name, 2), [ % Dir3
                    {?filename(Name, 1), % Dir4
                        lists:map(fun(Num) -> {?filename(Name, Num), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]} end, lists:seq(1, NumberOfFilesInDir)) % Guids3
                    }
                ]} ] ++ 
                lists:map(fun(Num) -> {?filename(Name, Num), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]} end, lists:seq(3, 2 + NumberOfFilesInDir)) % Guids1
            }
        ]},
    
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(Config, DirStructure, SpaceId, Name),

    Guids1 = lists:map(fun(Num) ->
        qos_tests_utils:get_guid(resolve_path(SpaceId, Name, [Num]), GuidsAndPaths)
    end, lists:seq(3, 2 + NumberOfFilesInDir)),
    
    Guids2 = lists:map(fun(Num) ->
        qos_tests_utils:get_guid(resolve_path(SpaceId, Name, [1, Num]), GuidsAndPaths)
    end, lists:seq(1, NumberOfFilesInDir)),
    
    Guids3 = lists:map(fun(Num) ->
        qos_tests_utils:get_guid(resolve_path(SpaceId, Name, [2, 1, Num]), GuidsAndPaths)
    end, lists:seq(1, NumberOfFilesInDir)),
    
    Dir1 = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, []), GuidsAndPaths),
    Dir2 = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, [1]), GuidsAndPaths),
    Dir3 = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, [2]), GuidsAndPaths),
    Dir4 = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, [2,1]), GuidsAndPaths),
    
    
    ok = qos_tests_utils:finish_all_transfers(Guids1),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, Guids1, QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, Guids2 ++ Guids3 ++ [Dir1, Dir2, Dir3, Dir4], QosList, ?PENDING_QOS_STATUS), ?ATTEMPTS),
    
    ok = qos_tests_utils:finish_all_transfers(Guids2),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, Guids1 ++ Guids2 ++ [Dir2], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, Guids3 ++ [Dir1, Dir3, Dir4], QosList, ?PENDING_QOS_STATUS), ?ATTEMPTS),
    
    ok = qos_tests_utils:finish_all_transfers(Guids3),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, Guids1 ++ Guids2 ++ Guids3 ++ [Dir1, Dir2, Dir3, Dir4], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS).


qos_status_during_traverse_with_hardlinks_test_base(Config, SpaceId) ->
    [Worker1 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    SpaceGuid = rpc:call(Worker1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    {ok, Dir1Guid} = lfm_proxy:mkdir(Worker1, SessId(Worker1), SpaceGuid, generator:gen_name(), ?DEFAULT_DIR_PERMS),
    {ok, Dir2Guid} = lfm_proxy:mkdir(Worker1, SessId(Worker1), SpaceGuid, generator:gen_name(), ?DEFAULT_DIR_PERMS),
    
    {ok, FileGuid1} = lfm_proxy:create(Worker1, SessId(Worker1), Dir1Guid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, FileGuid2} = lfm_proxy:create(Worker1, SessId(Worker1), Dir1Guid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_link(Worker1, SessId(Worker1), ?FILE_REF(FileGuid1), ?FILE_REF(Dir2Guid), generator:gen_name()),
    
    await_files_sync_between_workers(Workers, [FileGuid1, FileGuid2, LinkGuid], SessId),
    qos_tests_utils:mock_transfers(Workers),
    
    {ok, QosEntryId} = lfm_proxy:add_qos_entry(Worker1, SessId(Worker1), ?FILE_REF(Dir1Guid), <<"country=FR">>, 1),
    assert_effective_entry(Worker1, SessId(Worker1), QosEntryId, [FileGuid1, FileGuid2, LinkGuid], []),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid1, FileGuid2, LinkGuid], [QosEntryId], ?PENDING_QOS_STATUS), ?ATTEMPTS),
    qos_tests_utils:finish_all_transfers([FileGuid1]),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid1, LinkGuid], [QosEntryId], ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid2], [QosEntryId], ?PENDING_QOS_STATUS), ?ATTEMPTS),
    qos_tests_utils:finish_all_transfers([FileGuid2]),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid1, FileGuid2, LinkGuid], [QosEntryId], ?FULFILLED_QOS_STATUS), ?ATTEMPTS).


-spec qos_status_during_traverse_with_file_deletion_test_base(test_config:config(), od_space:id(), pos_integer(), file_type()) -> ok.
qos_status_during_traverse_with_file_deletion_test_base(Config, SpaceId, NumberOfFilesInDir, FileType) ->
    [Worker1 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    Name = generator:gen_name(),
    SpaceGuid = rpc:call(Worker1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    {ok, FileToLinkGuid} = lfm_proxy:create(Worker1, SessId(Worker1), SpaceGuid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    DirStructure =
        {SpaceId, [
            {Name, % Dir1
                lists:map(fun(Num) -> 
                    TypeSpec = prepare_type_spec(FileType, Workers, SessId, {target, FileToLinkGuid}),
                    {?filename(Name, Num), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)], TypeSpec} 
                end, lists:seq(1, NumberOfFilesInDir))
            }
        ]},
    
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(Config, DirStructure, SpaceId, Name),
    
    {ToFinish, ToDelete} = lists:foldl(fun(Num, {F, D}) ->
        Guid = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, [Num]), GuidsAndPaths),
        case Num rem 2 of
            0 -> {[Guid | F], D};
            1 -> {F, [Guid | D]}
        end
    end, {[], []}, lists:seq(1, NumberOfFilesInDir)),
    
    lists:foreach(fun(Guid) ->
        ok = lfm_proxy:unlink(Worker1, SessId(Worker1), ?FILE_REF(Guid))
    end, ToDelete),
    
    ok = qos_tests_utils:finish_all_transfers(ToFinish),
    
    Dir1 = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, []), GuidsAndPaths),
    ok = ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, ToFinish ++ [Dir1], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    % finish transfers to unlock waiting slave job processes
    ok = qos_tests_utils:finish_all_transfers(ToDelete),
    % These files where deleted so QoS is not fulfilled
    ok = ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, ToDelete, QosList, {error, enoent}), ?ATTEMPTS).


-spec qos_status_during_traverse_with_dir_deletion_test_base(test_config:config(), od_space:id(), pos_integer(), file_type()) -> ok.
qos_status_during_traverse_with_dir_deletion_test_base(Config, SpaceId, NumberOfFilesInDir, FileType) ->
    [Worker1 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    Name = generator:gen_name(),
    SpaceGuid = rpc:call(Worker1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    {ok, FileToLinkGuid} = lfm_proxy:create(Worker1, SessId(Worker1), SpaceGuid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    DirStructure =
        {SpaceId, [
            {Name, [ % Dir1
                {?filename(Name, 1), % Dir2
                    lists:map(fun(Num) ->
                        TypeSpec = prepare_type_spec(FileType, Workers, SessId, {target, FileToLinkGuid}),
                        {?filename(Name, Num), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)], TypeSpec}
                    end, lists:seq(1, NumberOfFilesInDir))
                }
            ]}
        ]},

    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(Config, DirStructure, SpaceId, Name),
    
    Dir1 = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, []), GuidsAndPaths),
    Dir2 = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, [1]), GuidsAndPaths),
    
    ok = lfm_proxy:rm_recursive(Worker1, SessId(Worker1), ?FILE_REF(Dir2)),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [Dir1], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    
    % finish transfers to unlock waiting slave job processes
    ok = qos_tests_utils:finish_all_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)]).


qos_status_during_traverse_file_without_qos_test_base(Config, SpaceId) ->
    [Worker1 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    
    Name = generator:gen_name(),
    DirStructure =
        {SpaceId, [
            {Name, [ % Dir1
                {?filename(Name, 1), 
                    [{?filename(Name, 0), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]}]
                }]
            }
        ]},
    
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(Config, DirStructure, SpaceId, filename:join(Name, ?filename(Name,1))),
    Dir1 = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, []), GuidsAndPaths),
    
    % create file outside QoS range
    {ok, {FileGuid, FileHandle}} = lfm_proxy:create_and_open(Worker1, SessId(Worker1), Dir1, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, _} = lfm_proxy:write(Worker1, FileHandle, 0, <<"new_data">>),
    ok = lfm_proxy:close(Worker1, FileHandle),
    lists:foreach(fun(W) ->
        ?assertMatch({ok, _}, lfm_proxy:stat(W, SessId(W), ?FILE_REF(FileGuid)), ?ATTEMPTS)
    end, Workers),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [Dir1, FileGuid], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    
    % finish transfer to unlock waiting slave job process
    ok = qos_tests_utils:finish_all_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)]).
    

qos_status_during_reconciliation_test_base(Config, SpaceId, DirStructure, Filename) ->
    [Worker1 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(Config, DirStructure, SpaceId, Filename),
    
    ok = qos_tests_utils:finish_all_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)]),
    
    FilesAndDirs = maps:get(files, GuidsAndPaths) ++ maps:get(dirs, GuidsAndPaths),
    FilesAndDirsGuids = lists:map(fun({G, _}) -> G end, FilesAndDirs),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, FilesAndDirsGuids, QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    
    IsAncestor = fun
        (F, F) -> true;
        (A, F) -> str_utils:binary_starts_with(F, <<A/binary, "/">>)
    end,
    
    lists:foreach(fun({FileGuid, FilePath}) ->
        ct:pal("writing to file ~p on worker ~p", [FilePath, Worker1]),
        {ok, FileHandle} = lfm_proxy:open(Worker1, SessId(Worker1), ?FILE_REF(FileGuid), write),
        {ok, _} = lfm_proxy:write(Worker1, FileHandle, 0, <<"new_data">>),
        ok = lfm_proxy:close(Worker1, FileHandle),
        lists:foreach(fun({G, P}) ->
            ct:pal("Checking file: ~p~n\tis_ancestor: ~p", [P, IsAncestor(P, FilePath)]),
            ExpectedStatus = case IsAncestor(P, FilePath) of
                true -> ?PENDING_QOS_STATUS;
                false -> ?FULFILLED_QOS_STATUS
            end,
            ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [G], QosList, ExpectedStatus), ?ATTEMPTS)
        end, FilesAndDirs),
        ok = qos_tests_utils:finish_all_transfers([FileGuid]),
        ct:pal("Checking after finish"),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, FilesAndDirsGuids, QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS)
    end, maps:get(files, GuidsAndPaths)).


-spec qos_status_during_reconciliation_with_file_deletion_test_base(test_config:config(), od_space:id(), pos_integer(), file_type()) -> ok.
qos_status_during_reconciliation_with_file_deletion_test_base(Config, SpaceId, NumOfFiles, FileType) ->
    [Worker1 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    
    Name = generator:gen_name(),
    DirStructure =
        {SpaceId, [
            {Name, % Dir1
                [{?filename(Name, 0), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]}]
            }
        ]},
    
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(Config, DirStructure, SpaceId, Name),
    
    TypeSpec = prepare_type_spec(FileType, Workers, SessId, {target, create_link_target(Worker1, SessId(Worker1), SpaceId)}),
    ok = qos_tests_utils:finish_all_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)]),
    
    FilesAndDirs = maps:get(files, GuidsAndPaths) ++ maps:get(dirs, GuidsAndPaths),
    FilesAndDirsGuids = lists:map(fun({G, _}) -> G end, FilesAndDirs),
    [{Dir1, _}] = maps:get(dirs, GuidsAndPaths),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, FilesAndDirsGuids, QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    
    lists:foreach(fun(Worker) ->
        Guids = create_files_and_write(Worker1, SessId(Worker1), Dir1, TypeSpec, NumOfFiles),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [Dir1 | Guids], QosList, ?PENDING_QOS_STATUS), ?ATTEMPTS),
        lists:foreach(fun(FileGuid) ->
            ok = lfm_proxy:unlink(Worker, SessId(Worker), ?FILE_REF(FileGuid))
        end, Guids),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, FilesAndDirsGuids, QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
        % finish transfer to unlock waiting slave job process
        ok = qos_tests_utils:finish_all_transfers(Guids, non_strict) % all hardlinks are to the same file so only one transfer started
    end, Workers).


-spec qos_status_during_reconciliation_with_dir_deletion_test_base(test_config:config(), od_space:id(), pos_integer(), file_type()) -> ok.
qos_status_during_reconciliation_with_dir_deletion_test_base(Config, SpaceId, NumOfFiles, FileType) ->
    [Worker1 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    Name = generator:gen_name(),
    DirStructure =
        {SpaceId, [
            {Name, % Dir1
                [{?filename(Name, 0), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]}]
            }
        ]},
    
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(Config, DirStructure, SpaceId, Name),
    Dir1 = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, []), GuidsAndPaths),
    ok = qos_tests_utils:finish_all_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)]),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [Dir1], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    TypeSpec = prepare_type_spec(FileType, Workers, SessId, {target, create_link_target(Worker1, SessId(Worker1), SpaceId)}),
    
    lists:foreach(fun(Worker) ->
        ct:print("Deleting worker: ~p", [Worker]), % log current deleting worker for greater verbosity during failures
        {ok, DirGuid} = lfm_proxy:mkdir(Worker1, SessId(Worker1), Dir1, generator:gen_name(), ?DEFAULT_DIR_PERMS),
        Guids = create_files_and_write(Worker1, SessId(Worker1), DirGuid, TypeSpec, NumOfFiles),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [Dir1], QosList, ?PENDING_QOS_STATUS), ?ATTEMPTS),
        ok = lfm_proxy:rm_recursive(Worker, SessId(Worker), ?FILE_REF(DirGuid)),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [Dir1], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
        % finish transfer to unlock waiting slave job process
        ok = qos_tests_utils:finish_all_transfers(Guids, non_strict) % all hardlinks are to the same file so only one transfer started
    end, Workers).


qos_status_after_failed_transfer(Config, SpaceId, TargetWorker) ->
    [Worker1 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    qos_tests_utils:mock_replica_synchronizer(Workers, {error, some_error}),
    
    Name = generator:gen_name(),
    DirStructure = fun(Distribution) ->
        {SpaceId, [
            {Name, ?TEST_DATA, Distribution}
        ]}
    end,
    % create new QoS entry and check, that it is not fulfilled
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(Config, DirStructure([?GET_DOMAIN_BIN(Worker1)]), SpaceId, Name),
    % check that file is on qos failed files list
    FileGuid = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, []), GuidsAndPaths),
    ?assertEqual(true, is_file_in_failed_files_list(TargetWorker, FileGuid), ?ATTEMPTS),
    ?assert(is_failed_files_list_empty(get_workers_list_without_provider(Workers, ?GET_DOMAIN_BIN(TargetWorker)), 
        file_id:guid_to_space_id(FileGuid))),
    % check file distribution (file blocks should be only on source provider)
    ?assert(qos_tests_utils:assert_distribution_in_dir_structure(Config, DirStructure([?GET_DOMAIN_BIN(Worker1)]), GuidsAndPaths)),
    % initialize periodic check of failed files
    utils:rpc_multicall(Workers, qos_worker, init_retry_failed_files, []),
    
    % check that after a successful transfer QoS entry is eventually fulfilled
    qos_tests_utils:mock_replica_synchronizer(Workers, passthrough),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    % check file distribution again (file blocks should be on both source and target provider)
    ?assertEqual(true, qos_tests_utils:assert_distribution_in_dir_structure(
        Config, DirStructure(lists:usort([?GET_DOMAIN_BIN(Worker1), ?GET_DOMAIN_BIN(TargetWorker)])), GuidsAndPaths)),
    % check that failed files list is empty 
    ?assert(is_failed_files_list_empty(Workers, file_id:guid_to_space_id(FileGuid))).


qos_status_after_failed_transfer_deleted_file(Config, SpaceId, TargetWorker) ->
    [Worker1 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    qos_tests_utils:mock_replica_synchronizer(Workers, {error, some_error}),
    
    Name = generator:gen_name(),
    DirStructure = fun(Distribution) ->
        {SpaceId, [
            {Name, [
                {?filename(Name, 1), ?TEST_DATA, Distribution}
            ]}
        ]}
    end,
    
    % create new QoS entry and check, that it is not fulfilled
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(Config, DirStructure([?GET_DOMAIN_BIN(Worker1)]), SpaceId, Name),
    % check that file is on qos failed files list
    FileGuid = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, [1]), GuidsAndPaths),
    ?assertEqual(true, is_file_in_failed_files_list(TargetWorker, FileGuid), ?ATTEMPTS),
    ?assert(is_failed_files_list_empty(get_workers_list_without_provider(Workers, ?GET_DOMAIN_BIN(TargetWorker)),
        file_id:guid_to_space_id(FileGuid))),
    % check file distribution (file blocks should be only on source provider)
    ?assert(qos_tests_utils:assert_distribution_in_dir_structure(Config, DirStructure([?GET_DOMAIN_BIN(Worker1)]), GuidsAndPaths)),
    % initialize periodic check of failed files
    utils:rpc_multicall(Workers, qos_worker, init_retry_failed_files, []),
    
    % delete file on random worker
    DirGuid = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, []), GuidsAndPaths),
    DeletingWorker = lists_utils:random_element(Workers),
    ok = lfm_proxy:unlink(DeletingWorker, SessId(DeletingWorker), ?FILE_REF(FileGuid)),
    lists:foreach(fun(W) ->
        ?assertEqual({error, enoent}, lfm_proxy:stat(W, SessId(W), ?FILE_REF(FileGuid)), ?ATTEMPTS)
    end, Workers),
    % check that QoS entry is eventually fulfilled
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [DirGuid], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    % no need to check distribution as file was deleted
    % check that failed files list is empty (attempts needed to wait for failed 
    % files retry to execute - qos status change was triggered by file deletion)
    ?assertEqual(true, is_failed_files_list_empty(Workers, file_id:guid_to_space_id(FileGuid)), 10).


qos_status_after_failed_transfer_deleted_entry(Config, SpaceId, TargetWorker) ->
    [Worker1 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    qos_tests_utils:mock_replica_synchronizer(Workers, {error, some_error}),
    
    Name = generator:gen_name(),
    DirStructure = fun(Distribution) ->
        {SpaceId, [
            {Name, ?TEST_DATA, Distribution}
        ]}
    end,
    
    % create new QoS entry and check, that it is not fulfilled
    {GuidsAndPaths, [QosEntryId]} = prepare_qos_status_test_env(Config, DirStructure([?GET_DOMAIN_BIN(Worker1)]), SpaceId, Name),
    % add second entry to the same file
    {_, QosEntryList} = prepare_qos_status_test_env(Config, undefined, SpaceId, Name),
    % check that file is on qos failed files list
    FileGuid = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, []), GuidsAndPaths),
    ?assertEqual(true, is_file_in_failed_files_list(TargetWorker, FileGuid), ?ATTEMPTS),
    ?assert(is_failed_files_list_empty(get_workers_list_without_provider(Workers, ?GET_DOMAIN_BIN(TargetWorker)),
        file_id:guid_to_space_id(FileGuid))),
    % check file distribution (file blocks should be only on source provider)
    ?assert(qos_tests_utils:assert_distribution_in_dir_structure(Config, DirStructure([?GET_DOMAIN_BIN(Worker1)]), GuidsAndPaths)),
    % initialize periodic check of failed files
    utils:rpc_multicall(Workers, qos_worker, init_retry_failed_files, []),
    
    % delete one QoS entry on random worker
    DeletingWorker = lists_utils:random_element(Workers),
    ok = lfm_proxy:remove_qos_entry(DeletingWorker, SessId(DeletingWorker), QosEntryId),
    lists:foreach(fun(W) ->
        ?assertEqual({error, not_found}, lfm_proxy:get_qos_entry(W, SessId(W), QosEntryId), ?ATTEMPTS)
    end, Workers),
    
    % check that after a successful transfer QoS entry is eventually fulfilled
    qos_tests_utils:mock_replica_synchronizer(Workers, passthrough),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid], QosEntryList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    % check file distribution again (file blocks should be on both source and target provider)
    ?assertEqual(true, qos_tests_utils:assert_distribution_in_dir_structure(
        Config, DirStructure(lists:usort([?GET_DOMAIN_BIN(Worker1), ?GET_DOMAIN_BIN(TargetWorker)])), GuidsAndPaths)),
    % check that failed files list is empty 
    ?assert(is_failed_files_list_empty(Workers, file_id:guid_to_space_id(FileGuid))).


%%%===================================================================
%%% QoS with links test bases
%%%===================================================================

qos_with_hardlink_test_base(Config, SpaceId, Mode) ->
    % Mode can be one of 
    %   * direct - QoS entry is added directly to file/hardlink; 
    %   * effective - file and hardlink are in different directories, QoS entry is added on these directories 
    
    [Worker1 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    SpaceGuid = rpc:call(Worker1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    {FileParent, LinkParent} = case Mode of
        direct -> {SpaceGuid, SpaceGuid};
        effective ->
            {ok, Dir1Guid} = lfm_proxy:mkdir(Worker1, SessId(Worker1), SpaceGuid, generator:gen_name(), ?DEFAULT_DIR_PERMS),
            {ok, Dir2Guid} = lfm_proxy:mkdir(Worker1, SessId(Worker1), SpaceGuid, generator:gen_name(), ?DEFAULT_DIR_PERMS),
            {Dir1Guid, Dir2Guid}
    end,
    
    {ok, FileGuid} = lfm_proxy:create(Worker1, SessId(Worker1), FileParent, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_link(Worker1, SessId(Worker1), ?FILE_REF(FileGuid), ?FILE_REF(LinkParent), generator:gen_name()),
    await_files_sync_between_workers(Workers, [FileGuid, LinkGuid], SessId),
    
    QosTargets = case Mode of
        direct -> [FileGuid, LinkGuid];
        effective -> [FileParent, LinkParent]
    end,
    
    QosList = lists:map(fun(GuidToAddQos) ->
        {ok, QosEntryId} = lfm_proxy:add_qos_entry(Worker1, SessId(Worker1), ?FILE_REF(GuidToAddQos), <<"country=FR">>, 1),
        assert_effective_entry(Worker1, SessId(Worker1), QosEntryId, [FileGuid, LinkGuid], []),
        QosEntryId
    end, QosTargets),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid, LinkGuid], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    
    qos_tests_utils:mock_transfers(Workers),
    lists:foreach(fun(GuidToWrite) ->
        {ok, Handle} = lfm_proxy:open(Worker1, SessId(Worker1), ?FILE_REF(GuidToWrite), write),
        {ok, _} = lfm_proxy:write(Worker1, Handle, 0, crypto:strong_rand_bytes(123)),
        ok = lfm_proxy:close(Worker1, Handle),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid, LinkGuid], QosList, ?PENDING_QOS_STATUS), ?ATTEMPTS),
        
        qos_tests_utils:finish_all_transfers([FileGuid]),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid, LinkGuid], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS)
    end, [FileGuid, LinkGuid]).


qos_with_hardlink_deletion_test_base(Config, SpaceId, ToDelete) ->
    % ToDelete can be one of 
    %   * inode - QoS entry is added to inode and it is later deleted; 
    %   * hardlink - QoS entry is added to hardlink and it is later deleted; 
    %   * mixed - file that QoS entry is added to is randomly selected, and deleted is the other one
    
    [Worker1 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    SpaceGuid = rpc:call(Worker1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    
    {ok, FileGuid} = lfm_proxy:create(Worker1, SessId(Worker1), SpaceGuid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_link(Worker1, SessId(Worker1), ?FILE_REF(FileGuid), ?FILE_REF(SpaceGuid), generator:gen_name()),
    await_files_sync_between_workers(Workers, [FileGuid, LinkGuid], SessId),
    Guids = [FileGuid, LinkGuid],
    
    {ToAddQosGuid, ToDeleteGuid} = case ToDelete of
        inode -> {FileGuid, FileGuid};
        hardlink -> {LinkGuid, LinkGuid};
        mixed -> 
            case rand:uniform(2) of
                1 -> {FileGuid, LinkGuid};
                2 -> {LinkGuid, FileGuid}
            end
    end,
    {ok, QosEntryId} = lfm_proxy:add_qos_entry(Worker1, SessId(Worker1), ?FILE_REF(ToAddQosGuid), <<"country=FR">>, 1),
    assert_effective_entry(Worker1, SessId(Worker1), QosEntryId, Guids, []),
    ok = lfm_proxy:unlink(Worker1, SessId(Worker1), ?FILE_REF(ToDeleteGuid)),
    assert_effective_entry(Worker1, SessId(Worker1), QosEntryId, Guids -- [ToDeleteGuid], []).


qos_on_symlink_test_base(Config, SpaceId) ->
    [Worker1 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    SpaceGuid = rpc:call(Worker1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    
    {ok, FileGuid} = lfm_proxy:create(Worker1, SessId(Worker1), SpaceGuid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, FilePath} = lfm_proxy:get_file_path(Worker1, SessId(Worker1), FileGuid),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_symlink(Worker1, SessId(Worker1), ?FILE_REF(SpaceGuid), generator:gen_name(), FilePath),
    await_files_sync_between_workers(Workers, [FileGuid, LinkGuid], SessId),
    
    {ok, QosEntryId} = lfm_proxy:add_qos_entry(Worker1, SessId(Worker1), ?FILE_REF(LinkGuid), <<"country=FR">>, 1),
    assert_effective_entry(Worker1, SessId(Worker1), QosEntryId, [LinkGuid], [FileGuid]),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid, LinkGuid], QosEntryId, ?FULFILLED_QOS_STATUS), ?ATTEMPTS).


effective_qos_with_symlink_test_base(Config, SpaceId) ->
    [Worker1 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    SpaceGuid = rpc:call(Worker1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    {ok, Dir1Guid} = lfm_proxy:mkdir(Worker1, SessId(Worker1), SpaceGuid, generator:gen_name(), ?DEFAULT_DIR_PERMS),
    {ok, Dir2Guid} = lfm_proxy:mkdir(Worker1, SessId(Worker1), SpaceGuid, generator:gen_name(), ?DEFAULT_DIR_PERMS),
    
    {ok, FileGuid} = lfm_proxy:create(Worker1, SessId(Worker1), Dir1Guid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, FilePath} = lfm_proxy:get_file_path(Worker1, SessId(Worker1), FileGuid),
    {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_symlink(Worker1, SessId(Worker1), ?FILE_REF(Dir2Guid), generator:gen_name(), FilePath),
    await_files_sync_between_workers(Workers, [FileGuid, LinkGuid], SessId),
    
    {ok, QosEntryId1} = lfm_proxy:add_qos_entry(Worker1, SessId(Worker1), ?FILE_REF(Dir1Guid), <<"country=FR">>, 1),
    assert_effective_entry(Worker1, SessId(Worker1), QosEntryId1, [FileGuid], [LinkGuid]),
    {ok, QosEntryId2} = lfm_proxy:add_qos_entry(Worker1, SessId(Worker1), ?FILE_REF(Dir2Guid), <<"country=FR">>, 1),
    assert_effective_entry(Worker1, SessId(Worker1), QosEntryId2, [LinkGuid], [FileGuid]),
    
    QosList = [QosEntryId1, QosEntryId2],
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid, LinkGuid], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    
    qos_tests_utils:mock_transfers(Workers),
    {ok, Handle} = lfm_proxy:open(Worker1, SessId(Worker1), ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(Worker1, Handle, 0, crypto:strong_rand_bytes(123)),
    ok = lfm_proxy:close(Worker1, Handle),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid], QosList, ?PENDING_QOS_STATUS), ?ATTEMPTS),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [LinkGuid], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    
    qos_tests_utils:finish_all_transfers([FileGuid]),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid, LinkGuid], QosList, ?FULFILLED_QOS_STATUS), ?ATTEMPTS).


create_hardlink_in_dir_with_qos(Config, SpaceId) ->
    [Worker1 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    SpaceGuid = rpc:call(Worker1, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    {ok, Dir1Guid} = lfm_proxy:mkdir(Worker1, SessId(Worker1), SpaceGuid, generator:gen_name(), ?DEFAULT_DIR_PERMS),
    {ok, FileGuid} = lfm_proxy:create(Worker1, SessId(Worker1), SpaceGuid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    {ok, QosEntryId} = lfm_proxy:add_qos_entry(Worker1, SessId(Worker1), ?FILE_REF(Dir1Guid), <<"country=FR">>, 1),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid, Dir1Guid], [QosEntryId], ?FULFILLED_QOS_STATUS), ?ATTEMPTS),
    
    qos_tests_utils:mock_transfers(Workers),
    lists:foreach(fun(Worker) ->
        {ok, #file_attr{guid = LinkGuid}} = lfm_proxy:make_link(Worker, SessId(Worker), ?FILE_REF(FileGuid), ?FILE_REF(Dir1Guid), generator:gen_name()),
        await_files_sync_between_workers(Workers, [FileGuid, LinkGuid], SessId),
        assert_effective_entry(Worker, SessId(Worker), QosEntryId, [LinkGuid, FileGuid], []),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [Dir1Guid, LinkGuid], [QosEntryId], ?PENDING_QOS_STATUS), ?ATTEMPTS),
        qos_tests_utils:finish_all_transfers([LinkGuid]),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [Dir1Guid, LinkGuid], [QosEntryId], ?FULFILLED_QOS_STATUS), ?ATTEMPTS)
    end, Workers).
    

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?APP_NAME, qos_retry_failed_files_interval_seconds, 5)
        end, ?config(op_worker_nodes, NewConfig)),
        initializer:mock_auth_manager(NewConfig),
        application:start(ssl),
        application:ensure_all_started(hackney),
        NewConfig
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, qos_tests_utils, ?MODULE]}
        | Config
    ].


end_per_suite(_Config) ->
    application:stop(hackney),
    application:stop(ssl).


init_per_testcase(Config) ->
    ct:timetrap(timer:minutes(10)),
    NewConfig = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(NewConfig),
    NewConfig.


end_per_testcase(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers),
    initializer:clean_test_users_and_spaces_no_validate(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
prepare_qos_status_test_env(Config, DirStructure, SpaceId, Name) ->
    [Worker1 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    QosRootFilePath = filename:join([<<"/">>, SpaceId, Name]),
    
    TestDirStructure = case DirStructure of
        undefined -> undefined;
        _ -> #test_dir_structure{dir_structure = DirStructure}
    end,
    
    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = TestDirStructure,
        qos_to_add = [
            #qos_to_add{
                worker = Worker1,
                qos_name = ?QOS1,
                path = QosRootFilePath,
                expression = <<"country=PT">>
            }
        ],
        % do not wait for QoS fulfillment
        wait_for_qos_fulfillment = [],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = Workers,
                qos_name = ?QOS1,
                file_key = {path, QosRootFilePath},
                qos_expression = [<<"country=PT">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(Worker1)}
            }
        ]
    },
    
    {GuidsAndPaths, QosNameIdMapping} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),
    QosList = maps:values(QosNameIdMapping),
    
    FilesAndDirs = maps:get(files, GuidsAndPaths, []) ++ maps:get(dirs, GuidsAndPaths, []),
    FilesAndDirsGuids = lists:filtermap(fun({G, P}) when P >= QosRootFilePath -> {true, G}; (_) -> false end, FilesAndDirs),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, FilesAndDirsGuids, QosList, ?PENDING_QOS_STATUS)),
    {GuidsAndPaths, QosList}.


%% @private
resolve_path(SpaceId, Name, Files) ->
    Files1 = lists:map(fun(A) -> ?filename(Name, A) end, Files),
    filepath_utils:join([<<"/">>, SpaceId, Name | Files1]).


%% @private
is_failed_files_list_empty(Workers, SpaceId) ->
    lists:all(fun(Worker) ->
        {ok, []} == get_qos_failed_files_list(Worker, SpaceId)
    end, Workers).


%% @private
is_file_in_failed_files_list(Worker, FileGuid) ->
    Uuid = file_id:guid_to_uuid(FileGuid),
    SpaceId = file_id:guid_to_space_id(FileGuid),
    {ok, List} = get_qos_failed_files_list(Worker, SpaceId),
    lists:member(Uuid, List).


%% @private
get_qos_failed_files_list(Worker, SpaceId) ->
    rpc:call(Worker, datastore_model, fold_links, [
        #{model => qos_entry}, 
        <<"failed_files_qos_key_", SpaceId/binary>>, 
        ?GET_DOMAIN_BIN(Worker), 
        fun qos_entry:accumulate_link_names/2,
        [], 
        #{}
    ]).


%% @private
get_workers_list_without_provider(Workers, ProviderId) ->
    lists:filter(fun(W) ->
        ProviderId =/= ?GET_DOMAIN_BIN(W)
    end, Workers).


%% @private
prepare_type_spec(reg_file, _Workers, _SessIdFun, _Target) -> reg_file;
prepare_type_spec(hardlink, Workers, SessIdFun, {target, FileToLinkGuid}) ->
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, _}, lfm_proxy:stat(Worker, SessIdFun(Worker), ?FILE_REF(FileToLinkGuid)), ?ATTEMPTS)
    end, Workers),
    {hardlink, FileToLinkGuid};
prepare_type_spec(random, Workers, SessIdFun, Target) -> 
    NewFileType = case rand:uniform(2) of
        1 -> reg_file;
        2 -> hardlink
    end,
    prepare_type_spec(NewFileType, Workers, SessIdFun, Target).


%% @private
create_link_target(Worker, SessId, SpaceId) ->
    SpaceGuid = rpc:call(Worker, fslogic_uuid, spaceid_to_space_dir_guid, [SpaceId]),
    {ok, FileToLinkGuid} = lfm_proxy:create(Worker, SessId, SpaceGuid, generator:gen_name(), ?DEFAULT_FILE_PERMS),
    FileToLinkGuid.


%% @private
await_files_sync_between_workers(Workers, Guids, SessIdFun) ->
    lists:foreach(fun(Worker) ->
        lists:foreach(fun(Guid) ->
            ?assertMatch({ok, _}, lfm_proxy:stat(Worker, SessIdFun(Worker), ?FILE_REF(Guid)), ?ATTEMPTS)
        end, Guids)
    end, Workers).


%% @private
create_files_and_write(Worker, SessId, ParentGuid, TypeSpec, NumOfFiles) ->
    lists:map(fun(_) ->
        {ok, {FileGuid, FileHandle}} =  qos_tests_utils:create_and_open(Worker, SessId, ParentGuid, TypeSpec),
        {ok, _} = lfm_proxy:write(Worker, FileHandle, 0, <<"new_data">>),
        ok = lfm_proxy:close(Worker, FileHandle),
        FileGuid
    end, lists:seq(1, NumOfFiles)).


%% @private
assert_effective_entry(Worker, SessId, QosEntryId, FilesToAssertTrue, FilesToAssertFalse) ->
    lists:foreach(fun(Guid) ->
        ?assertMatch({ok, {#{QosEntryId := _}, _}}, lfm_proxy:get_effective_file_qos(Worker, SessId, ?FILE_REF(Guid)))
    end, FilesToAssertTrue),
    lists:foreach(fun(Guid) ->
        ?assertNotMatch({ok, {#{QosEntryId := _}, _}}, lfm_proxy:get_effective_file_qos(Worker, SessId, ?FILE_REF(Guid)))
    end, FilesToAssertFalse).
