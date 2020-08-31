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
    qos_status_during_traverse_with_file_deletion_test_base/3,
    qos_status_during_traverse_with_dir_deletion_test_base/3,
    qos_status_during_traverse_file_without_qos_test_base/2,
    qos_status_during_reconciliation_test_base/4,
    qos_status_during_reconciliation_with_file_deletion_test_base/2,
    qos_status_during_reconciliation_with_dir_deletion_test_base/2,
    qos_status_after_failed_transfer/3,
    qos_status_after_failed_transfer_deleted_file/3,
    qos_status_after_failed_transfer_deleted_entry/3
]).

-define(ATTEMPTS, 60).

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
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, Guids1, QosList, ?FULFILLED), ?ATTEMPTS),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, Guids2 ++ Guids3 ++ [Dir1, Dir2, Dir3, Dir4], QosList, ?PENDING), ?ATTEMPTS),
    
    ok = qos_tests_utils:finish_all_transfers(Guids2),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, Guids1 ++ Guids2 ++ [Dir2], QosList, ?FULFILLED), ?ATTEMPTS),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, Guids3 ++ [Dir1, Dir3, Dir4], QosList, ?PENDING), ?ATTEMPTS),
    
    ok = qos_tests_utils:finish_all_transfers(Guids3),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, Guids1 ++ Guids2 ++ Guids3 ++ [Dir1, Dir2, Dir3, Dir4], QosList, ?FULFILLED), ?ATTEMPTS).


qos_status_during_traverse_with_file_deletion_test_base(Config, SpaceId, NumberOfFilesInDir) ->
    [Worker1 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    Name = generator:gen_name(),
    DirStructure =
        {SpaceId, [
            {Name, % Dir1
                lists:map(fun(Num) -> {?filename(Name, Num), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]} end, lists:seq(1, NumberOfFilesInDir))
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
        ok = lfm_proxy:unlink(Worker1, SessId(Worker1), {guid, Guid})
    end, ToDelete),
    
    ok = qos_tests_utils:finish_all_transfers(ToFinish),
    
    Dir1 = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, []), GuidsAndPaths),
    ok = ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, ToFinish ++ [Dir1], QosList, ?FULFILLED), ?ATTEMPTS),
    % finish transfers to unlock waiting slave job processes
    ok = qos_tests_utils:finish_all_transfers(ToDelete),
    % These files where deleted so QoS is not fulfilled
    ok = ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, ToDelete, QosList, {error, enoent}), ?ATTEMPTS).
    

qos_status_during_traverse_with_dir_deletion_test_base(Config, SpaceId, NumberOfFilesInDir) ->
    [Worker1 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    Name = generator:gen_name(),
    DirStructure =
        {SpaceId, [
            {Name, [ % Dir1
                {?filename(Name, 1), % Dir2
                    lists:map(fun(Num) -> {?filename(Name, Num), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]} end, lists:seq(1, NumberOfFilesInDir)) 
                }
            ]}
        ]},
    
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(Config, DirStructure, SpaceId, Name),
    
    Dir1 = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, []), GuidsAndPaths),
    Dir2 = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, [1]), GuidsAndPaths),
    
    ok = lfm_proxy:rm_recursive(Worker1, SessId(Worker1), {guid, Dir2}),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [Dir1], QosList, ?FULFILLED), ?ATTEMPTS),
    
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
    {ok, {FileGuid, FileHandle}} = lfm_proxy:create_and_open(Worker1, SessId(Worker1), Dir1, generator:gen_name(), 8#664),
    {ok, _} = lfm_proxy:write(Worker1, FileHandle, 0, <<"new_data">>),
    ok = lfm_proxy:close(Worker1, FileHandle),
    lists:foreach(fun(W) ->
        ?assertMatch({ok, _}, lfm_proxy:stat(W, SessId(W), {guid, FileGuid}), ?ATTEMPTS)
    end, Workers),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [Dir1, FileGuid], QosList, ?FULFILLED), ?ATTEMPTS),
    
    % finish transfer to unlock waiting slave job process
    ok = qos_tests_utils:finish_all_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)]).
    

qos_status_during_reconciliation_test_base(Config, SpaceId, DirStructure, Filename) ->
    [Worker1 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    
    {GuidsAndPaths, QosList} = prepare_qos_status_test_env(Config, DirStructure, SpaceId, Filename),
    
    ok = qos_tests_utils:finish_all_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)]),
    
    FilesAndDirs = maps:get(files, GuidsAndPaths) ++ maps:get(dirs, GuidsAndPaths),
    FilesAndDirsGuids = lists:map(fun({G, _}) -> G end, FilesAndDirs),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, FilesAndDirsGuids, QosList, ?FULFILLED), ?ATTEMPTS),
    
    IsAncestor = fun
        (F, F) -> true;
        (A, F) -> str_utils:binary_starts_with(F, <<A/binary, "/">>)
    end,
    
    lists:foreach(fun({FileGuid, FilePath}) ->
        ct:pal("writing to file ~p on worker ~p", [FilePath, Worker1]),
        {ok, FileHandle} = lfm_proxy:open(Worker1, SessId(Worker1), {guid, FileGuid}, write),
        {ok, _} = lfm_proxy:write(Worker1, FileHandle, 0, <<"new_data">>),
        ok = lfm_proxy:close(Worker1, FileHandle),
        lists:foreach(fun({G, P}) ->
            ct:pal("Checking file: ~p~n\tis_ancestor: ~p", [P, IsAncestor(P, FilePath)]),
            ExpectedStatus = case IsAncestor(P, FilePath) of
                true -> ?PENDING;
                false -> ?FULFILLED
            end,
            ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [G], QosList, ExpectedStatus), ?ATTEMPTS)
        end, FilesAndDirs),
        ok = qos_tests_utils:finish_all_transfers([FileGuid]),
        ct:pal("Checking after finish"),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, FilesAndDirsGuids, QosList, ?FULFILLED), ?ATTEMPTS)
    end, maps:get(files, GuidsAndPaths)).


qos_status_during_reconciliation_with_file_deletion_test_base(Config, SpaceId) ->
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
    
    ok = qos_tests_utils:finish_all_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)]),
    
    FilesAndDirs = maps:get(files, GuidsAndPaths) ++ maps:get(dirs, GuidsAndPaths),
    FilesAndDirsGuids = lists:map(fun({G, _}) -> G end, FilesAndDirs),
    [{Dir1, _}] = maps:get(dirs, GuidsAndPaths),
    
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, FilesAndDirsGuids, QosList, ?FULFILLED), ?ATTEMPTS),
    
    lists:foreach(fun(Worker) ->
        {ok, {FileGuid, FileHandle}} = lfm_proxy:create_and_open(Worker1, SessId(Worker1), Dir1, generator:gen_name(), 8#664),
        {ok, _} = lfm_proxy:write(Worker1, FileHandle, 0, <<"new_data">>),
        ok = lfm_proxy:close(Worker1, FileHandle),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid], QosList, ?PENDING), ?ATTEMPTS),
        ok = lfm_proxy:unlink(Worker, SessId(Worker), {guid, FileGuid}),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, FilesAndDirsGuids, QosList, ?FULFILLED), ?ATTEMPTS),
        % finish transfer to unlock waiting slave job process
        ok = qos_tests_utils:finish_all_transfers([FileGuid])
    end, Workers).


qos_status_during_reconciliation_with_dir_deletion_test_base(Config, SpaceId) ->
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
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [Dir1], QosList, ?FULFILLED), ?ATTEMPTS),
    
    lists:foreach(fun(Worker) ->
        {ok, DirGuid} = lfm_proxy:mkdir(Worker1, SessId(Worker1), Dir1, generator:gen_name(), 8#775),
        {ok, {FileGuid, FileHandle}} = lfm_proxy:create_and_open(Worker1, SessId(Worker1), DirGuid, generator:gen_name(), 8#664),
        {ok, _} = lfm_proxy:write(Worker1, FileHandle, 0, <<"new_data">>),
        ok = lfm_proxy:close(Worker1, FileHandle),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [Dir1], QosList, ?PENDING), ?ATTEMPTS),
        ok = lfm_proxy:rm_recursive(Worker, SessId(Worker), {guid, DirGuid}),
        ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [Dir1], QosList, ?FULFILLED), ?ATTEMPTS),
        % finish transfer to unlock waiting slave job process
        ok = qos_tests_utils:finish_all_transfers([FileGuid])
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
    rpc:call(Worker1, qos_entry, apply_to_all_in_failed_files_list, [
        file_id:guid_to_space_id(FileGuid),
        fun(FileUuid) -> ct:print(FileUuid) end
    ]),
    ?assert(is_file_in_failed_files_list(TargetWorker, FileGuid)),
    ?assert(is_failed_files_list_empty(get_workers_list_without_provider(Workers, ?GET_DOMAIN_BIN(TargetWorker)), 
        file_id:guid_to_space_id(FileGuid))),
    % check file distribution (file blocks should be only on source provider)
    ?assert(qos_tests_utils:assert_distribution_in_dir_structure(Config, DirStructure([?GET_DOMAIN_BIN(Worker1)]), GuidsAndPaths)),
    % initialize periodic check of failed files
    rpc:multicall(Workers, qos_worker, init_retry_failed_files, []),
    
    % check that after a successful transfer QoS entry is eventually fulfilled
    qos_tests_utils:mock_replica_synchronizer(Workers, passthrough),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid], QosList, ?FULFILLED), ?ATTEMPTS),
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
    % check file distribution (file blocks should be only on source provider)
    ?assert(qos_tests_utils:assert_distribution_in_dir_structure(Config, DirStructure([?GET_DOMAIN_BIN(Worker1)]), GuidsAndPaths)),
    % check that file is on qos failed files list
    FileGuid = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, [1]), GuidsAndPaths),
    ?assert(is_file_in_failed_files_list(TargetWorker, FileGuid)),
    ?assert(is_failed_files_list_empty(get_workers_list_without_provider(Workers, ?GET_DOMAIN_BIN(TargetWorker)),
        file_id:guid_to_space_id(FileGuid))),
    % initialize periodic check of failed files
    rpc:multicall(Workers, qos_worker, init_retry_failed_files, []),
    
    % delete file on random worker
    DirGuid = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, []), GuidsAndPaths),
    DeletingWorker = lists_utils:random_element(Workers),
    ok = lfm_proxy:unlink(DeletingWorker, SessId(DeletingWorker), {guid, FileGuid}),
    lists:foreach(fun(W) ->
        ?assertEqual({error, enoent}, lfm_proxy:stat(W, SessId(W), {guid, FileGuid}), ?ATTEMPTS)
    end, Workers),
    % check that QoS entry is eventually fulfilled
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [DirGuid], QosList, ?FULFILLED), ?ATTEMPTS),
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
    % check file distribution (file blocks should be only on source provider)
    ?assert(qos_tests_utils:assert_distribution_in_dir_structure(Config, DirStructure([?GET_DOMAIN_BIN(Worker1)]), GuidsAndPaths)),
    % check that file is on qos failed files list
    FileGuid = qos_tests_utils:get_guid(resolve_path(SpaceId, Name, []), GuidsAndPaths),
    ?assert(is_file_in_failed_files_list(TargetWorker, FileGuid)),
    ?assert(is_failed_files_list_empty(get_workers_list_without_provider(Workers, ?GET_DOMAIN_BIN(TargetWorker)),
        file_id:guid_to_space_id(FileGuid))),
    % initialize periodic check of failed files
    rpc:multicall(Workers, qos_worker, init_retry_failed_files, []),
    
    % delete one QoS entry on random worker
    DeletingWorker = lists_utils:random_element(Workers),
    ok = lfm_proxy:remove_qos_entry(DeletingWorker, SessId(DeletingWorker), QosEntryId),
    lists:foreach(fun(W) ->
        ?assertEqual({error, not_found}, lfm_proxy:get_qos_entry(W, SessId(W), QosEntryId), ?ATTEMPTS)
    end, Workers),
    
    % check that after a successful transfer QoS entry is eventually fulfilled
    qos_tests_utils:mock_replica_synchronizer(Workers, passthrough),
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, [FileGuid], QosEntryList, ?FULFILLED), ?ATTEMPTS),
    % check file distribution again (file blocks should be on both source and target provider)
    ?assertEqual(true, qos_tests_utils:assert_distribution_in_dir_structure(
        Config, DirStructure(lists:usort([?GET_DOMAIN_BIN(Worker1), ?GET_DOMAIN_BIN(TargetWorker)])), GuidsAndPaths)),
    % check that failed files list is empty 
    ?assert(is_failed_files_list_empty(Workers, file_id:guid_to_space_id(FileGuid))).


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
    ?assertEqual([], qos_tests_utils:gather_not_matching_statuses_on_all_workers(Config, FilesAndDirsGuids, QosList, ?PENDING)),
    {GuidsAndPaths, QosList}.


%% @private
resolve_path(SpaceId, Name, Files) ->
    Files1 = lists:map(fun(A) -> ?filename(Name, A) end, Files),
    fslogic_path:join([<<"/">>, SpaceId, Name | Files1]).


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