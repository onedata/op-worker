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
    effective_qos_for_files_in_different_directories_of_tree_structure_spec/5
]).


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
                qos_expression_in_rpn = [<<"country=FR">>],
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
                expression = <<"type=disk&tier=t2">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression_in_rpn = [<<"type=disk">>, <<"tier=t2">>, <<"&">>],
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
                expression = <<"type=disk-country=PT">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression_in_rpn = [<<"type=disk">>, <<"country=PT">>, <<"-">>],
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
                qos_expression_in_rpn = [<<"country=PL">>, <<"tier=t3">>, <<"|">>],
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
                qos_expression_in_rpn = [<<"type=disk">>],
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
                qos_expression_in_rpn = [<<"type=disk">>, <<"tier=t2">>, <<"&">>, <<"country=FR">>, <<"|">>],
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
                expression = <<"country=PL|country=FR-type=tape">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression_in_rpn = [<<"country=PL">>, <<"country=FR">>, <<"|">>, <<"type=tape">>, <<"-">>],
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
                expression = <<"type=disk&param1=val1-country=PL">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression_in_rpn = [<<"type=disk">>, <<"param1=val1">>, <<"&">>, <<"country=PL">>, <<"-">>],
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
                expression = <<"country=PL|country=FR|country=PT">>,
                replicas_num = 3
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression_in_rpn = [<<"country=PL">>, <<"country=FR">>, <<"|">>, <<"country=PT">>, <<"|">>],
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
                qos_expression_in_rpn = [<<"country=IT">>],
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
                expression = <<"country=PL|country=PT-type=disk">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression_in_rpn = [<<"country=PL">>, <<"country=PT">>, <<"|">>, <<"type=disk">>, <<"-">>],
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
                expression = <<"country=PL|(country=PT-type=disk)">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression_in_rpn = [<<"country=PL">>, <<"country=PT">>, <<"type=disk">>, <<"-">>, <<"|">>],
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
                qos_expression_in_rpn = [<<"type=disk">>, <<"tier=t2">>, <<"&">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos1)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression_in_rpn = [<<"country=FR">>],
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
                qos_expression_in_rpn = [<<"type=tape">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos1)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression_in_rpn = [<<"country=FR">>],
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
                qos_expression_in_rpn = [<<"type=tape">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos1)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression_in_rpn = [<<"type=tape">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos2)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS3,
                file_key = {path, Path},
                qos_expression_in_rpn = [<<"type=tape">>],
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
                expression = <<"type=tape-country=PL">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS1,
                file_key = {path, Path},
                qos_expression_in_rpn = [<<"country=PL">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos1)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression_in_rpn = [<<"type=tape">>, <<"country=PL">>, <<"-">>],
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
                qos_expression_in_rpn = [<<"country=FR">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos1)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression_in_rpn = [<<"country=IT">>],
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
                qos_expression_in_rpn = [<<"type=disk">>],
                replicas_num = 2,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos1)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS2,
                file_key = {path, Path},
                qos_expression_in_rpn = [<<"tier=t2">>],
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
                qos_expression_in_rpn = [<<"country=FR">>],
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
                qos_expression_in_rpn = [<<"country=PL">>],
                replicas_num = 1,
                file_key = {path, Dir1Path},
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos1)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS2,
                qos_expression_in_rpn = [<<"country=FR">>],
                replicas_num = 1,
                file_key = {path, Dir2Path},
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos2)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS3,
                qos_expression_in_rpn = [<<"country=PT">>],
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
                qos_expression_in_rpn = [<<"country=PL">>],
                replicas_num = 1,
                file_key = {path, Dir1Path},
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos1)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS2,
                qos_expression_in_rpn = [<<"country=FR">>],
                replicas_num = 1,
                file_key = {path, Dir2Path},
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerAddingQos2)}
            },
            #expected_qos_entry{
                workers = AssertionWorkers,
                qos_name = ?QOS3,
                qos_expression_in_rpn = [<<"country=PT">>],
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
