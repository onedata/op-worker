%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests for QoS management on multiple providers.
%%% @end
%%%--------------------------------------------------------------------
-module(multi_provider_qos_test_SUITE).
-author("Michal Cwiertnia").
-author("Michal Stanisz").

-include("qos_tests_utils.hrl").
-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, init_per_testcase/2,
    end_per_suite/1, end_per_testcase/2
]).

%% tests
-export([
    simple_key_val_qos/1,
    qos_with_intersection/1,
    qos_with_complement/1,
    qos_with_union/1,
    qos_with_multiple_replicas/1,
    qos_with_intersection_and_union/1,
    qos_with_union_and_complement/1,
    qos_with_intersection_and_complement/1,
    qos_with_multiple_replicas_and_union/1,
    key_val_qos_that_cannot_be_fulfilled/1,
    qos_that_cannot_be_fulfilled/1,
    qos_with_parens/1,

    multi_qos_resulting_in_different_storages/1,
    multi_qos_resulting_in_the_same_storages/1,
    same_qos_multiple_times/1,
    contrary_qos/1,
    multi_qos_where_one_cannot_be_satisfied/1,
    multi_qos_that_overlaps/1,

    effective_qos_for_file_in_directory/1,
    effective_qos_for_file_in_nested_directories/1,
    effective_qos_for_files_in_different_directories_of_tree_structure/1,

    qos_restoration_file_test/1,
    qos_restoration_dir_test/1,
    reconcile_qos_using_file_meta_posthooks_test/1,
    
    qos_status_during_traverse_test/1,
    qos_status_during_traverse_with_file_deletion_test/1,
    qos_status_during_traverse_with_dir_deletion_test/1,
    qos_status_during_reconciliation_test/1,
    qos_status_during_reconciliation_prefix_file_test/1,
    qos_status_during_reconciliation_with_file_deletion_test/1,
    qos_status_during_reconciliation_with_dir_deletion_test/1,
    qos_status_during_traverse_file_without_qos_test/1,
    qos_status_after_failed_transfers/1,
    qos_status_after_failed_transfers_deleted_file/1,
    qos_status_after_failed_transfers_deleted_entry/1,

    reevaluate_impossible_qos_test/1,
    reevaluate_impossible_qos_race_test/1,
    reevaluate_impossible_qos_conflict_test/1,
    
    qos_traverse_cancellation_test/1
]).

all() -> [
    simple_key_val_qos,
    qos_with_intersection,
    qos_with_complement,
    qos_with_union,
    qos_with_multiple_replicas,
    qos_with_intersection_and_union,
    qos_with_union_and_complement,
    qos_with_intersection_and_complement,
    qos_with_multiple_replicas_and_union,
    key_val_qos_that_cannot_be_fulfilled,
    qos_that_cannot_be_fulfilled,
    qos_with_parens,

    multi_qos_resulting_in_different_storages,
    multi_qos_resulting_in_the_same_storages,
    same_qos_multiple_times,
    contrary_qos,
    multi_qos_where_one_cannot_be_satisfied,
    multi_qos_that_overlaps,

    effective_qos_for_file_in_directory,
    effective_qos_for_file_in_nested_directories,
    effective_qos_for_files_in_different_directories_of_tree_structure,

    qos_restoration_file_test,
    qos_restoration_dir_test,
    reconcile_qos_using_file_meta_posthooks_test,

    qos_status_during_traverse_test,
    qos_status_during_traverse_with_file_deletion_test,
    qos_status_during_traverse_with_dir_deletion_test,
    qos_status_during_reconciliation_test,
    qos_status_during_reconciliation_prefix_file_test,
    qos_status_during_reconciliation_with_file_deletion_test,
    qos_status_during_reconciliation_with_dir_deletion_test,
    qos_status_during_traverse_file_without_qos_test,
    qos_status_after_failed_transfers,
    qos_status_after_failed_transfers_deleted_file,
    qos_status_after_failed_transfers_deleted_entry,

    reevaluate_impossible_qos_test,
    reevaluate_impossible_qos_race_test,
    reevaluate_impossible_qos_conflict_test,
    
    qos_traverse_cancellation_test
].


-define(FILE_PATH(FileName), filename:join([?SPACE_PATH1, FileName])).
-define(TEST_FILE_PATH, ?FILE_PATH(<<"file1">>)).
-define(TEST_DIR_PATH, ?FILE_PATH(<<"dir1">>)).

-define(SPACE_ID, <<"space1">>).
-define(SPACE_PATH1, <<"/space1">>).

-define(PROVIDER_ID(Worker), ?GET_DOMAIN_BIN(Worker)).
-define(GET_FILE_UUID(Worker, SessId, FilePath), qos_tests_utils:get_guid(Worker, SessId, FilePath)).


-define(simple_dir_structure(Name, Distribution),
    {?SPACE_ID, [
        {Name, ?TEST_DATA, Distribution}
    ]}
).
-define(nested_dir_structure(Name, Distribution),
    {?SPACE_ID, [
        {Name, [
            {?filename(Name, 1), [
                {?filename(Name, 1), ?TEST_DATA, Distribution},
                {?filename(Name, 2), ?TEST_DATA, Distribution},
                {?filename(Name, 3), ?TEST_DATA, Distribution},
                {?filename(Name, 4), ?TEST_DATA, Distribution}
            ]},
            {?filename(Name, 2), [
                {?filename(Name, 1), ?TEST_DATA, Distribution},
                {?filename(Name, 2), ?TEST_DATA, Distribution},
                {?filename(Name, 3), ?TEST_DATA, Distribution},
                {?filename(Name, 4), ?TEST_DATA, Distribution}
            ]}
        ]}
    ]}
).

-define(ATTEMPTS, 60).

%%%====================================================================
%%% Test function
%%%====================================================================

%%%===================================================================
%%% Group of tests that adds single QoS expression for file or directory
%%% and checks QoS docs and file distribution.
%%% Each test case is executed once for file and once for directory.
%%%===================================================================

simple_key_val_qos(Config) ->
    Workers = [WorkerP1, WorkerP2 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:simple_key_val_qos_spec(Path, WorkerP1, Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_with_intersection(Config) ->
    Workers = [WorkerP1, _WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP3)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_intersection_spec(Path, WorkerP1, Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_with_complement(Config) ->
    Workers = [WorkerP1 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_complement_spec(Path, WorkerP1, Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_with_union(Config) ->
    Workers = [WorkerP1, _WorkerP2, _WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_union_spec(Path, WorkerP1, Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_with_multiple_replicas(Config) ->
    Workers = [WorkerP1, _WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP3)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_multiple_replicas_spec(Path, WorkerP1, Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_with_intersection_and_union(Config) ->
    Workers = [WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2), ?PROVIDER_ID(WorkerP3)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_intersection_and_union_spec(Path, WorkerP1, Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_with_union_and_complement(Config) ->
    Workers = [WorkerP1 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_union_and_complement_spec(Path, WorkerP1, Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_with_intersection_and_complement(Config) ->
    Workers = [WorkerP1, _WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP3)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_intersection_and_complement_spec(Path, WorkerP1, Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).

qos_with_multiple_replicas_and_union(Config) ->
    Workers = [WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2), ?PROVIDER_ID(WorkerP3)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_multiple_replicas_and_union_spec(Path, WorkerP1, Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


key_val_qos_that_cannot_be_fulfilled(Config) ->
    Workers = [WorkerP1 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:key_val_qos_that_cannot_be_fulfilled_spec(Path, WorkerP1, Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_that_cannot_be_fulfilled(Config) ->
    Workers = [WorkerP1 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_that_cannot_be_fulfilled_spec(Path, WorkerP1, Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


qos_with_parens(Config) ->
    Workers = [WorkerP1 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:qos_with_parens_spec(Path, WorkerP1, Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


%%%===================================================================
%%% Group of tests that adds multiple QoS expression for single file or
%%% directory and checks QoS docs and file distribution.
%%% Each test case is executed once for file and once for directory.
%%%===================================================================

multi_qos_resulting_in_different_storages(Config) ->
    Workers = [WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2), ?PROVIDER_ID(WorkerP3)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:multi_qos_resulting_in_different_storages_spec(Path, [WorkerP1, WorkerP2], Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


multi_qos_resulting_in_the_same_storages(Config) ->
    Workers = [WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:multi_qos_resulting_in_the_same_storages_spec(Path, [WorkerP2, WorkerP3], Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


same_qos_multiple_times(Config) ->
    Workers = [WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:same_qos_multiple_times_spec(Path, [WorkerP1, WorkerP2, WorkerP3], Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


contrary_qos(Config) ->
    Workers = [WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP2, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:contrary_qos_spec(Path, [WorkerP2, WorkerP3], Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


multi_qos_where_one_cannot_be_satisfied(Config) ->
    Workers = [WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:multi_qos_where_one_cannot_be_satisfied_spec(Path, [WorkerP1, WorkerP3], Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


multi_qos_that_overlaps(Config) ->
    Workers = [WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    run_tests(Config, [file, dir], WorkerP1, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2), ?PROVIDER_ID(WorkerP3)],
        fun(Path, InitialDirStructure, ExpectedDirStructure) ->
            QosSpec = qos_test_base:multi_qos_that_overlaps_spec(Path, [WorkerP2, WorkerP3], Workers, get_provider_to_storage_mappings(Config)),
            #fulfill_qos_test_spec{
                initial_dir_structure = InitialDirStructure,
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_file_qos = QosSpec#qos_spec.expected_file_qos,
                expected_dir_structure = ExpectedDirStructure
            }
        end
    ).


%%%===================================================================
%%% Group of tests that creates directory structure, adds QoS on different
%%% levels of created structure and checks effective QoS and QoS docs.
%%%===================================================================

effective_qos_for_file_in_directory(Config) ->
    Workers = [WorkerP1, WorkerP2, Worker1P3, Worker2P3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    WorkersConfigurations = [
        [WorkerP1],
        [Worker1P3],
        [Worker2P3]
    ],

    lists:foreach(fun([Worker1] = WorkerConf) ->
        ct:pal("Starting for workers: ~p~n", [WorkerConf]),
        DirName = generator:gen_name(),
        DirPath = filename:join(?SPACE_PATH1, DirName),
        FilePath = filename:join(DirPath, <<"file1">>),

        QosSpec = qos_test_base:effective_qos_for_file_in_directory_spec(DirPath,
            FilePath, Worker1, Workers, get_provider_to_storage_mappings(Config)),
        add_qos_for_dir_and_check_effective_qos(Config,
            #effective_qos_test_spec{
                initial_dir_structure = #test_dir_structure{
                    worker = WorkerP1,
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"file1">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP1)]}
                        ]}
                    ]}
                },
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_effective_qos = QosSpec#qos_spec.expected_effective_qos,
                expected_dir_structure = #test_dir_structure{
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"file1">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)]}
                        ]}
                    ]}
                }
            }
        )
    end, WorkersConfigurations).


effective_qos_for_file_in_nested_directories(Config) ->
    Workers = [WorkerP1, WorkerP2, Worker1P3, Worker2P3, Worker3P3] = qos_tests_utils:get_op_nodes_sorted(Config),
    WorkersConfigurations = [
        [WorkerP1, WorkerP2, Worker1P3],
        [Worker1P3, Worker2P3, Worker3P3],
        [WorkerP1, Worker2P3, Worker3P3]
    ],

    lists:foreach(fun(WorkerConf) ->
        ct:pal("Starting for workers: ~p~n", [WorkerConf]),
        DirName = generator:gen_name(),
        Dir1Path = filename:join(?SPACE_PATH1, DirName),
        Dir2Path = filename:join(Dir1Path, <<"dir2">>),
        Dir3Path = filename:join(Dir2Path, <<"dir3">>),
        File21Path = filename:join(Dir2Path, <<"file21">>),
        File31Path = filename:join(Dir3Path, <<"file31">>),

        QosSpec = qos_test_base:effective_qos_for_file_in_nested_directories_spec(
            [Dir1Path, Dir2Path, Dir3Path], [File21Path, File31Path], WorkerConf, Workers, get_provider_to_storage_mappings(Config)
        ),
        add_qos_for_dir_and_check_effective_qos(Config,
            #effective_qos_test_spec{
                initial_dir_structure = #test_dir_structure{
                    worker = WorkerP1,
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"dir2">>, [
                                {<<"file21">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP1)]},
                                {<<"dir3">>, [
                                    {<<"file31">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP1)]}
                                ]}
                            ]}
                        ]}
                    ]}
                },
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_effective_qos = QosSpec#qos_spec.expected_effective_qos,
                expected_dir_structure = #test_dir_structure{
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"dir2">>, [
                                {<<"file21">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)]},
                                {<<"dir3">>, [
                                    {<<"file31">>, ?TEST_DATA, [
                                        ?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2), ?PROVIDER_ID(Worker1P3)
                                    ]}
                                ]}
                            ]}
                        ]}
                    ]}
                }
            }
        )
    end, WorkersConfigurations).


effective_qos_for_files_in_different_directories_of_tree_structure(Config) ->
    Workers = [WorkerP1, WorkerP2, Worker1P3, Worker2P3, Worker3P3] = qos_tests_utils:get_op_nodes_sorted(Config),
    WorkersConfigurations = [
        [WorkerP1, WorkerP2, Worker1P3],
        [Worker1P3, Worker2P3, Worker3P3],
        [WorkerP1, Worker2P3, Worker3P3]
    ],

    lists:foreach(fun(WorkerConf) ->
        ct:pal("Starting for workers: ~p~n", [WorkerConf]),
        DirName = generator:gen_name(),
        Dir1Path = filename:join(?SPACE_PATH1, DirName),
        Dir2Path = filename:join(Dir1Path, <<"dir2">>),
        Dir3Path = filename:join(Dir1Path, <<"dir3">>),
        File21Path = filename:join(Dir2Path, <<"file21">>),
        File31Path = filename:join(Dir3Path, <<"file31">>),
        QosSpec = qos_test_base:effective_qos_for_files_in_different_directories_of_tree_structure_spec(
            [Dir1Path, Dir2Path, Dir3Path], [File21Path, File31Path], WorkerConf, Workers, get_provider_to_storage_mappings(Config)
        ),

        add_qos_for_dir_and_check_effective_qos(Config,
            #effective_qos_test_spec{
                initial_dir_structure = #test_dir_structure{
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"dir2">>, [{<<"file21">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP1)]}]},
                            {<<"dir3">>, [{<<"file31">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP1)]}]}
                        ]}
                    ]}
                },
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_effective_qos = QosSpec#qos_spec.expected_effective_qos,
                expected_dir_structure = #test_dir_structure{
                    dir_structure = {?SPACE_PATH1, [
                        {DirName, [
                            {<<"dir2">>, [{<<"file21">>, ?TEST_DATA, [
                                ?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)]}
                            ]},
                            {<<"dir3">>, [{<<"file31">>, ?TEST_DATA, [
                                ?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(Worker1P3)]}
                            ]}
                        ]}
                    ]}
                }
            }
        )
    end, WorkersConfigurations).


%%%===================================================================
%%% QoS restoration tests
%%%===================================================================

qos_restoration_file_test(Config) ->
    basic_qos_restoration_test_base(Config, simple).

qos_restoration_dir_test(Config) ->
    basic_qos_restoration_test_base(Config, nested).


reconcile_qos_using_file_meta_posthooks_test(Config) ->
    [Worker1, Worker2 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    DirName = <<"dir1">>,
    DirPath = filename:join(?SPACE_PATH1, DirName),
    FilePath = filename:join(DirPath, <<"file1">>),

    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            worker = Worker1,
            dir_structure = {?SPACE_PATH1, [
                {DirName, []}
            ]}
        },
        qos_to_add = [
            #qos_to_add{
                worker = Worker1,
                qos_name = ?QOS1,
                path = DirPath,
                expression = <<"country=FR">>
            }
        ]
    },

    {_, _} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),

    mock_dbsync_changes(Config),
    mock_file_meta_posthooks(Config),

    Guid = qos_tests_utils:create_file(Worker1, SessId, FilePath, <<"test_data">>),

    DirStructureBefore = get_expected_structure_for_single_dir([?PROVIDER_ID(Worker1)]),
    DirStructureBefore2 = DirStructureBefore#test_dir_structure{assertion_workers = [Worker1]},
    qos_tests_utils:assert_distribution_in_dir_structure(
        Config, DirStructureBefore2, #{files => [{Guid, FilePath}], dirs => []}
    ),

    receive
        post_hook_created ->
            unmock_file_meta_posthooks(Config),
            unmock_dbsync_changes(Config)
    end,

    save_file_meta_docs(Config),

    DirStructureAfter = get_expected_structure_for_single_dir([?PROVIDER_ID(Worker1), ?PROVIDER_ID(Worker2)]),
    qos_tests_utils:assert_distribution_in_dir_structure(Config, DirStructureAfter,  #{files => [{Guid, FilePath}], dirs => []}).


%%%===================================================================
%%% QoS status tests
%%%===================================================================

qos_status_during_traverse_test(Config) ->
    qos_test_base:qos_status_during_traverse_test_base(Config, ?SPACE_ID, 8).

qos_status_during_traverse_with_file_deletion_test(Config) ->
    qos_test_base:qos_status_during_traverse_with_file_deletion_test_base(Config, ?SPACE_ID, 8).

qos_status_during_traverse_with_dir_deletion_test(Config) ->
    qos_test_base:qos_status_during_traverse_with_dir_deletion_test_base(Config, ?SPACE_ID, 1).

qos_status_during_reconciliation_test(Config) ->
    [Worker1 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    Filename = generator:gen_name(),
    DirStructure = ?nested_dir_structure(Filename, [?GET_DOMAIN_BIN(Worker1)]),
    qos_test_base:qos_status_during_reconciliation_test_base(Config, ?SPACE_ID, DirStructure, Filename).

qos_status_during_reconciliation_prefix_file_test(Config) ->
    [Worker1 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    Name = generator:gen_name(),
    DirStructure =
        {?SPACE_ID, [
            {Name, [
                {?filename(Name, 1), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]},
                {?filename(Name, 11), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]}
            ]}
        ]},
    
    qos_test_base:qos_status_during_reconciliation_test_base(Config, ?SPACE_ID, DirStructure, Name).

qos_status_during_reconciliation_with_file_deletion_test(Config) ->
    qos_test_base:qos_status_during_reconciliation_with_file_deletion_test_base(Config, ?SPACE_ID).

qos_status_during_reconciliation_with_dir_deletion_test(Config) ->
    qos_test_base:qos_status_during_reconciliation_with_dir_deletion_test_base(Config, ?SPACE_ID).

qos_status_during_traverse_file_without_qos_test(Config) ->
    qos_test_base:qos_status_during_traverse_file_without_qos_test_base(Config, ?SPACE_ID).

qos_status_after_failed_transfers(Config) ->
    [_Worker1, _Worker2, Worker3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    qos_test_base:qos_status_after_failed_transfer(Config, ?SPACE_ID, Worker3).

qos_status_after_failed_transfers_deleted_file(Config) ->
    [_Worker1, _Worker2, Worker3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    qos_test_base:qos_status_after_failed_transfer_deleted_file(Config, ?SPACE_ID, Worker3).

qos_status_after_failed_transfers_deleted_entry(Config) ->
    [_Worker1, _Worker2, Worker3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    qos_test_base:qos_status_after_failed_transfer_deleted_entry(Config, ?SPACE_ID, Worker3).


%%%===================================================================
%%% QoS reevaluate
%%%===================================================================

reevaluate_impossible_qos_test(Config) ->
    [Worker1, Worker2, _Worker3 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),

    DirName = <<"dir1">>,
    DirPath = filename:join(?SPACE_PATH1, DirName),
    FileName = <<"file1">>,

    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            worker = Worker2,
            dir_structure = {?SPACE_PATH1, [
                {DirName, [{FileName, ?TEST_DATA, [?PROVIDER_ID(Worker2)]}]}
            ]}
        },
        qos_to_add = [
            #qos_to_add{
                worker = Worker1,
                qos_name = ?QOS1,
                path = DirPath,
                expression = <<"country=PL">>,
                replicas_num = 2
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = Workers,
                qos_name = ?QOS1,
                file_key = {path, DirPath},
                replicas_num = 2,
                qos_expression = [<<"country=PL">>],
                possibility_check = {impossible, ?PROVIDER_ID(Worker1)}
            }
        ],
        wait_for_qos_fulfillment = []
    },

    {_GuidsAndPaths, QosNameIdMapping} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),

    ok = qos_tests_utils:set_qos_parameters(Worker2, #{<<"country">> => <<"PL">>}),
    % Impossible qos reevaluation is called after successful set_qos_parameters

    ExpectedQosEntriesAfter = [
        #expected_qos_entry{
            workers = Workers,
            qos_name = ?QOS1,
            file_key = {path, DirPath},
            replicas_num = 2,
            qos_expression = [<<"country=PL">>],
            possibility_check = {possible, ?PROVIDER_ID(Worker2)}
        }
    ],
    qos_tests_utils:wait_for_qos_fulfillment_in_parallel(Config, undefined, QosNameIdMapping, ExpectedQosEntriesAfter),

    ExpectedFileQos = [
        #expected_file_qos{
            workers = Workers,
            path = DirPath,
            qos_entries = [?QOS1],
            assigned_entries = #{
                initializer:get_storage_id(Worker1) => [?QOS1],
                initializer:get_storage_id(Worker2) => [?QOS1]
            }
        }
    ],
    qos_tests_utils:assert_file_qos_documents(Config, ExpectedFileQos, QosNameIdMapping, true, 10).


reevaluate_impossible_qos_race_test(Config) ->
    % this test checks appropriate handling of situation, when provider, on which entry was created,
    % do not have full knowledge of remote QoS parameters
    [Worker1, Worker2, _Worker3 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),

    DirName = <<"dir1">>,
    DirPath = filename:join(?SPACE_PATH1, DirName),
    FileName = <<"file1">>,

    ok = qos_tests_utils:set_qos_parameters(Worker2, #{<<"country">> => <<"other">>}),

    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            worker = Worker2,
            dir_structure = {?SPACE_PATH1, [
                {DirName, [{FileName, ?TEST_DATA, [?PROVIDER_ID(Worker2)]}]}
            ]}
        },
        qos_to_add = [
            #qos_to_add{
                worker = Worker1,
                qos_name = ?QOS1,
                path = DirPath,
                expression = <<"country=other">>
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = Workers,
                qos_name = ?QOS1,
                file_key = {path, DirPath},
                replicas_num = 1,
                qos_expression = [<<"country=other">>],
                possibility_check = {possible, ?PROVIDER_ID(Worker2)}
            }
        ],
        wait_for_qos_fulfillment = []
    },

    {_GuidsAndPaths, QosNameIdMapping} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),

    ExpectedFileQos = [
        #expected_file_qos{
            workers = Workers,
            path = DirPath,
            qos_entries = [?QOS1],
            assigned_entries = #{initializer:get_storage_id(Worker2) => [?QOS1]}
        }
    ],
    qos_tests_utils:assert_file_qos_documents(Config, ExpectedFileQos, QosNameIdMapping, true, 10).


reevaluate_impossible_qos_conflict_test(Config) ->
    % this test checks conflict resolution, when multiple providers mark entry as possible
    [Worker1, Worker2, _Worker3 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),

    DirName = <<"dir1">>,
    DirPath = filename:join(?SPACE_PATH1, DirName),
    FileName = <<"file1">>,

    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            worker = Worker2,
            dir_structure = {?SPACE_PATH1, [
                {DirName, [{FileName, ?TEST_DATA, [?PROVIDER_ID(Worker2)]}]}
            ]}
        },
        qos_to_add = [
            #qos_to_add{
                worker = Worker1,
                qos_name = ?QOS1,
                path = DirPath,
                expression = <<"country=other">>
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = Workers,
                qos_name = ?QOS1,
                file_key = {path, DirPath},
                replicas_num = 1,
                qos_expression = [<<"country=other">>],
                possibility_check = {impossible, ?PROVIDER_ID(Worker1)}
            }
        ],
        wait_for_qos_fulfillment = []
    },

    {_GuidsAndPaths, QosNameIdMapping} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),

    lists_utils:pforeach(fun(Worker) ->
        ok = qos_tests_utils:set_qos_parameters(Worker, #{<<"country">> => <<"other">>})
        % Impossible qos reevaluation is called after successful set_qos_parameters
    end, Workers),

    ExpectedQosEntriesAfter = [
        #expected_qos_entry{
            workers = Workers,
            qos_name = ?QOS1,
            file_key = {path, DirPath},
            replicas_num = 1,
            qos_expression = [<<"country=other">>],
            possibility_check = {possible, ?PROVIDER_ID(Worker1)}
        }
    ],
    qos_tests_utils:wait_for_qos_fulfillment_in_parallel(Config, undefined, QosNameIdMapping, ExpectedQosEntriesAfter),

    ExpectedFileQos = [
        #expected_file_qos{
            workers = Workers,
            path = DirPath,
            qos_entries = [?QOS1],
            assigned_entries = #{initializer:get_storage_id(Worker1) => [?QOS1]}
        }
    ],
    qos_tests_utils:assert_file_qos_documents(Config, ExpectedFileQos, QosNameIdMapping, true, 40).


%%%===================================================================
%%% QoS traverse tests
%%%===================================================================

qos_traverse_cancellation_test(Config) ->
    [Worker1, Worker2 | _] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    Name = generator:gen_name(),
    QosRootFilePath = filename:join([<<"/">>, ?SPACE_ID, Name]),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    
    DirStructure =
        {?SPACE_ID, [
            {Name, % Dir1
                [
                    {?filename(Name, 0), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]},
                    {?filename(Name, 1), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]},
                    {?filename(Name, 2), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]},
                    {?filename(Name, 3), ?TEST_DATA, [?GET_DOMAIN_BIN(Worker1)]}
                ]
            }
        ]},
    
    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = DirStructure
        },
        qos_to_add = [
            #qos_to_add{
                worker = Worker1,
                qos_name = ?QOS1,
                path = QosRootFilePath,
                expression = <<"country=PL">>
            }
        ],
        % do not wait for QoS fulfillment
        wait_for_qos_fulfillment = [],
        expected_qos_entries = [
            #expected_qos_entry{
                workers = Workers,
                qos_name = ?QOS1,
                file_key = {path, QosRootFilePath},
                qos_expression = [<<"country=PL">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(Worker1)}
            }
        ]
    },
    
    {GuidsAndPaths, QosNameIdMapping} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),
    [QosEntryId] = maps:values(QosNameIdMapping),
    
    DirGuid = qos_tests_utils:get_guid(QosRootFilePath, GuidsAndPaths),
    
    % create file and write to it on remote provider to trigger reconcile transfer
    {ok, {FileGuid, FileHandle}} = lfm_proxy:create_and_open(Worker2, SessId(Worker2), DirGuid, generator:gen_name(), 8#664),
    {ok, _} = lfm_proxy:write(Worker2, FileHandle, 0, <<"new_data">>),
    ok = lfm_proxy:close(Worker2, FileHandle),
    
    % wait for reconciliation transfer to start
    receive {qos_slave_job, _Pid, FileGuid} = Msg ->
        self() ! Msg
    end,
    
    % remove entry to trigger transfer cancellation
    lfm_proxy:remove_qos_entry(Worker1, SessId(Worker1), QosEntryId),
    
    % check that 5 transfers were cancelled (4 from traverse and 1 reconciliation)
    test_utils:mock_assert_num_calls(Worker1, replica_synchronizer, cancel, 1, 5, ?ATTEMPTS),
    
    % check that qos_entry document is deleted
    lists:foreach(fun(Worker) ->
        ?assertEqual(?ERROR_NOT_FOUND, rpc:call(Worker, qos_entry, get, [QosEntryId]), ?ATTEMPTS)
    end, Workers),
    
    % finish transfers to unlock waiting slave job processes
    ok = qos_tests_utils:finish_all_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)] ++ [FileGuid]).
    

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
        hackney:start(),
        NewConfig
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, transfers_test_utils, transfers_test_mechanism, qos_tests_utils, ?MODULE]}
        | Config
    ].


end_per_suite(Config) ->
    hackney:stop(),
    application:stop(ssl),
    initializer:unmock_auth_manager(Config).


init_per_testcase(Case, Config) when
    Case =:= qos_status_during_traverse_test;
    Case =:= qos_status_during_traverse_with_file_deletion_test;
    Case =:= qos_status_during_traverse_with_dir_deletion_test;
    Case =:= qos_status_during_reconciliation_test;
    Case =:= qos_status_during_reconciliation_prefix_file_test;
    Case =:= qos_status_during_reconciliation_with_file_deletion_test;
    Case =:= qos_status_during_reconciliation_with_dir_deletion_test;
    Case =:= qos_status_during_traverse_file_without_qos_test;
    Case =:= qos_status_after_failed_transfers;
    Case =:= qos_status_after_failed_transfers_deleted_file;
    Case =:= qos_status_after_failed_transfers_deleted_entry;
    Case =:= qos_status_after_failed_transfers; 
    Case =:= qos_traverse_cancellation_test ->
    
    Workers = ?config(op_worker_nodes, Config),
    qos_tests_utils:mock_transfers(Workers),
    init_per_testcase(default, Config);
init_per_testcase(_, Config) ->
    ct:timetrap(timer:minutes(10)),
    NewConfig = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(NewConfig),
    NewConfig.


end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    transfers_test_utils:unmock_replication_worker(Workers),
    transfers_test_utils:unmock_replica_synchronizer_failure(Workers),
    transfers_test_utils:remove_transfers(Config),
    transfers_test_utils:remove_all_views(Workers, ?SPACE_ID),
    transfers_test_utils:ensure_transfers_removed(Config),
    test_utils:mock_unload(Workers, providers_qos),
    initializer:clean_test_users_and_spaces_no_validate(Config).

%%%===================================================================
%%% Test bases
%%%===================================================================

add_qos_for_dir_and_check_effective_qos(Config, TestSpec) ->
    #effective_qos_test_spec{
        initial_dir_structure = InitialDirStructure,
        qos_to_add = QosToAddList,
        wait_for_qos_fulfillment = WaitForQosFulfillment,
        expected_qos_entries = ExpectedQosEntries,
        expected_effective_qos = ExpectedEffectiveQos
    } = TestSpec,

    % create initial dir structure
    GuidsAndPaths = qos_tests_utils:create_dir_structure(Config, InitialDirStructure),
    ?assertMatch(true, qos_tests_utils:assert_distribution_in_dir_structure(Config, InitialDirStructure, GuidsAndPaths)),

    % add QoS and wait for fulfillment
    QosNameIdMapping = qos_tests_utils:add_multiple_qos(Config, QosToAddList),
    qos_tests_utils:wait_for_qos_fulfillment_in_parallel(Config, WaitForQosFulfillment, QosNameIdMapping, ExpectedQosEntries),

    % check documents
    qos_tests_utils:assert_qos_entry_documents(Config, ExpectedQosEntries, QosNameIdMapping, ?ATTEMPTS),
    qos_tests_utils:assert_effective_qos(Config, ExpectedEffectiveQos, QosNameIdMapping, true).


basic_qos_restoration_test_base(Config, DirStructureType) ->
    [Worker1, _Worker2, Worker3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    SessionId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),

    Filename = generator:gen_name(),
    QosSpec = create_basic_qos_test_spec(Config, DirStructureType, Filename, undefined),
    {GuidsAndPaths, _} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),
    NewData = <<"new_test_data">>,
    StoragePaths = lists:map(fun({Guid, Path}) ->
        SpaceId = file_id:guid_to_space_id(Guid),
        % remove leading slash and space id
        [_, _ | PathTokens] = binary:split(Path, <<"/">>, [global]),
        StoragePath = storage_file_path(Worker3, SpaceId, filename:join(PathTokens)),
        ?assertEqual({ok, ?TEST_DATA}, read_file(Worker3, StoragePath)),
        {ok, FileHandle} = lfm_proxy:open(Worker1, SessionId1, {guid, Guid}, write),
        {ok, _} = lfm_proxy:write(Worker1, FileHandle, 0, NewData),
        ok = lfm_proxy:close(Worker1, FileHandle),
        StoragePath
    end, maps:get(files, GuidsAndPaths)),
    lists:foreach(fun(StoragePath) ->
        ?assertEqual({ok, NewData}, read_file(Worker3, StoragePath), ?ATTEMPTS)
    end, StoragePaths).


create_basic_qos_test_spec(Config, DirStructureType, QosFilename, WaitForQos) ->
    [Worker1, _Worker2, Worker3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    {DirStructure, DirStructureAfter} = case DirStructureType of
        simple ->
            {?simple_dir_structure(QosFilename, [?GET_DOMAIN_BIN(Worker1)]),
                ?simple_dir_structure(QosFilename, [?GET_DOMAIN_BIN(Worker1), ?GET_DOMAIN_BIN(Worker3)])};
        nested ->
            {?nested_dir_structure(QosFilename, [?GET_DOMAIN_BIN(Worker1)]),
                ?nested_dir_structure(QosFilename, [?GET_DOMAIN_BIN(Worker1), ?GET_DOMAIN_BIN(Worker3)])}
    end,

    #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = DirStructure
        },
        qos_to_add = [
            #qos_to_add{
                worker = Worker1,
                qos_name = ?QOS1,
                path = ?FILE_PATH(QosFilename),
                expression = <<"country=PT">>
            }
        ],
        wait_for_qos_fulfillment = WaitForQos,
        expected_qos_entries = [
            #expected_qos_entry{
                qos_name = ?QOS1,
                file_key = {path, ?FILE_PATH(QosFilename)},
                qos_expression = [<<"country=PT">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(Worker1)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                path = ?FILE_PATH(QosFilename),
                qos_entries = [?QOS1],
                assigned_entries = #{
                    initializer:get_storage_id(Worker3) => [?QOS1]
                }
            }
        ],
        expected_dir_structure = #test_dir_structure{
            dir_structure = DirStructureAfter
        }
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================

run_tests(Config, FileTypes, SourceProviderWorker, TargetProvidersWorkers, TestSpecFun) ->
    lists:foreach(fun(FileType) ->
        TestSpec = case FileType of
            file ->
                ct:pal("Starting for file"),
                InitialDirStructure = get_initial_structure_with_single_file(SourceProviderWorker),
                ExpectedDirStructure = get_expected_structure_for_single_file(TargetProvidersWorkers),
                TestSpecFun(?TEST_FILE_PATH, InitialDirStructure, ExpectedDirStructure);
            dir ->
                ct:pal("Starting for dir"),
                InitialDirStructure = get_initial_structure_with_single_dir(SourceProviderWorker),
                ExpectedDirStructure = get_expected_structure_for_single_dir(TargetProvidersWorkers),
                TestSpecFun(?TEST_DIR_PATH, InitialDirStructure, ExpectedDirStructure)
        end,
        qos_tests_utils:fulfill_qos_test_base(Config, TestSpec)
    end, FileTypes).


get_initial_structure_with_single_file(Worker) ->
    #test_dir_structure{
        worker = Worker,
        dir_structure = {?SPACE_ID, [
            {<<"file1">>, <<"test_data">>, [?PROVIDER_ID(Worker)]}
        ]}
    }.


get_expected_structure_for_single_file(ProviderIdList) ->
    #test_dir_structure{
        dir_structure = {?SPACE_ID, [
            {<<"file1">>, <<"test_data">>, ProviderIdList}
        ]}
    }.



get_initial_structure_with_single_dir(Worker) ->
    #test_dir_structure{
        worker = Worker,
        dir_structure = {?SPACE_ID, [
            {<<"dir1">>, [
                {<<"file1">>, <<"test_data">>, [?PROVIDER_ID(Worker)]}  
            ]}
        ]}
    }.


get_expected_structure_for_single_dir(ProviderIdList) ->
    #test_dir_structure{
        dir_structure = {?SPACE_ID, [
            {<<"dir1">>, [
                {<<"file1">>, <<"test_data">>, ProviderIdList}
            ]}
        ]}
    }.


storage_file_path(Worker, SpaceId, FilePath) ->
    SpaceMnt = get_space_mount_point(Worker, SpaceId),
    filename:join([SpaceMnt, SpaceId, FilePath]).


get_space_mount_point(Worker, SpaceId) ->
    StorageId = initializer:get_supporting_storage_id(Worker, SpaceId),
    storage_mount_point(Worker, StorageId).


storage_mount_point(Worker, StorageId) ->
    Helper = rpc:call(Worker, storage, get_helper, [StorageId]),
    HelperArgs = helper:get_args(Helper),
    maps:get(<<"mountPoint">>, HelperArgs).


read_file(Worker, FilePath) ->
    rpc:call(Worker, file, read_file, [FilePath]).


get_provider_to_storage_mappings(Config) ->
    [WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    #{
        ?P1 => initializer:get_storage_id(WorkerP1),
        ?P2 => initializer:get_storage_id(WorkerP2),
        ?P3 => initializer:get_storage_id(WorkerP3)
    }.


mock_dbsync_changes(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Worker1, dbsync_changes, [passthrough]),
    TestPid = self(),

    ok = test_utils:mock_expect(Worker1, dbsync_changes, apply,
        fun
            (Doc = #document{value = #file_meta{}}) ->
                TestPid ! {file_meta, Doc},
                ok;
            (Doc) ->
                meck:passthrough([Doc])
        end).


mock_file_meta_posthooks(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, file_meta_posthooks, [passthrough]),
    TestPid = self(),
    ok = test_utils:mock_expect(Workers, file_meta_posthooks, add_hook,
        fun(FileUuid, Identifier, Module, Function, Args) ->
            Res = meck:passthrough([FileUuid, Identifier, Module, Function, Args]),
            TestPid ! post_hook_created,
            Res
        end).


unmock_dbsync_changes(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker1, dbsync_changes).


unmock_file_meta_posthooks(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, file_meta_posthooks).


save_file_meta_docs(Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    receive
        {file_meta, Doc} ->
            ?assertMatch(ok, rpc:call(Worker1, dbsync_changes, apply, [Doc])),
            save_file_meta_docs(Config)
    after 0 ->
        ok
    end.
