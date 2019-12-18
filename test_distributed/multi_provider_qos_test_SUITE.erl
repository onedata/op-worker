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
    qos_status_test/1,

    reconcile_qos_using_file_meta_posthooks_test/1
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
    qos_status_test,

    reconcile_qos_using_file_meta_posthooks_test
].


-define(FILE_PATH(FileName), filename:join([?SPACE1, FileName])).
-define(TEST_FILE_PATH, ?FILE_PATH(<<"file1">>)).
-define(TEST_DIR_PATH, ?FILE_PATH(<<"dir1">>)).
-define(TEST_DATA, <<"test_data">>).

-define(SPACE_ID, <<"space1">>).
-define(SPACE1, <<"/space1">>).

-define(PROVIDER_ID(Worker), initializer:domain_to_provider_id(?GET_DOMAIN(Worker))).
-define(GET_FILE_UUID(Worker, SessId, FilePath), qos_tests_utils:get_guid(Worker, SessId, FilePath)).

-define(ATTEMPTS, 60).

-define(simple_dir_structure(Name, Distribution),
    {?SPACE_ID, [
        {Name, ?TEST_DATA, Distribution}
    ]}
).
-define(filename(Name, Num), <<Name/binary,(integer_to_binary(Num))/binary>>).
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
            QosSpec = qos_test_base:simple_key_val_qos_spec(Path, WorkerP1, Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:qos_with_intersection_spec(Path, WorkerP1, Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:qos_with_complement_spec(Path, WorkerP1, Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:qos_with_union_spec(Path, WorkerP1, Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:qos_with_multiple_replicas_spec(Path, WorkerP1, Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:qos_with_intersection_and_union_spec(Path, WorkerP1, Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:qos_with_union_and_complement_spec(Path, WorkerP1, Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:qos_with_intersection_and_complement_spec(Path, WorkerP1, Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:qos_with_multiple_replicas_and_union_spec(Path, WorkerP1, Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:key_val_qos_that_cannot_be_fulfilled_spec(Path, WorkerP1, Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:qos_that_cannot_be_fulfilled_spec(Path, WorkerP1, Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:qos_with_parens_spec(Path, WorkerP1, Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:multi_qos_resulting_in_different_storages_spec(Path, [WorkerP1, WorkerP2], Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:multi_qos_resulting_in_the_same_storages_spec(Path, [WorkerP2, WorkerP3], Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:same_qos_multiple_times_spec(Path, [WorkerP1, WorkerP2, WorkerP3], Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:contrary_qos_spec(Path, [WorkerP2, WorkerP3], Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:multi_qos_where_one_cannot_be_satisfied_spec(Path, [WorkerP1, WorkerP3], Workers, get_providers_mapping(Config)),
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
            QosSpec = qos_test_base:multi_qos_that_overlaps_spec(Path, [WorkerP2, WorkerP3], Workers, get_providers_mapping(Config)),
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
        DirPath = filename:join(?SPACE1, DirName),
        FilePath = filename:join(DirPath, <<"file1">>),

        QosSpec = qos_test_base:effective_qos_for_file_in_directory_spec(DirPath,
            FilePath, Worker1, Workers, get_providers_mapping(Config)),
        add_qos_for_dir_and_check_effective_qos(Config,
            #effective_qos_test_spec{
                initial_dir_structure = #test_dir_structure{
                    worker = WorkerP1,
                    dir_structure = {?SPACE1, [
                        {DirName, [
                            {<<"file1">>, ?TEST_DATA, [?PROVIDER_ID(WorkerP1)]}
                        ]}
                    ]}
                },
                qos_to_add = QosSpec#qos_spec.qos_to_add,
                expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
                expected_effective_qos = QosSpec#qos_spec.expected_effective_qos,
                expected_dir_structure = #test_dir_structure{
                    dir_structure = {?SPACE1, [
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
        Dir1Path = filename:join(?SPACE1, DirName),
        Dir2Path = filename:join(Dir1Path, <<"dir2">>),
        Dir3Path = filename:join(Dir2Path, <<"dir3">>),
        File21Path = filename:join(Dir2Path, <<"file21">>),
        File31Path = filename:join(Dir3Path, <<"file31">>),

        QosSpec = qos_test_base:effective_qos_for_file_in_nested_directories_spec(
            [Dir1Path, Dir2Path, Dir3Path], [File21Path, File31Path], WorkerConf, Workers, get_providers_mapping(Config)
        ),
        add_qos_for_dir_and_check_effective_qos(Config,
            #effective_qos_test_spec{
                initial_dir_structure = #test_dir_structure{
                    worker = WorkerP1,
                    dir_structure = {?SPACE1, [
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
                    dir_structure = {?SPACE1, [
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
        Dir1Path = filename:join(?SPACE1, DirName),
        Dir2Path = filename:join(Dir1Path, <<"dir2">>),
        Dir3Path = filename:join(Dir1Path, <<"dir3">>),
        File21Path = filename:join(Dir2Path, <<"file21">>),
        File31Path = filename:join(Dir3Path, <<"file31">>),
        QosSpec = qos_test_base:effective_qos_for_files_in_different_directories_of_tree_structure_spec(
            [Dir1Path, Dir2Path, Dir3Path], [File21Path, File31Path], WorkerConf, Workers, get_providers_mapping(Config)
        ),

        add_qos_for_dir_and_check_effective_qos(Config,
            #effective_qos_test_spec{
                initial_dir_structure = #test_dir_structure{
                    dir_structure = {?SPACE1, [
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
                    dir_structure = {?SPACE1, [
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


qos_status_test(Config) ->
    Workers = [Worker1 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = fun(Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    Filename = generator:gen_name(),
    DirStructure = ?nested_dir_structure(Filename, [?GET_DOMAIN_BIN(Worker1)]),

    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = DirStructure
        },
        qos_to_add = [
            #qos_to_add{
                worker = Worker1,
                qos_name = ?QOS1,
                path = ?FILE_PATH(Filename),
                expression = <<"country=PT">>
            }
        ],
        % do not wait for QoS fulfillment
        wait_for_qos_fulfillment = []
    },

    {GuidsAndPaths, QosNameIdMapping} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),
    QosList = maps:values(QosNameIdMapping),

    lists:foreach(fun({Guid, Path}) ->
        lists:foreach(fun(Worker) ->
            ?assertEqual({ok, false}, lfm_proxy:check_qos_fulfilled(Worker, SessId(Worker), QosList, {guid, Guid}), ?ATTEMPTS),
            ?assertEqual({ok, false}, lfm_proxy:check_qos_fulfilled(Worker, SessId(Worker), QosList, {path, Path}), ?ATTEMPTS)
        end, Workers)
    end, maps:get(files, GuidsAndPaths)),

    finish_transfers([F || {F, _} <- maps:get(files, GuidsAndPaths)]),

    lists:foreach(fun({Guid, Path}) ->
        lists:foreach(fun(Worker) ->
            ?assertEqual({ok, true}, lfm_proxy:check_qos_fulfilled(Worker, SessId(Worker), QosList, {guid, Guid}), ?ATTEMPTS),
            ?assertEqual({ok, true}, lfm_proxy:check_qos_fulfilled(Worker, SessId(Worker), QosList, {path, Path}), ?ATTEMPTS)
        end, Workers)
    end, maps:get(files, GuidsAndPaths)),

    % check status after restoration
    FilesAndDirs = maps:get(files, GuidsAndPaths) ++ maps:get(dirs, GuidsAndPaths),

    IsAncestor = fun(A, F) ->
        str_utils:binary_starts_with(F, A)
    end,

    lists:foreach(fun({FileGuid, FilePath}) ->
        ct:print("writing to file ~p", [FilePath]),
        {ok, FileHandle} = lfm_proxy:open(Worker1, SessId(Worker1), {guid, FileGuid}, write),
        {ok, _} = lfm_proxy:write(Worker1, FileHandle, 0, <<"new_data">>),
        ok = lfm_proxy:close(Worker1, FileHandle),
        lists:foreach(fun(Worker) ->
            lists:foreach(fun({G, P}) ->
                ct:print("Checking ~p: ~n\tfile: ~p~n\tis_ancestor: ~p", [Worker, P, IsAncestor(P, FilePath)]),
                ?assertEqual(
                    {ok, not IsAncestor(P, FilePath)},
                    lfm_proxy:check_qos_fulfilled(Worker, SessId(Worker), QosList, {guid, G}),
                    ?ATTEMPTS
                ),
                ?assertEqual(
                    {ok, not IsAncestor(P, FilePath)},
                    lfm_proxy:check_qos_fulfilled(Worker, SessId(Worker), QosList, {path, P}),
                    ?ATTEMPTS
                )
            end, FilesAndDirs)
        end, Workers),
        finish_transfers([FileGuid]),
        lists:foreach(fun(Worker) ->
            lists:foreach(fun({G, P}) ->
                ?assertEqual({ok, true}, lfm_proxy:check_qos_fulfilled(Worker, SessId(Worker), QosList, {guid, G}), ?ATTEMPTS),
                ?assertEqual({ok, true}, lfm_proxy:check_qos_fulfilled(Worker, SessId(Worker), QosList, {path, P}), ?ATTEMPTS)
            end, FilesAndDirs)
        end, Workers)
    end, maps:get(files, GuidsAndPaths)).


reconcile_qos_using_file_meta_posthooks_test(Config) ->
    [Worker1, Worker2 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker1)}}, Config),
    DirName = <<"dir1">>,
    DirPath = filename:join(?SPACE1, DirName),
    FilePath = filename:join(DirPath, <<"file1">>),

    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            worker = Worker1,
            dir_structure = {?SPACE1, [
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
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?APP_NAME, prefetching, off)
        end, ?config(op_worker_nodes, NewConfig)),

        application:start(ssl),
        hackney:start(),
        NewConfig
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, transfers_test_utils, transfers_test_mechanism, qos_tests_utils, ?MODULE]}
        | Config
    ].


end_per_suite(_Config) ->
    hackney:stop(),
    application:stop(ssl).


init_per_testcase(qos_status_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, qos_traverse, [passthrough]),
    TestPid = self(),
    ok = test_utils:mock_expect(Workers, replica_synchronizer, synchronize,
        fun(_, FileCtx, _, _, _, _) ->
            FileGuid = file_ctx:get_guid_const(FileCtx),
            TestPid ! {qos_slave_job, self(), FileGuid},
            receive {completed, FileGuid} -> {ok, FileGuid} end
        end),
    init_per_testcase(default, Config);

init_per_testcase(_, Config) ->
    ct:timetrap(timer:minutes(10)),
    NewConfig = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(NewConfig),
    qos_tests_utils:mock_providers_qos(NewConfig, ?TEST_PROVIDERS_QOS),
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

    % add QoS and w8 for fulfillment
    QosNameIdMapping = qos_tests_utils:add_multiple_qos_in_parallel(Config, QosToAddList),
    qos_tests_utils:wait_for_qos_fulfilment_in_parallel(Config, WaitForQosFulfillment, QosNameIdMapping, ExpectedQosEntries),

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
                qos_expression_in_rpn = [<<"country=PT">>],
                replicas_num = 1
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                path = ?FILE_PATH(QosFilename),
                qos_entries = [?QOS1],
                assigned_entries = #{
                    ?PROVIDER_ID(Worker3) => [?QOS1]
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


finish_transfers([]) -> ok;
finish_transfers(Files) ->
    receive {qos_slave_job, Pid, FileGuid} ->
        Pid ! {completed, FileGuid},
        finish_transfers(lists:delete(FileGuid, Files))
    end.


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


get_providers_mapping(Config) ->
    [WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    #{
        ?P1 => ?PROVIDER_ID(WorkerP1),
        ?P2 => ?PROVIDER_ID(WorkerP2),
        ?P3 => ?PROVIDER_ID(WorkerP3)
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
