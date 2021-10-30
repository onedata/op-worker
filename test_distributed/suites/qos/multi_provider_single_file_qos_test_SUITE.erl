%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests for single file QoS management on
%%% multiple providers.
%%% @end
%%%--------------------------------------------------------------------
-module(multi_provider_single_file_qos_test_SUITE).
-author("Michal Cwiertnia").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("qos_tests_utils.hrl").
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
    multi_qos_that_overlaps/1
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
    multi_qos_that_overlaps
].


-define(FILE_PATH(FileName), filename:join([?SPACE_PATH1, FileName])).
-define(TEST_DIR_PATH, ?FILE_PATH(<<"dir1">>)).

-define(SPACE_ID, <<"space1">>).
-define(SPACE_PATH1, <<"/space1">>).

-define(PROVIDER_ID(Worker), ?GET_DOMAIN_BIN(Worker)).

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
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    qos_test_base:init_per_suite(Config).


end_per_suite(Config) ->
    qos_test_base:end_per_suite(Config).


init_per_testcase(qos_traverse_cancellation_test , Config) ->
    Workers = ?config(op_worker_nodes, Config),
    qos_tests_utils:mock_transfers(Workers),
    init_per_testcase(default, Config);
init_per_testcase(_, Config) ->
    qos_test_base:init_per_testcase(Config).


end_per_testcase(_, Config) ->
    qos_test_base:end_per_testcase(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================

run_tests(Config, FileTypes, SourceProviderWorker, TargetProvidersWorkers, TestSpecFun) ->
    lists:foreach(fun(FileType) ->
        TestSpec = case FileType of
            file ->
                ct:pal("Starting for file"),
                Filename = generator:gen_name(),
                InitialDirStructure = get_initial_structure_with_single_file(SourceProviderWorker, Filename),
                ExpectedDirStructure = get_expected_structure_for_single_file(TargetProvidersWorkers, Filename),
                TestSpecFun(?FILE_PATH(Filename), InitialDirStructure, ExpectedDirStructure);
            dir ->
                ct:pal("Starting for dir"),
                InitialDirStructure = get_initial_structure_with_single_dir(SourceProviderWorker),
                ExpectedDirStructure = get_expected_structure_for_single_dir(TargetProvidersWorkers),
                TestSpecFun(?TEST_DIR_PATH, InitialDirStructure, ExpectedDirStructure)
        end,
        qos_tests_utils:fulfill_qos_test_base(Config, TestSpec)
    end, FileTypes).


get_initial_structure_with_single_file(Worker, Filename) ->
    #test_dir_structure{
        worker = Worker,
        dir_structure = {?SPACE_ID, [
            {Filename, <<"test_data">>, [?PROVIDER_ID(Worker)]}
        ]}
    }.


get_expected_structure_for_single_file(ProviderIdList, Filename) ->
    #test_dir_structure{
        dir_structure = {?SPACE_ID, [
            {Filename, <<"test_data">>, ProviderIdList}
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


get_provider_to_storage_mappings(Config) ->
    [WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    #{
        ?P1 => initializer:get_storage_id(WorkerP1),
        ?P2 => initializer:get_storage_id(WorkerP2),
        ?P3 => initializer:get_storage_id(WorkerP3)
    }.