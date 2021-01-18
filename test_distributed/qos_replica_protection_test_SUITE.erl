%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of protection of replicas created by QoS.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_replica_protection_test_SUITE).
-author("Michal Stanisz").

-include("transfers_test_mechanism.hrl").
-include("qos_tests_utils.hrl").
-include_lib("ctool/include/errors.hrl").

-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).

%% API
-export([
    autocleaning_of_replica_protected_by_qos_file/1,
    autocleaning_of_replica_not_protected_by_qos_file/1,
    autocleaning_of_replica_protected_by_qos_dir/1,
    autocleaning_of_replica_not_protected_by_qos_dir/1,

    eviction_of_replica_protected_by_qos_file/1,
    eviction_of_replica_not_protected_by_qos_file/1,
    migration_of_replica_protected_by_qos_file/1,
    migration_of_replica_not_protected_by_qos_file/1,
    migration_of_replica_protected_by_qos_on_equal_storage_file/1,
    eviction_of_replica_protected_by_qos_dir/1,
    eviction_of_replica_not_protected_by_qos_dir/1,
    migration_of_replica_protected_by_qos_dir/1,
    migration_of_replica_not_protected_by_qos_dir/1,
    migration_of_replica_protected_by_qos_on_equal_storage_dir/1,
    eviction_of_replica_protected_by_qos_dir_each_file_separately/1,
    eviction_of_replica_not_protected_by_qos_dir_each_file_separately/1,
    migration_of_replica_protected_by_qos_dir_each_file_separately/1,
    migration_of_replica_not_protected_by_qos_dir_each_file_separately/1,
    migration_of_replica_protected_by_qos_on_equal_storage_dir_each_file_separately/1,

    remote_eviction_of_replica_protected_by_qos_file/1,
    remote_eviction_of_replica_not_protected_by_qos_file/1,
    remote_migration_of_replica_protected_by_qos_file/1,
    remote_migration_of_replica_not_protected_by_qos_file/1,
    remote_migration_of_replica_protected_by_qos_on_equal_storage_file/1,
    remote_eviction_of_replica_protected_by_qos_dir/1,
    remote_eviction_of_replica_not_protected_by_qos_dir/1,
    remote_migration_of_replica_protected_by_qos_dir/1,
    remote_migration_of_replica_not_protected_by_qos_dir/1,
    remote_migration_of_replica_protected_by_qos_on_equal_storage_dir/1,
    remote_eviction_of_replica_protected_by_qos_dir_each_file_separately/1,
    remote_eviction_of_replica_not_protected_by_qos_dir_each_file_separately/1,
    remote_migration_of_replica_protected_by_qos_dir_each_file_separately/1,
    remote_migration_of_replica_not_protected_by_qos_dir_each_file_separately/1,
    remote_migration_of_replica_protected_by_qos_on_equal_storage_dir_each_file_separately/1
]).

all() -> [
    autocleaning_of_replica_protected_by_qos_file,
    autocleaning_of_replica_not_protected_by_qos_file,
    autocleaning_of_replica_protected_by_qos_dir,
    autocleaning_of_replica_not_protected_by_qos_dir,

    eviction_of_replica_protected_by_qos_file,
    eviction_of_replica_not_protected_by_qos_file,
    migration_of_replica_protected_by_qos_file,
    migration_of_replica_not_protected_by_qos_file,
    % TODO uncomment after resolving VFS-5715
%%    migration_of_replica_protected_by_qos_on_equal_storage_file,
    eviction_of_replica_protected_by_qos_dir,
    eviction_of_replica_not_protected_by_qos_dir,
    migration_of_replica_protected_by_qos_dir,
    migration_of_replica_not_protected_by_qos_dir,
    % TODO uncomment after resolving VFS-5715
%%    migration_of_replica_protected_by_qos_on_equal_storage_dir,
    eviction_of_replica_protected_by_qos_dir_each_file_separately,
    eviction_of_replica_not_protected_by_qos_dir_each_file_separately,
    migration_of_replica_protected_by_qos_dir_each_file_separately,
    migration_of_replica_not_protected_by_qos_dir_each_file_separately,
    % TODO uncomment after resolving VFS-5715
%%    migration_of_replica_protected_by_qos_on_equal_storage_dir_each_file_separately,

    remote_eviction_of_replica_protected_by_qos_file,
    remote_eviction_of_replica_not_protected_by_qos_file,
    remote_migration_of_replica_protected_by_qos_file,
    remote_migration_of_replica_not_protected_by_qos_file,
    % TODO uncomment after resolving VFS-5715
%%    remote_migration_of_replica_protected_by_qos_on_equal_storage_file,
    remote_eviction_of_replica_protected_by_qos_dir,
    remote_eviction_of_replica_not_protected_by_qos_dir,
    remote_migration_of_replica_protected_by_qos_dir,
    remote_migration_of_replica_not_protected_by_qos_dir,
    % TODO uncomment after resolving VFS-5715
%%    remote_migration_of_replica_protected_by_qos_on_equal_storage_dir,
    remote_eviction_of_replica_protected_by_qos_dir_each_file_separately,
    remote_eviction_of_replica_not_protected_by_qos_dir_each_file_separately,
    remote_migration_of_replica_protected_by_qos_dir_each_file_separately,
    remote_migration_of_replica_not_protected_by_qos_dir_each_file_separately
    % TODO uncomment after resolving VFS-5715
%%    remote_migration_of_replica_protected_by_qos_on_equal_storage_dir_each_file_separately
].

-define(PROVIDER_ID(Worker), initializer:domain_to_provider_id(?GET_DOMAIN(Worker))).
-define(SPACE_ID, <<"space1">>).
-define(FILE_PATH(FileName), filename:join(["/", ?SPACE_ID, FileName])).

% qos for test providers
-define(TEST_QOS(Val), #{
    <<"country">> => Val
}).

-define(Q1, <<"q1">>).

-record(test_spec_eviction, {
    evicting_node :: node(),
    % when remote_schedule is set to true, eviction is
    % scheduled on different worker than one creating qos
    remote_schedule = false :: boolean(),
    replicating_node = undefined :: node(),
    bytes_replicated = 0 :: non_neg_integer(),
    files_replicated = 0 :: non_neg_integer(),
    files_evicted = 0 :: non_neg_integer(),
    dir_structure_type = simple :: simple | nested,
    function = undefined,
    % used in tests when storages have equal qos.
    % This is needed so it is deterministic on
    % which node QoS will replicate files
    new_qos_params = undefined
}).

-record(test_spec_autocleaning, {
    run_node :: node(),
    dir_structure_type = simple :: simple | nested,
    released_bytes = 0 :: non_neg_integer(),
    bytes_to_release = 0 :: non_neg_integer(),
    files_number = 0 :: non_neg_integer()
}).


%%%===================================================================
%%% API
%%%===================================================================

eviction_of_replica_protected_by_qos_file(Config) ->
    [WorkerP1, _WorkerP2, _WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        files_evicted = 0,
        function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
    }).


eviction_of_replica_not_protected_by_qos_file(Config) ->
    [_WorkerP1, WorkerP2, _WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP2,
        files_evicted = 1,
        function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
    }).


migration_of_replica_protected_by_qos_file(Config) ->
    [WorkerP1, _WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        replicating_node = WorkerP3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 0,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
    }).


migration_of_replica_not_protected_by_qos_file(Config) ->
    [_WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP2,
        replicating_node = WorkerP3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 1,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
    }).


migration_of_replica_protected_by_qos_on_equal_storage_file(Config) ->
    [WorkerP1, _WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        replicating_node = WorkerP3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 1,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2,
        new_qos_params = #{
            initializer:get_storage_id(WorkerP3) => ?TEST_QOS(<<"PL">>)
        }
    }).


eviction_of_replica_protected_by_qos_dir(Config) ->
    [WorkerP1, _WorkerP2, _WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        files_evicted = 0,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:evict_root_directory/2
    }).


eviction_of_replica_not_protected_by_qos_dir(Config) ->
    [_WorkerP1, WorkerP2, _WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP2,
        files_evicted = 4,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:evict_root_directory/2
    }).


migration_of_replica_protected_by_qos_dir(Config) ->
    [WorkerP1, _WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        replicating_node = WorkerP3,
        bytes_replicated = 4*byte_size(?TEST_DATA),
        files_replicated = 4,
        files_evicted = 0,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_root_directory/2
    }).


migration_of_replica_not_protected_by_qos_dir(Config) ->
    [_WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP2,
        replicating_node = WorkerP3,
        bytes_replicated = 4*byte_size(?TEST_DATA),
        files_replicated = 4,
        files_evicted = 4,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_root_directory/2
    }).


migration_of_replica_protected_by_qos_on_equal_storage_dir(Config) ->
    [WorkerP1, _WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        replicating_node = WorkerP3,
        bytes_replicated = 4*byte_size(?TEST_DATA),
        files_replicated = 4,
        files_evicted = 4,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_root_directory/2,
        new_qos_params = #{
            initializer:get_storage_id(WorkerP3) => ?TEST_QOS(<<"PL">>)
        }
    }).


eviction_of_replica_protected_by_qos_dir_each_file_separately(Config) ->
    [WorkerP1, _WorkerP2, _WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        files_evicted = 0,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
    }).


eviction_of_replica_not_protected_by_qos_dir_each_file_separately(Config) ->
    [_WorkerP1, WorkerP2, _WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP2,
        files_evicted = 1,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
    }).


migration_of_replica_protected_by_qos_dir_each_file_separately(Config) ->
    [WorkerP1, _WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        replicating_node = WorkerP3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 0,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
    }).


migration_of_replica_not_protected_by_qos_dir_each_file_separately(Config) ->
    [_WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP2,
        replicating_node = WorkerP3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 1,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
    }).


migration_of_replica_protected_by_qos_on_equal_storage_dir_each_file_separately(Config) ->
    [WorkerP1, _WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        replicating_node = WorkerP3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 1,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2,
        new_qos_params = #{
            initializer:get_storage_id(WorkerP3) => ?TEST_QOS(<<"PL">>)
        }
    }).


remote_eviction_of_replica_protected_by_qos_file(Config) ->
    [WorkerP1, _WorkerP2, _WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        remote_schedule = true,
        files_evicted = 0,
        function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
    }).


remote_eviction_of_replica_not_protected_by_qos_file(Config) ->
    [_WorkerP1, WorkerP2, _WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP2,
        remote_schedule = true,
        files_evicted = 1,
        function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
    }).


remote_migration_of_replica_protected_by_qos_file(Config) ->
    [WorkerP1, _WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        remote_schedule = true,
        replicating_node = WorkerP3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 0,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
    }).


remote_migration_of_replica_not_protected_by_qos_file(Config) ->
    [_WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP2,
        remote_schedule = true,
        replicating_node = WorkerP3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 1,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
    }).


remote_migration_of_replica_protected_by_qos_on_equal_storage_file(Config) ->
    [WorkerP1, _WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        remote_schedule = true,
        replicating_node = WorkerP3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 1,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2,
        new_qos_params = #{
            initializer:get_storage_id(WorkerP3) => ?TEST_QOS(<<"PL">>)
        }
    }).


remote_eviction_of_replica_protected_by_qos_dir(Config) ->
    [WorkerP1, _WorkerP2, _WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        remote_schedule = true,
        files_evicted = 0,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:evict_root_directory/2
    }).


remote_eviction_of_replica_not_protected_by_qos_dir(Config) ->
    [_WorkerP1, WorkerP2, _WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP2,
        remote_schedule = true,
        files_evicted = 4,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:evict_root_directory/2
    }).


remote_migration_of_replica_protected_by_qos_dir(Config) ->
    [WorkerP1, _WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        remote_schedule = true,
        replicating_node = WorkerP3,
        bytes_replicated = 4*byte_size(?TEST_DATA),
        files_replicated = 4,
        files_evicted = 0,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_root_directory/2
    }).


remote_migration_of_replica_not_protected_by_qos_dir(Config) ->
    [_WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP2,
        remote_schedule = true,
        replicating_node = WorkerP3,
        bytes_replicated = 4*byte_size(?TEST_DATA),
        files_replicated = 4,
        files_evicted = 4,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_root_directory/2
    }).


remote_migration_of_replica_protected_by_qos_on_equal_storage_dir(Config) ->
    [WorkerP1, _WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        remote_schedule = true,
        replicating_node = WorkerP3,
        bytes_replicated = 4*byte_size(?TEST_DATA),
        files_replicated = 4,
        files_evicted = 4,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_root_directory/2,
        new_qos_params = #{
            initializer:get_storage_id(WorkerP3) => ?TEST_QOS(<<"PL">>)
        }
    }).


remote_eviction_of_replica_protected_by_qos_dir_each_file_separately(Config) ->
    [WorkerP1, _WorkerP2, _WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        remote_schedule = true,
        files_evicted = 0,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
    }).


remote_eviction_of_replica_not_protected_by_qos_dir_each_file_separately(Config) ->
    [_WorkerP1, WorkerP2, _WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP2,
        remote_schedule = true,
        files_evicted = 1,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
    }).


remote_migration_of_replica_protected_by_qos_dir_each_file_separately(Config) ->
    [WorkerP1, _WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        remote_schedule = true,
        replicating_node = WorkerP3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 0,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
    }).


remote_migration_of_replica_not_protected_by_qos_dir_each_file_separately(Config) ->
    [_WorkerP1, WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP2,
        remote_schedule = true,
        replicating_node = WorkerP3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 1,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
    }).


remote_migration_of_replica_protected_by_qos_on_equal_storage_dir_each_file_separately(Config) ->
    [WorkerP1, _WorkerP2, WorkerP3 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_node = WorkerP1,
        remote_schedule = true,
        replicating_node = WorkerP3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 1,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2,
        new_qos_params = #{
            initializer:get_storage_id(WorkerP3) => ?TEST_QOS(<<"PL">>)
        }
    }).


autocleaning_of_replica_protected_by_qos_file(Config) ->
    [WorkerP1 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_autocleaning_protection_test_base(Config, #test_spec_autocleaning{
        run_node = WorkerP1,
        dir_structure_type = simple,
        released_bytes = 0,
        bytes_to_release = byte_size(?TEST_DATA),
        files_number = 0
    }).


autocleaning_of_replica_not_protected_by_qos_file(Config) ->
    [_WorkerP1, WorkerP2 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_autocleaning_protection_test_base(Config, #test_spec_autocleaning{
        run_node = WorkerP2,
        dir_structure_type = simple,
        released_bytes = byte_size(?TEST_DATA),
        bytes_to_release = byte_size(?TEST_DATA),
        files_number = 1
    }).


autocleaning_of_replica_protected_by_qos_dir(Config) ->
    [WorkerP1 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_autocleaning_protection_test_base(Config, #test_spec_autocleaning{
        run_node = WorkerP1,
        dir_structure_type = nested,
        released_bytes = 0,
        bytes_to_release = 4*byte_size(?TEST_DATA),
        files_number = 0
    }).


autocleaning_of_replica_not_protected_by_qos_dir(Config) ->
    [_WorkerP1, WorkerP2 | _] = qos_tests_utils:get_op_nodes_sorted(Config),

    qos_autocleaning_protection_test_base(Config, #test_spec_autocleaning{
        run_node = WorkerP2,
        dir_structure_type = nested,
        released_bytes = 4*byte_size(?TEST_DATA),
        bytes_to_release = 4*byte_size(?TEST_DATA),
        files_number = 4
    }).


%%%===================================================================
%%% Tests bases
%%%===================================================================

qos_eviction_protection_test_base(Config, TestSpec) ->
    #test_spec_eviction{
        evicting_node = EvictingNode,
        remote_schedule = RemoteSchedule,
        replicating_node = ReplicatingNode,
        bytes_replicated = BytesReplicated,
        files_replicated = FilesReplicated,
        files_evicted = FilesEvicted,
        dir_structure_type = DirStructureType,
        function = Function,
        new_qos_params = NewQosParams
    } = TestSpec,
    [WorkerP1, WorkerP2, WorkerP3] = Workers = qos_tests_utils:get_op_nodes_sorted(Config),

    Filename = generator:gen_name(),
    QosSpec = create_basic_qos_test_spec(Config, DirStructureType, Filename),
    {GuidsAndPaths, _} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),

    case NewQosParams of
        undefined ->
            ok;
        _ ->
            lists:foreach(fun(Worker) ->
                maps:fold(fun(StorageId, Params, _) ->
                    rpc:call(Worker, storage, set_qos_parameters, [StorageId, Params])
                end, ok, NewQosParams)
            end, Workers)
    end,

    QosTransfers = transfers_test_utils:list_ended_transfers(WorkerP1, ?SPACE_ID),
    Config1 = Config ++ [{?OLD_TRANSFERS_KEY, [{<<"node">>, Tid, <<"guid">>, <<"path">>} || Tid <- QosTransfers]}],

    {ReplicationStatus, ReplicatingNodes, ReplicatingProvider} = case ReplicatingNode of
        undefined ->
            {skipped, [], undefined};
        _ ->
            {completed, [ReplicatingNode], transfers_test_utils:provider_id(ReplicatingNode)}
    end,

    ScheduleNode = case RemoteSchedule of
        true -> WorkerP1;
        false -> WorkerP2
    end,

    transfers_test_mechanism:run_test(
        Config1, #transfer_test_spec{
            setup = #setup{
                assertion_nodes = [WorkerP1, WorkerP2, WorkerP3],
                files_structure = {pre_created, GuidsAndPaths},
                root_directory = {qos_tests_utils:get_guid(?FILE_PATH(Filename), GuidsAndPaths), ?FILE_PATH(Filename)}
            },
            scenario = #scenario{
                type = rest,
                file_key_type = guid,
                schedule_node = ScheduleNode,
                evicting_nodes = [EvictingNode],
                replicating_nodes = ReplicatingNodes,
                function = Function
            },
            expected = #expected{
                expected_transfer = #{
                    replication_status => ReplicationStatus,
                    eviction_status => completed,
                    scheduling_provider => transfers_test_utils:provider_id(ScheduleNode),
                    evicting_provider => transfers_test_utils:provider_id(EvictingNode),
                    replicating_provider => ReplicatingProvider,
                    files_replicated => FilesReplicated,
                    bytes_replicated => BytesReplicated,
                    files_evicted => FilesEvicted
                },
                assertion_nodes = [WorkerP1, WorkerP2, WorkerP3]
            }
        }
    ).

qos_autocleaning_protection_test_base(Config, TestSpec) ->
    Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    #test_spec_autocleaning{
        run_node = RunNode,
        dir_structure_type = DirStructureType,
        released_bytes = ReleasedBytes,
        bytes_to_release = BytesToRelease,
        files_number = FilesNumber
    } = TestSpec,

    Name = <<"name">>,
    % remove possible remnants of previous test
    SessId = fun (Worker) -> ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config) end,
    lfm_proxy:rm_recursive(RunNode, SessId(RunNode), {path, <<"/", ?SPACE_ID/binary, "/", Name/binary>>}),
    lists:foreach(fun(Worker) ->
        ?assertEqual(
            {error, ?ENOENT},
            lfm_proxy:stat(Worker, SessId(Worker), {path, <<"/", ?SPACE_ID/binary, "/", Name/binary>>}),
            ?ATTEMPTS)
    end, Workers),

    rpc:call(RunNode, file_popularity_api, enable, [?SPACE_ID]),
    QosSpec = create_basic_qos_test_spec(Config, DirStructureType, Name),
    {_GuidsAndPaths, _} = qos_tests_utils:fulfill_qos_test_base(Config, QosSpec),

    Configuration =  #{
        enabled => true,
        target => 0,
        threshold => 100
    },
    rpc:call(RunNode, autocleaning_api, configure, [?SPACE_ID, Configuration]),
    {ok, ARId} = rpc:call(RunNode, autocleaning_api, force_run, [?SPACE_ID]),

    F = fun() ->
        {ok, #{stopped_at := StoppedAt}} = rpc:call(RunNode, autocleaning_api, get_run_report, [ARId]),
        StoppedAt
    end,
    % wait for auto-cleaning run to finish
    ?assertEqual(true, null =/= F(), ?ATTEMPTS),

    ?assertMatch({ok, #{
        released_bytes := ReleasedBytes,
        bytes_to_release := BytesToRelease,
        files_number := FilesNumber
    }}, rpc:call(RunNode, autocleaning_api, get_run_report, [ARId])).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
        end, ?config(op_worker_nodes, NewConfig)),

        application:start(ssl),
        hackney:start(),
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig, "env_desc.json"), NewConfig)
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, transfers_test_utils, transfers_test_mechanism, qos_tests_utils, ?MODULE]}
        | Config
    ].

init_per_testcase(_Case, Config) ->
    ct:timetrap(timer:minutes(60)),
    lfm_proxy:init(Config),
    case ?config(?SPACE_ID_KEY, Config) of
        undefined ->
            [{?SPACE_ID_KEY, ?SPACE_ID} | Config];
        _ ->
            Config
    end.

end_per_testcase(_Case, Config) ->
    Workers = qos_tests_utils:get_op_nodes_sorted(Config),
    transfers_test_utils:unmock_replication_worker(Workers),
    transfers_test_utils:unmock_replica_synchronizer_failure(Workers),
    transfers_test_utils:remove_transfers(Config),
    transfers_test_utils:ensure_transfers_removed(Config),
    test_utils:mock_unload(Workers, providers_qos).

end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================

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
                {?filename(Name, 2), ?TEST_DATA, Distribution}
            ]},
            {?filename(Name, 2), [
                {?filename(Name, 1), ?TEST_DATA, Distribution},
                {?filename(Name, 2), ?TEST_DATA, Distribution}
            ]}
        ]}
    ]}
).

create_basic_qos_test_spec(Config, DirStructureType, QosFilename) ->
    [WorkerP1, WorkerP2, _WorkerP3] = qos_tests_utils:get_op_nodes_sorted(Config),
    {DirStructure, DirStructureAfter} = case DirStructureType of
        simple ->
            {?simple_dir_structure(QosFilename, [?GET_DOMAIN_BIN(WorkerP2)]),
            ?simple_dir_structure(QosFilename, [?GET_DOMAIN_BIN(WorkerP1), ?GET_DOMAIN_BIN(WorkerP2)])};
        nested ->
            {?nested_dir_structure(QosFilename, [?GET_DOMAIN_BIN(WorkerP2)]),
            ?nested_dir_structure(QosFilename, [?GET_DOMAIN_BIN(WorkerP1), ?GET_DOMAIN_BIN(WorkerP2)])}
    end,

    #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            worker = WorkerP2,
            dir_structure = DirStructure
        },
        qos_to_add = [
            #qos_to_add{
                worker = WorkerP1,
                qos_name = ?QOS1,
                path = ?FILE_PATH(QosFilename),
                expression = <<"country=PL">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                qos_name = ?QOS1,
                file_key = {path, ?FILE_PATH(QosFilename)},
                qos_expression = [<<"country=PL">>],
                replicas_num = 1,
                possibility_check = {possible, ?GET_DOMAIN_BIN(WorkerP1)}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                path = ?FILE_PATH(QosFilename),
                qos_entries = [?QOS1],
                assigned_entries = #{initializer:get_storage_id(WorkerP1) => [?QOS1]}
            }
        ],
        expected_dir_structure = #test_dir_structure{
            dir_structure = DirStructureAfter
        }
    }.
