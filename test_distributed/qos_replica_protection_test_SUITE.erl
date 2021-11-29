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
-include_lib("onenv_ct/include/oct_background.hrl").

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

-define(SPACE_PLACEHOLDER, space1).
-define(SPACE_NAME, <<"space1">>).
-define(FILE_PATH(FileName), filename:join(["/", ?SPACE_NAME, FileName])).
-define(USER_PLACEHOLDER, user2).
-define(SESS_ID(ProviderPlaceholder), oct_background:get_user_session_id(?USER_PLACEHOLDER, ProviderPlaceholder)).

% qos for test providers
-define(TEST_QOS(Val), #{
    <<"country">> => Val
}).

-define(Q1, <<"q1">>).

-record(test_spec_eviction, {
    evicting_provider :: node(),
    % when remote_schedule is set to true, eviction is
    % scheduled on different worker than one creating qos
    remote_schedule = false :: boolean(),
    replicating_provider = undefined :: node(),
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
    run_provider :: node(),
    dir_structure_type = simple :: simple | nested,
    released_bytes = 0 :: non_neg_integer(),
    bytes_to_release = 0 :: non_neg_integer(),
    files_number = 0 :: non_neg_integer()
}).


%%%===================================================================
%%% API
%%%===================================================================

eviction_of_replica_protected_by_qos_file(Config) ->
    [Provider1 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        files_evicted = 0,
        function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
    }).


eviction_of_replica_not_protected_by_qos_file(Config) ->
    [_Provider1, Provider2, _Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider2,
        files_evicted = 1,
        function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
    }).


migration_of_replica_protected_by_qos_file(Config) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        replicating_provider = Provider3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 0,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
    }).


migration_of_replica_not_protected_by_qos_file(Config) ->
    [_Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider2,
        replicating_provider = Provider3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 1,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
    }).


migration_of_replica_protected_by_qos_on_equal_storage_file(Config) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    {ok, P3StorageId} = rpc:call(oct_background:get_random_provider_node(Provider3), space_logic,
        get_local_supporting_storage, [SpaceId]),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        replicating_provider = Provider3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 1,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2,
        new_qos_params = #{
            P3StorageId => ?TEST_QOS(<<"PL">>)
        }
    }).


eviction_of_replica_protected_by_qos_dir(Config) ->
    [Provider1, _Provider2, _Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        files_evicted = 0,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:evict_root_directory/2
    }).


eviction_of_replica_not_protected_by_qos_dir(Config) ->
    [_Provider1, Provider2, _Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider2,
        files_evicted = 4,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:evict_root_directory/2
    }).


migration_of_replica_protected_by_qos_dir(Config) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        replicating_provider = Provider3,
        bytes_replicated = 4*byte_size(?TEST_DATA),
        files_replicated = 4,
        files_evicted = 0,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_root_directory/2
    }).


migration_of_replica_not_protected_by_qos_dir(Config) ->
    [_Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider2,
        replicating_provider = Provider3,
        bytes_replicated = 4*byte_size(?TEST_DATA),
        files_replicated = 4,
        files_evicted = 4,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_root_directory/2
    }).


migration_of_replica_protected_by_qos_on_equal_storage_dir(Config) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    {ok, P3StorageId} = rpc:call(oct_background:get_random_provider_node(Provider3), space_logic,
        get_local_supporting_storage, [SpaceId]),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        replicating_provider = Provider3,
        bytes_replicated = 4*byte_size(?TEST_DATA),
        files_replicated = 4,
        files_evicted = 4,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_root_directory/2,
        new_qos_params = #{
            P3StorageId => ?TEST_QOS(<<"PL">>)
        }
    }).


eviction_of_replica_protected_by_qos_dir_each_file_separately(Config) ->
    [Provider1, _Provider2, _Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        files_evicted = 0,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
    }).


eviction_of_replica_not_protected_by_qos_dir_each_file_separately(Config) ->
    [_Provider1, Provider2, _Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider2,
        files_evicted = 1,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
    }).


migration_of_replica_protected_by_qos_dir_each_file_separately(Config) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        replicating_provider = Provider3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 0,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
    }).


migration_of_replica_not_protected_by_qos_dir_each_file_separately(Config) ->
    [_Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider2,
        replicating_provider = Provider3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 1,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
    }).


migration_of_replica_protected_by_qos_on_equal_storage_dir_each_file_separately(Config) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    {ok, P3StorageId} = rpc:call(oct_background:get_random_provider_node(Provider3), space_logic,
        get_local_supporting_storage, [SpaceId]),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        replicating_provider = Provider3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 1,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2,
        new_qos_params = #{
            P3StorageId => ?TEST_QOS(<<"PL">>)
        }
    }).


remote_eviction_of_replica_protected_by_qos_file(Config) ->
    [Provider1, _Provider2, _Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        remote_schedule = true,
        files_evicted = 0,
        function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
    }).


remote_eviction_of_replica_not_protected_by_qos_file(Config) ->
    [_Provider1, Provider2, _Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider2,
        remote_schedule = true,
        files_evicted = 1,
        function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
    }).


remote_migration_of_replica_protected_by_qos_file(Config) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        remote_schedule = true,
        replicating_provider = Provider3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 0,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
    }).


remote_migration_of_replica_not_protected_by_qos_file(Config) ->
    [_Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider2,
        remote_schedule = true,
        replicating_provider = Provider3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 1,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
    }).


remote_migration_of_replica_protected_by_qos_on_equal_storage_file(Config) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    {ok, P3StorageId} = rpc:call(oct_background:get_random_provider_node(Provider3), space_logic,
        get_local_supporting_storage, [SpaceId]),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        remote_schedule = true,
        replicating_provider = Provider3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 1,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2,
        new_qos_params = #{
            P3StorageId => ?TEST_QOS(<<"PL">>)
        }
    }).


remote_eviction_of_replica_protected_by_qos_dir(Config) ->
    [Provider1, _Provider2, _Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        remote_schedule = true,
        files_evicted = 0,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:evict_root_directory/2
    }).


remote_eviction_of_replica_not_protected_by_qos_dir(Config) ->
    [_Provider1, Provider2, _Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider2,
        remote_schedule = true,
        files_evicted = 4,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:evict_root_directory/2
    }).


remote_migration_of_replica_protected_by_qos_dir(Config) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        remote_schedule = true,
        replicating_provider = Provider3,
        bytes_replicated = 4*byte_size(?TEST_DATA),
        files_replicated = 4,
        files_evicted = 0,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_root_directory/2
    }).


remote_migration_of_replica_not_protected_by_qos_dir(Config) ->
    [_Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider2,
        remote_schedule = true,
        replicating_provider = Provider3,
        bytes_replicated = 4*byte_size(?TEST_DATA),
        files_replicated = 4,
        files_evicted = 4,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_root_directory/2
    }).


remote_migration_of_replica_protected_by_qos_on_equal_storage_dir(Config) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    {ok, P3StorageId} = rpc:call(oct_background:get_random_provider_node(Provider3), space_logic,
        get_local_supporting_storage, [SpaceId]),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        remote_schedule = true,
        replicating_provider = Provider3,
        bytes_replicated = 4*byte_size(?TEST_DATA),
        files_replicated = 4,
        files_evicted = 4,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_root_directory/2,
        new_qos_params = #{
            P3StorageId => ?TEST_QOS(<<"PL">>)
        }
    }).


remote_eviction_of_replica_protected_by_qos_dir_each_file_separately(Config) ->
    [Provider1, _Provider2, _Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        remote_schedule = true,
        files_evicted = 0,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
    }).


remote_eviction_of_replica_not_protected_by_qos_dir_each_file_separately(Config) ->
    [_Provider1, Provider2, _Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider2,
        remote_schedule = true,
        files_evicted = 1,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:evict_each_file_replica_separately/2
    }).


remote_migration_of_replica_protected_by_qos_dir_each_file_separately(Config) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        remote_schedule = true,
        replicating_provider = Provider3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 0,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
    }).


remote_migration_of_replica_not_protected_by_qos_dir_each_file_separately(Config) ->
    [_Provider1, Provider2, Provider3 | _] = oct_background:get_provider_ids(),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider2,
        remote_schedule = true,
        replicating_provider = Provider3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 1,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2
    }).


remote_migration_of_replica_protected_by_qos_on_equal_storage_dir_each_file_separately(Config) ->
    [Provider1, _Provider2, Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    {ok, P3StorageId} = rpc:call(oct_background:get_random_provider_node(Provider3), space_logic,
        get_local_supporting_storage, [SpaceId]),

    qos_eviction_protection_test_base(Config, #test_spec_eviction{
        evicting_provider = Provider1,
        remote_schedule = true,
        replicating_provider = Provider3,
        bytes_replicated = byte_size(?TEST_DATA),
        files_replicated = 1,
        files_evicted = 1,
        dir_structure_type = nested,
        function = fun transfers_test_mechanism:migrate_each_file_replica_separately/2,
        new_qos_params = #{
            P3StorageId => ?TEST_QOS(<<"PL">>)
        }
    }).


autocleaning_of_replica_protected_by_qos_file(Config) ->
    [Provider1 | _] = oct_background:get_provider_ids(),

    qos_autocleaning_protection_test_base(Config, #test_spec_autocleaning{
        run_provider = Provider1,
        dir_structure_type = simple,
        released_bytes = 0,
        bytes_to_release = byte_size(?TEST_DATA),
        files_number = 0
    }).


autocleaning_of_replica_not_protected_by_qos_file(Config) ->
    [_Provider1, Provider2, _Provider3 | _] = oct_background:get_provider_ids(),

    qos_autocleaning_protection_test_base(Config, #test_spec_autocleaning{
        run_provider = Provider2,
        dir_structure_type = simple,
        released_bytes = byte_size(?TEST_DATA),
        bytes_to_release = byte_size(?TEST_DATA),
        files_number = 1
    }).


autocleaning_of_replica_protected_by_qos_dir(Config) ->
    [Provider1, _Provider2, _Provider3 | _] = oct_background:get_provider_ids(),

    qos_autocleaning_protection_test_base(Config, #test_spec_autocleaning{
        run_provider = Provider1,
        dir_structure_type = nested,
        released_bytes = 0,
        bytes_to_release = 4*byte_size(?TEST_DATA),
        files_number = 0
    }).


autocleaning_of_replica_not_protected_by_qos_dir(Config) ->
    [_Provider1, Provider2, _Provider3 | _] = oct_background:get_provider_ids(),

    qos_autocleaning_protection_test_base(Config, #test_spec_autocleaning{
        run_provider = Provider2,
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
        evicting_provider = EvictingProvider,
        remote_schedule = RemoteSchedule,
        replicating_provider = ReplicatingProvider,
        bytes_replicated = BytesReplicated,
        files_replicated = FilesReplicated,
        files_evicted = FilesEvicted,
        dir_structure_type = DirStructureType,
        function = Function,
        new_qos_params = NewQosParamsPerProvider
    } = TestSpec,
    
    [Provider1, Provider2, _Provider3 | _] = oct_background:get_provider_ids(),
    Filename = generator:gen_name(),
    QosSpec = create_basic_qos_test_spec(DirStructureType, Filename),
    {GuidsAndPaths, _} = qos_tests_utils:fulfill_qos_test_base(QosSpec),

    case NewQosParamsPerProvider of
        undefined ->
            ok;
        _ ->
            maps:fold(fun(Provider, NewQosParams) ->
                maps:fold(fun(StorageId, Params, _) ->
                    rpc:call(oct_background:get_random_provider_node(Provider), storage, set_qos_parameters, [StorageId, Params])
                end, ok, NewQosParams)
            end, ok, NewQosParamsPerProvider)
    end,
    
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    QosTransfers = transfers_test_utils:list_ended_transfers(oct_background:get_random_provider_node(Provider1), SpaceId),
    Config1 = Config ++ [
        {?OLD_TRANSFERS_KEY, [{<<"node">>, Tid, <<"guid">>, <<"path">>} || Tid <- QosTransfers]},
        {{access_token, <<"user1">>}, oct_background:get_user_access_token(?USER_PLACEHOLDER)},
        {use_initializer, false},
        {?SPACE_ID_KEY, SpaceId}
    ],

    {ReplicationStatus, ReplicatingNodes} = case ReplicatingProvider of
        undefined ->
            {skipped, []};
        _ ->
            {completed, [oct_background:get_random_provider_node(ReplicatingProvider)]}
    end,

    ScheduleProvider = case RemoteSchedule of
        true -> Provider1;
        false -> Provider2
    end,
    UserId = oct_background:get_user_id(?USER_PLACEHOLDER),

    transfers_test_mechanism:run_test(
        Config1, #transfer_test_spec{
            setup = #setup{
                user = UserId,
                assertion_nodes = oct_background:get_all_providers_nodes(),
                files_structure = {pre_created, GuidsAndPaths},
                root_directory = {qos_tests_utils:get_guid(?FILE_PATH(Filename), GuidsAndPaths), ?FILE_PATH(Filename)}
            },
            scenario = #scenario{
                user = UserId,
                type = rest,
                file_key_type = guid,
                schedule_node = oct_background:get_random_provider_node(ScheduleProvider),
                evicting_nodes = [oct_background:get_random_provider_node(EvictingProvider)],
                replicating_nodes = ReplicatingNodes,
                function = Function
            },
            expected = #expected{
                user = UserId,
                expected_transfer = #{
                    replication_status => ReplicationStatus,
                    eviction_status => completed,
                    scheduling_provider => ScheduleProvider,
                    evicting_provider => EvictingProvider,
                    replicating_provider => ReplicatingProvider,
                    files_replicated => FilesReplicated,
                    bytes_replicated => BytesReplicated,
                    files_evicted => FilesEvicted
                },
                assertion_nodes = oct_background:get_all_providers_nodes()
            }
        }
    ).

qos_autocleaning_protection_test_base(Config, TestSpec) ->
    #test_spec_autocleaning{
        run_provider = RunProvider,
        dir_structure_type = DirStructureType,
        released_bytes = ReleasedBytes,
        bytes_to_release = BytesToRelease,
        files_number = FilesNumber
    } = TestSpec,
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    RunNode = oct_background:get_random_provider_node(RunProvider),

    Name = <<"name">>,
    % remove possible remnants of previous test
    lfm_proxy:rm_recursive(RunNode, ?SESS_ID(RunProvider), {path, <<"/", ?SPACE_NAME/binary, "/", Name/binary>>}),
    lists:foreach(fun(Provider) ->
        lists:foreach(fun(Node) ->
            ?assertEqual(
                {error, ?ENOENT},
                lfm_proxy:stat(Node, ?SESS_ID(Provider), {path, <<"/", ?SPACE_NAME/binary, "/", Name/binary>>}),
                ?ATTEMPTS)
        end, oct_background:get_provider_nodes(Provider))
    end, oct_background:get_provider_ids()),

    ok = rpc:call(RunNode, file_popularity_api, enable, [SpaceId]),
    QosSpec = create_basic_qos_test_spec(DirStructureType, Name),
    {_GuidsAndPaths, _} = qos_tests_utils:fulfill_qos_test_base(QosSpec),

    Configuration =  #{
        enabled => true,
        target => 0,
        threshold => 100
    },
    ok = rpc:call(RunNode, autocleaning_api, configure, [SpaceId, Configuration]),
    {ok, ARId} = rpc:call(RunNode, autocleaning_api, force_run, [SpaceId]),

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
    ExtendedConfig = [{?LOAD_MODULES, [qos_tests_utils, transfers_test_utils, transfers_test_mechanism]} | Config],
    oct_background:init_per_suite(ExtendedConfig, #onenv_test_config{
        onenv_scenario = "3op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {provider_token_ttl_sec, 24 * 60 * 60},
            {qos_retry_failed_files_interval_seconds, 5}
        ]}]
    }).

init_per_testcase(_Case, Config) ->
    qos_tests_utils:reset_qos_parameters(),
    lfm_proxy:init(Config),
    Config.

end_per_testcase(_Case, Config) ->
    Nodes = oct_background:get_all_providers_nodes(),
    transfers_test_utils:unmock_replication_worker(Nodes),
    transfers_test_utils:unmock_replica_synchronizer_failure(Nodes),
    transfers_test_utils:remove_transfers(Config),
    transfers_test_utils:ensure_transfers_removed(Config).

end_per_suite(_Config) ->
    oct_background:end_per_suite().


%%%===================================================================
%%% Internal functions
%%%===================================================================

-define(simple_dir_structure(Name, Distribution),
    {?SPACE_NAME, [
        {Name, ?TEST_DATA, Distribution}
    ]}
).
-define(nested_dir_structure(Name, Distribution),
    {?SPACE_NAME, [
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

create_basic_qos_test_spec(DirStructureType, QosFilename) ->
    [Provider1, Provider2, _Provider3 | _] = oct_background:get_provider_ids(),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),
    {ok, P1StorageId} = rpc:call(oct_background:get_random_provider_node(Provider1), space_logic,
        get_local_supporting_storage, [SpaceId]),
    {DirStructure, DirStructureAfter} = case DirStructureType of
        simple ->
            {?simple_dir_structure(QosFilename, [Provider2]),
            ?simple_dir_structure(QosFilename, [Provider1, Provider2])};
        nested ->
            {?nested_dir_structure(QosFilename, [Provider2]),
            ?nested_dir_structure(QosFilename, [Provider1, Provider2])}
    end,

    #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            provider = Provider2,
            dir_structure = DirStructure
        },
        qos_to_add = [
            #qos_to_add{
                provider = Provider1,
                qos_name = ?QOS1,
                path = ?FILE_PATH(QosFilename),
                expression = <<"providerId=", Provider1/binary>>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                qos_name = ?QOS1,
                file_key = {path, ?FILE_PATH(QosFilename)},
                qos_expression = [<<"providerId=", Provider1/binary>>],
                replicas_num = 1,
                possibility_check = {possible, Provider1}
            }
        ],
        expected_file_qos = [
            #expected_file_qos{
                path = ?FILE_PATH(QosFilename),
                qos_entries = [?QOS1],
                assigned_entries = #{P1StorageId => [?QOS1]}
            }
        ],
        expected_dir_structure = #test_dir_structure{
            dir_structure = DirStructureAfter
        }
    }.
