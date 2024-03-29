%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains tests of QoS status.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_status_test_SUITE).
-author("Michal Stanisz").

-include("qos_tests_utils.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    qos_status_during_traverse_test/1,
    qos_status_during_traverse_with_hardlinks_test/1,
    qos_status_during_traverse_with_file_deletion_test/1,
    qos_status_during_traverse_with_hardlink_deletion_test/1,
    qos_status_during_traverse_with_file_and_hardlink_deletion_test/1,
    qos_status_during_traverse_with_dir_deletion_test/1,
    qos_status_during_traverse_with_dir_deletion_with_hardlinks_test/1,
    qos_status_during_traverse_with_dir_deletion_with_random_children_test/1,
    qos_status_during_traverse_file_without_qos_test/1,
    qos_status_after_failed_transfers/1,
    qos_status_after_failed_transfers_deleted_file/1,
    qos_status_after_failed_transfers_deleted_entry/1,
    
    qos_status_during_reconciliation_test/1,
    qos_status_during_reconciliation_prefix_file_test/1,
    qos_status_during_reconciliation_with_file_deletion_test/1,
    qos_status_during_reconciliation_with_hardlink_deletion_test/1,
    qos_status_during_reconciliation_with_dir_containing_reg_file_deletion_test/1,
    qos_status_during_reconciliation_with_dir_containing_hardlink_deletion_test/1
]).

all() -> [
    qos_status_during_traverse_test,
    qos_status_during_traverse_with_hardlinks_test,
    qos_status_during_traverse_with_file_deletion_test,
    qos_status_during_traverse_with_hardlink_deletion_test,
    qos_status_during_traverse_with_file_and_hardlink_deletion_test,
    qos_status_during_traverse_with_dir_deletion_test,
    qos_status_during_traverse_with_dir_deletion_with_hardlinks_test,
    qos_status_during_traverse_with_dir_deletion_with_random_children_test,
    qos_status_during_traverse_file_without_qos_test,
    qos_status_after_failed_transfers,
    qos_status_after_failed_transfers_deleted_file,
    qos_status_after_failed_transfers_deleted_entry,
    qos_status_during_reconciliation_test,
    qos_status_during_reconciliation_prefix_file_test,
    qos_status_during_reconciliation_with_file_deletion_test,
    qos_status_during_reconciliation_with_hardlink_deletion_test,
    qos_status_during_reconciliation_with_dir_containing_reg_file_deletion_test,
    qos_status_during_reconciliation_with_dir_containing_hardlink_deletion_test
].

-define(SPACE_NAME, <<"space1">>).

%%%===================================================================
%%% Tests
%%%===================================================================

qos_status_during_traverse_test(_Config) ->
    qos_test_base:qos_status_during_traverse_test_base(8).

qos_status_during_traverse_with_hardlinks_test(_Config) ->
    qos_test_base:qos_status_during_traverse_with_hardlinks_test_base().

qos_status_during_traverse_with_file_deletion_test(_Config) ->
    qos_test_base:qos_status_during_traverse_with_file_deletion_test_base(8, reg_file).

qos_status_during_traverse_with_hardlink_deletion_test(_Config) ->
    qos_test_base:qos_status_during_traverse_with_file_deletion_test_base(8, hardlink).

qos_status_during_traverse_with_file_and_hardlink_deletion_test(_Config) ->
    qos_test_base:qos_status_during_traverse_with_file_deletion_test_base(16, random).

qos_status_during_traverse_with_dir_deletion_test(_Config) ->
    qos_test_base:qos_status_during_traverse_with_dir_deletion_test_base(4, reg_file).

qos_status_during_traverse_with_dir_deletion_with_hardlinks_test(_Config) ->
    qos_test_base:qos_status_during_traverse_with_dir_deletion_test_base(4, hardlink).

qos_status_during_traverse_with_dir_deletion_with_random_children_test(_Config) ->
    qos_test_base:qos_status_during_traverse_with_dir_deletion_test_base(8, random).

qos_status_during_traverse_file_without_qos_test(_Config) ->
    qos_test_base:qos_status_during_traverse_file_without_qos_test_base().

qos_status_after_failed_transfers(_Config) ->
    [_Provider1, Provider2 | _] = oct_background:get_provider_ids(),
    qos_test_base:qos_status_after_failed_transfer(Provider2).

qos_status_after_failed_transfers_deleted_file(_Config) ->
    [_Provider1, Provider2 | _] = oct_background:get_provider_ids(),
    qos_test_base:qos_status_after_failed_transfer_deleted_file(Provider2).

qos_status_after_failed_transfers_deleted_entry(_Config) ->
    [_Provider1, Provider2 | _] = oct_background:get_provider_ids(),
    qos_test_base:qos_status_after_failed_transfer_deleted_entry(Provider2).

qos_status_during_reconciliation_test(_Config) ->
    [Provider1 | _] = oct_background:get_provider_ids(),
    Filename = generator:gen_name(),
    DirStructure = ?nested_dir_structure(?SPACE_NAME, Filename, [Provider1]),
    qos_test_base:qos_status_during_reconciliation_test_base(DirStructure, Filename).

qos_status_during_reconciliation_prefix_file_test(_Config) ->
    [Provider1 | _] = oct_background:get_provider_ids(),
    Name = generator:gen_name(),
    DirStructure =
        {?SPACE_NAME, [
            {Name, [
                {?filename(Name, 1), ?TEST_DATA, [Provider1]},
                {?filename(Name, 11), ?TEST_DATA, [Provider1]}
            ]}
        ]},
    
    qos_test_base:qos_status_during_reconciliation_test_base(DirStructure, Name).

qos_status_during_reconciliation_with_file_deletion_test(_Config) ->
    qos_test_base:qos_status_during_reconciliation_with_file_deletion_test_base(8, reg_file).

qos_status_during_reconciliation_with_hardlink_deletion_test(_Config) ->
    qos_test_base:qos_status_during_reconciliation_with_file_deletion_test_base(8, hardlink).

qos_status_during_reconciliation_with_dir_containing_reg_file_deletion_test(_Config) ->
    qos_test_base:qos_status_during_reconciliation_with_dir_deletion_test_base(8, reg_file).

qos_status_during_reconciliation_with_dir_containing_hardlink_deletion_test(_Config) ->
    qos_test_base:qos_status_during_reconciliation_with_dir_deletion_test_base(8, hardlink).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite([{?LOAD_MODULES, [?MODULE, qos_tests_utils, dir_stats_test_utils]} | Config],
        #onenv_test_config{
            onenv_scenario = "2op",
            envs = [{op_worker, op_worker, [
                {fuse_session_grace_period_seconds, 24 * 60 * 60},
                {provider_token_ttl_sec, 24 * 60 * 60},
                {qos_retry_failed_files_interval_seconds, 5}
            ]}]
        }).


end_per_suite(Config) ->
    oct_background:end_per_suite(),
    dir_stats_test_utils:enable_stats_counting(Config).


init_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    qos_tests_utils:mock_transfers(Workers),
    lfm_proxy:init(Config),
    Config.

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    qos_tests_utils:finish_all_transfers(),
    test_utils:mock_unload(Workers, replica_synchronizer),
    lfm_proxy:teardown(Config).


