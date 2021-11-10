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
    qos_status_after_failed_transfers_deleted_entry/1
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
    qos_status_after_failed_transfers_deleted_entry
].

-define(SPACE_ID, <<"space1">>).

%%%===================================================================
%%% Tests
%%%===================================================================

qos_status_during_traverse_test(Config) ->
    qos_test_base:qos_status_during_traverse_test_base(Config, ?SPACE_ID, 8).

qos_status_during_traverse_with_hardlinks_test(Config) ->
    qos_test_base:qos_status_during_traverse_with_hardlinks_test_base(Config, ?SPACE_ID).

qos_status_during_traverse_with_file_deletion_test(Config) ->
    qos_test_base:qos_status_during_traverse_with_file_deletion_test_base(Config, ?SPACE_ID, 8, reg_file).

qos_status_during_traverse_with_hardlink_deletion_test(Config) ->
    qos_test_base:qos_status_during_traverse_with_file_deletion_test_base(Config, ?SPACE_ID, 8, hardlink).

qos_status_during_traverse_with_file_and_hardlink_deletion_test(Config) ->
    qos_test_base:qos_status_during_traverse_with_file_deletion_test_base(Config, ?SPACE_ID, 16, random).

qos_status_during_traverse_with_dir_deletion_test(Config) ->
    qos_test_base:qos_status_during_traverse_with_dir_deletion_test_base(Config, ?SPACE_ID, 4, reg_file).

qos_status_during_traverse_with_dir_deletion_with_hardlinks_test(Config) ->
    qos_test_base:qos_status_during_traverse_with_dir_deletion_test_base(Config, ?SPACE_ID, 4, hardlink).

qos_status_during_traverse_with_dir_deletion_with_random_children_test(Config) ->
    qos_test_base:qos_status_during_traverse_with_dir_deletion_test_base(Config, ?SPACE_ID, 8, random).

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
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    qos_test_base:init_per_suite(Config).


end_per_suite(Config) ->
    qos_test_base:end_per_suite(Config).


init_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    qos_tests_utils:mock_transfers(Workers),
    qos_test_base:init_per_testcase(Config).

end_per_testcase(_Case, Config) ->
    qos_test_base:end_per_testcase(Config).


