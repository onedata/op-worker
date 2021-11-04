%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains tests of QoS status during reconcilation.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_status_reconcilation_test_SUITE).
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
    qos_status_during_reconciliation_test/1,
    qos_status_during_reconciliation_prefix_file_test/1,
    qos_status_during_reconciliation_with_file_deletion_test/1,
    qos_status_during_reconciliation_with_hardlink_deletion_test/1,
    qos_status_during_reconciliation_with_dir_containing_reg_file_deletion_test/1,
    qos_status_during_reconciliation_with_dir_containing_hardlink_deletion_test/1
]).

all() -> [
    qos_status_during_reconciliation_test,
    qos_status_during_reconciliation_prefix_file_test,
    qos_status_during_reconciliation_with_file_deletion_test,
    qos_status_during_reconciliation_with_hardlink_deletion_test,
    qos_status_during_reconciliation_with_dir_containing_reg_file_deletion_test,
    qos_status_during_reconciliation_with_dir_containing_hardlink_deletion_test
].

-define(SPACE_ID, <<"space1">>).

%%%===================================================================
%%% Tests
%%%===================================================================

qos_status_during_reconciliation_test(Config) ->
    [Worker1 | _] = qos_tests_utils:get_op_nodes_sorted(Config),
    Filename = generator:gen_name(),
    DirStructure = ?nested_dir_structure(?SPACE_ID, Filename, [?GET_DOMAIN_BIN(Worker1)]),
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
    qos_test_base:qos_status_during_reconciliation_with_file_deletion_test_base(Config, ?SPACE_ID, 8, reg_file).

qos_status_during_reconciliation_with_hardlink_deletion_test(Config) ->
    qos_test_base:qos_status_during_reconciliation_with_file_deletion_test_base(Config, ?SPACE_ID, 8, hardlink).

qos_status_during_reconciliation_with_dir_containing_reg_file_deletion_test(Config) ->
    qos_test_base:qos_status_during_reconciliation_with_dir_deletion_test_base(Config, ?SPACE_ID, 8, reg_file).

qos_status_during_reconciliation_with_dir_containing_hardlink_deletion_test(Config) ->
    qos_test_base:qos_status_during_reconciliation_with_dir_deletion_test_base(Config, ?SPACE_ID, 8, hardlink).


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


