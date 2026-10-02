%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests replica migration transfers - a replication followed by
%%% the eviction of the source replica. All of its cases are the ones shared
%%% by every transfer type (transfer_tests), run against a migration ctx;
%%% migration adds no case of its own, as everything specific to either of its
%%% halves is covered by transfer_replication_test_SUITE and
%%% transfer_eviction_test_SUITE.
%%%
%%% Deliberately not covered here: the transfer REST/GraphSync API and its
%%% authorization (api_transfer_test_SUITE), and transfers surviving a
%%% provider restart (transfer_restarts_test_SUITE).
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_migration_test_SUITE).
-author("Bartosz Walkowicz").

-include("transfers/transfer_test.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0, groups/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    empty_dir_test/1,
    tree_of_empty_dirs_test/1,
    regular_file_test/1,
    file_in_directory_test/1,
    big_file_test/1,

    many_files_in_one_transfer_test/1,
    many_files_in_separate_transfers_test/1,

    transfer_despite_protection_flags_test/1,

    regular_file_by_view_test/1,
    files_matched_by_view_with_reduce_test/1,
    transfer_by_not_existing_view_test/1,
    transfer_by_view_emitting_invalid_file_id_test/1,
    transfer_by_view_emitting_not_existing_file_id_test/1,
    transfer_by_empty_view_test/1,
    transfer_by_view_with_not_matching_key_test/1,
    many_files_by_view_test/1,
    many_files_by_view_with_batch_10_test/1,

    cancel_ongoing_transfer_test/1,
    file_removed_during_transfer_test/1,

    rerun_failed_file_transfer_test/1,
    rerun_failed_file_transfer_by_other_user_test/1,
    rerun_failed_dir_transfer_test/1,
    rerun_failed_view_transfer_test/1,
    many_simultaneous_failed_transfers_test/1
]).

groups() -> [
    {file_tree_shape_tests, [], [
        empty_dir_test,
        tree_of_empty_dirs_test,
        regular_file_test,
        file_in_directory_test,
        big_file_test
    ]},
    {scale_tests, [], [
        many_files_in_one_transfer_test,
        many_files_in_separate_transfers_test
    ]},
    {protection_flag_tests, [], [
        transfer_despite_protection_flags_test
    ]},
    {view_transfer_tests, [], [
        regular_file_by_view_test,
        files_matched_by_view_with_reduce_test,
        transfer_by_not_existing_view_test,
        transfer_by_view_emitting_invalid_file_id_test,
        transfer_by_view_emitting_not_existing_file_id_test,
        transfer_by_empty_view_test,
        transfer_by_view_with_not_matching_key_test,
        many_files_by_view_test,
        many_files_by_view_with_batch_10_test
    ]},
    {interrupted_transfer_tests, [], [
        cancel_ongoing_transfer_test,
        file_removed_during_transfer_test
    ]},
    {failed_transfer_tests, [], [
        rerun_failed_file_transfer_test,
        rerun_failed_file_transfer_by_other_user_test,
        rerun_failed_dir_transfer_test,
        rerun_failed_view_transfer_test,
        many_simultaneous_failed_transfers_test
    ]}
].

all() -> [
    {group, file_tree_shape_tests},
    {group, scale_tests},
    {group, protection_flag_tests},
    {group, view_transfer_tests},
    {group, interrupted_transfer_tests},
    {group, failed_transfer_tests}
].

-define(SUITE_CTX, #transfer_test_suite_ctx{
    transfer_type = migration,
    space_selector = space_krk_par_p,
    user_selector = user1,
    creation_provider_selector = krakow,
    other_provider_selector = paris
}).
-define(run_test(), transfer_tests:?FUNCTION_NAME(?SUITE_CTX)).


%%%==================================================================
%%% Test functions
%%%===================================================================


%% --- file tree shapes ---


empty_dir_test(_Config) -> ?run_test().
tree_of_empty_dirs_test(_Config) -> ?run_test().
regular_file_test(_Config) -> ?run_test().
file_in_directory_test(_Config) -> ?run_test().
big_file_test(_Config) -> ?run_test().


%% --- scale ---


many_files_in_one_transfer_test(_Config) -> ?run_test().
many_files_in_separate_transfers_test(_Config) -> ?run_test().


%% --- protection flags ---


transfer_despite_protection_flags_test(_Config) -> ?run_test().


%% --- transfers by view ---


regular_file_by_view_test(_Config) -> ?run_test().
files_matched_by_view_with_reduce_test(_Config) -> ?run_test().
transfer_by_not_existing_view_test(_Config) -> ?run_test().
transfer_by_view_emitting_invalid_file_id_test(_Config) -> ?run_test().
transfer_by_view_emitting_not_existing_file_id_test(_Config) -> ?run_test().
transfer_by_empty_view_test(_Config) -> ?run_test().
transfer_by_view_with_not_matching_key_test(_Config) -> ?run_test().
many_files_by_view_test(_Config) -> ?run_test().
many_files_by_view_with_batch_10_test(_Config) -> ?run_test().


%% --- interrupted transfers ---


cancel_ongoing_transfer_test(_Config) -> ?run_test().
file_removed_during_transfer_test(_Config) -> ?run_test().


%% --- failed transfers ---


rerun_failed_file_transfer_test(_Config) -> ?run_test().
rerun_failed_file_transfer_by_other_user_test(_Config) -> ?run_test().
rerun_failed_dir_transfer_test(_Config) -> ?run_test().
rerun_failed_view_transfer_test(_Config) -> ?run_test().
many_simultaneous_failed_transfers_test(_Config) -> ?run_test().


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, transfer_test_utils, transfer_tests, permit_gate_test_utils],
    opt:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "2op",
        envs = [
            {op_worker, op_worker, [
                {fuse_session_grace_period_seconds, 24 * 60 * 60},
                {provider_token_ttl_sec, 24 * 60 * 60},
                % transfer status updates are sparse single-doc changes - with the
                % default (5s) broadcast interval they idle in the dbsync out-stream
                % aggregation window for several of its cycles, inflating every
                % cross-provider await by tens of seconds
                {dbsync_changes_broadcast_interval, 1000}
            ]},
            {op_worker, cluster_worker, [
                {cache_to_disk_delay_ms, timer:seconds(1)},
                {cache_to_disk_force_delay_ms, timer:seconds(2)}
            ]}
        ]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(Case, Config) ->
    transfer_tests:init_per_testcase(Case, ?SUITE_CTX, Config).


end_per_testcase(Case, Config) ->
    transfer_tests:end_per_testcase(Case, ?SUITE_CTX, Config).
