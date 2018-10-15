%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of replication jobs, scheduled via REST.
%%% @end
%%%-------------------------------------------------------------------
-module(replication_transfers_rest_test_SUITE).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([
    replicate_empty_dir_by_path/1,
    replicate_empty_dir_by_guid/1,
    replicate_tree_of_empty_dirs_by_guid/1,
    replicate_tree_of_empty_dirs_by_path/1,
    replicate_regular_file_by_guid/1,
    replicate_regular_file_by_path/1,
    replicate_file_in_directory_by_guid/1,
    replicate_file_in_directory_by_path/1,
    replicate_big_file/1,
    schedule_replication_to_source_provider/1,
    replicate_already_replicated_file/1,
    not_synced_file_should_not_be_replicated/1,
    replicate_100_files_separately/1,
    replicate_100_files_in_one_transfer/1,
    replication_should_succeed_when_there_is_enough_space_for_file/1,
    replication_should_fail_when_space_is_full/1,
    replicate_to_missing_provider_by_guid/1,
    replicate_to_missing_provider_by_path/1,
    replicate_to_not_supporting_provider_by_guid/1,
    replicate_to_not_supporting_provider_by_path/1,
    schedule_replication_on_not_supporting_provider_by_guid/1,
    schedule_replication_on_not_supporting_provider_by_path/1,
    cancel_replication_on_target_nodes/1,
    file_replication_failures_should_fail_whole_transfer/1,
    many_simultaneous_failed_transfers/1,
    schedule_replication_of_regular_file_by_index/1,
    schedule_replication_of_regular_file_by_index2/1,
    schedule_replication_of_regular_file_by_index_with_reduce/1,
    scheduling_replication_by_not_existing_index_should_fail/1,
    scheduling_replication_by_index_with_function_returning_wrong_value_should_fail/1,
    scheduling_replication_by_index_returning_not_existing_file_should_fail/1,
    scheduling_replication_by_empty_index_should_succeed/1,
    scheduling_replication_by_not_existing_key_in_index_should_succeed/1,
    schedule_replication_of_100_regular_files_by_index_with_batch_1000/1,
    schedule_replication_of_100_regular_files_by_index_with_batch_100/1,
    schedule_replication_of_100_regular_files_by_index_with_batch_10/1
]).

all() -> [
    replicate_empty_dir_by_guid,
    replicate_empty_dir_by_path,
    replicate_tree_of_empty_dirs_by_guid,
    replicate_tree_of_empty_dirs_by_path,
    replicate_regular_file_by_guid,
    replicate_regular_file_by_path,
    replicate_file_in_directory_by_guid,
    replicate_file_in_directory_by_path,
    replicate_big_file,
    schedule_replication_to_source_provider,
    replicate_already_replicated_file,
    not_synced_file_should_not_be_replicated,
    replicate_100_files_separately,
    replicate_100_files_in_one_transfer,
    replication_should_succeed_when_there_is_enough_space_for_file,
    replication_should_fail_when_space_is_full,
    replicate_to_missing_provider_by_guid,
    replicate_to_missing_provider_by_path,
    replicate_to_not_supporting_provider_by_guid,
    replicate_to_not_supporting_provider_by_path,
    schedule_replication_on_not_supporting_provider_by_guid,
    schedule_replication_on_not_supporting_provider_by_path,
    cancel_replication_on_target_nodes,
    % file_replication_failures_should_fail_whole_transfer, TODO uncomment after resolving VFS-4742
    many_simultaneous_failed_transfers,
    schedule_replication_of_regular_file_by_index,
    schedule_replication_of_regular_file_by_index2,
    schedule_replication_of_regular_file_by_index_with_reduce,
    scheduling_replication_by_not_existing_index_should_fail,
    scheduling_replication_by_index_with_function_returning_wrong_value_should_fail,
    scheduling_replication_by_index_returning_not_existing_file_should_fail,
    scheduling_replication_by_empty_index_should_succeed,
    scheduling_replication_by_not_existing_key_in_index_should_succeed,
    schedule_replication_of_100_regular_files_by_index_with_batch_1000,
    schedule_replication_of_100_regular_files_by_index_with_batch_100,
    schedule_replication_of_100_regular_files_by_index_with_batch_10
].

%%%===================================================================
%%% API
%%%===================================================================

replicate_empty_dir_by_guid(Config) ->
    replication_transfers_test_base:replicate_empty_dir(Config, rest, guid).

replicate_empty_dir_by_path(Config) ->
    replication_transfers_test_base:replicate_empty_dir(Config, rest, path).

replicate_tree_of_empty_dirs_by_guid(Config) ->
    replication_transfers_test_base:replicate_tree_of_empty_dirs(Config, rest, guid).

replicate_tree_of_empty_dirs_by_path(Config) ->
    replication_transfers_test_base:replicate_tree_of_empty_dirs(Config, rest, path).

replicate_regular_file_by_guid(Config) ->
    replication_transfers_test_base:replicate_regular_file(Config, rest, guid).

replicate_regular_file_by_path(Config) ->
    replication_transfers_test_base:replicate_regular_file(Config, rest, path).

replicate_file_in_directory_by_guid(Config) ->
    replication_transfers_test_base:replicate_file_in_directory(Config, rest, guid).

replicate_file_in_directory_by_path(Config) ->
    replication_transfers_test_base:replicate_file_in_directory(Config, rest, path).

replicate_big_file(Config) ->
    replication_transfers_test_base:replicate_big_file(Config, rest, guid).

schedule_replication_to_source_provider(Config) ->
    replication_transfers_test_base:schedule_replication_to_source_provider(Config, rest, guid).

replicate_already_replicated_file(Config) ->
    replication_transfers_test_base:replicate_already_replicated_file(Config, rest, guid).

not_synced_file_should_not_be_replicated(Config) ->
    % sync_req:replicate_files is mocked to return {error, not_found}
    replication_transfers_test_base:not_synced_file_should_not_be_replicated(Config, rest, guid).

replicate_100_files_separately(Config) ->
    replication_transfers_test_base:replicate_100_files_separately(Config, rest, guid).

replicate_100_files_in_one_transfer(Config) ->
    replication_transfers_test_base:replicate_100_files_in_one_transfer(Config, rest, guid).

replication_should_succeed_when_there_is_enough_space_for_file(Config) ->
    replication_transfers_test_base:replication_should_succeed_when_there_is_enough_space_for_file(Config, rest, guid).

replication_should_fail_when_space_is_full(Config) ->
    replication_transfers_test_base:replication_should_fail_when_space_is_full(Config, rest, guid).

replicate_to_missing_provider_by_guid(Config) ->
    replication_transfers_test_base:replicate_to_missing_provider(Config, rest, guid).

replicate_to_missing_provider_by_path(Config) ->
    replication_transfers_test_base:replicate_to_missing_provider(Config, rest, path).

replicate_to_not_supporting_provider_by_guid(Config) ->
    replication_transfers_test_base:replicate_to_not_supporting_provider(Config, rest, guid).

replicate_to_not_supporting_provider_by_path(Config) ->
    replication_transfers_test_base:replicate_to_not_supporting_provider(Config, rest, path).

schedule_replication_on_not_supporting_provider_by_guid(Config) ->
    replication_transfers_test_base:schedule_replication_on_not_supporting_provider(Config, rest, guid).

schedule_replication_on_not_supporting_provider_by_path(Config) ->
    replication_transfers_test_base:schedule_replication_on_not_supporting_provider(Config, rest, path).

cancel_replication_on_target_nodes(Config) ->
    replication_transfers_test_base:cancel_replication_on_target_nodes(Config, rest).

file_replication_failures_should_fail_whole_transfer(Config) ->
    replication_transfers_test_base:file_replication_failures_should_fail_whole_transfer(Config, rest, guid).

many_simultaneous_failed_transfers(Config) ->
    replication_transfers_test_base:many_simultaneous_failed_transfers(Config, rest, guid).

schedule_replication_of_regular_file_by_index(Config) ->
    replication_transfers_test_base:schedule_replication_of_regular_file_by_index(Config, rest).

schedule_replication_of_regular_file_by_index2(Config) ->
    replication_transfers_test_base:schedule_replication_of_regular_file_by_index2(Config, rest).

schedule_replication_of_regular_file_by_index_with_reduce(Config) ->
    replication_transfers_test_base:schedule_replication_of_regular_file_by_index_with_reduce(Config, rest).

scheduling_replication_by_not_existing_index_should_fail(Config) ->
    replication_transfers_test_base:scheduling_replication_by_not_existing_index_should_fail(Config, rest).

scheduling_replication_by_index_with_function_returning_wrong_value_should_fail(Config) ->
    replication_transfers_test_base:scheduling_replication_by_index_with_function_returning_wrong_value_should_fail(Config, rest).

scheduling_replication_by_index_returning_not_existing_file_should_fail(Config) ->
    replication_transfers_test_base:scheduling_replication_by_index_returning_not_existing_file_should_fail(Config, lfm).

scheduling_replication_by_empty_index_should_succeed(Config) ->
    replication_transfers_test_base:scheduling_replication_by_empty_index_should_succeed(Config, rest).

scheduling_replication_by_not_existing_key_in_index_should_succeed(Config) ->
    replication_transfers_test_base:scheduling_replication_by_not_existing_key_in_index_should_succeed(Config, rest).

schedule_replication_of_100_regular_files_by_index_with_batch_1000(Config) ->
    replication_transfers_test_base:schedule_replication_of_100_regular_files_by_index(Config, rest).

schedule_replication_of_100_regular_files_by_index_with_batch_100(Config) ->
    %replication_transfers_test_base:init_per_testcase sets replica_eviction_by_index_batch variable to 100
    replication_transfers_test_base:schedule_replication_of_100_regular_files_by_index(Config, rest).

schedule_replication_of_100_regular_files_by_index_with_batch_10(Config) ->
    %replication_transfers_test_base:init_per_testcase sets replica_eviction_by_index_batch variable to 10
    replication_transfers_test_base:schedule_replication_of_100_regular_files_by_index(Config, rest).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    replication_transfers_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    replication_transfers_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    replication_transfers_test_base:init_per_testcase(Case ,Config).

end_per_testcase(Case, Config) ->
    replication_transfers_test_base:end_per_testcase(Case, Config).