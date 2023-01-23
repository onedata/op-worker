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
-include("modules/fslogic/acl.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([
    replicate_empty_dir_by_guid/1,
    replicate_tree_of_empty_dirs_by_guid/1,
    replicate_regular_file_by_guid/1,
    replicate_file_in_directory_by_guid/1,
    replicate_big_file/1,
    schedule_replication_to_source_provider/1,
    replicate_already_replicated_file/1,
    not_synced_file_should_not_be_replicated/1,
    replicate_100_files_separately/1,
    replicate_100_files_in_one_transfer/1,
    replication_should_succeed_despite_protection_flags/1,
    replication_should_succeed_when_there_is_enough_space_for_file/1,
    replication_should_fail_when_space_is_full/1,
    replicate_to_missing_provider_by_guid/1,
    replicate_to_not_supporting_provider_by_guid/1,
    schedule_replication_on_not_supporting_provider_by_guid/1,
    transfer_continues_on_modified_storage/1,
    cancel_replication_on_target_nodes_by_scheduling_user/1,
    cancel_replication_on_target_nodes_by_other_user/1,
    file_replication_failures_should_fail_whole_transfer/1,
    many_simultaneous_failed_transfers/1,
    rerun_file_replication/1,
    rerun_file_replication_by_other_user/1,
    rerun_dir_replication/1,
    rerun_view_replication/1,
    schedule_replication_of_regular_file_by_view/1,
    schedule_replication_of_regular_file_by_view2/1,
    schedule_replication_of_regular_file_by_view_with_reduce/1,
    scheduling_replication_by_not_existing_view_should_fail/1,
    scheduling_replication_by_view_with_function_returning_wrong_value_should_fail/1,
    scheduling_replication_by_view_returning_not_existing_file_should_not_fail/1,
    scheduling_replication_by_empty_view_should_succeed/1,
    scheduling_replication_by_not_existing_key_in_view_should_succeed/1,
    schedule_replication_of_100_regular_files_by_view_with_batch_1000/1,
    schedule_replication_of_100_regular_files_by_view_with_batch_100/1,
    schedule_replication_of_100_regular_files_by_view_with_batch_10/1,
    file_removed_during_replication/1,
    rtransfer_works_between_providers_with_different_ports/1,
    warp_time_during_replication/1
]).

all() -> [
    replicate_empty_dir_by_guid,
    replicate_tree_of_empty_dirs_by_guid,
    replicate_regular_file_by_guid,
    replicate_file_in_directory_by_guid,
    replicate_big_file,
    schedule_replication_to_source_provider,
    replicate_already_replicated_file,
    not_synced_file_should_not_be_replicated,
    replicate_100_files_separately,
    replicate_100_files_in_one_transfer,
    replication_should_succeed_despite_protection_flags,
    replication_should_succeed_when_there_is_enough_space_for_file,
    replication_should_fail_when_space_is_full,
    replicate_to_missing_provider_by_guid,
    replicate_to_not_supporting_provider_by_guid,
    schedule_replication_on_not_supporting_provider_by_guid,
    transfer_continues_on_modified_storage,
    cancel_replication_on_target_nodes_by_scheduling_user,
    cancel_replication_on_target_nodes_by_other_user,
    % file_replication_failures_should_fail_whole_transfer, TODO uncomment after resolving VFS-4742
    many_simultaneous_failed_transfers,
    rerun_file_replication,
    rerun_file_replication_by_other_user,
    rerun_dir_replication,
    rerun_view_replication,
    schedule_replication_of_regular_file_by_view,
    schedule_replication_of_regular_file_by_view2,
    schedule_replication_of_regular_file_by_view_with_reduce,
    scheduling_replication_by_not_existing_view_should_fail,
    scheduling_replication_by_view_with_function_returning_wrong_value_should_fail,
    scheduling_replication_by_view_returning_not_existing_file_should_not_fail,
    scheduling_replication_by_empty_view_should_succeed,
    scheduling_replication_by_not_existing_key_in_view_should_succeed,
    schedule_replication_of_100_regular_files_by_view_with_batch_1000,
    schedule_replication_of_100_regular_files_by_view_with_batch_100,
    schedule_replication_of_100_regular_files_by_view_with_batch_10,
    file_removed_during_replication,
    rtransfer_works_between_providers_with_different_ports,
    warp_time_during_replication
].

%%%===================================================================
%%% API
%%%===================================================================

replicate_empty_dir_by_guid(Config) ->
    replication_transfers_test_base:replicate_empty_dir(Config, rest, guid).

replicate_tree_of_empty_dirs_by_guid(Config) ->
    replication_transfers_test_base:replicate_tree_of_empty_dirs(Config, rest, guid).

replicate_regular_file_by_guid(Config) ->
    replication_transfers_test_base:replicate_regular_file(Config, rest, guid).

replicate_file_in_directory_by_guid(Config) ->
    replication_transfers_test_base:replicate_file_in_directory(Config, rest, guid).

replicate_big_file(Config) ->
    replication_transfers_test_base:replicate_big_file(Config, rest, guid).

schedule_replication_to_source_provider(Config) ->
    replication_transfers_test_base:schedule_replication_to_source_provider(Config, rest, guid).

replicate_already_replicated_file(Config) ->
    replication_transfers_test_base:replicate_already_replicated_file(Config, rest, guid).

not_synced_file_should_not_be_replicated(Config) ->
    % replication_worker:transfer_regular_file is mocked to return {error, not_found}
    replication_transfers_test_base:not_synced_file_should_not_be_replicated(Config, rest, guid).

replicate_100_files_separately(Config) ->
    replication_transfers_test_base:replicate_100_files_separately(Config, rest, guid).

replicate_100_files_in_one_transfer(Config) ->
    replication_transfers_test_base:replicate_100_files_in_one_transfer(Config, rest, guid).

replication_should_succeed_despite_protection_flags(Config) ->
    replication_transfers_test_base:replication_should_succeed_despite_protection_flags(Config, rest, guid).

replication_should_succeed_when_there_is_enough_space_for_file(Config) ->
    replication_transfers_test_base:replication_should_succeed_when_there_is_enough_space_for_file(Config, rest, guid).

replication_should_fail_when_space_is_full(Config) ->
    replication_transfers_test_base:replication_should_fail_when_space_is_full(Config, rest, guid).

replicate_to_missing_provider_by_guid(Config) ->
    replication_transfers_test_base:replicate_to_missing_provider(Config, rest, guid).

replicate_to_not_supporting_provider_by_guid(Config) ->
    replication_transfers_test_base:replicate_to_not_supporting_provider(Config, rest, guid).

schedule_replication_on_not_supporting_provider_by_guid(Config) ->
    replication_transfers_test_base:schedule_replication_on_not_supporting_provider(Config, rest, guid).

transfer_continues_on_modified_storage(Config) ->
    replication_transfers_test_base:transfer_continues_on_modified_storage(Config, rest, guid).

cancel_replication_on_target_nodes_by_scheduling_user(Config) ->
    replication_transfers_test_base:cancel_replication_on_target_nodes_by_scheduling_user(Config, rest).

cancel_replication_on_target_nodes_by_other_user(Config) ->
    replication_transfers_test_base:cancel_replication_on_target_nodes_by_other_user(Config, rest).

file_replication_failures_should_fail_whole_transfer(Config) ->
    replication_transfers_test_base:file_replication_failures_should_fail_whole_transfer(Config, rest, guid).

many_simultaneous_failed_transfers(Config) ->
    replication_transfers_test_base:many_simultaneous_failed_transfers(Config, rest, guid).

rerun_file_replication(Config) ->
    replication_transfers_test_base:rerun_file_replication(Config, rest, guid).

rerun_file_replication_by_other_user(Config) ->
    replication_transfers_test_base:rerun_file_replication_by_other_user(Config, rest, guid).

rerun_dir_replication(Config) ->
    replication_transfers_test_base:rerun_dir_replication(Config, rest, guid).

rerun_view_replication(Config) ->
    replication_transfers_test_base:rerun_view_replication(Config, rest).

schedule_replication_of_regular_file_by_view(Config) ->
    replication_transfers_test_base:schedule_replication_of_regular_file_by_view(Config, rest).

schedule_replication_of_regular_file_by_view2(Config) ->
    replication_transfers_test_base:schedule_replication_of_regular_file_by_view2(Config, rest).

schedule_replication_of_regular_file_by_view_with_reduce(Config) ->
    replication_transfers_test_base:schedule_replication_of_regular_file_by_view_with_reduce(Config, rest).

scheduling_replication_by_not_existing_view_should_fail(Config) ->
    replication_transfers_test_base:scheduling_replication_by_not_existing_view_should_fail(Config, rest).

scheduling_replication_by_view_with_function_returning_wrong_value_should_fail(Config) ->
    replication_transfers_test_base:scheduling_replication_by_view_with_function_returning_wrong_value_should_fail(Config, rest).

scheduling_replication_by_view_returning_not_existing_file_should_not_fail(Config) ->
    replication_transfers_test_base:scheduling_replication_by_view_returning_not_existing_file_should_not_fail(Config, rest).

scheduling_replication_by_empty_view_should_succeed(Config) ->
    replication_transfers_test_base:scheduling_replication_by_empty_view_should_succeed(Config, rest).

scheduling_replication_by_not_existing_key_in_view_should_succeed(Config) ->
    replication_transfers_test_base:scheduling_replication_by_not_existing_key_in_view_should_succeed(Config, rest).

schedule_replication_of_100_regular_files_by_view_with_batch_1000(Config) ->
    replication_transfers_test_base:schedule_replication_of_100_regular_files_by_view(Config, rest).

schedule_replication_of_100_regular_files_by_view_with_batch_100(Config) ->
    %replication_transfers_test_base:init_per_testcase sets replica_eviction_by_view_batch variable to 100
    replication_transfers_test_base:schedule_replication_of_100_regular_files_by_view(Config, rest).

schedule_replication_of_100_regular_files_by_view_with_batch_10(Config) ->
    %replication_transfers_test_base:init_per_testcase sets replica_eviction_by_view_batch variable to 10
    replication_transfers_test_base:schedule_replication_of_100_regular_files_by_view(Config, rest).

file_removed_during_replication(Config) ->
    replication_transfers_test_base:file_removed_during_replication(Config, rest, guid).

rtransfer_works_between_providers_with_different_ports(Config) ->
    replication_transfers_test_base:rtransfer_works_between_providers_with_different_ports(Config, rest).

warp_time_during_replication(Config) ->
    replication_transfers_test_base:warp_time_during_replication(Config, rest).

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
