%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of replica eviction jobs, scheduled via REST.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_eviction_transfers_rest_test_SUITE).
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
    evict_empty_dir_by_guid/1,
    evict_tree_of_empty_dirs_by_guid/1,
    evict_regular_file_replica_by_guid/1,
    evict_regular_file_replica_in_directory_by_guid/1,
    evict_big_file_replica/1,
    evict_100_files_in_one_request/1,
    evict_100_files_each_file_separately/1,
    eviction_should_succeed_despite_protection_flags/1,
    many_simultaneous_failed_replica_evictions/1,
    rerun_replica_eviction/1,
    rerun_replica_eviction_by_other_user/1,
    rerun_dir_eviction/1,
    rerun_view_eviction/1,
    cancel_replica_eviction_on_target_nodes_by_scheduling_user/1,
    cancel_replica_eviction_on_target_nodes_by_other_user/1,
    fail_to_evict_file_replica_without_permissions/1,
    eviction_should_succeed_when_remote_provider_modified_file_replica/1,
    eviction_should_fail_when_evicting_provider_modified_file_replica/1,
    quota_decreased_after_eviction/1,
    schedule_replica_eviction_by_view/1,
    schedule_eviction_of_regular_file_by_view_with_reduce/1,
    scheduling_replica_eviction_by_not_existing_view_should_fail/1,
    scheduling_replica_eviction_by_view_with_function_returning_wrong_value_should_fail/1,
    scheduling_replica_eviction_by_view_returning_not_existing_file_should_not_fail/1,
    scheduling_replica_eviction_by_empty_view_should_succeed/1,
    scheduling_replica_eviction_by_not_existing_key_in_view_should_succeed/1,
    schedule_replica_eviction_of_100_regular_files_by_view_with_batch_1000/1,
    schedule_replica_eviction_of_100_regular_files_by_view_with_batch_100/1,
    schedule_replica_eviction_of_100_regular_files_by_view_with_batch_10/1,
    remove_file_during_eviction/1
]).

all() -> [
    evict_empty_dir_by_guid,
    evict_tree_of_empty_dirs_by_guid,
    evict_regular_file_replica_by_guid,
    evict_regular_file_replica_in_directory_by_guid,
    evict_big_file_replica,
    evict_100_files_in_one_request,
    evict_100_files_each_file_separately,
    eviction_should_succeed_despite_protection_flags,
    many_simultaneous_failed_replica_evictions,
    rerun_replica_eviction,
    rerun_replica_eviction_by_other_user,
    rerun_dir_eviction,
    rerun_view_eviction,
    cancel_replica_eviction_on_target_nodes_by_scheduling_user,
    cancel_replica_eviction_on_target_nodes_by_other_user,
%%    fail_to_evict_file_replica_without_permissions %todo VFS-10259,
    eviction_should_succeed_when_remote_provider_modified_file_replica,
    eviction_should_fail_when_evicting_provider_modified_file_replica,
    quota_decreased_after_eviction,
    schedule_replica_eviction_by_view,
    schedule_eviction_of_regular_file_by_view_with_reduce,
    scheduling_replica_eviction_by_not_existing_view_should_fail,
    scheduling_replica_eviction_by_view_with_function_returning_wrong_value_should_fail,
    scheduling_replica_eviction_by_view_returning_not_existing_file_should_not_fail,
    scheduling_replica_eviction_by_empty_view_should_succeed,
    scheduling_replica_eviction_by_not_existing_key_in_view_should_succeed,
    schedule_replica_eviction_of_100_regular_files_by_view_with_batch_1000,
    schedule_replica_eviction_of_100_regular_files_by_view_with_batch_100,
    schedule_replica_eviction_of_100_regular_files_by_view_with_batch_10,
    remove_file_during_eviction
].

%%%===================================================================
%%% API
%%%===================================================================

evict_empty_dir_by_guid(Config) ->
    replica_eviction_transfers_test_base:evict_empty_dir(Config, rest, guid).

evict_tree_of_empty_dirs_by_guid(Config) ->
    replica_eviction_transfers_test_base:evict_tree_of_empty_dirs(Config, rest, guid).

evict_regular_file_replica_by_guid(Config) ->
    replica_eviction_transfers_test_base:evict_regular_file_replica(Config, rest, guid).

evict_regular_file_replica_in_directory_by_guid(Config) ->
    replica_eviction_transfers_test_base:evict_regular_file_replica_in_directory(Config, rest, guid).

evict_big_file_replica(Config) ->
    replica_eviction_transfers_test_base:evict_big_file_replica(Config, rest, guid).

evict_100_files_in_one_request(Config) ->
    replica_eviction_transfers_test_base:evict_100_files_in_one_request(Config, rest, guid).

evict_100_files_each_file_separately(Config) ->
    replica_eviction_transfers_test_base:evict_100_files_each_file_separately(Config, rest, guid).

eviction_should_succeed_despite_protection_flags(Config) ->
    replica_eviction_transfers_test_base:evict_despite_protection_flags(Config, rest, guid).

many_simultaneous_failed_replica_evictions(Config) ->
    replica_eviction_transfers_test_base:many_simultaneous_failed_replica_evictions(Config, rest, guid).

rerun_replica_eviction(Config) ->
    replica_eviction_transfers_test_base:rerun_replica_eviction(Config, rest, guid).

rerun_replica_eviction_by_other_user(Config) ->
    replica_eviction_transfers_test_base:rerun_replica_eviction_by_other_user(Config, rest, guid).

rerun_dir_eviction(Config) ->
    replica_eviction_transfers_test_base:rerun_dir_eviction(Config, rest, guid).

rerun_view_eviction(Config) ->
    replica_eviction_transfers_test_base:rerun_view_eviction(Config, rest).

cancel_replica_eviction_on_target_nodes_by_scheduling_user(Config) ->
    replica_eviction_transfers_test_base:cancel_replica_eviction_on_target_nodes_by_scheduling_user(Config, rest).

cancel_replica_eviction_on_target_nodes_by_other_user(Config) ->
    replica_eviction_transfers_test_base:cancel_replica_eviction_on_target_nodes_by_other_user(Config, rest).

fail_to_evict_file_replica_without_permissions(Config) ->
    replica_eviction_transfers_test_base:fail_to_evict_file_replica_without_permissions(Config, rest, guid).

eviction_should_succeed_when_remote_provider_modified_file_replica(Config) ->
    replica_eviction_transfers_test_base:eviction_should_succeed_when_remote_provider_modified_file_replica(Config, rest, guid).

eviction_should_fail_when_evicting_provider_modified_file_replica(Config) ->
    replica_eviction_transfers_test_base:eviction_should_fail_when_evicting_provider_modified_file_replica(Config, rest, guid).

quota_decreased_after_eviction(Config) ->
    replica_eviction_transfers_test_base:quota_decreased_after_eviction(Config, rest, guid).

schedule_replica_eviction_by_view(Config) ->
    replica_eviction_transfers_test_base:schedule_replica_eviction_by_view(Config, rest).

schedule_eviction_of_regular_file_by_view_with_reduce(Config) ->
    replica_eviction_transfers_test_base:schedule_eviction_of_regular_file_by_view_with_reduce(Config, rest).

scheduling_replica_eviction_by_not_existing_view_should_fail(Config) ->
    replica_eviction_transfers_test_base:scheduling_replica_eviction_by_not_existing_view_should_fail(Config, rest).

scheduling_replica_eviction_by_view_with_function_returning_wrong_value_should_fail(Config) ->
    replica_eviction_transfers_test_base:scheduling_replica_eviction_by_view_with_function_returning_wrong_value_should_fail(Config, rest).

scheduling_replica_eviction_by_view_returning_not_existing_file_should_not_fail(Config) ->
    replica_eviction_transfers_test_base:scheduling_replica_eviction_by_view_returning_not_existing_file_should_not_fail(Config, rest).

scheduling_replica_eviction_by_empty_view_should_succeed(Config) ->
    replica_eviction_transfers_test_base:scheduling_replica_eviction_by_empty_view_should_succeed(Config, rest).

scheduling_replica_eviction_by_not_existing_key_in_view_should_succeed(Config) ->
    replica_eviction_transfers_test_base:scheduling_replica_eviction_by_not_existing_key_in_view_should_succeed(Config, rest).

schedule_replica_eviction_of_100_regular_files_by_view_with_batch_1000(Config) ->
    replica_eviction_transfers_test_base:schedule_replica_eviction_of_100_regular_files_by_view(Config, rest).

schedule_replica_eviction_of_100_regular_files_by_view_with_batch_100(Config) ->
    %replica_eviction_transfers_test_base:init_per_testcase sets replica_eviction_by_view_batch variable to 100
    replica_eviction_transfers_test_base:schedule_replica_eviction_of_100_regular_files_by_view(Config, rest).

schedule_replica_eviction_of_100_regular_files_by_view_with_batch_10(Config) ->
    %replica_eviction_transfers_test_base:init_per_testcase sets replica_eviction_by_view_batch variable to 10
    replica_eviction_transfers_test_base:schedule_replica_eviction_of_100_regular_files_by_view(Config, rest).

remove_file_during_eviction(Config) ->
    replica_eviction_transfers_test_base:remove_file_during_eviction(Config, rest, guid).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    replica_eviction_transfers_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    replica_eviction_transfers_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    replica_eviction_transfers_test_base:init_per_testcase(Case ,Config).

end_per_testcase(Case, Config) ->
    replica_eviction_transfers_test_base:end_per_testcase(Case, Config).
