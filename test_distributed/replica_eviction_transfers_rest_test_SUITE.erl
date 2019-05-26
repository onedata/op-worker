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
    evict_empty_dir_by_guid/1, evict_empty_dir_by_path/1,
    evict_tree_of_empty_dirs_by_guid/1, evict_tree_of_empty_dirs_by_path/1,
    evict_regular_file_replica_by_guid/1, evict_regular_file_replica_by_path/1,
    evict_regular_file_replica_in_directory_by_guid/1,
    evict_regular_file_replica_in_directory_by_path/1,
    evict_big_file_replica/1,
    evict_100_files_in_one_request/1,
    evict_100_files_each_file_separately/1,
    many_simultaneous_failed_replica_evictions/1,
    rerun_replica_eviction/1,
    rerun_replica_eviction_by_other_user/1,
    rerun_dir_eviction/1,
    cancel_replica_eviction_on_target_nodes/1,
    fail_to_evict_file_replica_without_permissions/1,
    eviction_should_succeed_when_remote_provider_modified_file_replica/1,
    eviction_should_fail_when_evicting_provider_modified_file_replica/1,
    quota_decreased_after_eviction/1,
    schedule_replica_eviction_by_index/1,
    schedule_eviction_of_regular_file_by_index_with_reduce/1,
    scheduling_replica_eviction_by_not_existing_index_should_fail/1,
    scheduling_replica_eviction_by_index_with_function_returning_wrong_value_should_fail/1,
    scheduling_replica_eviction_by_index_returning_not_existing_file_should_not_fail/1,
    scheduling_replica_eviction_by_empty_index_should_succeed/1,
    scheduling_replica_eviction_by_not_existing_key_in_index_should_succeed/1,
    schedule_replica_eviction_of_100_regular_files_by_index_with_batch_1000/1,
    schedule_replica_eviction_of_100_regular_files_by_index_with_batch_100/1,
    schedule_replica_eviction_of_100_regular_files_by_index_with_batch_10/1,
    remove_file_during_eviction/1
]).

all() -> [
    evict_empty_dir_by_guid,
    evict_empty_dir_by_path,
    evict_tree_of_empty_dirs_by_guid,
    evict_tree_of_empty_dirs_by_path,
    evict_regular_file_replica_by_guid,
    evict_regular_file_replica_by_path,
    evict_regular_file_replica_in_directory_by_guid,
    evict_regular_file_replica_in_directory_by_path,
    evict_big_file_replica,
    evict_100_files_in_one_request,
    evict_100_files_each_file_separately,
    many_simultaneous_failed_replica_evictions,
    rerun_replica_eviction,
    rerun_replica_eviction_by_other_user,
    rerun_dir_eviction,
    cancel_replica_eviction_on_target_nodes,
%%    fail_to_evict_file_replica_without_permissions %todo VFS-4844,
    eviction_should_succeed_when_remote_provider_modified_file_replica,
    eviction_should_fail_when_evicting_provider_modified_file_replica,
    quota_decreased_after_eviction,
    schedule_replica_eviction_by_index,
    schedule_eviction_of_regular_file_by_index_with_reduce,
    scheduling_replica_eviction_by_not_existing_index_should_fail,
    scheduling_replica_eviction_by_index_with_function_returning_wrong_value_should_fail,
    scheduling_replica_eviction_by_index_returning_not_existing_file_should_not_fail,
    scheduling_replica_eviction_by_empty_index_should_succeed,
    scheduling_replica_eviction_by_not_existing_key_in_index_should_succeed,
    schedule_replica_eviction_of_100_regular_files_by_index_with_batch_1000,
    schedule_replica_eviction_of_100_regular_files_by_index_with_batch_100,
    schedule_replica_eviction_of_100_regular_files_by_index_with_batch_10,
    remove_file_during_eviction
].

%%%===================================================================
%%% API
%%%===================================================================

evict_empty_dir_by_guid(Config) ->
    replica_eviction_transfers_test_base:evict_empty_dir(Config, rest, guid).

evict_empty_dir_by_path(Config) ->
    replica_eviction_transfers_test_base:evict_empty_dir(Config, rest, path).

evict_tree_of_empty_dirs_by_guid(Config) ->
    replica_eviction_transfers_test_base:evict_tree_of_empty_dirs(Config, rest, guid).

evict_tree_of_empty_dirs_by_path(Config) ->
    replica_eviction_transfers_test_base:evict_tree_of_empty_dirs(Config, rest, path).

evict_regular_file_replica_by_guid(Config) ->
    replica_eviction_transfers_test_base:evict_regular_file_replica(Config, rest, guid).

evict_regular_file_replica_by_path(Config) ->
    replica_eviction_transfers_test_base:evict_regular_file_replica(Config, rest, path).

evict_regular_file_replica_in_directory_by_guid(Config) ->
    replica_eviction_transfers_test_base:evict_regular_file_replica_in_directory(Config, rest, guid).

evict_regular_file_replica_in_directory_by_path(Config) ->
    replica_eviction_transfers_test_base:evict_regular_file_replica_in_directory(Config, rest, path).

evict_big_file_replica(Config) ->
    replica_eviction_transfers_test_base:evict_big_file_replica(Config, rest, guid).

evict_100_files_in_one_request(Config) ->
    replica_eviction_transfers_test_base:evict_100_files_in_one_request(Config, rest, path).

evict_100_files_each_file_separately(Config) ->
    replica_eviction_transfers_test_base:evict_100_files_each_file_separately(Config, rest, path).

many_simultaneous_failed_replica_evictions(Config) ->
    replica_eviction_transfers_test_base:many_simultaneous_failed_replica_evictions(Config, rest, guid).

rerun_replica_eviction(Config) ->
    replica_eviction_transfers_test_base:rerun_replica_eviction(Config, rest, guid).

rerun_replica_eviction_by_other_user(Config) ->
    replica_eviction_transfers_test_base:rerun_replica_eviction_by_other_user(Config, rest, guid).

rerun_dir_eviction(Config) ->
    replica_eviction_transfers_test_base:rerun_dir_eviction(Config, rest, guid).

cancel_replica_eviction_on_target_nodes(Config) ->
    replica_eviction_transfers_test_base:cancel_replica_eviction_on_target_nodes(Config, rest).

fail_to_evict_file_replica_without_permissions(Config) ->
    replica_eviction_transfers_test_base:fail_to_evict_file_replica_without_permissions(Config, rest, path).

eviction_should_succeed_when_remote_provider_modified_file_replica(Config) ->
    replica_eviction_transfers_test_base:eviction_should_succeed_when_remote_provider_modified_file_replica(Config, rest, path).

eviction_should_fail_when_evicting_provider_modified_file_replica(Config) ->
    replica_eviction_transfers_test_base:eviction_should_fail_when_evicting_provider_modified_file_replica(Config, rest, path).

quota_decreased_after_eviction(Config) ->
    replica_eviction_transfers_test_base:quota_decreased_after_eviction(Config, rest, path).

schedule_replica_eviction_by_index(Config) ->
    replica_eviction_transfers_test_base:schedule_replica_eviction_by_index(Config, rest).

schedule_eviction_of_regular_file_by_index_with_reduce(Config) ->
    replica_eviction_transfers_test_base:schedule_eviction_of_regular_file_by_index_with_reduce(Config, rest).

scheduling_replica_eviction_by_not_existing_index_should_fail(Config) ->
    replica_eviction_transfers_test_base:scheduling_replica_eviction_by_not_existing_index_should_fail(Config, rest).

scheduling_replica_eviction_by_index_with_function_returning_wrong_value_should_fail(Config) ->
    replica_eviction_transfers_test_base:scheduling_replica_eviction_by_index_with_function_returning_wrong_value_should_fail(Config, rest).

scheduling_replica_eviction_by_index_returning_not_existing_file_should_not_fail(Config) ->
    replica_eviction_transfers_test_base:scheduling_replica_eviction_by_index_returning_not_existing_file_should_not_fail(Config, rest).

scheduling_replica_eviction_by_empty_index_should_succeed(Config) ->
    replica_eviction_transfers_test_base:scheduling_replica_eviction_by_empty_index_should_succeed(Config, rest).

scheduling_replica_eviction_by_not_existing_key_in_index_should_succeed(Config) ->
    replica_eviction_transfers_test_base:scheduling_replica_eviction_by_not_existing_key_in_index_should_succeed(Config, rest).

schedule_replica_eviction_of_100_regular_files_by_index_with_batch_1000(Config) ->
    replica_eviction_transfers_test_base:schedule_replica_eviction_of_100_regular_files_by_index(Config, rest).

schedule_replica_eviction_of_100_regular_files_by_index_with_batch_100(Config) ->
    %replica_eviction_transfers_test_base:init_per_testcase sets replica_eviction_by_index_batch variable to 100
    replica_eviction_transfers_test_base:schedule_replica_eviction_of_100_regular_files_by_index(Config, rest).

schedule_replica_eviction_of_100_regular_files_by_index_with_batch_10(Config) ->
    %replica_eviction_transfers_test_base:init_per_testcase sets replica_eviction_by_index_batch variable to 10
    replica_eviction_transfers_test_base:schedule_replica_eviction_of_100_regular_files_by_index(Config, rest).

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