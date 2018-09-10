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
    evict_regular_file_replica_by_guid/1, evict_regular_file_replica_by_path/1
    , evict_regular_file_replica_in_directory_by_guid/1, evict_regular_file_replica_in_directory_by_path/1, evict_big_file_replica/1, fail_to_evict_file_replica_without_permissions/1]).

all() -> [
%%    evict_empty_dir_by_guid,
%%    evict_empty_dir_by_path,
%%    evict_tree_of_empty_dirs_by_guid,
%%    evict_tree_of_empty_dirs_by_path,
    evict_regular_file_replica_by_guid,
    evict_regular_file_replica_by_path,
    evict_regular_file_replica_in_directory_by_guid,
    evict_regular_file_replica_in_directory_by_path,
    evict_big_file_replica,
    fail_to_evict_file_replica_without_permissions
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

fail_to_evict_file_replica_without_permissions(Config) ->
    replica_eviction_transfers_test_base:fail_to_evict_file_replica_without_permissions(Config, rest, path).

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