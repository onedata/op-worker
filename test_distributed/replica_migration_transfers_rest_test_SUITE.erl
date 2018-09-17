%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests of replica migration jobs, scheduled via REST.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_migration_transfers_rest_test_SUITE).
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
    migrate_empty_dir_by_guid/1, migrate_empty_dir_by_path/1,
    migrate_tree_of_empty_dirs_by_guid/1, migrate_tree_of_empty_dirs_by_path/1,
    migrate_regular_file_replica_by_guid/1, migrate_regular_file_replica_by_path/1,
    migrate_regular_file_replica_in_directory_by_guid/1,
    migrate_regular_file_replica_in_directory_by_path/1,
    migrate_big_file_replica/1,
    migrate_100_files_in_one_request/1,
    migrate_100_files_each_file_separately/1,
    fail_to_migrate_file_replica_without_permissions/1
]).

all() -> [
    migrate_empty_dir_by_guid,
    migrate_empty_dir_by_path,
    migrate_tree_of_empty_dirs_by_guid,
    migrate_tree_of_empty_dirs_by_path,
    migrate_regular_file_replica_by_guid,
    migrate_regular_file_replica_by_path,
    migrate_regular_file_replica_in_directory_by_guid,
    migrate_regular_file_replica_in_directory_by_path,
    migrate_big_file_replica,
    migrate_100_files_in_one_request,
    migrate_100_files_each_file_separately
%%    fail_to_migrate_file_replica_without_permissions %todo VFS-4844
].

%%%===================================================================
%%% API
%%%===================================================================

migrate_empty_dir_by_guid(Config) ->
    replica_migration_transfers_test_base:migrate_empty_dir(Config, rest, guid).

migrate_empty_dir_by_path(Config) ->
    replica_migration_transfers_test_base:migrate_empty_dir(Config, rest, path).

migrate_tree_of_empty_dirs_by_guid(Config) ->
    replica_migration_transfers_test_base:migrate_tree_of_empty_dirs(Config, rest, guid).

migrate_tree_of_empty_dirs_by_path(Config) ->
    replica_migration_transfers_test_base:migrate_tree_of_empty_dirs(Config, rest, path).

migrate_regular_file_replica_by_guid(Config) ->
    replica_migration_transfers_test_base:migrate_regular_file_replica(Config, rest, guid).

migrate_regular_file_replica_by_path(Config) ->
    replica_migration_transfers_test_base:migrate_regular_file_replica(Config, rest, path).

migrate_regular_file_replica_in_directory_by_guid(Config) ->
    replica_migration_transfers_test_base:migrate_regular_file_replica_in_directory(Config, rest, guid).

migrate_regular_file_replica_in_directory_by_path(Config) ->
    replica_migration_transfers_test_base:migrate_regular_file_replica_in_directory(Config, rest, path).

migrate_big_file_replica(Config) ->
    replica_migration_transfers_test_base:migrate_big_file_replica(Config, rest, guid).

migrate_100_files_in_one_request(Config) ->
    replica_migration_transfers_test_base:migrate_100_files_in_one_request(Config, rest, path).

migrate_100_files_each_file_separately(Config) ->
    replica_migration_transfers_test_base:migrate_100_files_each_file_separately(Config, rest, path).

fail_to_migrate_file_replica_without_permissions(Config) ->
    replica_migration_transfers_test_base:fail_to_migrate_file_replica_without_permissions(Config, rest, path).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    replica_migration_transfers_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    replica_migration_transfers_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    replica_migration_transfers_test_base:init_per_testcase(Case ,Config).

end_per_testcase(Case, Config) ->
    replica_migration_transfers_test_base:end_per_testcase(Case, Config).