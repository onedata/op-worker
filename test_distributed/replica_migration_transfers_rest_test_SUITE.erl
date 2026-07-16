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
-include("modules/fslogic/acl.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([
    fail_to_migrate_file_replica_without_permissions/1,
    cancel_migration_on_target_nodes_by_scheduling_user/1,
    cancel_migration_on_target_nodes_by_other_user/1,
    rerun_file_migration/1,
    rerun_view_migration/1
]).

all() -> [
%%    fail_to_migrate_file_replica_without_permissions %todo VFS-10259
    cancel_migration_on_target_nodes_by_scheduling_user,
    cancel_migration_on_target_nodes_by_other_user,
    rerun_file_migration,
    rerun_view_migration
].

%%%===================================================================
%%% API
%%%===================================================================

fail_to_migrate_file_replica_without_permissions(Config) ->
    replica_migration_transfers_test_base:fail_to_migrate_file_replica_without_permissions(Config, rest, guid).

cancel_migration_on_target_nodes_by_scheduling_user(Config) ->
    replica_migration_transfers_test_base:cancel_migration_on_target_nodes_by_scheduling_user(Config, rest).

cancel_migration_on_target_nodes_by_other_user(Config) ->
    replica_migration_transfers_test_base:cancel_migration_on_target_nodes_by_other_user(Config, rest).

rerun_file_migration(Config) ->
    replica_migration_transfers_test_base:rerun_file_migration(Config, rest, guid).

rerun_view_migration(Config) ->
    replica_migration_transfers_test_base:rerun_view_migration(Config, rest).

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
