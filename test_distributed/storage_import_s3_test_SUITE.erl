%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage import on s3 storage.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_s3_test_SUITE).
-author("Jakub Kudzia").

-include("storage_import_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("kernel/include/file.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    % tests of import
    create_delete_import_test/1,
    create_remote_file_import_conflict_test/1,
    create_remote_file_import_race_test/1,
    create_file_import_race_test/1,
    close_file_import_race_test/1,
    delete_file_reimport_race_test/1,
    remote_delete_file_reimport_race_test/1,
    remote_delete_file_reimport_race2_test/1,
    delete_opened_file_reimport_race_test/1,

    sync_works_properly_after_delete_test/1,
    delete_and_update_files_simultaneously_update_test/1,
    create_delete_race_test/1,
    create_list_race_test/1,
    symlink_is_ignored_by_initial_scan/1,

    change_file_type4_test/1,
    recreate_file_deleted_by_sync_test/1,
    sync_should_not_invalidate_file_after_replication/1,
    time_warp_between_scans_test/1,
    time_warp_during_scan_test/1
]).

-define(TEST_CASES, [
    % tests of import
    create_delete_import_test,
    create_remote_file_import_conflict_test,
    create_remote_file_import_race_test,
    create_file_import_race_test,
    close_file_import_race_test,
    delete_file_reimport_race_test,
    remote_delete_file_reimport_race_test,
    remote_delete_file_reimport_race2_test,
    delete_opened_file_reimport_race_test,

    sync_works_properly_after_delete_test,
    delete_and_update_files_simultaneously_update_test,
    create_delete_race_test,
    create_list_race_test,
    symlink_is_ignored_by_initial_scan,

    change_file_type4_test,
    recreate_file_deleted_by_sync_test,
    sync_should_not_invalidate_file_after_replication,
    time_warp_between_scans_test,
    time_warp_during_scan_test
]).

all() -> ?ALL(?TEST_CASES).

%%%==================================================================
%%% Test functions
%%%===================================================================

create_delete_import_test(Config) ->
    storage_import_test_base:create_delete_import_test(Config).

create_remote_file_import_conflict_test(Config) ->
    storage_import_test_base:create_remote_file_import_conflict_test(Config).

create_remote_file_import_race_test(Config) ->
    storage_import_test_base:create_remote_file_import_race_test(Config).

create_file_import_race_test(Config) ->
    storage_import_test_base:create_file_import_race_test(Config).

close_file_import_race_test(Config) ->
    storage_import_test_base:close_file_import_race_test(Config, ?S3_HELPER_NAME).

delete_file_reimport_race_test(Config) ->
    storage_import_test_base:delete_file_reimport_race_test(Config, ?S3_HELPER_NAME).

remote_delete_file_reimport_race_test(Config) ->
    storage_import_test_base:remote_delete_file_reimport_race_test(Config, ?S3_HELPER_NAME).

remote_delete_file_reimport_race2_test(Config) ->
    storage_import_test_base:remote_delete_file_reimport_race2_test(Config, ?S3_HELPER_NAME).

delete_opened_file_reimport_race_test(Config) ->
    storage_import_test_base:delete_opened_file_reimport_race_test(Config, ?S3_HELPER_NAME).


sync_works_properly_after_delete_test(Config) ->
    storage_import_s3_test_base:sync_works_properly_after_delete_test(Config).

delete_and_update_files_simultaneously_update_test(Config) ->
    storage_import_s3_test_base:delete_and_update_files_simultaneously_update_test(Config).

create_delete_race_test(Config) ->
    storage_import_test_base:create_delete_race_test(Config, ?S3_HELPER_NAME).

create_list_race_test(Config) ->
    storage_import_s3_test_base:create_list_race_test(Config).

symlink_is_ignored_by_initial_scan(Config) ->
    storage_import_test_base:symlink_is_ignored_by_initial_scan(Config).


change_file_type4_test(Config) ->
    storage_import_test_base:change_file_type4_test(Config).

recreate_file_deleted_by_sync_test(Config) ->
    storage_import_test_base:recreate_file_deleted_by_sync_test(Config).

sync_should_not_invalidate_file_after_replication(Config) ->
    storage_import_test_base:sync_should_not_invalidate_file_after_replication(Config).

time_warp_between_scans_test(Config) ->
    storage_import_test_base:time_warp_between_scans_test(Config).

time_warp_during_scan_test(Config) ->
    storage_import_test_base:time_warp_during_scan_test(Config).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    storage_import_s3_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    storage_import_s3_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    storage_import_s3_test_base:init_per_testcase(Case, Config).

end_per_testcase(Case, Config) ->
    storage_import_s3_test_base:end_per_testcase(Case, Config).