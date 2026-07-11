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

-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    % tests of import
    create_delete_import_test/1,

    symlink_is_ignored_by_initial_scan/1,

    change_file_type4_test/1,
    recreate_file_deleted_by_sync_test/1
]).

-define(TEST_CASES, [
    % tests of import
    create_delete_import_test,

    symlink_is_ignored_by_initial_scan,

    change_file_type4_test,
    recreate_file_deleted_by_sync_test
]).

all() -> ?ALL(?TEST_CASES).

%%%==================================================================
%%% Test functions
%%%===================================================================

create_delete_import_test(Config) ->
    storage_import_test_base:create_delete_import_test(Config).

symlink_is_ignored_by_initial_scan(Config) ->
    storage_import_test_base:symlink_is_ignored_by_initial_scan(Config).

change_file_type4_test(Config) ->
    storage_import_test_base:change_file_type4_test(Config).

recreate_file_deleted_by_sync_test(Config) ->
    storage_import_test_base:recreate_file_deleted_by_sync_test(Config).

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