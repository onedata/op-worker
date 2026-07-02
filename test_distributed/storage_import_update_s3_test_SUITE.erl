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
-module(storage_import_update_s3_test_SUITE).
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
    sync_should_not_reimport_deleted_but_still_opened_file/1,
    sync_should_not_import_recreated_file_with_suffix_on_storage/1,
    sync_should_update_blocks_of_recreated_file_with_suffix_on_storage/1,
    sync_should_not_import_replicated_file_with_suffix_on_storage/1,
    sync_should_update_replicated_file_with_suffix_on_storage/1,
    create_delete_import2_test/1,
    create_subfiles_and_delete_before_import_is_finished_test/1,
    file_with_data_protection_should_not_be_updated_test/1,
    file_with_data_and_metadata_protection_should_not_be_updated_test/1,
    file_with_data_protection_should_not_be_deleted_test/1,
    file_with_data_and_metadata_protection_should_not_be_deleted_test/1,
    dir_and_its_child_with_data_protection_should_not_be_updated_test/1,
    dir_and_its_child_with_data_and_metadata_protection_should_not_be_updated_test/1,
    dir_and_its_child_with_data_protection_should_not_be_deleted_test/1,
    dir_and_its_child_with_data_and_metadata_protection_should_not_be_deleted_test/1
]).

-define(TEST_CASES, [
    sync_should_not_reimport_deleted_but_still_opened_file,
    sync_should_not_import_recreated_file_with_suffix_on_storage,
    sync_should_update_blocks_of_recreated_file_with_suffix_on_storage,
    sync_should_not_import_replicated_file_with_suffix_on_storage,
    sync_should_update_replicated_file_with_suffix_on_storage,
    create_delete_import2_test,
    create_subfiles_and_delete_before_import_is_finished_test,
    file_with_data_protection_should_not_be_updated_test,
    file_with_data_and_metadata_protection_should_not_be_updated_test,
    file_with_data_protection_should_not_be_deleted_test,
    file_with_data_and_metadata_protection_should_not_be_deleted_test,
    dir_and_its_child_with_data_protection_should_not_be_updated_test,
    dir_and_its_child_with_data_and_metadata_protection_should_not_be_updated_test,
    dir_and_its_child_with_data_protection_should_not_be_deleted_test,
    dir_and_its_child_with_data_and_metadata_protection_should_not_be_deleted_test
]).

all() -> ?ALL(?TEST_CASES).

%%%==================================================================
%%% Test functions
%%%===================================================================

sync_should_not_reimport_deleted_but_still_opened_file(Config) ->
    storage_import_s3_test_base:sync_should_not_reimport_deleted_but_still_opened_file(Config, ?S3_HELPER_NAME).

sync_should_not_import_recreated_file_with_suffix_on_storage(Config) ->
    storage_import_test_base:sync_should_not_import_recreated_file_with_suffix_on_storage(Config, ?S3_HELPER_NAME).

sync_should_update_blocks_of_recreated_file_with_suffix_on_storage(Config) ->
    storage_import_test_base:sync_should_update_blocks_of_recreated_file_with_suffix_on_storage(Config, ?S3_HELPER_NAME).

sync_should_not_import_replicated_file_with_suffix_on_storage(Config) ->
    storage_import_test_base:sync_should_not_import_replicated_file_with_suffix_on_storage(Config, ?S3_HELPER_NAME).

sync_should_update_replicated_file_with_suffix_on_storage(Config) ->
    storage_import_test_base:sync_should_update_replicated_file_with_suffix_on_storage(Config, ?S3_HELPER_NAME).

create_delete_import2_test(Config) ->
    storage_import_test_base:create_delete_import2_test(Config).

create_subfiles_and_delete_before_import_is_finished_test(Config) ->
    storage_import_s3_test_base:create_subfiles_and_delete_before_import_is_finished_test(Config).

file_with_data_protection_should_not_be_updated_test(Config) ->
    storage_import_test_base:file_with_data_protection_should_not_be_updated_test(Config, ?S3_HELPER_NAME).

file_with_data_and_metadata_protection_should_not_be_updated_test(Config) ->
    storage_import_test_base:file_with_data_and_metadata_protection_should_not_be_updated_test(Config, ?S3_HELPER_NAME).

file_with_data_protection_should_not_be_deleted_test(Config) ->
    storage_import_test_base:file_with_data_protection_should_not_be_deleted_test(Config, ?S3_HELPER_NAME).

file_with_data_and_metadata_protection_should_not_be_deleted_test(Config) ->
    storage_import_test_base:file_with_data_and_metadata_protection_should_not_be_deleted_test(Config, ?S3_HELPER_NAME).

dir_and_its_child_with_data_protection_should_not_be_updated_test(Config) ->
    storage_import_test_base:dir_and_its_child_with_data_protection_should_not_be_updated_test(Config, ?S3_HELPER_NAME).

dir_and_its_child_with_data_and_metadata_protection_should_not_be_updated_test(Config) ->
    storage_import_test_base:dir_and_its_child_with_data_and_metadata_protection_should_not_be_updated_test(Config, ?S3_HELPER_NAME).

dir_and_its_child_with_data_protection_should_not_be_deleted_test(Config) ->
    storage_import_test_base:dir_and_its_child_with_data_protection_should_not_be_deleted_test(Config).

dir_and_its_child_with_data_and_metadata_protection_should_not_be_deleted_test(Config) ->
    storage_import_test_base:dir_and_its_child_with_data_and_metadata_protection_should_not_be_deleted_test(Config).

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