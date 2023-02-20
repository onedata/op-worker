%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage import
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_update_test_SUITE).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    update_syncs_files_after_import_failed_test/1,
    update_syncs_files_after_previous_update_failed_test/1,
    sync_should_not_reimport_deleted_but_still_opened_file/1,
    sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage/1,
    sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage/1,
    sync_should_not_import_recreated_file_with_suffix_on_storage/1,
    sync_should_update_blocks_of_recreated_file_with_suffix_on_storage/1,
    sync_should_not_import_replicated_file_with_suffix_on_storage/1,
    sync_should_update_replicated_file_with_suffix_on_storage/1,
    sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed/1,
    create_delete_import2_test/1,
    create_subfiles_and_delete_before_import_is_finished_test/1,
    create_file_in_dir_update_test/1,
    changing_max_depth_test/1,
    create_file_in_dir_exceed_batch_update_test/1,
    force_start_test/1,
    force_stop_test/1,
    file_with_data_protection_should_not_be_updated_test/1,
    file_with_data_and_metadata_protection_should_not_be_updated_test/1,
    file_with_data_protection_should_not_be_deleted_test/1,
    file_with_data_and_metadata_protection_should_not_be_deleted_test/1,
    empty_dir_with_data_protection_should_not_be_updated_test/1,
    empty_dir_with_data_and_metadata_protection_should_not_be_updated_test/1,
    empty_dir_with_data_protection_should_not_be_deleted_test/1,
    empty_dir_with_data_and_metadata_protection_should_not_be_deleted_test/1,
    dir_and_its_child_with_data_protection_should_not_be_updated_test/1,
    dir_and_its_child_with_data_and_metadata_protection_should_not_be_updated_test/1,
    dir_and_its_child_with_data_protection_should_not_be_deleted_test/1,
    dir_and_its_child_with_data_and_metadata_protection_should_not_be_deleted_test/1
]).

-define(TEST_CASES, [
    update_syncs_files_after_import_failed_test,
    update_syncs_files_after_previous_update_failed_test,
    % todo VFS-5203 add more tests of sync handling suffixed files
    sync_should_not_reimport_deleted_but_still_opened_file,
    sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage,
    sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage,
    sync_should_not_import_recreated_file_with_suffix_on_storage,
    sync_should_update_blocks_of_recreated_file_with_suffix_on_storage,
    sync_should_not_import_replicated_file_with_suffix_on_storage,
    sync_should_update_replicated_file_with_suffix_on_storage,
    sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed,
    create_delete_import2_test,
    create_subfiles_and_delete_before_import_is_finished_test,
    create_file_in_dir_update_test,
    changing_max_depth_test,
    create_file_in_dir_exceed_batch_update_test,
    force_start_test,
    force_stop_test,
    file_with_data_protection_should_not_be_updated_test,
    file_with_data_and_metadata_protection_should_not_be_updated_test,
    file_with_data_protection_should_not_be_deleted_test,
    file_with_data_and_metadata_protection_should_not_be_deleted_test,
    empty_dir_with_data_protection_should_not_be_updated_test,
    empty_dir_with_data_and_metadata_protection_should_not_be_updated_test,
    empty_dir_with_data_protection_should_not_be_deleted_test,
    empty_dir_with_data_and_metadata_protection_should_not_be_deleted_test,
    dir_and_its_child_with_data_protection_should_not_be_updated_test,
    dir_and_its_child_with_data_and_metadata_protection_should_not_be_updated_test,
    dir_and_its_child_with_data_protection_should_not_be_deleted_test,
    dir_and_its_child_with_data_and_metadata_protection_should_not_be_deleted_test
]).

all() -> ?ALL(?TEST_CASES).

%%%==================================================================
%%% Test functions
%%%===================================================================

update_syncs_files_after_import_failed_test(Config) ->
    storage_import_test_base:update_syncs_files_after_import_failed_test(Config).

update_syncs_files_after_previous_update_failed_test(Config) ->
    storage_import_test_base:update_syncs_files_after_previous_update_failed_test(Config).

sync_should_not_reimport_deleted_but_still_opened_file(Config) ->
    storage_import_test_base:sync_should_not_reimport_deleted_but_still_opened_file(Config, ?POSIX_HELPER_NAME).

sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage(Config) ->
    storage_import_test_base:sync_should_not_reimport_directory_that_was_not_successfully_deleted_from_storage(Config).

sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage(Config) ->
    storage_import_test_base:sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage(Config, ?POSIX_HELPER_NAME).

sync_should_not_import_recreated_file_with_suffix_on_storage(Config) ->
    storage_import_test_base:sync_should_not_import_recreated_file_with_suffix_on_storage(Config, ?POSIX_HELPER_NAME).

sync_should_update_blocks_of_recreated_file_with_suffix_on_storage(Config) ->
    storage_import_test_base:sync_should_update_blocks_of_recreated_file_with_suffix_on_storage(Config, ?POSIX_HELPER_NAME).

sync_should_not_import_replicated_file_with_suffix_on_storage(Config) ->
    storage_import_test_base:sync_should_not_import_replicated_file_with_suffix_on_storage(Config, ?POSIX_HELPER_NAME).

sync_should_update_replicated_file_with_suffix_on_storage(Config) ->
    storage_import_test_base:sync_should_update_replicated_file_with_suffix_on_storage(Config, ?POSIX_HELPER_NAME).

sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed(Config) ->
    storage_import_test_base:sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed(Config).

create_delete_import2_test(Config) ->
    storage_import_test_base:create_delete_import2_test(Config).

create_subfiles_and_delete_before_import_is_finished_test(Config) ->
    storage_import_test_base:create_subfiles_and_delete_before_import_is_finished_test(Config).

create_file_in_dir_update_test(Config) ->
    storage_import_test_base:create_file_in_dir_update_test(Config).

changing_max_depth_test(Config) ->
    storage_import_test_base:changing_max_depth_test(Config).

create_file_in_dir_exceed_batch_update_test(Config) ->
    storage_import_test_base:create_file_in_dir_exceed_batch_update_test(Config).

force_start_test(Config) ->
    storage_import_test_base:force_start_test(Config).

force_stop_test(Config) ->
    storage_import_test_base:force_stop_test(Config).

file_with_data_protection_should_not_be_updated_test(Config) ->
    storage_import_test_base:file_with_data_protection_should_not_be_updated_test(Config, ?POSIX_HELPER_NAME).

file_with_data_and_metadata_protection_should_not_be_updated_test(Config) ->
    storage_import_test_base:file_with_data_and_metadata_protection_should_not_be_updated_test(Config, ?POSIX_HELPER_NAME).

file_with_data_protection_should_not_be_deleted_test(Config) ->
    storage_import_test_base:file_with_data_protection_should_not_be_deleted_test(Config, ?POSIX_HELPER_NAME).

file_with_data_and_metadata_protection_should_not_be_deleted_test(Config) ->
    storage_import_test_base:file_with_data_and_metadata_protection_should_not_be_deleted_test(Config, ?POSIX_HELPER_NAME).

empty_dir_with_data_protection_should_not_be_updated_test(Config) ->
    storage_import_test_base:empty_dir_with_data_protection_should_not_be_updated_test(Config).

empty_dir_with_data_and_metadata_protection_should_not_be_updated_test(Config) ->
    storage_import_test_base:empty_dir_with_data_and_metadata_protection_should_not_be_updated_test(Config).

empty_dir_with_data_protection_should_not_be_deleted_test(Config) ->
    storage_import_test_base:empty_dir_with_data_protection_should_not_be_deleted_test(Config).

empty_dir_with_data_and_metadata_protection_should_not_be_deleted_test(Config) ->
    storage_import_test_base:empty_dir_with_data_and_metadata_protection_should_not_be_deleted_test(Config).

dir_and_its_child_with_data_protection_should_not_be_updated_test(Config) ->
    storage_import_test_base:dir_and_its_child_with_data_protection_should_not_be_updated_test(Config, ?POSIX_HELPER_NAME).

dir_and_its_child_with_data_and_metadata_protection_should_not_be_updated_test(Config) ->
    storage_import_test_base:dir_and_its_child_with_data_and_metadata_protection_should_not_be_updated_test(Config, ?POSIX_HELPER_NAME).

dir_and_its_child_with_data_protection_should_not_be_deleted_test(Config) ->
    storage_import_test_base:dir_and_its_child_with_data_protection_should_not_be_deleted_test(Config).

dir_and_its_child_with_data_and_metadata_protection_should_not_be_deleted_test(Config) ->
    storage_import_test_base:dir_and_its_child_with_data_and_metadata_protection_should_not_be_deleted_test(Config).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    storage_import_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    storage_import_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    storage_import_test_base:init_per_testcase(Case, Config).

end_per_testcase(_Case, Config) ->
    storage_import_test_base:end_per_testcase(_Case, Config).