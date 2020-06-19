%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage_sync on s3 storage.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_sync_s3_test_SUITE).
-author("Jakub Kudzia").

-include("storage_sync_test.hrl").
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
    empty_import_test/1,
    create_empty_file_import_test/1,
    create_file_import_test/1,
    create_delete_import_test_read_both/1,
    create_delete_import_test_read_remote_only/1,
    create_file_in_dir_import_test/1,
    create_subfiles_import_many_test/1,
    create_subfiles_import_many2_test/1,
    create_remote_file_import_conflict_test/1,
    create_remote_file_import_race_test/1,
    cancel_scan/1,
    create_file_import_race_test/1,
    close_file_import_race_test/1,
    delete_file_reimport_race_test/1,
    delete_opened_file_reimport_race_test/1,

    % tests of update
    update_syncs_files_after_import_failed_test/1,
    update_syncs_files_after_previous_update_failed_test/1,
    sync_should_not_reimport_deleted_but_still_opened_file/1,
    sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage/1,
    sync_should_not_import_recreated_file_with_suffix_on_storage/1,
    sync_should_update_blocks_of_recreated_file_with_suffix_on_storage/1,
    sync_should_not_import_replicated_file_with_suffix_on_storage/1,
    sync_should_update_replicated_file_with_suffix_on_storage/1,
    sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed/1,
    create_delete_import2_test/1,
    create_subfiles_and_delete_before_import_is_finished_test/1,
    create_file_in_dir_update_test/1,
    create_file_in_dir_exceed_batch_update_test/1,

    delete_non_empty_directory_update_test/1,
    sync_works_properly_after_delete_test/1,
    delete_and_update_files_simultaneously_update_test/1,
    delete_file_update_test/1,
    delete_file_in_dir_update_test/1,
    delete_many_subfiles_test/1,
    create_delete_race_test/1,
    create_list_race_test/1,

    append_file_update_test/1,
    append_empty_file_update_test/1,
    truncate_file_update_test/1,
    change_file_content_constant_size_test/1,
    change_file_content_update_test/1,
    change_file_type_test/1,
    change_file_type3_test/1,
    change_file_type4_test/1,
    recreate_file_deleted_by_sync_test/1,
    sync_should_not_delete_not_replicated_file_created_in_remote_provider/1,
    sync_should_not_delete_dir_created_in_remote_provider/1,
    sync_should_not_delete_not_replicated_files_created_in_remote_provider2/1,
    should_not_sync_file_during_replication/1,
    sync_should_not_invalidate_file_after_replication/1
]).

-define(TEST_CASES, [
    % tests of import
    empty_import_test,
    create_empty_file_import_test,
    create_file_import_test,
    create_delete_import_test_read_both,
    create_delete_import_test_read_remote_only,
    create_file_in_dir_import_test,
    create_subfiles_import_many_test,
    create_subfiles_import_many2_test,
    create_remote_file_import_conflict_test,
    create_remote_file_import_race_test,
    cancel_scan,
    create_file_import_race_test,
    close_file_import_race_test,
    delete_file_reimport_race_test,
    delete_opened_file_reimport_race_test,

    % tests of update
    update_syncs_files_after_import_failed_test,
    update_syncs_files_after_previous_update_failed_test,
    sync_should_not_reimport_deleted_but_still_opened_file,
    sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage,
    sync_should_not_import_recreated_file_with_suffix_on_storage,
    sync_should_update_blocks_of_recreated_file_with_suffix_on_storage,
    sync_should_not_import_replicated_file_with_suffix_on_storage,
    sync_should_update_replicated_file_with_suffix_on_storage,
    sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed,
    create_delete_import2_test,
    create_subfiles_and_delete_before_import_is_finished_test,
    create_file_in_dir_update_test,
    create_file_in_dir_exceed_batch_update_test,

    delete_non_empty_directory_update_test,
    sync_works_properly_after_delete_test,
    delete_and_update_files_simultaneously_update_test,
    delete_file_update_test,
    delete_file_in_dir_update_test,
    delete_many_subfiles_test,
    create_delete_race_test,
    create_list_race_test,

    append_file_update_test,
    append_empty_file_update_test,
    truncate_file_update_test,
    change_file_content_constant_size_test,
    change_file_content_update_test,
    change_file_type_test,
    change_file_type3_test,
    change_file_type4_test,
    recreate_file_deleted_by_sync_test,
    sync_should_not_delete_not_replicated_file_created_in_remote_provider,
    sync_should_not_delete_dir_created_in_remote_provider,
    sync_should_not_delete_not_replicated_files_created_in_remote_provider2,
    should_not_sync_file_during_replication,
    sync_should_not_invalidate_file_after_replication
]).

all() -> ?ALL(?TEST_CASES).

%%%==================================================================
%%% Test functions
%%%===================================================================

empty_import_test(Config) ->
    storage_sync_test_base:empty_import_test(Config).

create_empty_file_import_test(Config) ->
    storage_sync_test_base:create_empty_file_import_test(Config, false).

create_file_import_test(Config) ->
    storage_sync_test_base:create_file_import_test(Config, false).

create_delete_import_test_read_both(Config) ->
    storage_sync_test_base:create_delete_import_test_read_both(Config, false).

create_delete_import_test_read_remote_only(Config) ->
    storage_sync_test_base:create_delete_import_test_read_remote_only(Config, false).

create_file_in_dir_import_test(Config) ->
    storage_sync_s3_test_base:create_file_in_dir_import_test(Config, false).

create_subfiles_import_many_test(Config) ->
    storage_sync_s3_test_base:create_subfiles_import_many_test(Config, false).

create_subfiles_import_many2_test(Config) ->
    storage_sync_s3_test_base:create_subfiles_import_many2_test(Config, false).

create_remote_file_import_conflict_test(Config) ->
    storage_sync_test_base:create_remote_file_import_conflict_test(Config, false).

create_remote_file_import_race_test(Config) ->
    storage_sync_test_base:create_remote_file_import_race_test(Config, false).

cancel_scan(Config) ->
    storage_sync_s3_test_base:cancel_scan(Config, false).

create_file_import_race_test(Config) ->
    storage_sync_test_base:create_file_import_race_test(Config).

close_file_import_race_test(Config) ->
    storage_sync_test_base:close_file_import_race_test(Config, ?S3_HELPER_NAME).

delete_file_reimport_race_test(Config) ->
    storage_sync_test_base:delete_file_reimport_race_test(Config, ?S3_HELPER_NAME).

delete_opened_file_reimport_race_test(Config) ->
    storage_sync_test_base:delete_opened_file_reimport_race_test(Config, ?S3_HELPER_NAME).


update_syncs_files_after_import_failed_test(Config) ->
    storage_sync_s3_test_base:update_syncs_files_after_import_failed_test(Config, false).

update_syncs_files_after_previous_update_failed_test(Config) ->
    storage_sync_s3_test_base:update_syncs_files_after_previous_update_failed_test(Config, false).

sync_should_not_reimport_deleted_but_still_opened_file(Config) ->
    storage_sync_s3_test_base:sync_should_not_reimport_deleted_but_still_opened_file(Config, ?S3_HELPER_NAME).

sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage(Config) ->
    storage_sync_test_base:sync_should_not_reimport_file_that_was_not_successfully_deleted_from_storage(Config, ?S3_HELPER_NAME).

sync_should_not_import_recreated_file_with_suffix_on_storage(Config) ->
    storage_sync_test_base:sync_should_not_import_recreated_file_with_suffix_on_storage(Config, ?S3_HELPER_NAME).

sync_should_update_blocks_of_recreated_file_with_suffix_on_storage(Config) ->
    storage_sync_test_base:sync_should_update_blocks_of_recreated_file_with_suffix_on_storage(Config, ?S3_HELPER_NAME).

sync_should_not_import_replicated_file_with_suffix_on_storage(Config) ->
    storage_sync_test_base:sync_should_not_import_replicated_file_with_suffix_on_storage(Config, ?S3_HELPER_NAME).

sync_should_update_replicated_file_with_suffix_on_storage(Config) ->
    storage_sync_test_base:sync_should_update_replicated_file_with_suffix_on_storage(Config, ?S3_HELPER_NAME).

sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed(Config) ->
    storage_sync_s3_test_base:sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed(Config, false).

create_delete_import2_test(Config) ->
    storage_sync_test_base:create_delete_import2_test(Config, false, true).

create_subfiles_and_delete_before_import_is_finished_test(Config) ->
    storage_sync_s3_test_base:create_subfiles_and_delete_before_import_is_finished_test(Config, false).

create_file_in_dir_update_test(Config) ->
    storage_sync_s3_test_base:create_file_in_dir_update_test(Config, false).

create_file_in_dir_exceed_batch_update_test(Config) ->
    storage_sync_s3_test_base:create_file_in_dir_exceed_batch_update_test(Config, false).


delete_non_empty_directory_update_test(Config) ->
    storage_sync_s3_test_base:delete_non_empty_directory_update_test(Config, false).

sync_works_properly_after_delete_test(Config) ->
    storage_sync_s3_test_base:sync_works_properly_after_delete_test(Config, false).

delete_and_update_files_simultaneously_update_test(Config) ->
    storage_sync_s3_test_base:delete_and_update_files_simultaneously_update_test(Config, false).

delete_file_update_test(Config) ->
    storage_sync_test_base:delete_file_update_test(Config, false).

delete_file_in_dir_update_test(Config) ->
    storage_sync_s3_test_base:delete_file_in_dir_update_test(Config, false).

delete_many_subfiles_test(Config) ->
    storage_sync_s3_test_base:delete_many_subfiles_test(Config, false).

create_delete_race_test(Config) ->
    storage_sync_test_base:create_delete_race_test(Config, false, ?S3_HELPER_NAME).

create_list_race_test(Config) ->
    storage_sync_s3_test_base:create_list_race_test(Config, false).


append_file_update_test(Config) ->
    storage_sync_test_base:append_file_update_test(Config, false).

append_empty_file_update_test(Config) ->
    storage_sync_test_base:append_empty_file_update_test(Config, false).

truncate_file_update_test(Config) ->
    storage_sync_test_base:truncate_file_update_test(Config, false).

change_file_content_constant_size_test(Config) ->
    storage_sync_test_base:change_file_content_constant_size_test(Config, false).

change_file_content_update_test(Config) ->
    storage_sync_test_base:change_file_content_update_test(Config, false).

change_file_type_test(Config) ->
    storage_sync_s3_test_base:change_file_type_test(Config, false).

change_file_type3_test(Config) ->
    storage_sync_s3_test_base:change_file_type3_test(Config, false).

change_file_type4_test(Config) ->
    storage_sync_test_base:change_file_type4_test(Config, false).

recreate_file_deleted_by_sync_test(Config) ->
    storage_sync_test_base:recreate_file_deleted_by_sync_test(Config, false).

sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config) ->
    storage_sync_test_base:sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config, false).

sync_should_not_delete_dir_created_in_remote_provider(Config) ->
    storage_sync_test_base:sync_should_not_delete_dir_created_in_remote_provider(Config, false).

sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config) ->
    storage_sync_test_base:sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config, false).

should_not_sync_file_during_replication(Config) ->
    % file size limit that can be created via lfm on s3
    storage_sync_test_base:should_not_sync_file_during_replication(Config).

sync_should_not_invalidate_file_after_replication(Config) ->
    storage_sync_test_base:sync_should_not_invalidate_file_after_replication(Config).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    storage_sync_s3_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    storage_sync_s3_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    storage_sync_s3_test_base:init_per_testcase(Case, Config, false).

end_per_testcase(Case, Config) ->
    storage_sync_s3_test_base:end_per_testcase(Case, Config, false).