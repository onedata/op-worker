%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage_sync on readonly s3 storage.
%%% Contrary to storage_sync_readonly_test_SUITE, all tests in this
%%% suite call tests from storage_sync_s3_test_base.
%%% This is because directories on s3 do not exist so their
%%% atimes do not change when sync traverses storage.
%%% @end
%%%--------------------------------------------------------------------
-module(storage_sync_readonly_s3_test_SUITE).

-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    empty_import_fails_due_to_enoent_test/1,
    create_empty_file_import_test/1,
    create_file_import_test/1,
    import_file_with_link_but_no_doc_test/1,
    create_file_in_dir_import_test/1,
    create_subfiles_import_many_test/1,
    create_subfiles_import_many2_test/1,
    cancel_scan/1,
    create_file_in_dir_update_test/1,
    create_file_in_dir_exceed_batch_update_test/1,
    update_syncs_files_after_import_failed_test/1,
    update_syncs_files_after_previous_update_failed_test/1,
    sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed/1,
    create_subfiles_and_delete_before_import_is_finished_test/1,
    delete_non_empty_directory_update_test/1,
    sync_works_properly_after_delete_test/1,
    delete_and_update_files_simultaneously_update_test/1,
    delete_file_update_test/1,
    delete_many_subfiles_test/1,
    append_file_update_test/1,
    append_empty_file_update_test/1,
    truncate_file_update_test/1,
    change_file_content_constant_size_test/1,
    change_file_content_update_test/1,
    recreate_file_deleted_by_sync_test/1,
    sync_should_not_delete_not_replicated_file_created_in_remote_provider/1,
    sync_should_not_delete_dir_created_in_remote_provider/1,
    sync_should_not_delete_not_replicated_files_created_in_remote_provider2/1
]).

-define(TEST_CASES, [
    % tests of import
    empty_import_fails_due_to_enoent_test,
    create_empty_file_import_test,
    create_file_import_test,
    import_file_with_link_but_no_doc_test,
    create_file_in_dir_import_test,
    create_subfiles_import_many_test,
    create_subfiles_import_many2_test,
    cancel_scan,

    % tests of update
    update_syncs_files_after_import_failed_test,
    update_syncs_files_after_previous_update_failed_test,
    sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed,
    create_subfiles_and_delete_before_import_is_finished_test,
    create_file_in_dir_update_test,
    create_file_in_dir_exceed_batch_update_test,
    delete_non_empty_directory_update_test,
    sync_works_properly_after_delete_test,
    delete_and_update_files_simultaneously_update_test,
    delete_file_update_test,
    delete_many_subfiles_test,
    append_file_update_test,
    append_empty_file_update_test,
    truncate_file_update_test,
    change_file_content_constant_size_test,
    change_file_content_update_test,
    sync_should_not_delete_not_replicated_file_created_in_remote_provider,
    sync_should_not_delete_dir_created_in_remote_provider,
    sync_should_not_delete_not_replicated_files_created_in_remote_provider2
]).

all() -> ?ALL(?TEST_CASES).

-define(WRITE_TEXT, <<"overwrite_test_data">>).

%%%==================================================================
%%% Test functions
%%%===================================================================

empty_import_fails_due_to_enoent_test(Config) ->
    storage_sync_test_base:empty_import_test(Config).

create_empty_file_import_test(Config) ->
    storage_sync_test_base:create_empty_file_import_test(Config, true).

create_file_import_test(Config) ->
    storage_sync_test_base:create_file_import_test(Config, true).

import_file_with_link_but_no_doc_test(Config) ->
    storage_sync_s3_test_base:import_file_with_link_but_no_doc_test(Config, true).

create_file_in_dir_import_test(Config) ->
    storage_sync_s3_test_base:create_file_in_dir_import_test(Config, true).

create_subfiles_import_many_test(Config) ->
    storage_sync_s3_test_base:create_subfiles_import_many_test(Config, true).

create_subfiles_import_many2_test(Config) ->
    storage_sync_s3_test_base:create_subfiles_import_many2_test(Config, true).

cancel_scan(Config) ->
    storage_sync_s3_test_base:cancel_scan(Config, true).

create_file_in_dir_update_test(Config) ->
    storage_sync_s3_test_base:create_file_in_dir_update_test(Config, true).

create_file_in_dir_exceed_batch_update_test(Config) ->
    storage_sync_s3_test_base:create_file_in_dir_exceed_batch_update_test(Config, true).

delete_non_empty_directory_update_test(Config) ->
    storage_sync_s3_test_base:delete_non_empty_directory_update_test(Config, true).

sync_works_properly_after_delete_test(Config) ->
    storage_sync_s3_test_base:sync_works_properly_after_delete_test(Config, true).

delete_and_update_files_simultaneously_update_test(Config) ->
    storage_sync_s3_test_base:delete_and_update_files_simultaneously_update_test(Config, true).

delete_file_update_test(Config) ->
    storage_sync_test_base:delete_file_update_test(Config, true).

delete_many_subfiles_test(Config) ->
    storage_sync_s3_test_base:delete_many_subfiles_test(Config, true).

update_syncs_files_after_import_failed_test(Config) ->
    storage_sync_test_base:update_syncs_files_after_import_failed_test(Config, true).

update_syncs_files_after_previous_update_failed_test(Config) ->
    storage_sync_test_base:update_syncs_files_after_previous_update_failed_test(Config, true).

sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed(Config) ->
    storage_sync_test_base:sync_should_not_process_file_if_hash_of_its_attrs_has_not_changed(Config, true).

create_subfiles_and_delete_before_import_is_finished_test(Config) ->
    storage_sync_s3_test_base:create_subfiles_and_delete_before_import_is_finished_test(Config, true).

append_file_update_test(Config) ->
    storage_sync_test_base:append_file_update_test(Config, true).

append_empty_file_update_test(Config) ->
    storage_sync_test_base:append_empty_file_update_test(Config, true).

truncate_file_update_test(Config) ->
    storage_sync_test_base:truncate_file_update_test(Config, true).

change_file_content_constant_size_test(Config) ->
    storage_sync_test_base:change_file_content_constant_size_test(Config, true).

change_file_content_update_test(Config) ->
    storage_sync_test_base:change_file_content_update_test(Config, true).

recreate_file_deleted_by_sync_test(Config) ->
    storage_sync_test_base:recreate_file_deleted_by_sync_test(Config, true).

sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config) ->
    storage_sync_test_base:sync_should_not_delete_not_replicated_file_created_in_remote_provider(Config, true).

sync_should_not_delete_dir_created_in_remote_provider(Config) ->
    storage_sync_test_base:sync_should_not_delete_dir_created_in_remote_provider(Config, true).

sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config) ->
    storage_sync_test_base:sync_should_not_delete_not_replicated_files_created_in_remote_provider2(Config, true).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    storage_sync_s3_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    storage_sync_s3_test_base:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    storage_sync_s3_test_base:init_per_testcase(Case, Config, true).

end_per_testcase(Case, Config) ->
    storage_sync_s3_test_base:end_per_testcase(Case, Config, true).