%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests storage import continuous (update) scans on S3 storage.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_update_s3_test_SUITE).
-author("Bartosz Walkowicz").

-include("storage_import_test.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    %% --- modifications ---
    append_file_update_test/1,
    append_empty_file_update_test/1,
    truncate_file_update_test/1,
    change_file_content_constant_size_test/1,
    change_file_content_update_test/1,
    replace_file_with_dir_test/1,
    replace_non_empty_dir_with_file_test/1,
    replace_remotely_created_non_empty_dir_with_file_test/1,
    create_file_in_dir_update_test/1,
    create_file_in_dir_exceed_batch_update_test/1,

    %% --- idempotency ---
    should_not_process_file_with_unchanged_attrs_hash_test/1,

    %% --- retry ---
    update_syncs_files_after_import_failed_test/1,
    update_syncs_files_after_previous_update_failed_test/1,

    %% --- suffixes ---
    should_not_import_recreated_file_with_suffix_on_storage_test/1,
    should_update_blocks_of_recreated_file_with_suffix_on_storage_test/1,
    should_not_import_replicated_file_with_suffix_on_storage_test/1,
    should_update_replicated_file_with_suffix_on_storage_test/1,

    %% --- config ---
    changing_max_depth_test/1,
    force_start_test/1,
    force_stop_test/1,

    %% --- protection ---
    file_with_data_protection_should_not_be_updated_test/1,
    file_with_data_and_metadata_protection_should_not_be_updated_test/1,
    file_with_data_protection_should_not_be_deleted_test/1,
    file_with_data_and_metadata_protection_should_not_be_deleted_test/1,
    dir_and_its_child_with_data_protection_should_not_be_deleted_test/1,
    dir_and_its_child_with_data_and_metadata_protection_should_not_be_deleted_test/1,

    %% --- not reimported ---
    should_not_reimport_file_that_was_not_successfully_deleted_from_storage_test/1,
    should_not_reimport_deleted_but_still_opened_file_test/1,
    should_not_delete_not_replicated_file_created_in_remote_provider_test/1,
    should_not_delete_dir_created_in_remote_provider_test/1,
    should_not_delete_not_replicated_file_in_dir_created_in_remote_provider_test/1,
    should_not_sync_file_during_replication_test/1,
    should_not_invalidate_file_after_replication_test/1
]).

all() -> [
    %% --- modifications ---
    append_file_update_test,
    append_empty_file_update_test,
    truncate_file_update_test,
    change_file_content_constant_size_test,
    change_file_content_update_test,
    replace_file_with_dir_test,
    replace_non_empty_dir_with_file_test,
    replace_remotely_created_non_empty_dir_with_file_test,
    create_file_in_dir_update_test,
    create_file_in_dir_exceed_batch_update_test,

    %% --- idempotency ---
    should_not_process_file_with_unchanged_attrs_hash_test,

    %% --- retry ---
    update_syncs_files_after_import_failed_test,
    update_syncs_files_after_previous_update_failed_test,

    %% --- suffixes ---
    should_not_import_recreated_file_with_suffix_on_storage_test,
    should_update_blocks_of_recreated_file_with_suffix_on_storage_test,
    should_not_import_replicated_file_with_suffix_on_storage_test,
    should_update_replicated_file_with_suffix_on_storage_test,

    %% --- config ---
    changing_max_depth_test,
    force_start_test,
    force_stop_test,

    %% --- protection ---
    %% NOTE: the empty_dir_* variants are POSIX-only (an empty directory has no
    %% storage object on S3) and so are the dir_and_its_child_*_updated variants
    %% (see the doc of their test base for the flat-storage re-application gap)
    file_with_data_protection_should_not_be_updated_test,
    file_with_data_and_metadata_protection_should_not_be_updated_test,
    file_with_data_protection_should_not_be_deleted_test,
    file_with_data_and_metadata_protection_should_not_be_deleted_test,
    dir_and_its_child_with_data_protection_should_not_be_deleted_test,
    dir_and_its_child_with_data_and_metadata_protection_should_not_be_deleted_test,

    %% --- not reimported ---
    should_not_reimport_file_that_was_not_successfully_deleted_from_storage_test,
    should_not_reimport_deleted_but_still_opened_file_test,
    should_not_delete_not_replicated_file_created_in_remote_provider_test,
    should_not_delete_dir_created_in_remote_provider_test,
    should_not_delete_not_replicated_file_in_dir_created_in_remote_provider_test,
    should_not_sync_file_during_replication_test,
    should_not_invalidate_file_after_replication_test
].

-define(IMPORTING_PROVIDER_SELECTOR, krakow).

-define(SUITE_CTX, #storage_import_test_suite_ctx{
    storage_type = s3,
    importing_provider_selector = ?IMPORTING_PROVIDER_SELECTOR,
    non_importing_provider_selector = paris,
    space_owner_selector = space_owner
}).
-define(run_test(), storage_import_update_test_base:?FUNCTION_NAME(?SUITE_CTX)).


%%%==================================================================
%%% Test functions
%%%===================================================================


%% --- modifications ---


append_file_update_test(_Config) -> ?run_test().
append_empty_file_update_test(_Config) -> ?run_test().
truncate_file_update_test(_Config) -> ?run_test().
change_file_content_constant_size_test(_Config) -> ?run_test().
change_file_content_update_test(_Config) -> ?run_test().
replace_file_with_dir_test(_Config) -> ?run_test().
replace_non_empty_dir_with_file_test(_Config) -> ?run_test().
replace_remotely_created_non_empty_dir_with_file_test(_Config) -> ?run_test().
create_file_in_dir_update_test(_Config) -> ?run_test().
create_file_in_dir_exceed_batch_update_test(_Config) -> ?run_test().


%% --- idempotency ---


should_not_process_file_with_unchanged_attrs_hash_test(_Config) -> ?run_test().


%% --- retry ---


update_syncs_files_after_import_failed_test(_Config) -> ?run_test().
update_syncs_files_after_previous_update_failed_test(_Config) -> ?run_test().


%% --- suffixes ---


should_not_import_recreated_file_with_suffix_on_storage_test(_Config) -> ?run_test().
should_update_blocks_of_recreated_file_with_suffix_on_storage_test(_Config) -> ?run_test().
should_not_import_replicated_file_with_suffix_on_storage_test(_Config) -> ?run_test().
should_update_replicated_file_with_suffix_on_storage_test(_Config) -> ?run_test().


%% --- config ---


changing_max_depth_test(_Config) -> ?run_test().
force_start_test(_Config) -> ?run_test().
force_stop_test(_Config) -> ?run_test().


%% --- protection ---


file_with_data_protection_should_not_be_updated_test(_Config) -> ?run_test().
file_with_data_and_metadata_protection_should_not_be_updated_test(_Config) -> ?run_test().
file_with_data_protection_should_not_be_deleted_test(_Config) -> ?run_test().
file_with_data_and_metadata_protection_should_not_be_deleted_test(_Config) -> ?run_test().
dir_and_its_child_with_data_protection_should_not_be_deleted_test(_Config) -> ?run_test().
dir_and_its_child_with_data_and_metadata_protection_should_not_be_deleted_test(_Config) -> ?run_test().


%% --- not reimported ---


should_not_reimport_file_that_was_not_successfully_deleted_from_storage_test(_Config) -> ?run_test().
should_not_reimport_deleted_but_still_opened_file_test(_Config) -> ?run_test().
should_not_delete_not_replicated_file_created_in_remote_provider_test(_Config) -> ?run_test().
should_not_delete_dir_created_in_remote_provider_test(_Config) -> ?run_test().
should_not_delete_not_replicated_file_in_dir_created_in_remote_provider_test(_Config) -> ?run_test().
should_not_sync_file_during_replication_test(_Config) -> ?run_test().
should_not_invalidate_file_after_replication_test(_Config) -> ?run_test().


%%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    ModulesToLoad = [
        ?MODULE, sd_test_utils, storage_file_setup_utils,
        storage_import_test_utils, storage_import_update_test_base
    ],
    opt:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "2op_s3",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {datastore_links_tree_order, 100},
            {cache_to_disk_delay_ms, timer:seconds(1)},
            {cache_to_disk_force_delay_ms, timer:seconds(2)}
        ]}],
        posthook = fun(NewConfig) ->
            storage_import_test_utils:clean_up_after_previous_run(all(), ?SUITE_CTX),
            storage_import_test_utils:mock_space_dir_statbuf_on_flat_storage(?IMPORTING_PROVIDER_SELECTOR),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    storage_import_test_utils:unmock_space_dir_statbuf_on_flat_storage(?IMPORTING_PROVIDER_SELECTOR),
    oct_background:end_per_suite().


init_per_testcase(Case, Config) ->
    storage_import_update_test_base:init_per_testcase(Case, ?SUITE_CTX, Config).


end_per_testcase(Case, Config) ->
    storage_import_update_test_base:end_per_testcase(Case, ?SUITE_CTX, Config).
