%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains base test functions for testing storage import
%%% continuous (update) scans.
%%%
%%% While storage_import_initial_oct_test_base covers the first import of a
%%% storage, this module covers what subsequent (continuous) scans detect and
%%% how the scanning engine behaves: modifications to already-imported files,
%%% idempotency (unchanged entries are not reprocessed), retrying after a
%%% failed scan, scan configuration (max_depth, force start/stop, batch
%%% handling) and entries a scan must NOT touch (leftovers of failed deletions,
%%% remote-only entries, replication targets).
%%%
%%% The generic machinery lives in storage_import_test_utils, whose module doc
%%% is also the CANONICAL description of scan classification, monitoring
%%% counters and flat-storage divergences (the "Scan classification and
%%% counters" and "Flat (object) storage divergences" sections). Docs below
%%% assume those general facts throughout and note only what is specific to
%%% their own scenario.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_update_oct_test_base).
-author("Bartosz Walkowicz").

-include("storage_import_oct_test.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/acl.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_delete.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/fuse_messages.hrl").

%% Attempts (polled every 1s) for awaiting a file replication transfer - slowed
%% down deliberately in should_not_sync_file_during_replication_test.
-define(TRANSFER_ATTEMPTS, 120).


% API
-export([
    init_per_testcase/3,
    end_per_testcase/3
]).

%% tests
-export([
    %% --- modifications ---
    append_file_update_test/1,
    append_file_not_changing_mtime_update_test/1,
    append_empty_file_update_test/1,
    truncate_file_update_test/1,
    chmod_file_update_test/1,
    chmod_file_update_in_batched_dir_test/1,
    move_file_update_test/1,
    copy_file_update_test/1,
    change_file_content_constant_size_test/1,
    change_file_content_update_test/1,
    change_file_content_the_same_moment_when_sync_performs_stat_on_file_test/1,
    replace_file_with_dir_test/1,
    replace_empty_dir_with_file_test/1,
    replace_non_empty_dir_with_file_test/1,
    update_timestamps_file_import_test/1,
    create_file_in_dir_update_test/1,
    create_file_in_dir_exceed_batch_update_test/1,
    update_nfs_acl_test/1,

    %% --- idempotency ---
    should_not_process_file_with_unchanged_attrs_hash_test/1,
    should_not_detect_timestamp_update_test/1,

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
    empty_dir_with_data_protection_should_not_be_updated_test/1,
    empty_dir_with_data_and_metadata_protection_should_not_be_updated_test/1,
    empty_dir_with_data_protection_should_not_be_deleted_test/1,
    empty_dir_with_data_and_metadata_protection_should_not_be_deleted_test/1,
    dir_and_its_child_with_data_protection_should_not_be_updated_test/1,
    dir_and_its_child_with_data_and_metadata_protection_should_not_be_updated_test/1,
    dir_and_its_child_with_data_protection_should_not_be_deleted_test/1,
    dir_and_its_child_with_data_and_metadata_protection_should_not_be_deleted_test/1,

    %% --- not reimported ---
    should_not_reimport_directory_that_was_not_successfully_deleted_from_storage_test/1,
    should_not_reimport_file_that_was_not_successfully_deleted_from_storage_test/1,
    should_not_delete_not_replicated_file_created_in_remote_provider_test/1,
    should_not_delete_dir_created_in_remote_provider_test/1,
    should_not_delete_not_replicated_file_in_dir_created_in_remote_provider_test/1,
    should_not_sync_file_during_replication_test/1
]).


%%%===================================================================
%%% API
%%%===================================================================


init_per_testcase(Case, TestSuiteCtx = #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector
}, Config) when
    Case =:= chmod_file_update_in_batched_dir_test;
    Case =:= create_file_in_dir_exceed_batch_update_test
->
    [Node | _] = Nodes = oct_background:get_provider_nodes(ImportingProviderSelector),
    {ok, OldDirBatchSize} = test_utils:get_env(Node, op_worker, storage_import_dir_batch_size),
    %% a low batch size forces the scan of the relevant directory's children to
    %% span multiple batches
    ok = test_utils:set_env(Nodes, op_worker, storage_import_dir_batch_size, 2),
    Config2 = [{old_storage_import_dir_batch_size, OldDirBatchSize} | Config],
    init_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config2);

init_per_testcase(Case = force_stop_test, TestSuiteCtx = #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector
}, Config) ->
    [Node | _] = Nodes = oct_background:get_provider_nodes(ImportingProviderSelector),
    {ok, OldDirBatchSize} = test_utils:get_env(Node, op_worker, storage_import_dir_batch_size),
    %% a batch size of 1 splits the scan of the test's large tree into the
    %% maximum number of separate traverse jobs, so that aborting the scan
    %% mid-flight reliably leaves a substantial part of the tree unprocessed
    ok = test_utils:set_env(Nodes, op_worker, storage_import_dir_batch_size, 1),
    Config2 = [{old_storage_import_dir_batch_size, OldDirBatchSize} | Config],
    init_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config2);

init_per_testcase(Case, TestSuiteCtx, Config) when
    Case =:= should_not_import_recreated_file_with_suffix_on_storage_test;
    Case =:= should_update_blocks_of_recreated_file_with_suffix_on_storage_test
->
    mock_opened_file_deletion_to_use_deletion_marker(TestSuiteCtx),
    init_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

init_per_testcase(_Case, _TestSuiteCtx, Config) ->
    lfm_proxy:init(Config).


end_per_testcase(Case = chmod_file_update_test, TestSuiteCtx, Config) ->
    unmock_storage_import_hash(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case, TestSuiteCtx = #storage_import_test_suite_ctx{
    storage_type = StorageType,
    importing_provider_selector = ImportingProviderSelector
}, Config) when
    Case =:= chmod_file_update_in_batched_dir_test;
    Case =:= create_file_in_dir_exceed_batch_update_test
->
    ?IF_POSIX(StorageType, begin
        unmock_storage_import_hash(TestSuiteCtx),
        unmock_storage_sync_traverse(TestSuiteCtx)
    end),
    Nodes = oct_background:get_provider_nodes(ImportingProviderSelector),
    OldDirBatchSize = ?config(old_storage_import_dir_batch_size, Config),
    ok = test_utils:set_env(Nodes, op_worker, storage_import_dir_batch_size, OldDirBatchSize),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case = create_file_in_dir_update_test, TestSuiteCtx = #storage_import_test_suite_ctx{
    storage_type = posix
}, Config) ->
    unmock_storage_import_hash(TestSuiteCtx),
    unmock_storage_sync_traverse(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case = update_nfs_acl_test, TestSuiteCtx, Config) ->
    storage_import_test_utils:unmock_storage_driver(TestSuiteCtx),
    storage_import_test_utils:unmock_luma(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case, TestSuiteCtx, Config) when
    Case =:= update_syncs_files_after_import_failed_test;
    Case =:= update_syncs_files_after_previous_update_failed_test
->
    %% on the happy path the mock is already torn down inline (the retrying
    %% scan must import for real) and this is a no-op; it matters only when
    %% the test fails before reaching the inline teardown
    storage_import_test_utils:unmock_storage_import_engine(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case = force_stop_test, TestSuiteCtx = #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector
}, Config) ->
    storage_import_test_utils:unmock_storage_import_engine(TestSuiteCtx),
    Nodes = oct_background:get_provider_nodes(ImportingProviderSelector),
    OldDirBatchSize = ?config(old_storage_import_dir_batch_size, Config),
    ok = test_utils:set_env(Nodes, op_worker, storage_import_dir_batch_size, OldDirBatchSize),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case, TestSuiteCtx, Config) when
    Case =:= should_not_import_recreated_file_with_suffix_on_storage_test;
    Case =:= should_update_blocks_of_recreated_file_with_suffix_on_storage_test
->
    unmock_fslogic_delete(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case, TestSuiteCtx, Config) when
    Case =:= should_not_reimport_directory_that_was_not_successfully_deleted_from_storage_test;
    Case =:= should_not_reimport_file_that_was_not_successfully_deleted_from_storage_test
->
    unmock_storage_helper(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case = should_not_sync_file_during_replication_test, TestSuiteCtx, Config) ->
    unmock_rtransfer(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(_Case, _TestSuiteCtx, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Tests
%%%===================================================================


%% --- modifications ---


append_file_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(),
    AppendedContent = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = InitialContent}, SuiteCtx,
        #{verify_dir_stats => true}
    ),

    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId,
        byte_size(InitialContent), AppendedContent
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    UpdatedFileSpec = #file_spec{
        name = FileName, content = <<InitialContent/binary, AppendedContent/binary>>
    },
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedFileSpec),
    storage_import_test_utils:verify_dir_stats(TestCaseCtx, UpdatedFileSpec),
    assert_monitoring_state_after_single_file_modification(TestCaseCtx).


%% Like append_file_update_test, but the file's mtime is forced back to its
%% pre-append value right after the write (as if the storage did not reliably
%% bump mtime) - the appended bytes must still be detected via the size clause
%% of the modification check. POSIX-only (relies on forcing the storage mtime).
append_file_not_changing_mtime_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(),
    AppendedContent = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = InitialContent}, SuiteCtx
    ),

    OldMtime = storage_file_setup_utils:get_mtime(
        ImportingProviderSelector, ImportedStorageId, StorageFileId
    ),
    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId,
        byte_size(InitialContent), AppendedContent
    ),
    storage_file_setup_utils:set_mtime(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, OldMtime
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    UpdatedFileSpec = #file_spec{
        name = FileName, content = <<InitialContent/binary, AppendedContent/binary>>
    },
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedFileSpec),
    assert_monitoring_state_after_single_file_modification(TestCaseCtx).


%% Like append_file_update_test, but the file is empty (0 bytes) at the time of
%% the initial scan - exercising the 0-byte-file import path.
append_empty_file_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    AppendedContent = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = <<>>}, SuiteCtx
    ),

    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, 0, AppendedContent
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    UpdatedFileSpec = #file_spec{name = FileName, content = AppendedContent},
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedFileSpec),
    assert_monitoring_state_after_single_file_modification(TestCaseCtx).


truncate_file_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(),
    TruncatedSize = 1,
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = InitialContent}, SuiteCtx
    ),

    storage_file_setup_utils:truncate(
        ImportingProviderSelector, ImportedStorageId, StorageFileId,
        TruncatedSize, byte_size(InitialContent)
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    TruncatedFileSpec = #file_spec{
        name = FileName, content = binary:part(InitialContent, 0, TruncatedSize)
    },
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, TruncatedFileSpec),
    assert_monitoring_state_after_single_file_modification(TestCaseCtx).


%% Beyond verifying that the new mode is imported, asserts (via a passthrough
%% mock) that the mutation was actually detected via the children-attrs hash
%% (storage_import_hash) - i.e. not missed by the hash-based bulk-skip of the
%% root's otherwise-unchanged batch. POSIX-only (object storages have no
%% per-object POSIX mode).
chmod_file_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    mock_storage_import_hash(SuiteCtx),

    FileName = ?RAND_STR(),
    NewMode = 8#600,
    RootStorageFileId = <<"/">>,
    StorageFileId = filepath_utils:join([RootStorageFileId, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_id = SpaceId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx,
        non_importing_provider_ctx = NonImportingProviderCtx
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{name = FileName}, SuiteCtx
    ),

    storage_file_setup_utils:chmod(ImportingProviderSelector, ImportedStorageId, StorageFileId, NewMode),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceTestFilePath, #{mode => NewMode}),
    storage_import_test_utils:assert_attrs(NonImportingProviderCtx, SpaceTestFilePath, #{mode => NewMode}),
    assert_children_hash_changed(ImportingProviderSelector, SpaceId, RootStorageFileId),
    assert_monitoring_state_after_single_file_modification(TestCaseCtx).


%% Like chmod_file_update_test, but the changed file sits inside a subdirectory
%% whose 3 children are scanned in more than one batch (storage_import_dir_batch_size
%% lowered to 2 in init_per_testcase) - exercising the children-attrs hash
%% detection one level below the space root, across listing batches. Also
%% asserts (via a passthrough mock on storage_sync_traverse) that the
%% modification is picked up in spite of, not because of, the parent
%% directory's own mtime - a child's chmod alone does not bump it. POSIX-only.
chmod_file_update_in_batched_dir_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    mock_storage_import_hash(SuiteCtx),
    mock_storage_sync_traverse(SuiteCtx),

    TestDirName = ?RAND_STR(),
    File1Name = ?RAND_STR(),
    NewMode = 8#600,
    TestDirStorageFileId = filepath_utils:join([<<"/">>, TestDirName]),
    File1StorageFileId = filepath_utils:join([TestDirStorageFileId, File1Name]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_id = SpaceId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx,
        non_importing_provider_ctx = NonImportingProviderCtx
    } = storage_import_test_utils:setup_and_verify_initial_import(?FUNCTION_NAME, [
        #dir_spec{name = TestDirName, children = [
            #file_spec{name = File1Name, content = ?RAND_STR()},
            #file_spec{name = ?RAND_STR(), content = ?RAND_STR()},
            #file_spec{name = ?RAND_STR(), content = ?RAND_STR()}
        ]},
        #dir_spec{name = ?RAND_STR()}
    ], SuiteCtx, #{monitoring_overrides => #{
        %% root's own verdict (1) + 1 extra disambiguating-batch pass each for
        %% root (exactly 2 children) and TestDirName (first batch exactly 2 of
        %% 3 children) - see the module doc's batch-size note; the empty
        %% sibling dir (0 children) needs none
        <<"unmodified">> => 3
    }}),

    storage_file_setup_utils:chmod(ImportingProviderSelector, ImportedStorageId, File1StorageFileId, NewMode),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    SpaceTestFilePath = filepath_utils:join([SpacePath, TestDirName, File1Name]),
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceTestFilePath, #{mode => NewMode}),
    storage_import_test_utils:assert_attrs(NonImportingProviderCtx, SpaceTestFilePath, #{mode => NewMode}),
    %% the change was detected via the hash-based children-attrs check on TestDirName...
    assert_children_hash_changed(ImportingProviderSelector, SpaceId, TestDirStorageFileId),
    %% ...and NOT via TestDirName's own mtime, which a child's chmod alone does not bump
    assert_children_mtime_unchanged(ImportingProviderSelector, SpaceId, TestDirStorageFileId),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        modified_hist => 1,
        %% same extra-batch-pass arithmetic as scan 1: root and TestDirName
        %% re-checked twice each, the empty sibling once, plus the 2 untouched
        %% files once each => 2+2+1+1+1 = 7
        <<"unmodified">> => 7
    }).


%% A storage-level rename is seen by the scan as two independent events: a
%% deletion at the old path and a creation at the new one. POSIX-only.
move_file_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    SrcFileName = ?RAND_STR(),
    DstFileName = ?RAND_STR(),
    Content = ?RAND_STR(),
    SrcStorageFileId = filepath_utils:join([<<"/">>, SrcFileName]),
    DstStorageFileId = filepath_utils:join([<<"/">>, DstFileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{name = SrcFileName, content = Content}, SuiteCtx
    ),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_file_setup_utils:rename(
        ImportingProviderSelector, ImportedStorageId, SrcStorageFileId, DstStorageFileId
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, #file_spec{
        name = DstFileName, content = Content
    }),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"deleted">> => 1,
        %% modified: the root (its children set changed), leaving nothing unmodified
        <<"modified">> => 1,
        modified_hist => 1,
        <<"unmodified">> => 0,
        created_hist => 2,
        deleted_hist => 1
    }).


%% A byte-identical file created at a new path is, to the scan, simply a new
%% file to import (indistinguishable from a host-level copy); the untouched
%% original counts as unmodified. POSIX-only.
copy_file_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    SrcFileName = ?RAND_STR(),
    DstFileName = ?RAND_STR(),
    Content = ?RAND_STR(),
    DstStorageFileId = filepath_utils:join([<<"/">>, DstFileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{name = SrcFileName, content = Content}, SuiteCtx
    ),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_file_setup_utils:create_file(ImportingProviderSelector, ImportedStorageId, DstStorageFileId, Content),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, [
        #file_spec{name = SrcFileName, content = Content},
        #file_spec{name = DstFileName, content = Content}
    ]),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        %% modified: the root (it gained a child); unmodified: the untouched original
        <<"modified">> => 1,
        modified_hist => 1,
        <<"unmodified">> => 1,
        created_hist => 2
    }).


%% The file's content is overwritten WITHOUT changing its size. Unlike
%% append/truncate/chmod, a same-size change has no size/mode signal to fall
%% back on - detection relies entirely on the file's mtime having advanced,
%% hence the explicit delay before the write.
change_file_content_constant_size_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(16),
    ChangedContent = ?RAND_STR(16),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = InitialContent}, SuiteCtx
    ),

    %% load-bearing - lets the mtime tick visibly past the initial scan's stat
    %% (test doc); do not remove
    timer:sleep(timer:seconds(2)),
    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, 0, ChangedContent
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    UpdatedFileSpec = #file_spec{name = FileName, content = ChangedContent},
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedFileSpec),
    assert_monitoring_state_after_single_file_modification(TestCaseCtx).


%% Like change_file_content_constant_size_test, but the new content differs in
%% length - the size clause detects it regardless of mtime resolution, so no
%% delay is needed.
change_file_content_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(16),
    ChangedContent = ?RAND_STR(32),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = InitialContent}, SuiteCtx
    ),

    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, 0, ChangedContent
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    UpdatedFileSpec = #file_spec{name = FileName, content = ChangedContent},
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedFileSpec),
    assert_monitoring_state_after_single_file_modification(TestCaseCtx).


%% A file's content is overwritten on the storage with a same-size change while
%% its storage_sync_info is forced into the EXACT boundary of the "already
%% handled" fast-path shortcut in
%% storage_import_engine:maybe_update_file_location/4: the recorded mtime and
%% last_stat are both set equal to the file's storage mtime. That shortcut skips
%% re-checking a file only when the recorded last_stat is STRICTLY greater than
%% the storage mtime, so sitting exactly on the boundary the scan must NOT skip
%% and must still detect the real content change (were the guard '>=' instead of
%% '>', the change would be wrongly skipped and the content assertion below would
%% fail). The change is same-size, so this shortcut is the only thing standing
%% between the test and a false "unmodified". The boundary value is chosen
%% strictly above the file's logical mtime, so that - once the shortcut is
%% (correctly) not taken - the same-size change is still detectable, its storage
%% mtime being newer than the logical one. POSIX-only (relies on forcing the
%% storage mtime).
change_file_content_the_same_moment_when_sync_performs_stat_on_file_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(),
    ChangedContent = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_id = SpaceId
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = InitialContent}, SuiteCtx
    ),

    %% read the file's storage mtime (equal to its logical mtime right after
    %% import) and derive a boundary timestamp strictly above it (see the doc
    %% above): the same-size overwrite stays detectable via mtime, while forcing
    %% the recorded mtime AND last_stat to that same value lands scan 2 exactly
    %% on the fast-path guard
    FileMtime = storage_file_setup_utils:get_mtime(
        ImportingProviderSelector, ImportedStorageId, StorageFileId
    ),
    BoundaryTime = FileMtime + 1,
    ok = ?rpc(ImportingProviderSelector, storage_sync_info:create_or_update(
        StorageFileId, SpaceId,
        fun(SSI) -> {ok, SSI#storage_sync_info{mtime = BoundaryTime, last_stat = BoundaryTime}} end
    )),
    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, 0, ChangedContent
    ),
    storage_file_setup_utils:set_mtime(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, BoundaryTime
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    UpdatedFileSpec = #file_spec{name = FileName, content = ChangedContent},
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedFileSpec),
    assert_monitoring_state_after_single_file_modification(TestCaseCtx).


%% A file is replaced, under the SAME name, by a directory holding one child
%% file - the scan must swap the entry's type: delete the old file entry and
%% import the directory (with its child) in its place. Also verifies the
%% newly-imported directory is fully functional (not just a passively-imported
%% node) by creating a new subdirectory inside it via LFM.
replace_file_with_dir_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector,
        storage_type = StorageType
    } = SuiteCtx,
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(),
    ChildFileName = ?RAND_STR(),
    ChildContent = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),
    ChildStorageFileId = filepath_utils:join([StorageFileId, ChildFileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            node = ImportingProviderNode, session_id = ImportingProviderSessionId
        },
        non_importing_provider_ctx = #provider_ctx{
            node = NonImportingProviderNode, session_id = NonImportingProviderSessionId
        }
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = InitialContent}, SuiteCtx
    ),

    storage_file_setup_utils:delete_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, byte_size(InitialContent)
    ),
    storage_file_setup_utils:create_dir(ImportingProviderSelector, ImportedStorageId, StorageFileId),
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId, ChildStorageFileId, ChildContent
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, #dir_spec{name = FileName, children = [
        #file_spec{name = ChildFileName, content = ChildContent}
    ]}),
    %% the new dir + its child on POSIX; only the child (object) on S3
    NewlyCreated = case StorageType of posix -> 2; s3 -> 1 end,
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => NewlyCreated,
        <<"modified">> => RootModified,
        <<"deleted">> => 1,
        <<"unmodified">> => RootUnmodified,
        created_hist => NewlyCreated + 1,
        modified_hist => RootModified,
        deleted_hist => 1
    }),

    %% test doc: the imported directory must be fully functional - mkdir in it
    %% via LFM on the non-importing provider, then see it on the importing one
    NewSubDirName = ?RAND_STR(),
    SpaceTestDirPath = filepath_utils:join([SpacePath, FileName]),
    {ok, #file_attr{guid = ReplacedDirGuid}} = ?assertMatch(
        {ok, #file_attr{type = ?DIRECTORY_TYPE}},
        lfm_proxy:stat(NonImportingProviderNode, NonImportingProviderSessionId, {path, SpaceTestDirPath})
    ),
    {ok, NewSubDirGuid} = ?assertMatch({ok, _}, lfm_proxy:mkdir(
        NonImportingProviderNode, NonImportingProviderSessionId, ReplacedDirGuid, NewSubDirName, ?DEFAULT_DIR_MODE
    )),
    ?assertMatch(
        {ok, _},
        lfm_proxy:stat(ImportingProviderNode, ImportingProviderSessionId, ?FILE_REF(NewSubDirGuid)),
        ?ATTEMPTS
    ).


%% Like replace_file_with_dir_test, but the other way round: an (empty)
%% directory is replaced by a regular file. POSIX-only (an empty directory
%% cannot exist on an object storage).
replace_empty_dir_with_file_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    DirName = ?RAND_STR(),
    NewContent = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, DirName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #dir_spec{name = DirName}, SuiteCtx
    ),

    storage_file_setup_utils:rmdir(ImportingProviderSelector, ImportedStorageId, StorageFileId),
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, NewContent
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, #file_spec{name = DirName, content = NewContent}),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        %% modified: the root (its children set changed)
        <<"modified">> => 1,
        <<"deleted">> => 1,
        <<"unmodified">> => 0,
        created_hist => 2,
        modified_hist => 1,
        deleted_hist => 1
    }).


replace_non_empty_dir_with_file_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector,
        storage_type = StorageType
    } = SuiteCtx,
    DirName = ?RAND_STR(),
    ChildFileName = ?RAND_STR(),
    ChildContent = ?RAND_STR(),
    NewContent = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, DirName]),
    ChildStorageFileId = filepath_utils:join([StorageFileId, ChildFileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:setup_and_verify_initial_import(?FUNCTION_NAME, #dir_spec{
        name = DirName, children = [#file_spec{name = ChildFileName, content = ChildContent}]
    }, SuiteCtx),

    storage_file_setup_utils:delete_file(
        ImportingProviderSelector, ImportedStorageId, ChildStorageFileId, byte_size(ChildContent)
    ),
    storage_file_setup_utils:rmdir(ImportingProviderSelector, ImportedStorageId, StorageFileId),
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, NewContent
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, #file_spec{name = DirName, content = NewContent}),
    %% scan 1: the dir + its child on POSIX, only the child on S3
    Scan1Created = case StorageType of posix -> 2; s3 -> 1 end,
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => RootModified,
        %% the dir + its child, on both storage types
        <<"deleted">> => 2,
        <<"unmodified">> => RootUnmodified,
        created_hist => 1 + Scan1Created,
        modified_hist => RootModified,
        deleted_hist => 2
    }).


%% Both atime and mtime are forced, on the storage, to an identical far-future
%% timestamp (content/size unchanged); the scan must propagate BOTH onto the
%% logical attrs - proving atime is synced too, not just mtime. The timestamp
%% is far enough in the future that no pre-write delay is needed (unlike in
%% change_file_content_constant_size_test). POSIX-only (relies on forcing
%% storage timestamps).
update_timestamps_file_import_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    Content = ?RAND_STR(),
    NewTimestamp = 9999999999,
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx,
        non_importing_provider_ctx = NonImportingProviderCtx
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = Content}, SuiteCtx
    ),

    storage_file_setup_utils:set_atime_and_mtime(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, NewTimestamp, NewTimestamp
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    ExpectedTimes = #{atime => NewTimestamp, mtime => NewTimestamp},
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceTestFilePath, ExpectedTimes),
    storage_import_test_utils:assert_attrs(NonImportingProviderCtx, SpaceTestFilePath, ExpectedTimes),
    assert_monitoring_state_after_single_file_modification(TestCaseCtx).


%% A new file appears inside one of two (initially empty) sibling directories.
%% Being nested one level below the space root, it leaves the root's own
%% children set (and hence its mtime) untouched: only the TOUCHED directory
%% flips to modified, while the root and the UNTOUCHED sibling stay unmodified.
%% On POSIX this is additionally proven at the mechanism level via passthrough
%% mocks: the touched directory's own mtime changed, while the root's
%% children-attrs hash and the untouched sibling's mtime did not. On S3 the
%% (empty) directories are unobservable and nothing can be classified modified
%% at all - the root is the single unmodified entry.
create_file_in_dir_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector,
        storage_type = StorageType
    } = SuiteCtx,
    TouchedDirName = ?RAND_STR(),
    UntouchedDirName = ?RAND_STR(),
    FileName = ?RAND_STR(),
    Content = ?RAND_STR(),
    RootStorageFileId = <<"/">>,
    TouchedDirStorageFileId = filepath_utils:join([RootStorageFileId, TouchedDirName]),
    UntouchedDirStorageFileId = filepath_utils:join([RootStorageFileId, UntouchedDirName]),
    FileStorageFileId = filepath_utils:join([TouchedDirStorageFileId, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_id = SpaceId
    } = storage_import_test_utils:setup_and_verify_initial_import(?FUNCTION_NAME, [
        #dir_spec{name = TouchedDirName},
        #dir_spec{name = UntouchedDirName}
    ], SuiteCtx),

    %% white-box mechanism checks, POSIX-only
    ?IF_POSIX(StorageType, begin
        mock_storage_import_hash(SuiteCtx),
        mock_storage_sync_traverse(SuiteCtx)
    end),

    %% POSIX-only on purpose: the added child registers via the touched dir's
    %% own mtime, which must visibly advance; on s3 the children-attrs hash
    %% catches it regardless
    ?IF_POSIX(StorageType, storage_import_test_utils:ensure_mtime_progression(TestCaseCtx)),
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId, FileStorageFileId, Content
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    UpdatedTreeSpec = [
        #dir_spec{name = TouchedDirName, children = [#file_spec{name = FileName, content = Content}]},
        #dir_spec{name = UntouchedDirName}
    ],
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedTreeSpec),
    ?IF_POSIX(StorageType, begin
        assert_children_mtime_changed(ImportingProviderSelector, SpaceId, TouchedDirStorageFileId),
        assert_children_hash_unchanged(ImportingProviderSelector, SpaceId, RootStorageFileId),
        assert_children_mtime_unchanged(ImportingProviderSelector, SpaceId, UntouchedDirStorageFileId)
    end),
    %% test doc: POSIX - touched dir modified, root + untouched sibling
    %% unmodified; S3 - nothing modified, root alone unmodified
    {ModifiedCount, UnmodifiedCount} = case StorageType of posix -> {1, 2}; s3 -> {0, 1} end,
    %% scan 1: the 2 dirs on POSIX, nothing on S3
    Scan1Created = case StorageType of posix -> 2; s3 -> 0 end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => ModifiedCount,
        <<"deleted">> => 0,
        <<"unmodified">> => UnmodifiedCount,
        created_hist => 1 + Scan1Created,
        modified_hist => ModifiedCount,
        deleted_hist => 0
    }).


%% Like create_file_in_dir_update_test, but the space root itself has more
%% direct children (2 dirs + 4 files = 6) than storage_import_dir_batch_size
%% (temporarily lowered to 2 in init_per_testcase) - so it's the ROOT's own
%% scan-1 baseline that spans multiple listing batches this time, not a
%% subdirectory's one level down.
create_file_in_dir_exceed_batch_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector,
        storage_type = StorageType
    } = SuiteCtx,
    TouchedDirName = ?RAND_STR(),
    UntouchedDirName = ?RAND_STR(),
    FileName = ?RAND_STR(),
    Content = ?RAND_STR(),
    OtherFileSpecs = [#file_spec{name = ?RAND_STR(), content = ?RAND_STR()} || _ <- lists:seq(1, 4)],
    RootStorageFileId = <<"/">>,
    TouchedDirStorageFileId = filepath_utils:join([RootStorageFileId, TouchedDirName]),
    UntouchedDirStorageFileId = filepath_utils:join([RootStorageFileId, UntouchedDirName]),
    FileStorageFileId = filepath_utils:join([TouchedDirStorageFileId, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_id = SpaceId
    } = storage_import_test_utils:init_testcase(?FUNCTION_NAME, [
        #dir_spec{name = TouchedDirName},
        #dir_spec{name = UntouchedDirName}
        | OtherFileSpecs
    ], SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    %% root's own verdict is re-run once per listing batch. POSIX: 6 children /
    %% batch size 2 => reads at offsets 0,2,4,6 (sizes 2,2,2,0) = 4 passes.
    %% S3: only the 4 files count as root's children and there is no extra
    %% disambiguating batch => ceil(4/2) = 2 passes
    Scan1Unmodified = case StorageType of posix -> 4; s3 -> 2 end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => Scan1Unmodified
    }),

    %% white-box mechanism checks, POSIX-only: prove the mutation is still
    %% detected under the hood even though detect_modifications=false (below)
    %% suppresses it from the monitoring counters
    ?IF_POSIX(StorageType, begin
        mock_storage_import_hash(SuiteCtx),
        mock_storage_sync_traverse(SuiteCtx)
    end),

    %% POSIX-only and load-bearing here: scan 2 runs with detect_modifications
    %% => false (below), so the children-attrs hash is NOT consulted and the new
    %% child is discovered only if TouchedDirName's own mtime has visibly changed
    %% (the {MTimeUnchanged, _, DetectModifications=false} branch of
    %% storage_sync_traverse:do_update_master_job/2 otherwise skips it) - force
    %% the mtime forward; on s3 detection stays enabled, so the hash catches it
    ?IF_POSIX(StorageType, storage_import_test_utils:ensure_mtime_progression(TestCaseCtx)),
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId, FileStorageFileId, Content
    ),
    ScanConfigOverrides = case StorageType of
        posix -> #{detect_deletions => false, detect_modifications => false};
        %% detection must stay enabled on S3 - see the test doc
        s3 -> #{}
    end,
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2, ScanConfigOverrides),

    UpdatedTreeSpec = [
        #dir_spec{name = TouchedDirName, children = [#file_spec{name = FileName, content = Content}]},
        #dir_spec{name = UntouchedDirName}
        | OtherFileSpecs
    ],
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedTreeSpec),
    ?IF_POSIX(StorageType, begin
        %% TouchedDir's mtime advanced (a child was added), which routes it through
        %% the hash-computing branch even under detect_modifications=false - so both
        %% mechanisms still fire under the hood (the flag only suppresses the
        %% "modified" classification in the counters, not the detection itself);
        %% the untouched sibling's mtime stays put
        assert_children_mtime_changed(ImportingProviderSelector, SpaceId, TouchedDirStorageFileId),
        assert_children_hash_changed(ImportingProviderSelector, SpaceId, TouchedDirStorageFileId),
        assert_children_mtime_unchanged(ImportingProviderSelector, SpaceId, UntouchedDirStorageFileId)
    end),
    %% nothing counts as "modified" this scan either way: POSIX suppresses the
    %% classification via detect_modifications=false, on S3 the root never can
    %% be modified and nothing else picks up a tally.
    %% POSIX unmodified (every already-known entry forced unmodified): root's 4
    %% batch passes (children count unchanged) + touched dir + untouched dir +
    %% 4 untouched files = 10. S3: the new file joins root's flat listing (5
    %% children => 3 passes) + the 4 untouched files = 7
    Scan2Unmodified = case StorageType of posix -> 10; s3 -> 7 end,
    %% scan 1: all 6 entries on POSIX, only the 4 files on S3
    Scan1Created = case StorageType of posix -> 6; s3 -> 4 end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"unmodified">> => Scan2Unmodified,
        created_hist => 1 + Scan1Created,
        modified_hist => 0,
        deleted_hist => 0
    }).


%% A file imported with sync_acl enabled has its on-storage NFS4 ACL changed;
%% the scan must re-APPLY the new ACL - changing its enforcement, not just
%% re-reading an xattr. Uses the same ACL/LUMA mocking approach as
%% storage_import_initial_oct_test_base:import_nfs_acl_test (see there).
%% The proof of re-enforcement is an order-dependent NFS4 ACL evaluation flip:
%% the initial ACL grants EVERYONE@ read_acl and denies the named principal
%% (mapped to user1, also the file owner) write_attributes; the updated ACL
%% instead grants ONLY that named principal read_acl and denies EVERYONE@ - so
%% user1 (matching the first, specific entry) keeps ACL read access, while
%% user2 falls through to the general deny and loses it. POSIX-only.
update_nfs_acl_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    %% user1/user2 are regular users of the 2op scenario, not yet members of the space
    User1Selector = user1,
    User2Selector = user2,
    User1Id = oct_background:get_user_id(User1Selector),
    User1FullName = oct_background:get_user_fullname(User1Selector),
    %% the file owner uid and both ACLs' named principal all map to user1
    User1PrincipalId = <<"ala@nfsdomain.org">>,

    FileName = ?RAND_STR(),
    Content = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),
    InitialAcl = [
        #access_control_entity{
            acetype = ?allow_mask, aceflags = ?no_flags_mask,
            identifier = <<"OWNER@">>, acemask = ?read_acl_mask
        },
        #access_control_entity{
            acetype = ?deny_mask, aceflags = ?no_flags_mask,
            identifier = <<"GROUP@">>, acemask = ?write_acl_mask
        },
        #access_control_entity{
            acetype = ?allow_mask, aceflags = ?no_flags_mask,
            identifier = <<"EVERYONE@">>, acemask = ?read_acl_mask
        },
        #access_control_entity{
            acetype = ?deny_mask, aceflags = ?no_flags_mask,
            identifier = User1PrincipalId, acemask = ?write_attributes_mask
        }
    ],
    UpdatedAcl = [
        #access_control_entity{
            acetype = ?allow_mask, aceflags = ?no_flags_mask,
            identifier = User1PrincipalId, acemask = ?read_acl_mask
        },
        #access_control_entity{
            acetype = ?deny_mask, aceflags = ?no_flags_mask,
            identifier = <<"EVERYONE@">>, acemask = ?read_acl_mask
        }
    ],
    InitialEncodedAcl = ?rpc(ImportingProviderSelector, storage_import_acl:encode(InitialAcl)),
    storage_import_test_utils:mock_storage_file_acl(SuiteCtx, StorageFileId, InitialEncodedAcl),
    storage_import_test_utils:mock_luma_acl_user(SuiteCtx, User1Id),

    TestCaseCtx = #storage_import_test_case_ctx{
        space_id = SpaceId,
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{node = ImportingProviderNode}
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = Content}, SuiteCtx, #{sync_acl => true}
    ),
    %% the imported file is owned by user1; make user1 and user2 space members so
    %% they can access it (membership propagates via dbsync, hence the retries below)
    ozw_test_rpc:add_user_to_space(SpaceId, User1Id),
    ozw_test_rpc:add_user_to_space(SpaceId, oct_background:get_user_id(User2Selector)),

    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    User1SessId = oct_background:get_user_session_id(User1Selector, ImportingProviderSelector),
    User2SessId = oct_background:get_user_session_id(User2Selector, ImportingProviderSelector),

    %% the file was imported with the initial ACL applied - readable by both
    %% users (read_acl granted to EVERYONE@)
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    InitialAclJson = storage_import_test_utils:expected_imported_acl_json(
        InitialAcl, User1FullName, User1Id
    ),
    ?assertEqual({ok, InitialAclJson},
        storage_import_test_utils:get_cdmi_acl(ImportingProviderNode, User1SessId, SpaceTestFilePath), ?ATTEMPTS),
    ?assertEqual({ok, InitialAclJson},
        storage_import_test_utils:get_cdmi_acl(ImportingProviderNode, User2SessId, SpaceTestFilePath), ?ATTEMPTS),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    %% change the storage's (mocked) ACL and let the next continuous scan re-apply it
    UpdatedEncodedAcl = ?rpc(ImportingProviderSelector, storage_import_acl:encode(UpdatedAcl)),
    storage_import_test_utils:mock_storage_file_acl(SuiteCtx, StorageFileId, UpdatedEncodedAcl),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% the roles have flipped: user1 (the named principal in the new ACL's first,
    %% specific entry) can still read the ACL, while user2 now falls through to
    %% the general EVERYONE@ deny and is rejected
    UpdatedAclJson = storage_import_test_utils:expected_imported_acl_json(
        UpdatedAcl, User1FullName, User1Id
    ),
    ?assertEqual({ok, UpdatedAclJson},
        storage_import_test_utils:get_cdmi_acl(ImportingProviderNode, User1SessId, SpaceTestFilePath), ?ATTEMPTS),
    ?assertMatch({error, ?EACCES},
        storage_import_test_utils:get_cdmi_acl(ImportingProviderNode, User2SessId, SpaceTestFilePath), ?ATTEMPTS),

    %% with sync_acl enabled the ACL is folded into the per-file attrs hash (see
    %% storage_import_hash:count_file_attrs_hash/2), so the ACL change surfaces
    %% as an ordinary single-file modification; the root's children are
    %% untouched, so it stays unmodified
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        modified_hist => 1,
        created_hist => 1
    }).


%% --- idempotency ---


%% Idempotency baseline: with the storage untouched between scans, scan 2 must
%% not reprocess the file - the root batch's children-attrs hash is unchanged,
%% so the file is bulk-skipped and merely counted "unmodified" alongside the
%% root, on either storage type.
should_not_process_file_with_unchanged_attrs_hash_test(SuiteCtx) ->
    TestCaseCtx = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{name = ?RAND_STR(), content = ?RAND_STR()}, SuiteCtx
    ),

    %% deliberately no storage mutation - rescan the untouched storage
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"unmodified">> => 2
    }).


%% The file's atime/mtime are forced (on the storage) back to epoch 1 while
%% scan 2 runs with BOTH detection flags disabled: the new timestamps must NOT
%% be propagated onto the logical file, and every entry is forced "unmodified"
%% without any attrs comparison. Note that with the DEFAULT config this
%% mutation WOULD be picked up: forcing the timestamps also bumps the file's
%% storage ctime to "now" (ctime cannot be set explicitly), which the
%% modification detection compares too - cf. update_timestamps_file_import_test.
%% POSIX-only (relies on forcing storage timestamps).
should_not_detect_timestamp_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = ?RAND_STR()}, SuiteCtx
    ),

    storage_file_setup_utils:set_atime_and_mtime(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, 1, 1
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2, #{
        detect_deletions => false,
        detect_modifications => false
    }),

    %% the logical timestamps must not have been overwritten with the forced ones
    #provider_ctx{node = ImportingProviderNode, session_id = SessionId} = ImportingProviderCtx,
    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    ?assertNotMatch(
        {ok, #file_attr{atime = 1, mtime = 1}},
        lfm_proxy:stat(ImportingProviderNode, SessionId, {path, SpaceTestFilePath})
    ),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"unmodified">> => 2
    }).


%% --- retry ---


%% Import of the declared file fails on the initial scan (mocked engine error,
%% counted "failed"); once the failure is gone, the next scan must retry and
%% import it successfully.
update_syncs_files_after_import_failed_test(SuiteCtx) ->
    FileName = ?RAND_STR(),
    storage_import_test_utils:mock_import_file_error(SuiteCtx, FileName),

    TestCaseCtx = #storage_import_test_case_ctx{
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        }
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = ?RAND_STR()}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(ImportingProviderNode, ImportingProviderSessionId, {path, SpaceTestFilePath}),
        ?ATTEMPTS
    ),
    storage_import_test_utils:assert_monitoring_state_after_failed_import(TestCaseCtx),

    storage_import_test_utils:unmock_storage_import_engine(SuiteCtx),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"unmodified">> => 1
    }).


%% Like update_syncs_files_after_import_failed_test, but the failure hits a
%% continuous scan (a file created after the initial import fails on scan 2);
%% scan 3 must retry and import it successfully.
update_syncs_files_after_previous_update_failed_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector,
        storage_type = StorageType
    } = SuiteCtx,
    FileName = ?RAND_STR(),
    Content = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        }
    } = storage_import_test_utils:setup_and_verify_initial_import(?FUNCTION_NAME, undefined, SuiteCtx),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, Content
    ),
    storage_import_test_utils:mock_import_file_error(SuiteCtx, FileName),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(ImportingProviderNode, ImportingProviderSessionId, {path, SpaceTestFilePath}),
        ?ATTEMPTS
    ),
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"failed">> => 1,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        modified_hist => RootModified
    }),

    storage_import_test_utils:unmock_storage_import_engine(SuiteCtx),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 3),

    storage_import_test_utils:verify_imported_tree(
        TestCaseCtx, #file_spec{name = FileName, content = Content}
    ),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 3,
        <<"created">> => 1,
        <<"unmodified">> => 1,
        created_hist => 1,
        %% scan-2's root modification (posix only)
        modified_hist => RootModified
    }).


%% --- suffixes ---
%%
%% When a storage file cannot be created under its plain name - the name being
%% still occupied by the storage file of a deleted-but-still-opened file
%% (guarded by a deletion marker), or by a same-named file of another provider
%% - it is created under a name with the conflicting-file suffix
%% (?CONFLICTING_STORAGE_FILE_NAME). Scans must recognize both kinds of entries
%% as belonging to already-known logical files: never import them as new files
%% (each is counted "unmodified"; the deletion-marker-guarded one is skipped
%% via the marker, the suffixed one is mapped back to its logical file by the
%% uuid embedded in the name - storage_import_engine:sync_file/2), yet still
%% detect their storage-side modifications and update the right logical file.
%% The suffixed layouts are arranged via LFM after an initial scan of an empty
%% storage, so it is the continuous scans that face them.


should_not_import_recreated_file_with_suffix_on_storage_test(SuiteCtx) ->
    TestCaseCtx = #storage_import_test_case_ctx{
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        }
    } = storage_import_test_utils:init_testcase(?FUNCTION_NAME, undefined, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    #{
        file_name := FileName,
        space_file_path := SpaceFilePath,
        old_file_handle := OldFileHandle,
        old_content := OldContent,
        recreated_file_guid := RecreatedFileGuid,
        recreated_content := RecreatedContent
    } = setup_recreated_file_with_suffix_on_storage(TestCaseCtx),

    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% only the recreated file is visible in the space
    ?assertMatch({ok, [{RecreatedFileGuid, FileName}]}, lfm_proxy:get_children(
        ImportingProviderNode, ImportingProviderSessionId, {path, SpacePath}, 0, 10
    )),
    %% the deleted file's still-open handle reads its original content
    ?assertEqual({ok, OldContent}, lfm_proxy:read(
        ImportingProviderNode, OldFileHandle, 0, byte_size(OldContent)
    )),
    ok = lfm_proxy:close(ImportingProviderNode, OldFileHandle),
    %% while the path resolves to the recreated file
    storage_import_test_utils:assert_file_content(
        ImportingProviderCtx, SpaceFilePath, RecreatedContent
    ),
    %% the recreated file is LFM-created, the deleted-but-open one marker-guarded
    assert_scan_recognized_suffixed_layout_as_known(TestCaseCtx, _LfmCreated = 1, _Replicated = 0).


should_update_blocks_of_recreated_file_with_suffix_on_storage_test(SuiteCtx) ->
    TestCaseCtx = #storage_import_test_case_ctx{
        importing_provider_ctx = ImportingProviderCtx,
        non_importing_provider_ctx = NonImportingProviderCtx
    } = storage_import_test_utils:init_testcase(?FUNCTION_NAME, undefined, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    #{
        recreated_file_guid := RecreatedFileGuid,
        recreated_content := RecreatedContent,
        suffixed_storage_file_id := SuffixedStorageFileId
    } = setup_recreated_file_with_suffix_on_storage(TestCaseCtx),

    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),
    assert_scan_recognized_suffixed_layout_as_known(TestCaseCtx, _LfmCreated = 1, _Replicated = 0),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    ChangedContent = change_one_byte_of_storage_file(
        TestCaseCtx, SuffixedStorageFileId, RecreatedContent
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 3),

    %% the change made to the suffixed storage file is reflected in the
    %% recreated logical file, on both providers
    assert_file_content_by_guid(
        ImportingProviderCtx, RecreatedFileGuid, ChangedContent, ?ATTEMPTS
    ),
    assert_file_content_by_guid(
        NonImportingProviderCtx, RecreatedFileGuid, ChangedContent,
        ?CROSS_PROVIDER_PROPAGATION_ATTEMPTS
    ),
    assert_suffixed_storage_file_update_detected(TestCaseCtx, _LfmCreated = 1, _Replicated = 0).


should_not_import_replicated_file_with_suffix_on_storage_test(SuiteCtx) ->
    TestCaseCtx = #storage_import_test_case_ctx{
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        }
    } = storage_import_test_utils:init_testcase(?FUNCTION_NAME, undefined, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    #{
        local_file_guid := LocalFileGuid,
        local_content := LocalContent,
        remote_file_guid := RemoteFileGuid,
        remote_content := RemoteContent
    } = setup_replicated_file_with_suffix_on_storage(TestCaseCtx),

    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% still exactly the two name-conflicting logical files in the space,
    %% each with its content intact
    ?assertMatch({ok, [_, _]}, lfm_proxy:get_children(
        ImportingProviderNode, ImportingProviderSessionId, {path, SpacePath}, 0, 10
    )),
    assert_file_content_by_guid(ImportingProviderCtx, LocalFileGuid, LocalContent, ?ATTEMPTS),
    assert_file_content_by_guid(ImportingProviderCtx, RemoteFileGuid, RemoteContent, ?ATTEMPTS),
    %% the local file is LFM-created, the remote one lands on storage as a replica
    assert_scan_recognized_suffixed_layout_as_known(TestCaseCtx, _LfmCreated = 1, _Replicated = 1).


should_update_replicated_file_with_suffix_on_storage_test(SuiteCtx) ->
    TestCaseCtx = #storage_import_test_case_ctx{
        importing_provider_ctx = ImportingProviderCtx,
        non_importing_provider_ctx = NonImportingProviderCtx
    } = storage_import_test_utils:init_testcase(?FUNCTION_NAME, undefined, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    #{
        remote_file_guid := RemoteFileGuid,
        remote_content := RemoteContent,
        suffixed_storage_file_id := SuffixedStorageFileId
    } = setup_replicated_file_with_suffix_on_storage(TestCaseCtx),

    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),
    assert_scan_recognized_suffixed_layout_as_known(TestCaseCtx, _LfmCreated = 1, _Replicated = 1),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    ChangedContent = change_one_byte_of_storage_file(
        TestCaseCtx, SuffixedStorageFileId, RemoteContent
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 3),

    %% the change made to the suffixed replica is reflected in the remotely
    %% created logical file - also back on its origin provider, whose replica
    %% is invalidated and re-fetched
    assert_file_content_by_guid(
        ImportingProviderCtx, RemoteFileGuid, ChangedContent, ?ATTEMPTS
    ),
    assert_file_content_by_guid(
        NonImportingProviderCtx, RemoteFileGuid, ChangedContent,
        ?CROSS_PROVIDER_PROPAGATION_ATTEMPTS
    ),
    assert_suffixed_storage_file_update_detected(TestCaseCtx, _LfmCreated = 1, _Replicated = 1).


%% --- config ---


%% A 3-level tree exists on the storage from the start, while the scans'
%% max_depth is raised scan by scan (1 -> 2 -> 3): each scan must import
%% exactly one more level, leaving the deeper entries untouched. Raising
%% max_depth makes the next scan re-process listings that were previously cut
%% short at the depth limit: the children-attrs batch hash covers only the
%% children within max_depth (see storage_traverse:process_children_batch/2),
%% so a raised limit changes the hash and forces individual processing of the
%% newly-eligible children; a directory that WAS at the limit has no children
%% hash/mtime recorded at all (the Depth =:= MaxDepth branch of
%% storage_sync_traverse's batch-finish callback), so it is re-traversed
%% unconditionally. Both storage types share this mechanism - on a flat
%% storage entry depths are computed from the objects' key structure (there is
%% just no per-directory traversal, so only the space root's listing is ever
%% cut/re-processed).
changing_max_depth_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{storage_type = StorageType} = SuiteCtx,
    Content = ?RAND_STR(),
    [Dir1Name, Dir2Name, File1Name, File2Name, File3Name] = [?RAND_STR() || _ <- lists:seq(1, 5)],
    File1Spec = #file_spec{name = File1Name, content = Content},
    File2Spec = #file_spec{name = File2Name, content = Content},
    File3Spec = #file_spec{name = File3Name, content = Content},

    TestCaseCtx = #storage_import_test_case_ctx{
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = SessionId
        }
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME,
        [
            %% /dir1/dir2/file3, /dir1/file2, /file1 - one file per depth level
            #dir_spec{name = Dir1Name, children = [
                #dir_spec{name = Dir2Name, children = [File3Spec]},
                File2Spec
            ]},
            File1Spec
        ],
        SuiteCtx,
        #{max_depth => 1}
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% only the depth-1 entries were imported (on posix that includes dir1,
    %% imported empty; dirs don't count towards "created" on s3)
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, [
        #dir_spec{name = Dir1Name}, File1Spec
    ]),
    Dir2Path = filepath_utils:join([SpacePath, Dir1Name, Dir2Name]),
    File2Path = filepath_utils:join([SpacePath, Dir1Name, File2Name]),
    File3Path = filepath_utils:join([SpacePath, Dir1Name, Dir2Name, File3Name]),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(ImportingProviderNode, SessionId, {path, Dir2Path})),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(ImportingProviderNode, SessionId, {path, File2Path})),

    {Scan1Created, Scan2Created, Scan3Created, Scan2Unmodified, Scan3Unmodified} =
        case StorageType of
            %% per scan: {dir1, file1} / {dir2, file2} / {file3};
            %% unmodified = root + everything imported by the previous scans
            posix -> {2, 2, 1, 3, 5};
            %% per scan: {file1} / {file2} / {file3}; unmodified = root (frozen,
            %% single batch pass - at most 3 file objects) + the earlier files
            s3 -> {1, 1, 1, 2, 3}
        end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"created">> => Scan1Created,
        created_hist => Scan1Created
    }),

    %% scan 2, max_depth raised to 2 - the next level gets imported
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2, #{max_depth => 2}),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, [
        #dir_spec{name = Dir1Name, children = [#dir_spec{name = Dir2Name}, File2Spec]},
        File1Spec
    ]),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(ImportingProviderNode, SessionId, {path, File3Path})),

    CreatedByScans12 = Scan1Created + Scan2Created,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => Scan2Created,
        <<"unmodified">> => Scan2Unmodified,
        created_hist => CreatedByScans12
    }),

    %% scan 3, max_depth raised to 3 - the whole declared tree is now in
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 3, #{max_depth => 3}),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    CreatedByScans123 = CreatedByScans12 + Scan3Created,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 3,
        <<"created">> => Scan3Created,
        <<"unmodified">> => Scan3Unmodified,
        created_hist => CreatedByScans123
    }).


%% With continuous scanning disabled, a single scan is forced (via
%% storage_import:start_auto_scan/1) after a file appears on the initially
%% empty storage: the forced scan must run and import the file.
force_start_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector,
        storage_type = StorageType
    } = SuiteCtx,
    FileName = ?RAND_STR(),
    Content = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:setup_and_verify_initial_import(?FUNCTION_NAME, undefined, SuiteCtx),

    %% load-bearing - without this delay the new file may land in the same
    %% mtime second as scan 1's root stat and the root would not be classified
    %% modified (the file itself would still be imported, via the
    %% children-attrs hash path); do not remove
    timer:sleep(timer:seconds(2)),
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, Content
    ),
    storage_import_test_utils:force_start_auto_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(
        TestCaseCtx, #file_spec{name = FileName, content = Content}
    ),
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        created_hist => 1,
        modified_hist => RootModified
    }).


%% The initial scan of a large tree (280 nodes, traversed with
%% storage_import_dir_batch_size = 1 - see init_per_testcase) is aborted via
%% storage_import:stop_auto_scan/1 as soon as the first file import is
%% observed (through a passthrough mock signalling this process): the scan
%% must promptly finish with only a part of the tree processed and no
%% failures; the next (continuous) scan must then import the remainder.
force_stop_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{storage_type = StorageType} = SuiteCtx,
    %% [5, 5, 10] => 5 dirs x 5 subdirs x 10 files = 280 nodes
    FileTreeSpec = storage_import_test_utils:gen_nested_tree_spec([5, 5, 10], ?RAND_STR()),

    %% set up before init_testcase, as the initial scan auto-runs on space setup
    mock_import_file_started_notification(SuiteCtx, self()),
    TestCaseCtx = storage_import_test_utils:init_testcase(?FUNCTION_NAME, FileTreeSpec, SuiteCtx),

    ?assertReceivedMatch(import_file_started, timer:seconds(?LARGE_IMPORT_SCAN_ATTEMPTS)),
    storage_import_test_utils:force_stop_auto_scan(TestCaseCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx, ?LARGE_IMPORT_SCAN_ATTEMPTS),

    %% an aborted scan's processed-entries tally is unpredictable (except that
    %% nothing may fail or be deleted - the defaults below) but must fall well
    %% short of a completed scan's; the bounds are conservative - with batch
    %% size 1 even the extra-batch-pass inflation puts a completed scan's sum
    %% far above them
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"created">> => skip,
        <<"unmodified">> => skip,
        created_hist => skip,
        <<"queueLengthMinHist">> => skip,
        <<"queueLengthHourHist">> => skip,
        <<"queueLengthDayHist">> => skip
    }),
    #{
        <<"created">> := Scan1Created,
        <<"modified">> := Scan1Modified,
        <<"unmodified">> := Scan1Unmodified
    } = storage_import_test_utils:get_storage_import_monitoring_state(TestCaseCtx),
    %% bound expressed relative to the imported-entry count (expected_created_count/1
    %% - dirs + files on POSIX, files only on S3): at most a single full pass (all
    %% entries + the space root) on POSIX, and strictly fewer than all files on S3
    %% (at least one must be left for the next scan). A COMPLETED batch-size-1 scan
    %% inflates far past either (re-listing each dir once per one-entry batch), so
    %% this cleanly separates an aborted scan from a completed one
    ExpectedCreated = storage_import_test_utils:expected_created_count(TestCaseCtx),
    MaxProcessedByAbortedScan = case StorageType of
        posix -> ExpectedCreated + 1;
        s3 -> ExpectedCreated - 1
    end,
    ?assert(Scan1Created + Scan1Modified + Scan1Unmodified =< MaxProcessedByAbortedScan),

    %% the next scan imports the rest of the tree
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2, #{}, ?LARGE_IMPORT_SCAN_ATTEMPTS),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => skip,
        <<"unmodified">> => skip,
        <<"createdMinHist">> => skip,
        <<"createdHourHist">> => skip,
        %% cumulative over both scans: the whole tree, imported exactly once
        <<"createdDayHist">> => ExpectedCreated
    }).


%% --- protection ---
%%
%% Entries under a dataset with protection flags set must be left alone by
%% continuous scans: neither a storage-side modification (content/mode) nor a
%% storage-side deletion may be reflected in the space while the flags are set.
%% Each scenario runs 3 scans: initial import, mutation under protection (scan
%% 2 must detect but suppress the change) and a re-scan after unsetting the
%% flags (scan 3 must finally apply it). Scan 3 re-detects the change with NO
%% further storage mutation: on suppressing a protected change the scan marks
%% the parent's storage_sync_info (any_protected_child_changed), which withholds
%% committing the parent's children-attrs hashes and mtime
%% (storage_sync_info:mark_processed_batch/8) - so the next scan sees the same
%% storage-vs-space difference again.
%%
%% Counter-wise the suppression shows up as follows (all worked out from
%% storage_sync_traverse:do_update_master_job/2,
%% storage_import_engine:maybe_update_attrs/4 and
%% storage_import_deletion:maybe_delete_file_and_update_counters/3):
%%  * a suppressed file modification counts as "unmodified";
%%  * a protected DIRECTORY is not counted at all (its master job returns
%%    before the counter update) and jobs for its children are never scheduled;
%%  * a protected entry found missing from the storage by deletion detection is
%%    skipped without touching the created/modified/unmodified/deleted counters.


file_with_data_protection_should_not_be_updated_test(SuiteCtx) ->
    file_with_protection_flags_should_not_be_updated_test_base(
        ?FUNCTION_NAME, SuiteCtx, ?DATA_PROTECTION
    ).


file_with_data_and_metadata_protection_should_not_be_updated_test(SuiteCtx) ->
    file_with_protection_flags_should_not_be_updated_test_base(
        ?FUNCTION_NAME, SuiteCtx, ?set_flags(?DATA_PROTECTION, ?METADATA_PROTECTION)
    ).


%% @private
%% A protected file is appended to and (on POSIX) chmod-ed directly on the
%% storage - scan 2 must suppress both, scan 3 (after the flags are unset) must
%% apply them in full. The chmod half is POSIX-only: object storages hold no
%% per-object POSIX mode (the helper's chmod is a no-op there), so on S3 only
%% the appended content/size is exercised.
-spec file_with_protection_flags_should_not_be_updated_test_base(
    atom(), storage_import_test_utils:suite_ctx(), data_access_control:bitmask()
) ->
    ok.
file_with_protection_flags_should_not_be_updated_test_base(TestCaseName, SuiteCtx, ProtectionFlags) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(),
    AppendedContent = ?RAND_STR(),
    NewMode = 8#777,
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx,
        non_importing_provider_ctx = NonImportingProviderCtx
    } = storage_import_test_utils:setup_and_verify_initial_import(
        TestCaseName, #file_spec{name = FileName, content = InitialContent}, SuiteCtx
    ),

    SpaceFilePath = filepath_utils:join([SpacePath, FileName]),
    DatasetId = establish_dataset_with_protection_flags(
        ImportingProviderCtx, SpaceFilePath, ProtectionFlags
    ),

    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId,
        byte_size(InitialContent), AppendedContent
    ),
    ?IF_POSIX(StorageType, storage_file_setup_utils:chmod(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, NewMode
    )),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% the change must not have been applied
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceFilePath,
        with_posix_mode(StorageType, ?DEFAULT_FILE_PERMS, #{size => byte_size(InitialContent)})
    ),
    storage_import_test_utils:assert_file_content(
        ImportingProviderCtx, SpaceFilePath, InitialContent
    ),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        %% the space root + the file (a suppressed modification counts as "unmodified")
        <<"unmodified">> => 2
    }),

    unset_dataset_protection_flags(ImportingProviderCtx, DatasetId, ProtectionFlags),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 3),

    UpdatedContent = <<InitialContent/binary, AppendedContent/binary>>,
    storage_import_test_utils:verify_imported_tree(
        TestCaseCtx, #file_spec{name = FileName, content = UpdatedContent}
    ),
    %% with the flags unset, the append is applied on both storages; the chmod is
    %% POSIX-only (a no-op on object storage), so its effect is asserted there only
    ExpectedAttrs = with_posix_mode(
        StorageType, NewMode, #{size => byte_size(UpdatedContent)}
    ),
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceFilePath, ExpectedAttrs),
    storage_import_test_utils:assert_attrs(NonImportingProviderCtx, SpaceFilePath, ExpectedAttrs),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 3,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"unmodified">> => 1,
        modified_hist => 1,
        %% over a three-scan run the initial import may age out of the 60 s Min window
        <<"createdMinHist">> => {range, 0, 1}
    }).


file_with_data_protection_should_not_be_deleted_test(SuiteCtx) ->
    file_with_protection_flags_should_not_be_deleted_test_base(
        ?FUNCTION_NAME, SuiteCtx, ?DATA_PROTECTION
    ).


file_with_data_and_metadata_protection_should_not_be_deleted_test(SuiteCtx) ->
    file_with_protection_flags_should_not_be_deleted_test_base(
        ?FUNCTION_NAME, SuiteCtx, ?set_flags(?DATA_PROTECTION, ?METADATA_PROTECTION)
    ).


%% @private
%% A protected file is removed directly from the storage - scan 2 must not
%% delete it from the space (its metadata stays intact, although reading the
%% data genuinely fails, as it is already gone from the storage); scan 3 (after
%% the flags are unset) must finally delete it.
-spec file_with_protection_flags_should_not_be_deleted_test_base(
    atom(), storage_import_test_utils:suite_ctx(), data_access_control:bitmask()
) ->
    ok.
file_with_protection_flags_should_not_be_deleted_test_base(TestCaseName, SuiteCtx, ProtectionFlags) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,
    FileName = ?RAND_STR(),
    Content = ?RAND_STR(),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        file_tree_spec = FileTreeSpec,
        importing_provider_ctx = ImportingProviderCtx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        }
    } = storage_import_test_utils:setup_and_verify_initial_import(
        TestCaseName, #file_spec{name = FileName, content = Content}, SuiteCtx
    ),

    SpaceFilePath = filepath_utils:join([SpacePath, FileName]),
    DatasetId = establish_dataset_with_protection_flags(
        ImportingProviderCtx, SpaceFilePath, ProtectionFlags
    ),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_import_test_utils:delete_file_tree_from_storage(
        ImportingProviderSelector, ImportedStorageId, FileTreeSpec
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% the file must have stayed in the space with its metadata intact...
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceFilePath,
        with_posix_mode(StorageType, ?DEFAULT_FILE_PERMS, #{size => byte_size(Content)})
    ),
    %% ...but its data is genuinely gone from the storage: opening the file
    %% still succeeds (its metadata is intact and the storage file is not
    %% touched until the data is actually accessed - lazily on object storages,
    %% and via direct IO on POSIX), yet reading its data fails, as the backing
    %% storage file has been removed
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(
        ImportingProviderNode, ImportingProviderSessionId, {path, SpaceFilePath}, read
    )),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:read(
        ImportingProviderNode, Handle, 0, byte_size(Content)
    )),
    ok = lfm_proxy:close(ImportingProviderNode, Handle),
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        modified_hist => RootModified
    }),

    unset_dataset_protection_flags(ImportingProviderCtx, DatasetId, ProtectionFlags),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 3),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 3,
        <<"created">> => 0,
        <<"deleted">> => 1,
        <<"unmodified">> => 1,
        deleted_hist => 1,
        <<"modifiedHourHist">> => RootModified,
        <<"modifiedDayHist">> => RootModified,
        <<"modifiedMinHist">> => {range, 0, RootModified},
        %% over a three-scan run the initial import may age out of the 60 s Min window
        <<"createdMinHist">> => {range, 0, 1}
    }).


empty_dir_with_data_protection_should_not_be_updated_test(SuiteCtx) ->
    empty_dir_with_protection_flags_should_not_be_updated_test_base(
        ?FUNCTION_NAME, SuiteCtx, ?DATA_PROTECTION
    ).


empty_dir_with_data_and_metadata_protection_should_not_be_updated_test(SuiteCtx) ->
    empty_dir_with_protection_flags_should_not_be_updated_test_base(
        ?FUNCTION_NAME, SuiteCtx, ?set_flags(?DATA_PROTECTION, ?METADATA_PROTECTION)
    ).


%% @private
%% A protected empty directory is chmod-ed directly on the storage - scan 2
%% must suppress the change, scan 3 (after the flags are unset) must apply it.
%% A directory's mode change is (re)detected by comparing its own storage attrs
%% against the logical ones on every scan (every visited directory gets its own
%% master job), independently of the children-hash/mtime machinery. POSIX-only
%% (an empty directory has no storage object on S3, nor a mode to change).
-spec empty_dir_with_protection_flags_should_not_be_updated_test_base(
    atom(), storage_import_test_utils:suite_ctx(), data_access_control:bitmask()
) ->
    ok.
empty_dir_with_protection_flags_should_not_be_updated_test_base(TestCaseName, SuiteCtx, ProtectionFlags) ->
    #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,
    DirName = ?RAND_STR(),
    NewMode = 8#777,
    StorageDirId = filepath_utils:join([<<"/">>, DirName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx,
        non_importing_provider_ctx = NonImportingProviderCtx
    } = storage_import_test_utils:setup_and_verify_initial_import(
        TestCaseName, #dir_spec{name = DirName}, SuiteCtx
    ),

    SpaceDirPath = filepath_utils:join([SpacePath, DirName]),
    DatasetId = establish_dataset_with_protection_flags(
        ImportingProviderCtx, SpaceDirPath, ProtectionFlags
    ),

    storage_file_setup_utils:chmod(
        ImportingProviderSelector, ImportedStorageId, StorageDirId, NewMode
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% the change must not have been applied
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceDirPath, #{
        mode => ?DEFAULT_DIR_PERMS
    }),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        %% the space root only - a protected directory is not counted at all
        <<"unmodified">> => 1
    }),

    unset_dataset_protection_flags(ImportingProviderCtx, DatasetId, ProtectionFlags),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 3),

    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceDirPath, #{mode => NewMode}),
    storage_import_test_utils:assert_attrs(NonImportingProviderCtx, SpaceDirPath, #{mode => NewMode}),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 3,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"unmodified">> => 1,
        modified_hist => 1,
        %% over a three-scan run the initial import may age out of the 60 s Min window
        <<"createdMinHist">> => {range, 0, 1}
    }).


empty_dir_with_data_protection_should_not_be_deleted_test(SuiteCtx) ->
    empty_dir_with_protection_flags_should_not_be_deleted_test_base(
        ?FUNCTION_NAME, SuiteCtx, ?DATA_PROTECTION
    ).


empty_dir_with_data_and_metadata_protection_should_not_be_deleted_test(SuiteCtx) ->
    empty_dir_with_protection_flags_should_not_be_deleted_test_base(
        ?FUNCTION_NAME, SuiteCtx, ?set_flags(?DATA_PROTECTION, ?METADATA_PROTECTION)
    ).


%% @private
%% A protected empty directory is removed directly from the storage - scan 2
%% must not delete it from the space, scan 3 (after the flags are unset) must.
%% POSIX-only (an empty directory has no storage object on S3 to remove).
-spec empty_dir_with_protection_flags_should_not_be_deleted_test_base(
    atom(), storage_import_test_utils:suite_ctx(), data_access_control:bitmask()
) ->
    ok.
empty_dir_with_protection_flags_should_not_be_deleted_test_base(TestCaseName, SuiteCtx, ProtectionFlags) ->
    #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,
    DirName = ?RAND_STR(),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        file_tree_spec = FileTreeSpec,
        importing_provider_ctx = ImportingProviderCtx
    } = storage_import_test_utils:setup_and_verify_initial_import(
        TestCaseName, #dir_spec{name = DirName}, SuiteCtx
    ),

    SpaceDirPath = filepath_utils:join([SpacePath, DirName]),
    DatasetId = establish_dataset_with_protection_flags(
        ImportingProviderCtx, SpaceDirPath, ProtectionFlags
    ),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_import_test_utils:delete_file_tree_from_storage(
        ImportingProviderSelector, ImportedStorageId, FileTreeSpec
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% the directory must have stayed in the space
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceDirPath, #{
        mode => ?DEFAULT_DIR_PERMS
    }),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 0,
        %% the removal of the root's direct child bumps the root's mtime
        <<"modified">> => 1,
        <<"unmodified">> => 0,
        modified_hist => 1
    }),

    unset_dataset_protection_flags(ImportingProviderCtx, DatasetId, ProtectionFlags),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 3),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 3,
        <<"created">> => 0,
        <<"deleted">> => 1,
        <<"unmodified">> => 1,
        deleted_hist => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"modifiedMinHist">> => {range, 0, 1},
        %% over a three-scan run the initial import may age out of the 60 s Min window
        <<"createdMinHist">> => {range, 0, 1}
    }).


dir_and_its_child_with_data_protection_should_not_be_updated_test(SuiteCtx) ->
    dir_and_its_child_with_protection_flags_should_not_be_updated_test_base(
        ?FUNCTION_NAME, SuiteCtx, ?DATA_PROTECTION
    ).


dir_and_its_child_with_data_and_metadata_protection_should_not_be_updated_test(SuiteCtx) ->
    dir_and_its_child_with_protection_flags_should_not_be_updated_test_base(
        ?FUNCTION_NAME, SuiteCtx, ?set_flags(?DATA_PROTECTION, ?METADATA_PROTECTION)
    ).


%% @private
%% A directory holding a file is protected (the file inherits the flags via the
%% effective dataset), then both are mutated directly on the storage (the dir is
%% chmod-ed; the file is appended to and chmod-ed). Scan 2 must suppress all of
%% it - having detected the dir's own change, it does not even schedule jobs for
%% the protected dir's children, so the file's mutation stays invisible; scan 3
%% (after the flags are unset) must apply everything.
%%
%% POSIX-only: on a flat (object) storage the re-application half breaks down
%% for entries nested under a directory - the suppressed-change marker lands on
%% the storage_sync_info of the file's direct parent (a virtual directory whose
%% document no flat-storage scan ever consults), while the space root's
%% children-attrs hash - the only change-detection input on a flat storage -
%% gets committed by the suppressing scan, so a later scan bulk-skips the file
%% and the suppressed change is never applied.
-spec dir_and_its_child_with_protection_flags_should_not_be_updated_test_base(
    atom(), storage_import_test_utils:suite_ctx(), data_access_control:bitmask()
) ->
    ok.
dir_and_its_child_with_protection_flags_should_not_be_updated_test_base(TestCaseName, SuiteCtx, ProtectionFlags) ->
    #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,
    DirName = ?RAND_STR(),
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(),
    AppendedContent = ?RAND_STR(),
    NewMode = 8#777,
    StorageDirId = filepath_utils:join([<<"/">>, DirName]),
    StorageFileId = filepath_utils:join([StorageDirId, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx,
        non_importing_provider_ctx = NonImportingProviderCtx
    } = storage_import_test_utils:setup_and_verify_initial_import(
        TestCaseName, #dir_spec{name = DirName, children = [
            #file_spec{name = FileName, content = InitialContent}
        ]}, SuiteCtx
    ),

    SpaceDirPath = filepath_utils:join([SpacePath, DirName]),
    SpaceFilePath = filepath_utils:join([SpaceDirPath, FileName]),
    DatasetId = establish_dataset_with_protection_flags(
        ImportingProviderCtx, SpaceDirPath, ProtectionFlags
    ),

    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId,
        byte_size(InitialContent), AppendedContent
    ),
    storage_file_setup_utils:chmod(
        ImportingProviderSelector, ImportedStorageId, StorageDirId, NewMode
    ),
    storage_file_setup_utils:chmod(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, NewMode
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% neither the dir nor the file must have been modified
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceDirPath, #{
        mode => ?DEFAULT_DIR_PERMS
    }),
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceFilePath, #{
        size => byte_size(InitialContent), mode => ?DEFAULT_FILE_PERMS
    }),
    storage_import_test_utils:assert_file_content(
        ImportingProviderCtx, SpaceFilePath, InitialContent
    ),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        %% the space root only - the protected directory is not counted at all
        %% and its child never even gets a job
        <<"unmodified">> => 1
    }),

    unset_dataset_protection_flags(ImportingProviderCtx, DatasetId, ProtectionFlags),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 3),

    UpdatedContent = <<InitialContent/binary, AppendedContent/binary>>,
    storage_import_test_utils:verify_imported_tree(
        TestCaseCtx, #dir_spec{name = DirName, children = [
            #file_spec{name = FileName, content = UpdatedContent}
        ]}
    ),
    lists:foreach(fun(ProviderCtx) ->
        storage_import_test_utils:assert_attrs(ProviderCtx, SpaceDirPath, #{mode => NewMode}),
        storage_import_test_utils:assert_attrs(ProviderCtx, SpaceFilePath, #{
            size => byte_size(UpdatedContent), mode => NewMode
        })
    end, [ImportingProviderCtx, NonImportingProviderCtx]),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 3,
        <<"created">> => 0,
        %% the dir + the file
        <<"modified">> => 2,
        <<"unmodified">> => 1,
        modified_hist => 2,
        %% over a three-scan run the initial import may age out of the 60 s Min window
        <<"createdMinHist">> => {range, 0, 2}
    }).


dir_and_its_child_with_data_protection_should_not_be_deleted_test(SuiteCtx) ->
    dir_and_its_child_with_protection_flags_should_not_be_deleted_test_base(
        ?FUNCTION_NAME, SuiteCtx, ?DATA_PROTECTION
    ).


dir_and_its_child_with_data_and_metadata_protection_should_not_be_deleted_test(SuiteCtx) ->
    dir_and_its_child_with_protection_flags_should_not_be_deleted_test_base(
        ?FUNCTION_NAME, SuiteCtx, ?set_flags(?DATA_PROTECTION, ?METADATA_PROTECTION)
    ).


%% @private
%% A protected directory holding a file is removed (whole) directly from the
%% storage - scan 2 must not delete it from the space (deletion detection skips
%% the protected dir without ever descending to the child), scan 3 (after the
%% flags are unset) must delete the whole subtree.
-spec dir_and_its_child_with_protection_flags_should_not_be_deleted_test_base(
    atom(), storage_import_test_utils:suite_ctx(), data_access_control:bitmask()
) ->
    ok.
dir_and_its_child_with_protection_flags_should_not_be_deleted_test_base(TestCaseName, SuiteCtx, ProtectionFlags) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,
    DirName = ?RAND_STR(),
    FileName = ?RAND_STR(),
    Content = ?RAND_STR(),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        file_tree_spec = FileTreeSpec,
        importing_provider_ctx = ImportingProviderCtx
    } = storage_import_test_utils:setup_and_verify_initial_import(
        TestCaseName, #dir_spec{name = DirName, children = [
            #file_spec{name = FileName, content = Content}
        ]}, SuiteCtx
    ),

    SpaceDirPath = filepath_utils:join([SpacePath, DirName]),
    SpaceFilePath = filepath_utils:join([SpaceDirPath, FileName]),
    DatasetId = establish_dataset_with_protection_flags(
        ImportingProviderCtx, SpaceDirPath, ProtectionFlags
    ),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_import_test_utils:delete_file_tree_from_storage(
        ImportingProviderSelector, ImportedStorageId, FileTreeSpec
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% both entries must have stayed in the space with their metadata intact
    %% (the file's data itself is gone from the storage, so it is not read here)
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceDirPath,
        with_posix_mode(StorageType, ?DEFAULT_DIR_PERMS, #{})
    ),
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceFilePath,
        with_posix_mode(StorageType, ?DEFAULT_FILE_PERMS, #{size => byte_size(Content)})
    ),
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        modified_hist => RootModified
    }),

    unset_dataset_protection_flags(ImportingProviderCtx, DatasetId, ProtectionFlags),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 3),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []),
    %% the (emulated/real) directory + its child are deleted on both storage types
    Deleted = storage_import_test_utils:expected_deleted_count(TestCaseCtx),
    Scan1Created = storage_import_test_utils:expected_created_count(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 3,
        <<"created">> => 0,
        <<"deleted">> => Deleted,
        <<"unmodified">> => 1,
        deleted_hist => Deleted,
        <<"modifiedHourHist">> => RootModified,
        <<"modifiedDayHist">> => RootModified,
        <<"modifiedMinHist">> => {range, 0, RootModified},
        %% over a three-scan run the initial import may age out of the 60 s Min window
        <<"createdMinHist">> => {range, 0, Scan1Created}
    }).


%% --- not reimported ---


%% An entry imported by the initial scan is deleted via LFM, but its removal
%% from the imported storage fails, leaving the storage entry behind - the next
%% scan must not reimport it (see should_not_reimport_leftover_entry_test_base).
%% This variant: a directory, deleted from the NON-importing provider, with the
%% helper's rmdir mocked to fail. POSIX-only (object storages have no real
%% directories to leave behind, nor an rmdir to fail).
should_not_reimport_directory_that_was_not_successfully_deleted_from_storage_test(SuiteCtx) ->
    should_not_reimport_leftover_entry_test_base(?FUNCTION_NAME, SuiteCtx, directory).


%% Like the directory variant above, but for a regular file, deleted from the
%% IMPORTING provider, with the helper's unlink mocked to fail - together the
%% two variants cover both entry types and both deletion origins.
should_not_reimport_file_that_was_not_successfully_deleted_from_storage_test(SuiteCtx) ->
    should_not_reimport_leftover_entry_test_base(?FUNCTION_NAME, SuiteCtx, file).


%% @private
-spec should_not_reimport_leftover_entry_test_base(
    atom(), storage_import_test_utils:suite_ctx(), directory | file
) ->
    ok.
should_not_reimport_leftover_entry_test_base(TestCaseName, SuiteCtx, EntryType) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,
    EntryName = ?RAND_STR(),
    EntrySpec = case EntryType of
        directory -> #dir_spec{name = EntryName};
        file -> #file_spec{name = EntryName, content = ?RAND_STR()}
    end,

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        },
        non_importing_provider_ctx = #provider_ctx{
            node = NonImportingProviderNode,
            session_id = NonImportingProviderSessionId
        }
    } = storage_import_test_utils:setup_and_verify_initial_import(TestCaseName, EntrySpec, SuiteCtx),

    %% the LFM deletion succeeds logically, but the (mocked) removal from the
    %% imported storage fails and the storage entry is left behind
    SpaceEntryPath = filepath_utils:join([SpacePath, EntryName]),
    case EntryType of
        directory ->
            mock_storage_helper_error(SuiteCtx, rmdir, ?ENOTEMPTY),
            lfm_proxy:rm_recursive(
                NonImportingProviderNode, NonImportingProviderSessionId, {path, SpaceEntryPath}
            );
        file ->
            mock_storage_helper_error(SuiteCtx, unlink, ?EBUSY),
            lfm_proxy:rm_recursive(
                ImportingProviderNode, ImportingProviderSessionId, {path, SpaceEntryPath}
            )
    end,
    %% the deletion may originate on the non-importing provider (directory
    %% variant), so awaiting it on the importing provider must tolerate the
    %% cross-provider propagation lag
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(ImportingProviderNode, ImportingProviderSessionId, {path, SpaceEntryPath}),
        ?CROSS_PROVIDER_PROPAGATION_ATTEMPTS
    ),
    EntryStorageFileId = filepath_utils:join([<<"/">>, EntryName]),
    ?assertMatch({ok, _}, storage_file_setup_utils:stat(
        ImportingProviderSelector, ImportedStorageId, EntryStorageFileId
    )),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    TriggerFileName = storage_import_test_utils:create_trigger_file_on_storage(TestCaseCtx),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(ImportingProviderNode, ImportingProviderSessionId, {path, SpaceEntryPath})
    ),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(NonImportingProviderNode, NonImportingProviderSessionId, {path, SpaceEntryPath}),
        ?CROSS_PROVIDER_PROPAGATION_ATTEMPTS
    ),
    ?assertMatch(
        {ok, [{_, TriggerFileName}]},
        lfm_proxy:get_children(
            ImportingProviderNode, ImportingProviderSessionId, {path, SpacePath}, 0, 10
        )
    ),
    %% modified: the root (posix only); unmodified: the leftover entry (+ the
    %% root on s3)
    {Scan2Modified, Scan2Unmodified} = case StorageType of
        posix -> {1, 1};
        s3 -> {0, 2}
    end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => Scan2Modified,
        <<"unmodified">> => Scan2Unmodified,
        created_hist => 2,
        modified_hist => Scan2Modified
    }).


should_not_delete_not_replicated_file_created_in_remote_provider_test(SuiteCtx) ->
    should_not_delete_remote_entries_test_base(
        ?FUNCTION_NAME, SuiteCtx, #file_spec{content = ?RAND_STR()}
    ).


should_not_delete_dir_created_in_remote_provider_test(SuiteCtx) ->
    should_not_delete_remote_entries_test_base(?FUNCTION_NAME, SuiteCtx, #dir_spec{}).


should_not_delete_not_replicated_file_in_dir_created_in_remote_provider_test(SuiteCtx) ->
    should_not_delete_remote_entries_test_base(?FUNCTION_NAME, SuiteCtx, #dir_spec{
        children = [#file_spec{content = ?RAND_STR()}]
    }).


%% @private
%% Shared body of the should_not_delete_*_created_in_remote_provider tests.
%% A file tree created in the space via the non-importing provider has no
%% counterpart on the imported storage (see
%% create_file_tree_via_remote_provider/2). A scan - forced to do real work by
%% a trigger file created on the storage - must not mistake the missing storage
%% entries for deletions: every entry must stay in the space on both providers
%% (files with their content intact) and must not materialize on the imported
%% storage.
-spec should_not_delete_remote_entries_test_base(
    atom(), storage_import_test_utils:suite_ctx(), onenv_file_test_utils:object_spec()
) ->
    ok.
should_not_delete_remote_entries_test_base(TestCaseName, SuiteCtx, RemoteFileTreeSpec) ->
    #storage_import_test_suite_ctx{storage_type = StorageType} = SuiteCtx,

    TestCaseCtx = storage_import_test_utils:init_testcase(TestCaseName, undefined, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    RemoteEntries = create_file_tree_via_remote_provider(TestCaseCtx, RemoteFileTreeSpec),

    %% load-bearing - opens the deletion-detection gate: the remote entries can
    %% only be (wrongly) deleted by deletion detection, so without this call the
    %% scan would never even attempt deletions (on the flat storage in
    %% particular) and the assertion below would pass vacuously
    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_import_test_utils:create_trigger_file_on_storage(TestCaseCtx),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    assert_remote_entries_not_affected_by_scan(TestCaseCtx, RemoteEntries),
    assert_monitoring_state_after_trigger_file_import(TestCaseCtx, StorageType).


%% While a file (created via the non-importing provider) is being replicated
%% to the importing provider, its storage file already exists - partially
%% written by rtransfer, which is artificially slowed down by a mock - while
%% continuous scans run every second: the scans must recognize the storage
%% file as a replication target in progress and must not sync it. After the
%% transfer completes, both providers must hold the full, valid replica.
should_not_sync_file_during_replication_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector,
        non_importing_provider_selector = NonImportingProviderSelector
    } = SuiteCtx,
    Content = ?RAND_STR(100),
    ContentSize = byte_size(Content),

    TestCaseCtx = #storage_import_test_case_ctx{
        importing_provider_ctx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        },
        non_importing_provider_ctx = #provider_ctx{
            node = NonImportingProviderNode,
            session_id = NonImportingProviderSessionId
        }
    } = storage_import_test_utils:init_testcase(?FUNCTION_NAME, undefined, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    #object{guid = FileGuid} = create_file_tree_via_remote_provider(
        TestCaseCtx, #file_spec{content = Content}
    ),

    ImportingProviderId = oct_background:get_provider_id(ImportingProviderSelector),
    NonImportingProviderId = oct_background:get_provider_id(NonImportingProviderSelector),
    file_test_utils:await_distribution(ImportingProviderNode, FileGuid, [
        {ImportingProviderNode, 0},
        {NonImportingProviderNode, ContentSize}
    ]),
    %% TODO VFS-9498 - not needed once file replication uses fetched file
    %% location instead of the dbsynced knowledge awaited here
    ?assertMatch(
        {ok, [[0, ContentSize]]},
        opt_file_metadata:get_local_knowledge_of_remote_provider_blocks(
            ImportingProviderNode, FileGuid, NonImportingProviderId
        ),
        ?ATTEMPTS
    ),

    %% slow rtransfer down so that the partially-replicated storage file is
    %% exposed to several scan cycles
    mock_rtransfer_open_delay(SuiteCtx, 10),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),

    {ok, TransferId} = opt_transfers:schedule_file_replication(
        ImportingProviderNode, ImportingProviderSessionId, ?FILE_REF(FileGuid), ImportingProviderId
    ),
    ?assertMatch(
        {ok, #document{value = #transfer{replication_status = completed}}},
        ?rpc(ImportingProviderSelector, transfer:get(TransferId)),
        ?TRANSFER_ATTEMPTS
    ),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the scans must not have invalidated any replica blocks (as seen by both providers)
    file_test_utils:await_distribution([ImportingProviderNode, NonImportingProviderNode], FileGuid, [
        {ImportingProviderNode, ContentSize},
        {NonImportingProviderNode, ContentSize}
    ]),
    ?assertMatch(
        {ok, #file_attr{size = ContentSize}},
        lfm_proxy:stat(NonImportingProviderNode, NonImportingProviderSessionId, ?FILE_REF(FileGuid))
    ).


%%%===================================================================
%%% Internal functions - shared test steps
%%%===================================================================


%% @private
%% @doc
%% Creates the given file tree in the space via the NON-importing (remote)
%% provider - purely logically, so nothing materializes on the imported storage
%% (a remotely-created file's data stays remote until replicated; a directory
%% gets no storage counterpart until a file is written into it on the importing
%% provider). Awaits the metadata propagation to the importing provider and
%% returns the created tree with all names concretized.
%% @end
-spec create_file_tree_via_remote_provider(
    storage_import_test_utils:case_ctx(), onenv_file_test_utils:object_spec()
) ->
    onenv_file_test_utils:object().
create_file_tree_via_remote_provider(#storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{
        non_importing_provider_selector = NonImportingProviderSelector,
        space_owner_selector = SpaceOwnerSelector
    },
    space_id = SpaceId,
    space_path = SpacePath,
    importing_provider_ctx = #provider_ctx{
        node = ImportingProviderNode,
        session_id = ImportingProviderSessionId
    }
}, FileTreeSpec) ->
    Object = onenv_file_test_utils:create_file_tree(
        oct_background:get_user_id(SpaceOwnerSelector),
        space_dir:guid(SpaceId),
        NonImportingProviderSelector,
        FileTreeSpec
    ),
    lists:foreach(fun({PathSegments, _}) ->
        ?assertMatch(
            {ok, #file_attr{}},
            lfm_proxy:stat(
                ImportingProviderNode, ImportingProviderSessionId,
                {path, filepath_utils:join([SpacePath | PathSegments])}
            ),
            ?CROSS_PROVIDER_PROPAGATION_ATTEMPTS
        )
    end, flatten_objects(Object)),
    Object.


%% @private
%% Flattens a created file tree into a list of every node (the declared root
%% included) tagged with its path segments relative to the tree's parent.
-spec flatten_objects(onenv_file_test_utils:object()) ->
    [{[file_meta:name()], onenv_file_test_utils:object()}].
flatten_objects(Object = #object{name = Name, children = Children}) ->
    [{[Name], Object} | [
        {[Name | DescendantSegments], Descendant}
        || Child <- utils:ensure_defined(Children, []),
           {DescendantSegments, Descendant} <- flatten_objects(Child)
    ]].


%% @private
%% @doc
%% Arranges, via LFM on the importing provider, the "recreated file" suffixed
%% storage layout: a file is created and opened, then deleted - the open handle
%% keeps its storage file alive under the plain name, guarded by a deletion
%% marker (the method is enforced via init_per_testcase's fslogic_delete mock)
%% - and a new file is created at the same path, landing on the storage under
%% a name with the conflicting-file suffix. Asserts both files exist on the
%% storage and returns the layout's details (the old file's handle is returned
%% still open - it is what keeps the plain-named storage file alive).
%% @end
-spec setup_recreated_file_with_suffix_on_storage(storage_import_test_utils:case_ctx()) ->
    #{atom() => term()}.
setup_recreated_file_with_suffix_on_storage(TestCaseCtx = #storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector
    },
    imported_storage_id = ImportedStorageId,
    space_path = SpacePath,
    importing_provider_ctx = #provider_ctx{
        node = ImportingProviderNode,
        session_id = ImportingProviderSessionId
    }
}) ->
    FileName = ?RAND_STR(),
    OldContent = ?RAND_STR(),
    RecreatedContent = ?RAND_STR(),
    SpaceFilePath = filepath_utils:join([SpacePath, FileName]),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),

    {ok, OldFileGuid} = lfm_proxy:create(
        ImportingProviderNode, ImportingProviderSessionId, SpaceFilePath
    ),
    write_file_via_lfm(ImportingProviderNode, ImportingProviderSessionId, OldFileGuid, OldContent),
    {ok, OldFileHandle} = lfm_proxy:open(
        ImportingProviderNode, ImportingProviderSessionId, ?FILE_REF(OldFileGuid), read
    ),
    ok = lfm_proxy:unlink(
        ImportingProviderNode, ImportingProviderSessionId, ?FILE_REF(OldFileGuid)
    ),

    {ok, RecreatedFileGuid} = lfm_proxy:create(
        ImportingProviderNode, ImportingProviderSessionId, SpaceFilePath
    ),
    write_file_via_lfm(
        ImportingProviderNode, ImportingProviderSessionId, RecreatedFileGuid, RecreatedContent
    ),

    PlainStorageFileId = filepath_utils:join([<<"/">>, FileName]),
    RecreatedFileUuid = file_id:guid_to_uuid(RecreatedFileGuid),
    SuffixedStorageFileId = ?CONFLICTING_STORAGE_FILE_NAME(PlainStorageFileId, RecreatedFileUuid),
    ?assertMatch({ok, _}, storage_file_setup_utils:stat(
        ImportingProviderSelector, ImportedStorageId, PlainStorageFileId
    ), ?ATTEMPTS),
    ?assertMatch({ok, _}, storage_file_setup_utils:stat(
        ImportingProviderSelector, ImportedStorageId, SuffixedStorageFileId
    ), ?ATTEMPTS),

    #{
        file_name => FileName,
        space_file_path => SpaceFilePath,
        old_file_handle => OldFileHandle,
        old_content => OldContent,
        recreated_file_guid => RecreatedFileGuid,
        recreated_content => RecreatedContent,
        suffixed_storage_file_id => SuffixedStorageFileId
    }.


%% @private
%% @doc
%% Arranges the "replicated file" suffixed storage layout: a file is created
%% via the importing provider (its storage file taking the plain name) and a
%% same-named file is created via the non-importing provider; reading the
%% latter on the importing provider replicates its data onto the imported
%% storage, where - its plain name being taken - it lands under a name with
%% the conflicting-file suffix. Asserts both files exist on the storage and
%% returns the layout's details.
%% @end
-spec setup_replicated_file_with_suffix_on_storage(storage_import_test_utils:case_ctx()) ->
    #{atom() => term()}.
setup_replicated_file_with_suffix_on_storage(TestCaseCtx = #storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector
    },
    imported_storage_id = ImportedStorageId,
    space_path = SpacePath,
    importing_provider_ctx = #provider_ctx{
        node = ImportingProviderNode,
        session_id = ImportingProviderSessionId
    },
    non_importing_provider_ctx = #provider_ctx{
        node = NonImportingProviderNode,
        session_id = NonImportingProviderSessionId
    }
}) ->
    FileName = ?RAND_STR(),
    LocalContent = ?RAND_STR(),
    RemoteContent = ?RAND_STR(),
    SpaceFilePath = filepath_utils:join([SpacePath, FileName]),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),

    {ok, LocalFileGuid} = lfm_proxy:create(
        ImportingProviderNode, ImportingProviderSessionId, SpaceFilePath
    ),
    write_file_via_lfm(ImportingProviderNode, ImportingProviderSessionId, LocalFileGuid, LocalContent),

    {ok, RemoteFileGuid} = lfm_proxy:create(
        NonImportingProviderNode, NonImportingProviderSessionId, SpaceFilePath
    ),
    write_file_via_lfm(
        NonImportingProviderNode, NonImportingProviderSessionId, RemoteFileGuid, RemoteContent
    ),

    %% reading the remotely created file on the importing provider replicates
    %% its data onto the imported storage
    {ok, ReadHandle} = ?assertMatch({ok, _}, lfm_proxy:open(
        ImportingProviderNode, ImportingProviderSessionId, ?FILE_REF(RemoteFileGuid), read
    ), ?CROSS_PROVIDER_PROPAGATION_ATTEMPTS),
    ?assertEqual({ok, RemoteContent}, lfm_proxy:read(
        ImportingProviderNode, ReadHandle, 0, byte_size(RemoteContent)
    ), ?ATTEMPTS),
    ok = lfm_proxy:close(ImportingProviderNode, ReadHandle),

    PlainStorageFileId = filepath_utils:join([<<"/">>, FileName]),
    RemoteFileUuid = file_id:guid_to_uuid(RemoteFileGuid),
    SuffixedStorageFileId = ?CONFLICTING_STORAGE_FILE_NAME(PlainStorageFileId, RemoteFileUuid),
    ?assertMatch({ok, _}, storage_file_setup_utils:stat(
        ImportingProviderSelector, ImportedStorageId, PlainStorageFileId
    ), ?ATTEMPTS),
    ?assertMatch({ok, _}, storage_file_setup_utils:stat(
        ImportingProviderSelector, ImportedStorageId, SuffixedStorageFileId
    ), ?ATTEMPTS),

    #{
        file_name => FileName,
        space_file_path => SpaceFilePath,
        local_file_guid => LocalFileGuid,
        local_content => LocalContent,
        remote_file_guid => RemoteFileGuid,
        remote_content => RemoteContent,
        suffixed_storage_file_id => SuffixedStorageFileId
    }.


%% @private
-spec write_file_via_lfm(oct_background:node(), session:id(), file_id:file_guid(), binary()) ->
    ok.
write_file_via_lfm(Node, SessionId, FileGuid, Content) ->
    {ok, WriteHandle} = lfm_proxy:open(Node, SessionId, ?FILE_REF(FileGuid), write),
    {ok, _} = lfm_proxy:write(Node, WriteHandle, 0, Content),
    ok = lfm_proxy:close(Node, WriteHandle).


%% @private
%% @doc
%% Overwrites a single byte of the given storage file (directly on the storage)
%% and returns the expected content of the file after the change - the file's
%% size (and content prefix up to the changed byte) stays intact, so the scan
%% can detect the change only via the file's storage mtime.
%% @end
-spec change_one_byte_of_storage_file(
    storage_import_test_utils:case_ctx(), helpers:file_id(), binary()
) ->
    binary().
change_one_byte_of_storage_file(#storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector
    },
    imported_storage_id = ImportedStorageId
}, StorageFileId, CurrentContent) ->
    Offset = 4,
    ChangedByte = <<"-">>,
    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, Offset, ChangedByte
    ),
    <<Prefix:Offset/binary, _:1/binary, Suffix/binary>> = CurrentContent,
    <<Prefix/binary, ChangedByte/binary, Suffix/binary>>.


%% @private
%% Establishes a dataset with the given protection flags on the entry at the
%% given path, via the importing provider. The flags take effect (also on the
%% entry's descendants, through the effective dataset) as soon as the call
%% returns.
-spec establish_dataset_with_protection_flags(
    #provider_ctx{}, file_meta:path(), data_access_control:bitmask()
) ->
    dataset:id().
establish_dataset_with_protection_flags(#provider_ctx{
    node = Node,
    session_id = SessionId
}, Path, ProtectionFlags) ->
    {ok, DatasetId} = ?assertMatch({ok, _}, opt_datasets:establish(
        Node, SessionId, {path, Path}, ProtectionFlags
    )),
    DatasetId.


%% @private
-spec unset_dataset_protection_flags(
    #provider_ctx{}, dataset:id(), data_access_control:bitmask()
) ->
    ok.
unset_dataset_protection_flags(#provider_ctx{
    node = Node,
    session_id = SessionId
}, DatasetId, ProtectionFlags) ->
    ok = opt_datasets:update(Node, SessionId, DatasetId, undefined, ?no_flags_mask, ProtectionFlags).


%% @private
%% @doc
%% Builds an expected-attrs map (for assert_attrs/3) that carries the given POSIX
%% mode ONLY on block storages. Object (flat) storages hold no per-object POSIX
%% mode - imported entries take the storage's default mode, not ?DEFAULT_*_PERMS -
%% and none of these tests chmod on S3 (the helper's chmod is a no-op there), so
%% the mode is asserted on POSIX only while the remaining attrs (size, ...) are
%% checked on both storage types.
%% @end
-spec with_posix_mode(posix | s3, file_meta:mode(), #{atom() => term()}) ->
    #{atom() => term()}.
with_posix_mode(posix, Mode, Attrs) -> Attrs#{mode => Mode};
with_posix_mode(s3, _Mode, Attrs) -> Attrs.


%%%===================================================================
%%% Internal functions - shared assertions
%%%===================================================================


%% @private
%% @doc
%% Number of the two arranged storage files that the scan facing a freshly
%% arranged suffixed layout is CERTAIN to classify "modified". A file guarded by a
%% deletion marker is always skipped via its marker (never "modified"); the rest
%% split by storage type:
%%  * a replicated file - its blocks written to the imported storage at
%%    replication time, strictly after the remotely-stamped logical mtime - is
%%    "modified" on ANY storage;
%%  * a locally LFM-created file matches its storage mtime on POSIX (same clock,
%%    one operation) so it stays "unmodified" there, but on flat storage the
%%    object store re-stamps the written object with its own (later) mtime, so it
%%    comes out "modified" too.
%% @end
-spec suffixed_layout_modified_file_count(posix | s3, non_neg_integer(), non_neg_integer()) ->
    non_neg_integer().
suffixed_layout_modified_file_count(posix, _LfmCreatedFileCount, ReplicatedFileCount) ->
    ReplicatedFileCount;
suffixed_layout_modified_file_count(s3, LfmCreatedFileCount, ReplicatedFileCount) ->
    LfmCreatedFileCount + ReplicatedFileCount.


%% @private
%% @doc
%% Asserts that the scan facing a freshly arranged suffixed storage layout (see
%% the setup_*_file_with_suffix_on_storage helpers) neither imported nor deleted
%% anything: both storage files were recognized as belonging to already-known
%% logical files (created => 0, deleted => 0 - the crux of these tests).
%%
%% The {modified, unmodified} split among the three scanned entries (the space
%% root + the two arranged storage files) is incidental and asserted only within
%% tolerances. It is driven by the layout's file composition - given here as the
%% counts of LFM-created and replicated files (see
%% suffixed_layout_modified_file_count/3) - plus the space root's own verdict.
%% The root is racy on POSIX: the layout is arranged via LFM, so the root's
%% storage and logical mtimes are bumped by the same operations and may land in
%% the same second - the root goes either way (see the "Mtime granularity and
%% root-verdict races" section of storage_import_test_utils), hence the
%% {range, ...} tolerances. On flat storage the mocked root statbuf is
%% deterministically "unmodified", so everything is exact.
%% @end
-spec assert_scan_recognized_suffixed_layout_as_known(
    storage_import_test_utils:case_ctx(), non_neg_integer(), non_neg_integer()
) ->
    ok.
assert_scan_recognized_suffixed_layout_as_known(TestCaseCtx = #storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{storage_type = StorageType}
}, LfmCreatedFileCount, ReplicatedFileCount) ->
    ModifiedFileCount = suffixed_layout_modified_file_count(
        StorageType, LfmCreatedFileCount, ReplicatedFileCount
    ),
    RootVerdictOverrides = case StorageType of
        posix -> #{
            <<"modified">> => {range, ModifiedFileCount, ModifiedFileCount + 1},
            <<"unmodified">> => {range, 2 - ModifiedFileCount, 3 - ModifiedFileCount},
            modified_hist => {range, ModifiedFileCount, ModifiedFileCount + 1}
        };
        s3 -> #{
            <<"modified">> => ModifiedFileCount,
            <<"unmodified">> => 3 - ModifiedFileCount,
            modified_hist => ModifiedFileCount
        }
    end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, maps:merge(
        #{
            <<"scans">> => 2,
            <<"created">> => 0,
            <<"deleted">> => 0
        },
        RootVerdictOverrides
    )).


%% @private
%% @doc
%% Asserts that the scan following change_one_byte_of_storage_file/3 detected the
%% change as a modification of the (single) mapped-back logical file - nothing was
%% imported or deleted. This scan reliably reports modified => 1 (only the changed
%% suffixed file; the direct storage write touches no directory listing, so the
%% root stays "unmodified") and unmodified => 2 (the root and the other, untouched
%% storage file - reconciled by scan 2).
%%
%% The cumulative modified*Hist additionally carries scan 2's events: the files it
%% classified "modified" (suffixed_layout_modified_file_count/3) plus, on POSIX,
%% that LFM-arranged scan's racy root verdict (0 or 1) - hence the tolerance. On
%% flat storage the root is deterministically "unmodified", so the histogram is
%% exact.
%% @end
-spec assert_suffixed_storage_file_update_detected(
    storage_import_test_utils:case_ctx(), non_neg_integer(), non_neg_integer()
) ->
    ok.
assert_suffixed_storage_file_update_detected(TestCaseCtx = #storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{storage_type = StorageType}
}, LfmCreatedFileCount, ReplicatedFileCount) ->
    Scan2ModifiedFileCount = suffixed_layout_modified_file_count(
        StorageType, LfmCreatedFileCount, ReplicatedFileCount
    ),
    ModifiedHistOverrides = case StorageType of
        posix -> #{
            modified_hist => {range, Scan2ModifiedFileCount + 1, Scan2ModifiedFileCount + 2}
        };
        s3 -> #{
            modified_hist => Scan2ModifiedFileCount + 1
        }
    end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, maps:merge(
        #{
            <<"scans">> => 3,
            <<"created">> => 0,
            <<"deleted">> => 0,
            <<"modified">> => 1,
            <<"unmodified">> => 2
        },
        ModifiedHistOverrides
    )).


%% @private
%% Like storage_import_test_utils:assert_file_content/3, but addresses the file
%% by guid - required where a path would be ambiguous (name-conflicting files)
%% - and with an explicit retry budget (a content change propagates to the
%% other provider via block invalidation and re-replication, which may take
%% longer than the default).
-spec assert_file_content_by_guid(
    #provider_ctx{}, file_id:file_guid(), binary(), non_neg_integer()
) ->
    ok.
assert_file_content_by_guid(#provider_ctx{
    node = Node,
    session_id = SessionId
}, FileGuid, ExpectedContent, Attempts) ->
    ReadContent = fun() ->
        case lfm_proxy:open(Node, SessionId, ?FILE_REF(FileGuid), read) of
            {ok, Handle} ->
                Result = lfm_proxy:check_size_and_read(
                    Node, Handle, 0, max(byte_size(ExpectedContent), 1)
                ),
                ok = lfm_proxy:close(Node, Handle),
                Result;
            OpenError ->
                OpenError
        end
    end,
    ?assertEqual({ok, ExpectedContent}, ReadContent(), Attempts),
    ok.


%% @private
%% Asserts, for every entry of a remotely-created file tree (see
%% create_file_tree_via_remote_provider/2), that the scan neither imported it
%% onto the storage nor deleted it from the space: the entry still has no
%% storage counterpart, is still reachable on both providers, and a file's
%% content - read via the non-importing provider, where its data lives - is
%% intact.
-spec assert_remote_entries_not_affected_by_scan(
    storage_import_test_utils:case_ctx(), onenv_file_test_utils:object()
) ->
    ok.
assert_remote_entries_not_affected_by_scan(#storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector
    },
    imported_storage_id = ImportedStorageId,
    space_path = SpacePath,
    importing_provider_ctx = #provider_ctx{
        node = ImportingProviderNode,
        session_id = ImportingProviderSessionId
    },
    non_importing_provider_ctx = NonImportingProviderCtx = #provider_ctx{
        node = NonImportingProviderNode,
        session_id = NonImportingProviderSessionId
    }
}, RemoteEntries) ->
    lists:foreach(fun({PathSegments, #object{type = Type, content = Content}}) ->
        %% the entry must not have materialized on the imported storage...
        ?assertMatch({error, ?ENOENT}, storage_file_setup_utils:stat(
            ImportingProviderSelector, ImportedStorageId,
            filepath_utils:join([<<"/">> | PathSegments])
        )),
        %% ...and must stay in the space on both providers
        SpaceEntryPath = filepath_utils:join([SpacePath | PathSegments]),
        ?assertMatch({ok, #file_attr{}},
            lfm_proxy:stat(ImportingProviderNode, ImportingProviderSessionId, {path, SpaceEntryPath})
        ),
        case Type of
            ?REGULAR_FILE_TYPE ->
                storage_import_test_utils:assert_file_content(
                    NonImportingProviderCtx, SpaceEntryPath, Content
                );
            ?DIRECTORY_TYPE ->
                ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(
                    NonImportingProviderNode, NonImportingProviderSessionId, {path, SpaceEntryPath}
                ))
        end
    end, flatten_objects(RemoteEntries)).


%% @private
%% Monitoring expectation shared by the should_not_delete_* tests: the second
%% scan imported exactly the trigger file (root verdict as per
%% root_scan_verdict/1), and the remotely-created entries, having no storage
%% counterparts, left no trace in the counters - in particular nothing was
%% deleted.
-spec assert_monitoring_state_after_trigger_file_import(
    storage_import_test_utils:case_ctx(), posix | s3
) ->
    ok.
assert_monitoring_state_after_trigger_file_import(TestCaseCtx, StorageType) ->
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        created_hist => 1,
        modified_hist => RootModified
    }).


%% @private
%% Monitoring expectation for the most common modifications-group outcome:
%% scan 2 detected exactly one modified (already-imported) file, nothing was
%% created or deleted, and the space root stays unmodified (the default).
%% The created*Hist histograms keep their defaults, i.e. scan-1's creation of
%% the single declared file is expected to still be within the Min window -
%% fine for these single-file tests, which reach this assertion within seconds
%% of the initial scan.
-spec assert_monitoring_state_after_single_file_modification(
    storage_import_test_utils:case_ctx()
) ->
    ok.
assert_monitoring_state_after_single_file_modification(TestCaseCtx) ->
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        modified_hist => 1
    }).


%%%===================================================================
%%% Internal functions - test case specific mocks
%%%===================================================================


%% @private
%% @doc
%% Forces the deletion-marker method of handling deletion of still-opened
%% files: the file's storage file survives under its plain name (guarded by a
%% deletion marker) until the last handle is released. Without the mock, on
%% storages whose helper supports rename (e.g. POSIX) the storage file would
%% instead be renamed away into a special directory, freeing its plain name -
%% while the recreation scenarios specifically exercise the occupied-name
%% (suffixed) layout. Torn down via unmock_fslogic_delete/1.
%% @end
-spec mock_opened_file_deletion_to_use_deletion_marker(storage_import_test_utils:suite_ctx()) ->
    ok.
mock_opened_file_deletion_to_use_deletion_marker(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, fslogic_delete),
    ok = test_utils:mock_expect(Nodes, fslogic_delete, get_open_file_handling_method, fun(FileCtx) ->
        {?SET_DELETION_MARKER, FileCtx}
    end).


%% @private
-spec unmock_fslogic_delete(storage_import_test_utils:suite_ctx()) -> ok.
unmock_fslogic_delete(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_unload(Nodes, fslogic_delete).


%% @private
%% Mocks the given storage helper operation to fail with the given POSIX error
%% (all other operations pass through). Torn down via unmock_storage_helper/1.
-spec mock_storage_helper_error(storage_import_test_utils:suite_ctx(), rmdir | unlink, atom()) ->
    ok.
mock_storage_helper_error(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}, Operation, Error) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, helpers),
    MockFun = case Operation of
        rmdir -> fun(_HelperHandle, _FileId) -> {error, Error} end;
        unlink -> fun(_HelperHandle, _FileId, _CurrentSize) -> {error, Error} end
    end,
    ok = test_utils:mock_expect(Nodes, helpers, Operation, MockFun).


%% @private
-spec unmock_storage_helper(storage_import_test_utils:suite_ctx()) -> ok.
unmock_storage_helper(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_unload(Nodes, helpers).


%% @private
%% Mocks (with a passthrough) rtransfer so that opening a file for an incoming
%% transfer takes at least the given delay - keeps the partially-replicated
%% storage file around long enough for several scan cycles to see it. Torn
%% down via unmock_rtransfer/1.
-spec mock_rtransfer_open_delay(storage_import_test_utils:suite_ctx(), non_neg_integer()) ->
    ok.
mock_rtransfer_open_delay(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}, DelaySeconds) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, rtransfer_config),
    ok = test_utils:mock_expect(Nodes, rtransfer_config, open, fun(Guid, Flag) ->
        Result = meck:passthrough([Guid, Flag]),
        timer:sleep(timer:seconds(DelaySeconds)),
        Result
    end).


%% @private
-spec unmock_rtransfer(storage_import_test_utils:suite_ctx()) -> ok.
unmock_rtransfer(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_unload(Nodes, rtransfer_config).


%% @private
%% Mocks (with a passthrough) the import engine so that every file import
%% signals the given process - lets a test react (e.g. abort the scan) the
%% moment importing actually starts. Torn down via
%% storage_import_test_utils:unmock_storage_import_engine/1.
-spec mock_import_file_started_notification(
    storage_import_test_utils:suite_ctx(), pid()
) -> ok.
mock_import_file_started_notification(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}, NotifyPid) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, storage_import_engine, [passthrough]),
    ok = test_utils:mock_expect(Nodes, storage_import_engine, import_file_unsafe,
        fun(StorageFileCtx, Info) ->
            NotifyPid ! import_file_started,
            meck:passthrough([StorageFileCtx, Info])
        end
    ).


%% @private
%% Mocks storage_import_hash with a passthrough, so that its calls are recorded
%% (via meck history) without altering its behaviour - used to assert whether a
%% storage mutation was (or was not) detected via the scan's hash-based
%% children-attrs check. Torn down via unmock_storage_import_hash/1.
-spec mock_storage_import_hash(storage_import_test_utils:suite_ctx()) -> ok.
mock_storage_import_hash(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, storage_import_hash, [passthrough]).


%% @private
-spec unmock_storage_import_hash(storage_import_test_utils:suite_ctx()) -> ok.
unmock_storage_import_hash(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_unload(Nodes, storage_import_hash).


%% @private
%% Asserts that storage_import_hash reported (at least once) that the children
%% attrs hash of the directory at DirStorageFileId changed. Requires the module
%% to have been mocked (with a passthrough) via mock_storage_import_hash/1.
-spec assert_children_hash_changed(oct_background:entity_selector(), od_space:id(), helpers:file_id()) ->
    ok.
assert_children_hash_changed(ProviderSelector, SpaceId, DirStorageFileId) ->
    ?assert(count_children_hash_changed_results(ProviderSelector, SpaceId, DirStorageFileId) >= 1).


%% @private
%% Asserts that storage_import_hash never reported that the children attrs hash
%% of the directory at DirStorageFileId changed. Requires the module to have been
%% mocked (with a passthrough) via mock_storage_import_hash/1.
-spec assert_children_hash_unchanged(oct_background:entity_selector(), od_space:id(), helpers:file_id()) ->
    ok.
assert_children_hash_unchanged(ProviderSelector, SpaceId, DirStorageFileId) ->
    ?assertEqual(0, count_children_hash_changed_results(ProviderSelector, SpaceId, DirStorageFileId)).


%% @private
-spec count_children_hash_changed_results(oct_background:entity_selector(), od_space:id(), helpers:file_id()) ->
    non_neg_integer().
count_children_hash_changed_results(ProviderSelector, SpaceId, DirStorageFileId) ->
    Id = ?rpc(ProviderSelector, storage_sync_info:id(DirStorageFileId, SpaceId)),
    History = ?rpc(ProviderSelector, meck:history(storage_import_hash)),
    lists:foldl(fun
        ({_, {storage_import_hash, children_attrs_hash_has_changed, Args}, true}, Acc) ->
            case lists:nth(4, Args) of
                #document{key = Id} -> Acc + 1;
                _ -> Acc
            end;
        (_, Acc) ->
            Acc
    end, 0, History).


%% @private
%% Mocks storage_sync_traverse with a passthrough, so that its calls are recorded
%% (via meck history) without altering its behaviour - used to assert whether a
%% directory's own mtime-based change detection did (or did not) report a change
%% (complementing assert_children_hash_changed/3 in pinpointing which mechanism
%% picked a mutation up). Torn down via unmock_storage_sync_traverse/1.
-spec mock_storage_sync_traverse(storage_import_test_utils:suite_ctx()) -> ok.
mock_storage_sync_traverse(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, storage_sync_traverse, [passthrough]).


%% @private
-spec unmock_storage_sync_traverse(storage_import_test_utils:suite_ctx()) -> ok.
unmock_storage_sync_traverse(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_unload(Nodes, storage_sync_traverse).


%% @private
%% Asserts that storage_sync_traverse never reported that the mtime of the
%% directory at DirStorageFileId changed. Requires the module to have been
%% mocked (with a passthrough) via mock_storage_sync_traverse/1.
-spec assert_children_mtime_unchanged(oct_background:entity_selector(), od_space:id(), helpers:file_id()) ->
    ok.
assert_children_mtime_unchanged(ProviderSelector, SpaceId, DirStorageFileId) ->
    ?assertEqual(0, count_children_mtime_changed_results(ProviderSelector, SpaceId, DirStorageFileId)).


%% @private
%% Asserts that storage_sync_traverse reported (at least once) that the mtime of
%% the directory at DirStorageFileId changed (adding/removing a direct child
%% bumps it). Requires the module to have been mocked (with a passthrough) via
%% mock_storage_sync_traverse/1.
-spec assert_children_mtime_changed(oct_background:entity_selector(), od_space:id(), helpers:file_id()) ->
    ok.
assert_children_mtime_changed(ProviderSelector, SpaceId, DirStorageFileId) ->
    ?assert(count_children_mtime_changed_results(ProviderSelector, SpaceId, DirStorageFileId) >= 1).


%% @private
-spec count_children_mtime_changed_results(oct_background:entity_selector(), od_space:id(), helpers:file_id()) ->
    non_neg_integer().
count_children_mtime_changed_results(ProviderSelector, SpaceId, DirStorageFileId) ->
    Id = ?rpc(ProviderSelector, storage_sync_info:id(DirStorageFileId, SpaceId)),
    History = ?rpc(ProviderSelector, meck:history(storage_sync_traverse)),
    lists:foldl(fun
        ({_, {storage_sync_traverse, has_mtime_changed, [Doc | _]}, true}, Acc) ->
            case Doc of
                #document{key = Id} -> Acc + 1;
                _ -> Acc
            end;
        (_, Acc) ->
            Acc
    end, 0, History).
