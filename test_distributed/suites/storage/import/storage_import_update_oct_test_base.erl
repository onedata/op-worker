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
%%% idempotency (unchanged entries are not reprocessed), retrying after a failed
%%% scan, conflict/suffix resolution for recreated files, scan configuration
%%% (max_depth, force start/stop, batch handling) and protection flags. The
%%% generic machinery (creating and mutating the tree on the storage, enabling
%%% and awaiting scans, verifying the imported tree, asserting the monitoring
%%% counters) lives in storage_import_test_utils.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_update_oct_test_base).
-author("Bartosz Walkowicz").

-include("storage_import_oct_test.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/acl.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/fuse_messages.hrl").


% API
-export([
    clean_up_after_previous_run/2,
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
    copy_file_update_test/1

    %% --- idempotency ---

    %% --- retry ---

    %% --- suffixes ---

    %% --- config ---

    %% --- protection ---

    %% --- not reimported ---
]).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% API
%%%===================================================================


-spec clean_up_after_previous_run([atom()], storage_import_test_utils:suite_ctx()) -> ok.
clean_up_after_previous_run(AllTestCases, SuiteCtx) ->
    storage_import_test_utils:clean_up_after_previous_run(AllTestCases, SuiteCtx).


init_per_testcase(Case = chmod_file_update_in_batched_dir_test, TestSuiteCtx = #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector
}, Config) ->
    [Node | _] = Nodes = oct_background:get_provider_nodes(ImportingProviderSelector),
    {ok, OldDirBatchSize} = test_utils:get_env(Node, op_worker, storage_import_dir_batch_size),
    %% a low batch size forces the scan of the test dir's children to span multiple batches
    ok = test_utils:set_env(Nodes, op_worker, storage_import_dir_batch_size, 2),
    Config2 = [{old_storage_import_dir_batch_size, OldDirBatchSize} | Config],
    init_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config2);

init_per_testcase(_Case, _TestSuiteCtx, Config) ->
    lfm_proxy:init(Config).


end_per_testcase(Case = chmod_file_update_test, TestSuiteCtx, Config) ->
    unmock_storage_import_hash(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case = chmod_file_update_in_batched_dir_test, TestSuiteCtx = #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector
}, Config) ->
    unmock_storage_import_hash(TestSuiteCtx),
    unmock_storage_sync_traverse(TestSuiteCtx),
    Nodes = oct_background:get_provider_nodes(ImportingProviderSelector),
    OldDirBatchSize = ?config(old_storage_import_dir_batch_size, Config),
    ok = test_utils:set_env(Nodes, op_worker, storage_import_dir_batch_size, OldDirBatchSize),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(_Case, _TestSuiteCtx, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Tests
%%%===================================================================


%% --- modifications ---


%% A file imported by the initial scan is appended to on the storage; the next
%% (continuous) scan detects the change and the appended bytes become readable
%% through the logical filesystem on both providers.
append_file_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(),
    AppendedContent = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = InitialContent}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% the file was imported by the initial scan with its initial content
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:verify_dir_stats(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }),

    %% append to the file on the storage and let the next continuous scan detect it
    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId,
        byte_size(InitialContent), AppendedContent
    ),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the appended content is now readable on both providers
    UpdatedFileSpec = #file_spec{
        name = FileName, content = <<InitialContent/binary, AppendedContent/binary>>
    },
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedFileSpec),
    storage_import_test_utils:verify_dir_stats(TestCaseCtx, UpdatedFileSpec),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"unmodified">> => 1
    }).


%% Like append_file_update_test, but the file's mtime is forced back to its
%% pre-append value right after the storage-level write (simulating a storage
%% backend that does not reliably bump mtime on writes at the resolution the
%% scan relies on) - the appended bytes must still be detected and imported,
%% since size/mtime are an OR in the modification check (see the OR condition
%% for regular files documented in storage_import_engine:maybe_update_file_location/4).
append_file_not_changing_mtime_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(),
    AppendedContent = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = InitialContent}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% the file was imported by the initial scan with its initial content
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }),

    %% append to the file, then force its mtime back to the pre-append value
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
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the appended content is now readable on both providers, despite the
    %% unchanged mtime
    UpdatedFileSpec = #file_spec{
        name = FileName, content = <<InitialContent/binary, AppendedContent/binary>>
    },
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedFileSpec),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"unmodified">> => 1
    }).


%% Like append_file_update_test, but the file is empty (0 bytes) at the time of
%% the initial scan, and only gains content on the continuous scan.
append_empty_file_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    AppendedContent = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = <<>>}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% the empty file was imported by the initial scan
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }),

    %% write to the (previously empty) file on the storage and let the next
    %% continuous scan detect it
    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, 0, AppendedContent
    ),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the appended content is now readable on both providers
    UpdatedFileSpec = #file_spec{name = FileName, content = AppendedContent},
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedFileSpec),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"unmodified">> => 1
    }).


%% A file imported by the initial scan is truncated on the storage; the next
%% (continuous) scan detects the change and the truncated content is reflected
%% through the logical filesystem on both providers.
truncate_file_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(),
    TruncatedSize = 1,
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = InitialContent}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% the file was imported by the initial scan with its initial content
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }),

    %% truncate the file on the storage and let the next continuous scan detect it
    storage_file_setup_utils:truncate(
        ImportingProviderSelector, ImportedStorageId, StorageFileId,
        TruncatedSize, byte_size(InitialContent)
    ),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the truncated content is now readable on both providers
    TruncatedFileSpec = #file_spec{
        name = FileName, content = binary:part(InitialContent, 0, TruncatedSize)
    },
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, TruncatedFileSpec),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"unmodified">> => 1
    }).


%% A file imported by the initial scan has its mode changed on the storage; the
%% next (continuous) scan detects the change and the new mode is reflected through
%% the logical filesystem on both providers. POSIX-only - object storages (S3) have
%% no notion of a per-object POSIX mode.
%% Also asserts (via a passthrough mock, torn down in end_per_testcase) that the
%% mutation was actually detected via storage_import_hash - i.e. that the scan's
%% hash-based change-detection optimization did not just skip the space root
%% directory that holds the file.
chmod_file_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    mock_storage_import_hash(ImportingProviderSelector),

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
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% the file was imported by the initial scan
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }),

    %% change the file's mode on the storage and let the next continuous scan detect it
    storage_file_setup_utils:chmod(ImportingProviderSelector, ImportedStorageId, StorageFileId, NewMode),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the new mode is now visible on both providers
    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceTestFilePath, #{mode => NewMode}),
    storage_import_test_utils:assert_attrs(NonImportingProviderCtx, SpaceTestFilePath, #{mode => NewMode}),
    assert_children_hash_changed(ImportingProviderSelector, SpaceId, RootStorageFileId),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"unmodified">> => 1
    }).


%% Like chmod_file_update_test, but the changed file sits inside a subdirectory
%% whose 3 children are scanned in more than one batch (storage_import_dir_batch_size
%% is temporarily lowered to 2) - exercising the hash-based children-attrs change
%% detection (see chmod_file_update_test's doc above) one level down from the space
%% root, across a directory scanned in multiple batches. Also asserts (via a
%% passthrough mock on storage_sync_traverse, also torn down in end_per_testcase)
%% that the modification is picked up in spite of, not because of, the parent
%% directory's own mtime - a child's chmod alone does not bump it. POSIX-only,
%% matching the old suite.
chmod_file_update_in_batched_dir_test(SuiteCtx) ->
    %% storage_import_dir_batch_size is temporarily lowered to 2 in init_per_testcase
    %% (and restored in end_per_testcase), so that the scan of TestDirName's 3
    %% children spans multiple batches
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    mock_storage_import_hash(ImportingProviderSelector),
    mock_storage_sync_traverse(ImportingProviderSelector),

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
    } = storage_import_test_utils:init_testcase(?FUNCTION_NAME, [
        #dir_spec{name = TestDirName, children = [
            #file_spec{name = File1Name, content = ?RAND_STR()},
            #file_spec{name = ?RAND_STR(), content = ?RAND_STR()},
            #file_spec{name = ?RAND_STR(), content = ?RAND_STR()}
        ]},
        #dir_spec{name = ?RAND_STR()}
    ], SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% both directories (with, respectively, their 3 files and no children) were imported
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }),

    %% change one of TestDirName's 3 files' mode on the storage and let the next
    %% continuous scan detect it
    storage_file_setup_utils:chmod(ImportingProviderSelector, ImportedStorageId, File1StorageFileId, NewMode),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the new mode is now visible on both providers
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
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        %% space root, TestDirName, TestDir2Name and the 2 untouched files
        <<"unmodified">> => 5
    }).


%% A file imported by the initial scan is renamed (moved) to a different path on
%% the storage; the next (continuous) scan detects the old path as deleted and the
%% new path as newly created, and the file becomes reachable at the new path (and
%% ?ENOENT at the old one) on both providers. POSIX-only, matching the old suite -
%% not exercised on S3 there either.
move_file_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    SrcFileName = ?RAND_STR(),
    DstFileName = ?RAND_STR(),
    Content = ?RAND_STR(),
    SrcStorageFileId = filepath_utils:join([<<"/">>, SrcFileName]),
    DstStorageFileId = filepath_utils:join([<<"/">>, DstFileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = SrcFileName, content = Content}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% the file was imported by the initial scan at its original path
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }),

    %% rename the file on the storage and let the next continuous scan detect it
    storage_file_setup_utils:rename(
        ImportingProviderSelector, ImportedStorageId, SrcStorageFileId, DstStorageFileId
    ),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the file is now reachable (with its original content) at the new path - and,
    %% implicitly, gone from the old one - on both providers, since verify_imported_tree
    %% asserts the exact set of the space root's children
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, #file_spec{
        name = DstFileName, content = Content
    }),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"deleted">> => 1,
        %% unlike append/truncate/chmod (which only touch an existing child's own
        %% attrs), adding/removing a directory entry changes the space root's own
        %% mtime - so, unlike those tests, the root itself is reported as modified
        %% (not unmodified) on this scan
        <<"modified">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"unmodified">> => 0,
        %% the scan-1 creation of the (now renamed-away) source file may have already
        %% aged out of the short createdMinHist window by the time scan 2 is awaited
        %% and the tree/monitoring re-verified; the longer hour/day windows still hold it
        <<"createdMinHist">> => {range, 1, 2},
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1
    }).


%% A file imported by the initial scan is copied to a different path on the
%% storage; the next (continuous) scan detects the new path as created (the
%% original is left untouched, hence unmodified), and both are reachable, with
%% identical content, on both providers. POSIX-only, matching the old suite - not
%% exercised on S3 there either.
copy_file_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    SrcFileName = ?RAND_STR(),
    DstFileName = ?RAND_STR(),
    Content = ?RAND_STR(),
    DstStorageFileId = filepath_utils:join([<<"/">>, DstFileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = SrcFileName, content = Content}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% the file was imported by the initial scan at its original path
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }),

    %% create the copy directly on the storage (content is already known to the
    %% test, so there is no need to go through a real host-level file copy)
    storage_file_setup_utils:create_file(ImportingProviderSelector, ImportedStorageId, DstStorageFileId, Content),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% both the original and the copy are reachable, with identical content, on both providers
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, [
        #file_spec{name = SrcFileName, content = Content},
        #file_spec{name = DstFileName, content = Content}
    ]),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        %% the space root gained a new child, bumping its own mtime - see move_file_update_test
        <<"modified">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        %% the original (untouched) file is counted unmodified this scan
        <<"unmodified">> => 1,
        %% the scan-1 creation of the original file may have already aged out of the
        %% short createdMinHist window by the time scan 2 is awaited and the
        %% tree/monitoring re-verified; the longer hour/day windows still hold it
        <<"createdMinHist">> => {range, 1, 2},
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2
    }).


%% --- idempotency ---


%% --- retry ---


%% --- suffixes ---


%% --- config ---


%% --- protection ---


%% --- not reimported ---


%%%===================================================================
%%% Internal functions - test case specific mocks
%%%===================================================================


%% @private
%% Mocks storage_import_hash with a passthrough, so that its calls are recorded
%% (via meck history) without altering its behaviour - used to assert that a
%% storage mutation was actually detected via the scan's hash-based
%% change-detection optimization, rather than the directory being skipped.
%% Torn down via unmock_storage_import_hash/1.
-spec mock_storage_import_hash(oct_background:entity_selector()) -> ok.
mock_storage_import_hash(ProviderSelector) ->
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
    Id = ?rpc(ProviderSelector, storage_sync_info:id(DirStorageFileId, SpaceId)),
    History = ?rpc(ProviderSelector, meck:history(storage_import_hash)),
    Matches = lists:foldl(fun
        ({_, {storage_import_hash, children_attrs_hash_has_changed, Args}, true}, Acc) ->
            case lists:nth(4, Args) of
                #document{key = Id} -> Acc + 1;
                _ -> Acc
            end;
        (_, Acc) ->
            Acc
    end, 0, History),
    ?assert(Matches >= 1).


%% @private
%% Mocks storage_sync_traverse with a passthrough, so that its calls are recorded
%% (via meck history) without altering its behaviour - used to assert that a
%% directory's own mtime-based change-detection shortcut did NOT report a change,
%% i.e. that a detected modification came from the hash-based children-attrs check
%% instead (see assert_children_hash_changed/3). Torn down via
%% unmock_storage_sync_traverse/1.
-spec mock_storage_sync_traverse(oct_background:entity_selector()) -> ok.
mock_storage_sync_traverse(ProviderSelector) ->
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
%% Asserts that storage_sync_traverse never reported that the directory at
%% DirStorageFileId's own mtime had changed - i.e. that a modification of one of
%% its children was picked up despite the directory's own mtime-based shortcut
%% saying "unchanged" (a child's chmod alone does not bump its parent's mtime).
%% Requires the module to have been mocked (with a passthrough) via
%% mock_storage_sync_traverse/1.
-spec assert_children_mtime_unchanged(oct_background:entity_selector(), od_space:id(), helpers:file_id()) ->
    ok.
assert_children_mtime_unchanged(ProviderSelector, SpaceId, DirStorageFileId) ->
    Id = ?rpc(ProviderSelector, storage_sync_info:id(DirStorageFileId, SpaceId)),
    History = ?rpc(ProviderSelector, meck:history(storage_sync_traverse)),
    Matches = lists:foldl(fun
        ({_, {storage_sync_traverse, has_mtime_changed, [Doc | _]}, true}, Acc) ->
            case Doc of
                #document{key = Id} -> Acc + 1;
                _ -> Acc
            end;
        (_, Acc) ->
            Acc
    end, 0, History),
    ?assertEqual(0, Matches).
