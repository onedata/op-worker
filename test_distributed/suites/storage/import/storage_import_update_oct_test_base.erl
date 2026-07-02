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
%%%
%%% Object storages (e.g. S3, see flat_storage_iterator.erl) diverge from POSIX
%%% in ways that recur across many tests below, so they are documented once
%%% here rather than repeated at every call site:
%%%  * they have no real directories - an empty one has no underlying storage
%%%    object at all, so it can never be created/observed via LFM (handled
%%%    transparently by storage_import_test_utils:verify_imported_tree/2, which
%%%    strips such directories from the expected tree on S3) or counted towards
%%%    "created" (see storage_import_test_utils:count_imported_nodes/2);
%%%  * the space root's own storage statbuf is permanently mocked into the past
%%%    for scan-1 timing determinism (mock_space_dir_statbuf_on_flat_storage/1),
%%%    so it can never be classified "modified" on any scan;
%%%  * there is no real per-directory traversal at all - the whole space is a
%%%    single traversal entity (flat_storage_iterator:should_generate_master_job/1
%%%    always returns false), with only regular files counted as its children
%%%    for batching, and the underlying listing API reports a definitive
%%%    end-of-listing marker on the very page that exhausts it (unlike POSIX's
%%%    tree_storage_iterator, whose ambiguous "read returned exactly
%%%    batch_size" heuristic needs one extra confirmatory batch to disambiguate).
%%% Tests below note only what is specific to their own scenario; assume these
%%% general facts hold on S3 unless stated otherwise.
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
    copy_file_update_test/1,
    change_file_content_constant_size_test/1,
    change_file_content_update_test/1,
    change_file_content_the_same_moment_when_sync_performs_stat_on_file_test/1,
    replace_file_with_dir_test/1,
    replace_empty_dir_with_file_test/1,
    replace_non_empty_dir_with_file_test/1,
    update_timestamps_file_import_test/1,
    create_file_in_dir_update_test/1,
    create_file_in_dir_exceed_batch_update_test/1

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
        %% space root (1) + 1 extra confirmatory-batch pass each for root and
        %% TestDirName - with dir_batch_size=2, a directory whose last real
        %% listing batch returns EXACTLY dir_batch_size children (root: 2
        %% children; TestDirName: first batch of 2, out of 3) is ambiguous
        %% ("maybe more?"), so one more (here: empty/undersized) batch is
        %% scheduled to confirm end-of-listing - and each such batch re-runs
        %% the directory's OWN modified/unmodified check from scratch (see
        %% tree_storage_iterator:get_children_and_next_batch_job/1: a next
        %% batch job is only omitted when a read returns FEWER than
        %% dir_batch_size children). TestDir2Name (0 children, 0 < 2) needs no
        %% confirmatory batch, so contributes none.
        <<"unmodified">> => 3
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
        %% same batch-continuation artifact as the initial scan above: root and
        %% TestDirName are each re-checked twice (2 confirmatory-batch passes,
        %% both unmodified - the tree's shape under them hasn't changed, only
        %% File1's mode), TestDir2Name once; the 2 untouched files (one
        %% individually re-stat'd alongside File1 in its own hash-changed
        %% batch, one bulk-marked via the other, hash-unchanged batch)
        %% contribute 1 each => 2+2+1+1+1 = 7
        <<"unmodified">> => 7
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


%% A file imported by the initial scan has its content overwritten on the storage
%% WITHOUT changing its size (a fixed-length random string is swapped for another);
%% the next (continuous) scan detects the change and the new content is reflected
%% through the logical filesystem on both providers. Unlike append/truncate/chmod,
%% a same-size content change has no size/mode signal to fall back on - detection
%% relies entirely on the file's mtime having advanced (see the OR condition in
%% storage_import_engine:maybe_update_file_location/4), so - unlike those tests -
%% an explicit delay before the write is required here.
change_file_content_constant_size_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(16),
    ChangedContent = ?RAND_STR(16),
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

    %% overwrite the file's content (same size) on the storage and let the next
    %% continuous scan detect it
    timer:sleep(timer:seconds(2)),
    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, 0, ChangedContent
    ),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the new content is now readable on both providers
    UpdatedFileSpec = #file_spec{name = FileName, content = ChangedContent},
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


%% Like change_file_content_constant_size_test, but the new content is a
%% different length than the original - the modification check's size clause
%% detects it regardless of mtime resolution, so (like append/truncate/chmod, and
%% unlike change_file_content_constant_size_test above) no explicit delay is needed.
change_file_content_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(16),
    ChangedContent = ?RAND_STR(32),
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

    %% overwrite the file's content (different size) on the storage and let the
    %% next continuous scan detect it
    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, 0, ChangedContent
    ),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the new content is now readable on both providers
    UpdatedFileSpec = #file_spec{name = FileName, content = ChangedContent},
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


%% A file's content is overwritten on the storage at (deliberately forced to be)
%% the EXACT same timestamp as storage_import's own last recorded stat of that
%% file - exercising the boundary of the "already handled" fast-path shortcut in
%% storage_import_engine:maybe_update_file_location/4, which requires the stored
%% last_stat to be STRICTLY greater than the storage mtime to skip re-checking a
%% file (if it used >= instead of >, this test would incorrectly skip the real
%% content change constructed here). Same-size content change (like
%% change_file_content_constant_size_test above), so the fast-path is the only
%% thing standing between this test and a false "unmodified". POSIX-only -
%% forcing the storage mtime relies on storage_file_setup_utils:set_mtime/4.
change_file_content_the_same_moment_when_sync_performs_stat_on_file_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(),
    ChangedContent = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_id = SpaceId
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = InitialContent}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% the file was imported by the initial scan with its initial content
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }),

    %% force storage_sync_info's own last_stat bookkeeping (which may have moved
    %% on since the initial scan) back to its scan-1 value, overwrite the file's
    %% content, then force the storage file's own mtime to that SAME value - so
    %% that last_stat and the storage mtime end up EQUAL, not last_stat > mtime
    LastStatTime = ?rpc(ImportingProviderSelector, begin
        {ok, #document{value = #storage_sync_info{last_stat = LastStat}}} =
            storage_sync_info:get(StorageFileId, SpaceId),
        LastStat
    end),
    ok = ?rpc(ImportingProviderSelector, storage_sync_info:create_or_update(
        StorageFileId, SpaceId, fun(SSI) -> {ok, SSI#storage_sync_info{last_stat = LastStatTime}} end
    )),
    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, 0, ChangedContent
    ),
    storage_file_setup_utils:set_mtime(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, LastStatTime
    ),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the change is still detected, despite last_stat and the (forced) new mtime
    %% being EQUAL - the fast-path shortcut requires last_stat to be STRICTLY
    %% greater than mtime to skip a file, so equality must not skip it
    UpdatedFileSpec = #file_spec{name = FileName, content = ChangedContent},
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


%% A file imported by the initial scan is deleted on the storage and replaced,
%% under the SAME name, by a directory holding one child file; the next
%% (continuous) scan detects the type change - deleting the old file entry and
%% importing the new directory (and its child) in its place. Also verifies the
%% newly-imported directory is fully functional (not just a passively-imported
%% node) by creating a new subdirectory inside it via LFM.
%% On POSIX, replacing a direct child of the space root (even under the same
%% name) bumps the root's own mtime, so root is classified modified this scan
%% (see the "timing mechanism" note on move_file_update_test). On S3, root can
%% never be classified modified at all (see module doc) - found via a real
%% onenv S3 run failing on the hardcoded posix-only expectation this test
%% originally had.
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
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = InitialContent}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% the file was imported by the initial scan
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }),

    %% delete the file and create a directory (holding one child file) at the
    %% same path on the storage, then let the next continuous scan detect it
    storage_file_setup_utils:delete_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, byte_size(InitialContent)
    ),
    storage_file_setup_utils:create_dir(ImportingProviderSelector, ImportedStorageId, StorageFileId),
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId, ChildStorageFileId, ChildContent
    ),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the path now resolves to a directory with the new child file, on both providers
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, #dir_spec{name = FileName, children = [
        #file_spec{name = ChildFileName, content = ChildContent}
    ]}),
    %% S3 has no real directories, so the new directory itself is not counted
    %% towards "created" there - only its child file is (see count_imported_nodes/2)
    NewlyCreated = case StorageType of posix -> 2; s3 -> 1 end,
    %% root is the only entity that is modified/unmodified this scan (the file
    %% delete/dir+child create are all created/deleted); on POSIX it's modified
    %% (a direct child was replaced), on S3 it's unmodified (see doc comment above)
    {RootModified, RootUnmodified} = case StorageType of posix -> {1, 0}; s3 -> {0, 1} end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => NewlyCreated,
        <<"modified">> => RootModified,
        <<"deleted">> => 1,
        <<"unmodified">> => RootUnmodified,
        %% the scan-1 creation of the (now deleted) original file may have already
        %% aged out of the short createdMinHist window by the time scan 2 is
        %% awaited and the tree/monitoring re-verified; the longer hour/day
        %% windows still hold it
        <<"createdMinHist">> => {range, NewlyCreated, NewlyCreated + 1},
        <<"createdHourHist">> => NewlyCreated + 1,
        <<"createdDayHist">> => NewlyCreated + 1,
        <<"modifiedMinHist">> => RootModified,
        <<"modifiedHourHist">> => RootModified,
        <<"modifiedDayHist">> => RootModified,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1
    }),

    %% the newly-imported directory is fully functional, not just a passively
    %% imported node - a new subdirectory can be created inside it via LFM (on
    %% the non-importing provider) and is visible on the importing provider too
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


%% An empty directory imported by the initial scan is deleted on the storage and
%% replaced, under the SAME name, by a regular file; the next (continuous) scan
%% detects the type change - deleting the old (empty) directory entry and
%% importing the new file in its place. POSIX-only - object storages (S3) cannot
%% hold empty directories.
replace_empty_dir_with_file_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    DirName = ?RAND_STR(),
    NewContent = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, DirName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #dir_spec{name = DirName}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% the empty directory was imported by the initial scan
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }),

    %% delete the (empty) directory and create a regular file at the same path on
    %% the storage, then let the next continuous scan detect it
    storage_file_setup_utils:rmdir(ImportingProviderSelector, ImportedStorageId, StorageFileId),
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, NewContent
    ),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the path now resolves to a regular file with the new content, on both providers
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, #file_spec{name = DirName, content = NewContent}),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => 1,
        <<"deleted">> => 1,
        <<"unmodified">> => 0,
        %% the scan-1 creation of the (now deleted) empty directory may have
        %% already aged out of the short createdMinHist window by the time scan 2
        %% is awaited and the tree/monitoring re-verified; the longer hour/day
        %% windows still hold it
        <<"createdMinHist">> => {range, 1, 2},
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"deletedMinHist">> => 1,
        <<"deletedHourHist">> => 1,
        <<"deletedDayHist">> => 1
    }).


%% A non-empty directory (holding one child file) imported by the initial scan
%% has its child, then itself, deleted on the storage and replaced, under the
%% SAME name, by a regular file; the next (continuous) scan detects the type
%% change - deleting both the old directory and its child, and importing the new
%% file in its place.
%% Root's own modified/unmodified classification this scan differs by storage
%% type for the same reason as replace_file_with_dir_test - it can never be
%% classified modified on S3 (see module doc).
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
    } = storage_import_test_utils:init_testcase(?FUNCTION_NAME, #dir_spec{
        name = DirName, children = [#file_spec{name = ChildFileName, content = ChildContent}]
    }, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% the directory (with its child file) was imported by the initial scan
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }),

    %% delete the child file, then the (now empty) directory, and create a
    %% regular file at the same path on the storage; let the next continuous
    %% scan detect it
    storage_file_setup_utils:delete_file(
        ImportingProviderSelector, ImportedStorageId, ChildStorageFileId, byte_size(ChildContent)
    ),
    storage_file_setup_utils:rmdir(ImportingProviderSelector, ImportedStorageId, StorageFileId),
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, NewContent
    ),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the path now resolves to a regular file with the new content, on both providers
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, #file_spec{name = DirName, content = NewContent}),
    %% the scan-1 created count (used below for the created hour/day hists) is
    %% dir+file on posix, file-only on S3 (see count_imported_nodes/2)
    Scan1Created = case StorageType of posix -> 2; s3 -> 1 end,
    %% root is the only entity modified/unmodified this scan - see
    %% replace_file_with_dir_test's doc comment for why this differs by storage type
    {RootModified, RootUnmodified} = case StorageType of posix -> {1, 0}; s3 -> {0, 1} end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => RootModified,
        %% both the directory and its child file are deleted this scan - on S3
        %% too, even though the directory itself was never counted towards
        %% "created" there; deletion detection counts it regardless
        <<"deleted">> => 2,
        <<"unmodified">> => RootUnmodified,
        %% the scan-1 creation(s) may have already aged out of the short
        %% createdMinHist window by the time scan 2 is awaited and the
        %% tree/monitoring re-verified; the longer hour/day windows still hold them
        <<"createdMinHist">> => {range, 1, 1 + Scan1Created},
        <<"createdHourHist">> => 1 + Scan1Created,
        <<"createdDayHist">> => 1 + Scan1Created,
        <<"modifiedMinHist">> => RootModified,
        <<"modifiedHourHist">> => RootModified,
        <<"modifiedDayHist">> => RootModified,
        <<"deletedMinHist">> => 2,
        <<"deletedHourHist">> => 2,
        <<"deletedDayHist">> => 2
    }).


%% A file imported by the initial scan has BOTH its atime and mtime forced, on
%% the storage, to an identical far-future timestamp (content/size unchanged);
%% the next (continuous) scan detects the modification via the mtime clause of
%% storage_import_engine:maybe_update_file_location/4's OR condition and
%% propagates BOTH new timestamps onto the file's own attrs, on both providers -
%% proving atime is synced too, not just mtime. The forced timestamp is so far in
%% the future (relative to the real one it replaces) that no explicit delay is
%% needed for the change to be detected, unlike change_file_content_constant_size_test.
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
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = Content}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% the file was imported by the initial scan
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }),

    %% force both the file's atime and mtime on the storage to the same far-future
    %% timestamp, then let the next continuous scan detect and propagate it
    storage_file_setup_utils:set_atime_and_mtime(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, NewTimestamp, NewTimestamp
    ),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the new timestamps are now reflected in the file's own attrs, on both providers
    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    ExpectedTimes = #{atime => NewTimestamp, mtime => NewTimestamp},
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceTestFilePath, ExpectedTimes),
    storage_import_test_utils:assert_attrs(NonImportingProviderCtx, SpaceTestFilePath, ExpectedTimes),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"modified">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"unmodified">> => 1
    }).


%% A new file is created (on the storage) inside one of two sibling directories
%% imported (both empty) by the initial scan; the next (continuous) scan detects
%% it, on both POSIX and S3. The new file is nested one level under the space
%% root (not a direct child of it), so - per the "timing mechanism" established
%% for move_file_update_test - the root's own mtime is untouched by this scan and
%% it stays classified unmodified; only the TOUCHED directory (which gained a
%% direct child) flips to modified, while the UNTOUCHED sibling stays unmodified
%% too. On POSIX only (matching the old envup test's actual coverage - the old S3
%% test never had this extra check), also verifies via passthrough mocks on
%% storage_import_hash/storage_sync_traverse (same mechanism as
%% chmod_file_update_test/chmod_file_update_in_batched_dir_test) that this is
%% backed by the expected mechanism: the TOUCHED directory's own mtime (not the
%% space root's hash-based children-attrs check, nor the UNTOUCHED sibling's mtime).
%%
%% On S3 (see module doc for the general facts this relies on), root can never
%% register as modified either way, and nothing else picks up a "modified" tally
%% in its place (confirmed via a real onenv run of replace_file_with_dir_test) -
%% so this test's S3 branch expects modified=>0, unmodified=>1 (root only).
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
    } = storage_import_test_utils:init_testcase(?FUNCTION_NAME, [
        #dir_spec{name = TouchedDirName},
        #dir_spec{name = UntouchedDirName}
    ], SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    %% both (empty) directories were imported by the initial scan - on S3 they
    %% are automatically excluded from this check (see verify_imported_tree/2
    %% and the module doc)
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => 1
    }),

    %% POSIX-only, matching the old envup test's own coverage (no S3 equivalent there either)
    ?IF_POSIX(StorageType, begin
        mock_storage_import_hash(ImportingProviderSelector),
        mock_storage_sync_traverse(ImportingProviderSelector)
    end),

    %% create a new file inside the first directory (the second stays untouched)
    %% and let the next continuous scan detect it
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId, FileStorageFileId, Content
    ),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the new file is now visible, with its content, on both providers - the
    %% still-empty untouched sibling directory is automatically excluded from
    %% this check on S3 (see verify_imported_tree/2 and the module doc)
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
    %% see doc comment: on POSIX the touched dir is modified, root + untouched
    %% dir are unmodified; on S3 nothing is modified (root can't be, the
    %% untouched dir doesn't exist, and the touched dir's own file_meta doesn't
    %% seem to pick up a separate tally either - only its new file does, via
    %% "created") and root alone is unmodified
    {ModifiedCount, UnmodifiedCount} = case StorageType of posix -> {1, 2}; s3 -> {0, 1} end,
    %% the scan-1 creation of the (untouched) two directories on POSIX may have
    %% already aged out of the short createdMinHist window by the time scan 2 is
    %% awaited and the tree/monitoring re-verified; the longer hour/day windows
    %% still hold them. On S3, directories don't count towards "created" at all
    %% (see count_imported_nodes/2), so scan 1 contributed 0
    Scan1Created = case StorageType of posix -> 2; s3 -> 0 end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => ModifiedCount,
        <<"deleted">> => 0,
        <<"unmodified">> => UnmodifiedCount,
        <<"createdMinHist">> => {range, 1, 1 + Scan1Created},
        <<"createdHourHist">> => 1 + Scan1Created,
        <<"createdDayHist">> => 1 + Scan1Created,
        <<"modifiedMinHist">> => ModifiedCount,
        <<"modifiedHourHist">> => ModifiedCount,
        <<"modifiedDayHist">> => ModifiedCount,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
    }).


%% Like create_file_in_dir_update_test, but the space root itself has more
%% direct children (2 dirs + 4 files = 6) than storage_import_dir_batch_size
%% (temporarily lowered to 2, like chmod_file_update_in_batched_dir_test) - so
%% it's the ROOT's own scan-1 baseline classification that spans multiple
%% batches this time, not a subdirectory's one level down. On POSIX, the
%% continuous (scan 2) mutation runs with both detect_modifications and
%% detect_deletions disabled, matching the old envup test - see the comment
%% above enable_continuous_scan/2 below for what that changes.
%%
%% S3 divergence: detect_modifications=false is NOT used for S3's scan 2 (a
%% real onenv run confirmed this the hard way - the new nested file was simply
%% never created). Root's MTimeHasChanged is always false on S3 (module doc),
%% so detect_modifications=false always takes do_update_master_job/2's
%% {false, _, false} shortcut into traverse_only_directories/2, which discards
%% that batch's slave_jobs outright (bulk-counting them as unmodified WITHOUT
%% running them) and falls back to recursing into subdirectories for anything
%% it skipped - a fallback that does not exist on S3 (no per-directory
%% traversal at all, module doc). On POSIX this shortcut is harmless (a real,
%% still-observable subdirectory catches what the parent's shortcut skipped),
%% which is exactly why old envup's S3 coverage of this flag combination never
%% caught it - without the mock, flat_storage_iterator:get_virtual_directory_ctx/3
%% stamps the (synthetic) root with the CURRENT wall-clock time on every single
%% read, so on real, unmocked storage its mtime differs on every scan and this
%% shortcut is never taken in the first place; mock_space_dir_statbuf_on_flat_storage/1
%% intercepts exactly this call to freeze it, which is what creates the gap here.
%% So S3's scan 2 below uses the default (enabled) detection config instead -
%% still exercising the same batch-exceeding math via the ordinary code path,
%% just not the disabled-detection angle old envup's S3 test also covered.
%%
%% POSIX-confirmed passing 2026-07-02; S3 fix above not yet re-run at time of writing.
create_file_in_dir_exceed_batch_update_test(SuiteCtx) ->
    %% storage_import_dir_batch_size is temporarily lowered to 2 in
    %% init_per_testcase (and restored in end_per_testcase), so that the scan of
    %% the space root's 6 children spans multiple batches
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

    %% both (empty) directories and the 4 plain files were imported by the
    %% initial scan - on S3 the empty directories are automatically excluded
    %% from this check (see verify_imported_tree/2 and the module doc)
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    %% root's own real (offset 0) verdict plus every "confirmatory" batch beyond
    %% it unconditionally counts as an extra unmodified, regardless of whether
    %% the directory being re-visited was itself newly created this scan (see
    %% do_slave_job_on_directory/1's `offset = 0` guard in
    %% storage_sync_traverse.erl). On POSIX, 6 children/batch_size 2 means reads
    %% at offsets 0,2,4,6 (sizes 2,2,2,0) => 4 total batches. On S3 only the 4
    %% real files count as root's children for batching (see module doc) =>
    %% ceil(4/2) = 2 total batches (no POSIX-style extra pass for landing on an
    %% exact multiple - see module doc)
    Scan1Unmodified = case StorageType of posix -> 4; s3 -> 2 end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"unmodified">> => Scan1Unmodified
    }),

    %% POSIX-only, matching the old envup test's own coverage (no S3 equivalent
    %% there either) - passthrough mocks so that the mutation below can be
    %% proven to still be detected under the hood despite detect_modifications
    %% suppressing it from the monitoring counters (see below)
    ?IF_POSIX(StorageType, begin
        mock_storage_import_hash(ImportingProviderSelector),
        mock_storage_sync_traverse(ImportingProviderSelector)
    end),

    %% create a new file inside the first directory (the second stays untouched)
    %% and let the next continuous scan detect it
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId, FileStorageFileId, Content
    ),
    %% on POSIX, both modification and deletion detection are disabled for this
    %% scan, matching the old envup test - storage_import_engine:maybe_update_file/4
    %% then returns ?FILE_UNMODIFIED unconditionally for every already-known
    %% entity's own classification, regardless of what actually changed on
    %% storage. The touched directory's mtime DOES genuinely change (a direct
    %% child was added), but this no longer surfaces as "modified" below - the
    %% mock asserts further down still prove the underlying detection fires
    %% despite the suppression. On S3 the default (enabled) config is used
    %% instead - see the doc comment above for why disabling detection there
    %% would prevent the new file from ever being imported at all
    case StorageType of
        posix ->
            storage_import_test_utils:enable_continuous_scan(TestCaseCtx, #{
                detect_deletions => false,
                detect_modifications => false
            });
        s3 ->
            storage_import_test_utils:enable_continuous_scan(TestCaseCtx)
    end,
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the new file is now visible, with its content, on both providers - the
    %% still-empty untouched sibling directory is automatically excluded from
    %% this check on S3 (see verify_imported_tree/2 and the module doc)
    UpdatedTreeSpec = [
        #dir_spec{name = TouchedDirName, children = [#file_spec{name = FileName, content = Content}]},
        #dir_spec{name = UntouchedDirName}
        | OtherFileSpecs
    ],
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedTreeSpec),
    ?IF_POSIX(StorageType, begin
        assert_children_mtime_changed(ImportingProviderSelector, SpaceId, TouchedDirStorageFileId),
        assert_children_hash_unchanged(ImportingProviderSelector, SpaceId, UntouchedDirStorageFileId),
        assert_children_mtime_unchanged(ImportingProviderSelector, SpaceId, UntouchedDirStorageFileId)
    end),
    %% root can never be modified on S3 (module doc) and detect_modifications is
    %% disabled on POSIX (see above), so nothing is ever "modified" either way -
    %% only the new nested file is "created". On POSIX every already-known
    %% entity is forced unmodified: root (4, same batch count as scan 1 - its
    %% direct children are unchanged) + touched dir (1) + untouched dir (1) + 4
    %% untouched files (4) = 10. On S3 root now has 5 children (the new file
    %% joins the same flat listing, see module doc) => ceil(5/2) = 3 total
    %% batches; there is no separate "touched dir" entity to tally, but the 4
    %% untouched files still each count towards unmodified regardless of
    %% whether they end up individually re-checked or bulk-marked as part of an
    %% unchanged batch (see the "Batch-hash mechanism" established for
    %% chmod_file_update_in_batched_dir_test) => 3 + 4 = 7
    Scan2Unmodified = case StorageType of posix -> 10; s3 -> 7 end,
    %% number of nodes counted towards "created" on scan 1 (dirs don't count on
    %% S3, see module doc), feeding the cumulative hour/day hists
    Scan1Created = case StorageType of posix -> 6; s3 -> 4 end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"unmodified">> => Scan2Unmodified,
        %% the scan-1 creations may have already aged out of the short
        %% createdMinHist window by the time scan 2 is awaited and the
        %% tree/monitoring re-verified; the longer hour/day windows still hold them
        <<"createdMinHist">> => {range, 1, 1 + Scan1Created},
        <<"createdHourHist">> => 1 + Scan1Created,
        <<"createdDayHist">> => 1 + Scan1Created,
        <<"modifiedMinHist">> => 0,
        <<"modifiedHourHist">> => 0,
        <<"modifiedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0
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
    ?assertEqual(0, count_children_mtime_changed_results(ProviderSelector, SpaceId, DirStorageFileId)).


%% @private
%% Asserts that storage_sync_traverse reported (at least once) that the
%% directory at DirStorageFileId's own mtime had changed - i.e. that a
%% modification of one of its children was picked up via the directory's own
%% mtime (adding/removing a direct child bumps it). Requires the module to have
%% been mocked (with a passthrough) via mock_storage_sync_traverse/1.
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
