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
%%% generic machinery lives in storage_import_test_utils (see its module doc).
%%%
%%% How scans classify entries - general facts relied on throughout, documented
%%% once here rather than repeated at every assertion (the counters themselves
%%% are asserted via assert_storage_import_monitoring_state/2 - see its doc for
%%% the per-scan counter vs cumulative time-windowed histogram distinction):
%%%  * on scan 1 every declared entry is counted "created" and the space root
%%%    is the single "unmodified" entry (the assertion helper's default);
%%%  * a directory's own modified/unmodified verdict is driven solely by its
%%%    own mtime/ctime, which only a change to its DIRECT children set
%%%    (add/remove/rename/replace) bumps - a child's content/attrs change does
%%%    not; in particular, the space root flips to "modified" exactly when a
%%%    direct child of the root is added/removed (never on S3, see below);
%%%  * a regular file is "modified" when its size OR mtime changed (see
%%%    storage_import_engine:maybe_update_file_location/4) - size/mode changes
%%%    are detected regardless of mtime resolution, while a same-size
%%%    content-only change relies on the mtime having visibly advanced;
%%%  * whether a directory's children get individually reprocessed is decided
%%%    separately, per listing batch, via a hash of the children's attrs (see
%%%    storage_import_hash) - but children of an unchanged batch, though
%%%    bulk-skipped, still count towards "unmodified" all the same;
%%%  * (POSIX) a directory listing that returns EXACTLY
%%%    storage_import_dir_batch_size entries schedules one extra
%%%    disambiguating batch to confirm end-of-listing (see
%%%    tree_storage_iterator:get_children_and_next_batch_job/1), and every
%%%    such extra pass re-runs the directory's OWN verdict, adding one more
%%%    "unmodified" - relevant only for tests that lower the batch size below
%%%    their trees' child counts.
%%%
%%% Object storages (e.g. S3, see flat_storage_iterator.erl) diverge from
%%% POSIX in ways that recur across many tests below:
%%%  * they have no real directories - an empty one has no underlying storage
%%%    object at all, so it can never be imported/observed via LFM (handled
%%%    transparently by storage_import_test_utils:verify_imported_tree/2,
%%%    which strips such directories from the expected tree on S3), and
%%%    directories never count towards "created" (only regular files do);
%%%    deletion counting, however, includes directories on S3 too;
%%%  * the space root's own storage statbuf is permanently mocked into the
%%%    past for scan-1 timing determinism (see
%%%    mock_space_dir_statbuf_on_flat_storage/1), so the root can never be
%%%    classified "modified" on any scan;
%%%  * there is no per-directory traversal at all - the whole space is a
%%%    single flat traversal entity, with only regular files counted as its
%%%    children for batching, and the listing API reports a definitive
%%%    end-of-listing marker on the very page that exhausts it (no POSIX-style
%%%    extra disambiguating batch);
%%%  * consequently (frozen root mtime + no recursion to fall back on),
%%%    running a continuous scan with detect_modifications => false would
%%%    discard ALL of the root's batch slave jobs outright - INCLUDING
%%%    brand-new files, which would then simply never be imported (on POSIX
%%%    the same shortcut is harmless: a subdirectory's own master job catches
%%%    what its parent's shortcut skipped). S3 update tests must therefore
%%%    keep modification detection enabled (see
%%%    create_file_in_dir_exceed_batch_update_test).
%%% Tests below note only what is specific to their own scenario; assume these
%%% general facts hold unless stated otherwise.
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
    update_syncs_files_after_previous_update_failed_test/1

    %% --- suffixes ---

    %% --- config ---

    %% --- protection ---

    %% --- not reimported ---
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
    storage_import_test_utils:unmock_import_file_error(TestSuiteCtx),
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
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:verify_dir_stats(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

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
%% pre-append value right after the storage-level write (simulating a storage
%% backend that does not reliably bump mtime on writes at the resolution the
%% scan relies on) - the appended bytes must still be detected and imported via
%% the size clause of the modification check (module doc). POSIX-only - forcing
%% the storage mtime relies on storage_file_setup_utils:set_mtime/4.
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
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

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
%% the initial scan, and only gains content on the continuous scan - exercising
%% the 0-byte-file code path specifically.
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
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, 0, AppendedContent
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    UpdatedFileSpec = #file_spec{name = FileName, content = AppendedContent},
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedFileSpec),
    assert_monitoring_state_after_single_file_modification(TestCaseCtx).


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
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

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
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    storage_file_setup_utils:chmod(ImportingProviderSelector, ImportedStorageId, StorageFileId, NewMode),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceTestFilePath, #{mode => NewMode}),
    storage_import_test_utils:assert_attrs(NonImportingProviderCtx, SpaceTestFilePath, #{mode => NewMode}),
    assert_children_hash_changed(ImportingProviderSelector, SpaceId, RootStorageFileId),
    assert_monitoring_state_after_single_file_modification(TestCaseCtx).


%% Like chmod_file_update_test, but the changed file sits inside a subdirectory
%% whose 3 children are scanned in more than one batch (storage_import_dir_batch_size
%% is temporarily lowered to 2 in init_per_testcase and restored in
%% end_per_testcase) - exercising the hash-based children-attrs change detection
%% one level down from the space root, across a directory scanned in multiple
%% batches. Also asserts (via a passthrough mock on storage_sync_traverse) that
%% the modification is picked up in spite of, not because of, the parent
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
    } = storage_import_test_utils:init_testcase(?FUNCTION_NAME, [
        #dir_spec{name = TestDirName, children = [
            #file_spec{name = File1Name, content = ?RAND_STR()},
            #file_spec{name = ?RAND_STR(), content = ?RAND_STR()},
            #file_spec{name = ?RAND_STR(), content = ?RAND_STR()}
        ]},
        #dir_spec{name = ?RAND_STR()}
    ], SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        %% root's own verdict (1) + 1 extra disambiguating-batch pass each for
        %% root (exactly 2 children) and TestDirName (first batch exactly 2 of
        %% 3 children) - see the module doc's batch-size note; the empty
        %% sibling dir (0 children) needs none
        <<"unmodified">> => 3
    }),

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
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        %% same extra-batch-pass arithmetic as scan 1: root and TestDirName
        %% re-checked twice each, the empty sibling once, plus the 2 untouched
        %% files once each => 2+2+1+1+1 = 7
        <<"unmodified">> => 7
    }).


%% A file imported by the initial scan is renamed (moved) to a different path on
%% the storage; the next (continuous) scan detects the old path as deleted and the
%% new path as newly created, and the file becomes reachable at the new path (and
%% only there) on both providers. POSIX-only.
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
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    storage_file_setup_utils:rename(
        ImportingProviderSelector, ImportedStorageId, SrcStorageFileId, DstStorageFileId
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% the file is now reachable (with its original content) at the new path -
    %% and, implicitly, gone from the old one, since verify_imported_tree
    %% asserts the exact set of the space root's children
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, #file_spec{
        name = DstFileName, content = Content
    }),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"deleted">> => 1,
        %% the root's direct children set changed => the root itself is
        %% modified this scan (module doc), leaving nothing to count as unmodified
        <<"modified">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"unmodified">> => 0,
        %% scan-1's creation may have aged out of the Min window by now
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
%% identical content, on both providers. POSIX-only.
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
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    %% the "copy" is created directly on the storage - a byte-identical file at
    %% a new path is indistinguishable from a host-level copy for the scan
    storage_file_setup_utils:create_file(ImportingProviderSelector, ImportedStorageId, DstStorageFileId, Content),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, [
        #file_spec{name = SrcFileName, content = Content},
        #file_spec{name = DstFileName, content = Content}
    ]),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        %% the root gained a new direct child => the root itself is modified
        %% this scan (module doc); the untouched original counts as unmodified
        <<"modified">> => 1,
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        <<"unmodified">> => 1,
        %% scan-1's creation may have aged out of the Min window by now
        <<"createdMinHist">> => {range, 1, 2},
        <<"createdHourHist">> => 2,
        <<"createdDayHist">> => 2
    }).


%% A file imported by the initial scan has its content overwritten on the storage
%% WITHOUT changing its size (a fixed-length random string is swapped for another);
%% the next (continuous) scan detects the change and the new content is reflected
%% through the logical filesystem on both providers. Unlike append/truncate/chmod,
%% a same-size content change has no size/mode signal to fall back on - detection
%% relies entirely on the file's mtime having advanced (module doc), so an
%% explicit delay before the write is required here.
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
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    %% load-bearing - lets the mtime tick visibly past the initial scan's stat
    %% (see the doc comment above); do not remove
    timer:sleep(timer:seconds(2)),
    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, 0, ChangedContent
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    UpdatedFileSpec = #file_spec{name = FileName, content = ChangedContent},
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedFileSpec),
    assert_monitoring_state_after_single_file_modification(TestCaseCtx).


%% Like change_file_content_constant_size_test, but the new content is a
%% different length than the original - the modification check's size clause
%% detects it regardless of mtime resolution, so (like append/truncate/chmod)
%% no explicit delay is needed.
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
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    storage_file_setup_utils:write_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, 0, ChangedContent
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    UpdatedFileSpec = #file_spec{name = FileName, content = ChangedContent},
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedFileSpec),
    assert_monitoring_state_after_single_file_modification(TestCaseCtx).


%% A file's content is overwritten on the storage at (deliberately forced to be)
%% the EXACT same timestamp as storage_import's own last recorded stat of that
%% file - exercising the boundary of the "already handled" fast-path shortcut in
%% storage_import_engine:maybe_update_file_location/4, which requires the stored
%% last_stat to be STRICTLY greater than the storage mtime to skip re-checking a
%% file (if it used >= instead of >, this test's real content change would be
%% incorrectly skipped). The change is same-size, so the fast-path is the only
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
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    %% overwrite the content, then force the file's storage mtime to be EQUAL
    %% (not smaller) to storage_sync_info's recorded last_stat - the exact
    %% boundary of the fast-path guard described in the doc above
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
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    UpdatedFileSpec = #file_spec{name = FileName, content = ChangedContent},
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, UpdatedFileSpec),
    assert_monitoring_state_after_single_file_modification(TestCaseCtx).


%% A file imported by the initial scan is deleted on the storage and replaced,
%% under the SAME name, by a directory holding one child file; the next
%% (continuous) scan detects the type change - deleting the old file entry and
%% importing the new directory (and its child) in its place. Also verifies the
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
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = InitialContent}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    storage_file_setup_utils:delete_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, byte_size(InitialContent)
    ),
    storage_file_setup_utils:create_dir(ImportingProviderSelector, ImportedStorageId, StorageFileId),
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId, ChildStorageFileId, ChildContent
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% the path now resolves to a directory with the new child file, on both providers
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, #dir_spec{name = FileName, children = [
        #file_spec{name = ChildFileName, content = ChildContent}
    ]}),
    %% the new directory counts towards "created" only on POSIX (module doc)
    NewlyCreated = case StorageType of posix -> 2; s3 -> 1 end,
    %% the root's direct children set changed => root is modified... except on
    %% S3, where it never can be (module doc) and stays unmodified instead
    {RootModified, RootUnmodified} = case StorageType of posix -> {1, 0}; s3 -> {0, 1} end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => NewlyCreated,
        <<"modified">> => RootModified,
        <<"deleted">> => 1,
        <<"unmodified">> => RootUnmodified,
        %% scan-1's creation may have aged out of the Min window by now
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
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    storage_file_setup_utils:rmdir(ImportingProviderSelector, ImportedStorageId, StorageFileId),
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, NewContent
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% the path now resolves to a regular file with the new content, on both providers
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, #file_spec{name = DirName, content = NewContent}),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        %% the root's direct children set changed => root is modified (module doc)
        <<"modified">> => 1,
        <<"deleted">> => 1,
        <<"unmodified">> => 0,
        %% scan-1's creation may have aged out of the Min window by now
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
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    storage_file_setup_utils:delete_file(
        ImportingProviderSelector, ImportedStorageId, ChildStorageFileId, byte_size(ChildContent)
    ),
    storage_file_setup_utils:rmdir(ImportingProviderSelector, ImportedStorageId, StorageFileId),
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, NewContent
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% the path now resolves to a regular file with the new content, on both providers
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, #file_spec{name = DirName, content = NewContent}),
    %% scan-1's created count differs by storage type (dirs don't count on S3, module doc)
    Scan1Created = case StorageType of posix -> 2; s3 -> 1 end,
    %% the root's direct children set changed => root is modified, except on S3
    %% where it never can be (module doc)
    {RootModified, RootUnmodified} = case StorageType of posix -> {1, 0}; s3 -> {0, 1} end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => RootModified,
        %% both the directory and its child count towards "deleted" - unlike
        %% creation, deletion counting includes directories on S3 too (module doc)
        <<"deleted">> => 2,
        <<"unmodified">> => RootUnmodified,
        %% scan-1's creation(s) may have aged out of the Min window by now
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
%% the modification check (module doc) and propagates BOTH new timestamps onto
%% the file's own attrs, on both providers - proving atime is synced too, not
%% just mtime. The forced timestamp is so far in the future that no explicit
%% delay is needed for the change to be detected (unlike
%% change_file_content_constant_size_test). POSIX-only - forcing storage
%% timestamps relies on storage_file_setup_utils:set_atime_and_mtime/5.
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
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    storage_file_setup_utils:set_atime_and_mtime(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, NewTimestamp, NewTimestamp
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    ExpectedTimes = #{atime => NewTimestamp, mtime => NewTimestamp},
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, SpaceTestFilePath, ExpectedTimes),
    storage_import_test_utils:assert_attrs(NonImportingProviderCtx, SpaceTestFilePath, ExpectedTimes),
    assert_monitoring_state_after_single_file_modification(TestCaseCtx).


%% A new file is created (on the storage) inside one of two sibling directories
%% imported (both empty) by the initial scan; the next (continuous) scan detects
%% it. The new file is nested one level below the space root, so the root's own
%% children set (and hence its mtime) is untouched: only the TOUCHED directory
%% flips to modified, while the root and the UNTOUCHED sibling stay unmodified
%% (module doc). On POSIX this is additionally proven at the mechanism level via
%% passthrough mocks: the touched directory's own mtime changed, while the
%% root's children-attrs hash and the untouched sibling's mtime did not.
%% On S3 the (empty) directories are unobservable and nothing can be classified
%% modified at all (module doc) - the root is the single unmodified entry.
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
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    %% white-box mechanism checks, POSIX-only
    ?IF_POSIX(StorageType, begin
        mock_storage_import_hash(SuiteCtx),
        mock_storage_sync_traverse(SuiteCtx)
    end),

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
    %% see the test doc: POSIX - touched dir modified, root + untouched sibling
    %% unmodified; S3 - nothing modified, root alone unmodified
    {ModifiedCount, UnmodifiedCount} = case StorageType of posix -> {1, 2}; s3 -> {0, 1} end,
    %% scan-1's created count feeding the cumulative hists (dirs don't count on S3)
    Scan1Created = case StorageType of posix -> 2; s3 -> 0 end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => ModifiedCount,
        <<"deleted">> => 0,
        <<"unmodified">> => UnmodifiedCount,
        %% scan-1's creations may have aged out of the Min window by now
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
%% (temporarily lowered to 2 in init_per_testcase) - so it's the ROOT's own
%% scan-1 baseline that spans multiple listing batches this time, not a
%% subdirectory's one level down.
%% On POSIX, scan 2 additionally runs with detect_modifications and
%% detect_deletions disabled - proving the new file is still discovered and
%% imported (the flags only suppress the modified/deleted classification of
%% already-known entries, see storage_import_engine:maybe_update_file/4, which
%% is why the white-box mock asserts below still see the underlying detection
%% fire). On S3, scan 2 keeps the default (enabled) detection config -
%% disabling modification detection there would prevent the new file from ever
%% being imported at all (module doc) - so S3 covers only the batch-exceeding
%% aspect of this test.
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
    %% root's own verdict is re-run once per listing batch (module doc). POSIX:
    %% 6 children / batch size 2 => reads at offsets 0,2,4,6 (sizes 2,2,2,0) =
    %% 4 passes. S3: only the 4 files count as root's children and there is no
    %% extra disambiguating batch => ceil(4/2) = 2 passes
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
        assert_children_mtime_changed(ImportingProviderSelector, SpaceId, TouchedDirStorageFileId),
        assert_children_hash_unchanged(ImportingProviderSelector, SpaceId, UntouchedDirStorageFileId),
        assert_children_mtime_unchanged(ImportingProviderSelector, SpaceId, UntouchedDirStorageFileId)
    end),
    %% nothing counts as "modified" this scan either way: POSIX suppresses the
    %% classification via detect_modifications=false, on S3 the root never can
    %% be modified and nothing else picks up a tally (module doc).
    %% POSIX unmodified (every already-known entry forced unmodified): root's 4
    %% batch passes (children count unchanged) + touched dir + untouched dir +
    %% 4 untouched files = 10. S3: the new file joins root's flat listing (5
    %% children => 3 passes) + the 4 untouched files (counted unmodified whether
    %% individually re-checked or bulk-marked, module doc) = 7
    Scan2Unmodified = case StorageType of posix -> 10; s3 -> 7 end,
    %% scan-1's created count feeding the cumulative hists (dirs don't count on S3)
    Scan1Created = case StorageType of posix -> 6; s3 -> 4 end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"unmodified">> => Scan2Unmodified,
        %% scan-1's creations may have aged out of the Min window by now
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


%% A file imported by the initial scan (with sync_acl enabled) has its
%% on-storage NFS4 ACL changed; the next (continuous) scan detects the change
%% and re-applies the new ACL, changing its enforcement accordingly - not just
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
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1,
        %% scan-1's creation may have aged out of the Min window by now
        <<"createdMinHist">> => {range, 0, 1},
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1
    }).


%% --- idempotency ---


%% A file imported by the initial scan is left completely untouched; the next
%% (continuous) scan must not process it again: the children-attrs hash of the
%% root's listing batch is unchanged, so the file is bulk-skipped and merely
%% counted "unmodified" alongside the root (module doc) - nothing is created,
%% modified or deleted, on either storage type.
should_not_process_file_with_unchanged_attrs_hash_test(SuiteCtx) ->
    TestCaseCtx = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = ?RAND_STR(), content = ?RAND_STR()}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    %% deliberately no storage mutation - rescan the untouched storage
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"unmodified">> => 2
    }).


%% A file imported by the initial scan has its atime and mtime forced (on the
%% storage) back to epoch 1 while the continuous scan runs with BOTH detection
%% flags disabled: the scan must NOT propagate the new timestamps onto the
%% logical file, and every entry is forced
%% "unmodified" without any attrs comparison (module doc). Note that with the
%% DEFAULT config this mutation would be picked up: forcing the timestamps also
%% bumps the file's storage ctime to "now" (ctime cannot be set explicitly),
%% which the times check of the modification detection compares too - cf.
%% update_timestamps_file_import_test, where a (future-)timestamps-only change
%% is detected and synced. POSIX-only - forcing storage timestamps relies on
%% storage_file_setup_utils:set_atime_and_mtime/5.
should_not_detect_timestamp_update_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector} = SuiteCtx,
    FileName = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx
    } = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = ?RAND_STR()}, SuiteCtx
    ),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

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


%% Importing the declared file fails on the initial scan (via a mock raising
%% from the import engine - counted "failed") and the file does not appear in
%% the space; once the failure is gone (mock removed), the next (continuous)
%% scan retries and imports it successfully.
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

    %% the file was not imported
    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(ImportingProviderNode, ImportingProviderSessionId, {path, SpaceTestFilePath}),
        ?ATTEMPTS
    ),
    storage_import_test_utils:assert_monitoring_state_after_failed_import(TestCaseCtx),

    %% with the failure gone, the next scan imports the file
    storage_import_test_utils:unmock_import_file_error(SuiteCtx),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"unmodified">> => 1
    }).


%% Like update_syncs_files_after_import_failed_test, but the failure hits a
%% continuous scan: the initial scan imports an empty storage, then a file is
%% created on the storage and its import fails on scan 2 (counted "failed", the
%% file stays absent from the space); with the failure gone, scan 3 retries and
%% imports it successfully.
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
    } = storage_import_test_utils:init_testcase(?FUNCTION_NAME, undefined, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{}),

    %% create the file on the storage, with its import mocked to fail
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, Content
    ),
    storage_import_test_utils:mock_import_file_error(SuiteCtx, FileName),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% the file was not imported; creating it bumped the root's mtime, so the
    %% root is classified modified on posix (never on s3 - module doc)
    SpaceTestFilePath = filepath_utils:join([SpacePath, FileName]),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:stat(ImportingProviderNode, ImportingProviderSessionId, {path, SpaceTestFilePath}),
        ?ATTEMPTS
    ),
    {RootModified, RootUnmodified} = case StorageType of
        posix -> {1, 0};
        s3 -> {0, 1}
    end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"failed">> => 1,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        <<"modifiedMinHist">> => RootModified,
        <<"modifiedHourHist">> => RootModified,
        <<"modifiedDayHist">> => RootModified
    }),

    %% with the failure gone, the next scan imports the file
    storage_import_test_utils:unmock_import_file_error(SuiteCtx),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 3),

    storage_import_test_utils:verify_imported_tree(
        TestCaseCtx, #file_spec{name = FileName, content = Content}
    ),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 3,
        <<"created">> => 1,
        <<"unmodified">> => 1,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        %% scan-2's root modification (posix only) is still within the hour/day
        %% histogram windows, but may have aged out of the min window by now
        <<"modifiedMinHist">> => {range, 0, RootModified},
        <<"modifiedHourHist">> => RootModified,
        <<"modifiedDayHist">> => RootModified
    }).


%% --- suffixes ---


%% --- config ---


%% --- protection ---


%% --- not reimported ---


%%%===================================================================
%%% Internal functions - shared assertions
%%%===================================================================


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
        <<"modifiedMinHist">> => 1,
        <<"modifiedHourHist">> => 1,
        <<"modifiedDayHist">> => 1
    }).


%%%===================================================================
%%% Internal functions - test case specific mocks
%%%===================================================================


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
