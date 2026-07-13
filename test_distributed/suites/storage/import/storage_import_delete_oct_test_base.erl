%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains base test functions for testing how storage import
%%% detects and propagates DELETIONS made directly on the imported storage: a
%%% continuous scan must notice that an entry that was imported earlier is gone
%%% from the storage, remove the corresponding logical file (together with its
%%% associated state - custom metadata, xattrs, ...) from the space on both
%%% providers, and report it in the scan's "deleted" monitoring counter.
%%%
%%% NOTE: Every storage deletion is preceded by ensure_mtime_progression/1
%%% (it opens the deletion-detection gate - see its doc).
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_delete_oct_test_base).
-author("Bartosz Walkowicz").

-include("storage_import_oct_test.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/errno.hrl").

% API
-export([
    init_per_testcase/3,
    end_per_testcase/3
]).

%% tests
-export([
    empty_directory_deletion_test/1,
    non_empty_directory_deletion_test/1,
    import_continues_after_deletion_test/1,
    recreate_file_deleted_by_sync_test/1,
    imported_file_delete_recreate_lifecycle_test/1,
    simultaneous_deletion_and_modification_test/1,
    file_deletion_purges_metadata_test/1,
    nested_file_deletion_test/1,
    bulk_deletion_test/1,
    create_subfiles_and_delete_before_import_is_finished_test/1
]).


%%%===================================================================
%%% SetUp and TearDown
%%%===================================================================


init_per_testcase(_Case, _SuiteCtx, Config) ->
    lfm_proxy:init(Config).


end_per_testcase(_Case, _SuiteCtx, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Tests
%%%===================================================================


%% Baseline deletion scenario. POSIX-only (an empty directory has no storage
%% object on S3, so it can neither be imported nor deleted there).
empty_directory_deletion_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        file_tree_spec = FileTreeSpec
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #dir_spec{children = []}, SuiteCtx
    ),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_import_test_utils:delete_file_tree_from_storage(
        ImportingProviderSelector, ImportedStorageId, FileTreeSpec
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []),

    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 1,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        %% scan 1 created the single (empty) directory
        created_hist => 1,
        modified_hist => RootModified,
        deleted_hist => 1
    }).


non_empty_directory_deletion_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        file_tree_spec = FileTreeSpec
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #dir_spec{children = [#file_spec{content = ?RAND_STR()}]}, SuiteCtx
    ),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_import_test_utils:delete_file_tree_from_storage(
        ImportingProviderSelector, ImportedStorageId, FileTreeSpec
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []),

    %% scan 1: the directory + its file on POSIX, only the file (object) on S3
    Scan1Created = storage_import_test_utils:expected_created_count(TestCaseCtx),
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        %% the (emulated/real) directory + its child, on both storage types
        <<"deleted">> => 2,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        created_hist => Scan1Created,
        modified_hist => RootModified,
        deleted_hist => 2
    }).


%% After an entry is deleted from the storage and the deletion has been
%% propagated, storage import must keep working normally - a subsequently created
%% entry is imported by the next scan just as it would have been without the
%% preceding deletion. Exercises three scans over the space root's direct child:
%% import a directory (holding a file), delete the whole directory, then create a
%% brand new directory (holding a file) - each change detected by its own scan.
import_continues_after_deletion_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        file_tree_spec = FileTreeSpec
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #dir_spec{children = [#file_spec{content = ?RAND_STR()}]}, SuiteCtx
    ),

    %% scan 1 created the directory + its file on POSIX, only the file on S3; a
    %% full deletion removes both logical entries on either storage type
    Scan1Created = storage_import_test_utils:expected_created_count(TestCaseCtx),
    Deleted = storage_import_test_utils:expected_deleted_count(TestCaseCtx),
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),

    %% scan 2: the whole imported directory disappears from the storage
    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_import_test_utils:delete_file_tree_from_storage(
        ImportingProviderSelector, ImportedStorageId, FileTreeSpec
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => Deleted,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        created_hist => Scan1Created,
        modified_hist => RootModified,
        deleted_hist => Deleted
    }),

    %% scan 3: a brand new directory (holding a file), of the same shape as the
    %% initial one, is created on the storage and must be imported normally
    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    NewFileTreeSpec = storage_import_test_utils:create_file_tree_on_storage(
        ImportingProviderSelector, ImportedStorageId,
        #dir_spec{children = [#file_spec{content = ?RAND_STR()}]}
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 3),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, NewFileTreeSpec),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 3,
        %% the new tree has the same shape as the initial one
        <<"created">> => Scan1Created,
        <<"deleted">> => 0,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        %% histograms are cumulative over scans 1..3; over this longer, three-scan
        %% run the earlier scans' events may age out of the 60 s Min window, so the
        %% Min histograms are bounded with a range while Hour/Day are asserted exactly
        <<"createdHourHist">> => 2 * Scan1Created,
        <<"createdDayHist">> => 2 * Scan1Created,
        <<"createdMinHist">> => {range, Scan1Created, 2 * Scan1Created},
        <<"modifiedHourHist">> => 2 * RootModified,
        <<"modifiedDayHist">> => 2 * RootModified,
        <<"modifiedMinHist">> => {range, RootModified, 2 * RootModified},
        <<"deletedHourHist">> => Deleted,
        <<"deletedDayHist">> => Deleted,
        <<"deletedMinHist">> => {range, 0, Deleted}
    }).


%% A file created via LFM on the remote provider and replicated onto the imported
%% storage disappears from the storage - the scan deletes it from the space (on
%% both providers, counting it "deleted" even though it was never "created" by
%% import). Recreating the same path via the remote provider must then work
%% normally: the sync-driven deletion leaves no state (marker, link, location)
%% blocking the name.
recreate_file_deleted_by_sync_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,
    FileName = ?RAND_STR(),
    InitialContent = ?RAND_STR(),
    RecreatedContent = ?RAND_STR(),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, undefined, SuiteCtx
    ),
    SpaceFilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    storage_import_test_utils:create_file_tree_via_remote_provider(
        TestCaseCtx, #file_spec{name = FileName, content = InitialContent}
    ),
    %% reading on the importing provider replicates the content, materializing
    %% the file on the imported storage
    storage_import_test_utils:assert_file_content(
        ImportingProviderCtx, SpaceFilePath, InitialContent
    ),
    ?assertMatch(
        {ok, _},
        storage_file_setup_utils:stat(ImportingProviderSelector, ImportedStorageId, StorageFileId),
        ?ATTEMPTS
    ),

    %% the file disappears from the storage - scan 2 must delete it from the space
    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_file_setup_utils:delete_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, byte_size(InitialContent)
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []),

    %% the root's logical mtime (stamped by the remote LFM create) predates the
    %% storage deletion by at least the ensure_mtime_progression second, so the
    %% root verdict is deterministic despite the LFM-arranged layout
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        %% scan 1 ran on an empty storage and the replica was never counted -
        %% the deletion is the file's only trace in the counters
        <<"created">> => 0,
        <<"deleted">> => 1,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        created_hist => 0,
        modified_hist => RootModified,
        deleted_hist => 1
    }),

    %% the same path must be recreatable via the remote provider and fully
    %% functional on the importing one
    storage_import_test_utils:create_file_tree_via_remote_provider(
        TestCaseCtx, #file_spec{name = FileName, content = RecreatedContent}
    ),
    storage_import_test_utils:assert_file_content(
        ImportingProviderCtx, SpaceFilePath, RecreatedContent
    ).


%% Full lifecycle of an imported file, exercising both directions of change
%% propagation: a file imported from the storage (scan 1) is unlinked via LFM on
%% the remote provider and the deletion must reach the imported storage; the same
%% path is then recreated via remote LFM and read on the importing provider
%% (replication re-materializes the storage file under the freed name); finally
%% the storage file is deleted directly on the storage and scan 2 must remove
%% the recreated file from both providers.
imported_file_delete_recreate_lifecycle_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,
    RecreatedContent = ?RAND_STR(),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        file_tree_spec = #file_spec{name = FileName},
        importing_provider_ctx = ImportingProviderCtx,
        non_importing_provider_ctx = #provider_ctx{
            node = NonImportingProviderNode,
            session_id = NonImportingProviderSessionId
        }
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{content = ?RAND_STR()}, SuiteCtx
    ),
    SpaceFilePath = filepath_utils:join([SpacePath, FileName]),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    %% unlinking the imported file on the remote provider must propagate down to
    %% the imported storage
    ?assertMatch(ok, lfm_proxy:unlink(
        NonImportingProviderNode, NonImportingProviderSessionId, {path, SpaceFilePath}
    )),
    ?assertMatch(
        {error, ?ENOENT},
        storage_file_setup_utils:stat(ImportingProviderSelector, ImportedStorageId, StorageFileId),
        ?CROSS_PROVIDER_PROPAGATION_ATTEMPTS
    ),

    %% the freed path is recreated remotely; reading on the importing provider
    %% replicates the new content back onto the imported storage
    storage_import_test_utils:create_file_tree_via_remote_provider(
        TestCaseCtx, #file_spec{name = FileName, content = RecreatedContent}
    ),
    storage_import_test_utils:assert_file_content(
        ImportingProviderCtx, SpaceFilePath, RecreatedContent
    ),
    ?assertMatch(
        {ok, _},
        storage_file_setup_utils:stat(ImportingProviderSelector, ImportedStorageId, StorageFileId),
        ?ATTEMPTS
    ),

    %% the storage file disappears - scan 2 must delete the recreated file from
    %% both providers
    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_file_setup_utils:delete_file(
        ImportingProviderSelector, ImportedStorageId, StorageFileId, byte_size(RecreatedContent)
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []),

    %% deterministic root verdict for the same reason as in
    %% recreate_file_deleted_by_sync_test above
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 1,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        %% scan 1 imported the original file; its LFM-recreated successor was
        %% never counted "created"
        created_hist => 1,
        modified_hist => RootModified,
        deleted_hist => 1
    }).


%% A file deleted from the storage and a sibling file modified on the storage
%% (here via chmod) between two scans must both be picked up by the SAME next
%% scan - the deletion reported as "deleted" and the modification as "modified",
%% in a single pass. POSIX-only: chmod is a no-op on object storages (see
%% keyValueAdapter::chmod, which persists nothing), so the modification half of
%% the scenario cannot be exercised on S3 (the deletion half alone is already
%% covered there by nested_file_deletion_test).
simultaneous_deletion_and_modification_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,
    NewMode = 8#600,

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        file_tree_spec = #dir_spec{name = DirName, children = [DeletedFileSpec, ModifiedFileSpec]},
        importing_provider_ctx = ImportingProviderCtx
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME,
        #dir_spec{children = [#file_spec{content = ?RAND_STR()}, #file_spec{content = ?RAND_STR()}]},
        SuiteCtx
    ),
    #file_spec{name = DeletedFileName, content = DeletedContent} = DeletedFileSpec,
    #file_spec{name = ModifiedFileName} = ModifiedFileSpec,

    %% delete one file and chmod its sibling, both directly on the storage
    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_file_setup_utils:delete_file(
        ImportingProviderSelector, ImportedStorageId,
        filepath_utils:join([<<"/">>, DirName, DeletedFileName]), byte_size(DeletedContent)
    ),
    storage_file_setup_utils:chmod(
        ImportingProviderSelector, ImportedStorageId,
        filepath_utils:join([<<"/">>, DirName, ModifiedFileName]), NewMode
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(
        TestCaseCtx, #dir_spec{name = DirName, children = [ModifiedFileSpec]}
    ),
    ModifiedFilePath = filepath_utils:join([SpacePath, DirName, ModifiedFileName]),
    storage_import_test_utils:assert_attrs(ImportingProviderCtx, ModifiedFilePath, #{mode => NewMode}),

    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 1,
        %% the parent directory (a child was removed, bumping its mtime) and the
        %% chmod'd file; the space root's own direct children are unchanged
        <<"modified">> => 2,
        <<"unmodified">> => 1,
        created_hist => storage_import_test_utils:expected_created_count(TestCaseCtx),
        modified_hist => 2,
        deleted_hist => 1
    }).


file_deletion_purges_metadata_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        file_tree_spec = #file_spec{name = FileName} = FileTreeSpec,
        importing_provider_ctx = #provider_ctx{node = Node, session_id = SessId}
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{content = ?RAND_STR()}, SuiteCtx
    ),

    %% attach an xattr (and thus a custom_metadata document) to the imported file
    FilePath = filepath_utils:join([SpacePath, FileName]),
    {ok, #file_attr{guid = FileGuid}} = ?assertMatch(
        {ok, #file_attr{}}, lfm_proxy:stat(Node, SessId, {path, FilePath})
    ),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    ok = lfm_proxy:set_xattr(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid),
        #xattr{name = <<"xattr_name">>, value = <<"xattr_value">>}),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_import_test_utils:delete_file_tree_from_storage(
        ImportingProviderSelector, ImportedStorageId, FileTreeSpec
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:get_xattr(Node, SessId, {path, FilePath}, <<"xattr_name">>), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT},
        lfm_proxy:get_xattr(Node, SessId, ?FILE_REF(FileGuid), <<"xattr_name">>), ?ATTEMPTS),
    ?assertMatch({error, not_found},
        ?rpc(ImportingProviderSelector, custom_metadata:get(FileUuid)), ?ATTEMPTS),

    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 1,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        %% scan 1: the single file (files count on both storage types)
        created_hist => 1,
        modified_hist => RootModified,
        deleted_hist => 1
    }).


%% One of two files held by an imported directory is deleted; the surviving
%% sibling keeps the directory alive (crucial on S3, where a directory with no
%% objects under its prefix would itself become unobservable). Unlike the
%% whole-(sub)tree deletions above, the space root is untouched (its direct
%% children are unchanged) - it is the parent DIRECTORY that is reclassified:
%% modified on POSIX, not tracked at all on S3 (emulated directories have no
%% statbuf).
nested_file_deletion_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        file_tree_spec = #dir_spec{name = DirName, children = [DeletedFileSpec, KeptFileSpec]}
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME,
        #dir_spec{children = [#file_spec{content = ?RAND_STR()}, #file_spec{content = ?RAND_STR()}]},
        SuiteCtx
    ),

    #file_spec{name = DeletedFileName, content = DeletedContent} = DeletedFileSpec,
    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_file_setup_utils:delete_file(
        ImportingProviderSelector, ImportedStorageId,
        filepath_utils:join([<<"/">>, DirName, DeletedFileName]), byte_size(DeletedContent)
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(
        TestCaseCtx, #dir_spec{name = DirName, children = [KeptFileSpec]}
    ),

    %% scan 1: the directory + its 2 files on POSIX, only the 2 files on S3
    Scan1Created = storage_import_test_utils:expected_created_count(TestCaseCtx),
    %% test doc: the parent directory - modified on POSIX, not tracked on S3
    DirModified = case StorageType of posix -> 1; s3 -> 0 end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 1,
        <<"modified">> => DirModified,
        %% the space root and the surviving sibling file are both unmodified
        <<"unmodified">> => 2,
        created_hist => Scan1Created,
        modified_hist => DirModified,
        deleted_hist => 1
    }).


bulk_deletion_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,

    %% 1 wrapping dir + 5 + 25 subdirectories (31 dirs) holding 250 files
    %% the default derives 'created' from the declared tree (281 entries on POSIX,
    %% only the 250 files on S3); at this scale scan 1 can outlast the 60 s Min
    %% histogram span, so its early creations may age out of even the whole (fully
    %% summed) Min histogram before it is read - hence createdMinHist => skip
    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        file_tree_spec = FileTreeSpec
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME,
        #dir_spec{children = storage_import_test_utils:gen_nested_tree_spec([5, 5, 10], ?RAND_STR())},
        SuiteCtx,
        #{
            scan_attempts => ?LARGE_IMPORT_SCAN_ATTEMPTS,
            monitoring_overrides => #{<<"createdMinHist">> => skip}
        }
    ),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_import_test_utils:delete_file_tree_from_storage(
        ImportingProviderSelector, ImportedStorageId, FileTreeSpec
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2, #{}, ?LARGE_IMPORT_SCAN_ATTEMPTS),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []),
    storage_import_test_utils:verify_dir_stats(TestCaseCtx, []),

    %% deleted: all 281 logical entries (250 files + 31 dirs), on both storage
    %% types; scan 1 created only the 250 files on S3
    Scan1Created = storage_import_test_utils:expected_created_count(TestCaseCtx),
    Deleted = storage_import_test_utils:expected_deleted_count(TestCaseCtx),
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => Deleted,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        %% Min histograms skipped - same aging rationale as at setup above
        created_hist => Scan1Created,
        <<"createdMinHist">> => skip,
        modified_hist => RootModified,
        deleted_hist => Deleted,
        <<"deletedMinHist">> => skip
    }).


%% The whole nested tree is removed from the storage WHILE the scan importing it
%% is still running - continuous scanning must converge regardless: subsequent
%% scans detect the deletions and the space ends up empty. Unlike the race suite
%% scenarios no scan process is parked by a mock - this exercises raw concurrency
%% between an ongoing import and a mass deletion at scale. Monitoring counters
%% are inherently nondeterministic here (how far the import got before the
%% deletion struck varies) and are deliberately not asserted.
create_subfiles_and_delete_before_import_is_finished_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_id = SpaceId,
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{node = Node, session_id = SessId}
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, undefined, SuiteCtx
    ),

    %% 1 wrapping dir + 5 + 25 subdirectories holding 250 files - large enough
    %% that the scan importing it is still running when the deletion strikes
    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    FileTreeSpec = storage_import_test_utils:create_file_tree_on_storage(
        ImportingProviderSelector, ImportedStorageId,
        #dir_spec{children = storage_import_test_utils:gen_nested_tree_spec([5, 5, 10], ?RAND_STR())}
    ),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    ?assertEqual(
        true,
        ?rpc(ImportingProviderSelector, storage_import_monitoring:is_scan_in_progress(SpaceId)),
        ?ATTEMPTS
    ),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_import_test_utils:delete_file_tree_from_storage(
        ImportingProviderSelector, ImportedStorageId, FileTreeSpec
    ),

    %% keep scanning until the space converges back to empty, then stop; the
    %% root mtime must progress on every attempt: the scan that first saw it
    %% changed raced with the still-running import and recorded the new value,
    %% which on flat storages (where the mocked root mtime moves only when
    %% advanced explicitly - see mock_space_dir_statbuf_on_flat_storage/1)
    %% would close the deletion-detection gate for all further scans
    ?assertMatch(
        {ok, []},
        begin
            storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
            lfm_proxy:get_children(Node, SessId, {path, SpacePath}, 0, 10)
        end,
        ?LARGE_IMPORT_SCAN_ATTEMPTS
    ),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []).
