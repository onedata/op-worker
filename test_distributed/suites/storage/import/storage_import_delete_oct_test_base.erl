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
%%% Contrast with storage_import_links_oct_test_base: there every removal went
%%% through LFM (the logical filesystem), so the scan itself had to delete
%%% NOTHING (deleted => 0) and a trigger file was needed to give the otherwise
%%% unchanged scan some work. Here every removal happens ON THE STORAGE, so the
%%% deletion itself is exactly what the scan must pick up (deleted > 0) and no
%%% trigger file is needed - the storage change already gives the scan work.
%%%
%%% Monitoring model (see also storage_import_update_oct_test_base for the full
%%% rationale). The scan's per-entry verdict for the SPACE ROOT is what makes
%%% these numbers storage-dependent:
%%%  * on POSIX removing a direct child of the space root bumps the root's mtime,
%%%    so on the continuous (deletion-detecting) scan the root is classified
%%%    "modified". This is a 1-second-granularity race against scan 1's recorded
%%%    root mtime, so storage_import_test_utils:ensure_mtime_progression/1 is
%%%    called right before every storage deletion to force it into a strictly
%%%    later mtime tick and make the verdict deterministic (root => modified). Note the INITIAL scan still
%%%    classifies the root as "unmodified" (that is the framework default) - the
%%%    "modified" verdict only appears on the continuous scan that mutates it;
%%%  * on S3 (flat storage) the space root statbuf is mocked to lie in the past
%%%    (mock_space_dir_statbuf_on_flat_storage), so the root can NEVER be
%%%    classified "modified" - it stays "unmodified" on every scan. Deletion
%%%    detection is gated on the root mtime having changed since the previous
%%%    scan, so ensure_mtime_progression/1 (the same call that sleeps on POSIX)
%%%    advances the mocked mtime - without it the scan would not detect any
%%%    deletions on the flat storage. Directories
%%%    are emulated by object key prefixes and have no storage object of their
%%%    own; a directory therefore disappears from the storage exactly when its
%%%    last descendant object is deleted (there is nothing to rmdir), yet storage
%%%    import still deletes the emulated logical directory, so it is counted in
%%%    "deleted" just like on POSIX.
%%% storage_import_test_utils:root_scan_verdict/1 captures this {Modified,
%%% Unmodified} split. See also the "mtime granularity and root-verdict races"
%%% section of storage_import_test_utils for the residual races behind these verdicts.
%%%
%%% The same set of cases is meaningful on both POSIX and object (S3) storages,
%%% except empty_directory_deletion_test, which is POSIX-only (an empty directory
%%% has no storage object at all on S3, so it can neither be imported nor deleted
%%% there). The thin per-storage suites are
%%% storage_import_delete_{posix,s3}_oct_test_SUITE.
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
    simultaneous_deletion_and_modification_test/1,
    file_deletion_purges_metadata_test/1,
    nested_file_deletion_test/1,
    bulk_deletion_test/1
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


%% An empty directory imported by the initial scan is removed (rmdir) directly on
%% the storage; the next (continuous) scan must detect it as deleted and remove it
%% from the space. POSIX-only - an empty directory has no storage object on S3.
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

    %% delete the (empty) directory directly on the storage
    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_import_test_utils:delete_file_tree_from_storage(
        ImportingProviderSelector, ImportedStorageId, FileTreeSpec
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% the directory is now gone from the space (on both providers)
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


%% A non-empty directory (holding one file) imported by the initial scan is
%% removed together with its content directly on the storage; the next
%% (continuous) scan must detect both the directory and its child as deleted.
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

    %% delete the whole (non-empty) directory directly on the storage - its child
    %% file, then the directory itself (a no-op rmdir on S3, where the directory is
    %% emulated by the object key prefix and vanishes together with its last object)
    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_import_test_utils:delete_file_tree_from_storage(
        ImportingProviderSelector, ImportedStorageId, FileTreeSpec
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% both the directory and its child are now gone from the space
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []),

    %% scan 1 created the directory + its file on POSIX, only the file (object) on S3
    Scan1Created = storage_import_test_utils:expected_created_count(TestCaseCtx),
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        %% both the emulated/real directory and its child are deleted, on both storages
        <<"deleted">> => 2,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        created_hist => Scan1Created,
        modified_hist => RootModified,
        deleted_hist => 2
    }).


%% TODO VFS-13687 port from storage_import_test_base:sync_works_properly_after_delete_test/1
import_continues_after_deletion_test(_SuiteCtx) ->
    error(not_yet_implemented).


%% TODO VFS-13687 port from storage_import_test_base:delete_and_update_files_simultaneously_update_test/1
simultaneous_deletion_and_modification_test(_SuiteCtx) ->
    error(not_yet_implemented).


%% A regular (top-level) file imported by the initial scan - to which an xattr
%% (and thus a custom_metadata document) is then attached - is deleted directly
%% on the storage; the next (continuous) scan must remove the file from the space
%% together with all of its associated metadata.
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

    %% delete the file directly on the storage
    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_import_test_utils:delete_file_tree_from_storage(
        ImportingProviderSelector, ImportedStorageId, FileTreeSpec
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% the file is gone from the space, together with its xattr and custom_metadata
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
        %% scan 1 created the single file (a file counts as created on both storages)
        created_hist => 1,
        modified_hist => RootModified,
        deleted_hist => 1
    }).


%% One of two files held by an imported directory is deleted directly on the
%% storage; the next (continuous) scan must remove only that file, leaving the
%% directory (kept alive by its surviving sibling - crucial on S3, where a
%% directory with no objects under its prefix would itself become unobservable)
%% and the sibling intact. Unlike the whole-(sub)tree deletions above, here the
%% space root is untouched (its direct children are unchanged) - it is the parent
%% DIRECTORY that is reclassified: modified on POSIX (the deletion bumps its
%% mtime), not tracked at all on S3 (emulated directories have no statbuf).
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

    %% delete just one of the directory's two files directly on the storage
    #file_spec{name = DeletedFileName, content = DeletedContent} = DeletedFileSpec,
    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_file_setup_utils:delete_file(
        ImportingProviderSelector, ImportedStorageId,
        filepath_utils:join([<<"/">>, DirName, DeletedFileName]), byte_size(DeletedContent)
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% the directory and its surviving file remain; only the deleted file is gone
    storage_import_test_utils:verify_imported_tree(
        TestCaseCtx, #dir_spec{name = DirName, children = [KeptFileSpec]}
    ),

    %% scan 1 created the directory + its 2 files on POSIX, only the 2 files on S3
    Scan1Created = storage_import_test_utils:expected_created_count(TestCaseCtx),
    %% POSIX: the parent directory is modified (its child was removed); S3: the
    %% emulated directory is not tracked, so nothing is classified as modified
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


%% A large nested tree imported by the initial scan is removed wholesale directly
%% on the storage; the next (continuous) scan must detect and propagate every
%% single deletion (and the dir stats must reflect the now-empty space). Exercises
%% the deletion path at scale.
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

    %% delete the whole tree directly on the storage
    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_import_test_utils:delete_file_tree_from_storage(
        ImportingProviderSelector, ImportedStorageId, FileTreeSpec
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2, #{}, ?LARGE_IMPORT_SCAN_ATTEMPTS),

    %% every entry is gone from the space and the space root's dir stats are zeroed
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []),
    storage_import_test_utils:verify_dir_stats(TestCaseCtx, []),

    %% all 281 logical entries (250 files + 31 dirs) are deleted, on both storages;
    %% scan 1 created only the 250 files on S3 (its emulated dirs are not storage entries)
    Scan1Created = storage_import_test_utils:expected_created_count(TestCaseCtx),
    Deleted = storage_import_test_utils:expected_deleted_count(TestCaseCtx),
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => Deleted,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        %% at this scale the early created/deleted events may age out of the 60 s
        %% Min histogram span, so only Hour/Day are asserted (Min => skip vent)
        created_hist => Scan1Created,
        <<"createdMinHist">> => skip,
        modified_hist => RootModified,
        deleted_hist => Deleted,
        <<"deletedMinHist">> => skip
    }).
