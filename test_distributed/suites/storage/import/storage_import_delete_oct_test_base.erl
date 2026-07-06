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


%% TODO VFS-13687 port from storage_import_test_base:sync_works_properly_after_delete_test/1
import_continues_after_deletion_test(_SuiteCtx) ->
    error(not_yet_implemented).


%% TODO VFS-13687 port from storage_import_test_base:delete_and_update_files_simultaneously_update_test/1
simultaneous_deletion_and_modification_test(_SuiteCtx) ->
    error(not_yet_implemented).


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
