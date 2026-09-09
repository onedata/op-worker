%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains base test functions for testing how storage import
%%% behaves when a scan RACES with concurrent filesystem activity: LFM
%%% operations and remote-provider changes interleaved with the scan's own
%%% steps (listing the storage, importing an entry, checking an entry's
%%% deletion marker or location, detecting deletions), plus time warps around
%%% the periodic scan scheduler.
%%%
%%% Most scenarios inject the race deterministically: a mock installed on the
%%% importing provider parks the scanning process at the exact step under test
%%% (see block_scan_process/1), the test performs the racing operation, and
%%% only then lets the scan resume. The remaining scenarios pre-arrange a
%%% half-synchronized state (a dbsync-ed link without its file_meta document,
%%% or a link deletion without the rest of the file's removal) that the scan
%%% then faces as a whole.
%%%
%%% The generic machinery lives in storage_import_test_utils, whose module doc
%%% is also the CANONICAL description of scan classification, monitoring
%%% counters, flat-storage divergences and root-verdict races. Docs below
%%% assume those general facts throughout and note only what is specific to
%%% their own scenario.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_race_test_base).
-author("Bartosz Walkowicz").

-include("storage_import_test.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/errno.hrl").

% API
-export([
    init_per_testcase/3,
    end_per_testcase/3
]).

%% tests
-export([
    create_remote_file_import_conflict_test/1,
    create_remote_dir_import_race_test/1,
    create_remote_file_import_race_test/1,
    create_file_import_race_test/1,
    close_file_import_race_test/1,
    delete_file_reimport_race_test/1,
    remote_delete_file_reimport_race_test/1,
    remote_delete_file_reimport_race2_test/1,
    delete_opened_file_reimport_race_test/1,
    create_delete_race_test/1,
    create_list_race_test/1,

    time_warp_between_scans_test/1,
    time_warp_during_scan_test/1
]).

%% Scan interval used by the time warp tests - long enough (compared to the 1 s
%% scheduler check resolution) for "no scan starts within the interval" phases
%% to be meaningfully observable.
-define(SCAN_INTERVAL_SECONDS, 10).


%%%===================================================================
%%% SetUp and TearDown
%%%===================================================================


init_per_testcase(Case = create_list_race_test, TestSuiteCtx = #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector
}, Config) ->
    [Node | _] = Nodes = oct_background:get_provider_nodes(ImportingProviderSelector),
    {ok, OldDirBatchSize} = test_utils:get_env(Node, op_worker, storage_import_dir_batch_size),
    %% a batch size below the test's file count (3) makes the space root's
    %% listing span multiple batches, opening the between-batches window the
    %% create-list race needs
    ok = test_utils:set_env(Nodes, op_worker, storage_import_dir_batch_size, 2),
    Config2 = [{old_storage_import_dir_batch_size, OldDirBatchSize} | Config],
    init_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config2);

init_per_testcase(Case, TestSuiteCtx, Config) when
    Case =:= close_file_import_race_test;
    Case =:= delete_opened_file_reimport_race_test
->
    storage_import_test_utils:mock_opened_file_deletion_to_use_deletion_marker(TestSuiteCtx),
    init_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

init_per_testcase(_Case, _TestSuiteCtx, Config) ->
    lfm_proxy:init(Config).


end_per_testcase(Case, TestSuiteCtx, Config) when
    Case =:= create_file_import_race_test;
    Case =:= delete_file_reimport_race_test
->
    storage_import_test_utils:unmock_storage_import_engine(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case = close_file_import_race_test, TestSuiteCtx, Config) ->
    unmock_deletion_marker(TestSuiteCtx),
    storage_import_test_utils:unmock_fslogic_delete(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case = delete_opened_file_reimport_race_test, TestSuiteCtx, Config) ->
    storage_import_test_utils:unmock_storage_import_engine(TestSuiteCtx),
    storage_import_test_utils:unmock_fslogic_delete(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case = create_delete_race_test, TestSuiteCtx, Config) ->
    unmock_storage_import_deletion(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case = create_list_race_test, TestSuiteCtx = #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector
}, Config) ->
    %% on the happy path the listing mock is already unloaded inline (before
    %% the test's final scan); this matters only when the test fails earlier
    storage_import_test_utils:unmock_storage_driver(TestSuiteCtx),
    Nodes = oct_background:get_provider_nodes(ImportingProviderSelector),
    OldDirBatchSize = ?config(old_storage_import_dir_batch_size, Config),
    ok = test_utils:set_env(Nodes, op_worker, storage_import_dir_batch_size, OldDirBatchSize),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case = time_warp_between_scans_test, TestSuiteCtx, Config) ->
    unfreeze_time_on_importing_provider(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(Case = time_warp_during_scan_test, TestSuiteCtx, Config) ->
    unfreeze_time_on_importing_provider(TestSuiteCtx),
    storage_import_test_utils:unmock_storage_import_engine(TestSuiteCtx),
    end_per_testcase(?DEFAULT_CASE(Case), TestSuiteCtx, Config);

end_per_testcase(_Case, _TestSuiteCtx, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Tests
%%%===================================================================


%% A file created in the space via the remote (non-importing) provider - fully
%% dbsync-ed, but with its data still remote-only - and a same-named file
%% created directly on the imported storage must BOTH survive the scan facing
%% this conflict: the remote file keeps its plain name, while the storage file
%% (whose name is already taken in the space) is imported under a name with the
%% imported-conflicting suffix. Both files, with their respective contents,
%% must end up visible on both providers.
create_remote_file_import_conflict_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,
    FileName = ?RAND_STR(),
    RemoteContent = ?RAND_STR(),
    StorageContent = ?RAND_STR(),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        },
        non_importing_provider_ctx = NonImportingProviderCtx = #provider_ctx{
            node = NonImportingProviderNode,
            session_id = NonImportingProviderSessionId
        }
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, undefined, SuiteCtx
    ),

    #object{guid = RemoteFileGuid} = storage_import_test_utils:create_file_tree_via_remote_provider(
        TestCaseCtx, #file_spec{name = FileName, content = RemoteContent}
    ),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId,
        filepath_utils:join([<<"/">>, FileName]), StorageContent
    ),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% the storage file is imported under the conflicting name...
    ImportedConflictingFileName = imported_conflicting_file_name(SuiteCtx, FileName),
    ImportedConflictingFilePath = filepath_utils:join([SpacePath, ImportedConflictingFileName]),
    ?assertMatch(
        {ok, #file_attr{name = ImportedConflictingFileName}},
        lfm_proxy:stat(
            ImportingProviderNode, ImportingProviderSessionId, {path, ImportedConflictingFilePath}
        ),
        ?ATTEMPTS
    ),
    storage_import_test_utils:assert_file_content(
        ImportingProviderCtx, ImportedConflictingFilePath, StorageContent
    ),
    %% ...and propagates to the non-importing provider
    ?assertMatch(
        {ok, #file_attr{}},
        lfm_proxy:stat(
            NonImportingProviderNode, NonImportingProviderSessionId,
            {path, ImportedConflictingFilePath}
        ),
        ?CROSS_PROVIDER_PROPAGATION_ATTEMPTS
    ),
    storage_import_test_utils:assert_file_content(
        NonImportingProviderCtx, ImportedConflictingFilePath, StorageContent
    ),

    %% the remote file survives untouched under its plain name
    RemoteFilePath = filepath_utils:join([SpacePath, FileName]),
    ?assertMatch({ok, #file_attr{guid = RemoteFileGuid}}, lfm_proxy:stat(
        ImportingProviderNode, ImportingProviderSessionId, {path, RemoteFilePath}
    )),
    storage_import_test_utils:assert_file_content(
        NonImportingProviderCtx, RemoteFilePath, RemoteContent
    ),
    ?assertMatch({ok, [_, _]}, lfm_proxy:get_children(
        ImportingProviderNode, ImportingProviderSessionId, {path, SpacePath}, 0, 10
    )),

    %% created => 1 (the conflicting-name import) and deleted => 0 are the crux:
    %% the remote file, having no storage counterpart, leaves no trace in the
    %% counters. The root follows the deterministic root_scan_verdict/1: the
    %% remote file's logical times, dbsync-ed verbatim, predate the (mtime-
    %% progression-guarded) storage file creation.
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"deleted">> => 0,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        created_hist => 1,
        modified_hist => RootModified
    }).


%% A directory created in the space by the remote provider races with a
%% same-named directory created directly on the imported storage - with only
%% the entry's tree LINK (not yet its file_meta document) dbsync-ed to the
%% importing provider when the scan runs. POSIX-only: object storages hold no
%% directories to import. See create_remote_entry_import_race_test_base/3.
create_remote_dir_import_race_test(SuiteCtx) ->
    create_remote_entry_import_race_test_base(?FUNCTION_NAME, SuiteCtx, dir).


%% Like create_remote_dir_import_race_test, but racing over a regular file
%% (here it is the file_location that is not yet known when the scan runs).
create_remote_file_import_race_test(SuiteCtx) ->
    create_remote_entry_import_race_test_base(?FUNCTION_NAME, SuiteCtx, file).


%% @private
%% @doc
%% Shared body of the create_remote_{dir,file}_import_race tests. An entry is
%% created in the space by the remote provider at the same time as a same-named
%% entry appears on the imported storage, and the scan runs at the moment when
%% only the entry's LINK has dbsync-ed to the importing provider (simulated by
%% planting a link with a dangling uuid directly in the space root's file_meta
%% forest on the remote provider). The scan must treat the occupied name as a
%% conflict - importing the storage entry under a name with the imported-
%% conflicting suffix, backed by a fresh file_meta document - rather than crash
%% on or merge with the half-synchronized entry.
%% @end
-spec create_remote_entry_import_race_test_base(
    atom(), storage_import_test_utils:suite_ctx(), dir | file
) ->
    ok.
create_remote_entry_import_race_test_base(TestCaseName, SuiteCtx, EntryType) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,
    EntryName = ?RAND_STR(),
    Content = ?RAND_STR(),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        },
        non_importing_provider_ctx = #provider_ctx{
            node = NonImportingProviderNode,
            session_id = NonImportingProviderSessionId
        }
    } = storage_import_test_utils:setup_and_verify_initial_import(
        TestCaseName, undefined, SuiteCtx
    ),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    StorageFileId = filepath_utils:join([<<"/">>, EntryName]),
    case EntryType of
        dir ->
            storage_file_setup_utils:create_dir(
                ImportingProviderSelector, ImportedStorageId, StorageFileId
            );
        file ->
            storage_file_setup_utils:create_file(
                ImportingProviderSelector, ImportedStorageId, StorageFileId, Content
            )
    end,

    %% pretend that only the remotely-created entry's LINK has been
    %% synchronized so far: plant it (with a dangling uuid) on the remote
    %% provider and await its dbsync to the importing one
    DanglingUuid = datastore_key:new(),
    add_space_root_child_link_via_remote_provider(TestCaseCtx, EntryName, DanglingUuid),
    ?assertMatch({ok, _, _}, get_space_root_child_link(TestCaseCtx, EntryName), ?ATTEMPTS),

    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    ImportedConflictingEntryName = imported_conflicting_file_name(SuiteCtx, EntryName),
    ImportedConflictingEntryPath = filepath_utils:join([SpacePath, ImportedConflictingEntryName]),
    {ok, #file_attr{guid = ImportedEntryGuid}} = ?assertMatch(
        {ok, #file_attr{name = ImportedConflictingEntryName}},
        lfm_proxy:stat(
            ImportingProviderNode, ImportingProviderSessionId, {path, ImportedConflictingEntryPath}
        ),
        ?ATTEMPTS
    ),
    %% the imported entry is backed by its own, fresh file_meta document - not
    %% by the half-synchronized one
    ?assertNotEqual(DanglingUuid, file_id:guid_to_uuid(ImportedEntryGuid)),
    EntryType =:= file andalso storage_import_test_utils:assert_file_content(
        ImportingProviderCtx, ImportedConflictingEntryPath, Content
    ),
    ?assertMatch(
        {ok, #file_attr{}},
        lfm_proxy:stat(
            NonImportingProviderNode, NonImportingProviderSessionId,
            {path, ImportedConflictingEntryPath}
        ),
        ?CROSS_PROVIDER_PROPAGATION_ATTEMPTS
    ),

    %% the dangling link add does not touch the root's times at all, so the
    %% root follows the deterministic root_scan_verdict/1
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"deleted">> => 0,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        created_hist => 1,
        modified_hist => RootModified
    }).


%% While the scan is importing a storage file - parked mid-import by a mock - a
%% same-named file is created (and written to) via LFM on the importing
%% provider. The resumed import must notice the now-occupied name and fall back
%% to the imported-conflicting suffix: both files, with their respective
%% contents, must end up visible on both providers. (The LFM file's own storage
%% file lands under a conflicting-suffix storage name of its own, unseen by the
%% already-done listing; created mid-scan, it must also not be mistaken for a
%% deletion - same protection as in create_delete_race_test.)
create_file_import_race_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,
    FileName = ?RAND_STR(),
    StorageContent = ?RAND_STR(),
    LfmContent = ?RAND_STR(),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        },
        non_importing_provider_ctx = NonImportingProviderCtx = #provider_ctx{
            node = NonImportingProviderNode,
            session_id = NonImportingProviderSessionId
        }
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, undefined, SuiteCtx
    ),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId,
        filepath_utils:join([<<"/">>, FileName]), StorageContent
    ),

    mock_file_import_to_block(SuiteCtx, FileName, self()),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    BlockedScanProcess = await_blocked_scan_process(),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    SpaceFilePath = filepath_utils:join([SpacePath, FileName]),
    {ok, LfmFileGuid} = lfm_proxy:create(
        ImportingProviderNode, ImportingProviderSessionId, SpaceFilePath
    ),
    lfm_test_utils:write_file(
        ImportingProviderNode, ImportingProviderSessionId, LfmFileGuid, LfmContent
    ),

    resume_scan_process(BlockedScanProcess),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),

    %% the LFM-created file keeps the plain name...
    storage_import_test_utils:assert_file_content(ImportingProviderCtx, SpaceFilePath, LfmContent),
    %% ...while the storage file is imported under the conflicting name, on
    %% both providers
    ImportedConflictingFileName = imported_conflicting_file_name(SuiteCtx, FileName),
    ImportedConflictingFilePath = filepath_utils:join([SpacePath, ImportedConflictingFileName]),
    ?assertMatch(
        {ok, #file_attr{}},
        lfm_proxy:stat(
            ImportingProviderNode, ImportingProviderSessionId, {path, ImportedConflictingFilePath}
        ),
        ?ATTEMPTS
    ),
    storage_import_test_utils:assert_file_content(
        ImportingProviderCtx, ImportedConflictingFilePath, StorageContent
    ),
    ?assertMatch(
        {ok, #file_attr{}},
        lfm_proxy:stat(
            NonImportingProviderNode, NonImportingProviderSessionId,
            {path, ImportedConflictingFilePath}
        ),
        ?CROSS_PROVIDER_PROPAGATION_ATTEMPTS
    ),
    storage_import_test_utils:assert_file_content(
        NonImportingProviderCtx, ImportedConflictingFilePath, StorageContent
    ),
    ?assertMatch({ok, [_, _]}, lfm_proxy:get_children(
        ImportingProviderNode, ImportingProviderSessionId, {path, SpacePath}, 0, 10
    )),

    %% the root's verdict was computed before the import (and thus before the
    %% racing LFM create could bump the root's logical mtime), so it follows
    %% the deterministic root_scan_verdict/1; the LFM file, materialized on the
    %% storage only after the listing, leaves no trace in the counters
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 1,
        <<"deleted">> => 0,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        created_hist => 1,
        modified_hist => RootModified
    }).


%% A file created and opened via LFM is deleted while still open - its storage
%% file survives under the plain name, guarded by a deletion marker (forced via
%% init_per_testcase's fslogic_delete mock; the natural behaviour on object
%% storages) - and the last handle is closed, completing the deferred deletion,
%% exactly while the scan is parked at checking the file's deletion marker. The
%% resumed check finds neither the marker nor the file's link (only its
%% storage_sync_info, which still records the file's guid) and must NOT
%% reimport the already-fully-deleted file.
close_file_import_race_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
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
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, undefined, SuiteCtx
    ),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    SpaceFilePath = filepath_utils:join([SpacePath, FileName]),
    {ok, {FileGuid, OpenHandle}} = lfm_proxy:create_and_open(
        ImportingProviderNode, ImportingProviderSessionId, SpaceFilePath
    ),
    {ok, _} = lfm_proxy:write(ImportingProviderNode, OpenHandle, 0, Content),
    ok = lfm_proxy:unlink(ImportingProviderNode, ImportingProviderSessionId, ?FILE_REF(FileGuid)),

    %% the file is already gone from the space, yet its (marker-guarded)
    %% storage file is still there, kept alive by the open handle
    ?assertMatch({ok, []}, lfm_proxy:get_children(
        ImportingProviderNode, ImportingProviderSessionId, {path, SpacePath}, 0, 10
    )),
    ?assertMatch({ok, _}, storage_file_setup_utils:stat(
        ImportingProviderSelector, ImportedStorageId, StorageFileId
    )),

    mock_deletion_marker_check_to_block(SuiteCtx, FileName, self()),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    BlockedScanProcess = await_blocked_scan_process(),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% closing the last handle completes the deferred deletion: both the
    %% storage file and its deletion marker are removed
    ok = lfm_proxy:close(ImportingProviderNode, OpenHandle),
    resume_scan_process(BlockedScanProcess),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []),
    ?assertMatch({error, ?ENOENT}, storage_file_setup_utils:stat(
        ImportingProviderSelector, ImportedStorageId, StorageFileId
    )),

    %% created => 0 is the crux: the fully-deleted file was not reimported (it
    %% still counts one "unmodified" - it was listed before the deletion
    %% completed). The space root, arranged via LFM, is racy on POSIX and
    %% deterministically "unmodified" on the (mocked-statbuf) flat storage.
    {Modified, Unmodified, ModifiedHist} = case StorageType of
        posix -> {{range, 0, 1}, {range, 1, 2}, {range, 0, 1}};
        s3 -> {0, 2, 0}
    end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"modified">> => Modified,
        <<"unmodified">> => Unmodified,
        modified_hist => ModifiedHist
    }).


%% A file created via LFM on the importing provider (and thus present on the
%% imported storage) is deleted via LFM exactly while the scan is parked at
%% checking the file's location - after its link was resolved, before its
%% file_location was inspected. The resumed scan must NOT reimport the file:
%% the space must stay empty on both providers.
delete_file_reimport_race_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,
    FileName = ?RAND_STR(),
    Content = ?RAND_STR(),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        }
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, undefined, SuiteCtx
    ),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    SpaceFilePath = filepath_utils:join([SpacePath, FileName]),
    {ok, FileGuid} = lfm_proxy:create(
        ImportingProviderNode, ImportingProviderSessionId, SpaceFilePath
    ),
    lfm_test_utils:write_file(
        ImportingProviderNode, ImportingProviderSessionId, FileGuid, Content
    ),
    ?assertMatch({ok, _}, storage_file_setup_utils:stat(
        ImportingProviderSelector, ImportedStorageId, filepath_utils:join([<<"/">>, FileName])
    )),

    mock_file_location_check_to_block(SuiteCtx, FileGuid, self()),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    BlockedScanProcess = await_blocked_scan_process(),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    ok = lfm_proxy:unlink(ImportingProviderNode, ImportingProviderSessionId, ?FILE_REF(FileGuid)),
    resume_scan_process(BlockedScanProcess),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []),

    %% created => 0 is the crux: the deleted file was not reimported (it still
    %% counts one "unmodified" - it was listed before the deletion). The space
    %% root, arranged via LFM, is racy on POSIX and deterministically
    %% "unmodified" on the (mocked-statbuf) flat storage.
    {Modified, Unmodified, ModifiedHist} = case StorageType of
        posix -> {{range, 0, 1}, {range, 1, 2}, {range, 0, 1}};
        s3 -> {0, 2, 0}
    end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"modified">> => Modified,
        <<"unmodified">> => Unmodified,
        modified_hist => ModifiedHist
    }).


%% A file is deleted by the REMOTE provider, and the scan runs at the moment
%% when only the deletion of the file's LINK has dbsync-ed to the importing
%% provider (simulated by removing the link directly from the file_meta forest
%% on the remote provider). This variant creates the file on the IMPORTING
%% provider (its data written to the imported storage locally). See
%% remote_delete_file_reimport_race_test_base/3.
remote_delete_file_reimport_race_test(SuiteCtx) ->
    remote_delete_file_reimport_race_test_base(?FUNCTION_NAME, SuiteCtx, importing_provider).


%% Like remote_delete_file_reimport_race_test, but the file is created on the
%% REMOTE provider (its data reaches the imported storage via replication).
remote_delete_file_reimport_race2_test(SuiteCtx) ->
    remote_delete_file_reimport_race_test_base(?FUNCTION_NAME, SuiteCtx, non_importing_provider).


%% @private
%% @doc
%% Shared body of the remote_delete_file_reimport_race tests. A file present on
%% the imported storage has its link deletion - but nothing else - already
%% dbsync-ed from the remote provider when the scan runs. The scan encounters
%% the storage file whose link is missing while its storage_sync_info still
%% records the file's guid, and must NOT reimport it (reimporting entries with
%% missing links is honoured on the initial scan only - the production default
%% for continuous scans); the storage file itself must stay untouched.
%% @end
-spec remote_delete_file_reimport_race_test_base(
    atom(), storage_import_test_utils:suite_ctx(), importing_provider | non_importing_provider
) ->
    ok.
remote_delete_file_reimport_race_test_base(TestCaseName, SuiteCtx, CreatingProvider) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,
    FileName = ?RAND_STR(),
    Content = ?RAND_STR(),
    StorageFileId = filepath_utils:join([<<"/">>, FileName]),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        importing_provider_ctx = ImportingProviderCtx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        },
        non_importing_provider_ctx = NonImportingProviderCtx
    } = storage_import_test_utils:setup_and_verify_initial_import(
        TestCaseName, undefined, SuiteCtx
    ),
    {
        #provider_ctx{node = CreatorNode, session_id = CreatorSessionId},
        #provider_ctx{node = ReplicatorNode, session_id = ReplicatorSessionId}
    } = case CreatingProvider of
        importing_provider -> {ImportingProviderCtx, NonImportingProviderCtx};
        non_importing_provider -> {NonImportingProviderCtx, ImportingProviderCtx}
    end,

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    SpaceFilePath = filepath_utils:join([SpacePath, FileName]),
    {ok, FileGuid} = lfm_proxy:create(CreatorNode, CreatorSessionId, SpaceFilePath),
    lfm_test_utils:write_file(CreatorNode, CreatorSessionId, FileGuid, Content),

    %% replicate the file to the other provider by reading it there
    {ok, ReadHandle} = ?assertMatch(
        {ok, _},
        lfm_proxy:open(ReplicatorNode, ReplicatorSessionId, ?FILE_REF(FileGuid), read),
        ?CROSS_PROVIDER_PROPAGATION_ATTEMPTS
    ),
    ?assertEqual(
        {ok, Content},
        lfm_proxy:check_size_and_read(ReplicatorNode, ReadHandle, 0, byte_size(Content)),
        ?ATTEMPTS
    ),
    ok = lfm_proxy:close(ReplicatorNode, ReadHandle),
    %% either way the file's data has reached the imported storage by now
    ?assertMatch({ok, _}, storage_file_setup_utils:stat(
        ImportingProviderSelector, ImportedStorageId, StorageFileId
    ), ?ATTEMPTS),

    %% pretend that only the deletion of the file's LINK has been synchronized:
    %% remove the link directly on the remote provider and await the removal's
    %% dbsync to the importing one
    ?assertMatch({ok, _, _}, get_space_root_child_link(TestCaseCtx, FileName)),
    remove_space_root_child_link_via_remote_provider(
        TestCaseCtx, FileName, file_id:guid_to_uuid(FileGuid)
    ),
    ?assertEqual({error, not_found}, get_space_root_child_link(TestCaseCtx, FileName), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(
        ImportingProviderNode, ImportingProviderSessionId, {path, SpaceFilePath}
    ), ?ATTEMPTS),

    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% not reimported: the space stays empty on both providers, while the
    %% storage file stays untouched
    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []),
    ?assertMatch({ok, _}, storage_file_setup_utils:stat(
        ImportingProviderSelector, ImportedStorageId, StorageFileId
    )),

    %% created => 0 is the crux (the missing-link file still counts one
    %% "unmodified"). The root: racy on POSIX when the file was written locally
    %% (LFM bumps the root's storage and logical mtimes together), deterministic
    %% when it arrived via replication (the replica's storage write restamps the
    %% root strictly after the remotely-stamped logical times); always
    %% "unmodified" on the (mocked-statbuf) flat storage.
    {Modified, Unmodified, ModifiedHist} = case {StorageType, CreatingProvider} of
        {posix, importing_provider} -> {{range, 0, 1}, {range, 1, 2}, {range, 0, 1}};
        {posix, non_importing_provider} -> {1, 1, 1};
        {s3, _} -> {0, 2, 0}
    end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"modified">> => Modified,
        <<"unmodified">> => Unmodified,
        modified_hist => ModifiedHist
    }).


%% Like delete_file_reimport_race_test, but the file is deleted while still
%% OPEN: its storage file survives under the plain name, guarded by a deletion
%% marker (forced via init_per_testcase's fslogic_delete mock; the natural
%% behaviour on object storages). The resumed scan must not reimport the
%% marker-guarded storage file, which stays alive for the still-open handle.
delete_opened_file_reimport_race_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        storage_type = StorageType,
        importing_provider_selector = ImportingProviderSelector
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
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, undefined, SuiteCtx
    ),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    SpaceFilePath = filepath_utils:join([SpacePath, FileName]),
    {ok, {FileGuid, OpenHandle}} = lfm_proxy:create_and_open(
        ImportingProviderNode, ImportingProviderSessionId, SpaceFilePath
    ),
    {ok, _} = lfm_proxy:write(ImportingProviderNode, OpenHandle, 0, Content),

    mock_file_location_check_to_block(SuiteCtx, FileGuid, self()),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    BlockedScanProcess = await_blocked_scan_process(),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    %% the still-open handle makes the LFM deletion defer: the storage file
    %% survives under its plain name, guarded by a deletion marker
    ok = lfm_proxy:unlink(ImportingProviderNode, ImportingProviderSessionId, ?FILE_REF(FileGuid)),
    resume_scan_process(BlockedScanProcess),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),

    storage_import_test_utils:verify_imported_tree(TestCaseCtx, []),
    ?assertMatch({ok, _}, storage_file_setup_utils:stat(
        ImportingProviderSelector, ImportedStorageId, StorageFileId
    )),
    ok = lfm_proxy:close(ImportingProviderNode, OpenHandle),

    %% same counter rationale as in delete_file_reimport_race_test
    {Modified, Unmodified, ModifiedHist} = case StorageType of
        posix -> {{range, 0, 1}, {range, 1, 2}, {range, 0, 1}};
        s3 -> {0, 2, 0}
    end,
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"modified">> => Modified,
        <<"unmodified">> => Unmodified,
        modified_hist => ModifiedHist
    }).


%% A file is created via LFM after the scan has already built its
%% storage_sync_links tree - here: while the deletion-detection master job is
%% parked by a mock. The resumed deletion detection, comparing the (stale)
%% links tree against the space's file_meta links, finds a file with no
%% links-tree counterpart; it must recognize it as newly created - NOT as
%% deleted from the storage - and leave it intact.
create_delete_race_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{storage_type = StorageType} = SuiteCtx,
    FileName = ?RAND_STR(),
    Content = ?RAND_STR(),

    TestCaseCtx = #storage_import_test_case_ctx{
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        }
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, undefined, SuiteCtx
    ),

    mock_deletion_detection_to_block(SuiteCtx, self()),
    open_deletion_detection_gate(TestCaseCtx),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),
    BlockedScanProcess = await_blocked_scan_process(),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),

    SpaceFilePath = filepath_utils:join([SpacePath, FileName]),
    {ok, FileGuid} = lfm_proxy:create(
        ImportingProviderNode, ImportingProviderSessionId, SpaceFilePath
    ),
    lfm_test_utils:write_file(
        ImportingProviderNode, ImportingProviderSessionId, FileGuid, Content
    ),
    resume_scan_process(BlockedScanProcess),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),

    %% the file survives, with its content, on both providers
    storage_import_test_utils:verify_imported_tree(
        TestCaseCtx, [#file_spec{name = FileName, content = Content}]
    ),

    %% deleted => 0 is the crux; the mid-scan-created file leaves no trace in
    %% the counters, and the root's verdict - fixed before the racing create by
    %% the gate-opening mtime bump - follows the deterministic root_scan_verdict/1
    {RootModified, RootUnmodified} = storage_import_test_utils:root_scan_verdict(StorageType),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"modified">> => RootModified,
        <<"unmodified">> => RootUnmodified,
        modified_hist => RootModified
    }).


%% The scan builds its storage_sync_links tree by listing the storage in
%% batches (storage_import_dir_batch_size lowered to 2 in init_per_testcase).
%% When entries are deleted while the listing is in progress, later entries
%% shift below the already-consumed listing window and may end up OMITTED from
%% the links tree - deletion detection must not mistake such a file for one
%% deleted from the storage. Choreography over 3 imported files: right after
%% the root listing's second batch is fetched (the listing mock notifies batch
%% by batch), both first-batch files are deleted - one via LFM, one directly on
%% the storage. The scan must delete NOTHING; the storage-deleted file's
%% disappearance is then picked up by the NEXT scan (while the file deleted via
%% LFM is, by then, simply gone from both the space and the storage).
create_list_race_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector
    } = SuiteCtx,
    %% deleting a file directly on the storage requires knowing its size, and
    %% which file gets deleted is decided mid-listing - hence one shared content
    Content = ?RAND_STR(),

    TestCaseCtx = #storage_import_test_case_ctx{
        imported_storage_id = ImportedStorageId,
        space_path = SpacePath,
        file_tree_spec = FileTreeSpec,
        importing_provider_ctx = ImportingProviderCtx = #provider_ctx{
            node = ImportingProviderNode,
            session_id = ImportingProviderSessionId
        },
        non_importing_provider_ctx = NonImportingProviderCtx = #provider_ctx{
            node = NonImportingProviderNode,
            session_id = NonImportingProviderSessionId
        }
    } = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME,
        [#file_spec{content = Content}, #file_spec{content = Content}, #file_spec{content = Content}],
        SuiteCtx,
        %% with the batch size (2) below the file count every extra listing
        %% pass re-runs the root's own verdict, inflating "unmodified"
        #{monitoring_overrides => #{<<"unmodified">> => skip}}
    ),

    mock_storage_listing_to_notify(SuiteCtx, self()),
    open_deletion_detection_gate(TestCaseCtx),
    storage_import_test_utils:enable_continuous_scan(TestCaseCtx),

    %% the first batch ([0, 2)) is listed and consumed as-is...
    {ListingProcess1, [FileToDeleteOnStorage, FileToDeleteViaLfm]} = await_storage_listing(0),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx),
    resume_scan_process(ListingProcess1),

    %% ...and right after the second batch ([2, 4)) is fetched - but before the
    %% scan consumes it - both first-batch files are deleted
    {ListingProcess2, _} = await_storage_listing(2),
    ok = lfm_proxy:unlink(
        ImportingProviderNode, ImportingProviderSessionId,
        {path, filepath_utils:join([SpacePath, FileToDeleteViaLfm])}
    ),
    storage_file_setup_utils:delete_file(
        ImportingProviderSelector, ImportedStorageId,
        filepath_utils:join([<<"/">>, FileToDeleteOnStorage]), byte_size(Content)
    ),
    resume_scan_process(ListingProcess2),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),

    %% deleted => 0 is the crux: the scan must have deleted nothing - neither
    %% the file whose deletion it cannot know of yet, nor (create-list race!)
    %% the third file. The modified/unmodified split is skipped: the low batch
    %% size inflates the root's re-verdicts and the racing deletions make the
    %% remaining verdicts timing-dependent.
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 2,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"modified">> => skip,
        <<"unmodified">> => skip,
        created_hist => 3,
        modified_hist => skip
    }),

    %% the storage-deleted file is still - for now - in the space, and the
    %% third file survived with its content
    [LeftFileName] = [Name || #file_spec{name = Name} <- FileTreeSpec,
        not lists:member(Name, [FileToDeleteOnStorage, FileToDeleteViaLfm])],
    FileDeletedOnStoragePath = filepath_utils:join([SpacePath, FileToDeleteOnStorage]),
    LeftFilePath = filepath_utils:join([SpacePath, LeftFileName]),
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(
        ImportingProviderNode, ImportingProviderSessionId, {path, FileDeletedOnStoragePath}
    )),
    storage_import_test_utils:assert_file_content(ImportingProviderCtx, LeftFilePath, Content),

    %% the NEXT scan finally picks up the storage-deleted file
    storage_import_test_utils:unmock_storage_driver(SuiteCtx),
    open_deletion_detection_gate(TestCaseCtx),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 3),

    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(
        ImportingProviderNode, ImportingProviderSessionId, {path, FileDeletedOnStoragePath}
    ), ?ATTEMPTS),
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(
        NonImportingProviderNode, NonImportingProviderSessionId, {path, FileDeletedOnStoragePath}
    ), ?CROSS_PROVIDER_PROPAGATION_ATTEMPTS),
    storage_import_test_utils:assert_file_content(ImportingProviderCtx, LeftFilePath, Content),
    storage_import_test_utils:assert_file_content(NonImportingProviderCtx, LeftFilePath, Content),

    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 3,
        <<"created">> => 0,
        <<"deleted">> => 1,
        <<"modified">> => skip,
        <<"unmodified">> => skip,
        %% histograms are cumulative over scans 1..3; over a three-scan run the
        %% early events may age out of the 60 s Min window
        <<"createdHourHist">> => 3,
        <<"createdDayHist">> => 3,
        <<"createdMinHist">> => {range, 0, 3},
        modified_hist => skip,
        deleted_hist => 1
    }).


%% A backward time warp BETWEEN scans must neither trigger spurious scans nor
%% stall the periodic scheduler forever: no scan may start while the
%% (warped-back) clock still reads earlier than the last scan's stop time +
%% scan_interval, and the moment it crosses that threshold the next scan runs.
time_warp_between_scans_test(SuiteCtx) ->
    TestCaseCtx = storage_import_test_utils:setup_and_verify_initial_import(
        ?FUNCTION_NAME, #file_spec{content = ?RAND_STR()}, SuiteCtx
    ),
    #{<<"scanStopTime">> := ScanStopTimeMillis} =
        storage_import_test_utils:get_storage_import_monitoring_state(TestCaseCtx),

    freeze_time_on_importing_provider(SuiteCtx),
    time_test_utils:set_current_time_seconds(
        ScanStopTimeMillis div 1000 - 3 * ?SCAN_INTERVAL_SECONDS
    ),
    storage_import_test_utils:enable_continuous_scan(
        TestCaseCtx, #{scan_interval => ?SCAN_INTERVAL_SECONDS}
    ),

    %% two (real) scan intervals pass, but the frozen clock still reads a time
    %% before the last scan's stop - no scan may have started
    timer:sleep(timer:seconds(2 * ?SCAN_INTERVAL_SECONDS)),
    assert_no_scan_in_progress(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 1
    }),

    %% one second short of the (stop time + scan_interval) threshold - still nothing
    time_test_utils:simulate_seconds_passing(4 * ?SCAN_INTERVAL_SECONDS - 1),
    timer:sleep(timer:seconds(2 * ?SCAN_INTERVAL_SECONDS)),
    assert_no_scan_in_progress(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 1
    }),

    %% crossing the threshold starts the scan
    time_test_utils:simulate_seconds_passing(2),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx).


%% A backward time warp DURING a scan: the clock is warped (far) back while the
%% scan is parked mid-import. The scan must complete sanely - the file imported
%% correctly and the scan's recorded stop time clamped to its start time (never
%% earlier) - and subsequent periodic scheduling must work off the recorded
%% times: nothing runs until the warped clock catches up with the recorded stop
%% time + scan_interval.
time_warp_during_scan_test(SuiteCtx) ->
    FileName = ?RAND_STR(),
    mock_file_import_to_block(SuiteCtx, FileName, self()),
    TestCaseCtx = storage_import_test_utils:init_testcase(
        ?FUNCTION_NAME, #file_spec{name = FileName, content = ?RAND_STR()}, SuiteCtx
    ),

    BlockedScanProcess = await_blocked_scan_process(),
    freeze_time_on_importing_provider(SuiteCtx),
    %% warp far back (the frozen clock starts off at the pre-freeze time)
    time_test_utils:set_current_time_seconds(100),
    #{<<"scanStartTime">> := ScanStartTimeMillis} =
        storage_import_test_utils:get_storage_import_monitoring_state(TestCaseCtx),
    resume_scan_process(BlockedScanProcess),

    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),
    storage_import_test_utils:verify_imported_tree(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        %% the recorded stop time must never precede the start time - with the
        %% clock warped back mid-scan it is clamped to the start time
        <<"scanStartTime">> => ScanStartTimeMillis,
        <<"scanStopTime">> => ScanStartTimeMillis
    }),

    storage_import_test_utils:enable_continuous_scan(
        TestCaseCtx, #{scan_interval => ?SCAN_INTERVAL_SECONDS}
    ),
    timer:sleep(timer:seconds(2 * ?SCAN_INTERVAL_SECONDS)),
    assert_no_scan_in_progress(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"scans">> => 1
    }),

    %% the warped clock catching up with (recorded stop time + scan_interval)
    %% starts the next scan
    time_test_utils:set_current_time_seconds(
        ScanStartTimeMillis div 1000 + ?SCAN_INTERVAL_SECONDS + 1
    ),
    storage_import_test_utils:await_scan_finished(TestCaseCtx, 2),
    storage_import_test_utils:disable_continuous_scan(TestCaseCtx).


%%%===================================================================
%%% Internal functions - shared test steps
%%%===================================================================


%% @private
%% The imported-conflicting name under which the scan imports a storage entry
%% whose plain name is already taken in the space.
-spec imported_conflicting_file_name(storage_import_test_utils:suite_ctx(), file_meta:name()) ->
    file_meta:name().
imported_conflicting_file_name(#storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector
}, FileName) ->
    ImportingProviderId = oct_background:get_provider_id(ImportingProviderSelector),
    ?IMPORTED_CONFLICTING_FILE_NAME(FileName, ImportingProviderId).


%% @private
%% @doc
%% Opens the deletion-detection gate for the next scan without touching the
%% root's children set: deletion detection for a directory runs only when its
%% storage mtime differs from the one recorded by the previous scan. On POSIX
%% the root's storage mtime is bumped to the provider's current time (a plain
%% "touch"; the preceding ensure_mtime_progression makes the new mtime strictly
%% newer than anything recorded before, so the root's "modified" verdict is
%% deterministic); on flat storages the mocked root statbuf mtime is advanced
%% instead (the root still stays "unmodified" - see root_scan_verdict/1).
%% @end
-spec open_deletion_detection_gate(storage_import_test_utils:case_ctx()) -> ok.
open_deletion_detection_gate(TestCaseCtx = #storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{
        storage_type = posix,
        importing_provider_selector = ImportingProviderSelector
    },
    imported_storage_id = ImportedStorageId
}) ->
    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    Now = ?rpc(ImportingProviderSelector, global_clock:timestamp_seconds()),
    storage_file_setup_utils:set_mtime(
        ImportingProviderSelector, ImportedStorageId, <<"/">>, Now
    );
open_deletion_detection_gate(TestCaseCtx = #storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{storage_type = s3}
}) ->
    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx).


%% @private
-spec assert_no_scan_in_progress(storage_import_test_utils:case_ctx()) -> ok.
assert_no_scan_in_progress(#storage_import_test_case_ctx{
    space_id = SpaceId,
    importing_provider_ctx = #provider_ctx{selector = ImportingProviderSelector}
}) ->
    ?assertEqual(false, ?rpc(
        ImportingProviderSelector, storage_import_monitoring:is_scan_in_progress(SpaceId)
    )),
    ok.


%%%===================================================================
%%% Internal functions - space root link manipulation
%%%===================================================================


%% @private
-spec get_space_root_child_link(storage_import_test_utils:case_ctx(), file_meta:name()) ->
    {ok, file_meta:uuid(), datastore_links:tree_id()} | {error, term()}.
get_space_root_child_link(#storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{
        importing_provider_selector = ImportingProviderSelector
    },
    space_id = SpaceId
}, ChildName) ->
    SpaceRootDirUuid = space_dir:uuid(SpaceId),
    ?rpc(ImportingProviderSelector, file_meta:get_child_uuid_and_tree_id(
        SpaceRootDirUuid, ChildName
    )).


%% @private
%% Plants a bare child link (pointing to the given, possibly dangling, uuid) in
%% the space root's file_meta forest, in the remote provider's own links tree -
%% simulating an entry whose link has already dbsync-ed while the rest of its
%% metadata has not.
-spec add_space_root_child_link_via_remote_provider(
    storage_import_test_utils:case_ctx(), file_meta:name(), file_meta:uuid()
) ->
    ok.
add_space_root_child_link_via_remote_provider(#storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{
        non_importing_provider_selector = NonImportingProviderSelector
    },
    space_id = SpaceId
}, ChildName, ChildUuid) ->
    SpaceRootDirUuid = space_dir:uuid(SpaceId),
    RemoteTreeId = oct_background:get_provider_id(NonImportingProviderSelector),
    FileMetaCtx = ?rpc(NonImportingProviderSelector, file_meta:get_ctx()),
    ?assertMatch({ok, _}, ?rpc(NonImportingProviderSelector, datastore_model:add_links(
        FileMetaCtx#{scope => SpaceId}, SpaceRootDirUuid, RemoteTreeId, {ChildName, ChildUuid}
    ))),
    ok.


%% @private
%% Removes a child link from the space root's file_meta forest on the remote
%% provider - simulating a remote deletion of which only the link's removal has
%% dbsync-ed so far.
-spec remove_space_root_child_link_via_remote_provider(
    storage_import_test_utils:case_ctx(), file_meta:name(), file_meta:uuid()
) ->
    ok.
remove_space_root_child_link_via_remote_provider(#storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{
        non_importing_provider_selector = NonImportingProviderSelector
    },
    space_id = SpaceId
}, ChildName, ChildUuid) ->
    SpaceRootDirUuid = space_dir:uuid(SpaceId),
    ok = ?rpc(NonImportingProviderSelector, file_meta_forest:delete(
        SpaceRootDirUuid, SpaceId, ChildName, ChildUuid
    )).


%%%===================================================================
%%% Internal functions - scan process blocking machinery
%%%===================================================================


%% @private
%% @doc
%% Executed INSIDE a mocked scan callback, on the importing provider: notifies
%% the test process and parks the calling scan process until the test resumes
%% it via resume_scan_process/1.
%% NOTE: for the mock's closure to be callable on the provider node this module
%% must be listed in the suite's ?LOAD_MODULES.
%% @end
-spec block_scan_process(pid()) -> ok.
block_scan_process(TestProcess) ->
    TestProcess ! {scan_process_blocked, self()},
    receive proceed -> ok end.


%% @private
-spec await_blocked_scan_process() -> pid().
await_blocked_scan_process() ->
    {scan_process_blocked, ScanProcess} = ?assertReceivedMatch(
        {scan_process_blocked, _}, timer:minutes(1)
    ),
    ScanProcess.


%% @private
-spec resume_scan_process(pid()) -> ok.
resume_scan_process(ScanProcess) ->
    ScanProcess ! proceed,
    ok.


%% @private
%% Awaits the notification (see mock_storage_listing_to_notify/2) that the
%% space root's listing batch starting at the given offset has been fetched;
%% the listing process stays parked until resumed via resume_scan_process/1.
-spec await_storage_listing(non_neg_integer()) -> {pid(), [file_meta:name()]}.
await_storage_listing(ExpectedOffset) ->
    receive
        {storage_listing, ListingProcess, ExpectedOffset, FileNames} ->
            {ListingProcess, FileNames}
    after timer:minutes(1) ->
        error({storage_listing_not_received, ExpectedOffset})
    end.


%%%===================================================================
%%% Internal functions - test case specific mocks
%%%===================================================================


%% @private
%% Parks the scanning process right before it imports the given storage file
%% (all other entries import undisturbed). Torn down via
%% storage_import_test_utils:unmock_storage_import_engine/1.
-spec mock_file_import_to_block(storage_import_test_utils:suite_ctx(), file_meta:name(), pid()) ->
    ok.
mock_file_import_to_block(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}, FileName, TestProcess) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, storage_import_engine),
    ok = test_utils:mock_expect(Nodes, storage_import_engine, import_file_unsafe,
        fun(StorageFileCtx, Info) ->
            case storage_file_ctx:get_file_name_const(StorageFileCtx) of
                FileName -> block_scan_process(TestProcess);
                _ -> ok
            end,
            % not meck:passthrough - the storage_file_ctx call above may itself hit
            % a mock (the module is mocked suite-wide on object storages) and such a
            % nested mocked call erases the process-dict current-call information
            % that meck:passthrough relies on
            apply(
                meck_util:original_name(storage_import_engine), import_file_unsafe,
                [StorageFileCtx, Info]
            )
        end
    ).


%% @private
%% Parks the scanning process right before it checks the location of the file
%% with the given guid - between resolving the file's link and inspecting its
%% file_location. Torn down via storage_import_test_utils:unmock_storage_import_engine/1.
-spec mock_file_location_check_to_block(
    storage_import_test_utils:suite_ctx(), file_id:file_guid(), pid()
) ->
    ok.
mock_file_location_check_to_block(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}, FileGuid, TestProcess) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, storage_import_engine),
    ok = test_utils:mock_expect(Nodes, storage_import_engine, check_location_and_maybe_sync,
        fun(StorageFileCtx, FileCtx, Info) ->
            case file_ctx:get_logical_guid_const(FileCtx) of
                FileGuid -> block_scan_process(TestProcess);
                _ -> ok
            end,
            meck:passthrough([StorageFileCtx, FileCtx, Info])
        end
    ).


%% @private
%% Parks the scanning process right before it checks the deletion marker of the
%% given file. Torn down via unmock_deletion_marker/1.
-spec mock_deletion_marker_check_to_block(
    storage_import_test_utils:suite_ctx(), file_meta:name(), pid()
) ->
    ok.
mock_deletion_marker_check_to_block(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}, FileName, TestProcess) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, deletion_marker),
    ok = test_utils:mock_expect(Nodes, deletion_marker, check,
        fun(ParentUuid, ChildName) ->
            case ChildName of
                FileName -> block_scan_process(TestProcess);
                _ -> ok
            end,
            meck:passthrough([ParentUuid, ChildName])
        end
    ).


%% @private
-spec unmock_deletion_marker(storage_import_test_utils:suite_ctx()) -> ok.
unmock_deletion_marker(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_unload(Nodes, deletion_marker).


%% @private
%% Parks the scanning process at the start of every deletion-detection master
%% job. Torn down via unmock_storage_import_deletion/1.
-spec mock_deletion_detection_to_block(storage_import_test_utils:suite_ctx(), pid()) -> ok.
mock_deletion_detection_to_block(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}, TestProcess) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, storage_import_deletion),
    ok = test_utils:mock_expect(Nodes, storage_import_deletion, do_master_job,
        fun(Job, Args) ->
            block_scan_process(TestProcess),
            meck:passthrough([Job, Args])
        end
    ).


%% @private
-spec unmock_storage_import_deletion(storage_import_test_utils:suite_ctx()) -> ok.
unmock_storage_import_deletion(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_unload(Nodes, storage_import_deletion).


%% @private
%% @doc
%% Mocks the storage listing operation (readdir on block storages, listobjects
%% on object storages) so that after each fetch of a space root batch the
%% listing process notifies the test with the batch's offset and (basename)
%% file names and parks until resumed via resume_scan_process/1 (see
%% await_storage_listing/1). Listings of other entries pass through
%% undisturbed. Torn down via storage_import_test_utils:unmock_storage_driver/1.
%% @end
-spec mock_storage_listing_to_notify(storage_import_test_utils:suite_ctx(), pid()) -> ok.
mock_storage_listing_to_notify(#storage_import_test_suite_ctx{
    storage_type = posix,
    importing_provider_selector = ProviderSelector
}, TestProcess) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, storage_driver),
    ok = test_utils:mock_expect(Nodes, storage_driver, readdir,
        fun(SDHandle, Offset, BatchSize) ->
            Result = meck:passthrough([SDHandle, Offset, BatchSize]),
            case SDHandle#sd_handle.file of
                <<"/">> ->
                    {ok, FileNames} = Result,
                    TestProcess ! {storage_listing, self(), Offset, FileNames},
                    receive proceed -> ok end;
                _ ->
                    ok
            end,
            Result
        end
    );
mock_storage_listing_to_notify(#storage_import_test_suite_ctx{
    storage_type = s3,
    importing_provider_selector = ProviderSelector
}, TestProcess) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, storage_driver),
    ok = test_utils:mock_expect(Nodes, storage_driver, listobjects,
        fun(SDHandle, Marker, Offset, BatchSize) ->
            Result = meck:passthrough([SDHandle, Marker, Offset, BatchSize]),
            case SDHandle#sd_handle.file of
                <<"/">> ->
                    {ok, {_NextMarker, FilesAndStats}} = Result,
                    FileNames = [filename:basename(FileId) || {FileId, _Stat} <- FilesAndStats],
                    TestProcess ! {storage_listing, self(), Offset, FileNames},
                    receive proceed -> ok end;
                _ ->
                    ok
            end,
            Result
        end
    ).


%%%===================================================================
%%% Internal functions - time manipulation
%%%===================================================================


%% @private
%% @doc
%% Freezes the global clock on the importing provider's nodes (the scan
%% scheduler and the scan monitoring only ever read the clock there); the
%% frozen time is then steered from the test via time_test_utils. Torn down via
%% unfreeze_time_on_importing_provider/1.
%% @end
-spec freeze_time_on_importing_provider(storage_import_test_utils:suite_ctx()) -> ok.
freeze_time_on_importing_provider(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = clock_freezer_mock:setup_for_ct(Nodes, [global_clock]).


%% @private
-spec unfreeze_time_on_importing_provider(storage_import_test_utils:suite_ctx()) -> ok.
unfreeze_time_on_importing_provider(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = clock_freezer_mock:teardown_for_ct(Nodes).
