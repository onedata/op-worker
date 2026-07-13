%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains base test functions for testing how storage import
%%% interacts with links (hardlinks and symlinks) living in the space.
%%%
%%% Note that both hardlinks and symlinks here are LOGICAL Onedata objects
%%% (created via lfm_proxy:make_link/make_symlink), NOT storage-level links -
%%% they exist only in the logical filesystem, with no counterpart on the
%%% imported storage. These tests assert that continuous (and initial) scans
%%% treat them correctly:
%%%  * a regular file that also has a logical hardlink must not be spuriously
%%%    deleted (nor re-imported) by a scan just because one of its references
%%%    was removed via LFM; the full matrix of {file, hardlink} x {kept,
%%%    deleted_while_open, deleted} is exercised (see hardlink_scan_test_base/4);
%%%  * a logical symlink is invisible on the storage, so a scan must never
%%%    mistake it for a deleted entry and remove it.
%%%
%%% Every removal here goes through LFM (never through the storage), so the scan
%%% must report NO deletions of its own (deleted => 0) in all cases. To keep the
%%% scan from bulk-skipping the otherwise-unchanged space root (which would make
%%% the assertions vacuous), each scenario first drops a trigger file directly
%%% on the storage (see storage_import_test_utils:create_trigger_file_on_storage/1).
%%% Since the root's classification is not what these tests are about, its
%%% inherently racy verdict (see the "Mtime granularity and root-verdict races"
%%% section of storage_import_test_utils) is asserted with tolerance
%%% (root_verdict_overrides/2, initial_scan_monitoring_overrides/1).
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_fs_links_test_base).
-author("Bartosz Walkowicz").

-include("storage_import_test.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/posix/errno.hrl").

% API
-export([
    init_per_testcase/3,
    end_per_testcase/3
]).

%% tests
-export([
    %% --- hardlinks (file x hardlink matrix) ---
    hardlink_file_kept_link_kept_test/1,
    hardlink_file_kept_link_deleted_while_open_test/1,
    hardlink_file_kept_link_deleted_test/1,
    hardlink_file_deleted_while_open_link_kept_test/1,
    hardlink_file_deleted_while_open_link_deleted_while_open_test/1,
    hardlink_file_deleted_while_open_link_deleted_test/1,
    hardlink_file_deleted_link_kept_test/1,
    hardlink_file_deleted_link_deleted_while_open_test/1,
    hardlink_file_deleted_link_deleted_test/1,

    %% --- symlinks ---
    symlink_is_ignored_by_continuous_scan_test/1
]).

%% Whether (and how) a reference is removed via LFM before the scan under test runs:
%%  * kept               - reference is left in place (must survive the scan);
%%  * deleted_while_open  - reference is unlinked while an open handle is held;
%%  * deleted            - reference is unlinked with no open handle.
%% A reference survives the scenario exactly when its mode is 'kept'.
-type deletion_mode() :: kept | deleted_while_open | deleted.


%%%===================================================================
%%% SetUp and TearDown
%%%===================================================================


init_per_testcase(_Case, _SuiteCtx, Config) ->
    lfm_proxy:init(Config).


end_per_testcase(_Case, _SuiteCtx, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Hardlink tests
%%%===================================================================


hardlink_file_kept_link_kept_test(SuiteCtx) ->
    hardlink_scan_test_base(?FUNCTION_NAME, SuiteCtx, kept, kept).

hardlink_file_kept_link_deleted_while_open_test(SuiteCtx) ->
    hardlink_scan_test_base(?FUNCTION_NAME, SuiteCtx, kept, deleted_while_open).

hardlink_file_kept_link_deleted_test(SuiteCtx) ->
    hardlink_scan_test_base(?FUNCTION_NAME, SuiteCtx, kept, deleted).

hardlink_file_deleted_while_open_link_kept_test(SuiteCtx) ->
    hardlink_scan_test_base(?FUNCTION_NAME, SuiteCtx, deleted_while_open, kept).

hardlink_file_deleted_while_open_link_deleted_while_open_test(SuiteCtx) ->
    hardlink_scan_test_base(?FUNCTION_NAME, SuiteCtx, deleted_while_open, deleted_while_open).

hardlink_file_deleted_while_open_link_deleted_test(SuiteCtx) ->
    hardlink_scan_test_base(?FUNCTION_NAME, SuiteCtx, deleted_while_open, deleted).

hardlink_file_deleted_link_kept_test(SuiteCtx) ->
    hardlink_scan_test_base(?FUNCTION_NAME, SuiteCtx, deleted, kept).

hardlink_file_deleted_link_deleted_while_open_test(SuiteCtx) ->
    hardlink_scan_test_base(?FUNCTION_NAME, SuiteCtx, deleted, deleted_while_open).

hardlink_file_deleted_link_deleted_test(SuiteCtx) ->
    hardlink_scan_test_base(?FUNCTION_NAME, SuiteCtx, deleted, deleted).


-spec hardlink_scan_test_base(
    atom(), storage_import_test_utils:suite_ctx(), deletion_mode(), deletion_mode()
) ->
    ok.
hardlink_scan_test_base(TestCaseName, SuiteCtx, FileDeletionMode, HardlinkDeletionMode) ->
    #storage_import_test_suite_ctx{storage_type = StorageType} = SuiteCtx,
    FileName = ?RAND_STR(),
    HardlinkName = ?RAND_STR(),
    Content = ?RAND_STR(),

    TestCaseCtx = #storage_import_test_case_ctx{
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{node = Node, session_id = SessId}
    } = storage_import_test_utils:setup_and_verify_initial_import(
        TestCaseName, #file_spec{name = FileName, content = Content}, SuiteCtx,
        #{monitoring_overrides => initial_scan_monitoring_overrides(StorageType)}
    ),

    FilePath = filepath_utils:join([SpacePath, FileName]),
    HardlinkPath = filepath_utils:join([SpacePath, HardlinkName]),

    {ok, #file_attr{guid = FileGuid}} = ?assertMatch(
        {ok, #file_attr{}}, lfm_proxy:stat(Node, SessId, {path, FilePath})
    ),
    {ok, #file_attr{guid = HardlinkGuid}} = lfm_proxy:make_link(Node, SessId, HardlinkPath, FileGuid),

    %% open both references up front - required for the deleted_while_open modes,
    %% harmless otherwise (closed again below unless kept open on purpose)
    {ok, FileHandle} = lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), read),
    {ok, HardlinkHandle} = lfm_proxy:open(Node, SessId, ?FILE_REF(HardlinkGuid), read),
    ?assertEqual({ok, Content}, lfm_proxy:read(Node, HardlinkHandle, 0, byte_size(Content))),

    close_if_applicable(Node, FileHandle, FileDeletionMode),
    close_if_applicable(Node, HardlinkHandle, HardlinkDeletionMode),
    unlink_if_applicable(Node, SessId, ?FILE_REF(FileGuid), FileDeletionMode),
    unlink_if_applicable(Node, SessId, ?FILE_REF(HardlinkGuid), HardlinkDeletionMode),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_import_test_utils:create_trigger_file_on_storage(TestCaseCtx),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    assert_reference_survival(Node, SessId, FilePath, FileDeletionMode),
    assert_reference_survival(Node, SessId, HardlinkPath, HardlinkDeletionMode),
    assert_hardlink_scan_monitoring_state(TestCaseCtx, StorageType, FileDeletionMode, HardlinkDeletionMode).


%%%===================================================================
%%% Symlink tests
%%%===================================================================


symlink_is_ignored_by_continuous_scan_test(SuiteCtx) ->
    #storage_import_test_suite_ctx{storage_type = StorageType} = SuiteCtx,
    SymlinkName = ?RAND_STR(),

    TestCaseCtx = #storage_import_test_case_ctx{
        space_path = SpacePath,
        importing_provider_ctx = #provider_ctx{node = Node, session_id = SessId}
    } = storage_import_test_utils:init_testcase(?FUNCTION_NAME, undefined, SuiteCtx),
    storage_import_test_utils:await_initial_scan_finished(TestCaseCtx),
    storage_import_test_utils:assert_storage_import_monitoring_state(
        TestCaseCtx, initial_scan_monitoring_overrides(StorageType)
    ),

    SymlinkPath = filepath_utils:join([SpacePath, SymlinkName]),
    {ok, _} = lfm_proxy:make_symlink(Node, SessId, SymlinkPath, <<"dummy symlink value">>),

    storage_import_test_utils:ensure_mtime_progression(TestCaseCtx),
    storage_import_test_utils:create_trigger_file_on_storage(TestCaseCtx),
    storage_import_test_utils:run_continuous_scan(TestCaseCtx, 2),

    %% the logical symlink has no storage counterpart, yet the scan must not
    %% mistake it for a deleted entry and remove it
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(Node, SessId, {path, SymlinkPath}), ?ATTEMPTS),
    assert_symlink_scan_monitoring_state(TestCaseCtx, StorageType).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec close_if_applicable(oct_background:node(), lfm:handle(), deletion_mode()) -> ok.
close_if_applicable(_Node, _Handle, deleted_while_open) ->
    ok;
close_if_applicable(Node, Handle, _) ->
    ok = lfm_proxy:close(Node, Handle).


%% @private
-spec unlink_if_applicable(
    oct_background:node(), session:id(), lfm:file_key(), deletion_mode()
) ->
    ok.
unlink_if_applicable(_Node, _SessId, _FileKey, kept) ->
    ok;
unlink_if_applicable(Node, SessId, FileKey, _) ->
    ok = lfm_proxy:unlink(Node, SessId, FileKey).


%% @private
%% A reference survives the scenario exactly when its own deletion mode is 'kept'
%% (both deleted_while_open and deleted remove it from the namespace - the former
%% via deferred deletion, so it is already ENOENT even with the handle still open).
-spec assert_reference_survival(
    oct_background:node(), session:id(), file_meta:path(), deletion_mode()
) ->
    ok.
assert_reference_survival(Node, SessId, Path, kept) ->
    ?assertMatch({ok, #file_attr{}}, lfm_proxy:stat(Node, SessId, {path, Path}), ?ATTEMPTS);
assert_reference_survival(Node, SessId, Path, _Deleted) ->
    ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(Node, SessId, {path, Path}), ?ATTEMPTS).


%% @private
%% @doc
%% Full monitoring state after the continuous scan of a hardlink scenario:
%%  * created => 1 - only the trigger file is imported; the LFM-side removals are
%%    never re-imported. created*Hist is 2 (original file @scan 1 + trigger
%%    @scan 2, both within this fast test's histogram window);
%%  * deleted => 0 (default) - the crux of these tests: every removal went
%%    through LFM, so the scan itself deletes nothing;
%%  * the space root: asserted with per-storage tolerance - see
%%    root_verdict_overrides/2;
%%  * the original file adds one "unmodified" on scan 2 when its shared storage
%%    object is still LISTED by scan 2 - and this is where POSIX and S3 diverge:
%%     - S3 (flat) keeps the object key until the last open handle closes, so a
%%       deleted_while_open reference keeps it listed; it is gone only when both
%%       references were hard-deleted;
%%     - POSIX (block) moves a deferred-deleted file out of the scanned path, so
%%       the object is listed only while at least one reference is kept.
%% @end
-spec assert_hardlink_scan_monitoring_state(
    storage_import_test_utils:case_ctx(), posix | s3, deletion_mode(), deletion_mode()
) ->
    ok.
assert_hardlink_scan_monitoring_state(TestCaseCtx, StorageType, FileDeletionMode, HardlinkDeletionMode) ->
    OriginalFileStillListed = case StorageType of
        posix -> FileDeletionMode =:= kept orelse HardlinkDeletionMode =:= kept;
        s3 -> not (FileDeletionMode =:= deleted andalso HardlinkDeletionMode =:= deleted)
    end,
    OriginalFileUnmodified = bool_to_count(OriginalFileStillListed),
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, maps:merge(
        #{
            <<"scans">> => 2,
            <<"created">> => 1,
            created_hist => 2
        },
        root_verdict_overrides(StorageType, OriginalFileUnmodified)
    )).


%% @private
%% @doc
%% Full monitoring state after the continuous scan of a symlink scenario. The
%% imported storage held nothing but the trigger file (the symlink is a purely
%% logical entry with no storage counterpart), so scan 2 imports exactly the
%% trigger (created => 1, created*Hist => 1) and deletes nothing. The only other
%% entry is the space root, asserted with per-storage tolerance (see
%% root_verdict_overrides/2 and assert_hardlink_scan_monitoring_state).
%% @end
-spec assert_symlink_scan_monitoring_state(storage_import_test_utils:case_ctx(), posix | s3) -> ok.
assert_symlink_scan_monitoring_state(TestCaseCtx, StorageType) ->
    storage_import_test_utils:assert_storage_import_monitoring_state(TestCaseCtx, maps:merge(
        #{
            <<"scans">> => 2,
            <<"created">> => 1,
            created_hist => 1
        },
        root_verdict_overrides(StorageType, 0)
    )).


%% @private
%% @doc
%% The space root's contribution to the continuous-scan (scan-2) monitoring
%% state. On POSIX the trigger file bumps the root's mtime, so the root is
%% normally classified "modified" - but the verdict is subject to the races
%% described in the "Mtime granularity and root-verdict races" section of
%% storage_import_test_utils; since the root's classification is not what these
%% tests are about, the affected fields are asserted with tolerance: modified
%% and unmodified may each go either way (they always sum to a constant, which
%% the assert map cannot express), and modified*Hist may additionally include a
%% root-modified event from a racy initial scan. On S3 the root statbuf is
%% mocked to lie in the past, so the root is deterministically "unmodified" and
%% everything is asserted exactly.
%% @end
-spec root_verdict_overrides(posix | s3, non_neg_integer()) ->
    #{binary() | atom() => integer() | {range, integer(), integer()}}.
root_verdict_overrides(posix, ExtraUnmodified) -> #{
    <<"modified">> => {range, 0, 1},
    <<"unmodified">> => {range, ExtraUnmodified, ExtraUnmodified + 1},
    modified_hist => {range, 0, 2}
};
root_verdict_overrides(s3, ExtraUnmodified) -> #{
    <<"modified">> => 0,
    <<"unmodified">> => 1 + ExtraUnmodified,
    modified_hist => 0
}.


%% @private
%% Tolerance for the initial-scan monitoring assert: on POSIX the setup race
%% (see the "Mtime granularity and root-verdict races" section of
%% storage_import_test_utils) may classify the space root "modified" instead of
%% the default "unmodified". On S3 the mocked root statbuf makes the default
%% exact.
-spec initial_scan_monitoring_overrides(posix | s3) ->
    #{binary() | atom() => integer() | {range, integer(), integer()}}.
initial_scan_monitoring_overrides(posix) -> #{
    <<"modified">> => {range, 0, 1},
    <<"unmodified">> => {range, 0, 1},
    modified_hist => {range, 0, 1}
};
initial_scan_monitoring_overrides(s3) -> #{}.


%% @private
-spec bool_to_count(boolean()) -> 0 | 1.
bool_to_count(true) -> 1;
bool_to_count(false) -> 0.
