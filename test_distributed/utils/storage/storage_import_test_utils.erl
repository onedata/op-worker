%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Generic machinery shared by the storage import suites for writing
%%% declarative storage import tests.
%%%
%%% == Framework overview ==
%%%
%%% A test declares the file tree to be created directly on the imported
%%% storage (#dir_spec{}/#file_spec{} records); init_testcase/3,4 concretizes
%%% and creates it, then sets up a space whose support by the imported storage
%%% auto-triggers the initial scan. The helpers below await scans, verify the
%%% imported logical tree on both providers and assert the scan's monitoring
%%% counters against expectations derived from the declared tree. Continuous
%%% scan tests additionally mutate the storage in between (via
%%% storage_file_setup_utils) and rerun a scan via run_continuous_scan/2,3,4.
%%%
%%% == Usage invariants ==
%%%
%%% Rules that look incidental but are load-bearing for the suites' correctness:
%%%  * this module (together with storage_file_setup_utils) must be added to
%%%    ?LOAD_MODULES of any suite that uses it, as some routines run on the
%%%    op_worker node via rpc;
%%%  * never hand-roll enable_continuous_scan/1,2 -> await ->
%%%    disable_continuous_scan/1; use run_continuous_scan/2,3,4, which disables
%%%    continuous scanning as soon as the awaited scan STARTS. The scan interval
%%%    is 1s, so disabling only after the scan finishes races the scheduler, and
%%%    a sneaked-in extra scan resets the per-scan monitoring counters right
%%%    before the assertion;
%%%  * call ensure_mtime_progression/1 before any direct-storage mutation a
%%%    later scan must detect (on flat storage it is the only thing that opens
%%%    the deletion-detection gate - see "Flat (object) storage divergences").
%%%    In convergence-under-race awaits, call it INSIDE the retried assertion: a
%%%    concurrently running scan can consume a single pre-bump, after which the
%%%    gate never reopens;
%%%  * a meck expectation fun must not call another possibly-mocked module
%%%    before meck:passthrough/0 - the nested mock's evaluation erases meck's
%%%    process-dictionary marker and the passthrough then crashes with
%%%    'badmatch: undefined'. Call apply(meck_util:original_name(Mod), Fun, Args)
%%%    instead (see mock_import_file_error/2 for the pattern).
%%%
%%% The sections below are the CANONICAL description of the scan behaviour the
%%% storage import suites rely on - suite/test docs reference these sections by
%%% name instead of re-explaining them. NOTE: this knowledge ultimately belongs
%%% with the import engine itself (storage_import_engine, storage_sync_traverse,
%%% flat_storage_iterator); should it get documented there, these sections
%%% should shrink to the test-relevant consequences.
%%%
%%% == Scan classification and counters ==
%%%
%%% How scans classify entries and tally the monitoring counters (asserted via
%%% assert_storage_import_monitoring_state/2 - see its doc for the per-scan
%%% counter vs cumulative time-windowed histogram distinction):
%%%  * on scan 1 every declared entry is counted "created" and the space root
%%%    is the single "unmodified" entry (the assertion helper's default);
%%%  * a directory's own modified/unmodified verdict is driven solely by its
%%%    own mtime/ctime, which only a change to its DIRECT children set
%%%    (add/remove/rename/replace) bumps - a child's content/attrs change does
%%%    not; in particular, the space root flips to "modified" exactly when a
%%%    direct child of the root is added/removed (never on flat storages -
%%%    see the next section);
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
%%% == Flat (object) storage divergences ==
%%%
%%% Flat storages (object storages with no concept of directories, e.g. S3 -
%%% see flat_storage_iterator.erl) diverge from POSIX in ways that recur across
%%% the storage import suites:
%%%  * there are no real directories - an empty one has no underlying storage
%%%    object at all, so it can never be imported/observed via LFM (handled
%%%    transparently by verify_imported_tree/2, which strips such directories
%%%    from the expected tree), and directories never count towards "created"
%%%    (only regular files do). Deletion is different: a directory vanishes
%%%    from the storage exactly when its last descendant object is deleted
%%%    (there is nothing to rmdir), yet storage import still deletes the
%%%    emulated logical directory and counts it "deleted" just like on POSIX;
%%%  * the space root's own storage statbuf is permanently mocked into the
%%%    past for scan timing determinism (see
%%%    mock_space_dir_statbuf_on_flat_storage/1), so the root can never be
%%%    classified "modified" on any scan; the mocked mtime can be advanced
%%%    (still staying in the past) to open the deletion-detection gate when a
%%%    test needs it - see ensure_mtime_progression/1;
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
%%%    what its parent's shortcut skipped). Continuous-scan tests must
%%%    therefore keep modification detection enabled on flat storages (see
%%%    create_file_in_dir_exceed_batch_update_test in
%%%    storage_import_update_oct_test_base).
%%%
%%% == Mtime granularity and root-verdict races ==
%%%
%%% Storage mtimes tick with 1-second granularity and the scan's verdict for a
%%% DIRECTORY - in practice the space root - hinges on mtime comparisons at that
%%% granularity: a dir counts as "modified" when its storage mtime is strictly
%%% newer than its logical mtime (storage_import_engine:maybe_update_times/5),
%%% and deletion detection for it is gated on the storage mtime differing from
%%% the one recorded by the previous scan (the mtime gate in
%%% storage_sync_traverse:do_update_master_job/2). Two inherent races follow on
%%% POSIX (both observed in practice, at a-few-per-mille rates):
%%%  * the SETUP race (affects scan 1): the storage file tree is created after
%%%    the space dir's logical creation; if the two land in different seconds,
%%%    the initial scan classifies the space root "modified" (storage mtime >
%%%    logical mtime) instead of the usual "unmodified". It happens inside
%%%    init_testcase, so no test-body code can prevent it;
%%%  * the CONTINUOUS-SCAN race (affects scans >= 2): even when
%%%    ensure_mtime_progression/1 is called before mutating the storage, the
%%%    root's LOGICAL mtime may be stamped asynchronously (parent-time updates
%%%    from LFM operations are applied in the background, with timestamps of
%%%    their own) into the same second as the storage mutation - flipping the
%%%    root's verdict from "modified" back to "unmodified".
%%% On flat (object) storages neither race exists as long as the suite installs
%%% mock_space_dir_statbuf_on_flat_storage/1 (see the previous section).
%%%
%%% Policy: tests whose SUBJECT is the classification itself assert the root
%%% verdict exactly (accepting the residual per-mille flake); tests where the
%%% root's verdict is incidental assert the affected fields with {range, ...}
%%% tolerances instead - see root_verdict_overrides/2 in
%%% storage_import_fs_links_oct_test_base for the canonical example.
%%%
%%% == Debugging a failed test ==
%%%
%%%  * per-space import audit log:
%%%    /var/log/op_worker/storage_import/<SpaceId>.log on the provider pod; in
%%%    CT results under log_private/<pod>/op-worker/storage_import/;
%%%  * a failed case leaves its space (and any continuous scans) RUNNING on the
%%%    deployment, so the live state is inspectable: kubectl cp an escript into
%%%    the pod and run it via
%%%    'kubectl exec <pod> -c oneprovider -- /usr/sbin/op_worker escript <path>'
%%%    (escript header: '%%! -name probe@<pod-fqdn> -setcookie cluster_node
%%%    -hidden'; node name in /etc/op_worker/autogenerated.vm.args), then
%%%    rpc:call into the node to inspect file_meta, storage_sync_info,
%%%    deletion_marker, storage_import_monitoring:describe/1, etc.;
%%%  * scan counters mid-test: get_storage_import_monitoring_state/1.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_test_utils).
-author("Bartosz Walkowicz").

-include("storage_import_test.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/acl.hrl").
-include("modules/fslogic/fslogic_delete.hrl").
-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/fuse_messages.hrl").

%% API - suite/testcase setup
-export([
    clean_up_after_previous_run/2, clean_up_after_previous_run/3,
    mock_space_dir_statbuf_on_flat_storage/1, unmock_space_dir_statbuf_on_flat_storage/1,
    create_storage/3,
    advance_mocked_space_dir_mtime/3,
    init_testcase/3, init_testcase/4,
    setup_and_verify_initial_import/3, setup_and_verify_initial_import/4,
    gen_nested_tree_spec/2,
    create_file_tree_on_storage/3,
    create_file_tree_via_remote_provider/2,
    delete_file_tree_from_storage/3,
    flatten_objects/1
]).
%% API - scan control
-export([
    ensure_mtime_progression/1,
    create_trigger_file_on_storage/1,
    await_initial_scan_finished/1, await_initial_scan_finished/2,
    await_scan_finished/2, await_scan_finished/3,
    run_continuous_scan/2, run_continuous_scan/3, run_continuous_scan/4,
    enable_continuous_scan/1, enable_continuous_scan/2,
    disable_continuous_scan/1,
    force_start_auto_scan/1, force_stop_auto_scan/1
]).
%% API - verification/assertions
-export([
    verify_imported_tree/1, verify_imported_tree/2,
    verify_dir_stats/1, verify_dir_stats/2,
    assert_attrs/3, assert_attrs/4,
    assert_file_content/3,
    assert_storage_import_monitoring_state/2,
    get_storage_import_monitoring_state/1,
    expected_created_count/1, expected_deleted_count/1,
    root_scan_verdict/1
]).
%% API - ACL import machinery (mocks + expectations)
-export([
    mock_storage_file_acl/3, unmock_storage_driver/1,
    mock_luma_acl_user/2, unmock_luma/1,
    get_cdmi_acl/3, expected_imported_acl_json/3
]).
%% API - import engine mock machinery (mocks + expectations)
-export([
    mock_import_file_error/2, unmock_storage_import_engine/1,
    assert_monitoring_state_after_failed_import/1
]).
%% API - open-file deletion handling mock machinery
-export([
    mock_opened_file_deletion_to_use_deletion_marker/1, unmock_fslogic_delete/1
]).

-type suite_ctx() :: #storage_import_test_suite_ctx{}.
-type case_ctx() :: #storage_import_test_case_ctx{}.
% A single node of a declared storage file tree: either a generic onenv file/dir
% spec, or a storage-import-specific FIFO spec (created on storage but not imported).
-type file_tree_node_spec() :: onenv_file_test_utils:object_spec() | #storage_fifo_spec{}.
-type file_tree_spec() ::
    undefined
    | file_tree_node_spec()
    | [file_tree_node_spec()].

-export_type([suite_ctx/0, case_ctx/0]).

% Max number of concurrent processes used to verify the imported tree; bounds the
% load on the providers while still parallelizing the (RPC-heavy, retry-prone)
% per-node assertions - important for large trees (hundreds/thousands of nodes).
-define(VERIFY_PARALLELISM, 20).
% Max number of concurrent processes used to create/delete the top-level tree nodes
% on the storage. Only the top-level siblings are parallelized (each subtree is
% processed sequentially), so the total concurrency stays bounded by this value -
% parallelizing every level would multiply across levels and overload the provider.
-define(SETUP_PARALLELISM, 20).

% Statbuf the space root dir is mocked with on flat (object) storages - see
% mock_space_dir_statbuf_on_flat_storage/1. The mtime lies far in the past on
% purpose and can be moved forward per-space via the node_cache key below - see
% advance_mocked_space_dir_mtime/3.
-define(MOCKED_SPACE_DIR_MTIME_SHIFT_KEY(__SpaceId),
    {mocked_space_dir_mtime_shift, __SpaceId}
).
-define(MOCK_SPACE_DIR_STATBUF, #statbuf{
    st_uid = ?ROOT_UID,
    st_gid = ?ROOT_GID,
    st_mode = ?DEFAULT_DIR_PERMS bor 8#40000,
    st_mtime = 1,
    st_atime = 1,
    st_ctime = 1,
    st_size = 0
}).


%%%===================================================================
%%% API
%%%===================================================================


-spec clean_up_after_previous_run([atom()], suite_ctx()) -> ok.
clean_up_after_previous_run(AllTestCases, #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector,
    non_importing_provider_selector = NonImportingProviderSelector
}) ->
    clean_up_after_previous_run(AllTestCases, ImportingProviderSelector, NonImportingProviderSelector).


%%--------------------------------------------------------------------
%% @doc
%% Deletes every space left over by a previous run of one of the given test
%% cases (matched by the space name) together with its supporting storages on
%% the two given providers. This selector-based variant is shared with other
%% storage test suites (e.g. file_registration_test_base) that carry a different
%% suite ctx but have the same two-supporting-providers cleanup need.
%% @end
%%--------------------------------------------------------------------
-spec clean_up_after_previous_run(
    [atom()], oct_background:entity_selector(), oct_background:entity_selector()
) ->
    ok.
clean_up_after_previous_run(AllTestCases, ProviderSelector1, ProviderSelector2) ->
    lists_utils:pforeach(fun(SpaceId) ->
        delete_space_with_supporting_storages(SpaceId, ProviderSelector1, ProviderSelector2)
    end, filter_spaces_from_previous_run(AllTestCases)).


-spec init_testcase(atom(), file_tree_spec(), suite_ctx()) -> case_ctx().
init_testcase(TestCaseName, FileTreeSpec, SuiteCtx) ->
    init_testcase(TestCaseName, FileTreeSpec, SuiteCtx, #{}).


%%--------------------------------------------------------------------
%% @doc
%% Like init_testcase/3 but additionally applies the given auto storage import
%% config (e.g. #{sync_acl => true}) to the importing provider's support, so that
%% the auto-triggered initial scan runs with non-default settings. An empty map
%% keeps the onepanel defaults (equivalent to init_testcase/3).
%% @end
%%--------------------------------------------------------------------
-spec init_testcase(atom(), file_tree_spec(), suite_ctx(), map()) -> case_ctx().
init_testcase(TestCaseName, FileTreeSpec, SuiteCtx = #storage_import_test_suite_ctx{
    storage_type = StorageType,
    importing_provider_selector = ImportingProviderSelector,
    non_importing_provider_selector = NonImportingProviderSelector,
    space_owner_selector = SpaceOwnerSelector
}, AutoImportConfig) ->
    ImportedStorageId = create_storage(StorageType, ImportingProviderSelector, true),
    ConcreteFileTreeSpec = create_file_tree_on_storage(
        ImportingProviderSelector, ImportedStorageId, FileTreeSpec
    ),
    OtherStorageId = create_storage(StorageType, NonImportingProviderSelector, false),

    SpaceId = space_setup_utils:set_up_space(#space_spec{
        name = TestCaseName,
        owner = SpaceOwnerSelector,
        users = [],
        supports = [
            #support_spec{
                provider = ImportingProviderSelector,
                storage_spec = ImportedStorageId,
                size = 1000000000,
                storage_import = build_auto_storage_import_config(AutoImportConfig)
            },
            #support_spec{
                provider = NonImportingProviderSelector,
                storage_spec = OtherStorageId,
                size = 1000000000
            }
        ]
    }),
    SpaceNameBin = str_utils:to_binary(TestCaseName),

    #storage_import_test_case_ctx{
        suite_ctx = SuiteCtx,
        imported_storage_id = ImportedStorageId,
        other_storage_id = OtherStorageId,
        space_id = SpaceId,
        space_path = <<"/", SpaceNameBin/binary>>,
        file_tree_spec = ConcreteFileTreeSpec,
        importing_provider_ctx = build_provider_ctx(SpaceOwnerSelector, ImportingProviderSelector),
        non_importing_provider_ctx = build_provider_ctx(SpaceOwnerSelector, NonImportingProviderSelector)
    }.


%%--------------------------------------------------------------------
%% @doc
%% Runs the canonical initial-import preamble shared by virtually every storage
%% import test - from init_testcase/4 through asserting the initial scan's
%% monitoring counters (see the framework overview in the module doc). Returns
%% the test case context for the body to destructure and drive the
%% continuous-scan phase.
%%
%% Opts (all optional):
%%  * auto_import_config := map() - non-default auto import config for the
%%    support (e.g. #{sync_acl => true}); default #{} (onepanel defaults);
%%  * scan_attempts := non_neg_integer() - attempts awaiting the initial scan;
%%    default ?ATTEMPTS, pass ?LARGE_IMPORT_SCAN_ATTEMPTS for large trees;
%%  * verify_dir_stats := boolean() - additionally assert the space's dir_size
%%    stats after import; default false;
%%  * monitoring_overrides := map() - overrides for
%%    assert_storage_import_monitoring_state/2; default #{} (pristine initial scan).
%% @end
%%--------------------------------------------------------------------
-spec setup_and_verify_initial_import(atom(), file_tree_spec(), suite_ctx()) -> case_ctx().
setup_and_verify_initial_import(CaseName, FileTreeSpec, SuiteCtx) ->
    setup_and_verify_initial_import(CaseName, FileTreeSpec, SuiteCtx, #{}).


-spec setup_and_verify_initial_import(atom(), file_tree_spec(), suite_ctx(), map()) -> case_ctx().
setup_and_verify_initial_import(CaseName, FileTreeSpec, SuiteCtx, Opts) ->
    TestCaseCtx = init_testcase(
        CaseName, FileTreeSpec, SuiteCtx, maps:get(auto_import_config, Opts, #{})
    ),
    await_initial_scan_finished(TestCaseCtx, maps:get(scan_attempts, Opts, ?ATTEMPTS)),
    verify_imported_tree(TestCaseCtx),
    maps:get(verify_dir_stats, Opts, false) andalso verify_dir_stats(TestCaseCtx),
    assert_storage_import_monitoring_state(TestCaseCtx, maps:get(monitoring_overrides, Opts, #{})),
    TestCaseCtx.


%%--------------------------------------------------------------------
%% @doc
%% Builds a declarative spec for a regular, nested directory tree from a branching
%% list: the LAST element is the number of regular files at the leaf level, and
%% each preceding element is the number of subdirectories at that level. All leaf
%% files get the given content. E.g. gen_nested_tree_spec([13, 13, 13], C) yields
%% 13 directories, each with 13 subdirectories, each with 13 files (2379 nodes).
%% @end
%%--------------------------------------------------------------------
-spec gen_nested_tree_spec([pos_integer()], binary()) -> [onenv_file_test_utils:object_spec()].
gen_nested_tree_spec([FilesCount], FileContent) ->
    [#file_spec{content = FileContent} || _ <- lists:seq(1, FilesCount)];
gen_nested_tree_spec([DirsCount | RestBranching], FileContent) ->
    [
        #dir_spec{children = gen_nested_tree_spec(RestBranching, FileContent)}
        || _ <- lists:seq(1, DirsCount)
    ].


%%--------------------------------------------------------------------
%% @doc
%% Creates the declared file tree directly on the storage (bypassing the logical
%% filesystem) so that it can later be imported. Returns the spec with all file
%% names concretized (undefined names are replaced with random ones), so that the
%% same structure can be used for verification.
%% @end
%%--------------------------------------------------------------------
-spec create_file_tree_on_storage(oct_background:entity_selector(), storage:id(), file_tree_spec()) ->
    file_tree_spec().
create_file_tree_on_storage(_ProviderSelector, _StorageId, undefined) ->
    undefined;
create_file_tree_on_storage(ProviderSelector, StorageId, Specs) when is_list(Specs) ->
    lists_utils:pmap(fun(Spec) ->
        create_file_tree_on_storage(ProviderSelector, StorageId, Spec)
    end, Specs, ?SETUP_PARALLELISM);
create_file_tree_on_storage(ProviderSelector, StorageId, Spec) ->
    create_node_on_storage(ProviderSelector, StorageId, <<"/">>, Spec).


%%--------------------------------------------------------------------
%% @doc
%% Creates the given file tree in the space via the NON-importing (remote)
%% provider - purely logically, so nothing materializes on the imported storage
%% (a remotely-created file's data stays remote until replicated; a directory
%% gets no storage counterpart until a file is written into it on the importing
%% provider). Awaits the metadata propagation to the importing provider and
%% returns the created tree with all names concretized.
%% @end
%%--------------------------------------------------------------------
-spec create_file_tree_via_remote_provider(case_ctx(), onenv_file_test_utils:object_spec()) ->
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


%%--------------------------------------------------------------------
%% @doc
%% Removes a declared file tree directly from the storage (bypassing the logical
%% filesystem) - the inverse of create_file_tree_on_storage/3, used by continuous
%% (update) scan tests to simulate whole (sub)trees disappearing from the storage.
%% Regular files are unlinked (their size taken from the declared content) and
%% directories are removed bottom-up (the rmdir is a no-op on object storages -
%% see storage_file_setup_utils:rmdir_on_storage/2). The spec must be concretized
%% (all names filled in) - pass the file_tree_spec stored in the case ctx.
%% @end
%%--------------------------------------------------------------------
-spec delete_file_tree_from_storage(oct_background:entity_selector(), storage:id(), file_tree_spec()) ->
    ok.
delete_file_tree_from_storage(_ProviderSelector, _StorageId, undefined) ->
    ok;
delete_file_tree_from_storage(ProviderSelector, StorageId, Specs) when is_list(Specs) ->
    lists_utils:pforeach(fun(Spec) ->
        delete_file_tree_from_storage(ProviderSelector, StorageId, Spec)
    end, Specs, ?SETUP_PARALLELISM);
delete_file_tree_from_storage(ProviderSelector, StorageId, Spec) ->
    delete_node_from_storage(ProviderSelector, StorageId, <<"/">>, Spec).


%%--------------------------------------------------------------------
%% @doc
%% Flattens a created file tree (see e.g. create_file_tree_via_remote_provider/2)
%% into a list of every node - the tree's root included - tagged with its path
%% segments relative to the tree's parent.
%% @end
%%--------------------------------------------------------------------
-spec flatten_objects(onenv_file_test_utils:object()) ->
    [{[file_meta:name()], onenv_file_test_utils:object()}].
flatten_objects(Object = #object{name = Name, children = Children}) ->
    [{[Name], Object} | [
        {[Name | DescendantSegments], Descendant}
        || Child <- utils:ensure_defined(Children, []),
           {DescendantSegments, Descendant} <- flatten_objects(Child)
    ]].


%%--------------------------------------------------------------------
%% @doc
%% Awaits the completion of the initial import scan, retrying for the default
%% (?ATTEMPTS) number of seconds. For large imports (many files), where the scan
%% may take much longer, use await_initial_scan_finished/2 with a higher Attempts
%% (e.g. ?LARGE_IMPORT_SCAN_ATTEMPTS) - kept per-test so that small tests still
%% fail fast if something goes wrong.
%% @end
%%--------------------------------------------------------------------
-spec await_initial_scan_finished(case_ctx()) -> true.
await_initial_scan_finished(CaseCtx) ->
    await_initial_scan_finished(CaseCtx, ?ATTEMPTS).


-spec await_initial_scan_finished(case_ctx(), non_neg_integer()) -> true.
await_initial_scan_finished(#storage_import_test_case_ctx{
    space_id = SpaceId,
    importing_provider_ctx = #provider_ctx{selector = ImportingProviderSelector}
}, Attempts) ->
    ?assertEqual(
        true,
        catch(?rpc(ImportingProviderSelector, storage_import_monitoring:is_initial_scan_finished(SpaceId))),
        Attempts
    ).


%%--------------------------------------------------------------------
%% @doc
%% Awaits the completion of the import scan number ScanNum (1 being the initial
%% scan). Use the /3 variant with a higher Attempts for large imports.
%% @end
%%--------------------------------------------------------------------
-spec await_scan_finished(case_ctx(), non_neg_integer()) -> true.
await_scan_finished(CaseCtx, ScanNum) ->
    await_scan_finished(CaseCtx, ScanNum, ?ATTEMPTS).


-spec await_scan_finished(case_ctx(), non_neg_integer(), non_neg_integer()) -> true.
await_scan_finished(#storage_import_test_case_ctx{
    space_id = SpaceId,
    importing_provider_ctx = #provider_ctx{selector = ImportingProviderSelector}
}, ScanNum, Attempts) ->
    ?assertEqual(
        true,
        catch(?rpc(ImportingProviderSelector, storage_import_monitoring:is_scan_finished(SpaceId, ScanNum))),
        Attempts
    ).


-spec run_continuous_scan(case_ctx(), non_neg_integer()) -> ok.
run_continuous_scan(CaseCtx, ScanNum) ->
    run_continuous_scan(CaseCtx, ScanNum, #{}).


%%--------------------------------------------------------------------
%% @doc
%% Runs a single continuous scan cycle: enables continuous scanning (with the
%% given scan config overrides, if any), awaits the completion of the scan
%% number ScanNum and disables further scanning. This is the standard
%% mutate-then-rescan step of update tests; use the enable/await/disable
%% primitives directly when a scenario needs finer control (e.g. several
%% consecutive scans, or acting while a scan is in progress).
%%
%% NOTE: scanning is disabled as soon as scan ScanNum STARTS, not after it
%% finishes - the periodic scheduler starts the next scan as soon as
%% scan_interval elapses since the last scan's stop, so disabling only once the
%% finish is noticed (with awaits polling at a similar granularity) races with
%% it, and a sneaked-in scan ScanNum+1 would reset the per-scan monitoring
%% counters before the test asserts them. Disabling mid-scan is safe: a running
%% scan uses the scan config snapshot taken at its start.
%% @end
%%--------------------------------------------------------------------
-spec run_continuous_scan(case_ctx(), non_neg_integer(), map()) -> ok.
run_continuous_scan(CaseCtx, ScanNum, ConfigOverrides) ->
    run_continuous_scan(CaseCtx, ScanNum, ConfigOverrides, ?ATTEMPTS).


%% Like run_continuous_scan/3, but with an explicit attempts budget for awaiting
%% the scan's completion - use for scans over large trees.
-spec run_continuous_scan(case_ctx(), non_neg_integer(), map(), non_neg_integer()) -> ok.
run_continuous_scan(CaseCtx, ScanNum, ConfigOverrides, AwaitAttempts) ->
    enable_continuous_scan(CaseCtx, ConfigOverrides),
    await_scan_started(CaseCtx, ScanNum),
    disable_continuous_scan(CaseCtx),
    await_scan_finished(CaseCtx, ScanNum, AwaitAttempts),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Enables continuous (periodic) scanning for the space, optionally with scan
%% config overrides. Intended to be paired with await_scan_finished/2 and
%% disable_continuous_scan/1.
%% @end
%%--------------------------------------------------------------------
-spec enable_continuous_scan(case_ctx()) -> ok.
enable_continuous_scan(CaseCtx) ->
    enable_continuous_scan(CaseCtx, #{}).


-spec enable_continuous_scan(case_ctx(), map()) -> ok.
enable_continuous_scan(#storage_import_test_case_ctx{
    space_id = SpaceId,
    importing_provider_ctx = #provider_ctx{selector = ImportingProviderSelector}
}, ConfigOverrides) ->
    Config = maps:merge(#{continuous_scan => true, scan_interval => 1}, ConfigOverrides),
    ok = ?rpc(ImportingProviderSelector, storage_import:set_or_configure_auto_mode(SpaceId, Config)).


-spec disable_continuous_scan(case_ctx()) -> ok.
disable_continuous_scan(#storage_import_test_case_ctx{
    space_id = SpaceId,
    importing_provider_ctx = #provider_ctx{selector = ImportingProviderSelector}
}) ->
    ok = ?rpc(
        ImportingProviderSelector,
        storage_import:set_or_configure_auto_mode(SpaceId, #{continuous_scan => false})
    ).


%%--------------------------------------------------------------------
%% @doc
%% Forces an immediate, single scan, without enabling continuous scanning (no
%% follow-up scans will run). Await its completion with await_scan_finished/2.
%% @end
%%--------------------------------------------------------------------
-spec force_start_auto_scan(case_ctx()) -> ok.
force_start_auto_scan(#storage_import_test_case_ctx{
    space_id = SpaceId,
    importing_provider_ctx = #provider_ctx{selector = ImportingProviderSelector}
}) ->
    ok = ?rpc(ImportingProviderSelector, storage_import:start_auto_scan(SpaceId)).


%%--------------------------------------------------------------------
%% @doc
%% Aborts the currently running scan - the scan is finished immediately, with
%% the not-yet-processed entries left untouched (a later scan may pick them
%% up). Tolerates the scan being already finished (a no-op then), as stopping
%% is inherently racy with the scan's own completion.
%% @end
%%--------------------------------------------------------------------
-spec force_stop_auto_scan(case_ctx()) -> ok.
force_stop_auto_scan(#storage_import_test_case_ctx{
    space_id = SpaceId,
    importing_provider_ctx = #provider_ctx{selector = ImportingProviderSelector}
}) ->
    case ?rpc(ImportingProviderSelector, storage_import:stop_auto_scan(SpaceId)) of
        ok -> ok;
        {error, not_found} -> ok
    end.


%%--------------------------------------------------------------------
%% @doc
%% Mocks the space root dir statbuf on a flat (object) storage so that storage
%% import tests run deterministically.
%%
%% Flat storages (object storages with no concept of directories - see
%% flat_storage_iterator.erl) emulate the space root dir, and its time stats are
%% always set to the current time. This may cause storage import tests to flake,
%% as the space root dir is sometimes reported as modified (if the scan started
%% later than the times doc was created) or unmodified (if it started in the same
%% second). Mocking its time stats to a constant past value makes the outcome
%% deterministic.
%%
%% For the behavioural consequences (the root can never be classified "modified";
%% deletion detection requires advancing the mocked mtime first - see
%% advance_mocked_space_dir_mtime/3) see the "Flat (object) storage divergences"
%% section of the module doc.
%%
%% Intended to be set up once per suite (in the env posthook) and torn down in
%% end_per_suite via unmock_space_dir_statbuf_on_flat_storage/1.
%% @end
%%--------------------------------------------------------------------
-spec mock_space_dir_statbuf_on_flat_storage(oct_background:entity_selector()) -> ok.
mock_space_dir_statbuf_on_flat_storage(ImportingProviderSelector) ->
    Nodes = oct_background:get_provider_nodes(ImportingProviderSelector),
    ok = test_utils:mock_new(Nodes, storage_file_ctx),
    ok = test_utils:mock_expect(Nodes, storage_file_ctx, new_with_stat,
        fun
            (StorageFileId = <<"/">>, SpaceId, StorageId, _Stat) ->
                BaseStatbuf = #statbuf{st_mtime = BaseMtime} = ?MOCK_SPACE_DIR_STATBUF,
                MtimeShift = node_cache:get(?MOCKED_SPACE_DIR_MTIME_SHIFT_KEY(SpaceId), 0),
                storage_file_ctx:new_with_stat(
                    StorageFileId, <<>>, SpaceId, StorageId,
                    BaseStatbuf#statbuf{st_mtime = BaseMtime + MtimeShift}
                );
            (StorageFileId, SpaceId, StorageId, Stat) ->
                meck:passthrough([StorageFileId, SpaceId, StorageId, Stat])
        end
    ).


%%--------------------------------------------------------------------
%% @doc
%% Moves the mocked flat-storage space root dir mtime (see
%% mock_space_dir_statbuf_on_flat_storage/1) forward by the given number of
%% seconds. The next scan then sees the root mtime as changed, which opens the
%% deletion-detection gate; the mtime still lies far in the past, so the root
%% keeps being classified "unmodified" (see the "Flat (object) storage
%% divergences" section of the module doc).
%% @end
%%--------------------------------------------------------------------
-spec advance_mocked_space_dir_mtime(
    oct_background:entity_selector(), od_space:id(), time:seconds()
) ->
    ok.
advance_mocked_space_dir_mtime(ImportingProviderSelector, SpaceId, Seconds) ->
    Key = ?MOCKED_SPACE_DIR_MTIME_SHIFT_KEY(SpaceId),
    lists:foreach(fun(Node) ->
        CurrentShift = ?rpc(Node, node_cache:get(Key, 0)),
        ok = ?rpc(Node, node_cache:put(Key, CurrentShift + Seconds))
    end, oct_background:get_provider_nodes(ImportingProviderSelector)).


%%--------------------------------------------------------------------
%% @doc
%% Guarantees that storage changes made after this call are perceived by the
%% next scan in a strictly later storage-mtime tick than anything recorded by
%% the previous scan. Both the dir-modified classification and the
%% deletion-detection gate compare mtimes recorded with 1-second granularity,
%% so without this call a mutation landing in the same second as the previous
%% scan's stat may go unnoticed (see the "mtime granularity and root-verdict
%% races" section of the module doc).
%%  * on posix (block) storages mtimes are real - sleep out the granularity;
%%  * on flat (object) storages the space root statbuf is mocked to a constant
%%    (see mock_space_dir_statbuf_on_flat_storage/1) and real time plays no
%%    role - the mocked mtime is advanced explicitly instead.
%% Call it in every test that mutates the imported storage between scans, right
%% before the mutation.
%% @end
%%--------------------------------------------------------------------
-spec ensure_mtime_progression(case_ctx()) -> ok.
ensure_mtime_progression(#storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{storage_type = posix}
}) ->
    timer:sleep(timer:seconds(1));
ensure_mtime_progression(#storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{storage_type = s3},
    space_id = SpaceId,
    importing_provider_ctx = #provider_ctx{selector = ImportingProviderSelector}
}) ->
    advance_mocked_space_dir_mtime(ImportingProviderSelector, SpaceId, 1).


%%--------------------------------------------------------------------
%% @doc
%% Creates a small file directly on the imported storage so that the next scan
%% has real work to do: the new file changes the space root's children-attrs
%% batch hash (and, on POSIX, the root's mtime), forcing the scan to re-examine
%% the root's children individually instead of bulk-skipping the
%% otherwise-unchanged batch - without it, tests whose only storage change went
%% through LFM (links) or targeted entries other than the root's direct children
%% would pass vacuously. Returns the (random) name of the created file.
%% @end
%%--------------------------------------------------------------------
-spec create_trigger_file_on_storage(case_ctx()) -> binary().
create_trigger_file_on_storage(#storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{importing_provider_selector = ImportingProviderSelector},
    imported_storage_id = ImportedStorageId
}) ->
    TriggerFileName = ?RAND_STR(),
    storage_file_setup_utils:create_file(
        ImportingProviderSelector, ImportedStorageId,
        filepath_utils:join([<<"/">>, TriggerFileName]), ?RAND_STR()
    ),
    TriggerFileName.


-spec unmock_space_dir_statbuf_on_flat_storage(oct_background:entity_selector()) -> ok.
unmock_space_dir_statbuf_on_flat_storage(ImportingProviderSelector) ->
    Nodes = oct_background:get_provider_nodes(ImportingProviderSelector),
    ok = test_utils:mock_unload(Nodes, storage_file_ctx).


%%--------------------------------------------------------------------
%% @doc
%% Generically verifies that the declared file tree was imported into the logical
%% filesystem - checked on both the importing and the non-importing provider.
%% For each declared node it asserts its type, and additionally:
%%  * for directories - that the set of its children matches the declaration,
%%  * for regular files - that their content matches the declaration.
%% The space root's children set is asserted as well, so the verification is
%% EXACT: an entry absent from the declaration must be absent from the space
%% (in particular, verifying against [] asserts an empty space).
%% NOTE: ownership (uid/gid/owner_id) is intentionally not verified here, as the
%% expected values differ between providers and depend on the LUMA configuration;
%% such assertions are left to the individual test cases.
%% @end
%%--------------------------------------------------------------------
-spec verify_imported_tree(case_ctx()) -> ok.
verify_imported_tree(CaseCtx = #storage_import_test_case_ctx{file_tree_spec = FileTreeSpec}) ->
    verify_imported_tree(CaseCtx, FileTreeSpec).


%%--------------------------------------------------------------------
%% @doc
%% Like verify_imported_tree/1, but verifies against an explicitly provided
%% expected file tree instead of the one declared at testcase init. Intended for
%% continuous-scan scenarios, where the storage (and thus the expected imported
%% tree) is mutated between scans.
%% @end
%%--------------------------------------------------------------------
-spec verify_imported_tree(case_ctx(), file_tree_spec()) -> ok.
verify_imported_tree(#storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{storage_type = StorageType},
    space_path = SpacePath,
    importing_provider_ctx = ImportingProviderCtx,
    non_importing_provider_ctx = NonImportingProviderCtx
}, ExpectedFileTreeSpec) ->
    % strip directories unobservable on flat/object storage (see
    % filter_out_unobservable_dirs/2) so that callers do not need to special-case S3
    TopLevelSpecs = filter_out_unobservable_dirs(StorageType, to_spec_list(ExpectedFileTreeSpec)),
    % flatten the whole tree (cheap, no RPC) into a list of {Path, Spec} so that
    % per-node verification (which is RPC-heavy and may retry while data propagates
    % to the non-importing provider) can be parallelized with bounded concurrency
    AllNodes = flatten_nodes(SpacePath, TopLevelSpecs),
    % the importing provider has the tree locally once the scan is done, while the
    % non-importing one receives it via dbsync, which may still be digesting the
    % backlog of preceding tests - hence the more generous attempts budget
    lists:foreach(fun({ProviderCtx, Attempts}) ->
        % the space root is not a declared node, so assert its children separately
        assert_children(ProviderCtx, SpacePath, TopLevelSpecs, Attempts),
        lists_utils:pforeach(fun({Path, Spec}) ->
            verify_node(ProviderCtx, Path, Spec, Attempts)
        end, AllNodes, ?VERIFY_PARALLELISM)
    end, [
        {ImportingProviderCtx, ?ATTEMPTS},
        {NonImportingProviderCtx, ?CROSS_PROVIDER_PROPAGATION_ATTEMPTS}
    ]).


%%--------------------------------------------------------------------
%% @doc
%% Verifies directory size statistics (dir_size_stats / dir_stats_collector) for
%% the space root and every directory declared in the imported tree, on both the
%% importing and the non-importing provider. For each directory the expected
%% counters - aggregated recursively over its whole subtree - are derived from the
%% declared spec: number of regular files, number of directories and total
%% logical/virtual byte size. For the space root the expectation is the aggregate
%% over the whole declared tree (special dirs like trash are not counted by
%% dir_size_stats). The storage-dependent physical size is intentionally not asserted.
%%
%% NOTE: relies on dir stats collecting being enabled for the space (the default in
%% onenv environments). Call only after verify_imported_tree/1, so that the declared
%% paths are guaranteed to exist on both providers.
%% @end
%%--------------------------------------------------------------------
-spec verify_dir_stats(case_ctx()) -> ok.
verify_dir_stats(CaseCtx = #storage_import_test_case_ctx{file_tree_spec = FileTreeSpec}) ->
    verify_dir_stats(CaseCtx, FileTreeSpec).


%%--------------------------------------------------------------------
%% @doc
%% Like verify_dir_stats/1, but verifies against an explicitly provided expected
%% file tree instead of the one declared at testcase init. Intended for
%% continuous-scan scenarios, where the storage (and thus the expected dir stats)
%% is mutated between scans.
%% @end
%%--------------------------------------------------------------------
-spec verify_dir_stats(case_ctx(), file_tree_spec()) -> ok.
verify_dir_stats(#storage_import_test_case_ctx{
    space_path = SpacePath,
    importing_provider_ctx = ImportingProviderCtx,
    non_importing_provider_ctx = NonImportingProviderCtx
}, ExpectedFileTreeSpec) ->
    TopLevelSpecs = to_spec_list(ExpectedFileTreeSpec),
    % the space root is not a declared node - assert it explicitly, with the
    % expectation aggregated over the whole declared tree
    SpaceRootSpec = #dir_spec{children = TopLevelSpecs},
    %% NOTE: uncomment when debugging
%%    ct:pal("Asserting dir_size_stats for space root ~tp, expected (whole-tree) state:~n~tp", [
%%        SpacePath, expected_dir_stats(SpaceRootSpec)
%%    ]),
    DirNodes = [{SpacePath, SpaceRootSpec} | [
        {Path, Spec} || {Path, #dir_spec{} = Spec} <- flatten_nodes(SpacePath, TopLevelSpecs)
    ]],
    lists:foreach(fun(ProviderCtx) ->
        lists_utils:pforeach(fun({Path, DirSpec}) ->
            assert_dir_stats(ProviderCtx, Path, DirSpec)
        end, DirNodes, ?VERIFY_PARALLELISM)
    end, [ImportingProviderCtx, NonImportingProviderCtx]).


%%--------------------------------------------------------------------
%% @doc
%% Provider-aware assertion of selected logical file attributes at the given path.
%% ExpectedAttrs is a map of #file_attr{} field name => expected value; only the
%% provided fields are checked. The expected values are supplied by the caller, as
%% they are provider-specific and depend on the LUMA configuration (e.g. the same
%% file has different uid/gid on the importing and the non-importing provider).
%% Supported fields: owner_id, uid, gid, mode, type, size, atime, mtime.
%%
%% The /4 variant accepts a custom number of retry attempts - useful when the file
%% is not awaited via verify_imported_tree first (which retries until the tree
%% propagates), so the assertion itself must tolerate a longer dbsync propagation
%% lag to the non-importing provider.
%% @end
%%--------------------------------------------------------------------
-spec assert_attrs(#provider_ctx{}, file_meta:path(), #{atom() => term()}) -> ok.
assert_attrs(ProviderCtx, Path, ExpectedAttrs) ->
    assert_attrs(ProviderCtx, Path, ExpectedAttrs, ?ATTEMPTS).


-spec assert_attrs(#provider_ctx{}, file_meta:path(), #{atom() => term()}, non_neg_integer()) -> ok.
assert_attrs(ProviderCtx, Path, ExpectedAttrs, Attempts) ->
    %% stat once per attempt and compare all requested fields against that single
    %% snapshot - avoids a stat RPC (and a full retry budget) per field, and
    %% surfaces every mismatching field at once on failure
    ?assertEqual(
        ExpectedAttrs,
        get_file_attr_fields(ProviderCtx, Path, maps:keys(ExpectedAttrs)),
        Attempts
    ),
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Asserts that the regular file at the given path holds exactly the given
%% content, as seen by the given provider. NOTE: reading via LFM fetches the
%% file's data - on a provider not yet holding a replica this triggers an
%% actual replication.
%% @end
%%--------------------------------------------------------------------
-spec assert_file_content(#provider_ctx{}, file_meta:path(), binary()) -> ok.
assert_file_content(ProviderCtx, Path, Content) ->
    assert_file_content(ProviderCtx, Path, Content, ?ATTEMPTS).


%% @private
-spec assert_file_content(#provider_ctx{}, file_meta:path(), binary(), non_neg_integer()) -> ok.
assert_file_content(#provider_ctx{node = Node, session_id = SessId}, Path, Content, Attempts) ->
    {ok, Handle} = ?assertMatch(
        {ok, _}, lfm_proxy:open(Node, SessId, {path, Path}, read), Attempts
    ),
    % read at least 1 byte, otherwise an empty file would not be checked at all
    ReadSize = max(byte_size(Content), 1),
    ?assertEqual({ok, Content}, lfm_proxy:check_size_and_read(Node, Handle, 0, ReadSize), Attempts),
    ok = lfm_proxy:close(Node, Handle).


%%--------------------------------------------------------------------
%% @doc
%% Asserts the storage_import_monitoring counters for the space. Overrides keys
%% are binaries, matching the storage_import_monitoring:describe/1 output.
%%
%% The defaults describe a successful initial scan of the declared file tree:
%% the number of created files/dirs (and the corresponding histograms) is
%% derived from the tree, and the space root itself is expected to be counted
%% as the single "unmodified" entry (it always exists before the scan). Any
%% field can be overridden via Overrides (e.g. scans/modified/deleted for
%% continuous-scan scenarios).
%%
%% NOTE: the counters (created/modified/deleted/unmodified/failed) are per-scan
%% (reset at each scan's start), while the *Hist histograms are cumulative - the
%% whole histogram is summed for the assertion (see
%% flatten_storage_import_histograms/1), so after scan N a histogram equals the
%% total over scans 1..N and is expected to be the same at every resolution
%% (Min/Hour/Day). One caveat: a histogram only spans slot_width * 12 back from
%% its last update - for *MinHist that is a mere 60 s, so in tests where scans
%% run long (bulk scenarios) the early events may age out of even the whole
%% MinHist. Hence two special override values:
%%  * 'skip' - excludes the field from the assertion entirely (use for the Min
%%    histograms in long-running tests);
%%  * {range, Min, Max} - asserts the field falls within [Min, Max] (inclusive);
%%    preferred over 'skip' when a (looser) bound is known.
%%
%% The three resolutions of a histogram (Min/Hour/Day) are expected to be equal
%% here (the whole window is summed), so each triple can be set at once via a
%% snake_case atom SUGAR key expanded to all three binary keys:
%%  * created_hist  => V  ==  <<"createdMinHist">>/<<"createdHourHist">>/<<"createdDayHist">> => V
%%  * modified_hist => V  ==  the three <<"modified*Hist">> keys
%%  * deleted_hist  => V  ==  the three <<"deleted*Hist">> keys
%% The atom form visually flags "expands to three fields"; an explicit binary key
%% for a single resolution present in the SAME map still wins over the sugar (the
%% vent for a lone <<"createdMinHist">> => skip / {range,...} in long scans).
%% @end
%%--------------------------------------------------------------------
-spec assert_storage_import_monitoring_state(
    case_ctx(),
    #{binary() | atom() => integer() | skip | {range, integer(), integer()}}
) ->
    ok.
assert_storage_import_monitoring_state(#storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{storage_type = StorageType},
    space_id = SpaceId,
    file_tree_spec = FileTreeSpec,
    importing_provider_ctx = #provider_ctx{selector = ImportingProviderSelector}
}, Overrides0) ->
    Overrides = expand_histogram_sugar(Overrides0),
    Created = count_imported_nodes(StorageType, FileTreeSpec),
    Default = #{
        <<"scans">> => 1,
        <<"created">> => Created,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"unmodified">> => 1,
        <<"failed">> => 0,
        <<"createdMinHist">> => Created,
        <<"createdHourHist">> => Created,
        <<"createdDayHist">> => Created,
        <<"modifiedMinHist">> => 0,
        <<"modifiedHourHist">> => 0,
        <<"modifiedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    },
    Expected = maps:merge(Default, Overrides),
    %% NOTE: uncomment when debugging
%%    ct:pal("Asserting storage_import_monitoring for space ~tp, expected state:~n~tp", [
%%        SpaceId, Expected
%%    ]),
    assert_monitoring_state(ImportingProviderSelector, SpaceId, Expected, 1).


%%--------------------------------------------------------------------
%% @doc
%% Returns the current storage_import_monitoring counters for the space, with
%% the histograms flattened to their window sums - the same shape (and keys) as
%% the expectations of assert_storage_import_monitoring_state/2. For scenarios
%% asserting a relation between counters (e.g. a bound on their sum) rather
%% than exact values.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_import_monitoring_state(case_ctx()) -> #{binary() => term()}.
get_storage_import_monitoring_state(#storage_import_test_case_ctx{
    space_id = SpaceId,
    importing_provider_ctx = #provider_ctx{selector = ImportingProviderSelector}
}) ->
    flatten_storage_import_histograms(
        ?rpc(ImportingProviderSelector, storage_import_monitoring:describe(SpaceId))
    ).


%%--------------------------------------------------------------------
%% @doc
%% The number of entries the initial scan reports as "created" for this test
%% case's declared file tree: every directory and regular file on POSIX, only
%% the regular files on flat (S3) storage - see the "Flat (object) storage
%% divergences" section of the module doc. Derived from the declared tree so
%% that reshaping it never requires recomputing magic numbers.
%% @end
%%--------------------------------------------------------------------
-spec expected_created_count(case_ctx()) -> non_neg_integer().
expected_created_count(#storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{storage_type = StorageType},
    file_tree_spec = FileTreeSpec
}) ->
    count_imported_nodes(StorageType, FileTreeSpec).


%%--------------------------------------------------------------------
%% @doc
%% The number of LOGICAL entries (directories + files) this test case's declared
%% file tree represents - i.e. how many space entries a full deletion of the
%% whole tree removes, and thus reports as "deleted", on ANY storage. Unlike
%% expected_created_count/1 this always counts directories - even on flat (S3)
%% storage the emulated logical directories get deleted and counted (see the
%% "Flat (object) storage divergences" section of the module doc) - hence the
%% count is taken with the posix semantics regardless of the actual storage type.
%% @end
%%--------------------------------------------------------------------
-spec expected_deleted_count(case_ctx()) -> non_neg_integer().
expected_deleted_count(#storage_import_test_case_ctx{file_tree_spec = FileTreeSpec}) ->
    count_imported_nodes(posix, FileTreeSpec).


%%--------------------------------------------------------------------
%% @doc
%% The SPACE ROOT's own {Modified, Unmodified} verdict on a continuous scan that
%% mutates the root's set of direct children (a top-level create/delete, or the
%% trigger file): {1, 0} on POSIX (the mutation bumps the root's storage mtime;
%% call ensure_mtime_progression/1 before mutating to make this deterministic),
%% {0, 1} on flat storages (the mocked root statbuf can never look modified) -
%% see the "Flat (object) storage divergences" and "Mtime granularity and
%% root-verdict races" sections of the module doc (including the policy on when
%% to assert this exact verdict vs a tolerance).
%% @end
%%--------------------------------------------------------------------
-spec root_scan_verdict(posix | s3) -> {0 | 1, 0 | 1}.
root_scan_verdict(posix) -> {1, 0};
root_scan_verdict(s3) -> {0, 1}.


%%--------------------------------------------------------------------
%% @doc
%% Mocks the storage driver so that the given (already encoded) NFS4 ACL is
%% reported as the xattr of the given storage file during a scan; all other
%% files (including the space root) keep their real xattrs. Torn down via
%% unmock_storage_driver/1.
%% @end
%%--------------------------------------------------------------------
-spec mock_storage_file_acl(suite_ctx(), helpers:file_id(), binary()) -> ok.
mock_storage_file_acl(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}, StorageFileId, EncodedAcl) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, storage_driver),
    ok = test_utils:mock_expect(Nodes, storage_driver, getxattr, fun
        (#sd_handle{file = FileId}, _Name) when FileId =:= StorageFileId ->
            {ok, EncodedAcl};
        (Handle, Name) ->
            meck:passthrough([Handle, Name])
    end).


-spec unmock_storage_driver(suite_ctx()) -> ok.
unmock_storage_driver(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_unload(Nodes, storage_driver).


%%--------------------------------------------------------------------
%% @doc
%% Mocks LUMA so that both the file owner uid and any named ACL principal map
%% to the given Onedata user, letting an imported/synced NFS ACL be applied to
%% that user. Torn down via unmock_luma/1.
%% @end
%%--------------------------------------------------------------------
-spec mock_luma_acl_user(suite_ctx(), od_user:id()) -> ok.
mock_luma_acl_user(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}, UserId) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, [luma]),
    ok = test_utils:mock_expect(Nodes, luma, map_uid_to_onedata_user, fun(_, _, _) ->
        {ok, UserId}
    end),
    ok = test_utils:mock_expect(Nodes, luma, map_acl_user_to_onedata_user, fun(_, _) ->
        {ok, UserId}
    end).


-spec unmock_luma(suite_ctx()) -> ok.
unmock_luma(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_unload(Nodes, [luma]).


-spec get_cdmi_acl(node(), session:id(), file_meta:path()) -> {ok, json_utils:json_term()} | {error, term()}.
get_cdmi_acl(Node, SessId, Path) ->
    case lfm_proxy:get_xattr(Node, SessId, {path, Path}, <<"cdmi_acl">>) of
        {ok, #xattr{value = Value}} -> {ok, Value};
        {error, _} = Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Builds the expected cdmi_acl xattr JSON for an NFS4 ACL imported with
%% sync_acl enabled: the special principals (OWNER@/GROUP@/EVERYONE@) are kept
%% verbatim, while a named principal is rendered as "<full_name>#<user_id>" of
%% the Onedata user it was mapped to (via LUMA - see mock_luma_acl_user/2).
%% @end
%%--------------------------------------------------------------------
-spec expected_imported_acl_json(
    [#access_control_entity{}], od_user:full_name(), od_user:id()
) ->
    json_utils:json_term().
expected_imported_acl_json(Acl, MappedUserFullName, MappedUserId) ->
    lists:map(fun(#access_control_entity{
        acetype = AceType, aceflags = AceFlags, identifier = Identifier, acemask = AceMask
    }) ->
        ImportedIdentifier = case binary:last(Identifier) of
            $@ -> Identifier;
            _ -> <<MappedUserFullName/binary, "#", MappedUserId/binary>>
        end,
        #{
            <<"acetype">> => ace_mask_hex(AceType),
            <<"aceflags">> => ace_mask_hex(AceFlags),
            <<"identifier">> => ImportedIdentifier,
            <<"acemask">> => ace_mask_hex(AceMask)
        }
    end, Acl).


%% @private
-spec ace_mask_hex(non_neg_integer()) -> binary().
ace_mask_hex(Mask) ->
    <<"0x", (integer_to_binary(Mask, 16))/binary>>.


%%--------------------------------------------------------------------
%% @doc
%% Mocks a failure of importing the given file/dir: the import engine raises
%% for it, while all other entries import normally. Torn down via
%% unmock_storage_import_engine/1.
%% @end
%%--------------------------------------------------------------------
-spec mock_import_file_error(suite_ctx(), binary()) -> ok.
mock_import_file_error(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}, ErroneousFile) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, storage_import_engine),
    ok = test_utils:mock_expect(Nodes, storage_import_engine, import_file_unsafe,
        fun(StorageFileCtx, Info) ->
            case storage_file_ctx:get_file_name_const(StorageFileCtx) of
                ErroneousFile ->
                    throw(test_error);
                _ ->
                    % not meck:passthrough - the storage_file_ctx call above may
                    % itself hit a mock (the module is mocked suite-wide on object
                    % storages) and such a nested mocked call erases the process-dict
                    % current-call information that meck:passthrough relies on
                    apply(
                        meck_util:original_name(storage_import_engine), import_file_unsafe,
                        [StorageFileCtx, Info]
                    )
            end
        end
    ).


%% Tears down any storage_import_engine mock (regardless of which of the mocks
%% above installed it).
%% NOTE: tolerates the mock being already torn down (a no-op then), so it can be
%% called both inline in a test's flow and defensively in end_per_testcase.
-spec unmock_storage_import_engine(suite_ctx()) -> ok.
unmock_storage_import_engine(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_unload(Nodes, storage_import_engine).


%%--------------------------------------------------------------------
%% @doc
%% Forces the deletion-marker method of handling deletion of still-opened
%% files: the file's storage file survives under its plain name (guarded by a
%% deletion marker) until the last handle is released. Without the mock, on
%% storages whose helper supports rename (e.g. POSIX) the storage file would
%% instead be renamed away into a special directory, freeing its plain name -
%% while the occupied-name scenarios specifically exercise the plain-named
%% (marker-guarded) layout. Torn down via unmock_fslogic_delete/1.
%% @end
%%--------------------------------------------------------------------
-spec mock_opened_file_deletion_to_use_deletion_marker(suite_ctx()) -> ok.
mock_opened_file_deletion_to_use_deletion_marker(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_new(Nodes, fslogic_delete),
    ok = test_utils:mock_expect(Nodes, fslogic_delete, get_open_file_handling_method, fun(FileCtx) ->
        {?SET_DELETION_MARKER, FileCtx}
    end).


-spec unmock_fslogic_delete(suite_ctx()) -> ok.
unmock_fslogic_delete(#storage_import_test_suite_ctx{
    importing_provider_selector = ProviderSelector
}) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    ok = test_utils:mock_unload(Nodes, fslogic_delete).


%%--------------------------------------------------------------------
%% @doc
%% Asserts the monitoring state of a scan whose only processed entry failed to
%% import: nothing was created (overriding the expectations derived from the
%% declared tree), the failure is counted and the space root is (as always) the
%% single unmodified entry.
%% @end
%%--------------------------------------------------------------------
-spec assert_monitoring_state_after_failed_import(case_ctx()) -> ok.
assert_monitoring_state_after_failed_import(TestCaseCtx) ->
    assert_storage_import_monitoring_state(TestCaseCtx, #{
        <<"created">> => 0,
        <<"failed">> => 1,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0
    }).


%%%===================================================================
%%% Internal functions - scan control
%%%===================================================================


%% @private
%% Polls much more often than the usual 1s of the await/assert macros: if the
%% scan turns out so quick that it is only ever observed already finished,
%% disable_continuous_scan (called right after this await) must still land
%% within scan_interval (1s) of the scan's stop - before the scheduler tick
%% that would otherwise start the next scan.
-spec await_scan_started(case_ctx(), non_neg_integer()) -> true.
await_scan_started(#storage_import_test_case_ctx{
    space_id = SpaceId,
    importing_provider_ctx = #provider_ctx{selector = ImportingProviderSelector}
}, ScanNum) ->
    ?assertEqual(
        true,
        catch(?rpc(ImportingProviderSelector, is_scan_started(SpaceId, ScanNum))),
        10 * ?ATTEMPTS,
        100
    ).


%% @private
%% NOTE: evaluated on the op_worker node (via ?rpc).
-spec is_scan_started(od_space:id(), non_neg_integer()) -> boolean().
is_scan_started(SpaceId, ScanNum) ->
    case storage_import_monitoring:get(SpaceId) of
        {ok, SIMDoc} ->
            storage_import_monitoring:is_scan_finished(SIMDoc, ScanNum) orelse (
                storage_import_monitoring:is_scan_finished(SIMDoc, ScanNum - 1) andalso
                    storage_import_monitoring:is_scan_in_progress(SIMDoc)
            );
        {error, not_found} ->
            false
    end.


%%%===================================================================
%%% Internal functions - space/storage setup
%%%===================================================================


%% @private
%% Builds the support-time storage import config map passed to onepanel. An empty
%% auto config keeps the defaults (empty map -> auto mode, default settings); a
%% non-empty one is wrapped so that e.g. #{sync_acl => true} reaches the importing
%% scan via onepanel's auto_storage_import_config.
-spec build_auto_storage_import_config(map()) -> map().
build_auto_storage_import_config(AutoImportConfig) when map_size(AutoImportConfig) =:= 0 ->
    #{};
build_auto_storage_import_config(AutoImportConfig) ->
    #{mode => <<"auto">>, auto_storage_import_config => AutoImportConfig}.


%% @private
-spec filter_spaces_from_previous_run([atom()]) -> [od_space:id()].
filter_spaces_from_previous_run(AllTestCases) ->
    lists:filter(fun(SpaceId) ->
        SpaceDetails = ozw_test_rpc:get_space_protected_data(?ROOT, SpaceId),
        SpaceName = maps:get(<<"name">>, SpaceDetails),
        lists:member(binary_to_atom(SpaceName), AllTestCases)
    end, ozw_test_rpc:list_spaces()).


%% @private
-spec delete_space_with_supporting_storages(
    od_space:id(), oct_background:entity_selector(), oct_background:entity_selector()
) ->
    ok.
delete_space_with_supporting_storages(SpaceId, ProviderSelector1, ProviderSelector2) ->
    % a space is normally supported by exactly one storage on each of the two providers, but
    % some cases set up spaces supported by just one of them (e.g. a source-share space on
    % the non-importing provider only) - tolerate any number of storages per provider rather
    % than assuming both are present
    Storages1 = get_local_storages(ProviderSelector1, SpaceId),
    Storages2 = get_local_storages(ProviderSelector2, SpaceId),

    ok = ozw_test_rpc:delete_space(SpaceId),

    lists:foreach(fun(Storage) -> delete_storage(ProviderSelector1, Storage) end, Storages1),
    lists:foreach(fun(Storage) -> delete_storage(ProviderSelector2, Storage) end, Storages2).


%% @private
-spec get_local_storages(oct_background:entity_selector(), od_space:id()) -> [od_storage:id()].
get_local_storages(NodeSelector, SpaceId) ->
    case ?rpc(NodeSelector, space_logic:get_local_storages(SpaceId)) of
        {ok, Storages} -> Storages;
        ?ERR_SPACE_NOT_SUPPORTED_BY(_, _) -> []
    end.


%% @private
-spec delete_storage(oct_background:node_selector(), storage:id()) -> ok.
delete_storage(NodeSelector, StorageId) ->
    ?assertEqual(ok, opw_test_rpc:call(NodeSelector, storage, delete, [StorageId]), ?ATTEMPTS).


%% @private
-spec create_storage(posix | s3, oct_background:entity_selector(), boolean()) -> storage:id().
create_storage(posix, ProviderSelector, IsImported) ->
    space_setup_utils:create_storage(ProviderSelector, #posix_storage_params{
        mount_point = <<"/mnt/st_", (generator:gen_name())/binary>>,
        imported_storage = IsImported
    });
create_storage(s3, ProviderSelector, true) ->
    space_setup_utils:create_storage(ProviderSelector, #s3_storage_params{
        storage_path_type = <<"canonical">>,
        imported_storage = true,
        hostname = build_s3_hostname(ProviderSelector),
        bucket_name = ?RAND_STR(15),
        block_size = 0
    });
create_storage(s3, ProviderSelector, false) ->
    space_setup_utils:create_storage(ProviderSelector, #s3_storage_params{
        storage_path_type = <<"flat">>,
        hostname = build_s3_hostname(ProviderSelector),
        bucket_name = ?RAND_STR(15)
    }).


%% @private
-spec build_s3_hostname(oct_background:entity_selector()) -> binary().
build_s3_hostname(ProviderSelector) ->
    <<
        "volume-s3.dev-volume-s3-",
        (atom_to_binary(oct_background:to_entity_placeholder(ProviderSelector)))/binary,
        ".default:9000"
    >>.


%% @private
-spec build_provider_ctx(oct_background:entity_selector(), oct_background:entity_selector()) ->
    #provider_ctx{}.
build_provider_ctx(SpaceOwnerSelector, ProviderSelector) ->
    #provider_ctx{
        selector = ProviderSelector,
        node = oct_background:get_random_provider_node(ProviderSelector),
        session_id = oct_background:get_user_session_id(SpaceOwnerSelector, ProviderSelector)
    }.


%%%===================================================================
%%% Internal functions - file tree creation on storage
%%%===================================================================


%% @private
-spec create_node_on_storage(
    oct_background:entity_selector(), storage:id(), file_meta:path(), file_tree_node_spec()
) ->
    file_tree_node_spec().
create_node_on_storage(ProviderSelector, StorageId, ParentPath, DirSpec = #dir_spec{}) ->
    #dir_spec{name = Name, mode = Mode, uid = Uid, gid = Gid, children = Children} =
        ConcreteDirSpec = ensure_name(DirSpec),
    StorageFileId = filepath_utils:join([ParentPath, Name]),
    ok = storage_file_setup_utils:create_dir(ProviderSelector, StorageId, StorageFileId, Mode),
    maybe_chown(ProviderSelector, StorageId, StorageFileId, Uid, Gid),
    ConcreteChildren = [
        create_node_on_storage(ProviderSelector, StorageId, StorageFileId, ChildSpec)
        || ChildSpec <- Children
    ],
    ConcreteDirSpec#dir_spec{children = ConcreteChildren};

create_node_on_storage(ProviderSelector, StorageId, ParentPath, FileSpec = #file_spec{}) ->
    #file_spec{name = Name, mode = Mode, content = Content, uid = Uid, gid = Gid} =
        ConcreteFileSpec = ensure_name(FileSpec),
    StorageFileId = filepath_utils:join([ParentPath, Name]),
    ok = storage_file_setup_utils:create_file(ProviderSelector, StorageId, StorageFileId, Content, Mode),
    maybe_chown(ProviderSelector, StorageId, StorageFileId, Uid, Gid),
    ConcreteFileSpec;

create_node_on_storage(ProviderSelector, StorageId, ParentPath, FifoSpec = #storage_fifo_spec{}) ->
    #storage_fifo_spec{name = Name} = ConcreteFifoSpec = ensure_name(FifoSpec),
    StorageFileId = filepath_utils:join([ParentPath, Name]),
    ok = storage_file_setup_utils:create_fifo(ProviderSelector, StorageId, StorageFileId),
    ConcreteFifoSpec.


%% @private
-spec delete_node_from_storage(
    oct_background:entity_selector(), storage:id(), file_meta:path(), file_tree_node_spec()
) ->
    ok.
delete_node_from_storage(ProviderSelector, StorageId, ParentPath, #dir_spec{name = Name, children = Children}) ->
    StorageFileId = filepath_utils:join([ParentPath, Name]),
    % delete all children first (required on POSIX, where a non-empty directory
    % cannot be removed), then the now-empty directory itself
    lists:foreach(fun(ChildSpec) ->
        delete_node_from_storage(ProviderSelector, StorageId, StorageFileId, ChildSpec)
    end, Children),
    storage_file_setup_utils:rmdir(ProviderSelector, StorageId, StorageFileId);
delete_node_from_storage(ProviderSelector, StorageId, ParentPath, #file_spec{name = Name, content = Content}) ->
    StorageFileId = filepath_utils:join([ParentPath, Name]),
    storage_file_setup_utils:delete_file(ProviderSelector, StorageId, StorageFileId, byte_size(Content));
delete_node_from_storage(ProviderSelector, StorageId, ParentPath, #storage_fifo_spec{name = Name}) ->
    StorageFileId = filepath_utils:join([ParentPath, Name]),
    storage_file_setup_utils:delete_file(ProviderSelector, StorageId, StorageFileId, 0).


%% @private
-spec maybe_chown(
    oct_background:entity_selector(), storage:id(), helpers:file_id(),
    luma:uid() | undefined, luma:gid() | undefined
) ->
    ok.
maybe_chown(_ProviderSelector, _StorageId, _StorageFileId, undefined, _Gid) ->
    ok;
maybe_chown(_ProviderSelector, _StorageId, _StorageFileId, _Uid, undefined) ->
    ok;
maybe_chown(ProviderSelector, StorageId, StorageFileId, Uid, Gid) ->
    ok = storage_file_setup_utils:chown(ProviderSelector, StorageId, StorageFileId, Uid, Gid).


%% @private
-spec ensure_name(file_tree_node_spec()) -> file_tree_node_spec().
ensure_name(DirSpec = #dir_spec{name = undefined}) ->
    DirSpec#dir_spec{name = str_utils:rand_hex(20)};
ensure_name(FileSpec = #file_spec{name = undefined}) ->
    FileSpec#file_spec{name = str_utils:rand_hex(20)};
ensure_name(FifoSpec = #storage_fifo_spec{name = undefined}) ->
    FifoSpec#storage_fifo_spec{name = str_utils:rand_hex(20)};
ensure_name(Spec) ->
    Spec.


%%%===================================================================
%%% Internal functions - imported tree verification
%%%===================================================================


%% @private
%% @doc
%% Flattens the declared tree into a list of {AbsolutePath, Spec} for every node
%% (directories and files alike). Pure, in-process - performs no RPC - so that the
%% expensive per-node verification can then be run in parallel.
%% @end
-spec flatten_nodes(file_meta:path(), [onenv_file_test_utils:object_spec()]) ->
    [{file_meta:path(), onenv_file_test_utils:object_spec()}].
flatten_nodes(ParentPath, Specs) ->
    lists:flatmap(fun(Spec) ->
        Path = filepath_utils:join([ParentPath, spec_name(Spec)]),
        Descendants = case Spec of
            #dir_spec{children = Children} -> flatten_nodes(Path, Children);
            #file_spec{} -> []
        end,
        [{Path, Spec} | Descendants]
    end, Specs).


%% @private
%% @doc
%% Verifies a single (already located by path) node - its type and, for files, its
%% content; for directories, the set of its immediate children. Descent into the
%% children is NOT done here - flatten_nodes/2 enumerates every node as a separate
%% entry, so each is verified independently (and possibly in parallel).
%% @end
-spec verify_node(
    #provider_ctx{}, file_meta:path(), onenv_file_test_utils:object_spec(), non_neg_integer()
) ->
    ok.
verify_node(ProviderCtx, Path, #dir_spec{children = Children}, Attempts) ->
    assert_node_type(ProviderCtx, Path, ?DIRECTORY_TYPE, Attempts),
    assert_children(ProviderCtx, Path, Children, Attempts);
verify_node(ProviderCtx, Path, #file_spec{content = Content}, Attempts) ->
    assert_node_type(ProviderCtx, Path, ?REGULAR_FILE_TYPE, Attempts),
    assert_file_content(ProviderCtx, Path, Content, Attempts).


%% @private
-spec get_file_attr_fields(#provider_ctx{}, file_meta:path(), [atom()]) ->
    #{atom() => term()} | {error, term()}.
get_file_attr_fields(#provider_ctx{node = Node, session_id = SessId}, Path, Fields) ->
    case lfm_proxy:stat(Node, SessId, {path, Path}) of
        {ok, FileAttr} ->
            maps:from_list([{Field, file_attr_field(Field, FileAttr)} || Field <- Fields]);
        {error, _} = Error ->
            Error
    end.


%% @private
-spec file_attr_field(atom(), #file_attr{}) -> term().
file_attr_field(owner_id, #file_attr{owner_id = Value}) -> Value;
file_attr_field(uid, #file_attr{uid = Value}) -> Value;
file_attr_field(gid, #file_attr{gid = Value}) -> Value;
file_attr_field(mode, #file_attr{mode = Value}) -> Value;
file_attr_field(type, #file_attr{type = Value}) -> Value;
file_attr_field(size, #file_attr{size = Value}) -> Value;
file_attr_field(atime, #file_attr{atime = Value}) -> Value;
file_attr_field(mtime, #file_attr{mtime = Value}) -> Value.


%% @private
-spec assert_node_type(#provider_ctx{}, file_meta:path(), onedata_file:type(), non_neg_integer()) ->
    ok.
assert_node_type(#provider_ctx{node = Node, session_id = SessId}, Path, ExpectedType, Attempts) ->
    ?assertMatch(
        {ok, #file_attr{type = ExpectedType}},
        lfm_proxy:stat(Node, SessId, {path, Path}),
        Attempts
    ),
    ok.


%% @private
-spec assert_children(
    #provider_ctx{}, file_meta:path(), [onenv_file_test_utils:object_spec()], non_neg_integer()
) ->
    ok.
assert_children(#provider_ctx{node = Node, session_id = SessId}, ParentPath, ChildrenSpecs, Attempts) ->
    ExpectedNames = lists:sort([spec_name(ChildSpec) || ChildSpec <- ChildrenSpecs]),
    ?assertEqual(ExpectedNames, list_child_names(Node, SessId, ParentPath), Attempts),
    ok.


%% @private
-spec list_child_names(oct_background:node(), session:id(), file_meta:path()) ->
    [file_meta:name()] | {error, term()}.
list_child_names(Node, SessId, ParentPath) ->
    case lfm_proxy:get_children(Node, SessId, {path, ParentPath}, 0, 10000) of
        {ok, Children} -> lists:sort([Name || {_Guid, Name} <- Children]);
        {error, _} = Error -> Error
    end.


%% @private
-spec spec_name(onenv_file_test_utils:object_spec()) -> file_meta:name().
spec_name(#dir_spec{name = Name}) -> Name;
spec_name(#file_spec{name = Name}) -> Name.


%% @private
-spec to_spec_list(file_tree_spec()) -> [onenv_file_test_utils:object_spec()].
to_spec_list(undefined) -> [];
to_spec_list(Specs) when is_list(Specs) -> Specs;
to_spec_list(Spec) -> [Spec].


%% @private
%% @doc
%% Recursively strips directories that end up with no children at all (after
%% this same filtering is applied to their own children) - such directories are
%% unobservable on a flat/object storage (see the "Flat (object) storage
%% divergences" section of the module doc). No-op on POSIX, where real (possibly
%% empty) directories are always observable.
%% @end
-spec filter_out_unobservable_dirs(posix | s3, [onenv_file_test_utils:object_spec()]) ->
    [onenv_file_test_utils:object_spec()].
filter_out_unobservable_dirs(posix, Specs) ->
    Specs;
filter_out_unobservable_dirs(s3, Specs) ->
    lists:filtermap(fun
        (DirSpec = #dir_spec{children = Children}) ->
            case filter_out_unobservable_dirs(s3, Children) of
                [] -> false;
                FilteredChildren -> {true, DirSpec#dir_spec{children = FilteredChildren}}
            end;
        (FileSpec = #file_spec{}) ->
            {true, FileSpec}
    end, Specs).


%% @private
-spec assert_dir_stats(#provider_ctx{}, file_meta:path(), #dir_spec{}) -> ok.
assert_dir_stats(#provider_ctx{selector = Selector, node = Node, session_id = SessId}, Path, DirSpec) ->
    {ok, #file_attr{guid = Guid}} = ?assertMatch(
        {ok, _}, lfm_proxy:stat(Node, SessId, {path, Path}), ?ATTEMPTS),
    ExpectedStats = expected_dir_stats(DirSpec),
    ?assertEqual(
        ExpectedStats,
        get_current_dir_stats(Selector, Guid, maps:keys(ExpectedStats)),
        ?ATTEMPTS
    ).


%% @private
-spec get_current_dir_stats(oct_background:entity_selector(), file_id:file_guid(), [binary()]) ->
    #{binary() => integer()} | {error, term()}.
get_current_dir_stats(Selector, Guid, StatNames) ->
    case ?rpc(Selector, dir_size_stats:get_stats(Guid)) of
        {ok, Stats} -> maps:with(StatNames, Stats);
        {error, _} = Error -> Error
    end.


%% @private
-spec expected_dir_stats(#dir_spec{}) -> #{binary() => integer()}.
expected_dir_stats(#dir_spec{children = Children}) ->
    {RegFileCount, DirCount, TotalSize} = aggregate_subtree_stats(Children),
    #{
        ?REG_FILE_AND_LINK_COUNT => RegFileCount,
        ?DIR_COUNT => DirCount,
        ?FILE_ERROR_COUNT => 0,
        ?DIR_ERROR_COUNT => 0,
        ?VIRTUAL_SIZE => TotalSize,
        ?LOGICAL_SIZE => TotalSize
    }.


%% @private
%% Aggregates a directory's children recursively into {reg_file_count, dir_count,
%% total_byte_size} - the counters maintained by dir_size_stats for that directory
%% (which cover the whole subtree but not the directory itself).
-spec aggregate_subtree_stats([#dir_spec{} | #file_spec{}]) ->
    {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
aggregate_subtree_stats(Children) ->
    lists:foldl(fun
        (#dir_spec{children = GrandChildren}, {RegAcc, DirAcc, SizeAcc}) ->
            {RegSub, DirSub, SizeSub} = aggregate_subtree_stats(GrandChildren),
            {RegAcc + RegSub, DirAcc + 1 + DirSub, SizeAcc + SizeSub};
        (#file_spec{content = Content}, {RegAcc, DirAcc, SizeAcc}) ->
            {RegAcc + 1, DirAcc, SizeAcc + byte_size(Content)}
    end, {0, 0, 0}, Children).


%% @private
%% Counts the storage entries that storage import reports as "created" for the
%% declared file tree - see expected_created_count/1. FIFOs are created on the
%% storage but never imported, hence never counted.
-spec count_imported_nodes(posix | s3, file_tree_spec()) -> non_neg_integer().
count_imported_nodes(_StorageType, undefined) -> 0;
count_imported_nodes(StorageType, Specs) when is_list(Specs) ->
    lists:sum([count_imported_nodes(StorageType, Spec) || Spec <- Specs]);
count_imported_nodes(posix, #dir_spec{children = Children}) -> 1 + count_imported_nodes(posix, Children);
count_imported_nodes(s3, #dir_spec{children = Children}) -> count_imported_nodes(s3, Children);
count_imported_nodes(_StorageType, #file_spec{}) -> 1;
count_imported_nodes(_StorageType, #storage_fifo_spec{}) -> 0.


%%%===================================================================
%%% Internal functions - monitoring state assertion
%%%===================================================================


%% @private
%% Expands the snake_case histogram sugar keys of an overrides map into their
%% three per-resolution binary keys - see the doc of
%% assert_storage_import_monitoring_state/2.
-spec expand_histogram_sugar(map()) -> map().
expand_histogram_sugar(Overrides) ->
    SugarKeys = #{
        created_hist => <<"created">>,
        modified_hist => <<"modified">>,
        deleted_hist => <<"deleted">>
    },
    Expanded = maps:fold(fun(SugarKey, CounterName, Acc) ->
        case maps:find(SugarKey, Overrides) of
            {ok, Value} -> add_hist_resolutions(CounterName, Value, Acc);
            error -> Acc
        end
    end, #{}, SugarKeys),
    % explicit binary keys (e.g. a lone <<"createdMinHist">> => skip) win over sugar
    ExplicitOverrides = maps:without(maps:keys(SugarKeys), Overrides),
    maps:merge(Expanded, ExplicitOverrides).


%% @private
-spec add_hist_resolutions(binary(), term(), map()) -> map().
add_hist_resolutions(CounterName, Value, Acc) ->
    Acc#{
        <<CounterName/binary, "MinHist">> => Value,
        <<CounterName/binary, "HourHist">> => Value,
        <<CounterName/binary, "DayHist">> => Value
    }.


%% @private
-spec assert_monitoring_state(
    oct_background:node_selector(), od_space:id(), #{binary() => integer()}, non_neg_integer()
) ->
    ok.
assert_monitoring_state(NodeSelector, SpaceId, ExpectedSIM, Attempts) ->
    SIM = ?rpc(NodeSelector, storage_import_monitoring:describe(SpaceId)),
    try
        assert_monitoring_fields(ExpectedSIM, flatten_storage_import_histograms(SIM))
    catch
        throw:{assertion_error, _} when Attempts > 0 ->
            timer:sleep(timer:seconds(1)),
            assert_monitoring_state(NodeSelector, SpaceId, ExpectedSIM, Attempts - 1);

        throw:{assertion_error, {Key, ExpectedValue, Value}}:Stacktrace ->
            {Format, Args} = build_storage_import_monitoring_description(SIM),
            ct:pal(
                "Assertion of field \"~tp\" in storage_import_monitoring for space ~tp failed.~n"
                "    Expected: ~tp~n"
                "    Value: ~tp~n"
                ++ Format ++
                    "~nStacktrace:~n~tp",
                [Key, SpaceId, ExpectedValue, Value] ++ Args ++ [Stacktrace]),
            ct:fail("assertion failed")
    end.


%% @private
assert_monitoring_fields(ExpectedSIM, SIM) ->
    maps:foreach(fun
        (_Key, skip) ->
            ok;
        (Key, {range, Min, Max}) ->
            Value = maps:get(Key, SIM),
            case Value >= Min andalso Value =< Max of
                true -> ok;
                false -> throw({assertion_error, {Key, {range, Min, Max}, Value}})
            end;
        (Key, ExpectedValue) ->
            case maps:get(Key, SIM) of
                ExpectedValue -> ok;
                Value -> throw({assertion_error, {Key, ExpectedValue, Value}})
            end
    end, ExpectedSIM).


%% @private
%% @doc
%% Prepares the raw histograms returned by storage_import_monitoring:describe/1
%% for assertions:
%%  * the event-counter histograms (created/modified/deleted) are flattened by
%%    summing ALL their windows - each oct test runs in a fresh space (fresh
%%    monitoring doc), so the whole-histogram sum equals exactly the number of
%%    events since the test began, at every resolution alike (modulo histogram
%%    aging - see the doc of assert_storage_import_monitoring_state/2);
%%  * the queueLength histograms are gauges, not counters - only the current
%%    (head) window is meaningful, so it is taken as-is.
%% @end
flatten_storage_import_histograms(SIM) ->
    SIM#{
        <<"createdMinHist">> => lists:sum(maps:get(<<"createdMinHist">>, SIM)),
        <<"modifiedMinHist">> => lists:sum(maps:get(<<"modifiedMinHist">>, SIM)),
        <<"deletedMinHist">> => lists:sum(maps:get(<<"deletedMinHist">>, SIM)),
        <<"queueLengthMinHist">> => hd(maps:get(<<"queueLengthMinHist">>, SIM)),

        <<"createdHourHist">> => lists:sum(maps:get(<<"createdHourHist">>, SIM)),
        <<"modifiedHourHist">> => lists:sum(maps:get(<<"modifiedHourHist">>, SIM)),
        <<"deletedHourHist">> => lists:sum(maps:get(<<"deletedHourHist">>, SIM)),
        <<"queueLengthHourHist">> => hd(maps:get(<<"queueLengthHourHist">>, SIM)),

        <<"createdDayHist">> => lists:sum(maps:get(<<"createdDayHist">>, SIM)),
        <<"modifiedDayHist">> => lists:sum(maps:get(<<"modifiedDayHist">>, SIM)),
        <<"deletedDayHist">> => lists:sum(maps:get(<<"deletedDayHist">>, SIM)),
        <<"queueLengthDayHist">> => hd(maps:get(<<"queueLengthDayHist">>, SIM))
    }.


%% @private
build_storage_import_monitoring_description(SIM) ->
    maps:fold(fun(Key, Value, {AccFormat, AccArgs}) ->
        {AccFormat ++ "    ~tp = ~tp~n", AccArgs ++ [Key, Value]}
    end, {"~n#storage_import_monitoring fields values:~n", []}, SIM).
