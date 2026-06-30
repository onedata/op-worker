%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Generic machinery for declarative storage import tests.
%%%
%%% NOTE: this module (together with storage_file_setup_utils) must be added to
%%% ?LOAD_MODULES of any suite that uses it, as some routines are executed on the
%%% op_worker node via rpc.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_test_utils).
-author("Bartosz Walkowicz").

-include("storage_import_oct_test.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/dir_stats_collector/dir_size_stats.hrl").

%% API
-export([
    clean_up_after_previous_run/2,
    init_testcase/3, init_testcase/4,
    gen_nested_tree_spec/2,
    create_file_tree_on_storage/3,
    await_initial_scan_finished/1, await_initial_scan_finished/2,
    await_scan_finished/2, await_scan_finished/3,
    enable_continuous_scan/1, enable_continuous_scan/2,
    disable_continuous_scan/1,
    verify_imported_tree/1, verify_imported_tree/2,
    verify_dir_stats/1,
    assert_attrs/3, assert_attrs/4,
    assert_storage_import_monitoring_state/2
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

-define(ATTEMPTS, 30).
% Max number of concurrent processes used to verify the imported tree; bounds the
% load on the providers while still parallelizing the (RPC-heavy, retry-prone)
% per-node assertions - important for large trees (hundreds/thousands of nodes).
-define(VERIFY_PARALLELISM, 20).
% Max number of concurrent processes used to create the top-level tree nodes on the
% storage. Only the top-level siblings are parallelized (each subtree is created
% sequentially), so the total concurrency stays bounded by this value - parallelizing
% every level would multiply across levels and overload the provider.
-define(SETUP_PARALLELISM, 20).


%%%===================================================================
%%% API
%%%===================================================================


-spec clean_up_after_previous_run([atom()], suite_ctx()) -> ok.
clean_up_after_previous_run(AllTestCases, SuiteCtx) ->
    lists_utils:pforeach(fun(SpaceId) ->
        delete_space_with_supporting_storages(SpaceId, SuiteCtx)
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
    % Parallelize creation of the top-level siblings (each subtree is still created
    % sequentially) to speed up setup of wide trees, keeping concurrency bounded
    lists_utils:pmap(fun(Spec) ->
        create_file_tree_on_storage(ProviderSelector, StorageId, Spec)
    end, Specs, ?SETUP_PARALLELISM);
create_file_tree_on_storage(ProviderSelector, StorageId, Spec) ->
    create_node_on_storage(ProviderSelector, StorageId, <<"/">>, Spec).


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
%% scan). Used in continuous-scan scenarios, where the storage is mutated between
%% scans and each consecutive scan is awaited before asserting the new state.
%% Use the /3 variant with a higher Attempts for large imports.
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


%%--------------------------------------------------------------------
%% @doc
%% Enables continuous (periodic) scanning for the space. After this call the
%% import will keep rescanning the storage, picking up modifications/deletions
%% according to the (optionally overridden) scan config. Intended to be paired
%% with await_scan_finished/2 and disable_continuous_scan/1.
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
%% Generically verifies that the declared file tree was imported into the logical
%% filesystem - checked on both the importing and the non-importing provider.
%% For each declared node it asserts its type, and additionally:
%%  * for directories - that the set of its children matches the declaration,
%%  * for regular files - that their content matches the declaration.
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
    space_path = SpacePath,
    importing_provider_ctx = ImportingProviderCtx,
    non_importing_provider_ctx = NonImportingProviderCtx
}, ExpectedFileTreeSpec) ->
    TopLevelSpecs = to_spec_list(ExpectedFileTreeSpec),
    % flatten the whole tree (cheap, no RPC) into a list of {Path, Spec} so that
    % per-node verification (which is RPC-heavy and may retry while data propagates
    % to the non-importing provider) can be parallelized with bounded concurrency
    AllNodes = flatten_nodes(SpacePath, TopLevelSpecs),
    lists:foreach(fun(ProviderCtx) ->
        % the space root is not a declared node, so assert its children separately
        assert_children(ProviderCtx, SpacePath, TopLevelSpecs),
        lists_utils:pforeach(fun({Path, Spec}) ->
            verify_node(ProviderCtx, Path, Spec)
        end, AllNodes, ?VERIFY_PARALLELISM)
    end, [ImportingProviderCtx, NonImportingProviderCtx]).


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
verify_dir_stats(#storage_import_test_case_ctx{
    space_path = SpacePath,
    file_tree_spec = FileTreeSpec,
    importing_provider_ctx = ImportingProviderCtx,
    non_importing_provider_ctx = NonImportingProviderCtx
}) ->
    TopLevelSpecs = to_spec_list(FileTreeSpec),
    % the space root is not a declared node - assert it explicitly, with the
    % expectation aggregated over the whole declared tree
    SpaceRootSpec = #dir_spec{children = TopLevelSpecs},
    %% TODO VFS-13529 remove debug logging before merge to develop
    ct:pal("Asserting dir_size_stats for space root ~tp, expected (whole-tree) state:~n~tp", [
        SpacePath, expected_dir_stats(SpaceRootSpec)
    ]),
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
%% Supported fields: owner_id, uid, gid, mode, type, size.
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
    maps:foreach(fun(Field, ExpectedValue) ->
        ?assertEqual(ExpectedValue, get_file_attr_field(ProviderCtx, Path, Field), Attempts)
    end, ExpectedAttrs).


%%--------------------------------------------------------------------
%% @doc
%% Asserts the storage_import_monitoring counters for the space. The expected
%% number of created files/dirs (and the corresponding histograms) is derived
%% from the declared file tree; any field can be overridden via Overrides (e.g.
%% to express failed/modified/deleted counters in non-trivial scenarios).
%% Overrides keys are binaries, matching the storage_import_monitoring:describe/1 output.
%% An override value of 'skip' excludes that field from the assertion entirely -
%% useful for time-windowed histograms that may have shifted out of the asserted
%% buckets by the time the monitoring is read (e.g. createdMinHist after importing
%% and verifying a large tree).
%% @end
%%--------------------------------------------------------------------
-spec assert_storage_import_monitoring_state(case_ctx(), #{binary() => integer() | skip}) -> ok.
assert_storage_import_monitoring_state(#storage_import_test_case_ctx{
    suite_ctx = #storage_import_test_suite_ctx{storage_type = StorageType},
    space_id = SpaceId,
    file_tree_spec = FileTreeSpec,
    importing_provider_ctx = #provider_ctx{selector = ImportingProviderSelector}
}, Overrides) ->
    Created = count_imported_nodes(StorageType, FileTreeSpec),
    Default = #{
        <<"scans">> => 1,
        <<"created">> => Created,
        <<"modified">> => 0,
        <<"deleted">> => 0,
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
    %% TODO VFS-13529 remove debug logging before merge to develop
    ct:pal("Asserting storage_import_monitoring for space ~tp, expected state:~n~tp", [
        SpaceId, Expected
    ]),
    assert_monitoring_state(ImportingProviderSelector, SpaceId, Expected, 1).


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
-spec delete_space_with_supporting_storages(od_space:id(), suite_ctx()) -> ok.
delete_space_with_supporting_storages(SpaceId, #storage_import_test_suite_ctx{
    importing_provider_selector = ImportingProviderSelector,
    non_importing_provider_selector = NonImportingProviderSelector
}) ->
    [StorageImportingProvider] = opw_test_rpc:get_space_local_storages(
        ImportingProviderSelector, SpaceId
    ),
    [StorageNonImportingProvider] = opw_test_rpc:get_space_local_storages(
        NonImportingProviderSelector, SpaceId
    ),

    ok = ozw_test_rpc:delete_space(SpaceId),

    ok = delete_storage(ImportingProviderSelector, StorageImportingProvider),
    ok = delete_storage(NonImportingProviderSelector, StorageNonImportingProvider).


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
-spec verify_node(#provider_ctx{}, file_meta:path(), onenv_file_test_utils:object_spec()) -> ok.
verify_node(ProviderCtx, Path, #dir_spec{children = Children}) ->
    assert_node_type(ProviderCtx, Path, ?DIRECTORY_TYPE),
    assert_children(ProviderCtx, Path, Children);
verify_node(ProviderCtx, Path, #file_spec{content = Content}) ->
    assert_node_type(ProviderCtx, Path, ?REGULAR_FILE_TYPE),
    assert_file_content(ProviderCtx, Path, Content).


%% @private
-spec get_file_attr_field(#provider_ctx{}, file_meta:path(), atom()) -> term().
get_file_attr_field(#provider_ctx{node = Node, session_id = SessId}, Path, Field) ->
    case lfm_proxy:stat(Node, SessId, {path, Path}) of
        {ok, FileAttr} -> file_attr_field(Field, FileAttr);
        {error, _} = Error -> Error
    end.


%% @private
-spec file_attr_field(atom(), #file_attr{}) -> term().
file_attr_field(owner_id, #file_attr{owner_id = Value}) -> Value;
file_attr_field(uid, #file_attr{uid = Value}) -> Value;
file_attr_field(gid, #file_attr{gid = Value}) -> Value;
file_attr_field(mode, #file_attr{mode = Value}) -> Value;
file_attr_field(type, #file_attr{type = Value}) -> Value;
file_attr_field(size, #file_attr{size = Value}) -> Value.


%% @private
-spec assert_node_type(#provider_ctx{}, file_meta:path(), onedata_file:type()) -> ok.
assert_node_type(#provider_ctx{node = Node, session_id = SessId}, Path, ExpectedType) ->
    ?assertMatch(
        {ok, #file_attr{type = ExpectedType}},
        lfm_proxy:stat(Node, SessId, {path, Path}),
        ?ATTEMPTS
    ),
    ok.


%% @private
-spec assert_children(#provider_ctx{}, file_meta:path(), [onenv_file_test_utils:object_spec()]) -> ok.
assert_children(#provider_ctx{node = Node, session_id = SessId}, ParentPath, ChildrenSpecs) ->
    ExpectedNames = lists:sort([spec_name(ChildSpec) || ChildSpec <- ChildrenSpecs]),
    ?assertEqual(ExpectedNames, list_child_names(Node, SessId, ParentPath), ?ATTEMPTS),
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
-spec assert_file_content(#provider_ctx{}, file_meta:path(), binary()) -> ok.
assert_file_content(#provider_ctx{node = Node, session_id = SessId}, Path, Content) ->
    {ok, Handle} = ?assertMatch(
        {ok, _}, lfm_proxy:open(Node, SessId, {path, Path}, read), ?ATTEMPTS
    ),
    % read at least 1 byte, otherwise an empty file would not be checked at all
    ReadSize = max(byte_size(Content), 1),
    ?assertEqual({ok, Content}, lfm_proxy:check_size_and_read(Node, Handle, 0, ReadSize), ?ATTEMPTS),
    ok = lfm_proxy:close(Node, Handle).


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
%% Counts the storage entries that storage import reports as 'created' for the
%% declared file tree. On POSIX storages every directory and regular file is a
%% real storage entry, so both are counted. On object storages (S3) there are no
%% directory entries - directories are emulated via object key prefixes and are
%% never reported as created - so only the regular files (objects) are counted.
%% FIFOs are created on the storage but never imported, hence not counted either.
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
        (Key, ExpectedValue) ->
            case maps:get(Key, SIM) of
                ExpectedValue -> ok;
                Value -> throw({assertion_error, {Key, ExpectedValue, Value}})
            end
    end, ExpectedSIM).


%% @private
flatten_storage_import_histograms(SIM) ->
    SIM#{
        % flatten beginnings of histograms for assertions
        <<"createdMinHist">> => lists:sum(lists:sublist(maps:get(<<"createdMinHist">>, SIM), 2)),
        <<"modifiedMinHist">> => lists:sum(lists:sublist(maps:get(<<"modifiedMinHist">>, SIM), 2)),
        <<"deletedMinHist">> => lists:sum(lists:sublist(maps:get(<<"deletedMinHist">>, SIM), 2)),
        <<"queueLengthMinHist">> => hd(maps:get(<<"queueLengthMinHist">>, SIM)),

        <<"createdHourHist">> => lists:sum(lists:sublist(maps:get(<<"createdHourHist">>, SIM), 3)),
        <<"modifiedHourHist">> => lists:sum(lists:sublist(maps:get(<<"modifiedHourHist">>, SIM), 3)),
        <<"deletedHourHist">> => lists:sum(lists:sublist(maps:get(<<"deletedHourHist">>, SIM), 3)),
        <<"queueLengthHourHist">> => hd(maps:get(<<"queueLengthHourHist">>, SIM)),

        <<"createdDayHist">> => lists:sum(lists:sublist(maps:get(<<"createdDayHist">>, SIM), 1)),
        <<"modifiedDayHist">> => lists:sum(lists:sublist(maps:get(<<"modifiedDayHist">>, SIM), 1)),
        <<"deletedDayHist">> => lists:sum(lists:sublist(maps:get(<<"deletedDayHist">>, SIM), 1)),
        <<"queueLengthDayHist">> => hd(maps:get(<<"queueLengthDayHist">>, SIM))
    }.


%% @private
build_storage_import_monitoring_description(SIM) ->
    maps:fold(fun(Key, Value, {AccFormat, AccArgs}) ->
        {AccFormat ++ "    ~tp = ~tp~n", AccArgs ++ [Key, Value]}
    end, {"~n#storage_import_monitoring fields values:~n", []}, SIM).
