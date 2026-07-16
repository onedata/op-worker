%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for testing transfers (replication, replica eviction
%%% and replica migration) in onenv-based CT tests.
%%%
%%% The typical test flow is:
%%% 1. create_file_tree/3 - creates a per-test-case file tree (a root
%%%    directory named after the test case wrapping the declared content)
%%%    on the creation provider and awaits its sync on the other one;
%%% 2. ensure_initial_replicas/2 - sets up the replicas the suite's transfer
%%%    type operates on (pre-replicates the tree for replica eviction;
%%%    no-op otherwise);
%%% 3. schedule_transfer/2 - schedules the suite-ctx-defined transfer type
%%%    for the given file tree object;
%%% 4. await_transfer_ended/4,5 - awaits the transfer doc reaching its
%%%    expected end state on both providers. The expected values of the
%%%    #transfer{} record fields are derived from the declared file tree
%%%    (file count, total byte size) and the transfer type; single fields
%%%    can be overridden with an exact value, 'skip', {gte, Min},
%%%    {range, Min, Max} or a predicate fun. The special 'tree_bytes'
%%%    override adjusts the derivation input rather than any single field:
%%%    it replaces the total byte size counted from the declared tree (for
%%%    content written outside of the tree spec, e.g. a big file filled
%%%    after creation) and drives the per-type expected byte counters and
%%%    histogram sums. It may itself be an expectation (e.g. {gte, Min})
%%%    when the exact transferred byte count is nondeterministic;
%%% 5. assert_distribution/2,3 - asserts the expected post-transfer block
%%%    distribution, again derived from the declared file tree and the
%%%    transfer type, with optional per-file size overrides. For tests
%%%    expecting a distribution that does not match this derivation (e.g.
%%%    after a failed or no-op transfer), await_distribution/3 asserts an
%%%    explicitly given one.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_test_utils).
-author("Bartosz Walkowicz").

-include("transfer_test.hrl").
-include("onenv_test_utils.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/fslogic/file_attr.hrl").
-include_lib("ctool/include/onedata_file.hrl").
-include_lib("ctool/include/posix/errno.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([
    remove_leftover_file_trees/2,
    gen_nested_tree_spec/2,
    create_file_tree/3,
    ensure_initial_replicas/2,
    schedule_transfer/2,
    schedule_view_transfer/3,

    rand_xattr_name/1,
    rand_view_name/1,
    gen_view_map_function/1, gen_view_map_function/2,
    gen_view_reduce_function/1,
    create_view/5,
    await_view_query_result/4,
    remove_all_views/1,

    mock_gated_file_replication/1,
    grant_file_replication_permits/2,
    await_gated_file_replication_job/0,
    await_files_replicated/3,
    unmock_gated_file_replication/1,
    await_transfer_ended/4, await_transfer_ended/5,
    assert_distribution/2, assert_distribution/3,
    assert_initial_distribution/2,
    await_distribution/3,
    remove_all_transfers/1,

    get_space_support_size/2,
    get_space_occupancy/2,
    set_space_occupancy/3
]).

-type transfer_type() :: replication | eviction | migration.

-type field_expectation() ::
    term() |
    skip |
    {gte, integer()} |
    {range, integer(), integer()} |
    % asserts a bytes-per-provider histogram field: the histogram of the given
    % provider must sum up to bytes satisfying the given (nested) expectation
    {histogram_sum, od_provider:id(), field_expectation()} |
    fun((term()) -> boolean()).
-type expected_transfer() :: #{atom() => field_expectation()}.

% transferred files given either as a (nested) file tree or a flat list of
% its objects (e.g. the subset of tree files matched by a view)
-type file_tree_objects() ::
    onenv_file_test_utils:object() | [onenv_file_test_utils:object()].

-export_type([transfer_type/0, field_expectation/0, expected_transfer/0, file_tree_objects/0]).

-type suite_ctx() :: #transfer_test_suite_ctx{}.

-define(SPACE_ROOT_LS_LIMIT, 10000).

-define(FILE_REPLICATION_PERMITS_KEY, file_replication_permits).
-define(FILE_REPLICATION_JOB_GATED_MSG, file_replication_job_gated).
-define(FILE_REPLICATION_PERMIT_POLL_INTERVAL_MS, 100).
-define(INFINITE_PERMITS, 1 bsl 50).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Removes the given test case's file trees left in the shared space by
%% previous runs of the suite (per-test-case root directories are kept
%% after each run so that the state of a failed test can be inspected).
%% @end
%%--------------------------------------------------------------------
-spec remove_leftover_file_trees(suite_ctx(), atom()) -> ok.
remove_leftover_file_trees(#transfer_test_suite_ctx{
    space_selector = SpaceSelector,
    user_selector = UserSelector
}, CaseName) ->
    % remove any leftover datasets first - their protection flags would otherwise
    % block the file tree removal below (rm_recursive moves the tree to the trash,
    % but its protected entries can never be purged from there)
    remove_all_datasets(SpaceSelector, UserSelector),
    {ok, Children} = onenv_file_test_utils:ls(UserSelector, SpaceSelector, 0, ?SPACE_ROOT_LS_LIMIT),
    CaseNamePrefix = <<(atom_to_binary(CaseName, utf8))/binary, "_">>,

    lists:foreach(fun({ChildGuid, ChildName}) ->
        case str_utils:binary_starts_with(ChildName, CaseNamePrefix) of
            true -> rm_leftover_file_tree(SpaceSelector, UserSelector, ChildGuid);
            false -> ok
        end
    end, Children).


%%--------------------------------------------------------------------
%% @doc
%% Generates a nested file tree spec of uniform branching, with all leaf
%% files getting the given content. Consecutive list elements give the
%% directory count on consecutive nesting levels and the last one gives
%% the file count in every innermost directory. E.g.:
%% - gen_nested_tree_spec([10, 10, 0], C) - 10 directories, each with
%%   10 subdirectories, no files;
%% - gen_nested_tree_spec([100], C) - 100 files.
%% @end
%%--------------------------------------------------------------------
-spec gen_nested_tree_spec([non_neg_integer()], binary()) ->
    [onenv_file_test_utils:object_spec()].
gen_nested_tree_spec([FilesCount], FileContent) ->
    [#file_spec{content = FileContent} || _ <- lists:seq(1, FilesCount)];
gen_nested_tree_spec([DirsCount | RestBranching], FileContent) ->
    [
        #dir_spec{children = gen_nested_tree_spec(RestBranching, FileContent)}
        || _ <- lists:seq(1, DirsCount)
    ].


-spec create_file_tree(suite_ctx(), atom(), onenv_file_test_utils:object_spec()) ->
    onenv_file_test_utils:object().
create_file_tree(#transfer_test_suite_ctx{
    space_selector = SpaceSelector,
    user_selector = UserSelector,
    creation_provider_selector = CreationProviderSelector
}, CaseName, #dir_spec{} = RootDirSpec) ->
    RootDirName = str_utils:format_bin("~ts_~ts", [CaseName, str_utils:rand_hex(6)]),
    onenv_file_test_utils:create_and_sync_file_tree(
        UserSelector, SpaceSelector,
        RootDirSpec#dir_spec{name = RootDirName},
        CreationProviderSelector
    ).


%%--------------------------------------------------------------------
%% @doc
%% Ensures the initial replicas required by the suite's transfer type exist
%% before the tested transfer is scheduled: replica eviction operates on
%% a file tree already replicated to the other provider, so the tree is
%% pre-replicated with an auxiliary replication transfer. For the other
%% transfer types the creation provider replicas suffice.
%% @end
%%--------------------------------------------------------------------
-spec ensure_initial_replicas(suite_ctx(), onenv_file_test_utils:object()) -> ok.
ensure_initial_replicas(#transfer_test_suite_ctx{
    transfer_type = eviction,
    user_selector = UserSelector,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}, #object{guid = RootFileGuid}) ->
    SessionId = oct_background:get_user_session_id(UserSelector, CreationProviderSelector),
    OtherProviderId = oct_background:get_provider_id(OtherProviderSelector),

    %% TODO consider reading every file on other provider - that will cause replication
    {ok, TransferId} = ?assertMatch({ok, _}, opt_transfers:schedule_file_replication(
        CreationProviderSelector, SessionId, ?FILE_REF(RootFileGuid), OtherProviderId
    )),
    ExpectedTransfer = #{replication_status => ?COMPLETED_STATUS, failed_files => 0},
    lists:foreach(fun(ProviderSelector) ->
        await_transfer_state(ProviderSelector, TransferId, ExpectedTransfer, ?LARGE_TRANSFER_ATTEMPTS)
    end, [CreationProviderSelector, OtherProviderSelector]);
ensure_initial_replicas(_TestSuiteCtx, _TransferRootObject) ->
    ok.


-spec schedule_transfer(suite_ctx(), onenv_file_test_utils:object()) ->
    transfer:id().
schedule_transfer(TestSuiteCtx = #transfer_test_suite_ctx{
    user_selector = UserSelector,
    creation_provider_selector = CreationProviderSelector
}, #object{guid = FileGuid}) ->
    SessionId = oct_background:get_user_session_id(UserSelector, CreationProviderSelector),
    {ReplicatingProviderId, EvictingProviderId} = get_transfer_target_providers(TestSuiteCtx),

    {ok, TransferId} = ?assertMatch({ok, _}, opt_transfers:schedule_file_transfer(
        CreationProviderSelector, SessionId, ?FILE_REF(FileGuid),
        ReplicatingProviderId, EvictingProviderId, undefined
    )),
    TransferId.


-spec schedule_view_transfer(suite_ctx(), index:name(), transfer:query_view_params()) ->
    transfer:id().
schedule_view_transfer(TestSuiteCtx = #transfer_test_suite_ctx{
    space_selector = SpaceSelector,
    user_selector = UserSelector,
    creation_provider_selector = CreationProviderSelector
}, ViewName, QueryViewParams) ->
    SessionId = oct_background:get_user_session_id(UserSelector, CreationProviderSelector),
    SpaceId = oct_background:get_space_id(SpaceSelector),
    {ReplicatingProviderId, EvictingProviderId} = get_transfer_target_providers(TestSuiteCtx),

    {ok, TransferId} = ?assertMatch({ok, _}, opt_transfers:schedule_view_transfer(
        CreationProviderSelector, SessionId, SpaceId, ViewName, QueryViewParams,
        ReplicatingProviderId, EvictingProviderId, undefined
    )),
    TransferId.


-spec rand_xattr_name(atom()) -> onedata_file:xattr_name().
rand_xattr_name(CaseName) ->
    str_utils:format_bin("xattr_~ts_~ts", [CaseName, str_utils:rand_hex(6)]).


-spec rand_view_name(atom()) -> index:name().
rand_view_name(CaseName) ->
    str_utils:format_bin("view_~ts_~ts", [CaseName, str_utils:rand_hex(6)]).


%%--------------------------------------------------------------------
%% @doc
%% Builds a view map function emitting, for every file having the given
%% xattr, its object id keyed by the xattr value.
%% @end
%%--------------------------------------------------------------------
-spec gen_view_map_function(onedata_file:xattr_name()) -> index:view_function().
gen_view_map_function(XattrName) ->
    <<"function (id, type, meta, ctx) {
        if (type == 'custom_metadata' && meta['", XattrName/binary, "']) {
            return [meta['", XattrName/binary, "'], id];
        }
        return null;
    }">>.


%%--------------------------------------------------------------------
%% @doc
%% Like gen_view_map_function/1 but emits [id, SecondXattrValue] pairs -
%% input for a reduce function filtering by the second xattr value
%% (see gen_view_reduce_function/1).
%% @end
%%--------------------------------------------------------------------
-spec gen_view_map_function(onedata_file:xattr_name(), onedata_file:xattr_name()) ->
    index:view_function().
gen_view_map_function(XattrName, SecondXattrName) ->
    <<"function (id, type, meta, ctx) {
        if (type == 'custom_metadata' && meta['", XattrName/binary, "']) {
            return [meta['", XattrName/binary, "'], [id, meta['", SecondXattrName/binary, "']]];
        }
        return null;
    }">>.


%%--------------------------------------------------------------------
%% @doc
%% Builds a view reduce function passing through only the file ids of the
%% [id, XattrValue] pairs (emitted by gen_view_map_function/2) with the
%% given xattr value.
%% @end
%%--------------------------------------------------------------------
-spec gen_view_reduce_function(term()) -> index:view_function().
gen_view_reduce_function(XattrValue) ->
    XattrValueBin = str_utils:to_binary(XattrValue),
    <<"function (key, values, rereduce) {
        var filtered = [];
        for (i = 0; i < values.length; i++)
            if (values[i][1] == ", XattrValueBin/binary, ")
                filtered.push(values[i][0]);
        return filtered;
    }">>.


%%--------------------------------------------------------------------
%% @doc
%% Creates a view on the other provider (the one that evaluates it when
%% processing transfers by view) and awaits the view doc dbsync on the
%% creation (scheduling) provider.
%% @end
%%--------------------------------------------------------------------
-spec create_view(
    suite_ctx(),
    index:name(),
    index:view_function(),
    undefined | index:view_function(),
    index:options()
) ->
    ok.
create_view(#transfer_test_suite_ctx{
    space_selector = SpaceSelector,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}, ViewName, MapFunction, ReduceFunction, ViewOptions) ->
    SpaceId = oct_background:get_space_id(SpaceSelector),
    OtherProviderId = oct_background:get_provider_id(OtherProviderSelector),

    ok = opw_test_rpc:call(OtherProviderSelector, index, save, [
        SpaceId, ViewName, MapFunction, ReduceFunction, ViewOptions,
        false, [OtherProviderId]
    ]),
    ?assertEqual(true, case opw_test_rpc:call(
        CreationProviderSelector, index, get, [ViewName, SpaceId]
    ) of
        {ok, _} -> true;
        {error, _} -> false
    end, ?ATTEMPTS).


%%--------------------------------------------------------------------
%% @doc
%% Awaits the view emitting exactly the expected values (in any order)
%% for the given query. Queried on the other provider - the one the view
%% is evaluated on.
%% @end
%%--------------------------------------------------------------------
-spec await_view_query_result(suite_ctx(), index:name(), index:options(), [term()]) ->
    ok.
await_view_query_result(#transfer_test_suite_ctx{
    space_selector = SpaceSelector,
    other_provider_selector = OtherProviderSelector
}, ViewName, QueryOptions, ExpectedValues) ->
    SpaceId = oct_background:get_space_id(SpaceSelector),

    ?assertEqual(lists:sort(ExpectedValues), try
        {ok, #{<<"rows">> := Rows}} = opw_test_rpc:call(OtherProviderSelector, index, query, [
            SpaceId, ViewName, QueryOptions
        ]),
        lists:sort(lists:flatmap(fun(Row) ->
            lists:flatten([maps:get(<<"value">>, Row)])
        end, Rows))
    catch _:_ ->
        query_failed
    end, ?ATTEMPTS).


%%--------------------------------------------------------------------
%% @doc
%% Removes all views of the space, on every supporting provider. Called
%% as part of the leftover cleanup - view test cases create views with
%% per-run random names, which would otherwise accumulate.
%% @end
%%--------------------------------------------------------------------
-spec remove_all_views(suite_ctx()) -> ok.
remove_all_views(#transfer_test_suite_ctx{space_selector = SpaceSelector}) ->
    SpaceId = oct_background:get_space_id(SpaceSelector),

    lists:foreach(fun(ProviderSelector) ->
        {ok, ViewNames} = opw_test_rpc:call(ProviderSelector, index, list, [SpaceId]),
        lists:foreach(fun(ViewName) ->
            case opw_test_rpc:call(ProviderSelector, index, delete, [SpaceId, ViewName]) of
                ok ->
                    ok;
                {error, not_found} ->
                    % already deleted alongside the other provider (dbsync)
                    ok
            end
        end, ViewNames)
    end, oct_background:get_space_supporting_providers(SpaceSelector)).


%%--------------------------------------------------------------------
%% @doc
%% Mocks file replication on the given provider so that every replication
%% job must acquire a permit before transferring its file. A job finding
%% no free permit parks (notifying the calling process - see
%% await_gated_file_replication_job/0) until one is granted with
%% grant_file_replication_permits/2; no permits are available initially.
%% This allows deterministically suspending an ongoing transfer and
%% releasing it in stages, interleaved with other operations.
%% Must be paired with unmock_gated_file_replication/1 in the test teardown.
%% @end
%%--------------------------------------------------------------------
-spec mock_gated_file_replication(oct_background:entity_selector()) -> ok.
mock_gated_file_replication(ProviderSelector) ->
    TestProcess = self(),
    Nodes = oct_background:get_provider_nodes(ProviderSelector),

    lists:foreach(fun(Node) ->
        % the permit counter must be created on the provider node (atomics are
        % node-local); the node-wide cache entry keeps the ref alive (an ets
        % table would die with its owner - the transient rpc process)
        ok = opw_test_rpc:call(Node, fun() ->
            node_cache:put(?FILE_REPLICATION_PERMITS_KEY, atomics:new(1, []))
        end)
    end, Nodes),

    ok = test_utils:mock_new(Nodes, replication_worker, [passthrough]),
    ok = test_utils:mock_expect(Nodes, replication_worker, transfer_regular_file, fun(
        FileCtx, TransferParams
    ) ->
        acquire_file_replication_permit(TestProcess),
        meck:passthrough([FileCtx, TransferParams])
    end).


-spec grant_file_replication_permits(oct_background:entity_selector(), pos_integer() | all) ->
    ok.
grant_file_replication_permits(ProviderSelector, CountOrAll) ->
    % permits are granted per provider node (irrelevant for the single-node
    % providers the transfer suites run on)
    lists:foreach(fun(Node) ->
        ok = opw_test_rpc:call(Node, fun() ->
            PermitsRef = node_cache:get(?FILE_REPLICATION_PERMITS_KEY),
            case CountOrAll of
                all -> atomics:put(PermitsRef, 1, ?INFINITE_PERMITS);
                Count -> atomics:add(PermitsRef, 1, Count)
            end
        end)
    end, oct_background:get_provider_nodes(ProviderSelector)).


%%--------------------------------------------------------------------
%% @doc
%% Awaits the notification a gated replication job sends when it parks
%% awaiting a permit - proof that the transfer traverse is underway and
%% suspended. Only jobs that actually park notify, so leftover mailbox
%% messages cannot produce a false positive for an already-drained gate.
%% @end
%%--------------------------------------------------------------------
-spec await_gated_file_replication_job() -> ok.
await_gated_file_replication_job() ->
    receive
        ?FILE_REPLICATION_JOB_GATED_MSG -> ok
    after timer:seconds(?ATTEMPTS) ->
        ct:fail(no_file_replication_job_awaiting_permit)
    end.


-spec await_files_replicated(oct_background:entity_selector(), transfer:id(), non_neg_integer()) ->
    ok.
await_files_replicated(ProviderSelector, TransferId, ExpFilesReplicated) ->
    ?assertEqual(ExpFilesReplicated, case opw_test_rpc:call(
        ProviderSelector, transfer, get, [TransferId]
    ) of
        {ok, #document{value = #transfer{files_replicated = FilesReplicated}}} ->
            FilesReplicated;
        {error, _} = Error ->
            Error
    end, ?ATTEMPTS).


-spec unmock_gated_file_replication(oct_background:entity_selector()) -> ok.
unmock_gated_file_replication(ProviderSelector) ->
    % release any still-parked jobs first - a process parked inside the mock
    % call would be killed by the code purge on unload
    grant_file_replication_permits(ProviderSelector, all),
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    test_utils:mock_unload(Nodes, replication_worker),
    lists:foreach(fun(Node) ->
        ok = opw_test_rpc:call(Node, node_cache, clear, [?FILE_REPLICATION_PERMITS_KEY])
    end, Nodes).


-spec await_transfer_ended(suite_ctx(), transfer:id(), file_tree_objects(), expected_transfer()) ->
    ok.
await_transfer_ended(SuiteCtx, TransferId, TransferRootObjects, Overrides) ->
    await_transfer_ended(SuiteCtx, TransferId, TransferRootObjects, Overrides, ?ATTEMPTS).


-spec await_transfer_ended(
    suite_ctx(),
    transfer:id(),
    file_tree_objects(),
    expected_transfer(),
    non_neg_integer()
) ->
    ok.
await_transfer_ended(#transfer_test_suite_ctx{
    transfer_type = TransferType,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
} = SuiteCtx, TransferId, TransferRootObjects, Overrides, Attempts) ->
    ExpectedTransfer = maps:merge(
        build_expected_transfer(SuiteCtx, TransferRootObjects, Overrides),
        maps:remove(tree_bytes, Overrides)
    ),

    %% TODO VFS-13678 remove debug logging before merge to develop
    ct:pal("Awaiting ~tp transfer ~ts end, expected state:~n~tp", [
        TransferType, TransferId, ExpectedTransfer
    ]),

    lists:foreach(fun(ProviderSelector) ->
        await_transfer_state(ProviderSelector, TransferId, ExpectedTransfer, Attempts)
    end, [CreationProviderSelector, OtherProviderSelector]).


-spec assert_distribution(suite_ctx(), file_tree_objects()) ->
    ok.
assert_distribution(TestSuiteCtx, TransferRootObjects) ->
    assert_distribution(TestSuiteCtx, TransferRootObjects, #{}).


%%--------------------------------------------------------------------
%% @doc
%% Asserts the expected post-transfer block distribution of every regular
%% file of the given tree, derived from the declared file sizes and the
%% transfer type. File sizes not reflected by the declared content (content
%% written outside of the tree spec) can be overridden per file guid.
%% @end
%%--------------------------------------------------------------------
-spec assert_distribution(
    suite_ctx(),
    file_tree_objects(),
    #{file_id:file_guid() => file_meta:size()}
) ->
    ok.
assert_distribution(#transfer_test_suite_ctx{
    transfer_type = TransferType,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}, TransferRootObjects, FileSizeOverrides) ->
    CreationNode = oct_background:get_random_provider_node(CreationProviderSelector),
    OtherNode = oct_background:get_random_provider_node(OtherProviderSelector),

    FilesWithExpDistribution = lists:map(fun(#object{
        guid = FileGuid, name = FileName, content = Content
    }) ->
        FileSize = maps:get(FileGuid, FileSizeOverrides, byte_size(Content)),
        ExpSizePerNode = case TransferType of
            replication -> [{CreationNode, FileSize}, {OtherNode, FileSize}];
            eviction -> [{CreationNode, FileSize}, {OtherNode, 0}];
            migration -> [{CreationNode, 0}, {OtherNode, FileSize}]
        end,
        {FileName, FileGuid, ExpSizePerNode}
    end, collect_regular_files(TransferRootObjects)),

    %% TODO VFS-13678 remove debug logging before merge to develop
    ct:pal("Asserting file distribution after ~tp, expected:~n~tp", [
        TransferType,
        [{FileName, ExpSizePerNode} || {FileName, _, ExpSizePerNode} <- FilesWithExpDistribution]
    ]),

    lists:foreach(fun({_FileName, FileGuid, ExpSizePerNode}) ->
        file_test_utils:await_distribution([CreationNode, OtherNode], FileGuid, ExpSizePerNode)
    end, FilesWithExpDistribution).


%%--------------------------------------------------------------------
%% @doc
%% Asserts the pre-transfer block distribution of every regular file of
%% the given tree (or file list): the creation provider holds the whole
%% content while the other provider holds full replicas for eviction
%% (set up by ensure_initial_replicas/2) and no blocks otherwise. Meant
%% for tests expecting a transfer to change nothing.
%% @end
%%--------------------------------------------------------------------
-spec assert_initial_distribution(suite_ctx(), file_tree_objects()) ->
    ok.
assert_initial_distribution(#transfer_test_suite_ctx{
    transfer_type = TransferType,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}, TransferRootObjects) ->
    lists:foreach(fun(#object{guid = FileGuid, content = Content}) ->
        FileSize = byte_size(Content),
        OtherProviderSize = case TransferType of
            eviction -> FileSize;
            _ -> 0
        end,
        await_distribution(
            [CreationProviderSelector, OtherProviderSelector], FileGuid,
            [{CreationProviderSelector, FileSize}, {OtherProviderSelector, OtherProviderSize}]
        )
    end, collect_regular_files(TransferRootObjects)).


%%--------------------------------------------------------------------
%% @doc
%% Like file_test_utils:await_distribution/3 but takes provider selectors
%% instead of nodes. Meant for tests expecting a bespoke distribution that
%% differs from the one assert_distribution/2,3 derives from the transfer
%% type (e.g. after a failed or no-op transfer).
%% NOTE: every regular file created with create_file_tree/3 has an empty
%% (zero blocks) distribution entry on each supporting provider - the
%% creation sync awaits it - so the expectation must list such providers
%% with size 0 rather than omit them.
%% @end
%%--------------------------------------------------------------------
-spec await_distribution(
    [oct_background:entity_selector()],
    file_id:file_guid(),
    [{oct_background:entity_selector(), file_meta:size()}]
) ->
    ok.
await_distribution(ProviderSelectors, FileGuid, ExpSizePerProvider) ->
    file_test_utils:await_distribution(
        [oct_background:get_random_provider_node(PS) || PS <- ProviderSelectors],
        FileGuid,
        [
            {oct_background:get_random_provider_node(PS), ExpSize}
            || {PS, ExpSize} <- ExpSizePerProvider
        ]
    ).


-spec remove_all_transfers(suite_ctx()) -> ok.
remove_all_transfers(#transfer_test_suite_ctx{
    space_selector = SpaceSelector,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}) ->
    SpaceId = oct_background:get_space_id(SpaceSelector),

    lists:foreach(fun(ProviderSelector) ->
        lists:foreach(fun(ListingFun) ->
            {ok, TransferIds} = opw_test_rpc:call(ProviderSelector, transfer, ListingFun, [SpaceId]),
            lists:foreach(fun(TransferId) ->
                opw_test_rpc:call(ProviderSelector, transfer, delete, [TransferId])
            end, TransferIds)
        end, [list_waiting_transfers, list_ongoing_transfers, list_ended_transfers])
    end, [CreationProviderSelector, OtherProviderSelector]).


-spec get_space_support_size(oct_background:entity_selector(), od_space:id()) ->
    non_neg_integer().
get_space_support_size(ProviderSelector, SpaceId) ->
    {ok, SupportSize} = opw_test_rpc:call(ProviderSelector, provider_logic, get_support_size, [SpaceId]),
    SupportSize.


-spec get_space_occupancy(oct_background:entity_selector(), od_space:id()) ->
    non_neg_integer().
get_space_occupancy(ProviderSelector, SpaceId) ->
    opw_test_rpc:call(ProviderSelector, space_quota, current_size, [SpaceId]).


-spec set_space_occupancy(oct_background:entity_selector(), od_space:id(), non_neg_integer()) ->
    ok.
set_space_occupancy(ProviderSelector, SpaceId, TargetSize) ->
    CurrentSize = get_space_occupancy(ProviderSelector, SpaceId),
    opw_test_rpc:call(ProviderSelector, space_quota, apply_size_change, [
        SpaceId, TargetSize - CurrentSize
    ]),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
%% Executed on the provider nodes within the gate-mocked replication job
%% processes (see mock_gated_file_replication/1).
-spec acquire_file_replication_permit(pid()) -> ok.
acquire_file_replication_permit(TestProcess) ->
    PermitsRef = node_cache:get(?FILE_REPLICATION_PERMITS_KEY),
    case try_acquire_file_replication_permit(PermitsRef) of
        true ->
            ok;
        false ->
            TestProcess ! ?FILE_REPLICATION_JOB_GATED_MSG,
            wait_for_file_replication_permit(PermitsRef)
    end.


%% @private
-spec wait_for_file_replication_permit(atomics:atomics_ref()) -> ok.
wait_for_file_replication_permit(PermitsRef) ->
    case try_acquire_file_replication_permit(PermitsRef) of
        true ->
            ok;
        false ->
            timer:sleep(?FILE_REPLICATION_PERMIT_POLL_INTERVAL_MS),
            wait_for_file_replication_permit(PermitsRef)
    end.


%% @private
-spec try_acquire_file_replication_permit(atomics:atomics_ref()) -> boolean().
try_acquire_file_replication_permit(PermitsRef) ->
    case atomics:sub_get(PermitsRef, 1, 1) of
        Permits when Permits >= 0 ->
            true;
        _ ->
            % return the overdrawn permit (the counter may transiently go
            % negative under concurrent acquisitions but never loses permits)
            atomics:add(PermitsRef, 1, 1),
            false
    end.


%% @private
%% Removes a leftover file tree, awaiting its disappearance from the
%% user-visible space on every provider. Unlike
%% onenv_file_test_utils:rm_and_sync_file/2, a tree that has been moved to the
%% trash already counts as removed - it does not wait for the (asynchronous,
%% size-proportional) purge from the trash to finish, which for large trees
%% may outlast the assertion attempts.
-spec rm_leftover_file_tree(
    oct_background:entity_selector(), oct_background:entity_selector(), file_id:file_guid()
) ->
    ok.
rm_leftover_file_tree(SpaceSelector, UserSelector, FileGuid) ->
    UserId = oct_background:get_user_id(UserSelector),
    SpaceId = oct_background:get_space_id(SpaceSelector),
    [RmProvider | RestProviders] = lists_utils:shuffle(
        oct_background:get_space_supporting_providers(SpaceSelector)
    ),

    RmNode = oct_background:get_random_provider_node(RmProvider),
    RmSessId = oct_background:get_user_session_id(UserId, RmProvider),
    ?assertMatch(ok, lfm_proxy:rm_recursive(RmNode, RmSessId, ?FILE_REF(FileGuid))),

    TrashGuid = opw_test_rpc:call(RmProvider, trash_dir, guid, [SpaceId]),
    lists:foreach(fun(Provider) ->
        Node = oct_background:get_random_provider_node(Provider),
        SessId = oct_background:get_user_session_id(UserId, Provider),
        ?assertEqual(true, is_removed_from_space(Node, SessId, FileGuid, TrashGuid), ?ATTEMPTS)
    end, RestProviders).


%% @private
-spec is_removed_from_space(node(), session:id(), file_id:file_guid(), file_id:file_guid()) ->
    boolean().
is_removed_from_space(Node, SessId, FileGuid, TrashGuid) ->
    case lfm_proxy:stat(Node, SessId, ?FILE_REF(FileGuid)) of
        {error, ?ENOENT} -> true;
        {ok, #file_attr{parent_guid = TrashGuid}} -> true;
        _ -> false
    end.


%% @private
%% Removes all top-level datasets in the space so that any protection flags
%% they carry no longer block the removal of leftover file trees. Unlike
%% onenv_dataset_test_utils:cleanup_all_datasets/1, it does not walk dataset
%% archives (transfer tests never create any, and listing them fails with
%% not_found for a never-archived dataset).
-spec remove_all_datasets(oct_background:entity_selector(), oct_background:entity_selector()) ->
    ok.
remove_all_datasets(SpaceSelector, UserSelector) ->
    SpaceId = oct_background:get_space_id(SpaceSelector),
    [ProviderSelector | _] = oct_background:get_space_supporting_providers(SpaceSelector),
    Node = oct_background:get_random_provider_node(ProviderSelector),
    SessId = oct_background:get_user_session_id(UserSelector, ProviderSelector),

    lists:foreach(fun(State) ->
        {ok, {Datasets, _IsLast}} = ?assertMatch({ok, {_, _}}, opt_datasets:list_top_datasets(
            Node, SessId, SpaceId, State, #{offset => 0, limit => 1000}
        )),
        lists:foreach(fun({DatasetId, _Name, _Index}) ->
            case opt_datasets:remove(Node, SessId, DatasetId) of
                ok ->
                    ok;
                {error, not_found} ->
                    % a stale structure entry whose backing dataset doc is
                    % already gone (e.g. its root file was moved to the trash
                    % but could not be purged) - it cannot and need not be
                    % removed, it only lingers harmlessly in the trash
                    ok;
                Other ->
                    ?assertEqual(ok, Other)
            end
        end, Datasets)
    end, [attached, detached]).


%% @private
-spec get_transfer_target_providers(suite_ctx()) ->
    {ReplicatingProviderId :: undefined | od_provider:id(), EvictingProviderId :: undefined | od_provider:id()}.
get_transfer_target_providers(#transfer_test_suite_ctx{
    transfer_type = TransferType,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}) ->
    CreationProviderId = oct_background:get_provider_id(CreationProviderSelector),
    OtherProviderId = oct_background:get_provider_id(OtherProviderSelector),

    case TransferType of
        replication -> {OtherProviderId, undefined};
        eviction -> {undefined, OtherProviderId};
        migration -> {OtherProviderId, CreationProviderId}
    end.


%% @private
-spec build_expected_transfer(suite_ctx(), file_tree_objects(), expected_transfer()) ->
    expected_transfer().
build_expected_transfer(#transfer_test_suite_ctx{
    transfer_type = TransferType,
    user_selector = UserSelector,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}, TransferRootObjects, Overrides) ->
    CreationProviderId = oct_background:get_provider_id(CreationProviderSelector),
    OtherProviderId = oct_background:get_provider_id(OtherProviderSelector),
    {FilesCount, DeclaredBytesCount} = count_files_and_bytes(TransferRootObjects),
    BytesExpectation = maps:get(tree_bytes, Overrides, DeclaredBytesCount),

    CommonExpectation = #{
        user_id => oct_background:get_user_id(UserSelector),
        scheduling_provider => CreationProviderId,
        failed_files => 0
    },
    TypeSpecificExpectation = case TransferType of
        replication ->
            maps:merge(#{
                replication_status => ?COMPLETED_STATUS,
                eviction_status => ?SKIPPED_STATUS,
                replicating_provider => OtherProviderId,
                evicting_provider => undefined,
                files_to_process => FilesCount,
                files_processed => FilesCount,
                files_replicated => FilesCount,
                bytes_replicated => BytesExpectation,
                files_evicted => 0
            }, build_expected_histograms(CreationProviderId, BytesExpectation));
        eviction ->
            maps:merge(#{
                replication_status => ?SKIPPED_STATUS,
                eviction_status => ?COMPLETED_STATUS,
                replicating_provider => undefined,
                evicting_provider => OtherProviderId,
                files_to_process => FilesCount,
                files_processed => FilesCount,
                files_replicated => 0,
                bytes_replicated => 0,
                files_evicted => FilesCount
            }, build_expected_histograms(CreationProviderId, 0));
        migration ->
            maps:merge(#{
                replication_status => ?COMPLETED_STATUS,
                eviction_status => ?COMPLETED_STATUS,
                replicating_provider => OtherProviderId,
                evicting_provider => CreationProviderId,
                % during migration every file is counted twice - once by the
                % replication and once by the eviction subtask
                files_to_process => 2 * FilesCount,
                files_processed => 2 * FilesCount,
                files_replicated => FilesCount,
                bytes_replicated => BytesExpectation,
                files_evicted => FilesCount
            }, build_expected_histograms(CreationProviderId, BytesExpectation))
    end,
    maps:merge(CommonExpectation, TypeSpecificExpectation).


%% @private
-spec build_expected_histograms(od_provider:id(), field_expectation()) ->
    expected_transfer().
build_expected_histograms(SourceProviderId, BytesExpectation) ->
    % transferred bytes are accounted per source provider; histogram window
    % sums (rather than exact slots) are asserted as slot boundaries shift
    % during the transfer
    HistExpectation = case BytesExpectation of
        0 -> #{};
        _ -> {histogram_sum, SourceProviderId, BytesExpectation}
    end,
    #{
        min_hist => HistExpectation,
        hr_hist => HistExpectation,
        dy_hist => HistExpectation,
        mth_hist => HistExpectation
    }.


%% @private
-spec count_files_and_bytes(file_tree_objects()) ->
    {non_neg_integer(), non_neg_integer()}.
count_files_and_bytes(Objects) when is_list(Objects) ->
    lists:foldl(fun(Object, {FilesCountAcc, BytesCountAcc}) ->
        {FilesCount, BytesCount} = count_files_and_bytes(Object),
        {FilesCountAcc + FilesCount, BytesCountAcc + BytesCount}
    end, {0, 0}, Objects);
count_files_and_bytes(#object{type = ?REGULAR_FILE_TYPE, content = Content}) ->
    {1, byte_size(Content)};
count_files_and_bytes(#object{type = ?DIRECTORY_TYPE, children = Children}) ->
    count_files_and_bytes(Children).


%% @private
-spec collect_regular_files(file_tree_objects()) ->
    [onenv_file_test_utils:object()].
collect_regular_files(Objects) when is_list(Objects) ->
    lists:flatmap(fun collect_regular_files/1, Objects);
collect_regular_files(#object{type = ?REGULAR_FILE_TYPE} = Object) ->
    [Object];
collect_regular_files(#object{type = ?DIRECTORY_TYPE, children = Children}) ->
    collect_regular_files(Children).


%% @private
-spec await_transfer_state(
    oct_background:entity_selector(), transfer:id(), expected_transfer(), non_neg_integer()
) ->
    ok | no_return().
await_transfer_state(ProviderSelector, TransferId, ExpectedTransfer, AttemptsLeft) ->
    case check_transfer_state(ProviderSelector, TransferId, ExpectedTransfer) of
        ok ->
            ok;
        _ when AttemptsLeft > 1 ->
            timer:sleep(timer:seconds(1)),
            await_transfer_state(ProviderSelector, TransferId, ExpectedTransfer, AttemptsLeft - 1);
        {error, _} = Error ->
            ct:pal("Transfer ~ts could not be fetched on provider ~tp due to: ~tp", [
                TransferId, ProviderSelector, Error
            ]),
            ct:fail(transfer_state_assertion_failed);
        {mismatched_transfer_fields, Mismatches, Transfer} ->
            ct:pal(
                "Transfer ~ts on provider ~tp did not reach the expected state.~n"
                "Mismatched fields:~n~ts~n"
                "Transfer record:~n~ts",
                [
                    TransferId, ProviderSelector,
                    format_mismatched_fields(Mismatches), format_transfer(Transfer)
                ]
            ),
            ct:fail(transfer_state_assertion_failed)
    end.


%% @private
-spec format_mismatched_fields([{atom(), field_expectation(), term()}]) -> iolist().
format_mismatched_fields(Mismatches) ->
    lists:map(fun({FieldName, FieldExpectation, Value}) ->
        io_lib:format("    ~tp:~n        expected: ~tp~n        actual:   ~tp~n", [
            FieldName, FieldExpectation, Value
        ])
    end, Mismatches).


%% @private
-spec format_transfer(transfer:transfer()) -> iolist().
format_transfer(Transfer) ->
    [_RecordTag | FieldValues] = tuple_to_list(Transfer),

    lists:map(fun({FieldName, Value}) ->
        io_lib:format("    ~tp = ~tp~n", [FieldName, Value])
    end, lists:zip(record_info(fields, transfer), FieldValues)).


%% @private
-spec check_transfer_state(oct_background:entity_selector(), transfer:id(), expected_transfer()) ->
    ok | {error, term()} | {mismatched_transfer_fields, [{atom(), field_expectation(), term()}], transfer:transfer()}.
check_transfer_state(ProviderSelector, TransferId, ExpectedTransfer) ->
    case opw_test_rpc:call(ProviderSelector, transfer, get, [TransferId]) of
        {ok, #document{value = Transfer}} ->
            case collect_mismatched_fields(ExpectedTransfer, Transfer) of
                [] -> ok;
                Mismatches -> {mismatched_transfer_fields, Mismatches, Transfer}
            end;
        {error, _} = Error ->
            Error
    end.


%% @private
-spec collect_mismatched_fields(expected_transfer(), transfer:transfer()) ->
    [{atom(), field_expectation(), term()}].
collect_mismatched_fields(ExpectedTransfer, Transfer) ->
    maps:fold(fun(FieldName, FieldExpectation, Acc) ->
        Value = get_transfer_field(FieldName, Transfer),
        case matches_expectation(FieldExpectation, Value) of
            true -> Acc;
            false -> [{FieldName, FieldExpectation, Value} | Acc]
        end
    end, [], ExpectedTransfer).


%% @private
-spec matches_expectation(field_expectation(), term()) -> boolean().
matches_expectation(skip, _Value) ->
    true;
matches_expectation({gte, Min}, Value) ->
    is_integer(Value) andalso Value >= Min;
matches_expectation({range, Min, Max}, Value) ->
    is_integer(Value) andalso Value >= Min andalso Value =< Max;
matches_expectation({histogram_sum, ProviderId, SumExpectation}, HistPerProvider) ->
    is_map(HistPerProvider) andalso case maps:find(ProviderId, HistPerProvider) of
        {ok, Hist} -> matches_expectation(SumExpectation, lists:sum(Hist));
        error -> false
    end;
matches_expectation(Predicate, Value) when is_function(Predicate, 1) ->
    Predicate(Value);
matches_expectation(ExpectedValue, Value) ->
    ExpectedValue =:= Value.


%% @private
-spec get_transfer_field(atom(), transfer:transfer()) -> term().
get_transfer_field(FieldName, Transfer) ->
    FieldsList = record_info(fields, transfer),
    case lists_utils:index_of(FieldName, FieldsList) of
        undefined -> error({invalid_transfer_field, FieldName, FieldsList});
        Index -> element(Index + 1, Transfer)
    end.
