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
%%%    type operates on (for replica eviction every tree file is read on the
%%%    other provider, which forces its replication; no-op otherwise);
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

-include("transfers/transfer_test.hrl").
-include("file/file_tree_test.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/onedata_file.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([
    remove_leftover_file_trees/2,
    create_file_tree/3,
    ensure_initial_replicas/2,
    schedule_transfer/2,
    schedule_view_transfer/3,

    create_view/5,
    await_view_query_result/4,
    remove_all_views/1,

    mock_gated_file_processing/1,
    grant_file_processing_permits/2,
    await_gated_file_processing_job/0,
    await_files_replicated/3,
    unmock_gated_file_processing/1,

    mock_file_processing_failure/1,
    disable_file_processing_failure/1,
    unmock_file_processing_failure/1,

    cancel_transfer/2,
    rerun_transfer/3,
    await_transfer_rerun_id/3,
    await_transfer_ended/4, await_transfer_ended/5,
    await_effective_replication_completed/3,
    assert_distribution/2, assert_distribution/3,
    assert_initial_distribution/2,
    await_distribution/3,
    remove_all_transfers/1
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

% how the transfer doc is fetched: 'get' returns the doc of exactly the given
% transfer, 'get_effective' follows `rerun_id` links to the doc of the transfer
% that actually continues the job (a transfer rerun manually or restarted by
% the provider after its own restart lives on under a new id)
-type transfer_getter() :: get | get_effective.

% transferred files given either as a (nested) file tree or a flat list of
% its objects (e.g. the subset of tree files matched by a view)
-type file_tree_objects() ::
    file_tree_test_utils:object() | [file_tree_test_utils:object()].

-type suite_ctx() :: #transfer_test_suite_ctx{}.

-export_type([
    suite_ctx/0,
    transfer_type/0, field_expectation/0, expected_transfer/0, transfer_getter/0,
    file_tree_objects/0
]).

-define(SPACE_ROOT_LS_LIMIT, 10000).

% how much longer to poll a transfer that has already ended but does not match
% the expectations - enough for the trailing dbsync revisions of the end state,
% while orders of magnitude less than the full attempt budgets
-define(ENDED_TRANSFER_GRACE_ATTEMPTS, 10).

% gate suspending the transfer's file jobs (see permit_gate_test_utils)
-define(FILE_PROCESSING_GATE, transfer_file_processing).

-define(FILE_PROCESSING_FAILURE_ENABLED_KEY, file_processing_failure_enabled).


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
    {ok, Children} = file_tree_test_utils:ls(UserSelector, SpaceSelector, 0, ?SPACE_ROOT_LS_LIMIT),
    CaseNamePrefix = <<(atom_to_binary(CaseName, utf8))/binary, "_">>,

    LeftoverTreeGuids = [ChildGuid || {ChildGuid, ChildName} <- Children,
        str_utils:binary_starts_with(ChildName, CaseNamePrefix)],
    lists_utils:pforeach(fun(ChildGuid) ->
        rm_leftover_file_tree(SpaceSelector, UserSelector, ChildGuid)
    end, LeftoverTreeGuids).


-spec create_file_tree(suite_ctx(), atom(), file_tree_test_utils:object_spec()) ->
    file_tree_test_utils:object().
create_file_tree(#transfer_test_suite_ctx{
    space_selector = SpaceSelector,
    user_selector = UserSelector,
    creation_provider_selector = CreationProviderSelector
}, CaseName, #dir_spec{} = RootDirSpec) ->
    RootDirName = str_utils:format_bin("~ts_~ts", [CaseName, str_utils:rand_hex(6)]),
    file_tree_test_utils:create_and_sync_file_tree(
        UserSelector, SpaceSelector,
        RootDirSpec#dir_spec{name = RootDirName},
        CreationProviderSelector
    ).


%%--------------------------------------------------------------------
%% @doc
%% Ensures the initial replicas required by the suite's transfer type exist
%% before the tested transfer is scheduled: replica eviction operates on
%% a file tree already replicated to the other provider, so every tree file
%% is read there, which forces its replication.
%% For the other transfer types the creation provider replicas suffice.
%%
%% Note that the file sizes are resolved by the helper from the creation
%% provider rather than taken from the declared tree content - the latter does
%% not cover data written outside of the tree spec (e.g. by the big file test),
%% and a file read with size 0 would end up with no replica registered, so its
%% eviction would then honestly evict nothing.
%% @end
%%--------------------------------------------------------------------
-spec ensure_initial_replicas(suite_ctx(), file_tree_test_utils:object()) -> ok.
ensure_initial_replicas(#transfer_test_suite_ctx{
    transfer_type = eviction,
    user_selector = UserSelector,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}, RootObject) ->
    CreationNode = oct_background:get_random_provider_node(CreationProviderSelector),
    OtherNode = oct_background:get_random_provider_node(OtherProviderSelector),
    OtherSessionId = oct_background:get_user_session_id(UserSelector, OtherProviderSelector),

    file_test_utils:replicate_by_read(CreationNode, OtherNode, OtherSessionId, [
        FileGuid || #object{guid = FileGuid} <- collect_regular_files(RootObject)
    ]);
ensure_initial_replicas(_TestSuiteCtx, _TransferRootObject) ->
    ok.


-spec schedule_transfer(suite_ctx(), file_tree_test_utils:object()) ->
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


%%--------------------------------------------------------------------
%% @doc
%% Creates a view on the providers that evaluate it when processing
%% transfers by view (see get_view_evaluating_provider_selectors/1) and
%% awaits the view doc dbsync on the creation (scheduling) provider.
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
create_view(TestSuiteCtx = #transfer_test_suite_ctx{
    space_selector = SpaceSelector,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}, ViewName, MapFunction, ReduceFunction, ViewOptions) ->
    SpaceId = oct_background:get_space_id(SpaceSelector),

    ok = view_test_utils:create_view(OtherProviderSelector, SpaceId, ViewName, #{
        map_function => MapFunction,
        reduce_function => ReduceFunction,
        options => ViewOptions,
        providers => get_view_evaluating_provider_selectors(TestSuiteCtx)
    }),
    view_test_utils:await_view_synced(CreationProviderSelector, SpaceId, ViewName).


%%--------------------------------------------------------------------
%% @doc
%% Awaits the view emitting exactly the expected values (in any order)
%% for the given query on every provider that evaluates the view when
%% processing transfers by view (the xattrs driving the emissions are set
%% on the other provider, so on the creation provider - queried only for
%% migration - the emissions additionally await the metadata dbsync).
%% @end
%%--------------------------------------------------------------------
-spec await_view_query_result(suite_ctx(), index:name(), index:options(), [term()]) ->
    ok.
await_view_query_result(TestSuiteCtx = #transfer_test_suite_ctx{
    space_selector = SpaceSelector
}, ViewName, QueryOptions, ExpectedValues) ->
    view_test_utils:await_query_result(
        get_view_evaluating_provider_selectors(TestSuiteCtx),
        oct_background:get_space_id(SpaceSelector),
        ViewName, QueryOptions, ExpectedValues
    ).


%%--------------------------------------------------------------------
%% @doc
%% Removes all views of the space, on every supporting provider. Called
%% as part of the leftover cleanup - view test cases create views with
%% per-run random names, which would otherwise accumulate.
%% @end
%%--------------------------------------------------------------------
-spec remove_all_views(suite_ctx()) -> ok.
remove_all_views(#transfer_test_suite_ctx{space_selector = SpaceSelector}) ->
    view_test_utils:remove_all_views(
        oct_background:get_space_supporting_providers(SpaceSelector),
        oct_background:get_space_id(SpaceSelector)
    ).


%%--------------------------------------------------------------------
%% @doc
%% Mocks the file jobs of the suite ctx's transfer type (see
%% get_file_processing_worker_module/1) so that every job must acquire a
%% permit before processing its file. A job finding no free permit parks
%% (notifying the calling process - see await_gated_file_processing_job/0)
%% until one is granted with grant_file_processing_permits/2; no permits
%% are available initially. This allows deterministically suspending an
%% ongoing transfer and releasing it in stages, interleaved with other
%% operations.
%% Must be paired with unmock_gated_file_processing/1 in the test teardown.
%% @end
%%--------------------------------------------------------------------
-spec mock_gated_file_processing(suite_ctx()) -> ok.
mock_gated_file_processing(SuiteCtx) ->
    TestProcess = self(),
    Nodes = get_file_processing_nodes(SuiteCtx),
    ok = permit_gate_test_utils:install(?FILE_PROCESSING_GATE, Nodes),

    WorkerModule = get_file_processing_worker_module(SuiteCtx),
    ok = test_utils:mock_new(Nodes, WorkerModule, [passthrough]),
    ok = test_utils:mock_expect(Nodes, WorkerModule, transfer_regular_file, fun(
        FileCtx, TransferParams
    ) ->
        permit_gate_test_utils:acquire_permit(?FILE_PROCESSING_GATE, TestProcess),
        meck:passthrough([FileCtx, TransferParams])
    end).


-spec grant_file_processing_permits(suite_ctx(), pos_integer() | all) ->
    ok.
grant_file_processing_permits(SuiteCtx, CountOrAll) ->
    permit_gate_test_utils:grant_permits(
        ?FILE_PROCESSING_GATE, get_file_processing_nodes(SuiteCtx), CountOrAll
    ).


%%--------------------------------------------------------------------
%% @doc
%% Awaits the notification a gated file processing job sends when it parks
%% awaiting a permit - proof that the transfer traverse is underway and
%% suspended. Only jobs that actually park notify, so leftover mailbox
%% messages cannot produce a false positive for an already-drained gate.
%% @end
%%--------------------------------------------------------------------
-spec await_gated_file_processing_job() -> ok.
await_gated_file_processing_job() ->
    permit_gate_test_utils:await_parked_job(?FILE_PROCESSING_GATE, ?TRANSFER_ATTEMPTS).


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
    end, ?TRANSFER_ATTEMPTS).


-spec unmock_gated_file_processing(suite_ctx()) -> ok.
unmock_gated_file_processing(SuiteCtx) ->
    permit_gate_test_utils:uninstall(
        ?FILE_PROCESSING_GATE,
        get_file_processing_nodes(SuiteCtx),
        get_file_processing_worker_module(SuiteCtx)
    ).


%%--------------------------------------------------------------------
%% @doc
%% Mocks the file jobs of the suite ctx's transfer type (see
%% get_file_processing_worker_module/1) so that every job fails: a
%% replication job fails its block synchronization request, an eviction
%% job fails its local block deletion. The failures can be turned off
%% without unloading the mock with disable_file_processing_failure/1
%% (e.g. before asserting that a rerun of a failed transfer succeeds).
%% Must be paired with unmock_file_processing_failure/1 in the test
%% teardown.
%% @end
%%--------------------------------------------------------------------
-spec mock_file_processing_failure(suite_ctx()) -> ok.
mock_file_processing_failure(SuiteCtx = #transfer_test_suite_ctx{transfer_type = TransferType}) ->
    Nodes = get_file_processing_nodes(SuiteCtx),
    lists:foreach(fun(Node) ->
        ok = opw_test_rpc:call(Node, node_cache, put, [?FILE_PROCESSING_FAILURE_ENABLED_KEY, true])
    end, Nodes),

    case TransferType of
        eviction ->
            ok = test_utils:mock_new(Nodes, replica_deletion_req, [passthrough]),
            ok = test_utils:mock_expect(Nodes, replica_deletion_req, delete_blocks, fun(
                FileCtx, Blocks, AllowedVV
            ) ->
                case node_cache:get(?FILE_PROCESSING_FAILURE_ENABLED_KEY, false) of
                    true -> {error, test_error};
                    false -> meck:passthrough([FileCtx, Blocks, AllowedVV])
                end
            end);
        _ ->
            ok = test_utils:mock_new(Nodes, replica_synchronizer, [passthrough]),
            ok = test_utils:mock_expect(Nodes, replica_synchronizer, synchronize, fun(
                UserCtx, FileCtx, Block, Prefetch, TransferId, Priority, StatsCallbackModule
            ) ->
                case node_cache:get(?FILE_PROCESSING_FAILURE_ENABLED_KEY, false) of
                    true ->
                        throw(test_error);
                    false ->
                        meck:passthrough([
                            UserCtx, FileCtx, Block, Prefetch, TransferId, Priority,
                            StatsCallbackModule
                        ])
                end
            end)
    end.


-spec disable_file_processing_failure(suite_ctx()) -> ok.
disable_file_processing_failure(SuiteCtx) ->
    lists:foreach(fun(Node) ->
        ok = opw_test_rpc:call(Node, node_cache, put, [?FILE_PROCESSING_FAILURE_ENABLED_KEY, false])
    end, get_file_processing_nodes(SuiteCtx)).


-spec unmock_file_processing_failure(suite_ctx()) -> ok.
unmock_file_processing_failure(SuiteCtx = #transfer_test_suite_ctx{transfer_type = TransferType}) ->
    Nodes = get_file_processing_nodes(SuiteCtx),
    MockedModule = case TransferType of
        eviction -> replica_deletion_req;
        _ -> replica_synchronizer
    end,
    % see the analogous await in permit_gate_test_utils:uninstall/3
    permit_gate_test_utils:await_no_ongoing_calls_within_module(Nodes, MockedModule),
    test_utils:mock_unload(Nodes, MockedModule),
    lists:foreach(fun(Node) ->
        ok = opw_test_rpc:call(Node, node_cache, clear, [?FILE_PROCESSING_FAILURE_ENABLED_KEY])
    end, Nodes).


%%--------------------------------------------------------------------
%% @doc
%% Cancels the given ongoing transfer. The request is made on the other
%% provider - the one executing the gated file jobs (see
%% get_file_processing_worker_module/1) - so that the cancellation takes
%% effect without waiting for dbsync; the operation itself is accepted
%% on any provider supporting the space.
%% @end
%%--------------------------------------------------------------------
-spec cancel_transfer(suite_ctx(), transfer:id()) -> ok.
cancel_transfer(#transfer_test_suite_ctx{
    other_provider_selector = OtherProviderSelector
}, TransferId) ->
    ?assertEqual(ok, opw_test_rpc:call(OtherProviderSelector, transfer, cancel, [TransferId])).


%%--------------------------------------------------------------------
%% @doc
%% Reruns the given ended transfer as the given user and returns the id
%% of the new transfer created this way. The request is made on the other
%% provider - any provider supporting the space may rerun a transfer, and
%% picking the one that did not schedule the original also exercises the
%% scheduling provider switch: the new transfer doc is attributed to the
%% rerunning provider and user, so callers must expect them accordingly.
%% @end
%%--------------------------------------------------------------------
-spec rerun_transfer(suite_ctx(), oct_background:entity_selector(), transfer:id()) ->
    transfer:id().
rerun_transfer(#transfer_test_suite_ctx{
    other_provider_selector = OtherProviderSelector
}, RerunningUserSelector, TransferId) ->
    UserId = oct_background:get_user_id(RerunningUserSelector),
    {ok, NewTransferId} = ?assertMatch({ok, _}, opw_test_rpc:call(
        OtherProviderSelector, transfer, rerun_ended, [UserId, TransferId]
    )),
    NewTransferId.


%%--------------------------------------------------------------------
%% @doc
%% Awaits the rerun linkage of a rerun transfer: its doc points to the
%% new transfer with rerun_id (set by the rerunning provider, reaching
%% the other one via dbsync).
%% @end
%%--------------------------------------------------------------------
-spec await_transfer_rerun_id(suite_ctx(), transfer:id(), transfer:id()) -> ok.
await_transfer_rerun_id(#transfer_test_suite_ctx{
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}, TransferId, ExpRerunId) ->
    lists:foreach(fun(ProviderSelector) ->
        ?assertEqual(ExpRerunId, case opw_test_rpc:call(
            ProviderSelector, transfer, get, [TransferId]
        ) of
            {ok, #document{value = #transfer{rerun_id = RerunId}}} -> RerunId;
            {error, _} = Error -> Error
        end, ?TRANSFER_ATTEMPTS)
    end, [CreationProviderSelector, OtherProviderSelector]).


-spec await_transfer_ended(suite_ctx(), transfer:id(), file_tree_objects(), expected_transfer()) ->
    ok.
await_transfer_ended(SuiteCtx, TransferId, TransferRootObjects, Overrides) ->
    await_transfer_ended(SuiteCtx, TransferId, TransferRootObjects, Overrides, ?TRANSFER_ATTEMPTS).


-spec await_transfer_ended(
    suite_ctx(),
    transfer:id(),
    file_tree_objects(),
    expected_transfer(),
    non_neg_integer()
) ->
    ok.
await_transfer_ended(#transfer_test_suite_ctx{
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
} = SuiteCtx, TransferId, TransferRootObjects, Overrides, Attempts) ->
    ExpectedTransfer = maps:merge(
        build_expected_transfer(SuiteCtx, TransferRootObjects, Overrides),
        maps:remove(tree_bytes, Overrides)
    ),

    lists:foreach(fun(ProviderSelector) ->
        case await_transfer_state(ProviderSelector, TransferId, ExpectedTransfer, get, Attempts) of
            ok ->
                ok;
            {failed, {error, _} = Error} ->
                ct:pal("Transfer ~ts could not be fetched on provider ~tp due to: ~tp", [
                    TransferId, ProviderSelector, Error
                ]),
                ct:fail(transfer_state_assertion_failed);
            {failed, {mismatched_transfer_fields, Mismatches, Transfer}} ->
                ct:pal(
                    "Transfer ~ts on provider ~tp did not reach the expected state.~n"
                    "Mismatched fields:~n~ts~n"
                    "Transfer record:~n~ts",
                    [
                        TransferId, ProviderSelector,
                        format_mismatched_fields(Mismatches), format_transfer(Transfer)
                    ]
                ),
                pal_current_file_distributions(SuiteCtx, TransferRootObjects),
                ct:fail(transfer_state_assertion_failed)
        end
    end, [CreationProviderSelector, OtherProviderSelector]).


%%--------------------------------------------------------------------
%% @doc
%% Awaits the replication part of the given transfer reaching the 'completed'
%% status on the given provider, following `rerun_id` links - a transfer
%% interrupted by a provider restart is resumed under a new id.
%%
%% Unlike await_transfer_ended/4,5 this asserts nothing but the status, so it
%% does not need a suite ctx nor the transferred file tree - for tests that
%% only need the transfer to be done before checking its effects.
%% @end
%%--------------------------------------------------------------------
-spec await_effective_replication_completed(
    oct_background:node_selector(), transfer:id(), non_neg_integer()
) ->
    ok.
await_effective_replication_completed(ProviderSelector, TransferId, Attempts) ->
    ExpectedTransfer = #{replication_status => ?COMPLETED_STATUS},

    case await_transfer_state(
        ProviderSelector, TransferId, ExpectedTransfer, get_effective, Attempts
    ) of
        ok ->
            ok;
        {failed, {error, _} = Error} ->
            ct:pal("Transfer ~ts could not be fetched on provider ~tp due to: ~tp", [
                TransferId, ProviderSelector, Error
            ]),
            ct:fail(replication_not_completed);
        {failed, {mismatched_transfer_fields, _, Transfer}} ->
            ct:pal(
                "Replication of transfer ~ts did not complete on provider ~tp.~n"
                "Transfer record:~n~ts",
                [TransferId, ProviderSelector, format_transfer(Transfer)]
            ),
            ct:fail(replication_not_completed)
    end.


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

    FilesWithExpDistribution = lists:map(fun(#object{guid = FileGuid, content = Content}) ->
        FileSize = maps:get(FileGuid, FileSizeOverrides, byte_size(Content)),
        ExpSizePerNode = case TransferType of
            replication -> [{CreationNode, FileSize}, {OtherNode, FileSize}];
            eviction -> [{CreationNode, FileSize}, {OtherNode, 0}];
            migration -> [{CreationNode, 0}, {OtherNode, FileSize}]
        end,
        {FileGuid, ExpSizePerNode}
    end, collect_regular_files(TransferRootObjects)),

    lists_utils:pforeach(fun({FileGuid, ExpSizePerNode}) ->
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
    lists_utils:pforeach(fun(#object{guid = FileGuid, content = Content}) ->
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
%% type (e.g. after a failed or no-op transfer). A provider expectation
%% given as a plain size stands for one contiguous block - an explicit
%% block list must be given for a fragmented replica.
%% NOTE: every regular file created with create_file_tree/3 has an empty
%% (zero blocks) distribution entry on each supporting provider - the
%% creation sync awaits it - so the expectation must list such providers
%% with size 0 rather than omit them.
%% @end
%%--------------------------------------------------------------------
-spec await_distribution(
    [oct_background:entity_selector()],
    file_id:file_guid(),
    [{oct_background:entity_selector(), file_meta:size() | [fslogic_blocks:block()]}]
) ->
    ok.
await_distribution(ProviderSelectors, FileGuid, ExpSizeOrBlocksPerProvider) ->
    file_test_utils:await_distribution(
        [oct_background:get_random_provider_node(PS) || PS <- ProviderSelectors],
        FileGuid,
        [
            {oct_background:get_random_provider_node(PS), ExpSizeOrBlocks}
            || {PS, ExpSizeOrBlocks} <- ExpSizeOrBlocksPerProvider
        ]
    ).


-spec remove_all_transfers(suite_ctx()) -> ok.
remove_all_transfers(#transfer_test_suite_ctx{
    space_selector = SpaceSelector,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}) ->
    SpaceId = oct_background:get_space_id(SpaceSelector),

    % the providers are cleaned SEQUENTIALLY and each deletion tolerates an
    % already-missing doc: both providers list mostly the same transfers (the
    % transfer links are dbsync-synced), so the second provider's listing may
    % still contain transfers the first pass just deleted (its view catches
    % up only after a dbsync propagation delay)
    lists:foreach(fun(ProviderSelector) ->
        lists:foreach(fun(ListingFun) ->
            {ok, TransferIds} = opw_test_rpc:call(ProviderSelector, transfer, ListingFun, [SpaceId]),
            lists_utils:pforeach(fun(TransferId) ->
                delete_transfer_ignoring_missing_doc(ProviderSelector, TransferId)
            end, TransferIds)
        end, [list_waiting_transfers, list_ongoing_transfers, list_ended_transfers])
    end, [CreationProviderSelector, OtherProviderSelector]).


%% @private
%% transfer:delete/1 is not idempotent - it badmatches on getting the doc if
%% it no longer exists (deleted concurrently or listed from a stale view)
-spec delete_transfer_ignoring_missing_doc(oct_background:entity_selector(), transfer:id()) ->
    ok.
delete_transfer_ignoring_missing_doc(ProviderSelector, TransferId) ->
    ok = opw_test_rpc:call(ProviderSelector, fun() ->
        try
            transfer:delete(TransferId)
        catch
            error:{badmatch, {error, not_found}} ->
                ok
        end
    end).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
%% The transfer_traverse_worker callback module whose file jobs the gate and
%% failure mocks intercept: replication_worker for replication and migration
%% (each file of a migration is replicated before being evicted, so gating or
%% failing the replication jobs suspends or fails the whole transfer),
%% replica_eviction_worker for eviction. For every transfer type the provider
%% executing the intercepted jobs (the replicating or the evicting one
%% respectively) is the other provider of the suite ctx layout.
-spec get_file_processing_worker_module(suite_ctx()) -> module().
get_file_processing_worker_module(#transfer_test_suite_ctx{transfer_type = eviction}) ->
    replica_eviction_worker;
get_file_processing_worker_module(#transfer_test_suite_ctx{}) ->
    replication_worker.


%% @private
-spec get_file_processing_nodes(suite_ctx()) -> [node()].
get_file_processing_nodes(#transfer_test_suite_ctx{
    other_provider_selector = OtherProviderSelector
}) ->
    oct_background:get_provider_nodes(OtherProviderSelector).


%% @private
%% Removes a leftover file tree. Only the removing provider is awaited
%% (lfm_proxy:rm_recursive synchronously moves the tree to the trash; the
%% purge from the trash is asynchronous and size-proportional). The tree
%% vanishing from the other providers' view of the space is NOT awaited -
%% the tree created by the current case run carries a fresh random suffix
%% in its name (see create_file_tree/3), so a lingering not-yet-synced-away
%% leftover cannot collide with it, while the await would hostage the setup
%% to the very dbsync backlog the trash purge generates.
-spec rm_leftover_file_tree(
    oct_background:entity_selector(), oct_background:entity_selector(), file_id:file_guid()
) ->
    ok.
rm_leftover_file_tree(SpaceSelector, UserSelector, FileGuid) ->
    RmProvider = lists_utils:random_element(
        oct_background:get_space_supporting_providers(SpaceSelector)
    ),
    RmNode = oct_background:get_random_provider_node(RmProvider),
    RmSessId = oct_background:get_user_session_id(UserSelector, RmProvider),
    ?assertMatch(ok, lfm_proxy:rm_recursive(RmNode, RmSessId, ?FILE_REF(FileGuid))),
    ok.


%% @private
%% Removes all top-level datasets in the space so that any protection flags
%% they carry no longer block the removal of leftover file trees. Unlike
%% dataset_test_utils:cleanup_all_datasets/1, it does not walk dataset
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
%% Providers that evaluate views when processing transfers by view. Both the
%% replication and the eviction (sub)task query the view LOCALLY on the
%% provider executing it (a view is instantiated only on the providers it was
%% created with - on the others its processing completes as a no-op), so for
%% migration - whose eviction phase runs on the creation provider - the view
%% must be evaluated on both providers.
-spec get_view_evaluating_provider_selectors(suite_ctx()) ->
    [oct_background:entity_selector()].
get_view_evaluating_provider_selectors(#transfer_test_suite_ctx{
    transfer_type = migration,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}) ->
    [CreationProviderSelector, OtherProviderSelector];
get_view_evaluating_provider_selectors(#transfer_test_suite_ctx{
    other_provider_selector = OtherProviderSelector
}) ->
    [OtherProviderSelector].


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
    [file_tree_test_utils:object()].
collect_regular_files(Objects) when is_list(Objects) ->
    lists:flatmap(fun collect_regular_files/1, Objects);
collect_regular_files(#object{type = ?REGULAR_FILE_TYPE} = Object) ->
    [Object];
collect_regular_files(#object{type = ?DIRECTORY_TYPE, children = Children}) ->
    collect_regular_files(Children).


%% @private
%% Polls the transfer doc on the given provider until it matches the
%% expectations. A transfer that has already ended will never change again -
%% only trailing dbsync revisions of the same end state (e.g. the other
%% migration subtask's fields or a late histogram flush) may still arrive -
%% so once an ended yet mismatched transfer is seen, the remaining attempts
%% are capped to a short grace instead of idling through the full (potentially
%% minutes-long) attempt budget.
-spec await_transfer_state(
    oct_background:node_selector(), transfer:id(), expected_transfer(), transfer_getter(),
    non_neg_integer()
) ->
    ok | {failed, {error, term()} | {mismatched_transfer_fields, list(), transfer:transfer()}}.
await_transfer_state(ProviderSelector, TransferId, ExpectedTransfer, GetFun, AttemptsLeft) ->
    Result = check_transfer_state(ProviderSelector, TransferId, ExpectedTransfer, GetFun),

    EffectiveAttemptsLeft = case Result of
        {mismatched_transfer_fields, _, Transfer} ->
            case is_transfer_ended(Transfer) of
                true -> min(AttemptsLeft, ?ENDED_TRANSFER_GRACE_ATTEMPTS);
                false -> AttemptsLeft
            end;
        _ ->
            AttemptsLeft
    end,

    case Result of
        ok ->
            ok;
        _ when EffectiveAttemptsLeft > 1 ->
            timer:sleep(timer:seconds(1)),
            await_transfer_state(
                ProviderSelector, TransferId, ExpectedTransfer, GetFun, EffectiveAttemptsLeft - 1
            );
        Failure ->
            {failed, Failure}
    end.


%% @private
-spec is_transfer_ended(transfer:transfer()) -> boolean().
is_transfer_ended(Transfer) ->
    EndedStatuses = [?COMPLETED_STATUS, ?FAILED_STATUS, ?CANCELLED_STATUS, ?SKIPPED_STATUS],
    lists:member(get_transfer_field(replication_status, Transfer), EndedStatuses) andalso
        lists:member(get_transfer_field(eviction_status, Transfer), EndedStatuses).


%% @private
%% One-shot dump of the current distribution of every regular file of the
%% transferred tree, as seen by each provider - printed when a transfer state
%% assertion fails, so that a file the transfer silently omitted (e.g. an
%% eviction skipping an opened file counts it as processed but not evicted,
%% with a debug-level log as the only product-side trace) can be identified
%% immediately.
-spec pal_current_file_distributions(suite_ctx(), file_tree_objects()) -> ok.
pal_current_file_distributions(#transfer_test_suite_ctx{
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}, TransferRootObjects) ->
    case collect_regular_files(TransferRootObjects) of
        [] ->
            ok;
        FileObjects ->
            Nodes = [
                oct_background:get_random_provider_node(CreationProviderSelector),
                oct_background:get_random_provider_node(OtherProviderSelector)
            ],
            FormattedFileEntries = lists:map(fun(#object{guid = FileGuid, name = FileName}) ->
                FormattedNodeViews = lists:map(fun(Node) ->
                    Distribution = try
                        {ok, D} = opt_file_metadata:get_distribution_deprecated(
                            Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid)
                        ),
                        lists:sort(D)
                    catch Class:Reason ->
                        {failed_to_fetch_distribution, Class, Reason}
                    end,
                    io_lib:format("        as seen by ~tp: ~tp~n", [Node, Distribution])
                end, Nodes),
                io_lib:format("    ~ts:~n~ts", [FileName, FormattedNodeViews])
            end, FileObjects),
            ct:pal("Current file distributions:~n~ts", [FormattedFileEntries])
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
-spec check_transfer_state(
    oct_background:node_selector(), transfer:id(), expected_transfer(), transfer_getter()
) ->
    ok | {error, term()} | {mismatched_transfer_fields, [{atom(), field_expectation(), term()}], transfer:transfer()}.
check_transfer_state(ProviderSelector, TransferId, ExpectedTransfer, GetFun) ->
    case opw_test_rpc:call(ProviderSelector, transfer, GetFun, [TransferId]) of
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
    % a provider absent from the histogram map has transferred no bytes
    % (e.g. a transfer cancelled before any data was fetched)
    is_map(HistPerProvider) andalso matches_expectation(
        SumExpectation, lists:sum(maps:get(ProviderId, HistPerProvider, []))
    );
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
