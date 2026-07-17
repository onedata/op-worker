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

-include("transfer_test.hrl").
-include("onenv_test_utils.hrl").
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

-define(PREREPLICATION_READ_CHUNK_SIZE, 33554432).  % 32 MiB
% TODO VFS-XXX remove the replica registration verification (the two defines
% below and the helpers marked with this ticket) once the lost file_location
% update after an on-read replication is fixed
-define(PREREPLICATION_MAX_READS_PER_FILE, 3).
-define(REPLICA_REGISTRATION_AWAIT_ATTEMPTS, 15).

% how much longer to poll a transfer that has already ended but does not match
% the expectations - enough for the trailing dbsync revisions of the end state,
% while orders of magnitude less than the full attempt budgets
-define(ENDED_TRANSFER_GRACE_ATTEMPTS, 10).

-define(FILE_PROCESSING_PERMITS_KEY, file_processing_permits).
-define(FILE_PROCESSING_JOB_GATED_MSG, file_processing_job_gated).
-define(FILE_PROCESSING_PERMIT_POLL_INTERVAL_MS, 100).
-define(INFINITE_PERMITS, 1 bsl 50).

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
    {ok, Children} = onenv_file_test_utils:ls(UserSelector, SpaceSelector, 0, ?SPACE_ROOT_LS_LIMIT),
    CaseNamePrefix = <<(atom_to_binary(CaseName, utf8))/binary, "_">>,

    LeftoverTreeGuids = [ChildGuid || {ChildGuid, ChildName} <- Children,
        str_utils:binary_starts_with(ChildName, CaseNamePrefix)],
    lists_utils:pforeach(fun(ChildGuid) ->
        rm_leftover_file_tree(SpaceSelector, UserSelector, ChildGuid)
    end, LeftoverTreeGuids).


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
%% a file tree already replicated to the other provider, so every tree file
%% is read (in parallel) on that provider, which forces its replication.
%% Each replication is verified against the replica metadata and retried
%% if its block registration got lost - see
%% replicate_file_and_verify_replica_registration/5.
%% For the other transfer types the creation provider replicas suffice.
%% @end
%%--------------------------------------------------------------------
-spec ensure_initial_replicas(suite_ctx(), onenv_file_test_utils:object()) -> ok.
ensure_initial_replicas(#transfer_test_suite_ctx{
    transfer_type = eviction,
    user_selector = UserSelector,
    other_provider_selector = OtherProviderSelector
}, RootObject) ->
    OtherNode = oct_background:get_random_provider_node(OtherProviderSelector),
    OtherProviderId = oct_background:get_provider_id(OtherProviderSelector),
    SessionId = oct_background:get_user_session_id(UserSelector, OtherProviderSelector),

    lists_utils:pforeach(fun(#object{guid = FileGuid}) ->
        replicate_file_and_verify_replica_registration(
            OtherNode, OtherProviderId, SessionId, FileGuid,
            ?PREREPLICATION_MAX_READS_PER_FILE
        )
    end, collect_regular_files(RootObject));
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
    EvaluatingProviderIds = lists:map(
        fun oct_background:get_provider_id/1,
        get_view_evaluating_provider_selectors(TestSuiteCtx)
    ),

    ok = opw_test_rpc:call(OtherProviderSelector, index, save, [
        SpaceId, ViewName, MapFunction, ReduceFunction, ViewOptions,
        false, EvaluatingProviderIds
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
    SpaceId = oct_background:get_space_id(SpaceSelector),

    lists_utils:pforeach(fun(ProviderSelector) ->
        ?assertEqual(lists:sort(ExpectedValues), try
            {ok, #{<<"rows">> := Rows}} = opw_test_rpc:call(ProviderSelector, index, query, [
                SpaceId, ViewName, QueryOptions
            ]),
            lists:sort(lists:flatmap(fun(Row) ->
                lists:flatten([maps:get(<<"value">>, Row)])
            end, Rows))
        catch _:_ ->
            query_failed
        end, ?ATTEMPTS)
    end, get_view_evaluating_provider_selectors(TestSuiteCtx)).


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

    lists_utils:pforeach(fun(ProviderSelector) ->
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

    lists:foreach(fun(Node) ->
        % the permit counter must be created on the provider node (atomics are
        % node-local); the node-wide cache entry keeps the ref alive (an ets
        % table would die with its owner - the transient rpc process)
        ok = opw_test_rpc:call(Node, fun() ->
            node_cache:put(?FILE_PROCESSING_PERMITS_KEY, atomics:new(1, []))
        end)
    end, Nodes),

    WorkerModule = get_file_processing_worker_module(SuiteCtx),
    ok = test_utils:mock_new(Nodes, WorkerModule, [passthrough]),
    ok = test_utils:mock_expect(Nodes, WorkerModule, transfer_regular_file, fun(
        FileCtx, TransferParams
    ) ->
        acquire_file_processing_permit(TestProcess),
        meck:passthrough([FileCtx, TransferParams])
    end).


-spec grant_file_processing_permits(suite_ctx(), pos_integer() | all) ->
    ok.
grant_file_processing_permits(SuiteCtx, CountOrAll) ->
    % permits are granted per provider node (irrelevant for the single-node
    % providers the transfer suites run on)
    lists:foreach(fun(Node) ->
        ok = opw_test_rpc:call(Node, fun() ->
            PermitsRef = node_cache:get(?FILE_PROCESSING_PERMITS_KEY),
            case CountOrAll of
                all -> atomics:put(PermitsRef, 1, ?INFINITE_PERMITS);
                Count -> atomics:add(PermitsRef, 1, Count)
            end
        end)
    end, get_file_processing_nodes(SuiteCtx)).


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
    receive
        ?FILE_PROCESSING_JOB_GATED_MSG -> ok
    after timer:seconds(?ATTEMPTS) ->
        ct:fail(no_file_processing_job_awaiting_permit)
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


-spec unmock_gated_file_processing(suite_ctx()) -> ok.
unmock_gated_file_processing(SuiteCtx) ->
    % release any still-parked jobs first - a process parked inside the mock
    % call would be killed by the code purge on unload
    grant_file_processing_permits(SuiteCtx, all),
    Nodes = get_file_processing_nodes(SuiteCtx),
    test_utils:mock_unload(Nodes, get_file_processing_worker_module(SuiteCtx)),
    lists:foreach(fun(Node) ->
        ok = opw_test_rpc:call(Node, node_cache, clear, [?FILE_PROCESSING_PERMITS_KEY])
    end, Nodes).


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
        end, ?ATTEMPTS)
    end, [CreationProviderSelector, OtherProviderSelector]).


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
        case await_transfer_state(ProviderSelector, TransferId, ExpectedTransfer, Attempts) of
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

    lists_utils:pforeach(fun({_FileName, FileGuid, ExpSizePerNode}) ->
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

    lists_utils:pforeach(fun(ProviderSelector) ->
        lists:foreach(fun(ListingFun) ->
            {ok, TransferIds} = opw_test_rpc:call(ProviderSelector, transfer, ListingFun, [SpaceId]),
            lists_utils:pforeach(fun(TransferId) ->
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
%% Executed on the provider nodes within the gate-mocked file processing job
%% processes (see mock_gated_file_processing/1).
-spec acquire_file_processing_permit(pid()) -> ok.
acquire_file_processing_permit(TestProcess) ->
    PermitsRef = node_cache:get(?FILE_PROCESSING_PERMITS_KEY),
    case try_acquire_file_processing_permit(PermitsRef) of
        true ->
            ok;
        false ->
            TestProcess ! ?FILE_PROCESSING_JOB_GATED_MSG,
            wait_for_file_processing_permit(PermitsRef)
    end.


%% @private
-spec wait_for_file_processing_permit(atomics:atomics_ref()) -> ok.
wait_for_file_processing_permit(PermitsRef) ->
    case try_acquire_file_processing_permit(PermitsRef) of
        true ->
            ok;
        false ->
            timer:sleep(?FILE_PROCESSING_PERMIT_POLL_INTERVAL_MS),
            wait_for_file_processing_permit(PermitsRef)
    end.


%% @private
-spec try_acquire_file_processing_permit(atomics:atomics_ref()) -> boolean().
try_acquire_file_processing_permit(PermitsRef) ->
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
    [onenv_file_test_utils:object()].
collect_regular_files(Objects) when is_list(Objects) ->
    lists:flatmap(fun collect_regular_files/1, Objects);
collect_regular_files(#object{type = ?REGULAR_FILE_TYPE} = Object) ->
    [Object];
collect_regular_files(#object{type = ?DIRECTORY_TYPE, children = Children}) ->
    collect_regular_files(Children).


%% @private
%% TODO VFS-XXX remove the verification (leaving plain replicate_file_by_read
%% calls) once the lost file_location update is fixed
%% Replicates the file to the given provider by reading it there and verifies
%% that the fetched blocks actually got registered in the replica metadata:
%% the fetch may write the whole content to the storage while the update of
%% the local file_location doc (its blocks and last_replication_timestamp)
%% silently never gets persisted. Such a replica is invisible (zero blocks in
%% the distribution) - a subsequent eviction quietly skips the file, breaking
%% the transfer counter expectations. A repeated read re-fetches the content
%% and re-registers the blocks.
-spec replicate_file_and_verify_replica_registration(
    node(), od_provider:id(), session:id(), file_id:file_guid(), pos_integer()
) ->
    ok.
replicate_file_and_verify_replica_registration(Node, ProviderId, SessionId, FileGuid, ReadsLeft) ->
    FileSize = replicate_file_by_read(Node, SessionId, FileGuid),
    case await_replica_registration(
        Node, ProviderId, FileGuid, FileSize, ?REPLICA_REGISTRATION_AWAIT_ATTEMPTS
    ) of
        ok ->
            ok;
        {error, replica_not_registered} when ReadsLeft > 1 ->
            ct:pal(
                "WARNING: the replication (by read) of file ~ts on provider ~ts left "
                "no blocks registered in its replica metadata - retrying the read "
                "(reads left: ~tp)",
                [FileGuid, ProviderId, ReadsLeft - 1]
            ),
            replicate_file_and_verify_replica_registration(
                Node, ProviderId, SessionId, FileGuid, ReadsLeft - 1
            );
        {error, replica_not_registered} ->
            ct:fail({replica_registration_lost, FileGuid, ProviderId})
    end.


%% @private
%% TODO VFS-XXX remove along with the verification above
-spec await_replica_registration(
    node(), od_provider:id(), file_id:file_guid(), file_meta:size(), non_neg_integer()
) ->
    ok | {error, replica_not_registered}.
await_replica_registration(_Node, _ProviderId, _FileGuid, _FileSize, 0) ->
    {error, replica_not_registered};
await_replica_registration(Node, ProviderId, FileGuid, FileSize, AttemptsLeft) ->
    % freshly registered blocks may become visible in the distribution only
    % after the (delayed) fslogic cache flush - poll before declaring them lost
    {ok, Distribution} = ?assertMatch({ok, _}, opt_file_metadata:get_distribution_deprecated(
        Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid)
    )),
    IsRegistered = lists:any(fun(ProviderDistribution) ->
        maps:get(<<"providerId">>, ProviderDistribution) =:= ProviderId
            andalso maps:get(<<"totalBlocksSize">>, ProviderDistribution) =:= FileSize
    end, Distribution),
    case IsRegistered of
        true ->
            ok;
        false ->
            timer:sleep(timer:seconds(1)),
            await_replica_registration(Node, ProviderId, FileGuid, FileSize, AttemptsLeft - 1)
    end.


%% @private
%% Reads the whole file on the given node, which forces replication of its
%% content, and returns its size. The size is taken from stat rather than the
%% declared tree content (which does not cover data written outside of the
%% tree spec, e.g. by the big file test) and the reads are chunked so that
%% even such files do not materialize as single huge binaries.
-spec replicate_file_by_read(node(), session:id(), file_id:file_guid()) ->
    file_meta:size().
replicate_file_by_read(Node, SessionId, FileGuid) ->
    {ok, #file_attr{size = FileSize}} = ?assertMatch({ok, _}, lfm_proxy:stat(
        Node, SessionId, ?FILE_REF(FileGuid)
    )),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(
        Node, SessionId, ?FILE_REF(FileGuid), read
    )),
    read_file_in_chunks(Node, Handle, 0, FileSize),
    ok = lfm_proxy:close(Node, Handle),
    FileSize.


%% @private
-spec read_file_in_chunks(node(), lfm:handle(), non_neg_integer(), non_neg_integer()) -> ok.
read_file_in_chunks(_Node, _Handle, Offset, FileSize) when Offset >= FileSize ->
    ok;
read_file_in_chunks(Node, Handle, Offset, FileSize) ->
    ChunkSize = min(?PREREPLICATION_READ_CHUNK_SIZE, FileSize - Offset),
    {ok, Data} = ?assertMatch({ok, _}, lfm_proxy:read(Node, Handle, Offset, ChunkSize)),
    ?assertEqual(ChunkSize, byte_size(Data)),
    read_file_in_chunks(Node, Handle, Offset + ChunkSize, FileSize).


%% @private
%% Polls the transfer doc on the given provider until it matches the
%% expectations. A transfer that has already ended will never change again -
%% only trailing dbsync revisions of the same end state (e.g. the other
%% migration subtask's fields or a late histogram flush) may still arrive -
%% so once an ended yet mismatched transfer is seen, the remaining attempts
%% are capped to a short grace instead of idling through the full (potentially
%% minutes-long) attempt budget.
-spec await_transfer_state(
    oct_background:entity_selector(), transfer:id(), expected_transfer(), non_neg_integer()
) ->
    ok | {failed, {error, term()} | {mismatched_transfer_fields, list(), transfer:transfer()}}.
await_transfer_state(ProviderSelector, TransferId, ExpectedTransfer, AttemptsLeft) ->
    Result = check_transfer_state(ProviderSelector, TransferId, ExpectedTransfer),

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
                ProviderSelector, TransferId, ExpectedTransfer, EffectiveAttemptsLeft - 1
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
