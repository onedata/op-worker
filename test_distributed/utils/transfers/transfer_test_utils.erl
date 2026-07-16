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
%%%    can be overridden with an exact value, 'skip', {range, Min, Max}
%%%    or a predicate fun. The special 'tree_bytes' override adjusts the
%%%    derivation input rather than any single field: it replaces the total
%%%    byte size counted from the declared tree (for content written outside
%%%    of the tree spec, e.g. a big file filled after creation) and drives
%%%    the per-type expected byte counters and histogram sums;
%%% 5. assert_distribution/2,3 - asserts the expected post-transfer block
%%%    distribution, again derived from the declared file tree and the
%%%    transfer type, with optional per-file size overrides.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_test_utils).
-author("Bartosz Walkowicz").

-include("transfer_test.hrl").
-include("onenv_test_utils.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/onedata_file.hrl").
-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([
    remove_leftover_file_trees/2,
    gen_nested_tree_spec/2,
    create_file_tree/3,
    ensure_initial_replicas/2,
    schedule_transfer/2,
    await_replication_started/2,
    await_transfer_ended/4, await_transfer_ended/5,
    assert_distribution/2, assert_distribution/3,
    remove_all_transfers/1,

    get_space_support_size/2,
    get_space_occupancy/2,
    set_space_occupancy/3
]).

-type transfer_type() :: replication | eviction | migration.

-type field_expectation() ::
    term() |
    skip |
    {range, integer(), integer()} |
    % asserts a bytes-per-provider histogram field: the histogram of the given
    % provider must span the given number of slots and sum up to the given bytes
    {histogram_sum, od_provider:id(), non_neg_integer(), non_neg_integer()} |
    fun((term()) -> boolean()).
-type expected_transfer() :: #{atom() => field_expectation()}.

-export_type([transfer_type/0, field_expectation/0, expected_transfer/0]).

-type suite_ctx() :: #transfer_test_suite_ctx{}.

-define(SPACE_ROOT_LS_LIMIT, 10000).


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
            true -> onenv_file_test_utils:rm_and_sync_file(UserSelector, ChildGuid);
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
schedule_transfer(#transfer_test_suite_ctx{
    transfer_type = TransferType,
    user_selector = UserSelector,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}, #object{guid = FileGuid}) ->
    SessionId = oct_background:get_user_session_id(UserSelector, CreationProviderSelector),
    CreationProviderId = oct_background:get_provider_id(CreationProviderSelector),
    OtherProviderId = oct_background:get_provider_id(OtherProviderSelector),

    {ReplicatingProviderId, EvictingProviderId} = case TransferType of
        replication -> {OtherProviderId, undefined};
        eviction -> {undefined, OtherProviderId};
        migration -> {OtherProviderId, CreationProviderId}
    end,
    {ok, TransferId} = ?assertMatch({ok, _}, opt_transfers:schedule_file_transfer(
        CreationProviderSelector, SessionId, ?FILE_REF(FileGuid),
        ReplicatingProviderId, EvictingProviderId, undefined
    )),
    TransferId.


-spec await_replication_started(oct_background:entity_selector(), transfer:id()) ->
    ok.
await_replication_started(ProviderSelector, TransferId) ->
    ?assertEqual(true, case opw_test_rpc:call(ProviderSelector, transfer, get, [TransferId]) of
        {ok, #document{value = #transfer{
            bytes_replicated = BytesReplicated,
            files_replicated = FilesReplicated
        }}} ->
            BytesReplicated > 0 orelse FilesReplicated > 0;
        {error, _} ->
            false
    end, ?ATTEMPTS).


-spec await_transfer_ended(suite_ctx(), transfer:id(), onenv_file_test_utils:object(), expected_transfer()) ->
    ok.
await_transfer_ended(SuiteCtx, TransferId, TransferRootObject, Overrides) ->
    await_transfer_ended(SuiteCtx, TransferId, TransferRootObject, Overrides, ?ATTEMPTS).


-spec await_transfer_ended(
    suite_ctx(),
    transfer:id(),
    onenv_file_test_utils:object(),
    expected_transfer(),
    non_neg_integer()
) ->
    ok.
await_transfer_ended(#transfer_test_suite_ctx{
    transfer_type = TransferType,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
} = SuiteCtx, TransferId, TransferRootObject, Overrides, Attempts) ->
    ExpectedTransfer = maps:merge(
        build_expected_transfer(SuiteCtx, TransferRootObject, Overrides),
        maps:remove(tree_bytes, Overrides)
    ),

    %% TODO VFS-13678 remove debug logging before merge to develop
    ct:pal("Awaiting ~tp transfer ~ts end, expected state:~n~tp", [
        TransferType, TransferId, ExpectedTransfer
    ]),

    lists:foreach(fun(ProviderSelector) ->
        await_transfer_state(ProviderSelector, TransferId, ExpectedTransfer, Attempts)
    end, [CreationProviderSelector, OtherProviderSelector]).


-spec assert_distribution(suite_ctx(), onenv_file_test_utils:object()) ->
    ok.
assert_distribution(TestSuiteCtx, TransferRootObject) ->
    assert_distribution(TestSuiteCtx, TransferRootObject, #{}).


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
    onenv_file_test_utils:object(),
    #{file_id:file_guid() => file_meta:size()}
) ->
    ok.
assert_distribution(#transfer_test_suite_ctx{
    transfer_type = TransferType,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}, TransferRootObject, FileSizeOverrides) ->
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
    end, collect_regular_files(TransferRootObject)),

    %% TODO VFS-13678 remove debug logging before merge to develop
    ct:pal("Asserting file distribution after ~tp, expected:~n~tp", [
        TransferType,
        [{FileName, ExpSizePerNode} || {FileName, _, ExpSizePerNode} <- FilesWithExpDistribution]
    ]),

    lists:foreach(fun({_FileName, FileGuid, ExpSizePerNode}) ->
        file_test_utils:await_distribution([CreationNode, OtherNode], FileGuid, ExpSizePerNode)
    end, FilesWithExpDistribution).


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
-spec build_expected_transfer(suite_ctx(), onenv_file_test_utils:object(), expected_transfer()) ->
    expected_transfer().
build_expected_transfer(#transfer_test_suite_ctx{
    transfer_type = TransferType,
    user_selector = UserSelector,
    creation_provider_selector = CreationProviderSelector,
    other_provider_selector = OtherProviderSelector
}, TransferRootObject, Overrides) ->
    CreationProviderId = oct_background:get_provider_id(CreationProviderSelector),
    OtherProviderId = oct_background:get_provider_id(OtherProviderSelector),
    {FilesCount, DeclaredBytesCount} = count_files_and_bytes(TransferRootObject),
    BytesCount = maps:get(tree_bytes, Overrides, DeclaredBytesCount),

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
                bytes_replicated => BytesCount,
                files_evicted => 0
            }, build_expected_histograms(CreationProviderId, BytesCount));
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
                bytes_replicated => BytesCount,
                files_evicted => FilesCount
            }, build_expected_histograms(CreationProviderId, BytesCount))
    end,
    maps:merge(CommonExpectation, TypeSpecificExpectation).


%% @private
-spec build_expected_histograms(od_provider:id(), non_neg_integer()) ->
    expected_transfer().
build_expected_histograms(SourceProviderId, BytesCount) ->
    #{
        min_hist => build_expected_histogram(SourceProviderId, BytesCount, ?MIN_HIST_LENGTH),
        hr_hist => build_expected_histogram(SourceProviderId, BytesCount, ?HOUR_HIST_LENGTH),
        dy_hist => build_expected_histogram(SourceProviderId, BytesCount, ?DAY_HIST_LENGTH),
        mth_hist => build_expected_histogram(SourceProviderId, BytesCount, ?MONTH_HIST_LENGTH)
    }.


%% @private
-spec build_expected_histogram(od_provider:id(), non_neg_integer(), non_neg_integer()) ->
    field_expectation().
build_expected_histogram(_SourceProviderId, 0, _HistLength) ->
    #{};
build_expected_histogram(SourceProviderId, BytesCount, HistLength) ->
    % transferred bytes are accounted per source provider; histogram window
    % sums (rather than exact slots) are asserted as slot boundaries shift
    % during the transfer
    % TODO why do we need HistLen ????
    {histogram_sum, SourceProviderId, BytesCount, HistLength}.


%% @private
-spec count_files_and_bytes(onenv_file_test_utils:object()) ->
    {non_neg_integer(), non_neg_integer()}.
count_files_and_bytes(#object{type = ?REGULAR_FILE_TYPE, content = Content}) ->
    {1, byte_size(Content)};
count_files_and_bytes(#object{type = ?DIRECTORY_TYPE, children = Children}) ->
    lists:foldl(fun(Child, {FilesCountAcc, BytesCountAcc}) ->
        {FilesCount, BytesCount} = count_files_and_bytes(Child),
        {FilesCountAcc + FilesCount, BytesCountAcc + BytesCount}
    end, {0, 0}, Children).


%% @private
-spec collect_regular_files(onenv_file_test_utils:object()) ->
    [onenv_file_test_utils:object()].
collect_regular_files(#object{type = ?REGULAR_FILE_TYPE} = Object) ->
    [Object];
collect_regular_files(#object{type = ?DIRECTORY_TYPE, children = Children}) ->
    lists:flatmap(fun collect_regular_files/1, Children).


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
matches_expectation({range, Min, Max}, Value) ->
    is_integer(Value) andalso Value >= Min andalso Value =< Max;
matches_expectation({histogram_sum, ProviderId, BytesCount, HistLength}, HistPerProvider) ->
    is_map(HistPerProvider) andalso case maps:find(ProviderId, HistPerProvider) of
        {ok, Hist} -> length(Hist) =:= HistLength andalso lists:sum(Hist) =:= BytesCount;
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
