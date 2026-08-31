%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module tests replica eviction transfers.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_eviction_test_SUITE).
-author("Bartosz Walkowicz").

-include("transfer_test.hrl").
-include("onenv_test_utils.hrl").
-include("modules/datastore/transfer.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    %% --- basic shapes ---
    empty_dir_test/1,
    tree_of_empty_dirs_test/1,
    regular_file_test/1,
    file_in_directory_test/1,
    big_file_test/1,

    %% --- scale ---
    hundred_files_in_one_transfer_test/1,
    hundred_files_in_separate_transfers_test/1,

    %% --- protection flags ---
    transfer_despite_protection_flags_test/1,

    %% --- transfers by view ---
    regular_file_by_view_test/1,
    files_matched_by_view_with_reduce_test/1,
    transfer_by_not_existing_view_test/1,
    transfer_by_view_emitting_invalid_file_id_test/1,
    transfer_by_view_emitting_not_existing_file_id_test/1,
    transfer_by_empty_view_test/1,
    transfer_by_view_with_not_matching_key_test/1,
    hundred_files_by_view_test/1,
    hundred_files_by_view_with_batch_10_test/1,

    %% --- transfer lifecycle ---
    cancel_ongoing_transfer_test/1,
    rerun_failed_file_transfer_test/1,
    rerun_failed_file_transfer_by_other_user_test/1,
    rerun_failed_dir_transfer_test/1,
    rerun_failed_view_transfer_test/1,

    %% --- failures and races ---
    many_simultaneous_failed_transfers_test/1,
    file_removed_during_transfer_test/1,

    %% --- modified replicas ---
    eviction_of_remotely_modified_replica_test/1,
    eviction_of_locally_modified_replica_test/1,

    %% --- space occupancy ---
    eviction_decreases_space_occupancy_test/1
]).

all() -> [
    %% --- basic shapes ---
    empty_dir_test,
    tree_of_empty_dirs_test,
    regular_file_test,
    file_in_directory_test,
    big_file_test,

    %% --- scale ---
    hundred_files_in_one_transfer_test,
    hundred_files_in_separate_transfers_test,

    %% --- protection flags ---
    transfer_despite_protection_flags_test,

    %% --- transfers by view ---
    regular_file_by_view_test,
    files_matched_by_view_with_reduce_test,
    transfer_by_not_existing_view_test,
    transfer_by_view_emitting_invalid_file_id_test,
    transfer_by_view_emitting_not_existing_file_id_test,
    transfer_by_empty_view_test,
    transfer_by_view_with_not_matching_key_test,
    hundred_files_by_view_test,
    hundred_files_by_view_with_batch_10_test,

    %% --- transfer lifecycle ---
    cancel_ongoing_transfer_test,
    rerun_failed_file_transfer_test,
    rerun_failed_file_transfer_by_other_user_test,
    rerun_failed_dir_transfer_test,
    rerun_failed_view_transfer_test,

    %% --- failures and races ---
    many_simultaneous_failed_transfers_test,
    file_removed_during_transfer_test,

    %% --- modified replicas ---
    eviction_of_remotely_modified_replica_test,
    eviction_of_locally_modified_replica_test,

    %% --- space occupancy ---
    eviction_decreases_space_occupancy_test
].

-define(SUITE_CTX, #transfer_test_suite_ctx{
    transfer_type = eviction,
    space_selector = space_krk_par_p,
    user_selector = user1,
    creation_provider_selector = krakow,
    other_provider_selector = paris
}).
-define(run_test(), transfer_common_test_base:?FUNCTION_NAME(?SUITE_CTX)).

% big enough for the modifications (a byte written at offset 1) to land
% strictly inside the file
-define(MODIFIED_REPLICA_FILE_SIZE, 1000).
% block layout of a replica whose second byte got invalidated by the byte
% written on the remote provider
-define(BLOCKS_WITH_SECOND_BYTE_INVALIDATED, [
    #file_block{offset = 0, size = 1},
    #file_block{offset = 2, size = ?MODIFIED_REPLICA_FILE_SIZE - 2}
]).


%%%==================================================================
%%% Test functions
%%%===================================================================


%% --- basic shapes ---


empty_dir_test(_Config) -> ?run_test().
tree_of_empty_dirs_test(_Config) -> ?run_test().
regular_file_test(_Config) -> ?run_test().
file_in_directory_test(_Config) -> ?run_test().
big_file_test(_Config) -> ?run_test().


%% --- scale ---


hundred_files_in_one_transfer_test(_Config) -> ?run_test().
hundred_files_in_separate_transfers_test(_Config) -> ?run_test().


%% --- protection flags ---


transfer_despite_protection_flags_test(_Config) -> ?run_test().


%% --- transfers by view ---


regular_file_by_view_test(_Config) -> ?run_test().
files_matched_by_view_with_reduce_test(_Config) -> ?run_test().
transfer_by_not_existing_view_test(_Config) -> ?run_test().
transfer_by_view_emitting_invalid_file_id_test(_Config) -> ?run_test().
transfer_by_view_emitting_not_existing_file_id_test(_Config) -> ?run_test().
transfer_by_empty_view_test(_Config) -> ?run_test().
transfer_by_view_with_not_matching_key_test(_Config) -> ?run_test().
hundred_files_by_view_test(_Config) -> ?run_test().
hundred_files_by_view_with_batch_10_test(_Config) -> ?run_test().


%% --- transfer lifecycle ---


cancel_ongoing_transfer_test(_Config) -> ?run_test().
rerun_failed_file_transfer_test(_Config) -> ?run_test().
rerun_failed_file_transfer_by_other_user_test(_Config) -> ?run_test().
rerun_failed_dir_transfer_test(_Config) -> ?run_test().
rerun_failed_view_transfer_test(_Config) -> ?run_test().


%% --- failures and races ---


many_simultaneous_failed_transfers_test(_Config) -> ?run_test().
file_removed_during_transfer_test(_Config) -> ?run_test().


%% --- modified replicas ---


eviction_of_remotely_modified_replica_test(_Config) ->
    TestSuiteCtx = #transfer_test_suite_ctx{
        user_selector = UserSelector,
        creation_provider_selector = CreationProviderSelector,
        other_provider_selector = OtherProviderSelector
    } = ?SUITE_CTX,
    RootDir = #object{children = [FileObject = #object{guid = FileGuid}]} =
        transfer_test_utils:create_file_tree(TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{
            children = [#file_spec{content = ?RAND_CONTENT(?MODIFIED_REPLICA_FILE_SIZE)}]
        }),
    transfer_test_utils:ensure_initial_replicas(TestSuiteCtx, RootDir),

    % modify the file on the creation provider - the evicting provider's
    % replica becomes partially outdated
    CreationNode = oct_background:get_random_provider_node(CreationProviderSelector),
    SessionId = oct_background:get_user_session_id(UserSelector, CreationProviderSelector),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(
        CreationNode, SessionId, ?FILE_REF(FileGuid), write
    )),
    ?assertMatch({ok, _}, lfm_proxy:write(CreationNode, Handle, 1, <<"#">>)),
    ok = lfm_proxy:close(CreationNode, Handle),

    % await the modification (with its version bump) syncing to the evicting
    % provider - the modified byte gets invalidated in its replica
    transfer_test_utils:await_distribution(
        [CreationProviderSelector, OtherProviderSelector], FileGuid, [
            {CreationProviderSelector, ?MODIFIED_REPLICA_FILE_SIZE},
            {OtherProviderSelector, ?BLOCKS_WITH_SECOND_BYTE_INVALIDATED}
        ]
    ),

    % the eviction succeeds - the creation provider's newer replica fully
    % covers what remains of the evicting provider's one
    TransferId = transfer_test_utils:schedule_transfer(TestSuiteCtx, FileObject),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, FileObject, #{}),
    transfer_test_utils:assert_distribution(TestSuiteCtx, FileObject).


eviction_of_locally_modified_replica_test(_Config) ->
    TestSuiteCtx = #transfer_test_suite_ctx{
        user_selector = UserSelector,
        creation_provider_selector = CreationProviderSelector,
        other_provider_selector = OtherProviderSelector
    } = ?SUITE_CTX,
    RootDir = #object{children = [FileObject = #object{guid = FileGuid}]} =
        transfer_test_utils:create_file_tree(TestSuiteCtx, ?FUNCTION_NAME, #dir_spec{
            children = [#file_spec{content = ?RAND_CONTENT(?MODIFIED_REPLICA_FILE_SIZE)}]
        }),
    transfer_test_utils:ensure_initial_replicas(TestSuiteCtx, RootDir),

    % modify the file on the evicting provider right before its blocks get
    % deleted (the module is mock-loaded in init_per_testcase) - the deletion
    % must detect that the replica version it was allowed to delete is no
    % longer the current one
    OtherProviderNodes = oct_background:get_provider_nodes(OtherProviderSelector),
    OtherSessionId = oct_background:get_user_session_id(UserSelector, OtherProviderSelector),
    ok = test_utils:mock_expect(OtherProviderNodes, replica_deletion_req, delete_blocks, fun(
        FileCtx, Blocks, AllowedVV
    ) ->
        {ok, Handle} = lfm:open(OtherSessionId, ?FILE_REF(FileGuid), write),
        {ok, _, 1} = lfm:write(Handle, 1, <<"#">>),
        ok = lfm:fsync(Handle),
        ok = lfm:release(Handle),
        % meck:passthrough does not work for functions calling other mocked
        % functions of the same module
        erlang:apply(meck_util:original_name(replica_deletion_req), delete_blocks, [
            FileCtx, Blocks, AllowedVV
        ])
    end),

    TransferId = transfer_test_utils:schedule_transfer(TestSuiteCtx, FileObject),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, FileObject, #{
        eviction_status => ?FAILED_STATUS,
        failed_files => 1,
        files_evicted => 0
    }),

    % nothing was evicted; the local modification (instead) invalidated the
    % modified byte in the creation provider's replica
    transfer_test_utils:await_distribution(
        [CreationProviderSelector, OtherProviderSelector], FileGuid, [
            {CreationProviderSelector, ?BLOCKS_WITH_SECOND_BYTE_INVALIDATED},
            {OtherProviderSelector, ?MODIFIED_REPLICA_FILE_SIZE}
        ]
    ).


%% --- space occupancy ---


eviction_decreases_space_occupancy_test(_Config) ->
    TestSuiteCtx = #transfer_test_suite_ctx{
        space_selector = SpaceSelector,
        other_provider_selector = OtherProviderSelector
    } = ?SUITE_CTX,
    SpaceId = oct_background:get_space_id(SpaceSelector),
    RootDir = #object{children = [FileObject = #object{content = FileContent}]} =
        transfer_test_utils:create_file_tree(
            TestSuiteCtx, ?FUNCTION_NAME,
            #dir_spec{children = [#file_spec{content = ?RAND_CONTENT()}]}
        ),
    FileSize = byte_size(FileContent),

    % the shared space is used by the other test cases too - assert the
    % occupancy changes relative to the state before the initial replication
    OccupancyBefore = transfer_test_utils:get_space_occupancy(OtherProviderSelector, SpaceId),
    transfer_test_utils:ensure_initial_replicas(TestSuiteCtx, RootDir),
    await_space_occupancy(OtherProviderSelector, SpaceId, OccupancyBefore + FileSize),

    TransferId = transfer_test_utils:schedule_transfer(TestSuiteCtx, FileObject),
    transfer_test_utils:await_transfer_ended(TestSuiteCtx, TransferId, FileObject, #{}),
    transfer_test_utils:assert_distribution(TestSuiteCtx, FileObject),

    % evicting the replica released its storage
    await_space_occupancy(OtherProviderSelector, SpaceId, OccupancyBefore).


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, transfer_test_utils, transfer_common_test_base],
    opt:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "2op",
        envs = [
            {op_worker, op_worker, [
                {fuse_session_grace_period_seconds, 24 * 60 * 60},
                {provider_token_ttl_sec, 24 * 60 * 60},
                % transfer status updates are sparse single-doc changes - with the
                % default (5s) broadcast interval they idle in the dbsync out-stream
                % aggregation window for several of its cycles, inflating every
                % cross-provider await by tens of seconds
                {dbsync_changes_broadcast_interval, timer:seconds(1)}
            ]},
            {op_worker, cluster_worker, [
                {cache_to_disk_delay_ms, timer:seconds(1)},
                {cache_to_disk_force_delay_ms, timer:seconds(2)}
            ]}
        ]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(Case = eviction_of_locally_modified_replica_test, Config) ->
    % the environment cleanup of the default clause runs FIRST - if it
    % crashes, ct skips the case WITHOUT running end_per_testcase and an
    % already-installed mock would leak into all subsequent cases
    NewConfig = init_per_testcase(?DEFAULT_CASE(Case), Config),
    % only mock-load the module here - the mock expectation needs the file
    % guid, so it is set in the test body
    #transfer_test_suite_ctx{other_provider_selector = OtherProviderSelector} = ?SUITE_CTX,
    OtherProviderNodes = oct_background:get_provider_nodes(OtherProviderSelector),
    ok = test_utils:mock_new(OtherProviderNodes, replica_deletion_req, [passthrough]),
    NewConfig;

init_per_testcase(Case, Config) ->
    transfer_common_test_base:init_per_testcase(Case, ?SUITE_CTX, Config).


end_per_testcase(Case = eviction_of_locally_modified_replica_test, Config) ->
    #transfer_test_suite_ctx{other_provider_selector = OtherProviderSelector} = ?SUITE_CTX,
    OtherProviderNodes = oct_background:get_provider_nodes(OtherProviderSelector),
    test_utils:mock_unload(OtherProviderNodes, replica_deletion_req),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(Case, Config) ->
    transfer_common_test_base:end_per_testcase(Case, ?SUITE_CTX, Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
await_space_occupancy(ProviderSelector, SpaceId, ExpOccupancy) ->
    ?assertEqual(
        ExpOccupancy,
        transfer_test_utils:get_space_occupancy(ProviderSelector, SpaceId),
        ?ATTEMPTS
    ).
